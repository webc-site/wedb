//! 向量域错误吞没修复回归（RMW 零覆写 + 幽灵索引桩）
//!
//! 对齐 C# 裁决语义的三条定向回归：
//!   * RMW 读存储 IO 失败 ⇒ 返回 false 且绝不覆写存量向量（对标
//!     ReadModifyWriteCallbackUnmanaged 失败返回 0、上游不落写；
//!     NotFound 空值属合法 initial update，两种语义不合并）；
//!   * 原生索引创建失败 ⇒ 报错且登记表不落桩（对标 CreateIndex 异常上抛
//!     会话 catch、登记记录不落）；
//!   * 原生索引重建失败 ⇒ 报错且登记记录维持 ptr=0 原状（后续访问可重试
//!     重建），绝不落 ptr=1 幽灵桩。

use std::sync::{
  Arc,
  atomic::{AtomicBool, Ordering},
};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wbase::pool::{AlignedBuf, BufferPool};
use wdev::{Device, Error, Result as DevResult, SegmentedDevice};
use wkv::{StoreConfig, WedbStore};
use wnode::resp::vector::{
  vector_manager::{VectorManager, VectorManagerOptions, VectorManagerResult},
  vector_manager_index::Index,
  vector_manager_locking::{CreateIndexParams, ReadIndexOutcome},
  vector_store_callbacks::WedbVectorStoreCallbacks,
};
use wval::SessionPrefixBuf;
use wvector::{
  Callbacks, VectorDistanceMetricType, VectorQuantType,
  store::{StoreCallbacks, term},
};

/// 读失败注入设备（包装真实单文件设备；armed 时全部读 IO 报短读 EOF）
struct FailingDevice {
  inner: Arc<SegmentedDevice>,
  fail_reads: AtomicBool,
}

fn read_blocked(buf: &AlignedBuf) -> DevResult<usize> {
  Err(Error::UnexpectedEof {
    expected: buf.len(),
    actual: 0,
  })
}

impl Device for FailingDevice {
  fn sector_size(&self) -> usize {
    self.inner.sector_size()
  }

  fn segment_size(&self) -> Option<u64> {
    self.inner.segment_size()
  }

  fn pool(&self) -> &Arc<BufferPool> {
    self.inner.pool()
  }

  async fn write_aligned(&self, offset: u64, buf: AlignedBuf) -> (DevResult<usize>, AlignedBuf) {
    self.inner.write_aligned(offset, buf).await
  }

  async fn read_aligned(&self, offset: u64, buf: AlignedBuf) -> (DevResult<usize>, AlignedBuf) {
    if self.fail_reads.load(Ordering::Acquire) {
      return (read_blocked(&buf), buf);
    }
    self.inner.read_aligned(offset, buf).await
  }

  async fn read_raw(&self, offset: u64, buf: AlignedBuf) -> (DevResult<usize>, AlignedBuf) {
    if self.fail_reads.load(Ordering::Acquire) {
      return (read_blocked(&buf), buf);
    }
    self.inner.read_raw(offset, buf).await
  }

  async fn sync(&self) -> DevResult<()> {
    self.inner.sync().await
  }

  async fn truncate_until_segment(&self, segment_id: u32) -> DevResult<()> {
    self.inner.truncate_until_segment(segment_id).await
  }
}

/// 内存存储桩（仅作回调句柄装配；本文件用例的失败注入均在 service/设备层）
struct MemVectorStore;

impl StoreCallbacks for MemVectorStore {
  fn read_multi<F>(&self, _context: u64, _keys: &[u8], _length_hint: usize, _f: F) -> bool
  where
    F: FnMut(u32, &[u8]),
  {
    true
  }

  fn read<F>(&self, _context: u64, _key: &[u8], _f: F) -> bool
  where
    F: FnMut(&[u8]),
  {
    false
  }

  fn write(&self, _context: u64, _key: &[u8], _value: &[u8]) -> bool {
    true
  }

  fn delete(&self, _context: u64, _key: &[u8]) -> bool {
    false
  }

  fn rmw<F>(&self, _context: u64, _key: &[u8], _write_len: usize, _f: F) -> bool
  where
    F: FnMut(&mut [u8]),
  {
    true
  }

  fn filter(&self, _context: u64, _internal_id: u32) -> bool {
    true
  }

  fn log(&self, _context: u64, _msg: &str) {}
}

fn manager() -> VectorManager<MemVectorStore> {
  VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(MemVectorStore)),
  )
}

fn failing_index_config() -> CreateIndexParams {
  CreateIndexParams {
    hash_slot: 0,
    dims: 2,
    reduce_dims: 0,
    // 非法量化类型 ⇒ 原生索引构建必失败（service.create_index → CreateIndexError）
    quant: VectorQuantType::Invalid,
    build_exploration_factor: 64,
    num_links: 8,
    distance_metric: VectorDistanceMetricType::L2,
  }
}

/// RMW 冷区读 IO 失败：返回 false、不进 updater、绝不零覆写存量向量
#[test]
fn rmw_read_io_failure_never_overwrites() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempdir().unwrap();
    let device = Arc::new(FailingDevice {
      inner: Arc::new(SegmentedDevice::single_file(dir.path().join("vec.db")).unwrap()),
      fail_reads: AtomicBool::new(false),
    });
    let config = StoreConfig::new(1024, 4096, 16, 0.5).unwrap();
    let store = Arc::new(WedbStore::open(config, device.clone()).unwrap());
    let session = Arc::new(store.new_session().unwrap());
    let callbacks = WedbVectorStoreCallbacks::new(session);
    let ctx = term::METADATA;
    let key = b"rmw-read-failure";

    // 存量向量落盘并整段驱逐至磁盘冷区（其后读全部走磁盘冷读路径）
    assert!(callbacks.write(ctx, key, &[1u8; 8]));
    store.flush_and_evict_all().await.unwrap();
    let mut val = [0u8; 8];
    assert!(callbacks.read(ctx, key, |curr| val.copy_from_slice(curr)));
    assert_eq!(val, [1u8; 8]);

    // 注入读失败：RMW 读冷区失败 ⇒ 返回 false，f 不被回调、写不执行
    device.fail_reads.store(true, Ordering::Release);
    let tail = store.tail_address();
    let mut f_called = false;
    assert!(
      !callbacks.rmw(ctx, key, 4, |data| {
        f_called = true;
        data[0] = 9;
      }),
      "读失败必须返回 false，对齐 C# RMW 失败返回 0"
    );
    assert!(!f_called, "读失败不进 updater，f 不应被回调");
    assert_eq!(store.tail_address(), tail, "读失败严禁零覆写存量向量");

    // 解除注入：存量向量原样保留（NotFound 与 IO 失败语义未合并的对照面）
    device.fail_reads.store(false, Ordering::Release);
    let mut val = [0u8; 8];
    assert!(callbacks.read(ctx, key, |curr| val.copy_from_slice(curr)));
    assert_eq!(val, [1u8; 8], "存量向量不得被零字节覆写");
  });
}

/// 原生索引创建失败：报错且登记表不落桩（对齐 C# CreateIndex 异常上抛、登记不落）
#[test]
fn create_index_failure_leaves_no_registry_stub() {
  let mgr = manager();
  let root = SessionPrefixBuf::ROOT.as_slice();
  let r = mgr.read_or_create_vector_index(root, b"ghost", Some(&failing_index_config()));
  assert!(
    matches!(r, Err(VectorManagerResult::Invalid)),
    "创建失败须报错"
  );
  assert!(
    mgr.read_stored_index(root, b"ghost").is_none(),
    "创建失败严禁落登记幽灵桩"
  );
}

/// 原生索引重建失败：报错且登记记录维持 ptr=0 原状（可重试重建），绝不落幽灵桩
#[test]
fn recreate_index_failure_keeps_stale_record() {
  let mgr = manager();
  let root = SessionPrefixBuf::ROOT.as_slice();
  let stale = Index {
    context: 8,
    dimensions: 2,
    quant_type: VectorQuantType::Invalid,
    ..Index::default()
  };
  mgr.write_stored_index(root, b"stale", &stale.to_bytes());

  let outcome = mgr.read_vector_index_core(root, b"stale", false);
  assert!(
    matches!(outcome, ReadIndexOutcome::Failed),
    "重建失败须以 Failed 报错，对齐 C# RecreateIndex 异常上抛"
  );

  // 登记记录维持 ptr=0 原状：绝不落 ptr=1 幽灵桩（登记表与原生索引失配）
  let bytes = mgr.read_stored_index(root, b"stale").unwrap();
  assert_eq!(Index::from_bytes(&bytes).unwrap().index_ptr, 0);

  // 便捷读面同样以缺读上报：重放层对 None 自有错误出口，错误不被吞
  let (index, _lock) = mgr.read_vector_index(root, b"stale");
  assert!(index.is_none());
}
