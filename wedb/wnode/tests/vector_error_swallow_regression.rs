//! 向量域错误吞没修复回归（RMW 零覆写 + 幽灵索引桩）
//!
//! 对齐 C# 裁决语义的四条定向回归：
//!   * RMW 读存储 IO 失败 ⇒ 返回 false 且绝不覆写存量向量（对标
//!     ReadModifyWriteCallbackUnmanaged 失败返回 0、上游不落写；
//!     NotFound 空值属合法 initial update，两种语义不合并）；
//!   * 原生索引创建失败 ⇒ 报错且登记表不落桩（对标 CreateIndex 异常上抛
//!     会话 catch、登记记录不落）；
//!   * 原生索引重建失败 ⇒ 报错且登记记录维持 ptr=0 原状（后续访问可重试
//!     重建），绝不落 ptr=1 幽灵桩；
//!   * 登记摘除写透失败 ⇒ remove 路径单点累计结构化计数且 INFO
//!     bg_task_health 可见（对标 ReplicateVectorSetRemove 失败 throw
//!     的错误必达口径，杜绝幽灵复活危害纯日志观测）。

use std::{
  future::{Future, ready},
  pin::Pin,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
};

use tempfile::tempdir;
use wbase::{
  pool::{AlignedBuf, BufferPool},
  supervise::{counter_snapshots, register_counter},
};
use wconf::RuntimeServerOptions;
use wdev::{Device, Error, Result as DevResult, SegmentedDevice};
use wkv::{StoreConfig, WedbStore};
use wnode::{
  GarnetAppendOnlyFile, GarnetLog,
  resp::vector::{
    resp_server_session_vectors::{RespServerSessionVectors, VectorReply},
    vector_manager::{VectorAddArgs, VectorManager, VectorManagerOptions, VectorManagerResult},
    vector_manager_index::Index,
    vector_manager_locking::{CreateIndexParams, ReadIndexOutcome},
    vector_manager_replication::VectorAofSink,
    vector_registry_recovery::RegistryPersistence,
    vector_store_callbacks::{ActiveVectorSessionGuard, WedbVectorStoreCallbacks},
  },
};
use wval::SessionPrefixBuf;
use wvector::{
  Callbacks, VectorDistanceMetricType, VectorQuantType, VectorValueType,
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

  fn segment_size(&self) -> u64 {
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
  async fn read_multi<F>(&self, _context: u64, _keys: &[u8], _length_hint: usize, _f: F) -> bool
  where
    F: FnMut(u32, &[u8]) + Send,
  {
    true
  }

  async fn read<F>(&self, _context: u64, _key: &[u8], _f: F) -> bool
  where
    F: FnMut(&[u8]),
  {
    false
  }

  async fn write(&self, _context: u64, _key: &[u8], _value: &[u8]) -> bool {
    true
  }

  async fn delete(&self, _context: u64, _key: &[u8]) -> bool {
    false
  }

  async fn rmw<F>(&self, _context: u64, _key: &[u8], _write_len: usize, _f: F) -> bool
  where
    F: FnMut(&mut [u8]) + Send,
  {
    true
  }

  async fn filter(&self, _context: u64, _internal_id: u32) -> bool {
    true
  }

  async fn purge_context(&self, _context: u64) -> bool {
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

fn valid_index_config() -> CreateIndexParams {
  CreateIndexParams {
    hash_slot: 0,
    dims: 2,
    reduce_dims: 0,
    quant: VectorQuantType::NoQuant,
    build_exploration_factor: 64,
    num_links: 8,
    distance_metric: VectorDistanceMetricType::L2,
  }
}

struct FaultRegistryPersistence {
  fail_put: AtomicBool,
  fail_remove: AtomicBool,
}

impl RegistryPersistence for FaultRegistryPersistence {
  fn put(
    &self,
    _physical_key: &[u8],
    _value: &[u8],
  ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
    let ok = !self.fail_put.load(Ordering::Acquire);
    Box::pin(ready(ok))
  }

  fn remove(&self, _physical_key: &[u8]) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
    let ok = !self.fail_remove.load(Ordering::Acquire);
    Box::pin(ready(ok))
  }
}

/// RMW 冷区读 IO 失败：返回 false、不进 updater、绝不零覆写存量向量
#[compio::test]
async fn rmw_read_io_failure_never_overwrites() {
  let dir = tempdir().unwrap();
  let device = Arc::new(FailingDevice {
    inner: Arc::new(SegmentedDevice::single_file(dir.path().join("vec.db")).unwrap()),
    fail_reads: AtomicBool::new(false),
  });
  let config = StoreConfig::new(1024, 4096, 16, 0.5).unwrap();
  let store = Arc::new(WedbStore::open(config, device.clone()).unwrap());
  let session = store.new_session().unwrap();
  let callbacks = WedbVectorStoreCallbacks::<FailingDevice>::new();
  // 本执行域绑定会话（回调臂无状态经线程槽取会话；本测试为单任务同步段，
  // 全程无他任务交错，守卫持至体尾即栈纪律许可形态）
  let _domain = ActiveVectorSessionGuard::bind(&session);
  let ctx = term::METADATA;
  let key = b"rmw-read-failure";

  // 存量向量落盘并整段驱逐至磁盘冷区（其后读全部走磁盘冷读路径）
  assert!(callbacks.write(ctx, key, &[1u8; 8]).await);
  store.flush_and_evict_all().await.unwrap();
  let mut val = [0u8; 8];
  assert!(
    callbacks
      .read(ctx, key, |curr| val.copy_from_slice(curr))
      .await
  );
  assert_eq!(val, [1u8; 8]);

  // 注入读失败：RMW 读冷区失败 ⇒ 返回 false，f 不被回调、写不执行
  device.fail_reads.store(true, Ordering::Release);
  let tail = store.tail_address();
  let mut f_called = false;
  assert!(
    !callbacks
      .rmw(ctx, key, 4, |data| {
        f_called = true;
        data[0] = 9;
      })
      .await,
    "读失败必须返回 false，对齐 C# RMW 失败返回 0"
  );
  assert!(!f_called, "读失败不进 updater，f 不应被回调");
  assert_eq!(store.tail_address(), tail, "读失败严禁零覆写存量向量");

  // 解除注入：存量向量原样保留（NotFound 与 IO 失败语义未合并的对照面）
  device.fail_reads.store(false, Ordering::Release);
  let mut val = [0u8; 8];
  assert!(
    callbacks
      .read(ctx, key, |curr| val.copy_from_slice(curr))
      .await
  );
  assert_eq!(val, [1u8; 8], "存量向量不得被零字节覆写");
}

/// 原生索引创建失败：报错且登记表不落桩（对齐 C# CreateIndex 异常上抛、登记不落）
#[compio::test]
async fn create_index_failure_leaves_no_registry_stub() {
  let mgr = manager();
  let root = SessionPrefixBuf::ROOT.as_slice();
  let r = mgr
    .read_or_create_vector_index(root, b"ghost", Some(&failing_index_config()))
    .await;
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
#[compio::test]
async fn recreate_index_failure_keeps_stale_record() {
  let mgr = manager();
  let root = SessionPrefixBuf::ROOT.as_slice();
  let stale = Index {
    context: 8,
    dimensions: 2,
    quant_type: VectorQuantType::Invalid,
    ..Index::default()
  };
  mgr
    .write_stored_index(root, b"stale", &stale.to_bytes())
    .await;

  let outcome = mgr.read_vector_index_core(root, b"stale", false).await;
  assert!(
    matches!(outcome, ReadIndexOutcome::Failed),
    "重建失败须以 Failed 报错，对齐 C# RecreateIndex 异常上抛"
  );

  // 登记记录维持 ptr=0 原状：绝不落 ptr=1 幽灵桩（登记表与原生索引失配）
  let bytes = mgr.read_stored_index(root, b"stale").unwrap();
  assert_eq!(Index::from_bytes(&bytes).unwrap().index_ptr, 0);

  // 便捷读面同样以缺读上报：重放层对 None 自有错误出口，错误不被吞
  let (index, _lock) = mgr.read_vector_index(root, b"stale").await;
  assert!(index.is_none());
}

/// 登记旁路写透失败：create_index_locked 报错 Invalid 且回滚内存登记
#[compio::test]
async fn registry_write_through_failure_rolls_back_memory_and_fails_create() {
  let mgr = manager();
  let fault = Arc::new(FaultRegistryPersistence {
    fail_put: AtomicBool::new(true),
    fail_remove: AtomicBool::new(false),
  });
  mgr.attach_registry_store(fault);
  let root = SessionPrefixBuf::ROOT.as_slice();
  let r = mgr
    .read_or_create_vector_index(root, b"ghost_wt", Some(&valid_index_config()))
    .await;
  assert!(
    matches!(r, Err(VectorManagerResult::Invalid)),
    "写透失败须使创建返回 Invalid"
  );
  assert!(
    mgr.read_stored_index(root, b"ghost_wt").is_none(),
    "写透失败严禁在内存登记表中残留条目（必须已回滚）"
  );
}

/// 登记旁路写透失败：recreate_index_locked 报错 Failed 且回滚内存登记
#[compio::test]
async fn registry_write_through_failure_on_recreate_rolls_back_memory() {
  let mgr = manager();
  let fault = Arc::new(FaultRegistryPersistence {
    fail_put: AtomicBool::new(false),
    fail_remove: AtomicBool::new(false),
  });
  mgr.attach_registry_store(fault.clone());
  let root = SessionPrefixBuf::ROOT.as_slice();
  let stale = Index {
    context: 8,
    dimensions: 2,
    quant_type: VectorQuantType::NoQuant,
    build_exploration_factor: 64,
    num_links: 8,
    distance_metric: VectorDistanceMetricType::L2,
    ..Index::default()
  };
  mgr
    .write_stored_index(root, b"stale_recreate", &stale.to_bytes())
    .await;

  // 开启写透故障
  fault.fail_put.store(true, Ordering::Release);
  let outcome = mgr
    .read_vector_index_core(root, b"stale_recreate", false)
    .await;
  assert!(
    matches!(outcome, ReadIndexOutcome::Failed),
    "重建写透失败须以 Failed 报错"
  );
  assert!(
    mgr.read_stored_index(root, b"stale_recreate").is_none(),
    "重建写透失败必须回滚内存登记条目"
  );
}

/// 登记旁路摘除写透失败：remove 路径失败单点计入结构化计数且 INFO 健康面可见
#[compio::test]
async fn registry_remove_failure_counts_into_structured_counter() {
  let mgr = manager();
  let fault = Arc::new(FaultRegistryPersistence {
    fail_put: AtomicBool::new(false),
    fail_remove: AtomicBool::new(false),
  });
  mgr.attach_registry_store(fault.clone());
  let root = SessionPrefixBuf::ROOT.as_slice();
  let stale = Index {
    context: 8,
    dimensions: 2,
    quant_type: VectorQuantType::NoQuant,
    ..Index::default()
  };

  // 计数登记进 wbase::supervise 快照（INFO bg_task_health 同一可见面）
  register_counter(
    "vector_registry_remove_failures",
    Arc::clone(&mgr.vector_registry_remove_failures),
  );
  let snapshot = || {
    counter_snapshots()
      .into_iter()
      .find(|c| c.name == "vector_registry_remove_failures")
      .unwrap()
      .value
  };
  assert_eq!(snapshot(), 0, "登记初始计数为零");

  // 播种登记后注入 remove 写透失败：DEL 摘除路径计数递增
  mgr
    .write_stored_index(root, b"ghost_rm", &stale.to_bytes())
    .await;
  fault.fail_remove.store(true, Ordering::Release);
  assert!(
    mgr.delete_vector_set(root, b"ghost_rm").await,
    "键存在时 DEL 摘除路径应已处理"
  );
  assert_eq!(
    mgr.vector_registry_remove_failures.load(Ordering::Acquire),
    1,
    "remove 写透失败必须计入结构化计数（内存已摘而盘上墓碑缺）"
  );
  assert_eq!(snapshot(), 1, "INFO bg_task_health 快照面必须可见该计数");

  // 解除注入：成功摘除不再累计
  fault.fail_remove.store(false, Ordering::Release);
  mgr
    .write_stored_index(root, b"ghost_rm2", &stale.to_bytes())
    .await;
  assert!(mgr.delete_vector_set(root, b"ghost_rm2").await);
  assert_eq!(
    mgr.vector_registry_remove_failures.load(Ordering::Acquire),
    1,
    "成功摘除不得累计失败计数"
  );
}

/// 向量 AOF 合成写入队失败：透传错误并使网络层 VADD/VREM/VSETATTR 返回错误帧
#[compio::test]
async fn vector_replication_aof_failure_aborts_commands() {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("vec.db")).unwrap());
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let store_session = store.new_session().unwrap();
  let _domain = ActiveVectorSessionGuard::bind(&store_session);

  let callbacks = WedbVectorStoreCallbacks::<SegmentedDevice>::new();
  let mgr = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(callbacks)),
  ));
  let sink = {
    let options = RuntimeServerOptions::default();
    let aof = Arc::new(GarnetAppendOnlyFile::new(
      Arc::new(
        GarnetLog::new(
          &options,
          {
            let (_dirs, backends) = wnode_test::test_sublogs("vec_net_fail", 1);
            backends
          },
          None,
        )
        .unwrap(),
      ),
      &options,
      None,
    ));
    let version = Arc::new(AtomicU64::new(0));
    Arc::new(VectorAofSink::new(&aof, version))
  };
  mgr.set_aof_sink(sink);
  let session = RespServerSessionVectors::new(mgr.clone());
  let prefix = SessionPrefixBuf::ROOT.as_slice();

  // 1. VADD 合成写失败 ⇒ 返回错误帧
  let vadd_args: Vec<&[u8]> = vec![b"vs_fail", b"FP32", b"\0\0\0\0\0\0\0\0", b"e1"];
  let reply = session.network_vadd(prefix, &vadd_args, 0, false).await;
  assert!(
    matches!(reply, VectorReply::Error(_)),
    "AOF 入队失败必须使 VADD 返回错误帧"
  );

  // 2. 预先建立好索引并插入元素（直接调用底座，绕过 broken sink）
  let (index, _guard) = mgr
    .read_or_create_vector_index(prefix, b"vs_pre", Some(&valid_index_config()))
    .await
    .unwrap();
  let index_bytes = index.to_bytes();
  let add_args = VectorAddArgs {
    element: b"e1",
    value_type: VectorValueType::FP32,
    values: &[0u8; 8],
    attributes: b"old_attr",
    reduce_dims: 0,
    quant_type: VectorQuantType::NoQuant,
    num_links: 8,
    distance_metric: VectorDistanceMetricType::L2,
  };
  assert_eq!(
    mgr
      .try_add(prefix, b"vs_pre", &index_bytes, &add_args)
      .await
      .unwrap(),
    VectorManagerResult::OK
  );

  // 3. VSETATTR 合成写失败 ⇒ 返回错误帧
  let vsetattr_args: Vec<&[u8]> = vec![b"vs_pre", b"e1", b"new_attr"];
  let reply = session
    .network_vsetattr(prefix, &vsetattr_args, false)
    .await;
  assert!(
    matches!(reply, VectorReply::Error(_)),
    "AOF 入队失败必须使 VSETATTR 返回错误帧"
  );

  // 4. VREM 合成写失败 ⇒ 返回错误帧
  let vrem_args: Vec<&[u8]> = vec![b"vs_pre", b"e1"];
  let reply = session.network_vrem(prefix, &vrem_args).await;
  assert!(
    matches!(reply, VectorReply::Error(_)),
    "AOF 入队失败必须使 VREM 返回错误帧"
  );
}
