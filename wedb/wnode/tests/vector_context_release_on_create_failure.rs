//! create_index 失败臂上下文回收回归（票：zcode-r27-vectordiskann 发现六）
//!
//! 缺陷形态：create_index_locked 先 next_vector_set_context 占位写透后调
//! create_index，rust create_index 为可失败函数（WedbProvider::new 的 fsm
//! 建块写、量化状态写、config 校验均可错），原失败臂仅返回 Invalid 无回收
//! 路径——存储抖动期每次失败的 VSET 首插各漏一个 8 宽上下文槽位，64 槽位
//! 块逐个损耗，最终整库 Vector Set 创建失败（单调不可逆泄漏）。
//!
//! 修复契约：失败臂取回刚占位的槽位（ContextMetadata::release：in_use 清零
//! 、slot 清零与 version 递增）并写透，槽位立即可复用；C# 无对位（原生
//! CreateIndex 以 Debug.Assert 声明不可失败）。
//!
//! 注入面：桥在 Metadata 域 write 落盘口按 armed 注入失败——VADD 默认 Q8
//! 集合首建时 WedbProvider::new 的量化状态写（_qnt）恰落该域，失败沿
//! create_index → create_index_locked 传导。

use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering},
};

use wbase::hash_slot::slot_of;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::resp::vector::{
  resp_server_session_vectors::{RespServerSessionVectors, VectorReply},
  vector_manager::{CONTEXT_STEP, VectorManager, VectorManagerOptions},
  vector_manager_index::Index,
  vector_store_callbacks::OwnedActiveVectorSession,
};
use wval::SessionPrefixBuf;
use wvector::{Callbacks, StoreCallbacks, store::Term};
use wvector_test::MemStore;

const SLOT0: u16 = slot_of(0, 0);
fn root() -> SessionPrefixBuf {
  SessionPrefixBuf::ROOT
}
/// 解除注入哨兵。
const DISARMED: u64 = u64::MAX;

/// Metadata 域写故障内存存储（基座 [`MemStore`] 转发 + write 故障注入臂）。
struct MetadataWriteFault {
  base: MemStore,
  fail_term: AtomicU64,
}

impl MetadataWriteFault {
  fn new() -> Self {
    Self {
      base: MemStore::new(),
      fail_term: AtomicU64::new(DISARMED),
    }
  }

  fn arm(&self) {
    self
      .fail_term
      .store(Term::Metadata as u64, Ordering::Release);
  }

  fn disarm(&self) {
    self.fail_term.store(DISARMED, Ordering::Release);
  }
}

impl StoreCallbacks for MetadataWriteFault {
  /// 故障注入臂：armed 项类型域的写落盘口截失败，其余原样转基座。
  async fn write(&self, context: u64, key: &[u8], value: &[u8]) -> bool {
    let fail = self.fail_term.load(Ordering::Acquire);
    if fail != DISARMED && context & 0b111 == fail {
      return false;
    }
    self.base.write(context, key, value).await
  }

  async fn read_multi<F>(&self, context: u64, keys: &[u8], length_hint: usize, f: F) -> bool
  where
    F: FnMut(u32, &[u8]) + Send,
  {
    self.base.read_multi(context, keys, length_hint, f).await
  }

  async fn read<F>(&self, context: u64, key: &[u8], f: F) -> bool
  where
    F: FnMut(&[u8]) + Send,
  {
    self.base.read(context, key, f).await
  }

  async fn delete(&self, context: u64, key: &[u8]) -> bool {
    self.base.delete(context, key).await
  }

  async fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, f: F) -> bool
  where
    F: FnMut(&mut [u8]) + Send,
  {
    self.base.rmw(context, key, write_len, f).await
  }

  async fn filter(&self, context: u64, internal_id: u32) -> bool {
    self.base.filter(context, internal_id).await
  }

  async fn purge_context(&self, context: u64) -> bool {
    self.base.purge_context(context).await
  }

  fn log(&self, context: u64, msg: &str) {
    self.base.log(context, msg);
  }
}

/// 执行域绑定装配（VADD 命令臂须已绑定向量存储会话，形态同
/// resp_vector_set::bound_domain）。
fn bound_domain() -> (
  tempfile::TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  OwnedActiveVectorSession<SegmentedDevice>,
) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("bind.db")).unwrap());
  let store = Arc::new(WedbStore::open(wtest_base::test_store_config(), device).unwrap());
  let bound = OwnedActiveVectorSession::new(store.new_session().unwrap());
  (dir, store, bound)
}

/// 主靶：首插失败（量化状态写被注入拦截）回收刚占位的上下文，重试复用
/// 同一槽位——修复前每次失败各烧一个 8 宽槽位（重试将分配下一槽位）。
#[compio::test]
async fn create_failure_releases_context_for_reuse() {
  let (_dir, _store, _bound) = bound_domain();
  let store = Arc::new(MetadataWriteFault::new());
  let manager = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::clone(&store)),
  ));
  let sess = RespServerSessionVectors::new(Arc::clone(&manager));

  // 注入 Metadata 域写故障：VADD 首建在量化状态写处失败
  store.arm();
  let r = sess
    .network_vadd(
      root().as_slice(),
      &[b"k1", b"VALUES", b"2", b"1.0", b"2.0", b"elem1"],
      SLOT0,
      false,
    )
    .await;
  assert!(
    matches!(r, VectorReply::Error(_)),
    "创建失败应答错误帧: {r:?}"
  );
  // 失败不落登记桩
  assert!(
    manager
      .read_stored_index(root().as_slice(), b"k1")
      .is_none()
  );

  // 解除故障重试：回收的槽位应被复用（修复前重试会分配下一个槽位）
  store.disarm();
  let r = sess
    .network_vadd(
      root().as_slice(),
      &[b"k1", b"VALUES", b"2", b"1.0", b"2.0", b"elem1"],
      SLOT0,
      false,
    )
    .await;
  assert_eq!(r, VectorReply::Integer(1));
  let index = manager
    .read_stored_index(root().as_slice(), b"k1")
    .map(|bytes| Index::from_bytes(bytes.as_slice()).unwrap())
    .unwrap();
  assert_eq!(
    index.context, CONTEXT_STEP,
    "重试应复用失败臂回收的首个上下文槽位"
  );

  // 后续集合分配顺延下一槽位，位图无洞
  let r = sess
    .network_vadd(
      root().as_slice(),
      &[b"k2", b"VALUES", b"2", b"3.0", b"4.0", b"elem2"],
      SLOT0,
      false,
    )
    .await;
  assert_eq!(r, VectorReply::Integer(1));
  let index2 = manager
    .read_stored_index(root().as_slice(), b"k2")
    .map(|bytes| Index::from_bytes(bytes.as_slice()).unwrap())
    .unwrap();
  assert_eq!(index2.context, CONTEXT_STEP * 2);
}
