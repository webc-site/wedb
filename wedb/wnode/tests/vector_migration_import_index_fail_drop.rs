//! 票 wnode-vector-import-migrated-index-fail-arm-missing-drop-index 回归：
//! import_migrated_index 在 create_index 成功（index_ptr=1）后，写透失败臂
//! 与复制失败臂裸 return Err 不回收刚建原生索引——服务侧索引滞留、context
//! in_use/migrating 位滞留直至重启 reconcile
//!
//! 对位 C# HandleMigratedIndexKey（VectorManager.Migration.cs:229-237）：
//! writeRes != OK 即 Service.DropIndex(context, newlyAllocatedIndex) 后
//! throw。rust 修形：两失败臂各经既有 drop_in_memory_index 单点丢弃刚建
//! 索引，Err 返回保持，位滞留交既有重启 reconcile 弃迁臂收敛（与 C#
//! throw 后位滞留同形）。
//!
//! 注入面（真故障，无 mock）：
//! 1. 写透臂——RegistryPersistence 故障桩按目标键短路失败
//!    （vector_error_swallow_regression.rs FaultRegistryPersistence 同款）；
//! 2. 复制臂——VectorAofSink 持 AOF Weak 引用，释放 AOF Arc 即
//!    upgrade 失败报 PipelineBroken（复制链断裂的自然故障面）。

use std::{
  future::ready,
  mem::forget,
  pin::Pin,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use wbase::hash_slot::slot_of;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  GarnetAppendOnlyFile,
  resp::vector::{
    vector_manager::{ERR_VECTOR_SERVICE_RESPONSE, VectorManager},
    vector_manager_index::Index,
    vector_manager_replication::VectorAofSink,
    vector_registry_recovery::RegistryPersistence,
  },
};
use wnode_test::{bind_vector_domain, vector_manager_of};
use wtest_base::test_store_config;
use wval::SessionPrefixBuf;
use wvector::{VectorDistanceMetricType, VectorQuantType, VectorSetFlags};
fn memory_aof() -> Arc<GarnetAppendOnlyFile> {
  wnode_test::memory_aof("vec_mig_import_fail")
}

/// 默认会话库槽（测试键不参与定槽，统一取 (0,0) 库槽位）
const SLOT0: u16 = slot_of(0, 0);

/// 目标键登记写透失败注入桩（armed 时仅目标用户键的登记写透短路失败——
/// 登记物理键编码尾段即用户键；其余物理键与摘除臂照常放行）
struct FailTargetKeyPersistence {
  armed: AtomicBool,
  target: &'static [u8],
}

impl RegistryPersistence for FailTargetKeyPersistence {
  fn put(
    &self,
    physical_key: &[u8],
    _value: &[u8],
  ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
    let ok = !(self.armed.load(Ordering::Acquire) && physical_key.ends_with(self.target));
    Box::pin(ready(ok))
  }

  fn remove(&self, _physical_key: &[u8]) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
    Box::pin(ready(true))
  }
}

/// 装配：存储 + AOF 直推 + 向量管理器 + 故障桩登记写透（生产装配同形态，
/// vector_rename_writeback_fail_rollback.rs 同款）
fn setup(
  target: &'static [u8],
) -> (
  Arc<WedbStore<SegmentedDevice>>,
  Arc<GarnetAppendOnlyFile>,
  Arc<VectorManager>,
  Arc<FailTargetKeyPersistence>,
) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("mig_fail.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  forget(dir);
  let aof = memory_aof();
  let vm = vector_manager_of(&store);
  let fault = Arc::new(FailTargetKeyPersistence {
    armed: AtomicBool::new(false),
    target,
  });
  vm.attach_registry_store(fault.clone() as Arc<dyn RegistryPersistence>);
  vm.set_aof_sink(Arc::new(VectorAofSink::new(
    &aof,
    Arc::clone(store.current_version_atomic()),
  )));
  (store, aof, vm, fault)
}

/// 迁移索引帧（index_ptr=0、上下文已预留形态；NoQuant 免量化建表通道）
fn migrated_index(context: u64) -> Index {
  Index {
    context,
    index_ptr: 0,
    dimensions: 4,
    reduce_dims: 0,
    num_links: 16,
    build_exploration_factor: 200,
    quant_type: VectorQuantType::NoQuant,
    distance_metric: VectorDistanceMetricType::L2,
    flags: VectorSetFlags::NONE,
  }
}

/// 主回归（写透失败臂）：登记写透失败 → Err 保持 + 刚建原生索引已回收 +
/// 登记无半成品；位滞留（in_use+migrating 未清）下解除注错同上下文重试闭环
/// ——重试后索引在位反证 drop 断言判别力
#[compio::test]
async fn write_through_failure_drops_created_index() {
  let (_store, _aof, vm, fault) = setup(b"mig_fail");
  let _vector_domain = bind_vector_domain(&vm);
  let root = SessionPrefixBuf::ROOT.as_slice();
  let reserved = vm
    .reserve_contexts_for_migration(1)
    .await
    .expect("迁移上下文预留");
  let context = reserved[0];
  let index = migrated_index(context);

  // 注入：armed 后 mig_fail 的登记写透短路失败
  fault.armed.store(true, Ordering::Release);
  let err = vm
    .import_migrated_index(root, b"mig_fail", &index.to_bytes(), SLOT0)
    .await
    .unwrap_err();
  assert_eq!(err, ERR_VECTOR_SERVICE_RESPONSE, "写透失败应回向量服务错误");
  // 刚建原生索引已回收（修复前滞留 Some(NoQuant)）
  assert!(
    vm.service.quant_of(context).is_none(),
    "写透失败臂必须经 drop_index 单点回收刚建的原生索引"
  );
  // 登记无半成品（put_stored_index 写透失败自回滚内存登记）
  assert!(
    vm.read_stored_index(root, b"mig_fail").is_none(),
    "写透失败不得残留半成品登记"
  );

  // 位滞留面：预留位未清（导入前置 in_use+migrating 仍成立），解除注错后
  // 同上下文重试闭环——重试成功即位滞留的直接观测
  fault.armed.store(false, Ordering::Release);
  vm.import_migrated_index(root, b"mig_fail", &index.to_bytes(), SLOT0)
    .await
    .expect("位滞留下解除注错重试应成功");
  assert_eq!(
    vm.service.quant_of(context),
    Some(VectorQuantType::NoQuant),
    "重试后原生索引应在位（反证上方 drop 断言判别力）"
  );
  let rebuilt = vm
    .read_stored_index(root, b"mig_fail")
    .expect("重试成功后登记应到位");
  assert_eq!(
    Index::from_bytes(&rebuilt).unwrap().index_ptr,
    1,
    "重试成功后 index_ptr 应置 1"
  );
}

/// 复制失败臂回归：AOF Arc 释放 → sink Weak 失效 → 复制臂自然失败 →
/// Err 保持 + 刚建原生索引已回收（登记已落在先，属既有写透成功行为）；
/// 位滞留可经 reconcile_recovered_state 收敛——migrating 位摘除后重试被
/// 上下文预留门拒绝（错误码异于向量服务错）
#[compio::test]
async fn replicate_failure_drops_created_index_and_reconcile_reclaims() {
  let (_store, aof, vm, _fault) = setup(b"mig_repl");
  let _vector_domain = bind_vector_domain(&vm);
  let root = SessionPrefixBuf::ROOT.as_slice();
  // 释放 AOF Arc：sink 内 Weak upgrade 失败 → 复制臂 PipelineBroken
  drop(aof);
  let reserved = vm
    .reserve_contexts_for_migration(1)
    .await
    .expect("迁移上下文预留");
  let context = reserved[0];
  let index = migrated_index(context);

  let err = vm
    .import_migrated_index(root, b"mig_repl", &index.to_bytes(), SLOT0)
    .await
    .unwrap_err();
  assert_eq!(err, ERR_VECTOR_SERVICE_RESPONSE, "复制失败应回向量服务错误");
  assert!(
    vm.service.quant_of(context).is_none(),
    "复制失败臂必须经 drop_index 单点回收刚建的原生索引"
  );
  // 登记已落在先（写透成功属既有行为；与位滞留一并交重启 reconcile 收敛）
  assert!(
    vm.read_stored_index(root, b"mig_repl").is_some(),
    "复制失败时登记写透已成功，登记应保持"
  );

  // 位滞留收敛：reconcile 弃迁臂摘 migrating 位后，重试被上下文预留门拒绝
  assert!(
    vm.reconcile_recovered_state(false).await,
    "reconcile 应收敛成功"
  );
  let err = vm
    .import_migrated_index(root, b"mig_repl", &index.to_bytes(), SLOT0)
    .await
    .unwrap_err();
  assert_ne!(
    err, ERR_VECTOR_SERVICE_RESPONSE,
    "reconcile 后重试应被上下文预留门拒绝（migrating 位已收敛），而非再走建索引链路"
  );
}
