#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 票 wnode-vector-registry-recovery-put-fail-half-recovery-context-leak 回归：
//! 恢复回建 Index 臂先落 recovered_indexes 标记后 put_stored_index，写透失败
//! 旧形态仅 log::error 照常 Some——半截恢复放行启动。危害链：内存登记已被
//! put_stored_index 失败臂回滚而 recovered_indexes 标记滞留 → reconcile sweep
//! 判据（!recovered_indexes.contains_key）视该 context 已恢复免清理 → in_use
//! 位永滞、向量集整体隐身、原生索引与盘上元素行孤儿。
//!
//! 修复形态（审核裁定）：put_stored_index 失败即归 None 整轮拒启（fail-closed），
//! 复用调用方 recover_vector_sets 既有 None 判 Err 拒启通道——不取「撤销标记
//! 交 reconcile 清理」备选（会把可由盘上记录再回建的活上下文交清理协程物理
//! 丢弃，制造本运行期清丢、重启复活的运行期两态分叉）。
//!
//! 注入面（真故障，无 mock）：RegistryPersistence::put 故障桩 armed 时返
//! false（vector_migration_import_index_fail_drop.rs 同款机制），盘上种子
//! 登记记录经生产同款物理键编码单点直写 wkv 存储域。

use std::{
  future::ready,
  pin::Pin,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use wdev::SegmentedDevice;
use wkv::{CollectionError, Error, WedbStore};
use wnode::{
  database::{GarnetDatabase, SingleDatabaseManager},
  resp::vector::{
    vector_manager::{CONTEXT_STEP, VectorManager, VectorManagerOptions},
    vector_manager_index::Index,
    vector_manager_locking::registry_key,
    vector_registry_recovery::{RegistryPersistence, index_registry_physical_key},
    vector_store_callbacks::WedbVectorStoreCallbacks,
  },
};
use wtest_base::test_store_config;
use wval::SessionPrefixBuf;
use wvector::{Callbacks, VectorDistanceMetricType, VectorQuantType, VectorSetFlags};

/// 登记写透故障桩（armed 时 put 一律返 false——设备错的自然故障面）
struct FailPutPersistence {
  armed: AtomicBool,
}

impl RegistryPersistence for FailPutPersistence {
  fn put(
    &self,
    _physical_key: &[u8],
    _value: &[u8],
  ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
    let ok = !self.armed.load(Ordering::Acquire);
    Box::pin(ready(ok))
  }

  fn remove(&self, _physical_key: &[u8]) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
    Box::pin(ready(true))
  }
}

type SetupFixture = (
  tempfile::TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  Arc<SingleDatabaseManager<SegmentedDevice>>,
  Arc<FailPutPersistence>,
);

/// 装配：存储 + 单库管理器 + 向量管理器 + 故障桩登记写透（生产装配同形态，
/// vector_migration_import_index_fail_drop.rs 同款）
fn setup() -> SetupFixture {
  let dir = tempfile::tempdir().expect("tempdir");
  let device = Arc::new(
    SegmentedDevice::single_file(dir.path().join("reg_rebuild_put_fail.db")).expect("device"),
  );
  let store = Arc::new(WedbStore::open(test_store_config(), device.clone()).expect("store"));
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&store),
    device,
    dir.path().to_path_buf(),
    None,
  ));
  let mgr = Arc::new(SingleDatabaseManager::new(dir.path().to_path_buf(), db));
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new())),
  ));
  let fault = Arc::new(FailPutPersistence {
    armed: AtomicBool::new(false),
  });
  vm.attach_registry_store(fault.clone() as Arc<dyn RegistryPersistence>);
  mgr.attach_vector_manager(vm);
  (dir, store, mgr, fault)
}

/// 盘上索引记录形态（index_ptr=1 在位句柄；NoQuant 免量化建表通道）。
/// context 须为整块上下文（CONTEXT_STEP=8 的倍数，生产分配口唯一产出形态；
/// 子位值触发元数据位图 debug_assert，非盘上合法记录）
fn stored_index(context: u64) -> Index {
  Index {
    context,
    index_ptr: 1,
    dimensions: 4,
    reduce_dims: 0,
    num_links: 16,
    build_exploration_factor: 200,
    quant_type: VectorQuantType::NoQuant,
    distance_metric: VectorDistanceMetricType::L2,
    flags: VectorSetFlags::NONE,
  }
}

/// 旁路登记记录种子直写（生产写透 WedbRegistryPersistence 同一物理键编码单点）
async fn seed_index_record(
  store: &Arc<WedbStore<SegmentedDevice>>,
  user_key: &[u8],
  index: &Index,
) {
  let rk = registry_key(SessionPrefixBuf::ROOT.as_slice(), user_key);
  let physical = index_registry_physical_key(rk.as_slice());
  let session = store.new_session().expect("session");
  session
    .upsert_raw(physical.as_slice(), &index.to_bytes())
    .await
    .expect("种子登记记录直写");
}

/// 主回归（写透失败臂）：登记写透失败 → recover_vector_sets 返回 Err 拒启，
/// 不再产出 Some 半截计数放行启动；内存登记无半成品（put_stored_index 失败臂
/// 自回滚），拒启错误指明登记镜像写回失败
#[compio::test]
async fn put_write_through_failure_rejects_startup() {
  let (_dir, store, mgr, fault) = setup();
  seed_index_record(&store, b"reg_fail", &stored_index(CONTEXT_STEP)).await;

  // 注入：登记写透全量短路失败
  fault.armed.store(true, Ordering::Release);
  match mgr.recover_vector_sets().await {
    Ok(count) => panic!("写透失败必须拒启，不得以半截计数 {count} 放行启动"),
    Err(Error::Collection(CollectionError::Corrupted(msg))) => {
      assert!(
        msg.contains("登记镜像写回失败"),
        "拒启错误应指明登记镜像写回失败，实际: {msg}"
      );
    }
    Err(other) => panic!("应回集合损坏错误拒启，实际: {other:?}"),
  }

  // 半截恢复不残留：内存登记无条目
  let vm = mgr.try_vector_manager().expect("向量管理器应已挂载");
  let root = SessionPrefixBuf::ROOT.as_slice();
  assert!(
    vm.read_stored_index(root, b"reg_fail").is_none(),
    "写透失败不得残留半成品登记条目"
  );
}

/// 对照臂：put 成功路径恢复计数与登记表条目一致——计数即种子条数，各条目
/// 回建到位且先清指针（句柄属于写入进程，重启进程不得沿用盘上旧指针）
#[compio::test]
async fn put_success_recovers_count_matching_registry_entries() {
  let (_dir, store, mgr, _fault) = setup();
  seed_index_record(&store, b"set_a", &stored_index(CONTEXT_STEP)).await;
  seed_index_record(&store, b"set_b", &stored_index(CONTEXT_STEP * 2)).await;

  let recovered = mgr
    .recover_vector_sets()
    .await
    .expect("写透成功路径恢复应放行");
  assert_eq!(recovered, 2, "恢复计数须等于种子登记条数");

  let vm = mgr.try_vector_manager().expect("向量管理器应已挂载");
  let root = SessionPrefixBuf::ROOT.as_slice();
  for (user_key, context) in [
    (b"set_a".as_slice(), CONTEXT_STEP),
    (b"set_b".as_slice(), CONTEXT_STEP * 2),
  ] {
    let bytes = vm
      .read_stored_index(root, user_key)
      .unwrap_or_else(|| panic!("登记条目 {user_key:?} 须回建到位"));
    let rebuilt =
      Index::from_bytes(&bytes).unwrap_or_else(|| panic!("登记条目 {user_key:?} 须可解码"));
    assert_eq!(rebuilt.context, context, "context 须原位回建");
    assert_eq!(
      rebuilt.index_ptr, 0,
      "回建须先清指针（{user_key:?} 句柄属于写入进程）"
    );
  }
}
