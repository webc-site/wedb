//! 票 wvector-put-stored-index-fail-arm-unregisters-registry-rollback-recovery-divergence
//! 回归：登记写透失败臂的撤除契约与回滚/恢复复原形分流
//!
//! 旧形态 put_stored_index 失败臂一律整条撤除内存登记：rename 开窗标记
//! 重写失败即撤旧键登记（单点失败即显形）；新键写败后的回滚原形重写再败
//! 也撤——旧集对用户消失、盘面 SUPPRESS 记录滞留至重启 reconcile，运行期
//! 客户端视图与盘面真值分叉无报痕（C# 主存写不失败，无此面对应物）。
//!
//! 修形：失败臂语义位单点收口（vector_manager_locking.rs put_stored_index_arm
//! 单套写透链）——常规写路径撤除契约不动；回滚/恢复类重写复原形
//! （开窗标记重写 + 回滚原形重写）失败恢复写前原值不撤旧登记；开窗写透
//! 失败即中止迁移（窗口是摘旧名臂的安全前提，续行会以无标志记录触发
//! request_deletion 误清新旧名共享的原生索引上下文）；新名注册显式剥除
//! 窗口标志（失败留存登记的重试快照携带 SUPPRESS_CLEANUP 时防其迁入新名）。
//! 注入面复用 vector_rename_writeback_fail_rollback.rs 同款 RegistryPersistence
//! 真故障桩（按物理键尾段用户键过滤短路，非假 mock）。断言六面：
//! 1. 开窗单点写透失败 → 错误帧、旧键登记保全（原形无窗口标志）、新键
//!    无半成品（迁移已中止）、AOF 无 RENAME 条目；
//! 2. 三重写透失败（开窗成 + 新键败 + 回滚重写再败）→ 错误帧、旧键登记
//!    保全（写前原值 = 开窗标记记录，内存与盘面一致）、上下文不变；
//! 3. 两场景解除注错后旧集原生索引存活可读可写，重试 RENAME 闭环 +OK；
//! 4. 重试成功后新名登记窗口标志未迁入（SUPPRESS_CLEANUP 已剥）；
//! 5. AOF 失败轮恒无 RENAME 条目、成功轮恰一入账（主从不发散）；
//! 6. 常规写路径失败撤除契约不回退：覆写存量登记写透失败仍整条撤除
//!    （Revert 语义，非复原形）。

use std::{
  future::ready,
  mem::forget,
  pin::Pin,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, Ordering},
  },
};

use compio::runtime::Runtime;
use waof::AofHeader;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  GarnetAppendOnlyFile, ReplayInput, RespSessionConsumer,
  resp::vector::{
    vector_manager::{RECORD_TYPE, VectorManager},
    vector_manager_index::{INDEX_SIZE, Index},
    vector_registry_recovery::RegistryPersistence,
  },
};
use wnode_test::{
  aof_data_records as data_records, bind_vector_domain, memory_aof, pump_slow, vadd_fp32 as vadd,
  vector_consumer_of as consumer_of, vector_manager_of, wire_vector_aof,
};
use wresp::command::RespCommand;
use wtest_base::resp_frame as frame;
use wval::SessionPrefixBuf;
use wvector::{VectorQuantType, VectorSetFlags};

/// 分键按序登记写透故障桩（FailTargetKeyPersistence 同款真故障面扩展）：
/// armed 时 `always_fail` 键恒短路失败、`fail_after_first` 键自第 2 次 put
/// 起短路失败（第 1 次放行）——rename 迁移内对旧键的登记写透恰为开窗
/// 标记第 1 次 + 回滚原形重写第 2 次，新键为独立物理键。登记物理键编码
/// 尾段即用户键；摘除臂照常放行。
struct RenameFaultPersistence {
  armed: AtomicBool,
  always_fail: Option<&'static [u8]>,
  fail_after_first: Option<&'static [u8]>,
  after_first_puts: AtomicU32,
}

impl RenameFaultPersistence {
  /// 构造未 armed 桩（规则预置，播种完成后由用例显式 arm）
  fn new(always_fail: Option<&'static [u8]>, fail_after_first: Option<&'static [u8]>) -> Self {
    Self {
      armed: AtomicBool::new(false),
      always_fail,
      fail_after_first,
      after_first_puts: AtomicU32::new(0),
    }
  }
}

impl RegistryPersistence for RenameFaultPersistence {
  fn put(
    &self,
    physical_key: &[u8],
    _value: &[u8],
  ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
    let ok = if !self.armed.load(Ordering::Acquire) {
      true
    } else if self.always_fail.is_some_and(|k| physical_key.ends_with(k)) {
      false
    } else if self
      .fail_after_first
      .is_some_and(|k| physical_key.ends_with(k))
    {
      // 放行第 1 次（开窗标记），自第 2 次（回滚原形重写）起失败
      //（fetch_add 返回自增前值：0 即本次为第 1 次 → 放行）
      self.after_first_puts.fetch_add(1, Ordering::AcqRel) == 0
    } else {
      true
    };
    Box::pin(ready(ok))
  }

  fn remove(&self, _physical_key: &[u8]) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
    Box::pin(ready(true))
  }
}

/// 装配生产端四元组（存储 + AOF 直推 + 绑定向量管理器 + 故障桩登记写透；
/// vector_rename_writeback_fail_rollback.rs 同款生产装配形态）
fn setup(
  tag: &str,
  always_fail: Option<&'static [u8]>,
  fail_after_first: Option<&'static [u8]>,
) -> (
  Arc<WedbStore<SegmentedDevice>>,
  Arc<GarnetAppendOnlyFile>,
  Arc<VectorManager>,
  Arc<RenameFaultPersistence>,
) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device)).unwrap());
  forget(dir);
  let aof = memory_aof(tag);
  let vm = vector_manager_of(&store);
  let fault = Arc::new(RenameFaultPersistence::new(always_fail, fail_after_first));
  let fault_dyn: Arc<dyn RegistryPersistence> = fault.clone();
  vm.attach_registry_store(fault_dyn);
  wire_vector_aof(&store, &aof, &vm);
  (store, aof, vm, fault)
}

/// VCARD 经 RESP 命令面全链
async fn vcard(consumer: &mut RespSessionConsumer, key: &[u8]) -> Vec<u8> {
  pump_slow(consumer, &frame(&[b"VCARD", key])).await
}

/// RENAME 经 RESP 命令面全链
async fn rename(consumer: &mut RespSessionConsumer, old_key: &[u8], new_key: &[u8]) -> Vec<u8> {
  pump_slow(consumer, &frame(&[b"RENAME", old_key, new_key])).await
}

/// 索引记录上下文读取
fn context_of(index_value: &[u8; INDEX_SIZE]) -> u64 {
  Index::from_bytes(index_value).unwrap().context
}

/// 索引记录窗口标志读取
fn suppress_clean(index_value: &[u8; INDEX_SIZE]) -> bool {
  Index::from_bytes(index_value)
    .unwrap()
    .flags
    .contains(VectorSetFlags::SUPPRESS_CLEANUP)
}

/// 解析记录的 ReplayInput 命令与参数
fn record_input(record: &waof::WalRecord) -> (RespCommand, Vec<Vec<u8>>, i64) {
  let header = AofHeader::TOTAL_SIZE;
  let key_len = u32::from_le_bytes(record.payload[header..header + 4].try_into().unwrap()) as usize;
  let input = ReplayInput::deserialize(&record.payload[header + 4 + key_len..])
    .expect("ReplayInput roundtrip");
  (input.cmd, input.args, input.arg1)
}

/// 主回归一：开窗标记写透单点失败 → 迁移中止（错误帧），旧键登记保全
///（复原形恢复写前原值，无窗口标志），新键无半成品；解除注错后重试闭环
#[test]
fn rename_marker_write_failure_aborts_and_keeps_old_set() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // 仅旧键写透恒失败：开窗标记必败；新键写透放行（若误续行即留下新键
    // 半成品，断言 3 就此收口）
    let (store, aof, vm, fault) = setup("vs_rename_marker_fail.db", Some(b"vs_old"), None);
    let _vector_domain = bind_vector_domain(&vm);
    let mut consumer = consumer_of(&store, &vm);
    let root = SessionPrefixBuf::ROOT.as_slice();

    vadd(&mut consumer, b"vs_old", b"e1").await;
    let src_ctx = context_of(&vm.read_stored_index(root, b"vs_old").unwrap());

    // 注入：armed 后仅旧键的登记写透短路失败（开窗标记必败）
    fault.armed.store(true, Ordering::Release);

    // 1. RENAME 回错误帧（旧形态开窗失败仅 log 续行，就此收口）
    let out = rename(&mut consumer, b"vs_old", b"vs_new").await;
    assert!(
      out.starts_with(b"-"),
      "开窗写透失败必须拒本命令（错误帧而非续行冒答），实际 {out:?}"
    );

    // 2. 旧键登记保全：登记在、原形无窗口标志（恢复写前原值）、上下文不变
    let old_index = vm
      .read_stored_index(root, b"vs_old")
      .expect("开窗写透失败后旧键登记必须存活（不得整条撤除）");
    assert!(
      !suppress_clean(&old_index),
      "复原形须恢复写前原值（无 SUPPRESS_CLEANUP），实际 {:?}",
      Index::from_bytes(&old_index).unwrap().flags
    );
    assert_eq!(context_of(&old_index), src_ctx, "旧键上下文不变");

    // 3. 新键无半成品：迁移已中止，新键登记从未入账
    assert!(
      vm.read_stored_index(root, b"vs_new").is_none(),
      "开窗失败即中止，新键登记不得存在"
    );

    // 4. AOF 无 RENAME 条目（失败轮仅 VADD 一条目，主从不发散）
    let records = data_records(&aof);
    assert_eq!(records.len(), 1, "失败轮 AOF 应仅 VADD 一条目");
    let (cmd, ..) = record_input(&records[0]);
    assert_eq!(cmd, RespCommand::Vadd, "幸存条目应为 VADD");

    // 解除注错：旧集原生索引存活可读可写（VCARD + VADD 走通 = 登记与
    // 原生索引链在，未受共享上下文误清）
    fault.armed.store(false, Ordering::Release);
    assert_eq!(vcard(&mut consumer, b"vs_old").await, b":1\r\n");
    vadd(&mut consumer, b"vs_old", b"e2").await;

    // 重试 RENAME 闭环：+OK、旧名摘净、新名到位、数据完整迁移、
    // 新名登记无窗口标志迁入
    assert_eq!(
      rename(&mut consumer, b"vs_old", b"vs_new").await,
      b"+OK\r\n",
      "解除注错后重试 RENAME 应闭环 +OK"
    );
    assert!(
      vm.read_stored_index(root, b"vs_old").is_none(),
      "重试成功后旧名登记应摘净"
    );
    let new_index = vm
      .read_stored_index(root, b"vs_new")
      .expect("重试成功后新名登记应到位");
    assert_eq!(context_of(&new_index), src_ctx, "迁移上下文不变");
    assert!(!suppress_clean(&new_index), "新名登记不得携带窗口标志迁入");
    assert_eq!(vcard(&mut consumer, b"vs_new").await, b":2\r\n");

    // AOF 在账：VADD e1 + VADD e2 + RENAME 恰三条目
    aof.log().commit();
    let records = data_records(&aof);
    assert_eq!(records.len(), 3, "VADD e1 + VADD e2 + RENAME 恰三条目");
    let (cmd, args, arg1) = record_input(&records[2]);
    assert_eq!(cmd, RespCommand::Rename, "重试轮 RENAME 条目必须在账");
    assert_eq!(arg1, i64::from(RECORD_TYPE), "arg1 = RecordType 哨兵");
    assert_eq!(args, [b"vs_old".to_vec(), b"vs_new".to_vec()]);
  });
}

/// 主回归二：三重写透失败（开窗成 + 新键败 + 回滚重写再败）→ 旧键登记
/// 保全（写前原值 = 开窗标记记录），运行期客户端视图与盘面真值不分叉；
/// 解除注错后重试闭环且窗口标志不迁入新名
#[test]
fn rename_triple_write_failure_restores_prior_registration() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // 新键写透恒失败 + 旧键自第 2 次写透（回滚原形重写）起失败：
    // 开窗标记（第 1 次）放行 → 新键败 → 回滚重写（第 2 次）败
    let (store, aof, vm, fault) =
      setup("vs_rename_triple_fail.db", Some(b"vs_new"), Some(b"vs_old"));
    let _vector_domain = bind_vector_domain(&vm);
    let mut consumer = consumer_of(&store, &vm);
    let root = SessionPrefixBuf::ROOT.as_slice();

    vadd(&mut consumer, b"vs_old", b"e1").await;
    let src_ctx = context_of(&vm.read_stored_index(root, b"vs_old").unwrap());

    // 注入：armed 后新键写透恒失败 + 旧键自第 2 次写透（回滚原形重写）起失败
    fault.armed.store(true, Ordering::Release);

    // 1. RENAME 回错误帧
    let out = rename(&mut consumer, b"vs_old", b"vs_new").await;
    assert!(
      out.starts_with(b"-"),
      "三重写透失败必须拒本命令（错误帧），实际 {out:?}"
    );

    // 2. 旧键登记保全：登记在、写前原值 = 开窗标记记录（SUPPRESS_CLEANUP
    //    在，与盘面一致）、上下文不变——旧集对用户不消失，不再滞留至重启
    let old_index = vm
      .read_stored_index(root, b"vs_old")
      .expect("回滚重写再败后旧键登记必须保全（不得整条撤除）");
    assert!(
      suppress_clean(&old_index),
      "复原形恢复写前原值 = 开窗标记记录（SUPPRESS_CLEANUP 在）"
    );
    assert_eq!(context_of(&old_index), src_ctx, "旧键上下文不变");

    // 3. 新键无半成品
    assert!(
      vm.read_stored_index(root, b"vs_new").is_none(),
      "新键写透失败不得残留半成品登记"
    );

    // 4. AOF 无 RENAME 条目
    let records = data_records(&aof);
    assert_eq!(records.len(), 1, "失败轮 AOF 应仅 VADD 一条目");
    let (cmd, ..) = record_input(&records[0]);
    assert_eq!(cmd, RespCommand::Vadd, "幸存条目应为 VADD");

    // 解除注错：旧集存活可读可写、原生索引在（VADD 走通）
    fault.armed.store(false, Ordering::Release);
    assert_eq!(vcard(&mut consumer, b"vs_old").await, b":1\r\n");
    vadd(&mut consumer, b"vs_old", b"e2").await;

    // 重试 RENAME 闭环：+OK、窗口标志不迁入新名、数据完整
    assert_eq!(
      rename(&mut consumer, b"vs_old", b"vs_new").await,
      b"+OK\r\n",
      "解除注错后重试 RENAME 应闭环 +OK"
    );
    assert!(vm.read_stored_index(root, b"vs_old").is_none());
    let new_index = vm
      .read_stored_index(root, b"vs_new")
      .expect("重试成功后新名登记应到位");
    assert_eq!(context_of(&new_index), src_ctx, "迁移上下文不变");
    assert!(
      !suppress_clean(&new_index),
      "失败留存登记的重试快照携带窗口标志，新名注册须剥除（不得迁入）"
    );
    assert_eq!(vcard(&mut consumer, b"vs_new").await, b":2\r\n");
  });
}

/// 主回归三：常规写路径失败撤除契约不回退——覆写存量登记写透失败仍整条
/// 撤除（Revert 语义，非复原形恢复原值）
#[test]
fn regular_write_failure_revert_contract_unchanged() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_store, _aof, vm, fault) = setup("vs_regular_revert.db", Some(b"dup"), None);
    let _vector_domain = bind_vector_domain(&vm);
    let root = SessionPrefixBuf::ROOT.as_slice();

    // 存量登记落桩（context 5, ptr 0）
    let stale = Index {
      context: 5,
      quant_type: VectorQuantType::NoQuant,
      ..Index::default()
    };
    assert!(vm.write_stored_index(root, b"dup", &stale.to_bytes()).await);

    // 注入：覆写写透失败（armed always_fail = dup）→ 整条撤除（Revert）：
    // 不得残留新值，也不得恢复原值——常规写路径失败契约与复原形分流
    fault.armed.store(true, Ordering::Release);
    assert!(
      !vm.write_stored_index(root, b"dup", &stale.to_bytes()).await,
      "注入写透失败必须返回 false"
    );
    assert!(
      vm.read_stored_index(root, b"dup").is_none(),
      "常规写路径失败撤除契约：覆写失败须整条撤除（不残留亦不复原）"
    );

    // 解除注错后写透恢复
    fault.armed.store(false, Ordering::Release);
    assert!(vm.write_stored_index(root, b"dup", &stale.to_bytes()).await);
    assert!(vm.read_stored_index(root, b"dup").is_some());
  });
}
