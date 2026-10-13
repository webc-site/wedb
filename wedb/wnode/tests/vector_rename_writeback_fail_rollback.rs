#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 票 wnode-vector-rename-new-key-writeback-fail-still-deletes-old 回归：
//! 向量集 RENAME 新键登记写透失败臂旧形态仅 log::error 后无条件删旧集——
//! 新键写失败仍摘旧键登记，集合对用户消失、原生索引成无登记孤儿
//!
//! 对标 C# 回滚契约（UnifiedStoreOps.cs RENAME 向量臂 SET(newKey) 非 OK 即
//! ClearSuppressCleanup(oldKey) 后 NOTFOUND，旧键原样保全可重试；
//! VectorManager.Index.cs:197-202 SetFlags(None) 形）。rust 修形：新键
//! put_stored_index 失败即以标记前快照原形重写旧键旁路记录（清
//! SUPPRESS_CLEANUP）、不摘旧集、返回 false，slow.rs 沿既有存储错误帧拒本
//! 命令。注入面复用 vector_error_swallow_regression.rs 同款 RegistryPersistence
//! 故障桩（按新键物理键过滤短路，非假 mock）。断言四件：
//! 1. 新键写透失败 → RENAME 回错误帧（禁 +OK 冒答），AOF 无 RENAME 条目；
//! 2. 旧集存活：登记在、SUPPRESS 位已清（恢复标记前原形）、上下文不变；
//! 3. 新键无半成品：登记表无残留条目；
//! 4. 解除注错后旧集数据完整可读可写，重试 RENAME 闭环 +OK 且迁移到位。

use std::{
  future::ready,
  mem::forget,
  pin::Pin,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
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
use wvector::VectorSetFlags;

/// 新键写透失败注入桩（vector_error_swallow_regression.rs FaultRegistryPersistence
/// 同款真故障面；armed 时仅目标用户键的登记写透短路失败——登记物理键编码
/// 尾段即用户键，其余物理键与摘除臂照常放行）
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

/// 装配生产端三元组（存储 + AOF 直推 + 绑定向量管理器 + 故障桩登记写透；
/// vector_rename_aof_fail.rs 同款生产装配形态）
fn setup(
  tag: &str,
) -> (
  Arc<WedbStore<SegmentedDevice>>,
  Arc<GarnetAppendOnlyFile>,
  Arc<VectorManager>,
  Arc<FailTargetKeyPersistence>,
) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device)).unwrap());
  forget(dir);
  let aof = memory_aof("vec_rename_writeback_fail");
  let vm = vector_manager_of(&store);
  let fault = Arc::new(FailTargetKeyPersistence {
    armed: AtomicBool::new(false),
    target: b"vs_new",
  });
  let fault_dyn: Arc<dyn RegistryPersistence> = fault.clone();
  vm.attach_registry_store(fault_dyn);
  wire_vector_aof(&store, &aof, &vm);
  (store, aof, vm, fault)
}

/// VCARD 经 RESP 命令面全链
async fn vcard(consumer: &mut RespSessionConsumer, key: &[u8]) -> Vec<u8> {
  pump_slow(consumer, &frame(&[b"VCARD", key])).await
}

/// 索引记录上下文读取
fn context_of(index_value: &[u8; INDEX_SIZE]) -> u64 {
  Index::from_bytes(index_value).unwrap().context
}

/// 解析记录的 ReplayInput 命令与参数
fn record_input(record: &waof::WalRecord) -> (RespCommand, Vec<Vec<u8>>, i64) {
  let header = AofHeader::TOTAL_SIZE;
  let key_len = u32::from_le_bytes(record.payload[header..header + 4].try_into().unwrap()) as usize;
  let input = ReplayInput::deserialize(&record.payload[header + 4 + key_len..])
    .expect("ReplayInput roundtrip");
  (input.cmd, input.args, input.arg1)
}

/// 主回归：新键写透失败 → 错误帧 + 旧集保全（SUPPRESS 已清）+ 新键无半成品
/// + AOF 无 RENAME 条目；解除注错后旧集可读可写、重试 RENAME 闭环迁移
#[test]
fn vector_rename_writeback_failure_rollback_keeps_old_set() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (store, aof, vm, fault) = setup("vs_rename_writeback_fail.db");
    // 本执行域绑定专用向量会话（直调回调臂经线程槽取会话）
    let _vector_domain = bind_vector_domain(&vm);
    let mut consumer = consumer_of(&store, &vm);
    let root = SessionPrefixBuf::ROOT.as_slice();

    vadd(&mut consumer, b"vs_old", b"e1").await;
    let src_ctx = context_of(&vm.read_stored_index(root, b"vs_old").unwrap());

    // 注入：armed 后仅 vs_new 的登记写透短路失败
    fault.armed.store(true, Ordering::Release);

    // 1. RENAME 回错误帧（旧形态新键写败仍删旧集并冒答 +OK 就此收口；
    // err_frame 臂 RESP_ERR_SLOW_PATH_STORAGE 恒 "-" 起头，"-" 即排除 +OK）
    let out = pump_slow(&mut consumer, &frame(&[b"RENAME", b"vs_old", b"vs_new"])).await;
    assert!(
      out.starts_with(b"-"),
      "新键写透失败必须拒本命令（错误帧而非 +OK 冒答），实际 {out:?}"
    );

    // 2. 旧集保全：登记在、SUPPRESS 位已清（恢复标记前原形）、上下文不变
    let old_index = vm
      .read_stored_index(root, b"vs_old")
      .expect("失败回滚后旧键登记必须存活");
    let old = Index::from_bytes(&old_index).unwrap();
    assert!(
      !old.flags.contains(VectorSetFlags::SUPPRESS_CLEANUP),
      "回滚须清 SUPPRESS_CLEANUP（对位 C# ClearSuppressCleanup），实际 {:?}",
      old.flags
    );
    assert_eq!(context_of(&old_index), src_ctx, "回滚后上下文不变");

    // 3. 新键无半成品：登记表无残留条目
    assert!(
      vm.read_stored_index(root, b"vs_new").is_none(),
      "新键写透失败不得残留半成品登记"
    );

    // 4. AOF 无 RENAME 条目（失败轮仅 VADD 一条目，主从不发散）
    let records = data_records(&aof);
    assert_eq!(records.len(), 1, "失败轮 AOF 应仅 VADD 一条目");
    let (cmd, ..) = record_input(&records[0]);
    assert_eq!(cmd, RespCommand::Vadd, "幸存条目应为 VADD");

    // 解除注错：旧集数据完整可读可写（VCARD 走通 = 登记与原生索引链在）
    fault.armed.store(false, Ordering::Release);
    assert_eq!(vcard(&mut consumer, b"vs_old").await, b":1\r\n");
    vadd(&mut consumer, b"vs_old", b"e2").await;

    // 重试 RENAME 闭环：+OK、旧名摘净、新名到位、数据完整迁移
    assert_eq!(
      pump_slow(&mut consumer, &frame(&[b"RENAME", b"vs_old", b"vs_new"])).await,
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
