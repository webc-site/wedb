//! 票 zcode-r167c-aoffail 案二回归：向量集 RENAME 合成 AOF 条目入队失败
//! 吞错冒泡（`replicate_vector_set_rename` 旧形态 `let _ =` 吞错冒答 +OK）
//!
//! 对标 C# 日志先行契约（GarnetLog.cs:Enqueue 故障沿调用栈上抛至 RESP 层
//! 报错；UnifiedStoreOps.cs:RENAME 向量臂复制失败即命令失败）与 rust 侧
//! error.rs `AofEnqueue` 契约原文「主存写入已生效，AOF 缺条目，调用方须以
//! 错误拒绝该命令防主从发散」及 promote.rs / migration.rs 既有冒泡臂单套
//! 机制。注入面复用 vector_error_swallow_regression.rs 同款真故障 sink
//! （Weak 悬空令 enqueue 恒 Err，非假 mock）。断言三件：
//! 1. 入队失败 → RENAME 回错误帧（禁 +OK 冒答令副本永缺 RENAME 条目）；
//! 2. 已生效不撤回：登记表迁移照常完成（「已生效 + 镜像缺失」形态）；
//! 3. AOF 确无 RENAME 条目；换回正常 sink 后重试整链闭环 +OK 且条目在账。

use std::{mem::forget, sync::Arc};

use compio::runtime::Runtime;
use waof::AofHeader;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  GarnetAppendOnlyFile, ReplayInput,
  resp::vector::{
    vector_manager::{RECORD_TYPE, VectorManager},
    vector_manager_index::{INDEX_SIZE, Index},
    vector_manager_replication::VectorAofSink,
  },
};
use wnode_test::{
  aof_data_records as data_records, bind_vector_domain, pump_slow, vadd_fp32 as vadd,
  vector_consumer_of as consumer_of, vector_manager_of, wire_vector_aof,
};
use wresp::command::RespCommand;
use wtest_base::resp_frame as frame;
use wval::SessionPrefixBuf;
fn memory_aof() -> Arc<GarnetAppendOnlyFile> {
  wnode_test::memory_aof("vec_rename_aof_fail")
}

/// 装配生产端三元组（存储 + AOF 直推 + 绑定向量管理器；vector_set_rename.rs
/// 同款生产装配形态）
fn setup(
  tag: &str,
) -> (
  Arc<WedbStore<SegmentedDevice>>,
  Arc<GarnetAppendOnlyFile>,
  Arc<VectorManager>,
) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device)).unwrap());
  forget(dir);
  let aof = memory_aof();
  let vm = vector_manager_of(&store);
  wire_vector_aof(&store, &aof, &vm);
  (store, aof, vm)
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

/// 案二主回归：RENAME 合成条目入队失败 → 错误帧 + 条目缺账 + 已生效不撤回；
/// 正常 sink 重试整链闭环 +OK 且 RENAME 条目在账
#[test]
fn vector_rename_aof_enqueue_failure_rejects_command() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (store, aof, vm) = setup("vs_rename_aof_fail.db");
    // 本执行域绑定专用向量会话（直调回调臂经线程槽取会话）
    let _vector_domain = bind_vector_domain(&vm);
    let mut consumer = consumer_of(&store, &vm);

    vadd(&mut consumer, b"vs_fail", b"foo").await;
    let src_ctx = context_of(
      &vm
        .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vs_fail")
        .unwrap(),
    );

    // 注入：换死 sink（Weak 悬空 → enqueue 恒 Err，vector_error_swallow_regression
    // 同款真故障面），RENAME 的 AOF 合成条目必败
    let dead_sink = {
      let dead_aof = memory_aof();
      Arc::new(VectorAofSink::new(
        &dead_aof,
        Arc::clone(store.current_version_atomic()),
      ))
    };
    vm.set_aof_sink(dead_sink);

    // 1. RENAME 回错误帧（旧吞错形态冒答 +OK 就此收口）
    let out = pump_slow(&mut consumer, &frame(&[b"RENAME", b"vs_fail", b"vs_gone"])).await;
    assert!(
      out.starts_with(b"-") && !out.starts_with(b"+OK"),
      "AOF 入队失败必须拒绝本命令，实际 {out:?}"
    );

    // 2. 已生效不撤回（error.rs AofEnqueue 契约「主存写入已生效」臂）：
    //    登记表迁移完成、旧名无幽灵、上下文不变
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vs_fail")
        .is_none(),
      "登记迁移已生效不撤回"
    );
    let dst_index = vm
      .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vs_gone")
      .expect("新名登记应已生效");
    assert_eq!(context_of(&dst_index), src_ctx, "迁移上下文不变");

    // 3. AOF 确无 RENAME 条目（仅 VADD 一条目；副本端不见半条命令）
    let records = data_records(&aof);
    assert_eq!(records.len(), 1, "入队失败轮 AOF 应仅 VADD 一条目");
    let (cmd, ..) = record_input(&records[0]);
    assert_eq!(cmd, RespCommand::Vadd, "幸存条目应为 VADD");

    // 换回正常 sink：对已迁移键重试 RENAME（vs_gone → vs_back）整链闭环
    vm.set_aof_sink(Arc::new(VectorAofSink::new(
      &aof,
      Arc::clone(store.current_version_atomic()),
    )));
    assert_eq!(
      pump_slow(&mut consumer, &frame(&[b"RENAME", b"vs_gone", b"vs_back"])).await,
      b"+OK\r\n",
      "解除注错后重试应闭环 +OK"
    );
    aof.log().commit();
    let records = data_records(&aof);
    assert_eq!(records.len(), 2, "VADD + 重试轮 RENAME 恰两条目");
    let (cmd, args, arg1) = record_input(&records[1]);
    assert_eq!(cmd, RespCommand::Rename, "重试轮 RENAME 条目必须在账");
    assert_eq!(arg1, i64::from(RECORD_TYPE), "arg1 = RecordType 哨兵");
    assert_eq!(args, [b"vs_gone".to_vec(), b"vs_back".to_vec()]);
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vs_back")
        .is_some(),
      "重试轮登记迁移闭环"
    );

    aok::OK
  })
  .unwrap();
}
