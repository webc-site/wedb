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
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  GarnetAppendOnlyFile, GarnetLog, MessageConsumerFace, ReplayInput, RespSessionConsumer,
  resp::{
    garnet_api::StoreGarnetApi,
    resp_server_session::RespServerSessionOptions,
    vector::{
      vector_manager::{RECORD_TYPE, VectorManager, VectorManagerOptions},
      vector_manager_index::{INDEX_SIZE, Index},
      vector_manager_replication::VectorAofSink,
      vector_store_callbacks::{
        ActiveDedicatedVectorSession, OwnedActiveVectorSession, WedbVectorStoreCallbacks,
      },
    },
  },
};
use wresp::command::RespCommand;
use wtest_base::resp_frame as frame;
use wval::SessionPrefixBuf;
use wvector::Callbacks;

/// 内存 AOF（与 vector_set_rename.rs 同形态）
fn memory_aof() -> Arc<GarnetAppendOnlyFile> {
  let options = RuntimeServerOptions::default();
  Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(
      GarnetLog::new(
        &options,
        {
          let (_dirs, backends) = wnode_test::test_sublogs("vec_rename_aof_fail", 1);
          backends
        },
        None,
      )
      .expect("构造 GarnetLog"),
    ),
    &options,
    None,
  ))
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
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new())),
  ));
  let s = Arc::clone(&store);
  vm.attach_dedicated_session_factory(Arc::new(move || {
    s.new_session()
      .ok()
      .map(OwnedActiveVectorSession::new)
      .map(ActiveDedicatedVectorSession::from_bound)
  }));
  vm.set_aof_sink(Arc::new(VectorAofSink::new(
    &aof,
    Arc::clone(store.current_version_atomic()),
  )));
  aof.set_vector_manager(Arc::clone(&vm));
  (store, aof, vm)
}

/// 写会话消费者（直挂命令面，RENAME 经快路径登记表命中降级慢路径）
fn consumer_of(
  store: &Arc<WedbStore<SegmentedDevice>>,
  vm: &Arc<VectorManager>,
) -> RespSessionConsumer {
  let api = StoreGarnetApi::new(store.new_session().unwrap()).with_vector_manager(Arc::clone(vm));
  RespSessionConsumer::new(1, RespServerSessionOptions::default(), Arc::new(api))
}

/// 泵等价消费（vector_set_rename.rs 同款）
async fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(remaining, Some(0), "帧应完整消费");
  if let Some(slow) = consumer.take_slow_wait() {
    resp.extend_from_slice(&slow.resolve().await);
  }
  resp
}

/// VADD 两维 FP32 向量（经 RESP 命令面全链）
async fn vadd(consumer: &mut RespSessionConsumer, key: &[u8], element: &[u8]) {
  let values: [u8; 8] = [0, 0, 128, 63, 0, 0, 0, 64];
  let out = pump(
    consumer,
    &frame(&[
      b"VADD",
      key,
      b"FP32",
      values.as_slice(),
      element,
      b"NOQUANT",
    ]),
  )
  .await;
  assert_eq!(out, b":1\r\n", "VADD {key:?}/{element:?} 应成功");
}

/// 索引记录上下文读取
fn context_of(index_value: &[u8; INDEX_SIZE]) -> u64 {
  Index::from_bytes(index_value).unwrap().context
}

/// AOF 逻辑数据条目（commit 元数据帧按重放消费面同一单点判据滤除）
fn data_records(aof: &GarnetAppendOnlyFile) -> Vec<waof::WalRecord> {
  let mut records = Vec::new();
  aof.log().scan_single_with(0, 0, i64::MAX, |rec| {
    if !waof::is_commit_frame(&rec.payload) {
      records.push(rec.clone());
    }
    true
  });
  records
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
    let _vector_domain = vm
      .bind_dedicated_session()
      .expect("专用向量会话工厂应已注入");
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
    let out = pump(&mut consumer, &frame(&[b"RENAME", b"vs_fail", b"vs_gone"])).await;
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
      pump(&mut consumer, &frame(&[b"RENAME", b"vs_gone", b"vs_back"])).await,
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
