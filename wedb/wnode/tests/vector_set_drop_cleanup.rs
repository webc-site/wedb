//! 向量集合 drop 清扫端到端测试（对标 libs/server/Resp/Vector/VectorManager.Cleanup.cs
//! 的 RunCleanupTaskAsync + PostDropCleanupFunctions 扫描删除链；测试语义对标
//! garnet/test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:DeleteVectorSet
//! —— 集合删除后其关联元素物理记录随清理迭代消亡）：
//!
//! 1. RESP DEL → request-cleanup（标记 + 索引丢弃）→ process_cleanup 物理清扫，
//!    目标上下文全部项类型子域记录不可扫描（零链首存活版）、不可点查；
//! 2. 清扫完成后上下文归还空闲池（cleaning_up / in_use 双清零）；
//! 3. 并存的其他集合记录不受波及（异上下文隔离）。

use std::{mem::forget, sync::Arc};

use aok::Void;
use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    garnet_api::StoreGarnetApi,
    resp_server_session::RespServerSessionOptions,
    vector::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_manager_index::{INDEX_SIZE, Index},
      vector_store_callbacks::{
        ActiveDedicatedVectorSession, OwnedActiveVectorSession, WedbVectorStoreCallbacks,
      },
    },
  },
};
use wtest_base::resp_frame as frame;
use wval::{KeyTag, NamespaceDbCodec, SessionPrefixBuf};
use wvector::Callbacks;

/// 上下文基址掩码（与 CONTEXT_STEP = 8 对齐的项类型子域位）
const TERM_MASK: u64 = 0b111;

/// 测试存储（小预算，GC 关闭）
fn test_store(tag: &str) -> Arc<WedbStore<SegmentedDevice>> {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device)).unwrap());
  forget(dir);
  store
}

/// 绑存储会话的向量管理器（生产装配同形态：回调无状态，会话按执行域绑定；
/// 后台/直调处理项经专用会话工厂自持会话）
fn vector_manager(store: &Arc<WedbStore<SegmentedDevice>>) -> Arc<VectorManager> {
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new())),
  ));
  let s = Arc::clone(store);
  vm.attach_dedicated_session_factory(Arc::new(move || {
    s.new_session()
      .ok()
      .map(OwnedActiveVectorSession::new)
      .map(ActiveDedicatedVectorSession::from_bound)
  }));
  vm
}

/// 写会话消费者（直挂向量命令面）
fn consumer_of(
  store: &Arc<WedbStore<SegmentedDevice>>,
  vm: &Arc<VectorManager>,
) -> RespSessionConsumer {
  let api = StoreGarnetApi::new(store.new_session().unwrap()).with_vector_manager(Arc::clone(vm));
  RespSessionConsumer::new(1, RespServerSessionOptions::default(), Arc::new(api))
}

/// 泵等价消费（直填会话接收缓冲 → 唯一入口 → 应答取出）；同步段挂起
/// SlowWait 的命令（VADD/VSETATTR 写族挂起化）随即泵等价驱动至完成，
/// 应答按流水线顺序并入（对标网络泵 take_slow_wait → resolve）
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

/// 索引记录上下文读取
fn context_of(index_value: &[u8; INDEX_SIZE]) -> u64 {
  Index::from_bytes(index_value).unwrap().context
}

/// 物理键是否为目标上下文基址的向量域记录
fn matches_ctx(key: &[u8], ctx: u64) -> bool {
  matches!(
    NamespaceDbCodec::decode_tagged_key(key),
    Ok((_, _, tag, payload))
      if tag == KeyTag::Vector
        && payload.len() >= 8
        && u64::from_be_bytes(payload[..8].try_into().unwrap()) & !TERM_MASK == ctx
  )
}

/// 全日志扫描收集目标上下文基址的链首存活记录物理键（不可扫描判据与
/// wkv tests/vector_cleanup.rs 同口径：墓碑/被取代旧版不算存活）
async fn live_keys(store: &Arc<WedbStore<SegmentedDevice>>, ctx: u64) -> Vec<Vec<u8>> {
  let index = store.index.load();
  let mut keys = Vec::new();
  store
    .hlog()
    .scan(store.begin_address(), store.tail_address(), |addr, rec| {
      if !rec.is_tombstone()
        && matches_ctx(rec.key(), ctx)
        && index.find_tag(rec.key()) == Some(addr)
      {
        keys.push(rec.key().to_vec());
      }
      Ok(true)
    })
    .await
    .unwrap();
  keys
}

/// VADD 两元素（经 RESP 命令面，走注册表 + 内存图全链）
async fn vadd_two(consumer: &mut RespSessionConsumer, key: &[u8]) {
  let values: [u8; 8] = [0, 0, 128, 63, 0, 0, 0, 64]; // f32 [1.0, 2.0]
  for name in ["el_a", "el_b"] {
    let out = pump(
      consumer,
      &frame(&[b"VADD", key, b"FP32", &values, name.as_bytes(), b"NOQUANT"]),
    )
    .await;
    assert_eq!(out, b":1\r\n", "VADD {name} 应成功");
  }
}

/// cleaning_up 位是否置位
fn is_cleaning_up(vm: &VectorManager, context: u64) -> bool {
  let (ci, cv) =
    VectorManager::<WedbVectorStoreCallbacks<SegmentedDevice>>::decompose_context(context);
  vm.context_metadatas
    .lock()
    .get(ci)
    .unwrap()
    .is_cleaning_up(ci != 0, cv)
}

/// 端到端：drop 向量集合 → 清扫 → 物理记录不可扫描/不可点查，上下文归还，
/// 并存集合不受波及
#[test]
fn drop_cleanup_physically_purges_element_records() -> Void {
  Runtime::new()?.block_on(async {
    let store = test_store("vs_drop_cleanup.db");
    let vm = vector_manager(&store);
    let mut consumer = consumer_of(&store, &vm);
    let probe = store.new_session().unwrap();

    // 目标集合 vs_a（两元素）+ 并存幸存集合 vs_b（一元素）
    vadd_two(&mut consumer, b"vs_a").await;
    let values: [u8; 8] = [0, 0, 128, 63, 0, 0, 0, 64];
    let out = pump(
      &mut consumer,
      &frame(&[b"VADD", b"vs_b", b"FP32", &values, b"el_x", b"NOQUANT"]),
    )
    .await;
    assert_eq!(out, b":1\r\n", "VADD vs_b 应成功");

    let ctx_a = context_of(
      &vm
        .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vs_a")
        .unwrap(),
    );
    let ctx_b = context_of(
      &vm
        .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vs_b")
        .unwrap(),
    );
    assert_ne!(ctx_a, ctx_b);

    // 前置：目标上下文元素记录已落盘且链首存活；点查可见
    let captured = live_keys(&store, ctx_a).await;
    assert!(
      captured.len() >= 2,
      "前置：两元素的向量域记录应可扫描，实际 {} 条",
      captured.len()
    );
    assert!(
      probe.read_raw(&captured[0]).await?.is_some(),
      "前置：物理记录应可点查"
    );
    let ctx_b_live = live_keys(&store, ctx_b).await;
    assert!(!ctx_b_live.is_empty(), "前置：幸存集合应有记录");

    // RESP DEL + 请求清理（标记 + 索引丢弃）+ 清理迭代（物理清扫 + 归还）
    assert_eq!(
      pump(&mut consumer, &frame(&[b"DEL", b"vs_a"])).await,
      b":1\r\n"
    );
    vm.process_request_cleanup(ctx_a).await;
    assert!(is_cleaning_up(&vm, ctx_a), "删除后应标记清理中");
    assert_eq!(vm.service.card(ctx_a), 0, "内存索引应已丢弃");

    vm.process_cleanup(ctx_a).await;

    // 不可扫描：目标上下文零链首存活版
    assert!(
      live_keys(&store, ctx_a).await.is_empty(),
      "清扫后不得残留可扫描存活记录"
    );
    // 不可点查：清扫前捕获的全部物理键点读皆空
    for key in &captured {
      assert!(
        probe.read_raw(key).await?.is_none(),
        "清扫后物理记录应不可点查: {key:?}"
      );
    }
    // 上下文归还空闲池
    assert!(!is_cleaning_up(&vm, ctx_a), "清理完成后位应清零");

    // 幸存集合不受波及（记录在位 + 元素可查）
    let ctx_b_after = live_keys(&store, ctx_b).await;
    assert_eq!(ctx_b_after.len(), ctx_b_live.len(), "幸存集合记录数不变");
    assert_eq!(vm.service.card(ctx_b), 1, "幸存集合元素应存活");

    aok::OK
  })
}
