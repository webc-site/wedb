//! 向量集中断删除恢复测试（对标 garnet/test/Garnet.test.vectorset/VectorSetTests.cs
//! 的 InterruptedVectorSetDelete* 族与 RenamesThenRecoverFromAOFAsync 语义）：
//!
//! 1. 标记删除后中断：RequestDeletion 已置 cleaning_up、清理迭代未跑，
//!    AOF 重放（VADD 重建）不得复活/复用旧上下文；
//! 2. 清理阶段中断：cleaning_up 标记后清理迭代恢复（process_cleanup），
//!    上下文必须归还空闲池；
//! 3. 重命名窗口语义：SUPPRESS_CLEANUP 置位期间键删除照常、清理被抑制
//!    （集合存活），标志清除后删除照常触发清理（C# MarkSuppressCleanup /
//!    RequestDeletion 忽略分支）；
//! 4. RESP DEL 端到端 + AOF 重放顺序语义：删除标记后重建落新上下文，
//!    旧上下文不复活（盘上残留交由清理迭代回收）。

use std::{mem::forget, sync::Arc};

use compio::runtime::Runtime;
use wbase::hash_slot::slot_of;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  aof::replay_input::ReplayInput,
  resp::{
    garnet_api::StoreGarnetApi,
    resp_server_session::RespServerSessionOptions,
    vector::{
      vector_manager::{VADD_APPEND_LOG_ARG, VectorManager, VectorManagerOptions},
      vector_manager_index::{INDEX_SIZE, Index},
      vector_store_callbacks::WedbVectorStoreCallbacks,
    },
  },
};
use wresp::command::RespCommand;

/// 默认会话库槽（测试键不参与定槽，统一取 (0,0) 库槽位）
const SLOT0: u16 = slot_of(0, 0);
use wval::SessionPrefixBuf;
use wvector::{
  Callbacks, VectorDistanceMetricType, VectorQuantType, VectorSetFlags, VectorValueType,
};

/// 测试存储（小预算，GC 关闭；目录驻留至进程退出供盘上记录断言）
fn test_store(tag: &str) -> Arc<WedbStore<SegmentedDevice>> {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device)).unwrap());
  forget(dir);
  store
}

/// 绑存储会话的向量管理器（生产装配同形态）
fn vector_manager(store: &Arc<WedbStore<SegmentedDevice>>) -> Arc<VectorManager> {
  let session = Arc::new(store.new_session().unwrap());
  Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new(session))),
  ))
}

/// VADD 重放入参（10 参布局，对齐 C# VectorSetAdd parseState 形态）
fn vadd_replay_input(values: &[u8], element: &[u8], attributes: &[u8]) -> ReplayInput {
  let le4 = |v: u32| v.to_le_bytes().to_vec();
  ReplayInput {
    cmd: RespCommand::Vadd,
    flags: 0,
    sub_id: 0,
    obj_type: 0,
    arg1: VADD_APPEND_LOG_ARG,
    arg2: 0,
    arg3: 0,
    args: vec![
      le4(2),                                   // dims
      le4(0),                                   // reduceDims
      le4(VectorValueType::FP32 as u32),        // valueType
      values.to_vec(),                          // values
      element.to_vec(),                         // element
      le4(VectorQuantType::NoQuant as u32),     // quantizer
      le4(200),                                 // buildExplorationFactor
      attributes.to_vec(),                      // attributes
      le4(8),                                   // numLinks
      le4(VectorDistanceMetricType::L2 as u32), // distanceMetric
    ],
  }
}

/// 索引记录上下文读取
fn context_of(index_value: &[u8; INDEX_SIZE]) -> u64 {
  Index::from_bytes(index_value).unwrap().context
}

/// 写会话消费者（直挂向量命令面）
fn consumer_of(
  store: &Arc<WedbStore<SegmentedDevice>>,
  vm: &Arc<VectorManager>,
) -> RespSessionConsumer {
  let api = StoreGarnetApi::new(store.new_session().unwrap()).with_vector_manager(Arc::clone(vm));
  RespSessionConsumer::new(1, RespServerSessionOptions::default(), Arc::new(api))
}

/// 泵等价消费（直填会话接收缓冲 → 唯一入口 → 应答取出）
fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(remaining, Some(0), "帧应完整消费");
  resp
}

/// RESP 数组帧
fn frame(parts: &[&[u8]]) -> Vec<u8> {
  let mut out = format!("*{}\r\n", parts.len()).into_bytes();
  for p in parts {
    out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
    out.extend_from_slice(p);
    out.extend_from_slice(b"\r\n");
  }
  out
}

/// 上下文元数据视图（decompose 后的块位）
fn meta_bit(context: u64) -> (usize, u16) {
  VectorManager::<WedbVectorStoreCallbacks<SegmentedDevice>>::decompose_context(context)
}

/// cleaning_up 位是否置位
fn is_cleaning_up(vm: &VectorManager, context: u64) -> bool {
  let (ci, cv) = meta_bit(context);
  vm.context_metadatas
    .lock()
    .get(ci)
    .unwrap()
    .is_cleaning_up(ci != 0, cv)
}

/// VADD 两元素（经 RESP 命令面，走注册表 + 内存图全链）
fn vadd_two(consumer: &mut RespSessionConsumer, key: &[u8]) {
  let values: [u8; 8] = [0, 0, 128, 63, 0, 0, 0, 64]; // f32 [1.0, 2.0]
  for name in ["el_a", "el_b"] {
    let out = pump(
      consumer,
      &frame(&[b"VADD", key, b"FP32", &values, name.as_bytes(), b"NOQUANT"]),
    );
    assert_eq!(out, b":1\r\n", "VADD {name} 应成功");
  }
}

/// 用例 1（InterruptedVectorSetDelete* 语义）：标记删除后中断 —— cleaning_up
/// 已标记、清理迭代未跑时，AOF 重放重建的集合不得复用/复活旧上下文；
/// 清理迭代恢复后旧上下文归还
#[test]
fn interrupted_mark_delete_then_aof_replay_no_resurrect() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let store = test_store("vs_interrupt1.db");
    let vm = vector_manager(&store);
    let mut consumer = consumer_of(&store, &vm);

    vadd_two(&mut consumer, b"vs_k1");
    let old_ctx = context_of(
      &vm
        .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vs_k1")
        .unwrap(),
    );
    assert_ne!(old_ctx, 0);

    // RESP DEL：登记表摘除
    assert_eq!(pump(&mut consumer, &frame(&[b"DEL", b"vs_k1"])), b":1\r\n");
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vs_k1")
        .is_none(),
      "删除后键应消失"
    );

    // 标记删除（request-cleanup 处理），随后清理迭代中断（不跑 process_cleanup）
    vm.process_request_cleanup(old_ctx);
    assert!(is_cleaning_up(&vm, old_ctx), "旧上下文应处于清理中");

    // 中断窗口内 AOF 重放（VADD 重建）：新集合必须落在全新上下文
    vm.replay_vector_set_add(
      SessionPrefixBuf::ROOT.as_slice(),
      b"vs_k1",
      SLOT0,
      &vadd_replay_input(&[0, 0, 128, 63, 0, 0, 0, 64], b"el_a", b""),
    )
    .unwrap();
    let new_index = vm
      .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vs_k1")
      .unwrap();
    let new_ctx = context_of(&new_index);
    assert_ne!(new_ctx, old_ctx, "重建集合不得复用清理中上下文");

    // 旧上下文元素已随索引丢弃清空（不复活）；新集合元素在位
    assert_eq!(vm.service.card(old_ctx), 0, "旧上下文不得复活元素");
    assert_eq!(vm.service.card(new_ctx), 1, "重放集合应存活");

    // 清理迭代恢复：旧上下文归还空闲池
    vm.process_cleanup(old_ctx);
    assert!(!is_cleaning_up(&vm, old_ctx), "清理完成后位应清零");
    let (ci, cv) = meta_bit(old_ctx);
    let meta = vm.context_metadatas.lock().get(ci).copied().unwrap();
    assert!(!meta.is_in_use(ci != 0, cv), "归还后 in_use 应清零");

    aok::OK
  })
  .unwrap();
}

/// 用例 2（InterruptedVectorSetDelete* 清理段）：清理阶段中断后恢复 ——
/// cleaning_up 标记即可终结归还，get_need_cleanup 随终结清空
#[test]
fn interrupted_cleanup_phase_context_returned() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let store = test_store("vs_interrupt2.db");
    let vm = vector_manager(&store);
    let mut consumer = consumer_of(&store, &vm);

    vadd_two(&mut consumer, b"vs_k2");
    let old_ctx = context_of(
      &vm
        .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vs_k2")
        .unwrap(),
    );

    pump(&mut consumer, &frame(&[b"DEL", b"vs_k2"]));
    vm.process_request_cleanup(old_ctx);
    assert!(is_cleaning_up(&vm, old_ctx));

    // 清理中上下文不可被新分配拿走（next_vector_set_context 跳过 cleaning_up）
    let fresh = vm.next_vector_set_context(0).unwrap();
    assert_ne!(fresh, old_ctx, "新分配不得复用清理中上下文");

    // 清理迭代恢复
    vm.process_cleanup(old_ctx);
    assert!(!is_cleaning_up(&vm, old_ctx));
    assert!(
      vm.context_metadatas
        .lock()
        .iter()
        .all(|m| m.get_need_cleanup().is_none()),
      "全部上下文的待清理标记应终结"
    );

    aok::OK
  })
  .unwrap();
}

/// 用例 3（RenamesThenRecoverFromAOFAsync 的 SUPPRESS 语义核心）：重命名
/// 窗口内置位 SUPPRESS_CLEANUP —— 键删除照常摘除、清理被抑制（元素存活）；
/// 标志清除后删除照常触发清理
#[test]
fn suppress_cleanup_delete_ignored_during_rename_window() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let store = test_store("vs_interrupt3.db");
    let vm = vector_manager(&store);
    let mut consumer = consumer_of(&store, &vm);

    vadd_two(&mut consumer, b"vs_k3");
    let mut index_value = vm
      .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vs_k3")
      .unwrap();
    let ctx = context_of(&index_value);

    // 重命名窗口：SUPPRESS_CLEANUP 置位写回登记表（C# MarkSuppressCleanup）
    let mut index = Index::from_bytes(&index_value).unwrap();
    index.flags = index.flags.union(VectorSetFlags::SUPPRESS_CLEANUP);
    index_value = index.to_bytes();
    vm.write_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vs_k3", &index_value);

    // 窗口内删除：键照常摘除（DEL 语义），但清理被抑制 —— 上下文不标记、
    // 内存索引不丢弃、元素存活（C# RequestDeletion 的忽略分支）
    assert!(
      vm.delete_vector_set(SessionPrefixBuf::ROOT.as_slice(), b"vs_k3"),
      "SUPPRESS_CLEANUP 置位期间键删除仍应成功"
    );
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vs_k3")
        .is_none(),
      "键应已摘除"
    );
    assert!(!is_cleaning_up(&vm, ctx), "窗口内清理应被抑制");
    assert_eq!(vm.service.card(ctx), 2, "窗口内元素应存活（清理被抑制）");

    // 窗口关闭（存活记录标志清除，C# ClearSuppressCleanup）：删除照常触发清理
    let mut index = Index::from_bytes(&index_value).unwrap();
    index.flags =
      VectorSetFlags::from_bits(index.flags.bits() & !VectorSetFlags::SUPPRESS_CLEANUP.bits());
    vm.write_stored_index(
      SessionPrefixBuf::ROOT.as_slice(),
      b"vs_k3",
      &index.to_bytes(),
    );
    assert!(
      vm.delete_vector_set(SessionPrefixBuf::ROOT.as_slice(), b"vs_k3"),
      "标志清除后删除应触发清理"
    );
    vm.process_request_cleanup(ctx);
    assert!(is_cleaning_up(&vm, ctx), "窗口外删除应标记清理");
    assert_eq!(vm.service.card(ctx), 0, "清理后内存索引应丢弃");

    aok::OK
  })
  .unwrap();
}

/// 用例 4（AOF 重放顺序语义）：重放重建集合存活；删除标记后（清理迭代
/// 中断、盘上扫描未跑）同键重放落新上下文，旧上下文不复活
#[test]
fn replay_rebuild_then_delete_stays_deleted() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let store = test_store("vs_interrupt4.db");
    let vm = vector_manager(&store);
    let mut consumer = consumer_of(&store, &vm);

    // 重放重建（AOF 恢复路径）：集合存活
    vm.replay_vector_set_add(
      SessionPrefixBuf::ROOT.as_slice(),
      b"vs_k4",
      SLOT0,
      &vadd_replay_input(&[0, 0, 128, 63, 0, 0, 0, 64], b"el_x", b"attr"),
    )
    .unwrap();
    vm.replay_vector_set_add(
      SessionPrefixBuf::ROOT.as_slice(),
      b"vs_k4",
      SLOT0,
      &vadd_replay_input(&[0, 0, 0, 64, 0, 0, 128, 63], b"el_y", b""),
    )
    .unwrap();
    let ctx = context_of(
      &vm
        .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vs_k4")
        .unwrap(),
    );
    assert_eq!(vm.service.card(ctx), 2);
    assert!(
      vm.service.get_attribute(ctx, b"el_x").as_deref() == Some(b"attr".as_slice()),
      "重放属性应恢复"
    );

    // RESP DEL + 标记删除（清理迭代中断：盘上元素扫描未跑）
    assert_eq!(pump(&mut consumer, &frame(&[b"DEL", b"vs_k4"])), b":1\r\n");
    vm.process_request_cleanup(ctx);
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vs_k4")
        .is_none()
    );
    assert!(is_cleaning_up(&vm, ctx), "删除后应标记清理");

    // 已删集合的同键重放 = 新生命周期的开始：分配器跳过清理中块，落新
    // 上下文，旧上下文不复活（盘上残留交由清理迭代回收）
    vm.replay_vector_set_add(
      SessionPrefixBuf::ROOT.as_slice(),
      b"vs_k4",
      SLOT0,
      &vadd_replay_input(&[0, 0, 128, 63, 0, 0, 0, 64], b"el_x", b""),
    )
    .unwrap();
    let reborn = context_of(
      &vm
        .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vs_k4")
        .unwrap(),
    );
    assert_ne!(reborn, ctx, "重建集合不得复用清理中上下文");
    assert_eq!(vm.service.card(reborn), 1, "新生命周期元素应存活");
    assert!(
      is_cleaning_up(&vm, ctx),
      "旧上下文保持中断态（盘上残留待清理迭代回收）"
    );

    aok::OK
  })
  .unwrap();
}
