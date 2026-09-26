//! 向量域 AOF 直推链路集成测试
//!
//! 对标 C# VectorManager.Replication.cs（AOF: YES）与 AofProcessor.cs
//! StoreRMW 的 VADD/VREM/VSETATTR 分派，裁剪 garnet.test 的向量恢复场景：
//! 1. 生产端：VADD/VREM/VSETATTR 命令成功后合成 StoreRMW 条目入队 AOF
//!    （参数布局对齐 C# VectorSetAdd 的 parseState 10 参形态）；
//! 2. 重放端：全新 VectorManager 经 AofProcessor 重放重建索引，
//!    VEMB/VCARD/VGETATTR/VDIM 断言向量数据完整（--recover / 副本全量
//!    同步同路径，此前该链路对向量条目显式失败）。

use std::{fs::remove_dir_all, sync::Arc};

use waof::{AofEntryType, AofHeader, WalRecord};
use wbase::hash_slot::slot_of;
use wconf::RuntimeServerOptions;

/// 默认会话库槽（测试键不参与定槽，统一取 (0,0) 库槽位）
const SLOT0: u16 = slot_of(0, 0);

use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  GarnetAppendOnlyFile, GarnetLog, ReplayInput,
  aof::{
    aof_processor::{AofProcessor, ReplayTarget},
    garnet_log::RecordShape,
    recover::aof_recover::AofRecover,
  },
  resp::vector::{
    resp_server_session_vectors::{RespServerSessionVectors, VectorReply},
    vector_manager::{
      VADD_APPEND_LOG_ARG, VSETINDEX_APPEND_LOG_ARG, VectorManager, VectorManagerOptions,
    },
    vector_manager_index::Index,
    vector_manager_replication::VectorAofSink,
    vector_store_callbacks::{
      ActiveDedicatedVectorSession, OwnedActiveVectorSession, WedbVectorStoreCallbacks,
    },
  },
  storage::session::storage_session::StorageSession,
};
use wnode_test::replay_input_bytes;
use wresp::command::RespCommand;
use wtest_base::test_store_config;
use wval::{KeyTag, NamespaceDbCodec, SessionPrefixBuf};
use wvector::{Callbacks, VectorDistanceMetricType, VectorQuantType, VectorSetFlags};

/// 内存 AOF（无盘拓扑；重放面与磁盘拓扑同路径）
fn memory_aof() -> Arc<GarnetAppendOnlyFile> {
  let options = RuntimeServerOptions::default();
  Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(
      GarnetLog::new(
        &options,
        {
          let (_dirs, backends) = wnode_test::test_sublogs("vec_repl", 1);
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

/// 向量管理器（生产装配同形态：回调无状态，会话按执行域绑定；后台处理项经
/// 专用会话工厂自备会话，直调臂用 [`VectorManager::bind_dedicated_session`]）
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

/// FP32 向量参数（4 维）
fn fp32_bytes(vals: [f32; 4]) -> Vec<u8> {
  vals.iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// 解析记录的物理 key 与 ReplayInput（payload = 头 + key 长度前缀 + key + input）
fn parse_record(record: &WalRecord) -> (Vec<u8>, ReplayInput) {
  let header = AofHeader::TOTAL_SIZE;
  let key_len = u32::from_le_bytes(record.payload[header..header + 4].try_into().unwrap()) as usize;
  let key = record.payload[header + 4..header + 4 + key_len].to_vec();
  let input = ReplayInput::deserialize(&record.payload[header + 4 + key_len..])
    .expect("ReplayInput roundtrip");
  (key, input)
}

/// 应答整数值
fn as_int(reply: &VectorReply) -> i64 {
  match reply {
    VectorReply::Integer(v) => *v,
    other => panic!("期望整数应答，实际 {other:?}"),
  }
}

/// 应答浮点数组值（VEMB 嵌入）
fn as_double_array(reply: &VectorReply) -> Vec<f64> {
  match reply {
    VectorReply::Array(items) => items
      .iter()
      .map(|i| match i {
        VectorReply::Double(d) => *d,
        other => panic!("期望浮点应答项，实际 {other:?}"),
      })
      .collect(),
    other => panic!("期望数组应答，实际 {other:?}"),
  }
}

/// 生产端入队形状 + 端到端重放闭环：
/// VADD×3 → VSETATTR → VREM → AOF 条目断言 → 全新 vm 重放 → 数据完整断言
#[compio::test]
async fn vector_writes_replay_to_fresh_manager() {
  // ── 生产端装配 ──
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("v.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线）
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let aof = memory_aof();
  let vm = vector_manager(&store);
  // 本执行域绑定专用向量会话（测试单任务段，持至用例尾，对标旧 v_session 长持形态）
  let _vector_domain = vm
    .bind_dedicated_session()
    .expect("专用向量会话工厂应已注入");
  vm.set_aof_sink(Arc::new(VectorAofSink::new(
    &aof,
    Arc::clone(store.current_version_atomic()),
  )));
  aof.set_vector_manager(Arc::clone(&vm));
  let session = RespServerSessionVectors::new(Arc::clone(&vm));

  // VADD 三元素（含 SETATTR 属性与 NOQUANT 量化）
  let v1 = fp32_bytes([1.0, 0.0, 0.0, 0.0]);
  let v2 = fp32_bytes([0.0, 1.0, 0.0, 0.0]);
  let v3 = fp32_bytes([0.9, 0.1, 0.0, 0.0]);
  let args_v1: Vec<Vec<u8>> = vec![
    b"a".to_vec(),
    b"FP32".to_vec(),
    v1.clone(),
    b"e1".to_vec(),
    b"NOQUANT".to_vec(),
    b"SETATTR".to_vec(),
    b"attr-1".to_vec(),
  ];
  let unit: Vec<Vec<u8>> = vec![
    b"a".to_vec(),
    b"FP32".to_vec(),
    v2,
    b"e2".to_vec(),
    b"NOQUANT".to_vec(),
  ];
  let near_v1: Vec<Vec<u8>> = vec![
    b"a".to_vec(),
    b"FP32".to_vec(),
    v3,
    b"e3".to_vec(),
    b"NOQUANT".to_vec(),
  ];
  fn arg_refs(v: &[Vec<u8>]) -> Vec<&[u8]> {
    v.iter().map(Vec::as_slice).collect()
  }
  assert_eq!(
    as_int(
      &session
        .network_vadd(
          SessionPrefixBuf::ROOT.as_slice(),
          &arg_refs(&args_v1),
          SLOT0,
          false,
        )
        .await
    ),
    1,
    "VADD e1 应成功"
  );
  assert_eq!(
    as_int(
      &session
        .network_vadd(
          SessionPrefixBuf::ROOT.as_slice(),
          &arg_refs(&unit),
          SLOT0,
          false
        )
        .await
    ),
    1
  );
  assert_eq!(
    as_int(
      &session
        .network_vadd(
          SessionPrefixBuf::ROOT.as_slice(),
          &arg_refs(&near_v1),
          SLOT0,
          false,
        )
        .await
    ),
    1
  );

  // VSETATTR 更新 e1 属性；VREM 删除 e2
  let setattr_args: [&[u8]; 3] = [b"a", b"e1", b"attr-2"];
  assert_eq!(
    as_int(
      &session
        .network_vsetattr(SessionPrefixBuf::ROOT.as_slice(), &setattr_args, false)
        .await
    ),
    1,
    "VSETATTR 应成功"
  );
  let vrem_args: [&[u8]; 2] = [b"a", b"e2"];
  assert_eq!(
    as_int(
      &session
        .network_vrem(SessionPrefixBuf::ROOT.as_slice(), &vrem_args)
        .await
    ),
    1,
    "VREM 应成功"
  );

  // 重复 VADD 不入日志（幂等跳过）
  assert_eq!(
    as_int(
      &session
        .network_vadd(
          SessionPrefixBuf::ROOT.as_slice(),
          &arg_refs(&args_v1),
          SLOT0,
          false,
        )
        .await
    ),
    0,
    "重复 VADD 应返回 0"
  );

  aof.log().commit();

  // ── AOF 条目形状断言（3 VADD + 1 VSETATTR + 1 VREM）──
  let mut records = Vec::new();
  aof.log().scan_single_with(0, 0, i64::MAX, |rec| {
    if !waof::is_commit_frame(&rec.payload) {
      records.push(rec.clone());
    }
    true
  });
  assert_eq!(records.len(), 5, "重复 VADD 不应入队");
  for record in &records {
    let header = AofHeader::parse(&record.payload).unwrap();
    assert_eq!(header.op_type, AofEntryType::StoreRMW as u8);
  }
  let (_, vadd_input) = parse_record(&records[0]);
  assert_eq!(vadd_input.cmd, RespCommand::Vadd);
  assert_eq!(vadd_input.arg1, VADD_APPEND_LOG_ARG);
  assert_eq!(vadd_input.args.len(), 10, "VADD 条目 10 参形态");
  let dims = u32::from_le_bytes(vadd_input.args[0].clone().try_into().unwrap());
  assert_eq!(dims, 4);
  assert_eq!(&vadd_input.args[3], &v1, "values 参与原向量一致");
  assert_eq!(&vadd_input.args[4], b"e1", "element 参一致");
  assert_eq!(&vadd_input.args[7], b"attr-1", "attributes 参一致");
  let (_, vsetattr_input) = parse_record(&records[3]);
  assert_eq!(vsetattr_input.cmd, RespCommand::Vsetattr);
  assert_eq!(vsetattr_input.args.len(), 2);
  let (_, vrem_input) = parse_record(&records[4]);
  assert_eq!(vrem_input.cmd, RespCommand::Vrem);
  assert_eq!(vrem_input.args.len(), 1);
  assert_eq!(&vrem_input.args[0], b"e2");

  // ── 重放端：全新 vm（重启语义，registry 为空）重建 ──
  let recovered_vm = vector_manager(&store);
  // 重放段与重启后直调检查的会话绑定（同上口径）
  let _replay_domain = recovered_vm
    .bind_dedicated_session()
    .expect("专用向量会话工厂应已注入");
  aof.set_vector_manager(Arc::clone(&recovered_vm));
  let replay_session = store.new_session().unwrap();
  let batch = replay_session.enter_batch();
  let storage = StorageSession::new(batch);
  let processor = AofProcessor::new(Arc::clone(&aof));
  let target = ReplayTarget::new(&storage, &store);
  let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target)
    .await
    .unwrap();
  assert_eq!(replayed, 5, "五条向量条目应全部重放");

  // ── 数据完整断言（VEMB / VCARD / VGETATTR / VDIM）──
  let check = RespServerSessionVectors::new(Arc::clone(&recovered_vm));
  let vcard_args: [&[u8]; 1] = [b"a"];
  assert_eq!(
    as_int(
      &check
        .network_vcard(SessionPrefixBuf::ROOT.as_slice(), &vcard_args)
        .await
    ),
    2,
    "3 VADD - 1 VREM"
  );

  let vemb_args: [&[u8]; 2] = [b"a", b"e1"];
  let embedding = as_double_array(
    &check
      .network_vemb(SessionPrefixBuf::ROOT.as_slice(), &vemb_args)
      .await,
  );
  assert_eq!(embedding, vec![1.0, 0.0, 0.0, 0.0], "e1 嵌入应精确还原");

  let vgetattr_args: [&[u8]; 2] = [b"a", b"e1"];
  match check
    .network_vgetattr(SessionPrefixBuf::ROOT.as_slice(), &vgetattr_args)
    .await
  {
    VectorReply::Bulk(Some(attr)) => assert_eq!(&attr[..], b"attr-2", "VSETATTR 后属性生效"),
    other => panic!("e1 属性应可读，实际 {other:?}"),
  }

  let vdim_args: [&[u8]; 1] = [b"a"];
  assert_eq!(
    as_int(
      &check
        .network_vdim(SessionPrefixBuf::ROOT.as_slice(), &vdim_args)
        .await
    ),
    4,
    "维度还原"
  );

  // 被删元素不可读
  let vemb_gone: [&[u8]; 2] = [b"a", b"e2"];
  assert!(
    as_double_array(
      &check
        .network_vemb(SessionPrefixBuf::ROOT.as_slice(), &vemb_gone)
        .await
    )
    .is_empty(),
    "VREM 后 e2 应缺失"
  );

  let _ = remove_dir_all(dir.path());
}

/// 空向量集迁移 → 目标重启 → 键仍在（import_migrated_index 的索引条目
/// 合成写闭环）：索引帧导入零元素（card=0 合法空集），AOF 恰一条
/// VSETINDEX 哨兵条目；全新 VectorManager 重放后登记表与内存索引可重建
#[compio::test]
async fn migrated_empty_vector_set_rebuilds_after_replay() {
  // ── 目标端装配 + 迁移导入（预留上下文 → 索引帧，无元素帧）──
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("ve.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let aof = memory_aof();
  let vm = vector_manager(&store);
  // 本执行域绑定专用向量会话（测试单任务段，持至用例尾，对标旧 v_session 长持形态）
  let _vector_domain = vm
    .bind_dedicated_session()
    .expect("专用向量会话工厂应已注入");
  vm.set_aof_sink(Arc::new(VectorAofSink::new(
    &aof,
    Arc::clone(store.current_version_atomic()),
  )));
  aof.set_vector_manager(Arc::clone(&vm));

  let reserved = vm
    .reserve_contexts_for_migration(1)
    .await
    .expect("迁移上下文预留");
  let index = Index {
    context: reserved[0],
    index_ptr: 0,
    dimensions: 4,
    reduce_dims: 0,
    num_links: 16,
    build_exploration_factor: 200,
    quant_type: VectorQuantType::NoQuant,
    distance_metric: VectorDistanceMetricType::L2,
    flags: VectorSetFlags::NONE,
  };
  vm.import_migrated_index(
    SessionPrefixBuf::ROOT.as_slice(),
    b"migrated_empty",
    &index.to_bytes(),
    SLOT0,
  )
  .await
  .expect("迁移索引导入应成功");
  assert!(
    vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"migrated_empty")
      .is_some(),
    "导入后登记表在位"
  );
  aof.log().commit();

  // ── AOF 条目形状断言：恰一条 VSETINDEX 哨兵条目（六参几何）──
  // 逐条构成（逻辑条目恰 1，物理帧至多 2）：①StoreRMW VSETINDEX 哨兵条目
  // （key=migrated_empty 物理键、arg1=VSETINDEX_APPEND_LOG_ARG、六参）；
  // commit() 随批尾的 24B commit 元数据帧（waof/src/wal/commit.rs，取证
  // 实测：强制等待提交协程完成后该帧必现，payload=MAGIC||begin||cookie）
  // 是恢复用元数据、非数据条目，按消费面单点判据 is_commit_frame 滤除，
  // 与本文件 vector_writes_replay_to_fresh_manager 及重放端 aof_processor
  // 同口径；帧落账时机随常驻提交协程调度而变，滤除后计数恒确定。
  // 反向注入已验：向该臂补写一条数据记录（StoreRMW 非帧负载）后
  // 本计数断言红（left:2 right:1），判别力在场。
  let mut records = Vec::new();
  aof.log().scan_single_with(0, 0, i64::MAX, |rec| {
    if !waof::is_commit_frame(&rec.payload) {
      records.push(rec.clone());
    }
    true
  });
  assert_eq!(records.len(), 1, "空集迁移应恰产生一条索引条目");
  let (key, input) = parse_record(&records[0]);
  assert_eq!(input.cmd, RespCommand::Vadd);
  assert_eq!(
    input.arg1, VSETINDEX_APPEND_LOG_ARG,
    "arg1 = VSETINDEX 哨兵（区分于用户 VADD）"
  );
  assert_eq!(input.args.len(), 6, "六参紧凑几何形态");
  let dims = u32::from_le_bytes(input.args[0].clone().try_into().unwrap());
  assert_eq!(dims, 4);
  assert_eq!(
    NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, b"migrated_empty").as_slice(),
    key,
    "条目键 = 向量集键"
  );

  // ── 重放端：全新 vm（重启语义）重建 ──
  let recovered_vm = vector_manager(&store);
  // 重放段与重启后直调检查的会话绑定（同上口径）
  let _replay_domain = recovered_vm
    .bind_dedicated_session()
    .expect("专用向量会话工厂应已注入");
  aof.set_vector_manager(Arc::clone(&recovered_vm));
  let replay_session = store.new_session().unwrap();
  let batch = replay_session.enter_batch();
  let storage = StorageSession::new(batch);
  let processor = AofProcessor::new(Arc::clone(&aof));
  let target = ReplayTarget::new(&storage, &store);
  let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target)
    .await
    .unwrap();
  assert_eq!(replayed, 1, "索引条目应重放");

  // 键仍在（card=0 合法空集语义），登记与内存索引可重建
  let rebuilt = recovered_vm
    .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"migrated_empty")
    .expect("重启后键应仍在（登记表可重建）");
  let rebuilt_index = Index::from_bytes(&rebuilt).unwrap();
  assert_ne!(rebuilt_index.index_ptr, 0, "内存索引应在位");
  assert_eq!(rebuilt_index.dimensions, 4, "几何应还原");
  assert_eq!(
    recovered_vm.service.card(rebuilt_index.context),
    0,
    "空集迁移语义：card = 0"
  );

  let _ = remove_dir_all(dir.path());
}

/// 重放对向量条目的失败语义：aof 未装配 vm → 重放错误中止（对齐 RI 未注入
/// 面的同文案失败路径）；条目参数损坏 → 报 corrupt 错误
#[compio::test]
async fn vector_replay_fails_without_manager_or_on_corrupt_input() {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("v.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线）
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let aof = memory_aof();

  // 未装配 vm：直推一条 VADD 形状条目 → 重放失败
  let input = replay_input_bytes(RespCommand::Vadd, vec![]);
  let key = NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, b"a");
  let _ = aof.log().enqueue(&RecordShape {
    op_type: AofEntryType::StoreRMW,
    version: 0,
    session_id: 0,
    key: key.as_slice(),
    value: &[],
    input: &input,
    database_id: 0,
  });
  aof.log().commit();

  let replay_session = store.new_session().unwrap();
  let batch = replay_session.enter_batch();
  let storage = StorageSession::new(batch);
  let processor = AofProcessor::new(Arc::clone(&aof));
  let target = ReplayTarget::new(&storage, &store);
  let err = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target)
    .await
    .unwrap_err();
  assert!(
    err.to_string().contains("Vector Set"),
    "未装配面应报错: {err}"
  );

  // 装配 vm 后：空参条目（损坏形态）→ corrupt 错误
  let vm = vector_manager(&store);
  // 本执行域绑定专用向量会话（测试单任务段，持至用例尾，对标旧 v_session 长持形态）
  let _vector_domain = vm
    .bind_dedicated_session()
    .expect("专用向量会话工厂应已注入");
  aof.set_vector_manager(Arc::clone(&vm));
  let processor = AofProcessor::new(Arc::clone(&aof));
  let err = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target)
    .await
    .unwrap_err();
  assert!(
    err
      .to_string()
      .contains("vector VADD replay input corrupted"),
    "损坏条目应报错: {err}"
  );

  let _ = remove_dir_all(dir.path());
}
