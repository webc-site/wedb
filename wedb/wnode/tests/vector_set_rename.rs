//! 向量集 RENAME 集成测试（对标 garnet/test/Garnet.test.vectorset/
//! RespVectorSetTests.cs:RenamesThenRecoverFromAOFAsync 四场景语义 + AOF
//! 恢复一致性 + UnifiedStoreOps.cs:RENAME 重命名窗口抑制清理语义）：
//!
//! 1. 旧名向量集 → 新名空缺：登记项随键迁移（同上下文零迁移），旧名无
//!    幽灵、VSIM/VCARD/VEMB 经新名命中，槽位元数据同步；
//! 2. 旧名向量集 → 新名字符串 / 新名向量集：覆写前显式清退（字符串残留
//!    删除、被顶替集合完整清理）；
//! 3. RENAMENX 新名存活（含登记表命中）→ 0；缺失旧名 → NOSUCHKEY；
//! 4. AOF 重放：VADD + 合成 RENAME 条目（cmd=RENAME、arg1=RecordType 哨兵、
//!    参数=[旧名, 新名]、条目键=新名）在全新 VectorManager 上重放后一致；
//! 5. 重命名窗口抑制清理语义端到端：真实 RENAME 后新名登记项不带
//!    SUPPRESS_CLEANUP，删除照常触发清理（C# MarkSuppressCleanup 窗口
//!    不随拷贝迁移、标志清除后删除生效）。

use std::{collections::BTreeSet, mem::forget, str::from_utf8, sync::Arc};

use compio::runtime::Runtime;
use waof::{AofEntryType, AofHeader};
use wbase::hash_slot::slot_of;
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  GarnetAppendOnlyFile, GarnetLog, MessageConsumerFace, ReplayInput, RespSessionConsumer,
  aof::{
    aof_processor::{AofProcessor, ReplayTarget},
    recover::aof_recover::AofRecover,
  },
  resp::{
    garnet_api::StoreGarnetApi,
    resp_server_session::RespServerSessionOptions,
    vector::{
      resp_server_session_vectors::{RespServerSessionVectors, VectorReply},
      vector_manager::{RECORD_TYPE, VectorManager, VectorManagerOptions},
      vector_manager_index::{INDEX_SIZE, Index},
      vector_manager_locking::split_registry_key,
      vector_manager_replication::VectorAofSink,
      vector_store_callbacks::WedbVectorStoreCallbacks,
    },
  },
  storage::session::storage_session::StorageSession,
};
use wresp::command::RespCommand;
use wval::{KeyTag, NamespaceDbCodec, SessionPrefixBuf};
use wvector::{Callbacks, VectorSetFlags};

/// 内存 AOF（无盘拓扑；重放面与磁盘拓扑同路径）
fn memory_aof() -> Arc<GarnetAppendOnlyFile> {
  let options = RuntimeServerOptions::default();
  Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(
      GarnetLog::new(
        &options,
        {
          let (_dirs, backends) = wnode_test::test_sublogs("vec_rename", 1);
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

/// 测试存储（小预算，GC 关闭）
fn test_store(tag: &str) -> Arc<WedbStore<SegmentedDevice>> {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device)).unwrap());
  forget(dir);
  store
}

/// 装配生产端三元组（存储 + AOF 直推 + 绑定向量管理器）
fn setup(
  tag: &str,
) -> (
  Arc<WedbStore<SegmentedDevice>>,
  Arc<GarnetAppendOnlyFile>,
  Arc<VectorManager>,
) {
  let store = test_store(tag);
  let aof = memory_aof();
  let session = Arc::new(store.new_session().unwrap());
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new(session))),
  ));
  vm.set_aof_sink(Arc::new(VectorAofSink::new(
    &aof,
    Arc::clone(store.current_version_atomic()),
  )));
  aof.set_vector_manager(Arc::clone(&vm));
  (store, aof, vm)
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

/// VADD 两维 FP32 向量（经 RESP 命令面全链）
fn vadd(consumer: &mut RespSessionConsumer, key: &[u8], element: &[u8], values: &[u8]) {
  let out = pump(
    consumer,
    &frame(&[b"VADD", key, b"FP32", values, element, b"NOQUANT"]),
  );
  assert_eq!(out, b":1\r\n", "VADD {key:?}/{element:?} 应成功");
}

/// VCARD 经 RESP 命令面
fn vcard(consumer: &mut RespSessionConsumer, key: &[u8]) -> i64 {
  let mut out = pump(consumer, &frame(&[b"VCARD", key]));
  assert_eq!(out.remove(0), b':', "VCARD 应答整数");
  let end = out.iter().position(|b| *b == b'\r').unwrap();
  from_utf8(&out[..end]).unwrap().parse().unwrap()
}

/// 索引记录上下文读取
fn context_of(index_value: &[u8; INDEX_SIZE]) -> u64 {
  Index::from_bytes(index_value).unwrap().context
}

/// 解析记录的物理 key 与 ReplayInput（payload = 头 + key 长度前缀 + key + input）
fn parse_record(record: &waof::WalRecord) -> (Vec<u8>, wnode::ReplayInput) {
  let header = AofHeader::TOTAL_SIZE;
  let key_len = u32::from_le_bytes(record.payload[header..header + 4].try_into().unwrap()) as usize;
  let key = record.payload[header + 4..header + 4 + key_len].to_vec();
  let input = ReplayInput::deserialize(&record.payload[header + 4 + key_len..])
    .expect("ReplayInput roundtrip");
  (key, input)
}

/// 登记表承接断言：仅 expect_key 一项登记（旧名幽灵即在此暴露）
///
/// 枚举面为登记表复合键口径（`registry_key` 单点：`[NsVarint][DbVarint]` +
/// 用户键，源端会话域内），断言经 `split_registry_key` 剥域后比对用户键
fn assert_registry_exactly(vm: &VectorManager, expect_key: &[u8]) {
  let slot_keys = [i32::from(slot_of(0, 0))];
  let keys = vm.get_vector_set_keys_for_slots(&slot_keys.iter().copied().collect());
  assert_eq!(keys.len(), 1, "登记表应恰有一项：{keys:?}");
  let (domain, user_key) = split_registry_key(&keys[0].0).expect("登记键应为复合键（可剥域）");
  assert_eq!(
    (domain.vns, domain.vdb),
    (0, 0),
    "登记键域应为默认会话域：{keys:?}"
  );
  assert_eq!(user_key, expect_key, "登记键应为新名");
}

/// 场景 1（RenamesThenRecoverFromAOFAsync 旧名向量集 → 新名空缺）：登记项
/// 随键迁移（同上下文零迁移），旧名消失、新名 VSIM/VCARD/VEMB 全链命中
#[test]
fn rename_to_absent_key_migrates_registry() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (store, _aof, vm) = setup("vs_rename1.db");
    let mut consumer = consumer_of(&store, &vm);

    let values: [u8; 8] = [0, 0, 128, 63, 0, 0, 0, 64]; // f32 [1.0, 2.0]
    vadd(&mut consumer, b"vs_src", b"foo", &values);
    let old_index = vm
      .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vs_src")
      .unwrap();
    let ctx = context_of(&old_index);

    assert_eq!(
      pump(&mut consumer, &frame(&[b"RENAME", b"vs_src", b"vs_dst"])),
      b"+OK\r\n"
    );

    // 旧名消失（登记表无幽灵），新名登记在位且上下文不变（索引零迁移）
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vs_src")
        .is_none(),
      "旧名应无登记项"
    );
    let new_index = vm
      .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vs_dst")
      .expect("新名应有登记项");
    assert_eq!(context_of(&new_index), ctx, "登记迁移不得变更上下文");
    assert_registry_exactly(&vm, b"vs_dst");

    // 上下文槽位仍指向库级槽位（库级定槽 doc/zh/db.md 4.1：同库 rename
    // 键内容不参与定槽，槽位恒为所属库槽位）
    let slot = BTreeSet::from([i32::from(slot_of(0, 0))]);
    assert!(
      vm.get_namespaces_for_hash_slots(&slot).contains(&ctx),
      "上下文槽位应指向新键 hash slot"
    );

    // 新名全链命中：VCARD / VEMB / VSIM
    assert_eq!(vcard(&mut consumer, b"vs_dst"), 1);
    let vectors = RespServerSessionVectors::new(Arc::clone(&vm));
    let emb = vectors.network_vemb(SessionPrefixBuf::ROOT.as_slice(), &[b"vs_dst", b"foo"]);
    match emb {
      VectorReply::Array(items) => {
        let dims: Vec<f64> = items
          .iter()
          .map(|i| match i {
            VectorReply::Double(d) => *d,
            other => panic!("期望浮点应答项，实际 {other:?}"),
          })
          .collect();
        assert_eq!(dims, vec![1.0, 2.0], "VEMB 应精确还原");
      }
      other => panic!("VEMB 应答数组，实际 {other:?}"),
    }
    let sim = vectors.network_vsim(
      SessionPrefixBuf::ROOT.as_slice(),
      &[b"vs_dst", b"FP32", values.as_slice(), b"COUNT", b"1"],
    );
    match sim {
      VectorReply::Array(items) => assert_eq!(items.len(), 1, "VSIM 经新名应命中"),
      other => panic!("VSIM 应答数组，实际 {other:?}"),
    }

    aok::OK
  })
  .unwrap();
}

/// 场景 2/3（RenamesThenRecoverFromAOFAsync 新名字符串 / 新名向量集）：
/// 覆写前显式清退 —— 字符串残留删除，被顶替向量集完整清理（上下文随
/// delete_vector_set 登记回收），登记项迁移后仅剩源集合
#[test]
fn rename_onto_string_and_vector_dest_cleans_displaced() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (store, _aof, vm) = setup("vs_rename2.db");
    let mut consumer = consumer_of(&store, &vm);
    let values: [u8; 8] = [0, 0, 128, 63, 0, 0, 0, 64];

    // ── 场景 2：新名 = 字符串 ──
    vadd(&mut consumer, b"src", b"foo", &values);
    assert_eq!(
      pump(&mut consumer, &frame(&[b"SET", b"dst", b"fizzbuzz"])),
      b"+OK\r\n"
    );
    let src_ctx = context_of(
      &vm
        .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"src")
        .unwrap(),
    );
    assert_eq!(
      pump(&mut consumer, &frame(&[b"RENAME", b"src", b"dst"])),
      b"+OK\r\n"
    );

    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"src")
        .is_none()
    );
    assert_eq!(
      context_of(
        &vm
          .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"dst")
          .unwrap()
      ),
      src_ctx
    );
    assert_eq!(vcard(&mut consumer, b"dst"), 1, "向量语义经新名命中");
    let get_out = pump(&mut consumer, &frame(&[b"GET", b"dst"]));
    assert!(
      get_out.starts_with(b"-WRONGTYPE "),
      "新名现为向量集，GET 应回 WRONGTYPE: {:?}",
      from_utf8(&get_out)
    );
    assert_registry_exactly(&vm, b"dst");

    // ── 场景 3：新名 = 另一向量集（被顶替集合完整清理）──
    vadd(&mut consumer, b"src2", b"el_a", &values);
    let src2_ctx = context_of(
      &vm
        .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"src2")
        .unwrap(),
    );
    vadd(
      &mut consumer,
      b"dst2",
      b"el_b",
      &[0, 0, 0, 64, 0, 0, 128, 63],
    );
    let displaced_ctx = context_of(
      &vm
        .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"dst2")
        .unwrap(),
    );
    assert_eq!(
      pump(&mut consumer, &frame(&[b"RENAME", b"src2", b"dst2"])),
      b"+OK\r\n"
    );

    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"src2")
        .is_none(),
      "旧名应无登记项"
    );
    let final_index = vm
      .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"dst2")
      .unwrap();
    assert_eq!(context_of(&final_index), src2_ctx, "新名应承接源集合上下文");
    assert_ne!(
      context_of(&final_index),
      displaced_ctx,
      "被顶替上下文应让位"
    );
    assert_eq!(vcard(&mut consumer, b"dst2"), 1, "仅源集合元素存活");
    let vectors = RespServerSessionVectors::new(Arc::clone(&vm));
    match vectors.network_vemb(SessionPrefixBuf::ROOT.as_slice(), &[b"dst2", b"el_b"]) {
      VectorReply::Array(items) => assert!(items.is_empty(), "被顶替元素应不可读"),
      other => panic!("VEMB 应答数组，实际 {other:?}"),
    }

    aok::OK
  })
  .unwrap();
}

/// RENAMENX/缺失语义：新名向量集存活（登记表命中）→ 0；缺失旧名 →
/// NOSUCHKEY；同键名恒成功（C# UnifiedStoreOps.RENAME 判定序）
#[test]
fn renamenx_and_missing_key_semantics() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (store, _aof, vm) = setup("vs_rename3.db");
    let mut consumer = consumer_of(&store, &vm);
    let values: [u8; 8] = [0, 0, 128, 63, 0, 0, 0, 64];

    vadd(&mut consumer, b"nx_a", b"foo", &values);
    vadd(&mut consumer, b"nx_b", b"foo", &values);

    // 新名为向量集（登记表命中）→ 0，两键均不动
    assert_eq!(
      pump(&mut consumer, &frame(&[b"RENAMENX", b"nx_a", b"nx_b"])),
      b":0\r\n"
    );
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"nx_a")
        .is_some()
    );
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"nx_b")
        .is_some()
    );

    // 缺失旧名 → NOSUCHKEY（wkv 双域 + 登记表皆缺）
    let err = pump(&mut consumer, &frame(&[b"RENAME", b"no_such", b"any"]));
    assert!(err.starts_with(b"-ERR"), "缺失旧名应报错：{err:?}");

    // 新名空缺 → 1，登记项迁移
    assert_eq!(
      pump(&mut consumer, &frame(&[b"RENAMENX", b"nx_a", b"nx_c"])),
      b":1\r\n"
    );
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"nx_a")
        .is_none()
    );
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"nx_c")
        .is_some()
    );

    // 同键名恒成功（RENAME → +OK）
    assert_eq!(
      pump(&mut consumer, &frame(&[b"RENAME", b"nx_c", b"nx_c"])),
      b"+OK\r\n"
    );

    aok::OK
  })
  .unwrap();
}

/// 场景 4（RenamesThenRecoverFromAOFAsync 恢复段）：VADD + RENAME 合成
/// 条目形状（cmd=RENAME、arg1=RecordType 哨兵、参数=[旧名, 新名]、条目键=
/// 新名）与全新 VectorManager 重放一致性 —— 新名命中、旧名无幽灵
#[test]
fn renamed_vector_set_replays_from_aof() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (store, aof, vm) = setup("vs_rename4.db");
    let mut consumer = consumer_of(&store, &vm);
    let values: [u8; 8] = [0, 0, 128, 63, 0, 0, 0, 64];

    vadd(&mut consumer, b"aof_src", b"foo", &values);
    assert_eq!(
      pump(&mut consumer, &frame(&[b"RENAME", b"aof_src", b"aof_dst"])),
      b"+OK\r\n"
    );
    aof.log().commit();

    // ── AOF 条目形状断言（1 VADD + 1 RENAME）──
    let mut records = Vec::new();
    aof.log().scan_single_with(0, 0, i64::MAX, |rec| {
      records.push(rec.clone());
      true
    });
    assert_eq!(records.len(), 2, "VADD + RENAME 两条目");
    for record in &records {
      let header = AofHeader::parse(&record.payload).unwrap();
      assert_eq!(header.op_type, AofEntryType::StoreRMW as u8);
    }
    let (vadd_key, vadd_input) = parse_record(&records[0]);
    assert_eq!(vadd_input.cmd, RespCommand::Vadd);
    assert_eq!(
      NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, b"aof_src").as_slice(),
      vadd_key,
      "VADD 条目键 = 旧名"
    );
    let (rename_key, rename_input) = parse_record(&records[1]);
    assert_eq!(rename_input.cmd, RespCommand::Rename, "条目命令 RENAME");
    assert_eq!(
      rename_input.arg1,
      i64::from(RECORD_TYPE),
      "arg1 = RecordType 哨兵（C# UnifiedInput.arg1 同布局）"
    );
    assert_eq!(rename_input.args.len(), 2);
    assert_eq!(rename_input.args[0], b"aof_src", "参数[0] = 旧名");
    assert_eq!(rename_input.args[1], b"aof_dst", "参数[1] = 新名");
    assert_eq!(
      NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, b"aof_dst").as_slice(),
      rename_key,
      "条目键 = 新名（C# SET(newKey) 同布局）"
    );

    // ── 重放端：全新 vm（重启语义）恢复 ──
    let recovered = {
      let session = Arc::new(store.new_session().unwrap());
      Arc::new(VectorManager::new(
        VectorManagerOptions {
          is_enabled: true,
          ..Default::default()
        },
        Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new(session))),
      ))
    };
    aof.set_vector_manager(Arc::clone(&recovered));
    let replay_session = store.new_session().unwrap();
    let batch = replay_session.enter_batch();
    let storage = StorageSession::new(batch);
    let processor = AofProcessor::new(Arc::clone(&aof));
    let target = ReplayTarget::new(&storage, &store);
    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target)
      .await
      .unwrap();
    assert_eq!(replayed, 2, "两条向量条目应全部重放");

    // 重放后一致：新名登记且元素可读，旧名无幽灵
    assert!(
      recovered
        .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"aof_src")
        .is_none(),
      "重放端旧名应无登记项"
    );
    let dst_index = recovered
      .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"aof_dst")
      .expect("重放端新名应登记");
    assert!(
      Index::from_bytes(&dst_index).is_some_and(|index| index.index_ptr != 0),
      "重放端索引应在位"
    );
    let check = RespServerSessionVectors::new(Arc::clone(&recovered));
    match check.network_vemb(SessionPrefixBuf::ROOT.as_slice(), &[b"aof_dst", b"foo"]) {
      VectorReply::Array(items) => {
        let dims: Vec<f64> = items
          .iter()
          .map(|i| match i {
            VectorReply::Double(d) => *d,
            other => panic!("期望浮点应答项，实际 {other:?}"),
          })
          .collect();
        assert_eq!(dims, vec![1.0, 2.0], "重放后 VEMB 应精确还原");
      }
      other => panic!("VEMB 应答数组，实际 {other:?}"),
    }

    aok::OK
  })
  .unwrap();
}

/// 重命名窗口抑制清理语义端到端（对标 InterruptedVectorSetDelete* 的
/// SUPPRESS 分支 + C# MarkSuppressCleanup 窗口不随拷贝迁移）：真实 RENAME
/// 后新名登记项不带 SUPPRESS_CLEANUP，删除照常触发清理并归还上下文
#[test]
fn rename_window_suppress_cleanup_follows_rename() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (store, _aof, vm) = setup("vs_rename5.db");
    let mut consumer = consumer_of(&store, &vm);
    let values: [u8; 8] = [0, 0, 128, 63, 0, 0, 0, 64];

    vadd(&mut consumer, b"win_a", b"el_a", &values);
    vadd(
      &mut consumer,
      b"win_a",
      b"el_b",
      &[0, 0, 0, 64, 0, 0, 128, 63],
    );
    let ctx = context_of(
      &vm
        .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"win_a")
        .unwrap(),
    );

    // 真实 RESP RENAME：开窗 → 迁移 → 摘除全程
    assert_eq!(
      pump(&mut consumer, &frame(&[b"RENAME", b"win_a", b"win_b"])),
      b"+OK\r\n"
    );

    // 窗口标志不随拷贝迁移：新名登记项 SUPPRESS_CLEANUP 未置位
    //（C# 拷贝先于标记；新名后续删除照常触发清理）
    let new_index = Index::from_bytes(
      &vm
        .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"win_b")
        .unwrap(),
    )
    .unwrap();
    assert!(
      !new_index.flags.contains(VectorSetFlags::SUPPRESS_CLEANUP),
      "窗口标志不得残留于新名登记项"
    );
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"win_a")
        .is_none(),
      "旧名应无登记项"
    );
    assert_eq!(vcard(&mut consumer, b"win_b"), 2, "迁移后元素存活");

    // 新名删除照常触发清理（窗口已随迁移闭环）：标记 → 迭代 → 归还
    assert_eq!(pump(&mut consumer, &frame(&[b"DEL", b"win_b"])), b":1\r\n");
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"win_b")
        .is_none()
    );
    vm.process_request_cleanup(ctx);
    let (ci, cv) =
      VectorManager::<WedbVectorStoreCallbacks<SegmentedDevice>>::decompose_context(ctx);
    assert!(
      vm.context_metadatas
        .lock()
        .get(ci)
        .unwrap()
        .is_cleaning_up(ci != 0, cv),
      "重命名后删除应照常标记清理"
    );
    vm.process_cleanup(ctx);
    assert_eq!(vm.service.card(ctx), 0, "清理迭代后内存索引应丢弃");

    aok::OK
  })
  .unwrap();
}

/// rename onto 既有向量集的重放清退（主端 delete_vector_set(new_key) 不入
/// AOF，重放端覆写前须对被顶记录 request_deletion）：VADD×2 + RENAME 条目
/// 在全新 VectorManager 重放后——登记表计数不增长（恰一项）、新名承接源
/// 上下文、被顶上下文的 HNSW 索引随重放丢弃（无泄漏）
#[test]
fn rename_onto_vector_set_replay_cleans_displaced() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (store, aof, vm) = setup("vs_rename6.db");
    let mut consumer = consumer_of(&store, &vm);
    let values: [u8; 8] = [0, 0, 128, 63, 0, 0, 0, 64];

    vadd(&mut consumer, b"rp_src", b"foo", &values);
    vadd(
      &mut consumer,
      b"rp_dst",
      b"el_b",
      &[0, 0, 0, 64, 0, 0, 128, 63],
    );
    let ctx_src = context_of(
      &vm
        .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"rp_src")
        .unwrap(),
    );
    let ctx_dst = context_of(
      &vm
        .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"rp_dst")
        .unwrap(),
    );
    assert_ne!(ctx_src, ctx_dst, "两键应各持上下文");

    assert_eq!(
      pump(&mut consumer, &frame(&[b"RENAME", b"rp_src", b"rp_dst"])),
      b"+OK\r\n"
    );
    aof.log().commit();

    // AOF 条目形状：2 VADD + 1 RENAME（主端覆写清退不入 AOF，重放端自清）
    let mut records = Vec::new();
    aof.log().scan_single_with(0, 0, i64::MAX, |rec| {
      records.push(rec.clone());
      true
    });
    assert_eq!(records.len(), 3, "VADD×2 + RENAME 三条目");
    let (_, rename_input) = parse_record(&records[2]);
    assert_eq!(rename_input.cmd, RespCommand::Rename);
    assert_eq!(rename_input.args, [b"rp_src".to_vec(), b"rp_dst".to_vec()]);

    // ── 重放端：全新 vm（副本/恢复语义）──
    let recovered = {
      let session = Arc::new(store.new_session().unwrap());
      Arc::new(VectorManager::new(
        VectorManagerOptions {
          is_enabled: true,
          ..Default::default()
        },
        Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new(session))),
      ))
    };
    aof.set_vector_manager(Arc::clone(&recovered));
    let replay_session = store.new_session().unwrap();
    let batch = replay_session.enter_batch();
    let storage = StorageSession::new(batch);
    let processor = AofProcessor::new(Arc::clone(&aof));
    let target = ReplayTarget::new(&storage, &store);
    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target)
      .await
      .unwrap();
    assert_eq!(replayed, 3, "三条向量条目应全部重放");

    // 登记表计数不增长：旧名无幽灵、新名恰一项且承接源上下文
    assert!(
      recovered
        .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"rp_src")
        .is_none()
    );
    let dst_index = recovered
      .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"rp_dst")
      .expect("重放端新名应登记");
    assert_eq!(context_of(&dst_index), ctx_src, "新名应承接源集合上下文");
    let slots: BTreeSet<i32> = [i32::from(slot_of(0, 0))].iter().copied().collect();
    let keys = recovered.get_vector_set_keys_for_slots(&slots);
    assert_eq!(keys.len(), 1, "重放后登记表应恰一项：{keys:?}");

    // 被顶上下文自清：request_deletion 已丢弃其 HNSW 内存索引（无泄漏）
    assert_eq!(
      recovered.service.card(ctx_dst),
      0,
      "被顶上下文的 HNSW 索引应随重放清理"
    );

    aok::OK
  })
  .unwrap();
}
