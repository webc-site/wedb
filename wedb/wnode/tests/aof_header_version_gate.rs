//! AOF 头版本门恢复臂集成测试（对标 C# AofProcessor.cs:240-241 读取侧版本
//! 校验与 RespAofDownlevelVersionTests.cs:132 UplevelAofVersionIsRejected）
//!
//! C# 一手语义核对结论：读取侧拒的是 `version > MaxSupportedAofHeaderVersion`
//! （上界臂），downlevel（≤ 当前界）带重映射放行。本仓自持版本域
//! （`AofHeader::AOF_FORMAT_VERSION = 0x80`，与 C# 1..=5 刻意不同号）采取
//! **等值门**（`!= 即拒`，`wnode/src/aof/aof_processor.rs`
//! `process_aof_record_internal`），上越界、旧代际、跨仓（C# 戳）文件一律
//! 显式拒绝——分叉已在 doc/zh/deviations.md「历史既有裁决·AOF 版本域」括注
//! 登记，本文件三臂即其锁测。
//!
//! 移植对应：
//! - ①合法版本往返读通 = C# 测试写盘段（RespAofDownlevelVersionTests.cs:138-148）
//!   的正常恢复对照（C# 该文件无独立合法恢复例，取写盘臂为锚）；
//! - ②uplevel 拒收 = UplevelAofVersionIsRejected（:132-158，戳
//!   MaxSupported+1、断恢复失败且内层错误含版本拒绝文案）；
//! - ③跨仓/旧代戳 = C# DownlevelV4AofRecoversOnCurrentBuild（:51-129）的
//!   **rust 分叉臂**：C# 期望旧代透明恢复，rust 等值门恒拒（本仓无 v 代际
//!   兼容域，不硬造恢复例，只锁拒收形）。
//!
//! C# `RewriteAofHeaderVersion`（:175-221）的「落盘后逐记录改写版本字节」
//! 在此以 rust 侧等价手法实现：真实写侧（`GarnetLog::enqueue`）产帧 → 回扫
//! 取帧 → 仅改 byte 0 → 重入队新日志，除版本字节外与生产记录逐字节同形，
//! 杜绝手搓帧与写侧布局漂移。

use std::sync::Arc;

use waof::{AofEntryType, AofHeader, is_commit_frame};
use wconf::RuntimeServerOptions;
use wnode::{
  aof::{
    aof_processor::{AofProcessor, AofReplayError, ReplayTarget},
    garnet_append_only_file::GarnetAppendOnlyFile,
    garnet_log::{GarnetLog, RecordShape},
    recover::aof_recover::AofRecover,
  },
  storage::session::storage_session::StorageSession,
};
use wnode_test::replay_input_bytes;
use wresp::command::RespCommand;
use wtest_base::open_test_store;
use wval::{KeyTag, NamespaceDbCodec};

/// 物理键编码（条目 key 统一为 wkv 物理键：[NsVarint][DbVarint][KeyTag][用户键]）
fn physical(user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// 单物理日志拓扑的 AOF 装配（与 aof_replay.rs::aof_fixture 同形）
fn aof_fixture(case: &str) -> aok::Result<Arc<GarnetAppendOnlyFile>> {
  let options = RuntimeServerOptions::default();
  let (_dirs, backends) = wnode_test::test_sublogs(case, 1);
  Ok(Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
    &options,
    None,
  )))
}

/// SET upsert 条目入队（生产写侧形状：key 物理编码 + Set 重放输入）
fn enqueue_upsert(log: &GarnetLog, version: i64, key: &[u8], value: &[u8]) -> waof::Result<i64> {
  let input = replay_input_bytes(RespCommand::Set, vec![key.to_vec(), value.to_vec()]);
  log.enqueue(&RecordShape {
    op_type: AofEntryType::StoreUpsert,
    version,
    session_id: 1,
    key: &physical(key),
    value,
    input: &input,
    database_id: 0,
  })
}

/// 回扫取回全部数据记录帧（排除 commit 元数据帧；对标 C#
/// RewriteAofHeaderVersion 遍历 aof.log.* 段取正长度数据记录）
fn collect_data_records(log: &GarnetLog) -> Vec<Vec<u8>> {
  let mut frames = Vec::new();
  log.scan_single_with(0, log.get_begin_address(0), log.get_tail_address(0), |r| {
    if !is_commit_frame(&r.payload) {
      frames.push(r.payload.clone());
    }
    true
  });
  frames
}

/// 真实写侧产两记录 → 逐记录仅改写头 byte 0 版本字节 → 重入队新日志并刷盘，
/// 返回 stamp 后日志（C# RewriteAofHeaderVersion :175 的 rust 等价手法）
async fn rebuilt_with_stamped_version(
  case: &str,
  stamp: u8,
) -> aok::Result<Arc<GarnetAppendOnlyFile>> {
  let src = aof_fixture(&format!("{case}-src"))?;
  enqueue_upsert(src.log(), 5, b"gate-k1", b"v1")?;
  enqueue_upsert(src.log(), 5, b"gate-k2", b"v2")?;
  src.log().commit_async().await;
  let mut frames = collect_data_records(src.log());
  assert_eq!(frames.len(), 2, "写侧应产出两条数据记录");

  let dst = aof_fixture(&format!("{case}-dst"))?;
  for frame in &mut frames {
    frame[0] = stamp;
    dst.log().get_sub_log(0).enqueue(frame)?;
  }
  dst.log().commit_async().await;
  Ok(dst)
}

/// 恢复落点装配 + 单日志恢复入口（版本 5 对齐记录 store_version）
async fn recover_case(
  aof: &Arc<GarnetAppendOnlyFile>,
  db_name: &str,
) -> aok::Result<Result<u64, AofReplayError>> {
  let (_dir, store) = open_test_store(db_name)?;
  let session = store.new_session()?;
  let storage = StorageSession::new(session.enter_batch());
  store.set_current_version(5);
  let target = ReplayTarget {
    session: &storage,
    store: Arc::clone(&store),
    aof_floor: vec![],
  };
  let processor = AofProcessor::new(Arc::clone(aof));
  let result = AofRecover::single_log_recover(&processor, aof, 0, 0, -1, &target).await;
  // 拒收臂后顺检键未落库（fail-fast，对标 C# failOnRecoveryError 拒启形态的
  // rust 断言侧承接）
  if result.is_err() {
    assert_eq!(
      storage.read_string(b"gate-k1").await?,
      None,
      "版本门拒收不得先写库"
    );
  }
  Ok(result)
}

/// ①对照臂：合法版本（AOF_FORMAT_VERSION）记录恢复重放读通
#[compio::test]
async fn legal_version_roundtrip_replays() -> aok::Result<()> {
  let aof = aof_fixture("aof_ver_gate_legal")?;
  enqueue_upsert(aof.log(), 5, b"gate-k1", b"v1")?;
  enqueue_upsert(aof.log(), 5, b"gate-k2", b"v2")?;
  aof.log().commit_async().await;

  let (_dir, store) = open_test_store("aof-ver-gate-legal-dst.db")?;
  let session = store.new_session()?;
  let storage = StorageSession::new(session.enter_batch());
  store.set_current_version(5);
  let target = ReplayTarget {
    session: &storage,
    store: Arc::clone(&store),
    aof_floor: vec![],
  };
  let processor = AofProcessor::new(Arc::clone(&aof));
  let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
  assert_eq!(replayed, 2, "两条合法记录全部重放");
  assert_eq!(storage.read_string(b"gate-k1").await?, Some(b"v1".to_vec()));
  assert_eq!(storage.read_string(b"gate-k2").await?, Some(b"v2".to_vec()));
  Ok(())
}

/// ②uplevel 臂（对标 RespAofDownlevelVersionTests.cs:132
/// UplevelAofVersionIsRejected）：戳 AOF_FORMAT_VERSION+1 → 恢复拒收，
/// 错误携带版本拒绝文案（C# 内层含 "Unsupported AOF header version"，
/// rust 单串 "Unsupported AOF format version"，thiserror 透明上抛）
#[compio::test]
async fn uplevel_version_is_rejected() -> aok::Result<()> {
  let stamp = AofHeader::AOF_FORMAT_VERSION + 1;
  let aof = rebuilt_with_stamped_version("aof_ver_gate_uplevel", stamp).await?;
  let result = recover_case(&aof, "aof-ver-gate-uplevel-dst.db").await?;
  let err = result.expect_err("上越界版本必须判败恢复");
  let msg = err.to_string();
  assert!(
    msg.contains("Unsupported AOF format version") && msg.contains(&stamp.to_string()),
    "错误须为版本门拒绝并携带被拒版本号，实得: {msg}"
  );
  Ok(())
}

/// ③跨仓/旧代际臂（C# DownlevelV4AofRecoversOnCurrentBuild :51 的 rust
/// 分叉锁测：C# 对 ≤MaxSupported 旧代透明恢复，rust 等值门恒拒——
/// deviations「历史既有裁决·AOF 版本域」括注登记的锁面对象）：
/// 戳 C# 当前版本 5 与本仓旧代 0x7F 两形，均须以版本门文案判败
#[compio::test]
async fn foreign_generation_versions_are_rejected() -> aok::Result<()> {
  for stamp in [5u8, AofHeader::AOF_FORMAT_VERSION - 1] {
    let aof = rebuilt_with_stamped_version(&format!("aof_ver_gate_gen{stamp}"), stamp).await?;
    let result = recover_case(&aof, &format!("aof-ver-gate-gen{stamp}-dst.db")).await?;
    let err = result.expect_err("异代际/跨仓版本戳必须判败恢复");
    let msg = err.to_string();
    assert!(
      msg.contains("Unsupported AOF format version") && msg.contains(&stamp.to_string()),
      "戳 {stamp} 须以版本门文案拒收并携带版本号，实得: {msg}"
    );
  }
  Ok(())
}
