//! 数据条目帧头会话 id 生产接线回归（waof-replay-session-id-zero-grouping-dead）
//!
//! 缺陷根因：生产写面数据条目曾一律以裸 `i64`→[`AofWriteContext::from_version`]
//! 入队（`session_id` 恒 0），而事务标记经 `transaction_manager` 携真值连接会话
//! id（≥1）。[`waof`] 重放协调器按 `header.session_id` 归组（查 `active_txns`），
//! 数据条目 session_id 为 0 时永不落入任何事务组 → 组原子重放与半提交尾部残组
//! 丢弃在生产路径结构性失效，仅被手工凑齐一致 session_id 的测试掩盖。
//!
//! 对标 C#：数据面 `Session.ID`（UpsertMethods.cs:139、RMWMethods.cs:271、
//! DeleteMethods.cs:57 → Log.Enqueue(...,sessionId,...)）与标记面
//! TransactionManager.cs:512-517（EnqueueTxn Session.ID）同键——本回归锁死
//! rust 侧数据条目帧头 session_id 与同连接事务标记 session_id 一致这一归组前提。
//!
//! 修复前断言必然失败（数据条目 session_id 为 0，与标记 4 不等）；修复后经
//! 真实 `StorageSessionProvider`/RESP 消费会话入队，帧头携连接真值 id。

use tempfile::tempdir;
use waof::{AofEntryType, AofHeader};
use wconf::RuntimeServerOptions;
use wnode::{
  GarnetLog, MessageConsumerFace, SessionProviderFace, WireFormat,
  resp::resp_session_consumer::RespSessionConsumer, service::StorageSessionProvider,
};
use wnode_test::session_factory;
use wtest_base::test_store_config;

/// 流内 (op_type, session_id) 记录对（真实日志扫描，decode AofHeader 取帧头）
fn scan_op_session_pairs(log: &GarnetLog) -> Vec<(u8, i32)> {
  let mut pairs = Vec::new();
  log.scan_single_with(0, log.get_begin_address(0), log.get_tail_address(0), |r| {
    if let Some(header) = AofHeader::parse(&r.payload) {
      pairs.push((header.op_type, header.session_id));
    }
    true
  });
  pairs
}

/// RESP 帧喂入 + 应答收取（网络泵角色由同步收取承接）
fn drive(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut resp = Vec::new();
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let consumed = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  resp
}

/// 场景 A：直连会话裸 SET（非事务）的数据条目帧头携真值连接会话 id
///
/// 回归核心：修复前生产写面恒以 session_id 0 入队，本断言（存在 StoreUpsert
/// 条目 session_id == 连接 id 7）在修复前必失败。
#[compio::test]
async fn plain_set_data_record_frames_carry_connection_session_id() -> aok::Result<()> {
  let dir = tempdir()?;
  let data_path = dir.path().join("plain_set.db");
  let provider = StorageSessionProvider::open_with_config_and_aof(
    test_store_config(),
    &data_path,
    None,
    RuntimeServerOptions::default(),
    session_factory,
  )?;
  // 连接 id 7（session_id_counter 自 1 起编，任意真值连接 id 均可作观测点）
  let mut consumer = provider
    .get_session(WireFormat::Ascii, 7)
    .expect("会话创建成功");
  let resp = drive(&mut consumer, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
  assert_eq!(resp, b"+OK\r\n", "裸 SET 应答 +OK");

  let log = provider.aof().expect("aof 点亮").log();
  let pairs = scan_op_session_pairs(log);
  let upserts: Vec<i32> = pairs
    .iter()
    .filter(|(op, _)| *op == AofEntryType::StoreUpsert as u8)
    .map(|(_, sid)| *sid)
    .collect();
  assert!(
    !upserts.is_empty(),
    "裸 SET 必须在流内产出 StoreUpsert 数据条目"
  );
  assert!(
    upserts.contains(&7),
    "数据条目帧头 session_id 必须携真值连接 id 7（修复前恒 0，此断言失败）：观测到 {upserts:?}"
  );
  // 修复前签名：本连接的数据条目绝不再残留 session_id 0（归组死键）
  assert!(
    !upserts.contains(&0),
    "数据条目不得以 session_id 0 入队（否则永不命中事务组）：观测到 {upserts:?}"
  );
  Ok(())
}

/// 场景 B：MULTI/EXEC 下数据条目与事务标记 session_id 同键（归组前提）
///
/// 归组机制的唯一命门：重放协调器以 `header.session_id` 为组键，标记与数据
/// 必须同值方可命中。修复前标记携真值 4、数据携 0 → 二者不等 → 组内零数据
/// → 组原子重放/残组丢弃全失效。此断言直接暴露该缺陷。
#[compio::test]
async fn multi_exec_data_and_marker_share_same_session_id() -> aok::Result<()> {
  let dir = tempdir()?;
  let data_path = dir.path().join("txn_same_key.db");
  let provider = StorageSessionProvider::open_with_config_and_aof(
    test_store_config(),
    &data_path,
    None,
    RuntimeServerOptions::default(),
    session_factory,
  )?;
  let mut consumer = provider
    .get_session(WireFormat::Ascii, 4)
    .expect("会话创建成功");
  let req = b"*1\r\n$5\r\nMULTI\r\n\
              *3\r\n$3\r\nSET\r\n$4\r\ntx:a\r\n$2\r\nv1\r\n\
              *1\r\n$4\r\nEXEC\r\n";
  let resp = drive(&mut consumer, req);
  assert_eq!(
    resp, b"+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n",
    "MULTI/SET/EXEC 应答匹配"
  );

  let log = provider.aof().expect("aof 点亮").log();
  let pairs = scan_op_session_pairs(log);

  let txn_start_sid = pairs
    .iter()
    .find(|(op, _)| *op == AofEntryType::TxnStart as u8)
    .map(|(_, sid)| *sid);
  let data_sids: Vec<i32> = pairs
    .iter()
    .filter(|(op, _)| *op == AofEntryType::StoreUpsert as u8)
    .map(|(_, sid)| *sid)
    .collect();

  let marker_sid = txn_start_sid.expect("MULTI/EXEC 必须落 TxnStart 标记");
  assert_eq!(marker_sid, 4, "事务标记 session_id 必须为真值连接 id 4");
  assert!(
    !data_sids.is_empty(),
    "事务内 SET 必须产出 StoreUpsert 数据条目"
  );
  // 归组前提：组内每条数据条目 session_id 与标记 session_id 同键
  for sid in &data_sids {
    assert_eq!(
      *sid, marker_sid,
      "数据条目帧头 session_id 必须与同事务标记同键（否则归组落空、组原子重放死）：标记 {marker_sid} 数据 {sid}"
    );
  }
  Ok(())
}
