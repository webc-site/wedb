//! MULTI/EXEC 事务 AOF 标记生产接线与恢复回归（task/done/zcode-r20-wtxn 发现一）
//!
//! 对标 C# 事务 AOF 契约（libs/server/Transaction/TransactionManager.cs:173
//! 构造期注入 appendOnlyFile、:513 Run 落 TxnStart、:392 Commit 落 TxnCommit；
//! 重放侧 AofReplayCoordinator 按标记整组重放、尾部残组整组丢弃）：
//!
//! 1. 生产装配断线回归：AOF 点亮的真实会话 MULTI 两条写加 EXEC，日志流必须
//!    出现恰一对 TxnStart/TxnCommit 标记（修复前 attach_transaction_components
//!    恒传 None，全仓零第二赋值点，标记对自产日志流零产出）；
//! 2. 完整组恢复（对标 garnet/test/standalone/Garnet.test/AofShardedTxnRecoveryTests.cs
//!    的重启恢复闭环）：组内写在重启恢复后整组可见；
//! 3. 残组丢弃：尾部无 TxnCommit 的组（崩溃窗口产物，经真实入队 API 构造）
//!    恢复后整组不可见。

use tempfile::tempdir;
use waof::{AofEntryType, AofHeader};
use wconf::RuntimeServerOptions;
use wnode::{
  GarnetLog, MessageConsumerFace, RecordShape, SessionProviderFace, WireFormat,
  resp::resp_session_consumer::RespSessionConsumer, service::StorageSessionProvider,
};
use wnode_test::{replay_input_bytes, session_factory};
use wresp::command::RespCommand;
use wtest_base::test_store_config;
use wval::{KeyTag, NamespaceDbCodec, TaggedKeyBuf};

/// 物理键编码（条目 key 统一为 wkv 物理键：[NsVarint][DbVarint][KeyTag][用户键]；
/// 与生产写面 event sink 的 physical_key 同一编码域）
fn physical(user_key: &[u8]) -> TaggedKeyBuf {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, user_key)
}

/// 流内 TxnStart/TxnCommit 标记计数（真实日志扫描，decode AofHeader 判型）
fn txn_marker_counts(log: &GarnetLog) -> (usize, usize) {
  let mut starts = 0;
  let mut commits = 0;
  log.scan_single_with(0, log.get_begin_address(0), log.get_tail_address(0), |r| {
    if let Some(header) = AofHeader::parse(&r.payload) {
      if header.op_type == AofEntryType::TxnStart as u8 {
        starts += 1;
      }
      if header.op_type == AofEntryType::TxnCommit as u8 {
        commits += 1;
      }
    }
    true
  });
  (starts, commits)
}

/// upsert 条目编码入队（对标 aof_replay 回归同一真实入队面）
fn enqueue_upsert(log: &GarnetLog, key: &[u8], value: &[u8]) -> waof::Result<i64> {
  let input = replay_input_bytes(RespCommand::Set, vec![key.to_vec(), value.to_vec()]);
  let pkey = physical(key);
  log.enqueue(&RecordShape {
    op_type: AofEntryType::StoreUpsert,
    version: 0,
    session_id: 1,
    key: pkey.as_slice(),
    value,
    input: &input,
    database_id: 0,
  })
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

/// 场景 1+2：生产会话 MULTI/EXEC 落标记对，重启恢复后整组可见
#[compio::test]
async fn multi_exec_emits_txn_markers_and_group_survives_recovery() -> aok::Result<()> {
  let dir = tempdir()?;
  let data_path = dir.path().join("txn_recovery.db");

  // AOF 点亮的生产装配（默认 auto_commit：enqueue 同步提交）
  let provider = StorageSessionProvider::open_with_config_and_aof(
    test_store_config(),
    &data_path,
    None,
    runtime_server_options_min(),
    session_factory,
  )?;
  let mut consumer = provider
    .get_session(WireFormat::Ascii, 1)
    .expect("会话创建成功");

  // MULTI 两条写加 EXEC（应答：+OK +QUEUED ×2 + 双元素结果数组）
  let req = b"*1\r\n$5\r\nMULTI\r\n\
              *3\r\n$3\r\nSET\r\n$5\r\ntxn:a\r\n$2\r\nv1\r\n\
              *3\r\n$3\r\nSET\r\n$5\r\ntxn:b\r\n$2\r\nv2\r\n\
              *1\r\n$4\r\nEXEC\r\n";
  let resp = drive(&mut consumer, req);
  assert_eq!(
    resp, b"+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n+OK\r\n",
    "MULTI/EXEC 应答匹配"
  );

  // 生产日志流必须出现恰一对事务标记（发现一装配断线回归的核心断言：
  // 修复前 TransactionManager::new 恒传 None，此计数恒为 (0, 0)）
  let log = provider.aof().expect("aof 点亮").log();
  assert_eq!(
    txn_marker_counts(log),
    (1, 1),
    "MULTI/EXEC 必须落恰一对 TxnStart/TxnCommit 标记"
  );
  log.commit_async().await;
  drop(consumer);
  drop(provider);

  // 重启恢复（检查点为空 + WAL 全量重放，对标 C# RecoverAsync 分支）
  let recovered = StorageSessionProvider::open_recovered_with_config_and_aof(
    test_store_config(),
    &data_path,
    None,
    runtime_server_options_min(),
    false,
    session_factory,
  )
  .await?;
  let mut c2 = recovered
    .get_session(WireFormat::Ascii, 2)
    .expect("恢复后会话创建成功");
  let resp = drive(&mut c2, b"*2\r\n$3\r\nGET\r\n$5\r\ntxn:a\r\n");
  assert_eq!(resp, b"$2\r\nv1\r\n", "完整组键 txn:a 恢复后必须可见");
  let resp = drive(&mut c2, b"*2\r\n$3\r\nGET\r\n$5\r\ntxn:b\r\n");
  assert_eq!(resp, b"$2\r\nv2\r\n", "完整组键 txn:b 恢复后必须可见");
  Ok(())
}

/// 最小运行时选项（缺省：auto_commit 开，enqueue 同步提交）
fn runtime_server_options_min() -> wconf::RuntimeServerOptions {
  RuntimeServerOptions::default()
}

/// 场景 3：尾部无 TxnCommit 的残组恢复后整组丢弃（崩溃窗口产物；经真实
/// 入队 API 构造——EXEC 单命令原子期内无法在进程内制造「标记对之间崩溃」，
/// 流级构造即该窗口的磁盘态镜像，恢复走生产 recovery 全链）
#[compio::test]
async fn residual_txn_group_without_commit_is_discarded_on_recovery() -> aok::Result<()> {
  let dir = tempdir()?;
  let data_path = dir.path().join("residual.db");

  let provider = StorageSessionProvider::open_with_config_and_aof(
    test_store_config(),
    &data_path,
    None,
    runtime_server_options_min(),
    session_factory,
  )?;
  // 残组：TxnStart + 一条写，无 TxnCommit（组内截断崩溃的磁盘态）
  let log = provider.aof().expect("aof 点亮").log();
  log.enqueue(&RecordShape {
    op_type: AofEntryType::TxnStart,
    version: 0,
    session_id: 1,
    key: &[],
    value: &[],
    input: &[],
    database_id: 0,
  })?;
  enqueue_upsert(log, b"txn:c", b"v9")?;
  log.commit_async().await;
  drop(provider);

  // 恢复：重放面按标记缓冲整组，EOF 无 TxnCommit → 残组整组丢弃
  //（对标 C# AofReplayCoordinator.AddOrReplayTransactionOperation 契约）
  let recovered = StorageSessionProvider::open_recovered_with_config_and_aof(
    test_store_config(),
    &data_path,
    None,
    runtime_server_options_min(),
    false,
    session_factory,
  )
  .await?;
  let mut c2 = recovered
    .get_session(WireFormat::Ascii, 2)
    .expect("恢复后会话创建成功");
  let resp = drive(&mut c2, b"*2\r\n$3\r\nGET\r\n$5\r\ntxn:c\r\n");
  assert_eq!(
    resp, b"$-1\r\n",
    "尾部残组（无 TxnCommit）恢复后必须整组不可见"
  );
  Ok(())
}
