//! 事务组件挂载回归测试（StorageSessionProvider 生产会话链路）
//!
//! 验证 StorageSessionProvider::open / get_session 创建的会话正确注入
//! WatchVersionMap 与 TransactionManager，支持 MULTI / 入队命令 / EXEC /
//! DISCARD 以及 WATCH 乐观并发校验，杜绝因未挂载导致的未接线报错。

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::tempdir;
use wedb_test::test_store_config;
use wnode::{
  MessageConsumerFace, SessionProviderFace, WireFormat,
  resp::{
    resp_server_session::RespServerSessionOptions, resp_session_consumer::RespSessionConsumer,
  },
  service::StorageSessionProvider,
};
use wtxn::{DEFAULT_VERSION_MAP_SIZE, TxnKeyEntryComparison};

const DB_NAME: &str = "node.db";

#[test]
fn session_provider_multi_set_exec_pipelined() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let provider = StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join(DB_NAME),
      |sender_id, api| {
        Some(RespSessionConsumer::new(
          sender_id,
          RespServerSessionOptions::default(),
          api,
        ))
      },
    )?;

    assert_eq!(
      provider.watch_version_map.size(),
      DEFAULT_VERSION_MAP_SIZE,
      "WatchVersionMap 正确初始化"
    );

    let mut consumer = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("会话创建成功");

    // 1. 流水线执行 MULTI -> SET k1 v1 -> EXEC
    let req =
      b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n*1\r\n$4\r\nEXEC\r\n";
    let mut resp = Vec::new();
    let consumed = consumer.try_consume_messages_into(req, &mut resp);
    assert_eq!(consumed, req.len());

    // 预期输出：+OK (MULTI) +QUEUED (SET) *1\r\n+OK\r\n (EXEC 结果数组)
    let expected = b"+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n";
    assert_eq!(
      resp, expected,
      "MULTI/SET/EXEC 流水线应答匹配，无未接线报错"
    );

    // 2. 验证写入已持久生效：GET k1 -> $2\r\nv1\r\n
    resp.clear();
    let get_req = b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n";
    let consumed_get = consumer.try_consume_messages_into(get_req, &mut resp);
    assert_eq!(consumed_get, get_req.len());
    assert_eq!(resp, b"$2\r\nv1\r\n");

    Ok(())
  })
}

#[test]
fn session_provider_multi_incr_get_exec() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let provider = StorageSessionProvider::open_with_config(test_store_config(),
      dir.path().join(DB_NAME),
      |sender_id, api| {
        Some(RespSessionConsumer::new(
          sender_id,
          RespServerSessionOptions::default(),
          api,
        ))
      },
    )?;

    let mut consumer = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("会话创建成功");

    // MULTI -> SET num 10 -> INCR num -> GET num -> EXEC
    let req = b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$3\r\nnum\r\n$2\r\n10\r\n*2\r\n$4\r\nINCR\r\n$3\r\nnum\r\n*2\r\n$3\r\nGET\r\n$3\r\nnum\r\n*1\r\n$4\r\nEXEC\r\n";
    let mut resp = Vec::new();
    let consumed = consumer.try_consume_messages_into(req, &mut resp);
    assert_eq!(consumed, req.len());

    let expected = b"+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*3\r\n+OK\r\n:11\r\n$2\r\n11\r\n";
    assert_eq!(resp, expected, "多命令事务执行与复合应答数组正确");

    Ok(())
  })
}

#[test]
fn session_provider_scratch_incremental_batches() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let provider = StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join(DB_NAME),
      |sender_id, api| {
        Some(RespSessionConsumer::new(
          sender_id,
          RespServerSessionOptions::default(),
          api,
        ))
      },
    )?;

    let mut consumer = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("会话创建成功");

    // 模拟网络泵直读 scratch 模式分批次灌入
    let mut resp = Vec::new();

    // 批次 1: MULTI
    let mut scratch = consumer.take_recv_scratch().unwrap();
    scratch.extend_from_slice(b"*1\r\n$5\r\nMULTI\r\n");
    consumer.return_recv_scratch(scratch);
    assert!(consumer.try_consume_scratch_into(&mut resp).is_some());
    assert_eq!(resp, b"+OK\r\n");
    resp.clear();

    // 批次 2: SET batch_key batch_val
    let mut scratch = consumer.take_recv_scratch().unwrap();
    scratch.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$9\r\nbatch_key\r\n$9\r\nbatch_val\r\n");
    consumer.return_recv_scratch(scratch);
    assert!(consumer.try_consume_scratch_into(&mut resp).is_some());
    assert_eq!(resp, b"+QUEUED\r\n");
    resp.clear();

    // 批次 3: EXEC
    let mut scratch = consumer.take_recv_scratch().unwrap();
    scratch.extend_from_slice(b"*1\r\n$4\r\nEXEC\r\n");
    consumer.return_recv_scratch(scratch);
    assert!(consumer.try_consume_scratch_into(&mut resp).is_some());
    assert_eq!(resp, b"*1\r\n+OK\r\n");
    resp.clear();

    // 批次 4: 验证写结果 GET batch_key
    let mut scratch = consumer.take_recv_scratch().unwrap();
    scratch.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$9\r\nbatch_key\r\n");
    consumer.return_recv_scratch(scratch);
    assert!(consumer.try_consume_scratch_into(&mut resp).is_some());
    assert_eq!(resp, b"$9\r\nbatch_val\r\n");

    Ok(())
  })
}

#[test]
fn session_provider_discard_flow() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let provider = StorageSessionProvider::open_with_config(test_store_config(),
      dir.path().join(DB_NAME),
      |sender_id, api| {
        Some(RespSessionConsumer::new(
          sender_id,
          RespServerSessionOptions::default(),
          api,
        ))
      },
    )?;

    let mut consumer = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("会话创建成功");

    // MULTI -> SET k_disc v_disc -> DISCARD -> GET k_disc
    let req = b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$6\r\nk_disc\r\n$6\r\nv_disc\r\n*1\r\n$7\r\nDISCARD\r\n*2\r\n$3\r\nGET\r\n$6\r\nk_disc\r\n";
    let mut resp = Vec::new();
    let consumed = consumer.try_consume_messages_into(req, &mut resp);
    assert_eq!(consumed, req.len());

    // +OK (MULTI), +QUEUED (SET), +OK (DISCARD), $-1\r\n (GET key 不存在)
    let expected = b"+OK\r\n+QUEUED\r\n+OK\r\n$-1\r\n";
    assert_eq!(resp, expected, "DISCARD 成功放弃事务");

    Ok(())
  })
}

#[test]
fn session_provider_watch_and_conflict_abort() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let provider = Arc::new(StorageSessionProvider::open_with_config(test_store_config(),
      dir.path().join(DB_NAME),
      |sender_id, api| {
        Some(RespSessionConsumer::new(
          sender_id,
          RespServerSessionOptions::default(),
          api,
        ))
      },
    )?);

    let mut consumer = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("会话创建成功");

    // 1. 无并发修改场景：WATCH -> MULTI -> SET -> EXEC 正常提交
    let req1 = b"*2\r\n$5\r\nWATCH\r\n$4\r\nkey1\r\n*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$4\r\nval1\r\n*1\r\n$4\r\nEXEC\r\n";
    let mut resp = Vec::new();
    let consumed = consumer.try_consume_messages_into(req1, &mut resp);
    assert_eq!(consumed, req1.len());
    assert_eq!(resp, b"+OK\r\n+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n");

    // 2. 并发冲突场景：WATCH key2 -> 外部修改 key2 -> MULTI -> SET key2 -> EXEC 提交失败返回 *-1\r\n
    resp.clear();
    let watch_req = b"*2\r\n$5\r\nWATCH\r\n$4\r\nkey2\r\n";
    assert_eq!(consumer.try_consume_messages_into(watch_req, &mut resp), watch_req.len());
    assert_eq!(resp, b"+OK\r\n");

    // 通过共享的 watch_version_map 模拟外部并发修改
    let key2_hash = TxnKeyEntryComparison::key_hash(b"key2") as u64;
    provider.watch_version_map.increment_version(key2_hash);

    resp.clear();
    let txn_req = b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$4\r\nval2\r\n*1\r\n$4\r\nEXEC\r\n";
    assert_eq!(consumer.try_consume_messages_into(txn_req, &mut resp), txn_req.len());
    assert_eq!(
      resp,
      b"+OK\r\n+QUEUED\r\n*-1\r\n",
      "WATCH 键被并发推进版本后 EXEC 必须返回 nil 数组"
    );

    Ok(())
  })
}
