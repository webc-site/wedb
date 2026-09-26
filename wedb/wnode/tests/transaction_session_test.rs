//! 事务组件挂载回归测试（StorageSessionProvider 生产会话链路）
//!
//! 验证 StorageSessionProvider::open_with_config / get_session 创建的会话正确注入
//! WatchVersionMap 与 TransactionManager，支持 MULTI / 入队命令 / EXEC /
//! DISCARD 以及 WATCH 乐观并发校验，杜绝因未挂载导致的未接线报错。

use std::sync::Arc;

use tempfile::tempdir;
use wnode::{
  MessageConsumerFace, SessionProviderFace, WireFormat,
  resp::{
    resp_server_session::RespServerSessionOptions, resp_session_consumer::RespSessionConsumer,
  },
  service::StorageSessionProvider,
};
use wtest_base::test_store_config;
use wtxn::TxnKeyEntryComparison;
use wval::SessionPrefixBuf;

const DB_NAME: &str = "node.db";

/// 版本表分槽（与写面推进、WATCH 登记同一 scoped 单点：默认连接归属根
/// (ns0,db0)，其 `session_prefix` 虚域恒等 ROOT，与写钩子取值同源）
fn h(key: &[u8]) -> u64 {
  TxnKeyEntryComparison::scoped_key_hash(SessionPrefixBuf::ROOT.as_slice(), key) as u64
}

#[compio::test]
async fn session_provider_multi_set_exec_pipelined() -> aok::Result<()> {
  let dir = tempdir()?;
  let provider = StorageSessionProvider::open_with_config(
    test_store_config(),
    dir.path().join(DB_NAME),
    |sender_id, api| {
      Some(RespSessionConsumer::new(
        sender_id,
        RespServerSessionOptions::default(),
        Arc::new(api),
      ))
    },
  )?;

  // 新节点的版本表按桶零初始化（WATCH 栅栏起点）
  assert_eq!(
    provider.watch_version_map.read_version(u64::MAX),
    0,
    "WatchVersionMap 正确初始化"
  );

  let mut consumer = provider
    .get_session(WireFormat::Ascii, 1)
    .expect("会话创建成功");

  // 1. 流水线执行 MULTI -> SET k1 v1 -> EXEC
  let req = b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n*1\r\n$4\r\nEXEC\r\n";
  let mut resp = Vec::new();
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(req);
  consumer.return_recv_scratch(scratch);
  let consumed = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(consumed, Some(0));

  // 预期输出：+OK (MULTI) +QUEUED (SET) *1\r\n+OK\r\n (EXEC 结果数组)
  let expected = b"+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n";
  assert_eq!(
    resp, expected,
    "MULTI/SET/EXEC 流水线应答匹配，无未接线报错"
  );

  // 2. 验证写入已持久生效：GET k1 -> $2\r\nv1\r\n
  resp.clear();
  let get_req = b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n";
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(get_req);
  consumer.return_recv_scratch(scratch);
  let consumed_get = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(consumed_get, Some(0));
  assert_eq!(resp, b"$2\r\nv1\r\n");
  Ok(())
}

#[compio::test]
async fn session_provider_multi_incr_get_exec() -> aok::Result<()> {
  let dir = tempdir()?;
  let provider = StorageSessionProvider::open_with_config(
    test_store_config(),
    dir.path().join(DB_NAME),
    |sender_id, api| {
      Some(RespSessionConsumer::new(
        sender_id,
        RespServerSessionOptions::default(),
        Arc::new(api),
      ))
    },
  )?;

  let mut consumer = provider
    .get_session(WireFormat::Ascii, 1)
    .expect("会话创建成功");

  // MULTI -> SET num 10 -> INCR num -> GET num -> EXEC
  let req = b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$3\r\nnum\r\n$2\r\n10\r\n*2\r\n$4\r\nINCR\r\n$3\r\nnum\r\n*2\r\n$3\r\nGET\r\n$3\r\nnum\r\n*1\r\n$4\r\nEXEC\r\n";
  let mut resp = Vec::new();
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(req);
  consumer.return_recv_scratch(scratch);
  let consumed = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(consumed, Some(0));

  let expected = b"+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*3\r\n+OK\r\n:11\r\n$2\r\n11\r\n";
  assert_eq!(resp, expected, "多命令事务执行与复合应答数组正确");
  Ok(())
}

#[compio::test]
async fn session_provider_scratch_incremental_batches() -> aok::Result<()> {
  let dir = tempdir()?;
  let provider = StorageSessionProvider::open_with_config(
    test_store_config(),
    dir.path().join(DB_NAME),
    |sender_id, api| {
      Some(RespSessionConsumer::new(
        sender_id,
        RespServerSessionOptions::default(),
        Arc::new(api),
      ))
    },
  )?;

  let mut consumer = provider
    .get_session(WireFormat::Ascii, 1)
    .expect("会话创建成功");

  // 模拟网络泵直读 scratch 模式分批次灌入
  let mut resp = Vec::new();

  // 批次 1: MULTI
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(b"*1\r\n$5\r\nMULTI\r\n");
  consumer.return_recv_scratch(scratch);
  assert!(consumer.try_consume_messages_into(&mut resp).is_some());
  assert_eq!(resp, b"+OK\r\n");
  resp.clear();

  // 批次 2: SET batch_key batch_val
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$9\r\nbatch_key\r\n$9\r\nbatch_val\r\n");
  consumer.return_recv_scratch(scratch);
  assert!(consumer.try_consume_messages_into(&mut resp).is_some());
  assert_eq!(resp, b"+QUEUED\r\n");
  resp.clear();

  // 批次 3: EXEC
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(b"*1\r\n$4\r\nEXEC\r\n");
  consumer.return_recv_scratch(scratch);
  assert!(consumer.try_consume_messages_into(&mut resp).is_some());
  assert_eq!(resp, b"*1\r\n+OK\r\n");
  resp.clear();

  // 批次 4: 验证写结果 GET batch_key
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$9\r\nbatch_key\r\n");
  consumer.return_recv_scratch(scratch);
  assert!(consumer.try_consume_messages_into(&mut resp).is_some());
  assert_eq!(resp, b"$9\r\nbatch_val\r\n");
  Ok(())
}

#[compio::test]
async fn session_provider_discard_flow() -> aok::Result<()> {
  let dir = tempdir()?;
  let provider = StorageSessionProvider::open_with_config(
    test_store_config(),
    dir.path().join(DB_NAME),
    |sender_id, api| {
      Some(RespSessionConsumer::new(
        sender_id,
        RespServerSessionOptions::default(),
        Arc::new(api),
      ))
    },
  )?;

  let mut consumer = provider
    .get_session(WireFormat::Ascii, 1)
    .expect("会话创建成功");

  // MULTI -> SET k_disc v_disc -> DISCARD -> GET k_disc
  let req = b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$6\r\nk_disc\r\n$6\r\nv_disc\r\n*1\r\n$7\r\nDISCARD\r\n*2\r\n$3\r\nGET\r\n$6\r\nk_disc\r\n";
  let mut resp = Vec::new();
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(req);
  consumer.return_recv_scratch(scratch);
  let consumed = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(consumed, Some(0));

  // +OK (MULTI), +QUEUED (SET), +OK (DISCARD), $-1\r\n (GET key 不存在)
  let expected = b"+OK\r\n+QUEUED\r\n+OK\r\n$-1\r\n";
  assert_eq!(resp, expected, "DISCARD 成功放弃事务");
  Ok(())
}

#[compio::test]
async fn session_provider_watch_and_conflict_abort() -> aok::Result<()> {
  let dir = tempdir()?;
  let provider = Arc::new(StorageSessionProvider::open_with_config(
    test_store_config(),
    dir.path().join(DB_NAME),
    |sender_id, api| {
      Some(RespSessionConsumer::new(
        sender_id,
        RespServerSessionOptions::default(),
        Arc::new(api),
      ))
    },
  )?);

  let mut consumer = provider
    .get_session(WireFormat::Ascii, 1)
    .expect("会话创建成功");

  // 1. 无并发修改场景：WATCH -> MULTI -> SET -> EXEC 正常提交
  let req1 = b"*2\r\n$5\r\nWATCH\r\n$4\r\nkey1\r\n*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$4\r\nval1\r\n*1\r\n$4\r\nEXEC\r\n";
  let mut resp = Vec::new();
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(req1);
  consumer.return_recv_scratch(scratch);
  let consumed = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(consumed, Some(0));
  assert_eq!(resp, b"+OK\r\n+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n");

  // 2. 并发冲突场景：WATCH key2 -> 外部修改 key2 -> MULTI -> SET key2 -> EXEC 提交失败返回 *-1\r\n
  resp.clear();
  let watch_req = b"*2\r\n$5\r\nWATCH\r\n$4\r\nkey2\r\n";
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(watch_req);
  consumer.return_recv_scratch(scratch);
  assert_eq!(consumer.try_consume_messages_into(&mut resp), Some(0));
  assert_eq!(resp, b"+OK\r\n");

  // 通过共享的 watch_version_map 模拟外部并发修改（同域写面推进的目标槽位：
  // 与 version_map_watch_hook 同一 scoped 单点，杜绝裸键哈希旧口径）
  provider.watch_version_map.increment_version(h(b"key2"));

  resp.clear();
  let txn_req =
    b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$4\r\nval2\r\n*1\r\n$4\r\nEXEC\r\n";
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(txn_req);
  consumer.return_recv_scratch(scratch);
  assert_eq!(consumer.try_consume_messages_into(&mut resp), Some(0));
  assert_eq!(
    resp, b"+OK\r\n+QUEUED\r\n*-1\r\n",
    "WATCH 键被并发推进版本后 EXEC 必须返回 nil 数组"
  );
  Ok(())
}

/// WATCH 夭折用例五元组：(键, WATCH 帧, 慢路径 TTL 命令帧, TTL 应答期望,
/// MULTI/EXEC 帧)
type WatchAbortCase = (
  &'static [u8],
  &'static [u8],
  &'static [u8],
  &'static [u8],
  &'static [u8],
);

/// 帧喂入 + 慢路径挂起补答（网络泵角色由本协程 await 承担；对位
/// resp_slow_path 的 slow_roundtrip，磁盘候选降级帧不产输出、慢路径闭环后续答）
async fn feed_and_settle(consumer: &mut RespSessionConsumer, frame: &[u8]) -> aok::Result<Vec<u8>> {
  let mut resp = Vec::new();
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let consumed = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  if let Some(slow) = consumer.take_slow_wait() {
    resp.extend_from_slice(&slow.resolve().await);
  }
  Ok(resp)
}

/// WATCH + 磁盘候选键 + TTL 族慢路径 + EXEC 夭折端到端（台账轮 4 条 1 钉死：
/// EXPIRE/PERSIST/GETEX 的慢路径臂必须经 StorageSession 包装推进 WATCH 版本
/// 表——修复前慢路径臂裸调 batch.expire_at/batch.persist/batch.put_ttl，键态
/// 已变而版本不动，C# 同序 RMW 完成钩子 IncrementVersion 必夭折）
#[compio::test]
async fn session_provider_watch_disk_candidate_slow_ttl_abort() -> aok::Result<()> {
  let dir = tempdir()?;
  let provider = Arc::new(StorageSessionProvider::open_with_config(
    test_store_config(),
    dir.path().join(DB_NAME),
    |sender_id, api| {
      Some(RespSessionConsumer::new(
        sender_id,
        RespServerSessionOptions::default(),
        Arc::new(api),
      ))
    },
  )?);

  let mut consumer = provider
    .get_session(WireFormat::Ascii, 1)
    .expect("会话创建成功");

  // 四键布景（全部快路径内存闭环）：dk 无 TTL（EXPIRE 臂）、pk 带 TTL
  //（PERSIST 臂）、gk 带 TTL（GETEX PERSIST 臂）、gk2 带 TTL（GETEX At 臂）
  for (frame, expected) in [
    (
      &b"*3\r\n$3\r\nSET\r\n$2\r\ndk\r\n$1\r\nv\r\n"[..],
      &b"+OK\r\n"[..],
    ),
    (b"*3\r\n$3\r\nSET\r\n$2\r\npk\r\n$1\r\nv\r\n", b"+OK\r\n"),
    (
      b"*3\r\n$6\r\nEXPIRE\r\n$2\r\npk\r\n$3\r\n100\r\n",
      b":1\r\n",
    ),
    (b"*3\r\n$3\r\nSET\r\n$2\r\ngk\r\n$1\r\nv\r\n", b"+OK\r\n"),
    (
      b"*3\r\n$6\r\nEXPIRE\r\n$2\r\ngk\r\n$3\r\n100\r\n",
      b":1\r\n",
    ),
    (b"*3\r\n$3\r\nSET\r\n$3\r\ngk2\r\n$1\r\nv\r\n", b"+OK\r\n"),
    (
      b"*3\r\n$6\r\nEXPIRE\r\n$3\r\ngk2\r\n$3\r\n100\r\n",
      b":1\r\n",
    ),
  ] {
    assert_eq!(
      feed_and_settle(&mut consumer, frame).await?,
      expected,
      "布景帧应答与预期不符: {frame:?}"
    );
  }
  // 冷化落盘：四键数据与 TTL 记录全为磁盘候选，TTL 族快路径 probe_alive
  // 降级，命令整体转 exec_slow 慢路径臂
  provider.store().flush_and_evict_all().await?;

  // 逐键：WATCH → 慢路径 TTL 写（应答与版本推进双断言）→ MULTI/EXEC 夭折
  // 五元组：(键, WATCH 帧, 慢路径 TTL 命令帧, TTL 应答期望, MULTI/EXEC 帧)
  let cases: &[WatchAbortCase] = &[
    (
      b"dk",
      b"*2\r\n$5\r\nWATCH\r\n$2\r\ndk\r\n",
      b"*3\r\n$6\r\nEXPIRE\r\n$2\r\ndk\r\n$3\r\n100\r\n",
      b":1\r\n",
      b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$2\r\ndk\r\n$1\r\nx\r\n*1\r\n$4\r\nEXEC\r\n",
    ),
    (
      b"pk",
      b"*2\r\n$5\r\nWATCH\r\n$2\r\npk\r\n",
      b"*2\r\n$7\r\nPERSIST\r\n$2\r\npk\r\n",
      b":1\r\n",
      b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$2\r\npk\r\n$1\r\nx\r\n*1\r\n$4\r\nEXEC\r\n",
    ),
    (
      b"gk",
      b"*2\r\n$5\r\nWATCH\r\n$2\r\ngk\r\n",
      b"*3\r\n$5\r\nGETEX\r\n$2\r\ngk\r\n$7\r\nPERSIST\r\n",
      b"$1\r\nv\r\n",
      b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$2\r\ngk\r\n$1\r\nx\r\n*1\r\n$4\r\nEXEC\r\n",
    ),
    (
      b"gk2",
      b"*2\r\n$5\r\nWATCH\r\n$3\r\ngk2\r\n",
      b"*4\r\n$5\r\nGETEX\r\n$3\r\ngk2\r\n$2\r\nEX\r\n$3\r\n100\r\n",
      b"$1\r\nv\r\n",
      b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$3\r\ngk2\r\n$1\r\nx\r\n*1\r\n$4\r\nEXEC\r\n",
    ),
  ];
  for &(key, watch_frame, cmd_frame, expected, txn_frame) in cases {
    assert_eq!(
      feed_and_settle(&mut consumer, watch_frame).await?,
      b"+OK\r\n",
      "WATCH {key:?} 应答 +OK"
    );
    let hash = h(key);
    let before = provider.watch_version_map.read_version(hash);

    assert_eq!(
      feed_and_settle(&mut consumer, cmd_frame).await?,
      expected,
      "磁盘候选键 {key:?} 慢路径 TTL 应答"
    );
    assert!(
      provider.watch_version_map.read_version(hash) > before,
      "慢路径 TTL 写后 WATCH 版本表必须推进（修复前裸调 batch 原语缺席推进）: {key:?}"
    );

    assert_eq!(
      feed_and_settle(&mut consumer, txn_frame).await?,
      b"+OK\r\n+QUEUED\r\n*-1\r\n",
      "WATCH 键 {key:?} 被慢路径 TTL 写推进版本后 EXEC 必须夭折"
    );
  }
  Ok(())
}
