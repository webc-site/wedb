use std::sync::Arc;

use wconf::{RuntimeServerConfig, ServerConfigType};
use wdev::SegmentedDevice;
use wnode::resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSession};
use wnode_test::test_env;
use wresp::command::RespCommand;

/// 构造包含指定接收缓冲的测试会话与存储环境
fn session_with_input(
  input: &[u8],
) -> (
  tempfile::TempDir,
  wkv::StoreSession<SegmentedDevice>,
  RespServerSession,
) {
  let (dir, session, mut resp) = test_env(false);
  resp.runtime_config = Arc::new(RuntimeServerConfig::with_defaults());
  resp.recv_buffer.extend_from_slice(input);
  resp.bytes_read = input.len();
  resp.read_head = 0;
  resp.end_read_head = 0;
  (dir, session, resp)
}

#[test]
fn test_next_command_maybe_get() -> aok::Result<()> {
  // 1. 包含 GET 命令前缀
  let input = b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n*2\r\n$3\r\nGET\r\n$2\r\nk2\r\n";
  let (_dir, _session, mut s) = session_with_input(input);
  // 尚未解析第一条时，end_read_head = 0 处确实是 GET
  assert!(s.next_command_maybe_get());

  // 解析第一条命令
  let cmd = s.parse_command().unwrap();
  assert_eq!(cmd, RespCommand::Get);
  // 第一条命令解析完后，end_read_head 停在第二条 GET 的前缀起点
  assert!(s.next_command_maybe_get());

  // 解析第二条命令
  s.read_head = s.end_read_head;
  let cmd2 = s.parse_command().unwrap();
  assert_eq!(cmd2, RespCommand::Get);
  // 已到末尾，无下一条
  assert!(!s.next_command_maybe_get());

  // 2. 下一条是非 GET 命令（如 SET 或 PING）
  let input_set = b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$2\r\nv2\r\n";
  let (_dir, _session, mut s_set) = session_with_input(input_set);
  s_set.parse_command().unwrap();
  assert!(!s_set.next_command_maybe_get());

  // 3. 长度不足
  let input_short = b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n*2\r\n$3";
  let (_dir, _session, mut s_short) = session_with_input(input_short);
  s_short.parse_command().unwrap();
  assert!(!s_short.next_command_maybe_get());
  Ok(())
}

#[test]
fn test_parse_get_and_key() -> aok::Result<()> {
  let input = b"*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n*2\r\n$3\r\nGET\r\n$4\r\nkey2\r\n*2\r\n$3\r\nGET\r\n$4\r\nkey3\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
  let (_dir, _session, mut s) = session_with_input(input);

  // 解析第一条 GET
  let cmd = s.parse_command().unwrap();
  assert_eq!(cmd, RespCommand::Get);
  assert_eq!(s.parse_state.arg_in(&s.recv_buffer, 0), b"key1");

  // 使用 parse_get_and_key 连续提取后续 GET
  let slice2 = s.parse_get_and_key().expect("should parse key2");
  assert_eq!(slice2.resolve(&s.recv_buffer), b"key2");

  let slice3 = s.parse_get_and_key().expect("should parse key3");
  assert_eq!(slice3.resolve(&s.recv_buffer), b"key3");

  // 第四条是 SET，不是 GET，应返回 None 且回退游标
  let old_head = s.end_read_head;
  assert!(s.parse_get_and_key().is_none());
  assert_eq!(s.end_read_head, old_head);

  // 验证后续 SET 仍能正常解析
  s.read_head = s.end_read_head;
  let cmd_set = s.parse_command().unwrap();
  assert_eq!(cmd_set, RespCommand::Set);
  Ok(())
}

#[compio::test]
async fn test_network_get_sg_memory_pipeline() -> aok::Result<()> {
  let (_dir, store_session, mut s) = session_with_input(b"");
  let batch = store_session.enter_batch();

  // 写入若干初始键值
  batch.try_upsert_sync(b"sg:k1", b"val1").unwrap().unwrap();
  batch.try_upsert_sync(b"sg:k2", b"val2").unwrap().unwrap();
  batch.try_upsert_sync(b"sg:k3", b"val3").unwrap().unwrap();

  // 构造连续 4 条 GET 流水线包（含命中、未命中）
  let pipeline = b"*2\r\n$3\r\nGET\r\n$5\r\nsg:k1\r\n*2\r\n$3\r\nGET\r\n$5\r\nsg:k2\r\n*2\r\n$3\r\nGET\r\n$5\r\nsg:k3\r\n*2\r\n$3\r\nGET\r\n$10\r\nsg:missing\r\n";
  s.recv_buffer.clear();
  s.recv_buffer.extend_from_slice(pipeline);
  s.bytes_read = pipeline.len();
  s.read_head = 0;
  s.end_read_head = 0;

  // 解析第一条命令
  let cmd = s.parse_command().unwrap();
  assert_eq!(cmd, RespCommand::Get);

  let mut out = Vec::new();
  let res = s.network_get(&[b"sg:k1"], &batch, &mut out)?;
  assert!(res);

  // 验证 4 条 GET 的响应被一次性聚合写出
  assert_eq!(out, b"$4\r\nval1\r\n$4\r\nval2\r\n$4\r\nval3\r\n$-1\r\n");

  // 验证整个流水线已被完全消费
  assert_eq!(s.end_read_head, pipeline.len());
  Ok(())
}

#[compio::test]
async fn test_network_get_sg_disabled_config() -> aok::Result<()> {
  let (_dir, store_session, mut s) = session_with_input(b"");
  let batch = store_session.enter_batch();

  batch.try_upsert_sync(b"sg:off1", b"v1").unwrap().unwrap();
  batch.try_upsert_sync(b"sg:off2", b"v2").unwrap().unwrap();

  // 关闭 sg-get
  s.runtime_config.try_set(ServerConfigType::SgGet, "no")?;
  assert!(!s.runtime_config.get_bool(ServerConfigType::SgGet));

  let pipeline = b"*2\r\n$3\r\nGET\r\n$7\r\nsg:off1\r\n*2\r\n$3\r\nGET\r\n$7\r\nsg:off2\r\n";
  s.recv_buffer.clear();
  s.recv_buffer.extend_from_slice(pipeline);
  s.bytes_read = pipeline.len();
  s.read_head = 0;
  s.end_read_head = 0;

  let cmd = s.parse_command().unwrap();
  assert_eq!(cmd, RespCommand::Get);

  let mut out = Vec::new();
  let res = s.network_get(&[b"sg:off1"], &batch, &mut out)?;
  assert!(res);

  // 仅第一条 GET 被处理
  assert_eq!(out, b"$2\r\nv1\r\n");

  // 第二条未被 SG 提前消费
  s.read_head = s.end_read_head;
  let cmd2 = s.parse_command().unwrap();
  assert_eq!(cmd2, RespCommand::Get);
  Ok(())
}

#[compio::test]
async fn test_network_get_sg_cold_read_batch() -> aok::Result<()> {
  let (dir, store_session, mut s) = session_with_input(b"");
  let batch = store_session.enter_batch();
  batch
    .try_upsert_sync(b"cold:k1", b"val_cold1")
    .unwrap()
    .unwrap();
  batch
    .try_upsert_sync(b"cold:k2", b"val_cold2")
    .unwrap()
    .unwrap();
  batch
    .try_upsert_sync(b"cold:k3", b"val_cold3")
    .unwrap()
    .unwrap();
  drop(batch);

  let store = StoreGarnetApi::new(store_session);
  s.set_garnet_api(Arc::new(store));

  // 构造包含 3 条 GET 的流水线
  let pipeline =
    b"*2\r\n$3\r\nGET\r\n$7\r\ncold:k1\r\n*2\r\n$3\r\nGET\r\n$7\r\ncold:k2\r\n*2\r\n$3\r\nGET\r\n$7\r\ncold:k3\r\n";
  s.recv_buffer.clear();
  s.recv_buffer.extend_from_slice(pipeline);
  s.bytes_read = pipeline.len();
  s.read_head = 0;
  s.end_read_head = 0;

  // 经 try_consume_messages 完整驱动消费
  let remaining = s.try_consume_messages().unwrap();
  assert_eq!(remaining, 0);

  let mut resp = Vec::new();
  s.take_output_into(&mut resp);
  assert_eq!(
    resp,
    b"$9\r\nval_cold1\r\n$9\r\nval_cold2\r\n$9\r\nval_cold3\r\n"
  );

  drop(dir);
  Ok(())
}

/// 冷读批量口 GET 与 MGET 对同一过期键必须同答 nil
///
/// 证伪票面：把过期键挤入冷区（值记录与 TTL 记录一并落盘），首个冷键令 SG
/// 快路径整批判停降级，过期键经异步批量口 `read_batch_with` 交付。修复前批量口
/// 无 TTL 门 → GET 回已过期值、MGET 逐条口有门 → 回 nil，两路终态分叉；修复后
/// 二者同答 nil。过期键 TTL 用 `put_ttl` 裸写一个极旧刻度（绕开 EXPIRE 命令把
/// 过去时间戳直接物理删除的写入口径，留住"已过期但记录仍在"的批量口目标形态）。
#[compio::test]
async fn test_network_get_cold_batch_expired_key_matches_mget() -> aok::Result<()> {
  let (dir, store_session, mut s) = session_with_input(b"");
  s.runtime_config = Arc::new(RuntimeServerConfig::with_defaults());

  {
    let batch = store_session.enter_batch();
    batch.try_upsert_sync(b"gb:hot", b"v_hot").unwrap().unwrap();
    batch.try_upsert_sync(b"gb:exp", b"v_exp").unwrap().unwrap();
    drop(batch);
  }
  // 裸写极旧 TTL 刻度（.NET Ticks 纪元 0001 起算，取远小于当前刻度的正整数）
  store_session.put_ttl(b"gb:exp", 1_000).await.unwrap();
  // 全库挤入冷区：值与 TTL 记录一并落盘，SG 冷读必降级走批量口
  store_session.store.flush_and_evict_all().await.unwrap();

  let store = StoreGarnetApi::new(store_session);
  s.set_garnet_api(Arc::new(store));

  // GET 冷读批量流水线（SG）：热冷键 + 过期冷键
  let get_pipeline = b"*2\r\n$3\r\nGET\r\n$6\r\ngb:hot\r\n*2\r\n$3\r\nGET\r\n$6\r\ngb:exp\r\n";
  s.recv_buffer.clear();
  s.recv_buffer.extend_from_slice(get_pipeline);
  s.bytes_read = get_pipeline.len();
  s.read_head = 0;
  s.end_read_head = 0;
  s.output.clear();
  s.try_consume_messages();
  let slow = s
    .take_slow_wait()
    .expect("SG 冷读必须降级挂起批量口，否则本用例不触达 read_batch_with");
  let get_out = slow.resolve().await;
  assert_eq!(
    get_out, b"$5\r\nv_hot\r\n$-1\r\n",
    "GET 冷读批量口须对过期键施 TTL 门回 nil（与 MGET 一致）"
  );

  // MGET 同两键（逐条口本就有门）：热键回值、过期键回 nil
  let mget_cmd = b"*3\r\n$4\r\nMGET\r\n$6\r\ngb:hot\r\n$6\r\ngb:exp\r\n";
  s.recv_buffer.clear();
  s.recv_buffer.extend_from_slice(mget_cmd);
  s.bytes_read = mget_cmd.len();
  s.read_head = 0;
  s.end_read_head = 0;
  s.output.clear();
  s.try_consume_messages();
  let mut mget_out = Vec::new();
  match s.take_slow_wait() {
    Some(slow) => mget_out = slow.resolve().await,
    None => s.take_output_into(&mut mget_out),
  }
  assert_eq!(
    mget_out, b"*2\r\n$5\r\nv_hot\r\n$-1\r\n",
    "MGET 逐条口须对同一过期键回 nil（与 GET 同答）"
  );

  drop(dir);
  Ok(())
}

/// 批量读口 TTL 门：Due（内存过期）键恒按 NOTFOUND 交付，Pass/Due 混排与跨块交付
/// 恒按 idx 升序、每键恰回调一次（验证 read_batch_with 的交付次序契约）
///
/// 全内存批量（无磁盘候选，不触达 Degrade 异步裁决臂），键数越过预取窗口覆盖多块，
/// 混入缺失键与三处过期键（首块一处、次块两处）
#[compio::test]
async fn test_read_batch_ttl_gate_due_and_ascending_order() -> aok::Result<()> {
  let (_dir, store_session, _s) = session_with_input(b"");
  const WINDOW: usize = 12;
  let n = WINDOW + 3; // 15，跨两块
  const MISSING: usize = 5;
  const EXPIRED: [usize; 3] = [1, 13, 14]; // 1 落首块，13/14 落次块

  let keys: Vec<Vec<u8>> = (0..n).map(|i| format!("rb:{i}").into_bytes()).collect();
  {
    let batch = store_session.enter_batch();
    for (i, k) in keys.iter().enumerate() {
      if i == MISSING {
        continue;
      }
      batch.try_upsert_sync(k, b"val").unwrap().unwrap();
    }
    drop(batch);
  }
  for &i in &EXPIRED {
    store_session.put_ttl(&keys[i], 1_000).await.unwrap();
  }

  let mut got: Vec<(usize, Option<Vec<u8>>)> = Vec::new();
  store_session
    .read_batch_with(&keys, |idx, v| got.push((idx, v.map(|s| s.to_vec()))))
    .await?;

  assert_eq!(got.len(), n, "每键须恰回调一次");
  for (pos, (idx, _)) in got.iter().enumerate() {
    assert_eq!(*idx, pos, "交付须按 idx 升序");
  }
  for (idx, val) in &got {
    let alive = *idx != MISSING && !EXPIRED.contains(idx);
    assert_eq!(val.is_some(), alive, "idx {idx} 存活/过期/缺失判定不符");
    if alive {
      assert_eq!(val.as_deref(), Some(&b"val"[..]));
    }
  }
  Ok(())
}

#[compio::test]
async fn test_network_get_sg_expired_key_returns_nil() -> aok::Result<()> {
  let (dir, store_session, mut s) = session_with_input(b"");
  let batch = store_session.enter_batch();
  batch
    .try_upsert_sync(b"exp:k1", b"val_exp1")
    .unwrap()
    .unwrap();
  batch
    .try_upsert_sync(b"cold:k2", b"val_cold2")
    .unwrap()
    .unwrap();
  drop(batch);

  // 对 exp:k1 设置过去的过期时间戳（Ticks = 1）
  store_session.put_ttl(b"exp:k1", 1).await?;

  let store = StoreGarnetApi::new(store_session);
  s.set_garnet_api(Arc::new(store));

  // 构造包含 2 条 GET 的流水线：一条已过期键，一条正常键
  let pipeline = b"*2\r\n$3\r\nGET\r\n$6\r\nexp:k1\r\n*2\r\n$3\r\nGET\r\n$7\r\ncold:k2\r\n";
  s.recv_buffer.clear();
  s.recv_buffer.extend_from_slice(pipeline);
  s.bytes_read = pipeline.len();
  s.read_head = 0;
  s.end_read_head = 0;

  let remaining = s.try_consume_messages().unwrap();
  assert_eq!(remaining, 0);

  let mut resp = Vec::new();
  s.take_output_into(&mut resp);
  assert_eq!(resp, b"$-1\r\n$9\r\nval_cold2\r\n");

  drop(dir);
  Ok(())
}
