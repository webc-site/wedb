use std::sync::Arc;

use compio::runtime::Runtime;
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

#[test]
fn test_network_get_sg_memory_pipeline() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
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
    assert_eq!(
      out,
      b"$4\r\nval1\r\n$4\r\nval2\r\n$4\r\nval3\r\n$-1\r\n"
    );

    // 验证整个流水线已被完全消费
    assert_eq!(s.end_read_head, pipeline.len());
    Ok(())
  })
}

#[test]
fn test_network_get_sg_disabled_config() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
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
  })
}

#[test]
fn test_network_get_sg_cold_read_batch() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
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
  })
}
