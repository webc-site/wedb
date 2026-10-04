//! 会话主循环三计数端到端测试（calls / failed / rejected）
//!
//! 对标 garnet/test/standalone/Garnet.test/RespCommandStatsTests.cs 的会话侧
//! 用例族与 libs/server/Resp/RespServerSession.cs:683-716 的计数语义：
//! 执行后 calls 必计、错误应答随 commandErrorWritten 计 failed、ACL 拒绝计
//! rejected，INFO COMMANDSTATS 段按 Redis 约定输出。

use std::{str::from_utf8, sync::Arc};

use compio::runtime::Runtime;
use wacl::{AccessControlList, GarnetAclAuthenticator};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::resp::{
  garnet_api::StoreGarnetApi,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::{assert_cmdstat, drain_output};
use wresp::command::RespCommand;
use wtest_base::open_test_store;
fn feed(s: &mut RespServerSession, bytes: &[u8]) -> Option<usize> {
  s.recv_buffer.extend_from_slice(bytes);
  let mut resp_buf = Vec::new();
  let remaining = s.try_consume_messages();
  s.take_output_into(&mut resp_buf, true);
  Runtime::new()
    .unwrap()
    .block_on(wnode_test::drive_pending_parks(s, &mut resp_buf, true));
  s.output.extend_from_slice(&resp_buf);
  remaining
}

/// 构造挂载 ACL 认证器 + 存储执行域且开启逐命令统计的会话
///
/// AUTH / ACL 族经存储点查（存储为 ACL 唯一真源），须挂存储执行域
fn stats_session(
  acl: &Arc<AccessControlList>,
  store: &Arc<WedbStore<SegmentedDevice>>,
) -> RespServerSession {
  let mut session = RespServerSession::new(
    1,
    RespServerSessionOptions {
      default_user: "default".into(),
      command_stats_monitor: true,
      ..RespServerSessionOptions::default()
    },
  );
  session.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(Arc::clone(acl)))));
  session.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));
  session
}

/// 单命令往返并取应答
fn roundtrip(session: &mut RespServerSession, frame: &[u8]) -> Vec<u8> {
  assert!(feed(session, frame).is_some(), "帧应被完整消费: {frame:?}");
  drain_output(session)
}

#[test]
fn commandstats_calls_failed_rejected_end_to_end() {
  // default 用户构造期 requirepass 装配（免密关闭，认证后 +@all 放行），构造期认证失败，后续显式 AUTH
  let acl = Arc::new(AccessControlList::new("pw").unwrap());
  let (_dir, store) = open_test_store("commandstats-acl.db").unwrap();
  let mut session = stats_session(&acl, &store);

  // 1. AUTH 放行 → calls auth=1
  assert_eq!(
    roundtrip(&mut session, b"*2\r\n$4\r\nAUTH\r\n$2\r\npw\r\n"),
    b"+OK\r\n"
  );

  // 2. PING 放行 → calls ping=1
  assert_eq!(
    roundtrip(&mut session, b"*1\r\n$4\r\nPING\r\n"),
    b"+PONG\r\n"
  );

  // 3. PING 参数过多 → 错误应答 → calls ping=2 failed=1
  assert_eq!(
    roundtrip(&mut session, b"*3\r\n$4\r\nPING\r\n$1\r\nx\r\n$1\r\ny\r\n"),
    b"-ERR wrong number of arguments for 'PING' command\r\n"
  );

  // 4a. ACL CAT 子命令调用（放行）
  assert!(roundtrip(&mut session, b"*2\r\n$3\r\nACL\r\n$3\r\nCAT\r\n").starts_with(b"*"));

  // 4b. ACL SETUSER default -ping 后同会话 PING → 拒绝 → rejected ping=1
  assert_eq!(
    roundtrip(
      &mut session,
      b"*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$7\r\ndefault\r\n$5\r\n-ping\r\n"
    ),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut session, b"*1\r\n$4\r\nPING\r\n"),
    b"-NOPERM this user has no permissions to run the command\r\n"
  );

  // 5. INFO COMMANDSTATS 段（同步路径）输出计数与竖线名
  let info = roundtrip(&mut session, b"*2\r\n$4\r\nINFO\r\n$12\r\ncommandstats\r\n");
  let text = from_utf8(&info).unwrap();
  assert!(text.contains("cmdstat_auth"), "AUTH 应有统计条目: {text}");
  assert_cmdstat(&info, "ping", 2, 1, 1);
  assert_cmdstat(&info, "auth", 1, 0, 0);
  assert_cmdstat(&info, "acl|setuser", 1, 0, 0);
  assert_cmdstat(&info, "acl|cat", 1, 0, 0);
  assert!(
    text.contains("cmdstat_acl|cat:"),
    "应包含子命令竖线名 cmdstat_acl|cat: {text}"
  );
  assert!(
    !text.contains("cmdstat_acl_cat:"),
    "不得包含下划线名 cmdstat_acl_cat: {text}"
  );
  assert!(
    text.contains("cmdstat_acl|setuser:"),
    "应包含子命令竖线名 cmdstat_acl|setuser: {text}"
  );
  assert!(
    !text.contains("cmdstat_acl_setuser:"),
    "不得包含下划线名 cmdstat_acl_setuser: {text}"
  );
}

/// 目录缺席判别值（如 RespCommand::None）不回空名行且跳过输出
#[test]
fn commandstats_catalog_absent_discriminant_skips_row() {
  let acl = Arc::new(AccessControlList::new("pw").unwrap());
  let (_dir, store) = open_test_store("commandstats-absent.db").unwrap();
  let mut session = stats_session(&acl, &store);

  // 模拟目录缺席判别值（RespCommand::None 未入册 RespCommandsInfo.json）
  if let Some(stats) = &session.command_stats {
    stats.lock().increment_calls(RespCommand::None);
  }

  let info = roundtrip(&mut session, b"*2\r\n$4\r\nINFO\r\n$12\r\ncommandstats\r\n");
  let text = from_utf8(&info).unwrap();
  assert!(
    !text.contains("cmdstat_:"),
    "目录缺席判别值不得输出空名行: {text}"
  );
  assert!(
    !text.contains("cmdstat_unknown:"),
    "目录缺席判别值不得输出 unknown 行: {text}"
  );
  assert!(
    !text.contains("cmdstat_none:"),
    "目录缺席判别值不得输出未入册成员行: {text}"
  );
}

/// 统计监视关闭（默认）：INFO COMMANDSTATS 出禁用提示，不出统计条目
#[test]
fn commandstats_disabled_by_default() {
  let session = &mut RespServerSession::default();
  let info = roundtrip(session, b"*2\r\n$4\r\nINFO\r\n$12\r\ncommandstats\r\n");
  let text = from_utf8(&info).unwrap();
  assert!(text.contains("Commandstats"));
  assert!(
    text.contains("Command stats monitoring is disabled"),
    "应出禁用提示: {text}"
  );
  assert!(!text.contains("cmdstat_"), "禁用态不得出统计条目: {text}");
}

/// 拒缴（NOPERM）后跟放行命令：拒缴臂就地复位 abort 置位标志，下条放行
/// 命令的收尾门不得被残留标志虚增 failed（修复前 ACL CAT 会被前导 PING
/// 拒缴的 command_error_written 污染为 failed=1）
#[test]
fn noperm_does_not_leak_flag_to_next_allowed_command() {
  let acl = Arc::new(AccessControlList::new("pw").unwrap());
  let (_dir, store) = open_test_store("commandstats-noperm-leak.db").unwrap();
  let session = &mut stats_session(&acl, &store);

  // 认证放行 + 收紧 default 用户（撤 ping 权限制造 NOPERM 拒缴源）
  assert_eq!(
    roundtrip(session, b"*2\r\n$4\r\nAUTH\r\n$2\r\npw\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(
      session,
      b"*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$7\r\ndefault\r\n$5\r\n-ping\r\n"
    ),
    b"+OK\r\n"
  );

  // PING 拒缴（NOPERM，计 rejected；修复前 abort 置位标志就此泄漏）
  assert_eq!(
    roundtrip(session, b"*1\r\n$4\r\nPING\r\n"),
    b"-NOPERM this user has no permissions to run the command\r\n"
  );

  // ACL CAT 放行：收尾门不得因残留标志虚增 failed
  assert!(roundtrip(session, b"*2\r\n$3\r\nACL\r\n$3\r\nCAT\r\n").starts_with(b"*"));

  let info = roundtrip(session, b"*2\r\n$4\r\nINFO\r\n$12\r\ncommandstats\r\n");
  let text = from_utf8(&info).unwrap();
  assert!(
    text.contains("cmdstat_ping:calls=0,usec=0,usec_per_call=0.00,rejected_calls=1,failed_calls=0"),
    "拒缴 ping 计 rejected 不计 failed: {text}"
  );
  assert!(
    text.contains(
      "cmdstat_acl|cat:calls=1,usec=0,usec_per_call=0.00,rejected_calls=0,failed_calls=0"
    ),
    "后继放行命令不得被残留标志虚增 failed: {text}"
  );
}

/// 未知命令后跟放行命令：Invalid 臂就地复位解析器 abort 置位标志，下条放行
/// 命令的收尾门不得被残留标志虚增 failed（修复前 PING 会被前导未知命令的
/// command_error_written 污染为 failed=1）；未知命令自身不入账（calls 门对
/// Invalid 跳过，对位 C# 解析器 TryWriteError 直写不置 commandErrorWritten）
#[test]
fn unknown_command_does_not_leak_flag_to_next_allowed_command() {
  let acl = Arc::new(AccessControlList::new("pw").unwrap());
  let (_dir, store) = open_test_store("commandstats-invalid-leak.db").unwrap();
  let session = &mut stats_session(&acl, &store);

  assert_eq!(
    roundtrip(session, b"*2\r\n$4\r\nAUTH\r\n$2\r\npw\r\n"),
    b"+OK\r\n"
  );

  // 未知命令：解析器经 abort_error_message 落错误帧并置位旗标（修复前旗标就此泄漏）
  let reply = roundtrip(session, b"*1\r\n$3\r\nFOO\r\n");
  assert!(reply.starts_with(b"-ERR"), "未知命令应答错误帧: {reply:?}");

  // PING 放行：收尾门不得因残留标志虚增 failed
  assert_eq!(roundtrip(session, b"*1\r\n$4\r\nPING\r\n"), b"+PONG\r\n");

  let info = roundtrip(session, b"*2\r\n$4\r\nINFO\r\n$12\r\ncommandstats\r\n");
  let text = from_utf8(&info).unwrap();
  assert!(
    !text.contains("cmdstat_foo"),
    "未知命令不入账无统计行: {text}"
  );
  assert!(
    text.contains("cmdstat_ping:calls=1,usec=0,usec_per_call=0.00,rejected_calls=0,failed_calls=0"),
    "后继放行命令不得被残留标志虚增 failed: {text}"
  );
}
