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
use wnode_test::drain_output;
use wtest_base::open_test_store;

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

/// cmdstat 行断言（INFO COMMANDSTATS 段内的 Redis 约定格式）
fn assert_cmdstat(info: &[u8], cmd: &str, calls: u64, rejected: u64, failed: u64) {
  let text = from_utf8(info).unwrap();
  let line = text
    .split("\r\n")
    .find(|l| l.starts_with(&format!("cmdstat_{cmd}:")))
    .unwrap_or_else(|| panic!("缺少 cmdstat_{cmd} 条目: {text}"));
  assert_eq!(
    line,
    format!(
      "cmdstat_{cmd}:calls={calls},usec=0,usec_per_call=0.00,rejected_calls={rejected},failed_calls={failed}"
    ),
  );
}

/// 模拟泵直填一批字节（take → extend → return → consume 的会话侧等价）；
/// 停车臂（AUTH/HELLO/ACL 族与挂载刷新）经共享驱动器内联闭环
fn feed(s: &mut RespServerSession, bytes: &[u8]) -> Option<usize> {
  s.recv_buffer.extend_from_slice(bytes);
  let mut resp_buf = Vec::new();
  let remaining = s.try_consume_messages();
  s.take_output_into(&mut resp_buf);
  Runtime::new()
    .unwrap()
    .block_on(wnode_test::drive_pending_parks(s, &mut resp_buf, true));
  s.output.extend_from_slice(&resp_buf);
  remaining
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

  // 4. ACL SETUSER default -ping 后同会话 PING → 拒绝 → rejected ping=1
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

  // 5. INFO COMMANDSTATS 段（同步路径）输出三计数
  let info = roundtrip(&mut session, b"*2\r\n$4\r\nINFO\r\n$12\r\ncommandstats\r\n");
  let text = from_utf8(&info).unwrap();
  assert!(text.contains("cmdstat_auth"), "AUTH 应有统计条目: {text}");
  assert_cmdstat(&info, "ping", 2, 1, 1);
  assert_cmdstat(&info, "auth", 1, 0, 0);
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
