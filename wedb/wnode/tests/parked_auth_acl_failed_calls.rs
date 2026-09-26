//! 停车臂失败应答统一计入 commandstats failed_calls 端到端回归
//!（zcode-r126c-hello1 案二 P3）
//!
//! 票面命题：HELLO/AUTH/ACL 族经 dispatch_via_garnet_api 预筛停车，应答实际
//! 组装发生在泵侧异步域（drive.rs auth_fut await），写帧点全为 cs 自由函数
//! 不经会话置位包装；同步段 CommandStats 门扫描时 output 尚无应答、判据恒
//! 假——calls 计、failed_calls 不计，INFO commandstats 对认证失败族系统性
//! 少计。收口：停车登记随快照留存本命令应答段起点（输出水位），泵侧 await
//! 闭环后、冲出前按同步段同一判据补计 failed（`account_parked_auth_acl_failure`
//! 单点，calls 已在同步段计、不双计）。
//!
//! C# 契约对位：全部错误帧经会话级 WriteError（RespServerSessionOutput.cs:
//! 114-126）统一置 commandErrorWritten，命令收尾门（RespServerSession.cs:
//! 683-689）据此计 failed_calls 并复位——HELLO 认证失败 WRONGPASS、AUTH 族
//! 错误帧全数入账。

use std::{str::from_utf8, sync::Arc};

use compio::runtime::Runtime;
use wacl::{AccessControlList, GarnetAclAuthenticator};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::resp::{
  garnet_api::StoreGarnetApi,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::{drain_output, err_frame};
use wresp::cmd_strings::{
  RESP_WRONGPASS_INVALID_PASSWORD, RESP_WRONGPASS_INVALID_USERNAME_PASSWORD,
};
use wtest_base::{open_test_store, resp_frame};

/// 构造挂载 ACL 认证器 + 存储执行域且开启逐命令统计的会话
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

/// 模拟泵直填一批字节并驱动停车臂闭环（与产线泵同序：await 闭环 → 泵侧
/// 补计 → 冲出），返回线面应答字节
fn roundtrip(session: &mut RespServerSession, frame: &[u8]) -> Vec<u8> {
  session.recv_buffer.extend_from_slice(frame);
  let mut resp_buf = Vec::new();
  assert!(
    session.try_consume_messages().is_some(),
    "帧应被完整消费: {frame:?}"
  );
  session.take_output_into(&mut resp_buf);
  Runtime::new()
    .unwrap()
    .block_on(wnode_test::drive_pending_parks(
      session,
      &mut resp_buf,
      true,
    ));
  session.output.extend_from_slice(&resp_buf);
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

/// 停车臂三形失败各一条经 INFO commandstats 直断入账，成功路径不虚增：
/// 修复前 hello/auth/acl_setuser 失败样本全部漏计（calls 计、failed 不计）
#[test]
fn parked_auth_acl_failures_counted_in_commandstats() {
  // requirepass 部署（default 口令 pw）：WRONGPASS 为常态错误源
  let acl = Arc::new(AccessControlList::new("pw").unwrap());
  let (_dir, store) = open_test_store("parked-auth-failed-calls.db").unwrap();
  let session = &mut stats_session(&acl, &store);

  // 0. 先认证放行（后续 ACL 族命令过权限门进入停车臂）
  assert_eq!(
    roundtrip(session, &resp_frame(&[b"AUTH", b"pw"])),
    b"+OK\r\n"
  );

  // 1. HELLO 3 AUTH 错口令 → WRONGPASS（用户名非空形）
  assert_eq!(
    roundtrip(
      session,
      &resp_frame(&[b"HELLO", b"3", b"AUTH", b"default", b"badpw"])
    ),
    err_frame(RESP_WRONGPASS_INVALID_USERNAME_PASSWORD)
  );

  // 2. 裸 AUTH 错口令 → WRONGPASS（空用户名形）
  assert_eq!(
    roundtrip(session, &resp_frame(&[b"AUTH", b"badpw"])),
    err_frame(RESP_WRONGPASS_INVALID_PASSWORD)
  );

  // 3. ACL 族拒绝形：非法规则 token → 错误帧（泵侧 await 域内组装）
  let acl_err = roundtrip(
    session,
    &resp_frame(&[b"ACL", b"SETUSER", b"bob", b"not-a-rule"]),
  );
  assert!(
    acl_err.starts_with(b"-ERR"),
    "非法 ACL 规则应回错误帧: {acl_err:?}"
  );

  // 4. 成功路径不虚增：合法 ACL SETUSER → +OK
  assert_eq!(
    roundtrip(
      session,
      &resp_frame(&[b"ACL", b"SETUSER", b"bob", b"on", b"nopass", b"+@all"])
    ),
    b"+OK\r\n"
  );

  // INFO commandstats 直断（C# 收尾门口径：失败臂全数入账）
  let info = roundtrip(session, &resp_frame(&[b"INFO", b"commandstats"]));
  assert_cmdstat(&info, "hello", 1, 0, 1);
  // auth：1 成功 + 1 失败（HELLO 臂计在 hello 行）→ calls=2 failed=1
  // （成功不虚增、失败不漏计）
  assert_cmdstat(&info, "auth", 2, 0, 1);
  // acl|setuser 小写名 2 条：1 失败 + 1 成功 → failed=1
  assert_cmdstat(&info, "acl_setuser", 2, 0, 1);
}
