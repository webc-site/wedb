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
use wnode_test::{assert_cmdstat, drain_output, err_frame};
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
  session.take_output_into(&mut resp_buf, true);
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

/// 流水线先导应答长于停车闭环应答形（未认证 PING 的 NOAUTH 长错误帧前置
/// AUTH +OK 5B）：停车返回即被 take_output_into 冲空缓冲，闭环应答独占
/// output[0..]——陈旧停车水位跨冲出边界失效，按其切片 output[watermark..]
/// 于 5 字节新缓冲越界 panic（process_stream catch_unwind 隔离成断连，客户端
/// 可稳定触发）。记账切点恒 0 后本形不炸且双应答序完好
#[test]
fn parked_auth_pipeline_with_leading_reply_does_not_mislice() {
  let acl = Arc::new(AccessControlList::new("pw").unwrap());
  let (_dir, store) = open_test_store("parked-auth-pipeline-mislice.db").unwrap();
  let session = &mut stats_session(&acl, &store);

  let mut frame = resp_frame(&[b"PING"]);
  frame.extend_from_slice(&resp_frame(&[b"AUTH", b"pw"]));
  assert_eq!(
    roundtrip(session, &frame),
    b"-NOAUTH Authentication required.\r\n+OK\r\n",
    "前导 NOAUTH 帧与停车 AUTH +OK 须按流水线序完整出帧"
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
  // acl|setuser 竖线名 2 条：1 失败 + 1 成功 → failed=1
  assert_cmdstat(&info, "acl|setuser", 2, 0, 1);
  let text = from_utf8(&info).unwrap();
  assert!(
    text.contains("cmdstat_acl|setuser:"),
    "应包含子命令竖线名 cmdstat_acl|setuser: {text}"
  );
  assert!(
    !text.contains("cmdstat_acl_setuser:"),
    "不得包含下划线名 cmdstat_acl_setuser: {text}"
  );
}
