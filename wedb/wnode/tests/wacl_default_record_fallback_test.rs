//! requirepass 档 ACL SETUSER default 落盘记录后引导回落臂收口集成测试
//!（工单 wacl-default-store-record-bootstrap-fallback-stale-password，
//! doc/zh/deviations.md §98；出生红订正见 task/done/
//! wnode-wacl-default-setuser-oldpass-revival-two-red-tests.md）
//!
//! 对标 C# `GarnetACLAuthenticator.Authenticate`
//!（garnet/libs/server/Auth/GarnetACLAuthenticator.cs:58-79）：按用户名直查、
//! 查得句柄即委托 AuthenticateInternal 定成败、查无 return false——全链零
//! 回落臂。rust 侧存储为唯一真源，引导期内存单例仅在存储【无 default 记录】
//!（AclAuthOutcome::NoRecord）时经回落臂承接（§90 锁面）；记录在场后
//! Denied（停用/口令不符）绝不回落，旧 requirepass 即时死。
//!
//! 口令文法基线：`>` 系追加非换密（双侧同义：garnet
//! libs/server/ACL/ACLParser.cs:171 AddPasswordHash 落 `_passwordHashes.Add`
//! / rust wedb/wacl/src/acl_parser.rs:140 add_password_hash），引导 default
//! 自带 requirepass，单 `>newpass` 后记录 = {旧口令, 新口令}，旧口令经记录
//! Success 臂仍有效系对拍 C# 的正确形态；真换密须 `resetpass` 组合。
//!
//! 覆盖票面测试验证点：
//! a 换密——单 `>newpass` 为追加语义锁（旧口令双参形仍 +OK）；`resetpass
//!   >newpass` 真换密后旧口令单/双参形均 -WRONGPASS（Denied 不坠落回落臂）、
//!> 新口令 +OK；HELLO 3 AUTH 旧口令 -WRONGPASS 且协议未升级；
//!> b 停用——SETUSER default off 后任意口令 -WRONGPASS；
//!> c 收权——SETUSER default resetpass >np2 -@all +get 真换密收权后旧口令
//!> -WRONGPASS、新口令会话 GET 放行、SET 回 -NOPERM；
//!> d 无记录回落锁保持——fresh 实例 AUTH default 正口令 +OK（§90 面回归）、
//!> 异名+正确口令 -WRONGPASS 锁不松；authenticate_user_via_store 三态
//!> （NoRecord/Denied 停用/Denied 口令不符）单点直断。

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::tempdir;
use wacl::AccessControlList;
use wnode::{
  MessageConsumerFace, SessionProviderFace, WireFormat,
  resp::{
    acl_commands::AclAuthOutcome, resp_server_session::RespServerSessionOptions,
    resp_session_consumer::RespSessionConsumer,
  },
  service::StorageSessionProvider,
};
use wtest_base::test_store_config;

const DB_NAME: &str = "test_wacl_default_fallback.db";

/// 组 RESP 命令帧（测试侧编码口，杜绝手数字节长度错位）
fn req(args: &[&[u8]]) -> Vec<u8> {
  let mut out = Vec::new();
  out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
  for a in args {
    out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
    out.extend_from_slice(a);
    out.extend_from_slice(b"\r\n");
  }
  out
}

/// 送一批命令字节并闭环停车臂（消费轮 + 异步认证/ACL 臂泵替身），返回应答
async fn send(
  consumer: &mut RespSessionConsumer,
  bytes: &[u8],
  resp: &mut Vec<u8>,
) -> aok::Result<()> {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(bytes);
  consumer.return_recv_scratch(scratch);
  consumer.try_consume_messages_into(resp);
  wnode_test::drive_pending_parks_consumer(consumer, resp).await;
  aok::OK
}

/// 装配 requirepass 档 provider（引导 default 内存单例，装配期不落盘）
macro_rules! requirepass_provider {
  ($dir:expr, $pass:expr) => {
    StorageSessionProvider::open_with_config(
      test_store_config(),
      $dir.path().join(DB_NAME),
      |sender_id, api| {
        Some(RespSessionConsumer::new(
          sender_id,
          RespServerSessionOptions::default(),
          Arc::new(api),
        ))
      },
    )?
    .with_requirepass(Some($pass))
  };
}

/// a 换密：SETUSER default >newpass 落盘后，旧 requirepass 经 AUTH 单参形/
/// 双参形均 -WRONGPASS（Denied 不再坠落回落臂命中引导单例），新口令 +OK；
/// HELLO 3 AUTH 旧口令 -WRONGPASS 且协议未升级（无参 HELLO 应答仍 RESP2 形）
#[test]
fn setuser_default_newpass_kills_old_requirepass() -> aok::Result<()> {
  const OLD: &[u8] = b"oldpass";
  const NEW: &[u8] = b"newpass";
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let provider = requirepass_provider!(dir, "oldpass");
    assert!(provider.acl.is_some());

    // 管理连接：无记录期回落臂认证 +OK（d 面锁），SETUSER default >newpass 落盘
    let mut admin = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("会话创建成功");
    let mut resp = Vec::new();
    send(&mut admin, &req(&[b"AUTH", b"default", OLD]), &mut resp).await?;
    assert_eq!(resp, b"+OK\r\n", "fresh 实例 AUTH default 正口令回落臂 +OK");
    resp.clear();
    send(
      &mut admin,
      &req(&[b"ACL", b"SETUSER", b"default", b">newpass"]),
      &mut resp,
    )
    .await?;
    assert_eq!(resp, b"+OK\r\n");

    // 追加语义锁：单 `>newpass` 系追加非换密（双侧同义 ACLParser.cs:171 /
    // acl_parser.rs:140），记录 = {oldpass, newpass}，旧口令经记录 Success
    // 臂仍 +OK——此 +OK 与回落臂无关（三态拆分测直断机制），不得按本值判缝
    resp.clear();
    send(&mut admin, &req(&[b"AUTH", b"default", OLD]), &mut resp).await?;
    assert_eq!(
      resp, b"+OK\r\n",
      "单 > 追加后旧口令仍有效（对拍 C# AddPasswordHash）"
    );

    // 真换密：resetpass 清空口令后仅持 newpass，记录在场且旧口令不符 →
    // Denied 绝不坠落回落臂命中引导单例
    resp.clear();
    send(
      &mut admin,
      &req(&[b"ACL", b"SETUSER", b"default", b"resetpass", b">newpass"]),
      &mut resp,
    )
    .await?;
    assert_eq!(resp, b"+OK\r\n");

    // 记录在场后：新连接旧口令双参形 -WRONGPASS（Invalid username/password）
    let mut victim = provider
      .get_session(WireFormat::Ascii, 2)
      .expect("会话创建成功");
    let mut resp = Vec::new();
    send(&mut victim, &req(&[b"AUTH", b"default", OLD]), &mut resp).await?;
    assert_eq!(
      resp, b"-WRONGPASS Invalid username/password combination\r\n",
      "换密后旧口令双参形经回落臂复活即本缝失守"
    );
    // 单参形（规范化 default）同死：-WRONGPASS Invalid password
    resp.clear();
    send(&mut victim, &req(&[b"AUTH", OLD]), &mut resp).await?;
    assert_eq!(resp, b"-WRONGPASS Invalid password\r\n");
    // 换密后异名+新口令仍 -WRONGPASS（§90 用户名门锁不松）
    resp.clear();
    send(&mut victim, &req(&[b"AUTH", b"wronguser", NEW]), &mut resp).await?;
    assert_eq!(
      resp,
      b"-WRONGPASS Invalid username/password combination\r\n"
    );
    // 新口令 +OK 且放行后续命令
    resp.clear();
    send(&mut victim, &req(&[b"AUTH", b"default", NEW]), &mut resp).await?;
    assert_eq!(resp, b"+OK\r\n");
    resp.clear();
    send(&mut victim, &req(&[b"PING"]), &mut resp).await?;
    assert_eq!(resp, b"+PONG\r\n");

    // HELLO 3 AUTH 旧口令 -WRONGPASS 且协议未升级
    let mut hello = provider
      .get_session(WireFormat::Ascii, 3)
      .expect("会话创建成功");
    let mut resp = Vec::new();
    send(
      &mut hello,
      &req(&[b"HELLO", b"3", b"AUTH", b"default", OLD]),
      &mut resp,
    )
    .await?;
    assert_eq!(
      resp,
      b"-WRONGPASS Invalid username/password combination\r\n"
    );
    resp.clear();
    send(&mut hello, &req(&[b"HELLO"]), &mut resp).await?;
    let frame = String::from_utf8_lossy(&resp);
    assert!(
      frame.starts_with("*16\r\n") && frame.contains("$5\r\nproto\r\n:2\r\n"),
      "HELLO 认证失败后协议不得升级（应答仍 RESP2 双倍数组形）: {frame}"
    );

    aok::OK
  })
}

/// b 停用：SETUSER default off 落盘停用记录后，旧 requirepass（引导单例恒
/// 启用）经回落臂不再可达——任意口令均 -WRONGPASS，kill-switch 对 default
/// 真实生效（对标 C# GarnetAclWithPasswordAuthenticator.AuthenticateInternal
/// 的 user.IsEnabled 判定，garnet/libs/server/Auth/
/// GarnetAclWithPasswordAuthenticator.cs:25-38）
#[test]
fn setuser_default_off_denies_all_passwords() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let provider = requirepass_provider!(dir, "oldpass");

    let mut admin = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("会话创建成功");
    let mut resp = Vec::new();
    send(
      &mut admin,
      &req(&[b"AUTH", b"default", b"oldpass"]),
      &mut resp,
    )
    .await?;
    assert_eq!(resp, b"+OK\r\n");
    resp.clear();
    send(
      &mut admin,
      &req(&[b"ACL", b"SETUSER", b"default", b"off"]),
      &mut resp,
    )
    .await?;
    assert_eq!(resp, b"+OK\r\n");

    // 停用记录在场：正确旧口令（单/双参形）与任意口令一律 -WRONGPASS
    let mut victim = provider
      .get_session(WireFormat::Ascii, 2)
      .expect("会话创建成功");
    let mut resp = Vec::new();
    send(
      &mut victim,
      &req(&[b"AUTH", b"default", b"oldpass"]),
      &mut resp,
    )
    .await?;
    assert_eq!(
      resp, b"-WRONGPASS Invalid username/password combination\r\n",
      "停用后旧口令经回落臂复活即 kill-switch 失效缝"
    );
    resp.clear();
    send(&mut victim, &req(&[b"AUTH", b"oldpass"]), &mut resp).await?;
    assert_eq!(resp, b"-WRONGPASS Invalid password\r\n");
    // 停用形无口令可过：认证态不破，后续命令仍 -NOAUTH
    resp.clear();
    send(&mut victim, &req(&[b"PING"]), &mut resp).await?;
    assert_eq!(resp, b"-NOAUTH Authentication required.\r\n");

    aok::OK
  })
}

/// c 收权：SETUSER default resetpass >np2 -@all +get 落盘后，旧口令
/// -WRONGPASS（resetpass 真换密 + 收权，不换密即入场绕开收权规则的旧通道
/// 被闭合；单 `>np2` 系追加，旧口令仍在记录内，见测 a 追加语义锁）；新口令
/// 会话按存储记录权限运行——GET 放行、SET 回 -NOPERM
#[test]
fn setuser_default_revoked_rules_take_effect() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let provider = requirepass_provider!(dir, "oldpass");

    let mut admin = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("会话创建成功");
    let mut resp = Vec::new();
    send(
      &mut admin,
      &req(&[b"AUTH", b"default", b"oldpass"]),
      &mut resp,
    )
    .await?;
    assert_eq!(resp, b"+OK\r\n");
    resp.clear();
    send(
      &mut admin,
      &req(&[
        b"ACL",
        b"SETUSER",
        b"default",
        b"resetpass",
        b">np2",
        b"-@all",
        b"+get",
      ]),
      &mut resp,
    )
    .await?;
    assert_eq!(resp, b"+OK\r\n");

    // 旧口令即死（收权连带换密生效）
    let mut victim = provider
      .get_session(WireFormat::Ascii, 2)
      .expect("会话创建成功");
    let mut resp = Vec::new();
    send(
      &mut victim,
      &req(&[b"AUTH", b"default", b"oldpass"]),
      &mut resp,
    )
    .await?;
    assert_eq!(
      resp,
      b"-WRONGPASS Invalid username/password combination\r\n"
    );

    // 新口令入场即按存储记录权限（+get 放行、-@all 其余 NOPERM），
    // 不再挂引导单例 +@all
    resp.clear();
    send(&mut victim, &req(&[b"AUTH", b"default", b"np2"]), &mut resp).await?;
    assert_eq!(resp, b"+OK\r\n");
    resp.clear();
    send(&mut victim, &req(&[b"GET", b"k"]), &mut resp).await?;
    assert_eq!(resp, b"$-1\r\n", "+get 在记录内，GET 须放行");
    resp.clear();
    send(&mut victim, &req(&[b"SET", b"k", b"v"]), &mut resp).await?;
    assert_eq!(
      resp, b"-NOPERM this user has no permissions to run the command\r\n",
      "-@all 收权不得被引导单例绕开"
    );

    aok::OK
  })
}

/// d 三态拆分单判据直断：authenticate_user_via_store 对存储 default 用户
/// 无记录回 NoRecord（回落臂唯一保留面）、记录在场停用回 Denied、口令
/// 不符回 Denied——Denied 与 NoRecord 判据分离，回落仅认 NoRecord
#[test]
fn store_auth_three_states_split_no_record_vs_denied() -> aok::Result<()> {
  use wacl::{AclParser, GarnetAclAuthenticator};
  use wdev::SegmentedDevice;
  use wkv::WedbStore;
  use wnode::resp::{
    acl_store::AclStore, garnet_api::StoreGarnetApi, resp_server_session::RespServerSession,
  };
  use wtest_base::open_test_store;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store): (_, Arc<WedbStore<SegmentedDevice>>) =
      open_test_store("acl-default-3state.db").unwrap();
    let session_store = store.new_session().unwrap();
    let acl_store = AclStore::new(&session_store);

    let mut session = RespServerSession::new(
      1,
      RespServerSessionOptions {
        default_user: "default".into(),
        ..RespServerSessionOptions::default()
      },
    );
    session.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(Arc::new(
      AccessControlList::new("oldpass").unwrap(),
    )))));
    session.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));

    // 态一：存储无 default 记录 → NoRecord（唯一回落判据）
    assert!(
      matches!(
        session
          .authenticate_user_via_store(&acl_store, b"default", b"oldpass")
          .await,
        AclAuthOutcome::NoRecord
      ),
      "无记录必回 NoRecord，引导回落臂唯一保留面（§90 锁面）"
    );

    // 态二：记录在场且停用 → Denied（绝不回 NoRecord，回落臂必不可达）
    let off = AclParser::parse_acl_rule("user default off >newpass +@all").unwrap();
    acl_store
      .write(0, b"default", &off.to_bytes())
      .await
      .unwrap();
    assert!(
      matches!(
        session
          .authenticate_user_via_store(&acl_store, b"default", b"newpass")
          .await,
        AclAuthOutcome::Denied
      ),
      "停用记录在场必回 Denied，停用对 default 真实生效"
    );

    // 态三：记录在场启用、口令不符 → Denied（旧引导口令不复活）
    let on = AclParser::parse_acl_rule("user default on >newpass +@all").unwrap();
    acl_store
      .write(0, b"default", &on.to_bytes())
      .await
      .unwrap();
    assert!(
      matches!(
        session
          .authenticate_user_via_store(&acl_store, b"default", b"oldpass")
          .await,
        AclAuthOutcome::Denied
      ),
      "口令不符记录在场必回 Denied，旧 requirepass 零回落"
    );
    // 同记录新口令 → Success（存储为唯一真源）
    assert!(
      matches!(
        session
          .authenticate_user_via_store(&acl_store, b"default", b"newpass")
          .await,
        AclAuthOutcome::Success(..)
      ),
      "在场记录的新口令须认证成功"
    );

    aok::OK
  })
}
