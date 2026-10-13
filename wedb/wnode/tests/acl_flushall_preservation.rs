#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! ns0 FLUSHALL 与 ACL 用户注册表隔离回归（票
//! wnode-flushall-destroys-acl-user-records-auth-lockout，P1）
//!
//! 缺陷形态：FLUSHALL 在 ns0 臂全域物理截断日志且无恒根域住户豁免，全部租户
//! ACL 用户记录（认证唯一真源）不可逆丢失——重连 WRONGPASS 永锁、被 SETUSER
//! 改密的 default 经 ns0 回落臂以旧 requirepass 复活（认证绕过面）。
//!
//! C# 契约：FLUSHALL 只清键空间数据、绝不动 ACL 用户注册表
//! （libs/server/StoreWrapper.cs:FlushAllDatabases 仅转发 databaseManager，
//! 用户句柄驻 AccessControlList._userHandles 与存储彻底分离）。
//!
//! 锁测（票面三点裁两）：
//! 1. SETUSER 建户 → ns0 FLUSHALL → AUTH 仍 +OK；
//! 2. default 改密 → ns0 FLUSHALL → 旧 requirepass 必 WRONGPASS（回落臂
//!    复活缝回归），新口令 +OK。

use std::path::Path;

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wnode::{
  SessionProviderFace, WireFormat,
  resp::{garnet_api::StoreGarnetApi, resp_session_consumer::RespSessionConsumer},
  service::StorageSessionProvider,
};
use wnode_test::{acl_session_factory as default_consumer, send_consumer_args as send_cmd};
use wtest_base::test_store_config;

type Api = StoreGarnetApi<SegmentedDevice>;

type TestProvider = StorageSessionProvider<fn(u64, Api) -> Option<RespSessionConsumer>>;

/// 统一装配：requirepass 旧口令引导（bootstrap default 单例，装配期不落盘）。
/// provider 只开一次（引擎单实例），多会话经 get_session 派生
fn open_provider(dir: &Path) -> aok::Result<TestProvider> {
  Ok(
    StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.join("acl_flushall.db"),
      default_consumer as fn(u64, Api) -> Option<RespSessionConsumer>,
    )?
    .with_requirepass(Some("oldpw")),
  )
}

/// SETUSER 建户 → ns0 FLUSHALL → AUTH 仍 +OK；default 改密 → FLUSHALL →
/// 旧 requirepass 必 WRONGPASS、新口令 +OK
#[test]
fn test_flushall_preserves_acl_users_and_does_not_resurrect_old_password() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let provider = open_provider(dir.path())?;
    let mut admin = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("会话创建成功");
    assert_eq!(admin.session().namespace, 0, "新连接初始位于命名空间 0");

    // requirepass 引导认证（存储尚无 default 记录，ns0 回落臂放行）
    let out = send_cmd(&mut admin, &[b"AUTH", b"default", b"oldpw"]).await;
    assert_eq!(out, b"+OK\r\n", "requirepass 引导口令认证成功");

    // default 改密（存储记录自此成为唯一真源）+ 租户建户
    let out = send_cmd(
      &mut admin,
      &[
        b"ACL",
        b"SETUSER",
        b"default",
        b"on",
        b"resetpass",
        b">newpw",
        b"+@all",
      ],
    )
    .await;
    assert_eq!(out, b"+OK\r\n", "default 改密落盘");
    let out = send_cmd(
      &mut admin,
      &[b"ACL", b"SETUSER", b"1#bob", b"on", b">bobpw", b"+@all"],
    )
    .await;
    assert_eq!(out, b"+OK\r\n", "租户 1#bob 建户落盘");

    // ns0 FLUSHALL（全域物理截断，恒根域住户重挂保真）
    let out = send_cmd(&mut admin, &[b"FLUSHALL"]).await;
    assert_eq!(out, b"+OK\r\n", "ns0 FLUSHALL 成功");

    // 锁测一：租户用户跨 FLUSHALL 存活，重连 AUTH 仍 +OK（缺陷形态即 WRONGPASS 永锁）
    let mut tenant = provider
      .get_session(WireFormat::Ascii, 2)
      .expect("会话创建成功");
    let out = send_cmd(&mut tenant, &[b"AUTH", b"1#bob", b"bobpw"]).await;
    assert_eq!(out, b"+OK\r\n", "FLUSHALL 后 1#bob 认证必须仍成功");
    assert_eq!(tenant.session().namespace, 1, "认证后会话命名空间切换为 1");
    let out = send_cmd(&mut tenant, &[b"SET", b"k", b"v"]).await;
    assert_eq!(out, b"+OK\r\n", "认证用户命令权限随记录存活");

    // 锁测二：default 旧 requirepass 绝不经 ns0 回落臂复活（认证绕过面回归）
    let mut legacy = provider
      .get_session(WireFormat::Ascii, 3)
      .expect("会话创建成功");
    let out = send_cmd(&mut legacy, &[b"AUTH", b"default", b"oldpw"]).await;
    assert!(
      out.starts_with(b"-WRONGPASS"),
      "记录在场即存储为唯一真源：旧引导口令必须 WRONGPASS，绝不回落复活，实得: {}",
      String::from_utf8_lossy(&out)
    );

    // 新口令按存储记录放行
    let out = send_cmd(&mut legacy, &[b"AUTH", b"default", b"newpw"]).await;
    assert_eq!(out, b"+OK\r\n", "FLUSHALL 后 default 新口令认证成功");

    aok::OK
  })
}

/// ns0 FLUSHALL 后 ACL 管理面按存储记录回读：用户在册、规则原样
///（用户集与主端全等的单机形态）
#[test]
fn test_flushall_keeps_acl_registry_queryable() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let provider = open_provider(dir.path())?;
    let mut admin = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("会话创建成功");
    assert_eq!(
      send_cmd(&mut admin, &[b"AUTH", b"default", b"oldpw"]).await,
      b"+OK\r\n"
    );
    assert_eq!(
      send_cmd(
        &mut admin,
        &[b"ACL", b"SETUSER", b"1#bob", b"on", b">bobpw", b"+@all"]
      )
      .await,
      b"+OK\r\n"
    );

    assert_eq!(
      send_cmd(&mut admin, &[b"FLUSHALL"]).await,
      b"+OK\r\n",
      "ns0 FLUSHALL 成功"
    );

    // 管理面按存储记录回读：用户仍在册、规则原样（缺陷形态即查无此户）
    let out = send_cmd(&mut admin, &[b"ACL", b"GETUSER", b"1#bob"]).await;
    let out_str = String::from_utf8_lossy(&out);
    assert!(
      out_str.contains("flags") && out_str.contains("passwords"),
      "FLUSHALL 后 GETUSER 1#bob 须按存活记录回读规则，实得: {out_str}"
    );

    aok::OK
  })
}
