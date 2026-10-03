//! requirepass 认证链路集成测试
//!
//! 验证 StorageSessionProvider 与 RespSessionConsumer 的 requirepass 配置与 ACL 拦截行为：
//! 1. 未配置 requirepass 时：默认全放行，无密码阻拦。
//! 2. 配置了 requirepass 时：未认证拦截为 -NOAUTH、密码错误拒绝为 -WRONGPASS、正确密码通过并放行后续操作。
//! 3. 覆盖单参数 AUTH <password> 与双参数 AUTH default <password> 两种形式。
//! 4. RespSessionConsumer::attach_acl 显式注入链路。
//! 5. 用户名门锁面（doc/zh/deviations.md §90）：双参 AUTH / HELLO 3 AUTH
//!    非 default 异名+正确口令拒绝为 -WRONGPASS，协议升级与客户端名不落。

use std::{path::Path, sync::Arc};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wacl::{AccessControlList, GarnetAclAuthenticator};
use wdev::SegmentedDevice;
use wnode::{
  MessageConsumerFace, SessionProviderFace, WireFormat,
  resp::{
    garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions,
    resp_session_consumer::RespSessionConsumer,
  },
  service::StorageSessionProvider,
};
use wtest_base::test_store_config;

const DB_NAME: &str = "test_requirepass.db";
/// 主用例口令（与 RESP 帧内 $18 字节序列同值，改动须同步帧）
const PWD: &str = "my_secret_password";

type Api = StoreGarnetApiAlias;
type StoreGarnetApiAlias = StoreGarnetApi<SegmentedDevice>;

/// 单机 RESP 会话默认装饰钩子（open_with_config 的 decorate 参数）
fn default_consumer(sender_id: u64, api: Api) -> Option<RespSessionConsumer> {
  Some(RespSessionConsumer::new(
    sender_id,
    RespServerSessionOptions::default(),
    Arc::new(api),
  ))
}

type Provider = StorageSessionProvider<fn(u64, Api) -> Option<RespSessionConsumer>>;

/// 统一装配：开库 → 注 requirepass → 建单会话；返回（provider 保活句柄, 会话）
fn open_session(
  dir: &Path,
  requirepass: Option<&str>,
) -> aok::Result<(Provider, RespSessionConsumer)> {
  let provider = StorageSessionProvider::open_with_config(
    test_store_config(),
    dir.join(DB_NAME),
    default_consumer as fn(u64, Api) -> Option<RespSessionConsumer>,
  )?
  .with_requirepass(requirepass);
  let consumer = provider
    .get_session(WireFormat::Ascii, 1)
    .expect("会话创建成功");
  Ok((provider, consumer))
}

/// 注入一帧请求并即刻消费（断言整帧消化 consumed == Some(0)）
fn feed(consumer: &mut RespSessionConsumer, req: &[u8], resp: &mut Vec<u8>) {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(req);
  consumer.return_recv_scratch(scratch);
  assert_eq!(consumer.try_consume_messages_into(resp), Some(0));
}

/// feed + 停车驱动闭环（AUTH/HELLO 等经存储点查的命令臂）
async fn call(consumer: &mut RespSessionConsumer, req: &[u8], resp: &mut Vec<u8>) {
  feed(consumer, req, resp);
  wnode_test::drive_pending_parks_consumer(consumer, resp).await;
}

/// 未配置 requirepass 保持默认全放行
#[test]
fn session_without_requirepass_allows_all() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let (provider, mut consumer) = open_session(dir.path(), None)?;

    assert!(provider.acl.is_none());

    let mut resp = Vec::new();

    // 未认证 PING 直接放行
    feed(&mut consumer, b"*1\r\n$4\r\nPING\r\n", &mut resp);
    assert_eq!(resp, b"+PONG\r\n");

    // SET 命令直接放行
    resp.clear();
    feed(
      &mut consumer,
      b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n",
      &mut resp,
    );
    assert_eq!(resp, b"+OK\r\n");

    aok::OK
  })
}

/// 配置空字符串密码等价于未配置
#[test]
fn session_with_empty_requirepass_allows_all() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let (provider, mut consumer) = open_session(dir.path(), Some(""))?;

    assert!(provider.acl.is_none());

    let mut resp = Vec::new();
    feed(&mut consumer, b"*1\r\n$4\r\nPING\r\n", &mut resp);
    assert_eq!(resp, b"+PONG\r\n");

    aok::OK
  })
}

/// 配置了 requirepass 时的完整认证拦截闭环：
/// 未 AUTH 拦截 -> 密码错误拒绝 -> 正确密码通过 -> 放行后续操作
/// 涵盖单参数 AUTH <password> 与双参数 AUTH <user> <password>
#[test]
fn session_with_requirepass_lifecycle() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let (provider, mut consumer) = open_session(dir.path(), Some(PWD))?;

    assert!(provider.acl.is_some());

    let mut resp = Vec::new();

    // 1. 未认证时执行 PING 拦截为 -NOAUTH
    feed(&mut consumer, b"*1\r\n$4\r\nPING\r\n", &mut resp);
    assert_eq!(resp, b"-NOAUTH Authentication required.\r\n");

    // 2. 未认证时执行 SET 同样拦截为 -NOAUTH
    resp.clear();
    feed(
      &mut consumer,
      b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
      &mut resp,
    );
    assert_eq!(resp, b"-NOAUTH Authentication required.\r\n");

    // 3. AUTH 错误密码被拒绝（单参数形式）
    resp.clear();
    call(
      &mut consumer,
      b"*2\r\n$4\r\nAUTH\r\n$9\r\nwrongpass\r\n",
      &mut resp,
    )
    .await;
    assert_eq!(resp, b"-WRONGPASS Invalid password\r\n");

    // 4. AUTH 错误用户名或密码被拒绝（双参数形式）
    resp.clear();
    call(
      &mut consumer,
      b"*3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$9\r\nwrongpass\r\n",
      &mut resp,
    )
    .await;
    assert_eq!(
      resp,
      b"-WRONGPASS Invalid username/password combination\r\n"
    );

    // 5. 再次执行 PING 仍然被 -NOAUTH 拦截
    resp.clear();
    feed(&mut consumer, b"*1\r\n$4\r\nPING\r\n", &mut resp);
    assert_eq!(resp, b"-NOAUTH Authentication required.\r\n");

    // 6. AUTH 正确密码成功通过（双参数 AUTH default <pass>）
    resp.clear();
    call(
      &mut consumer,
      b"*3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$18\r\nmy_secret_password\r\n",
      &mut resp,
    )
    .await;
    assert_eq!(resp, b"+OK\r\n");

    // 7. 认证成功后执行 PING 正常返回 +PONG
    resp.clear();
    feed(&mut consumer, b"*1\r\n$4\r\nPING\r\n", &mut resp);
    assert_eq!(resp, b"+PONG\r\n");

    // 8. 认证成功后执行 SET/GET 读写正常放行
    resp.clear();
    feed(
      &mut consumer,
      b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
      &mut resp,
    );
    assert_eq!(resp, b"+OK\r\n");

    resp.clear();
    feed(
      &mut consumer,
      b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
      &mut resp,
    );
    assert_eq!(resp, b"$3\r\nbar\r\n");

    aok::OK
  })
}

/// 用户名门锁（doc/zh/deviations.md §90）：双参 AUTH 非 default 异名+正确口令
/// 拒绝为 -WRONGPASS，认证态不破、后续命令仍 -NOAUTH。
/// C# GarnetPasswordAuthenticator 弃用户名形同形回 +OK，rust 单档用户名参与
/// 匹配系严向收口（对齐真 Redis「requirepass 须 username==default」），
/// 严禁按 C# 形回退
#[test]
fn requirepass_twoarg_auth_non_default_username_denied() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let (_provider, mut consumer) = open_session(dir.path(), Some(PWD))?;

    let mut resp = Vec::new();

    // 双参 AUTH 异名+正确口令 → -WRONGPASS invalid username-password pair
    call(
      &mut consumer,
      b"*3\r\n$4\r\nAUTH\r\n$9\r\nwronguser\r\n$18\r\nmy_secret_password\r\n",
      &mut resp,
    )
    .await;
    assert_eq!(
      resp,
      b"-WRONGPASS Invalid username/password combination\r\n"
    );

    // 认证态未破：后续命令仍 -NOAUTH
    resp.clear();
    feed(&mut consumer, b"*1\r\n$4\r\nPING\r\n", &mut resp);
    assert_eq!(resp, b"-NOAUTH Authentication required.\r\n");

    aok::OK
  })
}

/// 用户名门锁（doc/zh/deviations.md §90）：HELLO 3 一体臂 AUTH 非 default
/// 异名+正确口令（随行 SETNAME）拒绝为 -WRONGPASS，且认证失败提前返回——
/// 协议未升级（无参 HELLO 应答按会话当前版本组帧：RESP2 双倍数组头 *16
/// 而非 RESP3 map 头 %8，proto 字段仍 :2）、客户端名未落（补认证后
/// CLIENT GETNAME 回 nil）
#[test]
fn requirepass_hello_auth_non_default_username_no_side_effect() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let (_provider, mut consumer) = open_session(dir.path(), Some(PWD))?;

    let mut resp = Vec::new();

    // 1. HELLO 3 AUTH 异名+正确口令 SETNAME → -WRONGPASS
    call(
      &mut consumer,
      b"*7\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$4\r\nAUTH\r\n$9\r\nwronguser\r\n$18\r\nmy_secret_password\r\n$7\r\nSETNAME\r\n$3\r\nzzz\r\n",
      &mut resp,
    )
    .await;
    assert_eq!(
      resp,
      b"-WRONGPASS Invalid username/password combination\r\n"
    );

    // 2. 协议未升级：无参 HELLO 应答仍 RESP2 双倍数组形（8 字段 ×2 项），
    //    proto 字段为 2
    resp.clear();
    call(&mut consumer, b"*1\r\n$5\r\nHELLO\r\n", &mut resp).await;
    let frame = String::from_utf8_lossy(&resp);
    assert!(frame.starts_with("*16\r\n"));
    assert!(frame.contains("$5\r\nproto\r\n:2\r\n"));

    // 3. 补认证成功（default 正确口令 +OK 锁保持绿）
    resp.clear();
    call(
      &mut consumer,
      b"*3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$18\r\nmy_secret_password\r\n",
      &mut resp,
    )
    .await;
    assert_eq!(resp, b"+OK\r\n");

    // 4. 客户端名未落：CLIENT GETNAME 回 nil（第 1 步的 SETNAME 未落地）
    resp.clear();
    call(&mut consumer, b"*2\r\n$6\r\nCLIENT\r\n$7\r\nGETNAME\r\n", &mut resp).await;
    assert_eq!(resp, b"$-1\r\n");

    aok::OK
  })
}

/// 显式调用 RespSessionConsumer::attach_acl 委托测试
#[test]
fn consumer_attach_acl_delegation() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let (_provider, mut consumer) = open_session(dir.path(), None)?;

    let acl = Arc::new(AccessControlList::new("manual_pass")?);
    let auth = Some(Arc::new(GarnetAclAuthenticator::new(acl)));
    consumer.attach_acl(auth);

    let mut resp = Vec::new();

    // 未认证 PING
    feed(&mut consumer, b"*1\r\n$4\r\nPING\r\n", &mut resp);
    assert_eq!(resp, b"-NOAUTH Authentication required.\r\n");

    // AUTH 成功（停车臂经共享驱动器闭环：存储点查须 async 域）
    resp.clear();
    call(
      &mut consumer,
      b"*2\r\n$4\r\nAUTH\r\n$11\r\nmanual_pass\r\n",
      &mut resp,
    )
    .await;
    wnode_test::drive_pending_parks_consumer(&mut consumer, &mut resp).await;
    assert_eq!(resp, b"+OK\r\n");

    // 认证后 PING 放行
    resp.clear();
    feed(&mut consumer, b"*1\r\n$4\r\nPING\r\n", &mut resp);
    assert_eq!(resp, b"+PONG\r\n");

    aok::OK
  })
}
