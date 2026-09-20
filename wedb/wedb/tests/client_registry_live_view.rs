//! 注册表条目对「他者会话」动态字段的实时可见性专项（对标 r4-client 问题 1）
//!
//! C# `CLIENT LIST/KILL` 经 `garnetServer.ActiveConsumers().OfType<RespServerSession>()`
//! 拿到目标会话本体后直读其 `activeDbId` / `_userHandle` / `respProtocolVersion`
//! / `isSubscriptionSession` 活字段（`libs/server/Resp/BasicCommands.cs:WriteClientInfo`、
//! `libs/server/Resp/ClientCommands.cs:IsMatch`），任意时刻枚举即真值。
//!
//! 本文件用真 socket 双会话击穿 rust 侧「镜像只在 CLIENT 族执行点自刷新」的陈旧
//! 视图缺陷：会话 A 执行 SELECT / SUBSCRIBE / HELLO 后全程不跑任何 CLIENT 命令，
//! 会话 B 的 CLIENT LIST 仍须看到 A 的真实 db / flags / resp / user，CLIENT KILL
//! 的 TYPE PUBSUB 与 USER default 过滤亦须命中 A。改动前 A 行恒为注册时默认值
//! （db=0 / flags=N / resp=2 / user 缺省），用例必红。

use std::{
  str::from_utf8,
  sync::{Arc, Mutex},
};

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wnode::{
  resp::{RespSessionConsumer, resp_server_session::RespServerSessionOptions},
  service::StorageSessionProvider,
};
use wnode_test::{cmd, read_reply, send_cmd, start_server};
use wtest_base::test_store_config;

/// 进程级注册表为全局单例；本文件多用例串行执行（nextest 按进程隔离，此处兜底
/// libtest 线程并发）
static REGISTRY_LOCK: Mutex<()> = Mutex::new(());

/// bulk 帧载荷解析（作用于 cmd 已收整帧字节 `$N\r\n<body>\r\n`）
fn bulk(frame: &[u8]) -> String {
  assert!(frame.starts_with(b"$"), "期望 bulk 帧: {frame:?}");
  let nl = frame.iter().position(|&b| b == b'\n').expect("bulk header");
  let len: usize = from_utf8(&frame[1..nl - 1])
    .expect("len utf8")
    .parse()
    .expect("len");
  String::from_utf8(frame[nl + 1..nl + 1 + len].to_vec()).expect("body utf8")
}

/// 简单整数应答解析（`:N\r\n`）
fn int(frame: &[u8]) -> i64 {
  assert!(frame.starts_with(b":"), "期望整数帧: {frame:?}");
  from_utf8(&frame[1..frame.len() - 2])
    .expect("int utf8")
    .parse()
    .expect("int")
}

/// 建一台默认装配的单机服务器（注册表进程级安装 + PubSub 默认启用）并注入
/// 会话工厂
macro_rules! spawn_server {
  ($dir:expr) => {{
    let provider = Arc::new(StorageSessionProvider::open_with_config(
      test_store_config(),
      $dir.path().join("node.db"),
      |sender_id, api| {
        Some(RespSessionConsumer::new(
          sender_id,
          RespServerSessionOptions::default(),
          Arc::new(api),
        ))
      },
    )?);
    start_server(provider.clone())
  }};
}

/// A 依次改动态字段后，B 的 CLIENT LIST 必须看到 A 的真实 db / flags / resp / user
#[test]
fn client_list_reflects_cross_session_dynamic_state() -> aok::Result<()> {
  let _lock = REGISTRY_LOCK.lock().unwrap();
  let dir = tempdir()?;
  let (server, addr) = spawn_server!(dir);
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut a = TcpStream::connect(addr).await?;
    let mut b = TcpStream::connect(addr).await?;
    // 预热：泵在建连后首个数据帧才创建并注册会话
    assert_eq!(cmd(&mut a, &[b"PING"]).await, b"+PONG\r\n");
    assert_eq!(cmd(&mut b, &[b"PING"]).await, b"+PONG\r\n");
    // A 取自身 id 供 LIST 定位
    let a_id = int(&cmd(&mut a, &[b"CLIENT", b"ID"]).await);

    // A 全程不跑 CLIENT：先 HELLO 3（升协议），再 SELECT 5，再 SUBSCRIBE（转订阅态）
    let _ = cmd(&mut a, &[b"HELLO", b"3"]).await;
    assert_eq!(cmd(&mut a, &[b"SELECT", b"5"]).await, b"+OK\r\n");
    let _ = cmd(&mut a, &[b"SUBSCRIBE", b"ch"]).await;
    // 屏障：A 再发一条 PING，其应答抵达即保证前序各批的注册表发布已完成
    let _ = cmd(&mut a, &[b"PING"]).await;

    // B 的 LIST 必须看到 A 的真实动态字段（改动前恒为 db=0 / resp=2 / flags=N / 无 user）
    send_cmd(&mut b, &[b"CLIENT", b"LIST"]).await?;
    let list = bulk(&read_reply(&mut b).await);
    let line_a = list
      .split('\n')
      .find(|l| l.starts_with(&format!("id={a_id} ")))
      .unwrap_or_else(|| panic!("LIST 未含 A 行: {list}"));
    assert!(
      line_a.contains(" db=5"),
      "A 行须反映 SELECT 5 的真实库: {line_a}"
    );
    assert!(
      line_a.contains(" resp=3"),
      "A 行须反映 HELLO 3 的真实协议: {line_a}"
    );
    assert!(
      line_a.contains(" flags=P"),
      "A 行须反映 SUBSCRIBE 后的 PUBSUB flags: {line_a}"
    );
    assert!(
      line_a.contains(" user=default"),
      "A 行须反映默认认证用户: {line_a}"
    );
    Ok::<(), aok::Error>(())
  })?;
  server.stop();
  Ok(())
}

/// 订阅态的 A（不跑 CLIENT），B 发起 CLIENT KILL TYPE PUBSUB 必须命中并关闭之
#[test]
fn client_kill_type_pubsub_matches_cross_session_subscriber() -> aok::Result<()> {
  let _lock = REGISTRY_LOCK.lock().unwrap();
  let dir = tempdir()?;
  let (server, addr) = spawn_server!(dir);
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut a = TcpStream::connect(addr).await?;
    let mut b = TcpStream::connect(addr).await?;
    assert_eq!(cmd(&mut a, &[b"PING"]).await, b"+PONG\r\n");
    assert_eq!(cmd(&mut b, &[b"PING"]).await, b"+PONG\r\n");

    // A 订阅但从不跑 CLIENT；屏障 PING 保证订阅态发布完成
    let sub_ack = cmd(&mut a, &[b"SUBSCRIBE", b"ch"]).await;
    assert!(sub_ack.starts_with(b"*3"), "SUBSCRIBE 确认帧: {sub_ack:?}");
    let _ = cmd(&mut a, &[b"PING"]).await;

    // B 按 TYPE PUBSUB 下杀：改动前 A 镜像 client_type=Normal，过滤不中，killed=0
    let killed = int(&cmd(&mut b, &[b"CLIENT", b"KILL", b"TYPE", b"PUBSUB"]).await);
    assert_eq!(killed, 1, "TYPE PUBSUB 须命中订阅态的 A");
    // A 被服务端关闭（挂起读被取消）
    assert!(read_reply(&mut a).await.is_empty(), "A 连接应被 KILL 关闭");
    Ok::<(), aok::Error>(())
  })?;
  server.stop();
  Ok(())
}

/// 默认用户的 A（不跑 CLIENT），B 发起 CLIENT KILL USER default 必须命中并关闭之
#[test]
fn client_kill_user_default_matches_cross_session_default_user() -> aok::Result<()> {
  let _lock = REGISTRY_LOCK.lock().unwrap();
  let dir = tempdir()?;
  let (server, addr) = spawn_server!(dir);
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut a = TcpStream::connect(addr).await?;
    let mut b = TcpStream::connect(addr).await?;
    assert_eq!(cmd(&mut a, &[b"PING"]).await, b"+PONG\r\n");
    assert_eq!(cmd(&mut b, &[b"PING"]).await, b"+PONG\r\n");

    // A 改库但从不跑 CLIENT；屏障 PING 保证视图发布完成
    assert_eq!(cmd(&mut a, &[b"SELECT", b"3"]).await, b"+OK\r\n");
    assert_eq!(cmd(&mut a, &[b"PING"]).await, b"+PONG\r\n");

    // B 按 USER default 下杀（SKIPME 默认跳过自身 B）：改动前 A 镜像 user=None，
    // 过滤不中，killed=0
    let killed = int(&cmd(&mut b, &[b"CLIENT", b"KILL", b"USER", b"default"]).await);
    assert_eq!(killed, 1, "USER default 须命中默认用户的 A");
    assert!(read_reply(&mut a).await.is_empty(), "A 连接应被 KILL 关闭");
    Ok::<(), aok::Error>(())
  })?;
  server.stop();
  Ok(())
}
