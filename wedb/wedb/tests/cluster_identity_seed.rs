#![recursion_limit = "256"]
//! 集群身份播种端到端回归（票 wconf-enable-cluster-projection-seed-missing）
//!
//! 对标 C# 契约链：Options.cs:923 GetServerOptions 构造段
//! EnableCluster = EnableCluster.GetValueOrDefault() 投影入 serverOptions →
//! RuntimeServerConfig.cs:135 只读槽 cluster-enabled 格式器直读该字段 →
//! StoreWrapper.cs:176 RunId 三叉（EnableCluster ? clusterProvider.GetRunId()
//! : runId）与 GarnetInfoMetrics.cs:71/:155 的 redis_mode / cluster_enabled
//! 全按该位渲染——配置输入直达身份观测面。
//!
//! rust 无 --cluster 布尔旋钮，模式真源即装配入口：run_cluster_server 装配段
//! 以 cluster provider 在位单点播种（boot.rs 全仓唯一置位点），嵌入式/单机
//! 缺省链保持 RuntimeServerOptions::default() 的 false。两章断言：
//! 1. 集群形态（真起 run_cluster_server 生产链 + 真 TCP 客户端）：CONFIG GET
//!    cluster-enabled = yes、INFO SERVER redis_mode = cluster 且 run_id 取
//!    provider 复制 id（rm.primary_repl_id 随机 40hex，非进程级串）、INFO
//!    CLUSTER cluster_enabled = 1、INFO STATS 含 gossip 段行；
//! 2. 单机缺省链回归（StorageSessionProvider::open_with_config 嵌入式形态，
//!    service 缺省构造位同源）：四面 no / standalone / 0 / 进程级 run_id，
//!    INFO STATS 无 gossip 段行。
//!
//! run_id 双章同进程互证：集群节点报 provider 串、单机会话报进程级
//! OnceLock 串（info_provider::run_id 直比），一红即锁播种位回归。

use std::{
  net,
  net::TcpListener,
  str::from_utf8,
  sync::Arc,
  thread::{sleep, spawn},
  time::{Duration, Instant},
};

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wconf::ConfigFileArgs;
use wedb::{ClusterArgs, server::boot::run_cluster_server};
use wnode::{
  RespSessionConsumer, ShutdownCoordinator,
  resp::{info_provider::run_id, resp_server_session::RespServerSessionOptions},
  service::StorageSessionProvider,
};
use wnode_test::{cmd, read_reply, send_cmd, start_server};
use wtest_base::test_store_config;

/// 取一个当前空闲的回环端口（bind :0 后释放；窗口竞态由测试环境低并发兜底）
fn free_port() -> u16 {
  let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
  let port = l.local_addr().expect("local addr").port();
  drop(l);
  port
}

/// 阻塞等待端口可连（服务端 accept 就绪；10s 上限防挂死）
fn wait_port_open(port: u16) {
  let deadline = Instant::now() + Duration::from_secs(10);
  while net::TcpStream::connect(("127.0.0.1", port)).is_err() {
    assert!(
      Instant::now() < deadline,
      "集群端点 10s 内未就绪 (127.0.0.1:{port})"
    );
    sleep(Duration::from_millis(50));
  }
}

/// INFO <section> 往返并剥出 RESP2 bulk 载荷（同步直返段，非扫描族慢路径）
async fn info_section(stream: &mut TcpStream, section: &str) -> String {
  send_cmd(stream, &[b"INFO", section.as_bytes()])
    .await
    .expect("send INFO");
  let out = read_reply(stream).await;
  let text = from_utf8(&out).expect("INFO 应答应为 UTF-8 文本");
  let (len_s, rest) = text
    .strip_prefix('$')
    .unwrap_or_else(|| panic!("非 RESP2 bulk 帧: {out:?}"))
    .split_once("\r\n")
    .expect("bulk 帧头");
  let body = rest.strip_suffix("\r\n").expect("bulk 帧尾");
  assert_eq!(
    len_s.parse::<usize>().expect("bulk 长度"),
    body.len(),
    "bulk 长度字节与载荷实长不符"
  );
  body.to_string()
}

/// INFO 段体内取 `name:value` 行的值
fn row_value(body: &str, name: &str) -> String {
  body
    .split("\r\n")
    .find(|l| l.starts_with(name))
    .and_then(|l| l.split_once(':'))
    .map(|(_, v)| v.to_owned())
    .unwrap_or_else(|| panic!("缺 {name} 行: {body}"))
}

/// 集群形态四章：生产二进制装配链（run_cluster_server 真起进程内节点）
/// 自报 cluster 身份——CONFIG GET / INFO SERVER / INFO CLUSTER / INFO STATS
/// 四观测面全部按播种位渲染（改动前恒 no / standalone / 0 / 进程级 run_id /
/// 无 gossip 段，用例必红）
#[test]
fn run_cluster_server_seeds_cluster_identity() {
  let dir = tempdir().expect("tempdir");
  let port = free_port();
  let args = ClusterArgs::from_args_iter([
    "wedb",
    "--port",
    &port.to_string(),
    "--dir",
    dir.path().to_str().expect("dir utf-8"),
    // gossip 静默档：单节点裸启动无 meet 对端，长周期避免无关出站流量
    "--gossip-delay-secs",
    "3600",
  ])
  .expect("parse cluster args");

  let coordinator = ShutdownCoordinator::new();
  let coord = coordinator.clone();
  let handle = spawn(move || run_cluster_server(args, Some(coord)));

  wait_port_open(port);
  let rt = Runtime::new().expect("compio runtime");
  rt.block_on(async {
    let mut s = TcpStream::connect(("127.0.0.1", port))
      .await
      .expect("connect cluster endpoint");
    // 预热：泵在建连后首个数据帧才创建会话
    assert_eq!(cmd(&mut s, &[b"PING"]).await, b"+PONG\r\n");

    // 一：CONFIG GET cluster-enabled = yes（只读槽格式器直读播种位）
    let reply = cmd(&mut s, &[b"CONFIG", b"GET", b"cluster-enabled"]).await;
    assert!(
      reply.starts_with(b"*2\r\n") && reply.ends_with(b"$3\r\nyes\r\n"),
      "cluster-enabled 应答应为一对 name/value 且值 yes: {reply:?}"
    );

    // 二：INFO SERVER redis_mode = cluster，run_id 取 provider 复制 id
    //（StoreWrapper.cs:176 三叉的集群臂：40hex 随机串 != 进程级 OnceLock 串）
    let server = info_section(&mut s, "SERVER").await;
    assert!(
      server.contains("redis_mode:cluster"),
      "redis_mode 应为 cluster: {server}"
    );
    let node_run_id = row_value(&server, "run_id");
    assert_eq!(
      node_run_id.len(),
      40,
      "集群 run_id 应为 40hex: {node_run_id}"
    );
    assert_ne!(
      node_run_id,
      run_id(),
      "集群 run_id 须取 provider 复制 id，不得回落进程级串"
    );

    // 三：INFO CLUSTER cluster_enabled = 1
    let cluster = info_section(&mut s, "CLUSTER").await;
    assert!(
      cluster.contains("cluster_enabled:1"),
      "cluster_enabled 应为 1: {cluster}"
    );

    // 四：INFO STATS 含 gossip 段行（facts.enable_cluster 门放行；
    // 缺省形态监视器缺席逐行折零，行在场即可）
    let stats = info_section(&mut s, "STATS").await;
    assert!(
      stats.contains("meet_requests_recv:"),
      "INFO STATS 应含 gossip 段行: {stats}"
    );
  });

  coordinator.stop();
  let res = handle.join().expect("集群线程异常退出");
  assert!(res.is_ok(), "集群节点应优雅退出: {res:?}");
}

/// 单机缺省链四章回归：嵌入式形态（service 缺省构造位同源的
/// RuntimeServerOptions::default()）四面全按 false 渲染——no / standalone /
/// 0 / 进程级 run_id（同进程 OnceLock 直比相等），INFO STATS 无 gossip 段
#[test]
fn standalone_default_chain_keeps_process_identity() {
  let dir = tempdir().expect("tempdir");
  let provider = Arc::new(
    StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join("node.db"),
      |sender_id, api| {
        Some(RespSessionConsumer::new(
          sender_id,
          RespServerSessionOptions::default(),
          Arc::new(api),
        ))
      },
    )
    .expect("open standalone provider"),
  );
  let (_server, addr) = start_server(provider);

  let rt = Runtime::new().expect("compio runtime");
  rt.block_on(async {
    let mut s = TcpStream::connect(addr).await.expect("connect standalone");
    assert_eq!(cmd(&mut s, &[b"PING"]).await, b"+PONG\r\n");

    let reply = cmd(&mut s, &[b"CONFIG", b"GET", b"cluster-enabled"]).await;
    assert!(
      reply.starts_with(b"*2\r\n") && reply.ends_with(b"$2\r\nno\r\n"),
      "单机 cluster-enabled 应答值应为 no: {reply:?}"
    );

    let server = info_section(&mut s, "SERVER").await;
    assert!(
      server.contains("redis_mode:standalone"),
      "单机 redis_mode 应为 standalone: {server}"
    );
    assert_eq!(
      row_value(&server, "run_id"),
      run_id(),
      "单机 run_id 应为进程级串（与集群章互证播种位不越界）"
    );

    let cluster = info_section(&mut s, "CLUSTER").await;
    assert!(
      cluster.contains("cluster_enabled:0"),
      "单机 cluster_enabled 应为 0: {cluster}"
    );

    let stats = info_section(&mut s, "STATS").await;
    assert!(
      !stats.contains("meet_requests_recv:"),
      "单机 INFO STATS 不应含 gossip 段行: {stats}"
    );
  });
}
