#![cfg(feature = "tls")]

//! TLS 收场尾帧（close_notify）有界收口集成测试
//! （回归 task/ing/wnode-tls-teardown-shutdown-tail-unbounded-defers-unregister）
//!
//! 在 garnet 中的相对路径： test/standalone/Garnet.test/RespTlsTests.cs
//!
//! 对标面：C# TcpNetworkHandlerBase.cs:148-170 Dispose——Shutdown/Close syscall
//! 先行、DisposeImpl 注销无条件跟进，收场链上不存在可被对端窗口卡住的异步等
//! 待点；rust TLS 臂 close_notify 须经 rustls poll_close 把发送队列写尽 socket
//! 才返回，黑洞对端（发送缓冲满且持续不读）令收场尾永久 Pending、dispose 永不
//! 执行——注册表幽灵条目 / 幽灵订阅 / 容量守卫三泄漏。修复后收场尾 shutdown
//! 挂 KILL 令牌与确定性超时双边界（口径同 TLS 握手臂）：KILL 先行时令牌已触发
//! 即刻弃帧不吃超时界，超时兜底弃帧，弃帧不弃注销。
//!
//! 收场超时生产默认 1s 对测试过长，经
//! [`GarnetServer::with_tls_shutdown_timeout`] 注入短值收敛等待（该装配位仅
//! 测试使用，ServerBootstrap 标准启动路径恒取缺省值）。

use std::{
  io::{Error, ErrorKind},
  net::SocketAddr,
  num::NonZeroUsize,
  path::PathBuf,
  str::from_utf8,
  sync::{Arc, Mutex},
  time::{Duration, Instant},
};

use compio::{
  BufResult,
  io::{AsyncWrite, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
  time::{sleep, timeout},
};
use compio_tls::TlsConnector;
use socket2::SockRef;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::StoreConfig;
use wnode::{
  GarnetServer,
  resp::{
    RespSessionConsumer, garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions,
  },
  service::StorageSessionProvider,
};
use wnode_test::{read_reply, send_cmd};
use wnode_tls_test::{test_connector, test_server_tls};
use wtest_base::{resp_frame, test_store_config};

/// 测试注入的收场尾帧超时：短到快速收敛、长到覆盖令牌臂即时打断之外的超时
/// 兜底臂验证
const TEST_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(200);
/// KILL 后注销收敛预算（注入超时 + 裕量）：防护失效（收场尾无界悬挂回归）时
/// 确定性失败而非死等
const CONVERGE_BUDGET: Duration = Duration::from_secs(2);
/// 全用例有界时限
const TEST_DEADLINE: Duration = Duration::from_secs(30);
/// 黑洞受害端收缓冲（握手流量内、应答洪峰远外）
const VICTIM_RECV_BUF: u32 = 4 * 1024;
/// 受害端管道积压载荷（MB 级 PING 流水线，稳定超过服务端发送缓冲 autotune
/// 上限与受害端收缓冲之和，令服务端写臂卡满后 close_notify 挤不进管道）
const VICTIM_PIPELINE_BYTES: usize = 2 * 1024 * 1024;

/// 本文件测试串行锁（保护进程级 ConsumerRegistry 单例在多线程 test runner 下
/// 隔离执行——两用例各自装配 provider 即覆盖安装全局注册表槽，并行时后装
/// 实例接管 CLIENT LIST/KILL 治理面、先装实例的条目枚举不到，互扰成红；
/// client_commands_tests.rs 的 CLIENT_TESTS_LOCK 同款形态）
static TLS_TAIL_TESTS_LOCK: Mutex<()> = Mutex::new(());

type Factory =
  Box<dyn Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer> + Send + Sync>;
type TestProvider = StorageSessionProvider<Factory>;

/// 打开真实 RESP 节点基座（CLIENT KILL/LIST 命令面 + 注册表观测面；
/// client_commands_tests 同形装配）
fn open_provider(db_path: PathBuf, config: StoreConfig) -> aok::Result<TestProvider> {
  let factory: Factory = Box::new(|sender_id, api| {
    Some(RespSessionConsumer::new(
      sender_id,
      RespServerSessionOptions::default(),
      Arc::new(api),
    ))
  });
  Ok(StorageSessionProvider::open_with_config(
    config, db_path, factory,
  )?)
}

/// TLS 流上写出 RESP array 命令帧
///
/// 写后必须 `flush`：compio_tls 客户端流的底层是带用户态写缓冲的
/// AsyncStream（BufWriter 语义），`write_all` 只入缓冲、不满不上 socket——
/// 无 flush 时命令帧滞留客户端进程内永不发出，对端收不到任何字节（明文
/// TcpStream 的 write_all 直入内核无此形态，[`wnode_test::send_cmd`] 与
/// 本函数不同形的根因）。`wnode_tls_test::ping` 的 write+flush 序为同一
/// 事实面的既有正确写法
async fn tls_send_cmd<S: AsyncWrite + Unpin>(stream: &mut S, args: &[&[u8]]) -> Result<(), Error> {
  let frame = resp_frame(args);
  let BufResult(res, _) = stream.write_all(frame).await;
  res?;
  stream.flush().await
}

/// bulk 帧载荷断言解析器（作用于已收 frame 字节）
fn bulk_body(frame: &[u8]) -> String {
  let nl = frame.iter().position(|&b| b == b'\n').expect("bulk header");
  let len: usize = from_utf8(&frame[1..nl - 1])
    .expect("utf8 len")
    .parse()
    .expect("bulk len");
  String::from_utf8(frame[nl + 1..nl + 1 + len].to_vec()).expect("bulk body")
}

/// 整数应答断言解析器（:N\r\n → i64）
fn int_reply(frame: &[u8]) -> i64 {
  from_utf8(&frame[1..frame.len() - 2])
    .expect("utf8 int")
    .parse()
    .expect("int reply")
}

/// 轮询谓词至真（CONVERGE_BUDGET 内每 10ms 一轮；耗尽返回末次观测值时刻的
/// false 交调用点断言——注册/注销收敛均为异步）
async fn wait_pred(done: impl Fn() -> bool) -> bool {
  let rounds = CONVERGE_BUDGET.as_millis() / 10;
  for _ in 0..rounds {
    if done() {
      return true;
    }
    sleep(Duration::from_millis(10)).await;
  }
  done()
}

/// TLS 黑洞对端 + CLIENT KILL：收场尾 close_notify 无界时 dispose 永不执行
/// （条目永驻、CLIENT LIST 幽灵行、disposed 计数不跟进——现码红形态）；
/// 修复后 KILL 令牌即刻打断收场尾、注入超时兜底，弃帧不弃注销
#[test]
fn test_tls_shutdown_tail_blackhole_kill_disposes_registry_entry() -> aok::Result<()> {
  let _lock = TLS_TAIL_TESTS_LOCK.lock();
  let dir = tempdir()?;
  let provider = Arc::new(open_provider(
    dir.path().join("node.db"),
    test_store_config(),
  )?);
  let server = GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, 8, provider.clone())?
    .with_tls_config(test_server_tls()?)
    .with_tls_shutdown_timeout(TEST_SHUTDOWN_TIMEOUT);
  server.start(NonZeroUsize::new(1))?;
  let addr = server.local_addr()?;
  let connector = test_connector()?;

  let rt = Runtime::new()?;
  rt.block_on(async {
    timeout(
      TEST_DEADLINE,
      run_blackhole_kill(addr, &provider, &connector),
    )
    .await
    .map_err(|_| Error::new(ErrorKind::TimedOut, "用例整体时限已到"))??;
    Ok::<(), Error>(())
  })?;
  server.stop();
  Ok(())
}

async fn run_blackhole_kill(
  addr: SocketAddr,
  provider: &Arc<TestProvider>,
  connector: &TlsConnector,
) -> Result<(), Error> {
  let registry = &provider.registry;

  // 1. 控制连接（KILL 与 CLIENT LIST 观测面）
  let ctrl_tcp = TcpStream::connect(addr).await?;
  let mut ctrl = connector.connect("localhost", ctrl_tcp).await?;
  tls_send_cmd(&mut ctrl, &[b"PING"]).await?;
  if read_reply(&mut ctrl).await != b"+PONG\r\n" {
    return Err(Error::other("控制连接预热应答非 +PONG"));
  }

  // 2. 黑洞受害连接：收缓冲压小 → TLS 握手（流量在缓冲内）→ 只写不读
  let victim_tcp = TcpStream::connect(addr).await?;
  SockRef::from(&victim_tcp).set_recv_buffer_size(VICTIM_RECV_BUF as usize)?;
  let victim_port = victim_tcp.local_addr()?.port();
  let mut victim = connector.connect("localhost", victim_tcp).await?;

  // 3. 管道积压：MB 级 PING 流水线，受害端不读任何应答；写满自身发送侧即止
  //    （服务端读臂逐批消费、应答洪峰挤满发送管道，写臂随即卡死在满管道上）。
  //    写超时打断后受害连接保持存活——dispose 前服务端读不到 EOF/RST，唯一
  //    退出路径即 KILL 令牌，与生产慢订阅者被 CLIENT KILL 命中同构
  let mut pipeline = Vec::with_capacity(VICTIM_PIPELINE_BYTES);
  while pipeline.len() < VICTIM_PIPELINE_BYTES {
    pipeline.extend_from_slice(b"PING\r\n");
  }
  let _ = timeout(CONVERGE_BUDGET, victim.write_all(pipeline)).await;
  // 服务端消化至写臂卡点（读臂与写臂交替推进，需数轮批处理）
  sleep(Duration::from_millis(300)).await;

  // 4. 两连接均在册（预注册条目 accept 即入治理面）
  if registry.active_consumers().len() != 2 {
    return Err(Error::other("两连接应均在注册表在册"));
  }
  let (_, disposed_before, _) = registry.connection_totals();

  // 5. CLIENT LIST 定位受害条目 id（addr 端口区分两连接）
  tls_send_cmd(&mut ctrl, &[b"CLIENT", b"LIST"]).await?;
  let list = bulk_body(&read_reply(&mut ctrl).await);
  let victim_line = list
    .lines()
    .find(|l| l.contains(&format!("addr=127.0.0.1:{victim_port}")))
    .ok_or_else(|| Error::other("CLIENT LIST 未见受害连接条目"))?;
  let victim_id: i64 = victim_line
    .split("id=")
    .nth(1)
    .expect("id 字段")
    .split(' ')
    .next()
    .expect("id 值")
    .parse()
    .expect("id 整数");

  // 6. CLIENT KILL ID：应答杀掉连接数 1
  let killed_at = Instant::now();
  tls_send_cmd(
    &mut ctrl,
    &[b"CLIENT", b"KILL", b"ID", victim_id.to_string().as_bytes()],
  )
  .await?;
  let reply = read_reply(&mut ctrl).await;
  if int_reply(&reply) != 1 {
    return Err(Error::other(format!(
      "CLIENT KILL 应答非 :1，实际 {:?}",
      String::from_utf8_lossy(&reply)
    )));
  }

  // 7. 有界收敛（注入超时 + 裕量内）：条目自 entries 注销——现码红形态为
  //    收场尾裸 shutdown 挂在黑洞管道上永不返回、条目永驻
  if !wait_pred(|| registry.active_consumers().len() == 1).await {
    return Err(Error::other(
      "KILL 后受害条目未在注入超时+裕量内自注册表注销（幽灵条目）",
    ));
  }
  let elapsed = killed_at.elapsed();
  if elapsed > CONVERGE_BUDGET {
    return Err(Error::other(format!(
      "注销收敛耗时 {elapsed:?} 超出注入超时+裕量预算"
    )));
  }

  // 8. CLIENT LIST 无幽灵行
  tls_send_cmd(&mut ctrl, &[b"CLIENT", b"LIST"]).await?;
  let list = bulk_body(&read_reply(&mut ctrl).await);
  if list.contains(&format!("addr=127.0.0.1:{victim_port}")) {
    return Err(Error::other("CLIENT LIST 仍含受害连接幽灵行"));
  }

  // 9. total_connections_disposed 跟进
  let (_, disposed_after, _) = registry.connection_totals();
  if disposed_after != disposed_before + 1 {
    return Err(Error::other(format!(
      "total_connections_disposed 未跟进：{disposed_before} -> {disposed_after}"
    )));
  }

  // 10. 控制连接不受扰（连接任务与命令面健康）
  tls_send_cmd(&mut ctrl, &[b"PING"]).await?;
  if read_reply(&mut ctrl).await != b"+PONG\r\n" {
    return Err(Error::other("KILL 后控制连接应答非 +PONG"));
  }
  drop(victim);
  Ok(())
}

/// 明文回归：收场尾包裹对明文臂零成本、收场序不变（QUIT 主动收场与 EOF
/// 被动收场后条目照常注销、disposed 照常跟进、存活连接不受扰）
#[test]
fn test_plain_shutdown_tail_regression() -> aok::Result<()> {
  let _lock = TLS_TAIL_TESTS_LOCK.lock();
  let dir = tempdir()?;
  let provider = Arc::new(open_provider(
    dir.path().join("node.db"),
    test_store_config(),
  )?);
  let server = GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, 8, provider.clone())?;
  server.start(NonZeroUsize::new(1))?;
  let addr = server.local_addr()?;

  let rt = Runtime::new()?;
  rt.block_on(async {
    timeout(TEST_DEADLINE, run_plain_regression(addr, &provider))
      .await
      .map_err(|_| Error::new(ErrorKind::TimedOut, "用例整体时限已到"))??;
    Ok::<(), Error>(())
  })?;
  server.stop();
  Ok(())
}

async fn run_plain_regression(addr: SocketAddr, provider: &Arc<TestProvider>) -> Result<(), Error> {
  let registry = &provider.registry;

  let mut a = TcpStream::connect(addr).await?;
  let mut b = TcpStream::connect(addr).await?;
  send_cmd(&mut a, &[b"PING"]).await?;
  if read_reply(&mut a).await != b"+PONG\r\n" {
    return Err(Error::other("明文连接 a 预热应答非 +PONG"));
  }
  send_cmd(&mut b, &[b"PING"]).await?;
  if read_reply(&mut b).await != b"+PONG\r\n" {
    return Err(Error::other("明文连接 b 预热应答非 +PONG"));
  }
  if !wait_pred(|| registry.active_consumers().len() == 2).await {
    return Err(Error::other("两明文连接应均在册"));
  }
  let (_, disposed_before, _) = registry.connection_totals();

  // QUIT 主动收场：+OK 发尽 → 收场尾（明文臂 shutdown 即刻就绪）→ dispose 注销
  send_cmd(&mut b, &[b"QUIT"]).await?;
  if read_reply(&mut b).await != b"+OK\r\n" {
    return Err(Error::other("QUIT 应答非 +OK"));
  }
  if !wait_pred(|| registry.active_consumers().len() == 1).await {
    return Err(Error::other("QUIT 后条目未注销（收场序回归）"));
  }
  let (_, disposed_mid, _) = registry.connection_totals();
  if disposed_mid != disposed_before + 1 {
    return Err(Error::other("QUIT 后 disposed 未跟进（收场序回归）"));
  }

  // EOF 被动收场：直接断开 → 读臂 Ok(0) → 同一条收场尾巴
  let mut c = TcpStream::connect(addr).await?;
  send_cmd(&mut c, &[b"PING"]).await?;
  if read_reply(&mut c).await != b"+PONG\r\n" {
    return Err(Error::other("明文连接 c 预热应答非 +PONG"));
  }
  drop(c);
  if !wait_pred(|| registry.active_consumers().len() == 1).await {
    return Err(Error::other("EOF 后条目未注销（收场序回归）"));
  }
  let (_, disposed_end, _) = registry.connection_totals();
  if disposed_end != disposed_before + 2 {
    return Err(Error::other("EOF 后 disposed 未跟进（收场序回归）"));
  }

  // 存活连接不受扰
  send_cmd(&mut a, &[b"PING"]).await?;
  if read_reply(&mut a).await != b"+PONG\r\n" {
    return Err(Error::other("收场回归后存活连接应答非 +PONG"));
  }
  Ok(())
}
