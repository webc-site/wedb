//! CLIENT LIST / CLIENT KILL / 监视器采样 端到端对标测试
//!
//! C# 参照 Garnet.test/RespTests/ClientsTests 与 libs/server/Resp/
//! ClientCommands.cs 语义：真实服务器 + 活跃消费者注册表 + 双连接交互；
//! 监视器段对标 GarnetServerMonitor.Start / MainMonitorTaskAsync 的
//! 采样-瞬时吞吐滚动与连接计数承接。
//!
//! 注册表与监视器均为进程级单例（CLIENT 族命令经 global 直取），断言收敛
//! 在单个测试函数内顺序执行，避免并行用例互扰。

use std::{
  future::ready,
  io::Error,
  path::PathBuf,
  str::from_utf8,
  sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
  },
  thread::sleep,
  time::{Duration, Instant},
};

use compio::{buf::BufResult, io::AsyncRead, net::TcpStream, runtime::Runtime, time::timeout};
use parking_lot::Mutex;
use tempfile::tempdir;
use wacl::AccessControlList;
use wdev::SegmentedDevice;
use wkv::StoreConfig;
use wmetric::GarnetServerMonitor;
use wnode::{
  resp::{
    RespSessionConsumer, garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions,
  },
  servers::consumer_registry::ConsumerRegistry,
  service::StorageSessionProvider,
};
use wnode_test::{err_frame, read_reply, send, send_cmd, start_server};
use wresp::cmd_strings::{RESP_ERR_CLIENT_ID_GREATER_THAN_ZERO, RESP_ERR_GENERIC_SYNTAX_ERROR};
use wtest_base::{test_store_config, test_store_config_with_budget};

/// CLIENT 族测试串行锁（保护进程级 ConsumerRegistry 单例在多线程 test runner 下隔离执行）
static CLIENT_TESTS_LOCK: Mutex<()> = Mutex::new(());

/// bulk 帧载荷断言解析器（作用于已收 frame 字节，非 IO 读取——
/// wnode_test::read_bulk_reply 是流级直读，无法对 CLIENT LIST 的
/// 多行文本做逐字段解析，故保留本地字节切片版）
fn bulk_body(frame: &[u8]) -> String {
  let nl = frame.iter().position(|&b| b == b'\n').expect("bulk header");
  let len: usize = from_utf8(&frame[1..nl - 1])
    .expect("utf8 len")
    .parse()
    .expect("bulk len");
  String::from_utf8(frame[nl + 1..nl + 1 + len].to_vec()).expect("utf8 body")
}

/// 整数应答断言解析器（:N\r\n → i64，作用于已收 frame 字节，同 bulk_body）
fn int_reply(frame: &[u8]) -> i64 {
  from_utf8(&frame[1..frame.len() - 2])
    .expect("utf8 int")
    .parse()
    .expect("int reply")
}

type Factory =
  Box<dyn Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer> + Send + Sync>;
type TestProvider = StorageSessionProvider<Factory>;

/// 打开测试节点基座（各用例同形装配块单源：RESP 会话消费者工厂按注入
/// 选项逐连接构造；数据路径、存储配置与会话选项差异项由调用点参数化。
/// 形参持所有权避开 Rust 2024 RPIT 生命周期捕获，保住 start_server 的 'static 界）
fn open_provider(
  db_path: PathBuf,
  config: StoreConfig,
  options: RespServerSessionOptions,
) -> aok::Result<TestProvider> {
  let factory: Factory = Box::new(move |sender_id, api| {
    Some(RespSessionConsumer::new(
      sender_id,
      options.clone(),
      Arc::new(api),
    ))
  });
  Ok(StorageSessionProvider::open_with_config(
    config, db_path, factory,
  )?)
}

/// 轮询谓词至真（每轮 10ms、最多 rounds 轮外加末次复核；耗尽返回 false
/// 交调用点断言——注册/注销收敛均为异步）
fn wait_until(rounds: u32, done: impl Fn() -> bool) -> bool {
  for _ in 0..rounds {
    if done() {
      return true;
    }
    sleep(Duration::from_millis(10));
  }
  done()
}

/// 轮询连接计数至谓词命中或轮次耗尽，返回末次观测值交调用点断言
fn wait_totals(
  registry: &ConsumerRegistry,
  rounds: u32,
  pred: impl Fn((i64, i64, i64)) -> bool,
) -> (i64, i64, i64) {
  let mut totals = registry.connection_totals();
  for _ in 0..rounds {
    if pred(totals) {
      break;
    }
    sleep(Duration::from_millis(10));
    totals = registry.connection_totals();
  }
  totals
}

/// CLIENT ID 取号（会话自报 id 的两步收发单源）
async fn client_id(c: &mut TcpStream) -> Result<i64, Error> {
  send_cmd(c, &[b"CLIENT", b"ID"]).await?;
  Ok(int_reply(&read_reply(c).await))
}

/// CLIENT KILL ID 单杀（应答须为实际杀掉连接数 1）
async fn kill_by_id(c: &mut TcpStream, id: i64, msg: &str) -> Result<(), Error> {
  send_cmd(c, &[b"CLIENT", b"KILL", b"ID", id.to_string().as_bytes()]).await?;
  assert_eq!(int_reply(&read_reply(c).await), 1, "{msg}");
  Ok(())
}

/// 被杀连接挂起读须断开（500ms 内无应答或见 EOF）
async fn assert_socket_closed(c: &mut TcpStream) {
  let read_result = timeout(Duration::from_millis(500), read_reply(c)).await;
  assert!(
    read_result.is_err() || read_result.unwrap().is_empty(),
    "Socket should be closed"
  );
}

/// CLIENT LIST 轮询直至目标 id 行消失（KILL 后异步注销，10ms×50 收敛）
async fn wait_client_gone(c: &mut TcpStream, target: &str) -> Result<(), Error> {
  let mut cleared = false;
  for _ in 0..50 {
    send_cmd(c, &[b"CLIENT", b"LIST"]).await?;
    let list = bulk_body(&read_reply(c).await);
    if !list.contains(target) {
      cleared = true;
      break;
    }
    sleep(Duration::from_millis(10));
  }
  assert!(cleared, "Client a should be removed from client list");
  Ok(())
}

/// 从 CLIENT LIST 单行提取 addr 字段值（断言解析器，作用于单行文本）
fn addr_of(line: &str) -> &str {
  line
    .split(" addr=")
    .nth(1)
    .expect("addr field")
    .split(' ')
    .next()
    .expect("addr value")
}

/// 驱动 N 轮监视器采样（复位回调与宿主装配同构：
/// [`ConsumerRegistry::monitor_iteration_inputs`]，C# 监视器直查 servers 的承接）
async fn run_sampling_rounds(
  monitor: &GarnetServerMonitor,
  registry: &Arc<ConsumerRegistry>,
  rounds: u32,
) {
  let done = Arc::new(AtomicU32::new(0));
  let done_cancel = Arc::clone(&done);
  monitor
    .main_monitor_task_async(
      |_duration| ready(()),
      move || done_cancel.load(Ordering::Relaxed) >= rounds,
      || {
        done.fetch_add(1, Ordering::Relaxed);
        // gossip 与复活化两臂本用例不观测（宿主装配点由
        // server_monitor_tests 的复位臂用例覆盖），此处注入空操作
        registry.monitor_iteration_inputs(|| {}, || {})
      },
    )
    .await;
}

#[test]
fn client_list_kill_and_monitor_flow() -> aok::Result<()> {
  let _lock = CLIENT_TESTS_LOCK.lock();
  let dir = tempdir()?;
  // 采样走真实轨：装配期按 secs>0 建句柄并经 attach_session_metrics 注入，
  // 会话与存储执行域共持同一 Arc（替代已删的选项侧布尔死轨）
  let provider = Arc::new(
    open_provider(
      dir.path().join("node.db"),
      test_store_config(),
      RespServerSessionOptions::default(),
    )?
    .with_metrics_sampling_frequency_secs(1),
  );

  let (server, addr) = start_server(provider.clone());

  // 监视器进程级安装（C# StoreWrapper 构造 monitor + Start；频率 1s 作
  // 瞬时吞吐折算基准），未装配时 dispose 归并为无害空操作
  let monitor = Arc::new(GarnetServerMonitor::new(1, true, false, false));
  assert!(monitor.install_global(), "监视器进程级安装");

  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut a = TcpStream::connect(addr).await?;
    let mut b = TcpStream::connect(addr).await?;

    // 预热：泵在建连后首个数据帧才创建并注册会话
    send_cmd(&mut a, &[b"PING"]).await?;
    assert_eq!(read_reply(&mut a).await, b"+PONG\r\n");
    send_cmd(&mut b, &[b"PING"]).await?;
    assert_eq!(read_reply(&mut b).await, b"+PONG\r\n");

    // 命名（CLIENT SETNAME → 注册表条目视图自刷新）
    send_cmd(&mut a, &[b"CLIENT", b"SETNAME", b"alpha"]).await?;
    assert_eq!(read_reply(&mut a).await, b"+OK\r\n");
    send_cmd(&mut b, &[b"CLIENT", b"SETNAME", b"beta"]).await?;
    assert_eq!(read_reply(&mut b).await, b"+OK\r\n");

    // CLIENT LIST：两条记录，字段序对齐 C# WriteClientInfo
    send_cmd(&mut a, &[b"CLIENT", b"LIST"]).await?;
    let lines: Vec<String> = bulk_body(&read_reply(&mut a).await)
      .trim_end_matches('\n')
      .split('\n')
      .map(str::to_string)
      .collect();
    assert_eq!(lines.len(), 2);
    let alpha = lines
      .iter()
      .find(|l| l.contains("name=alpha"))
      .expect("alpha");
    assert!(alpha.contains("id="));
    assert!(alpha.contains(" addr=127.0.0.1:"));
    assert!(alpha.contains(" laddr=127.0.0.1:"));
    assert!(alpha.contains(" age="));
    // C# WriteClientInfo 恒带 user 字段（GetDefaultUserHandle；NoAuth 档
    // 会话经构造期 AuthenticateUser 落到 default 用户）
    assert!(alpha.contains(" user=default"));
    assert!(alpha.contains(" flags=N"));
    assert!(alpha.contains(" db=0"));
    assert!(alpha.contains(" resp=2"));
    assert!(alpha.contains(" lib-name= lib-ver="));
    assert!(
      lines
        .iter()
        .any(|l| l.contains("name=beta") && l.contains("flags=N"))
    );
    let a_addr = addr_of(alpha).to_string();

    // TYPE 过滤（normal 双双命中）
    send_cmd(&mut a, &[b"CLIENT", b"LIST", b"TYPE", b"normal"]).await?;
    assert_eq!(
      bulk_body(&read_reply(&mut a).await)
        .trim_end_matches('\n')
        .split('\n')
        .count(),
      2
    );

    // SLAVE 于 CLIENT LIST 非法（原始大小写保留进文案）
    send_cmd(&mut a, &[b"CLIENT", b"LIST", b"TYPE", b"slave"]).await?;
    assert_eq!(
      read_reply(&mut a).await,
      b"-ERR Unknown client type 'slave'\r\n"
    );

    // 参数错误面（C# 文案逐字对齐）
    send_cmd(&mut a, &[b"CLIENT", b"KILL"]).await?;
    assert_eq!(
      read_reply(&mut a).await,
      b"-ERR wrong number of arguments for 'CLIENT|KILL' command\r\n"
    );
    send_cmd(&mut a, &[b"CLIENT", b"KILL", b"ID", b"abc"]).await?;
    assert_eq!(
      read_reply(&mut a).await,
      b"-ERR client-id should be greater than 0\r\n"
    );
    // ID 非正值门（deviations §154 严向收口：C# 解析成功入过滤器恒不匹配回 :0，
    // rust 同帧拒绝；既有 abc 例系解析失败臂双侧同帧，非分叉面）
    send_cmd(&mut a, &[b"CLIENT", b"KILL", b"ID", b"0"]).await?;
    assert_eq!(
      read_reply(&mut a).await,
      err_frame(RESP_ERR_CLIENT_ID_GREATER_THAN_ZERO)
    );
    send_cmd(&mut a, &[b"CLIENT", b"KILL", b"ID", b"-3"]).await?;
    assert_eq!(
      read_reply(&mut a).await,
      err_frame(RESP_ERR_CLIENT_ID_GREATER_THAN_ZERO)
    );
    // 双例仅同帧拒绝，零会话被杀且连接存活（a/b 后续流程即旁证，PING 直钉）
    send_cmd(&mut a, &[b"PING"]).await?;
    assert_eq!(read_reply(&mut a).await, b"+PONG\r\n");
    send_cmd(&mut a, &[b"CLIENT", b"KILL", b"ID", b"1", b"ID", b"2"]).await?;
    assert_eq!(
      read_reply(&mut a).await,
      b"-ERR Filter 'ID' defined multiple times\r\n"
    );
    send_cmd(&mut a, &[b"CLIENT", b"KILL", b"FOO", b"bar"]).await?;
    assert_eq!(
      read_reply(&mut a).await,
      err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR)
    );

    // B 的会话 id（老式 KILL 与 ID 过滤的目标准备）
    let b_id = client_id(&mut b).await?;

    // 老式 ip:port：无匹配 → NO_SUCH_CLIENT
    send_cmd(&mut a, &[b"CLIENT", b"KILL", b"127.0.0.1:1"]).await?;
    assert_eq!(read_reply(&mut a).await, b"-ERR No such client\r\n");

    // ADDR 过滤杀 A（由 B 发起；应答为实际杀掉数）
    send_cmd(&mut b, &[b"CLIENT", b"KILL", b"ADDR", a_addr.as_bytes()]).await?;
    assert_eq!(int_reply(&read_reply(&mut b).await), 1);

    // A 被服务端关闭（挂起读被取消，连接断开）
    assert!(read_reply(&mut a).await.is_empty());

    // LIST 仅剩 beta
    send_cmd(&mut b, &[b"CLIENT", b"LIST"]).await?;
    let remaining = bulk_body(&read_reply(&mut b).await);
    assert!(remaining.contains("name=beta"));
    assert!(!remaining.contains("name=alpha"));

    // ID + SKIPME NO 自杀（默认 SKIPME=yes 时无法命中自身）
    let b_id = b_id.to_string();
    send_cmd(
      &mut b,
      &[b"CLIENT", b"KILL", b"ID", b_id.as_bytes(), b"SKIPME", b"NO"],
    )
    .await?;
    assert_eq!(int_reply(&read_reply(&mut b).await), 1);
    assert!(read_reply(&mut b).await.is_empty());

    // 注销闭环（C# TotalConnectionsReceived/Disposed 语义）
    let totals = wait_totals(&provider.registry, 200, |t| t.1 >= 2);
    assert_eq!(totals, (2, 2, 0));

    // —— 监视器采样链路（C# MainMonitorTaskAsync + UpdateInstantaneousMetrics）——
    // 基线：连接 c 一条 PING，泵镜像回包后首轮采样落基线
    let mut c = TcpStream::connect(addr).await?;
    send_cmd(&mut c, &[b"PING"]).await?;
    assert_eq!(read_reply(&mut c).await, b"+PONG\r\n");
    run_sampling_rounds(&monitor, &provider.registry, 1).await;

    // 窗口内恰 5 条命令：单轮采样后瞬时 ops/s 精确滚动（频率 1s 折算），
    // 连接计数与网络字节随采样承接
    for _ in 0..5 {
      send(&mut c, b"PING\r\n").await?;
      assert_eq!(read_reply(&mut c).await, b"+PONG\r\n");
    }
    run_sampling_rounds(&monitor, &provider.registry, 1).await;
    let snap = monitor.snapshot().expect("采样轮装配全局指标快照");
    assert_eq!(snap.instantaneous_cmd_per_sec, 5.0);
    assert!(snap.instantaneous_net_input_tpt > 0.0);
    assert!(snap.instantaneous_net_output_tpt > 0.0);

    // 释放 c：注销后采样轮承接连接计数（dispose 归并已并入历史）
    drop(c);
    let totals = wait_totals(&provider.registry, 200, |t| t == (3, 3, 0));
    assert_eq!(totals, (3, 3, 0));
    run_sampling_rounds(&monitor, &provider.registry, 1).await;
    let snap = monitor.snapshot().expect("采样轮装配全局指标快照");
    assert_eq!(
      (
        snap.total_connections_received,
        snap.total_connections_disposed,
        snap.total_connections_active
      ),
      (3, 3, 0)
    );
    Ok::<(), Error>(())
  })?;

  server.stop();
  Ok(())
}

/// deviations §154 防外溢锁：MAXAGE 臂双侧同收敛无值域门（C# 仅 TryReadLong
/// 失败回 syntax error，rust 同款仅 strict_i64）——负值解析成功入过滤器照常
/// 参与匹配（age > maxAge 对非负 age 恒真），唯一会话系自身被 SKIPME 默认
/// true 排除回 :0；若误仿 ID 臂加值域门即错帧现形。正对照 CLIENT KILL ID
/// <自身ID> 走合法解析路径同样回 :0，证明非正值门零外溢
#[test]
fn client_kill_nonpositive_id_and_maxage_negative_gates() -> aok::Result<()> {
  let _lock = CLIENT_TESTS_LOCK.lock();
  let dir = tempdir()?;
  let provider = Arc::new(open_provider(
    dir.path().join("node.db"),
    test_store_config(),
    RespServerSessionOptions::default(),
  )?);

  let (server, addr) = start_server(provider);
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut c = TcpStream::connect(addr).await?;
    send_cmd(&mut c, &[b"PING"]).await?;
    assert_eq!(read_reply(&mut c).await, b"+PONG\r\n");

    // MAXAGE 负值形：收整数帧非错误帧（无值域拒，钉双侧同收敛现状）
    send_cmd(&mut c, &[b"CLIENT", b"KILL", b"MAXAGE", b"-5"]).await?;
    assert_eq!(read_reply(&mut c).await, b":0\r\n");

    // 正对照：自身 ID 合法解析，SKIPME 默认排除自身回 :0，连接存活零杀
    let own_id = client_id(&mut c).await?;
    send_cmd(
      &mut c,
      &[b"CLIENT", b"KILL", b"ID", own_id.to_string().as_bytes()],
    )
    .await?;
    assert_eq!(int_reply(&read_reply(&mut c).await), 0);
    send_cmd(&mut c, &[b"PING"]).await?;
    assert_eq!(read_reply(&mut c).await, b"+PONG\r\n");

    Ok::<(), Error>(())
  })?;

  server.stop();
  Ok(())
}

/// test/standalone/Garnet.test.acl/Resp/ACL/BasicTests.cs:ClientSetInfoRejectsInvalidAttributeValue
#[test]
fn client_setinfo_rejects_invalid_attribute_value() -> aok::Result<()> {
  let _lock = CLIENT_TESTS_LOCK.lock();
  let dir = tempdir()?;
  let provider = Arc::new(open_provider(
    dir.path().join("node.db"),
    test_store_config(),
    RespServerSessionOptions::default(),
  )?);

  let (server, addr) = start_server(provider);
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut c = TcpStream::connect(addr).await?;

    send_cmd(&mut c, &[b"CLIENT", b"SETINFO", b"LIB-NAME", b"foo\nbar"]).await?;
    assert_eq!(
      read_reply(&mut c).await,
      b"-ERR LIB-NAME cannot contain spaces, newlines or special characters.\r\n"
    );

    send_cmd(&mut c, &[b"CLIENT", b"SETINFO", b"LIB-VER", b"1.0 2.0"]).await?;
    assert_eq!(
      read_reply(&mut c).await,
      b"-ERR LIB-VER cannot contain spaces, newlines or special characters.\r\n"
    );

    send_cmd(
      &mut c,
      &[b"CLIENT", b"SETINFO", b"CRLF-INJ\r\n-ERR injected", b"a b"],
    )
    .await?;
    assert_eq!(read_reply(&mut c).await, b"-ERR syntax error\r\n");

    send_cmd(&mut c, &[b"CLIENT", b"SETINFO", b"LIB-VER", b"\x7f"]).await?;
    assert_eq!(
      read_reply(&mut c).await,
      b"-ERR LIB-VER cannot contain spaces, newlines or special characters.\r\n"
    );

    send_cmd(&mut c, &[b"CLIENT", b"SETINFO", b"LIB-NAME", b"a!~b"]).await?;
    assert_eq!(read_reply(&mut c).await, b"+OK\r\n");
    send_cmd(&mut c, &[b"CLIENT", b"INFO"]).await?;
    let info = bulk_body(&read_reply(&mut c).await);
    assert!(info.contains("lib-name=a!~b"));

    send_cmd(&mut c, &[b"CLIENT", b"SETINFO", b"LIB-NAME", b"my-lib"]).await?;
    assert_eq!(read_reply(&mut c).await, b"+OK\r\n");
    send_cmd(&mut c, &[b"CLIENT", b"SETINFO", b"LIB-VER", b"1.2.3"]).await?;
    assert_eq!(read_reply(&mut c).await, b"+OK\r\n");
    send_cmd(&mut c, &[b"CLIENT", b"INFO"]).await?;
    let info = bulk_body(&read_reply(&mut c).await);
    assert!(info.contains("lib-name=my-lib"));
    assert!(info.contains("lib-ver=1.2.3"));

    send_cmd(&mut c, &[b"CLIENT", b"SETINFO", b"lib-ver", b"9.9.9"]).await?;
    assert_eq!(read_reply(&mut c).await, b"+OK\r\n");
    send_cmd(&mut c, &[b"CLIENT", b"INFO"]).await?;
    let info = bulk_body(&read_reply(&mut c).await);
    assert!(info.contains("lib-ver=9.9.9"));

    send_cmd(&mut c, &[b"CLIENT", b"SETINFO", b"LIB-NAME", b"bad\nvalue"]).await?;
    assert_eq!(
      read_reply(&mut c).await,
      b"-ERR LIB-NAME cannot contain spaces, newlines or special characters.\r\n"
    );
    send_cmd(&mut c, &[b"CLIENT", b"INFO"]).await?;
    let info = bulk_body(&read_reply(&mut c).await);
    assert!(info.contains("lib-name=my-lib"));

    send_cmd(&mut c, &[b"CLIENT", b"SETINFO", b"LIB-NAME", b""]).await?;
    assert_eq!(read_reply(&mut c).await, b"+OK\r\n");
    send_cmd(&mut c, &[b"CLIENT", b"INFO"]).await?;
    let info = bulk_body(&read_reply(&mut c).await);
    assert!(info.contains(" lib-name= "));

    send_cmd(&mut c, &[b"CLIENT", b"LIST"]).await?;
    let list = bulk_body(&read_reply(&mut c).await);
    assert!(!list.contains('\r'));

    Ok::<(), Error>(())
  })?;

  server.stop();
  Ok(())
}

#[test]
fn client_kill_blocked_session() -> aok::Result<()> {
  let _lock = CLIENT_TESTS_LOCK.lock();
  let dir = tempdir()?;
  let provider = Arc::new(open_provider(
    dir.path().join("node.db"),
    test_store_config(),
    RespServerSessionOptions::default(),
  )?);

  let (server, addr) = start_server(provider);
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut a = TcpStream::connect(addr).await?;
    let mut b = TcpStream::connect(addr).await?;

    let a_id = client_id(&mut a).await?;

    send(&mut a, b"*3\r\n$5\r\nBLPOP\r\n$6\r\nno_key\r\n$1\r\n0\r\n").await?;
    sleep(Duration::from_millis(100));

    kill_by_id(&mut b, a_id, "KILL ID 应答须为实际杀掉连接数 1").await?;

    assert_socket_closed(&mut a).await;

    wait_client_gone(&mut b, &format!("id={a_id} ")).await?;

    Ok::<(), Error>(())
  })?;
  server.stop();
  Ok(())
}

#[test]
fn client_kill_eval_blocked_session() -> aok::Result<()> {
  let _lock = CLIENT_TESTS_LOCK.lock();
  let dir = tempdir()?;
  let lua_options = RespServerSessionOptions {
    enable_lua: true,
    ..Default::default()
  };
  let provider = Arc::new(open_provider(
    dir.path().join("node.db"),
    test_store_config(),
    lua_options,
  )?);

  let broker = provider.item_broker();
  let (server, addr) = start_server(provider);
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut a = TcpStream::connect(addr).await?;
    let mut b = TcpStream::connect(addr).await?;

    let a_id = client_id(&mut a).await?;

    // EVAL 内 BLPOP 0：挂起脚本协程
    send_cmd(
      &mut a,
      &[
        b"EVAL",
        b"return redis.call('BLPOP', KEYS[1], 0)",
        b"1",
        b"no_key",
      ],
    )
    .await?;
    sleep(Duration::from_millis(100));

    // 断言经纪内已登记该会话的观察者
    assert!(
      broker.try_get_observer(a_id as usize).is_some(),
      "会话 a 应在经纪中登记观察者"
    );

    kill_by_id(&mut b, a_id, "KILL ID 应答须为实际杀掉连接数 1").await?;

    assert_socket_closed(&mut a).await;

    wait_client_gone(&mut b, &format!("id={a_id} ")).await?;

    // 秒级断言经纪内观察者注销计数归零
    assert!(
      wait_until(50, || broker.try_get_observer(a_id as usize).is_none()),
      "Broker observer should be cleared"
    );

    Ok::<(), Error>(())
  })?;
  server.stop();
  Ok(())
}

#[test]
fn server_shutdown_drains_eval_blocked_session() -> aok::Result<()> {
  let _lock = CLIENT_TESTS_LOCK.lock();
  let dir = tempdir()?;
  let lua_options = RespServerSessionOptions {
    enable_lua: true,
    ..Default::default()
  };
  let provider = Arc::new(open_provider(
    dir.path().join("node.db"),
    test_store_config(),
    lua_options,
  )?);

  let broker = provider.item_broker();
  let (server, addr) = start_server(provider);
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut a = TcpStream::connect(addr).await?;

    let a_id = client_id(&mut a).await?;

    // EVAL 内 BLPOP 0：挂起脚本协程
    send_cmd(
      &mut a,
      &[
        b"EVAL",
        b"return redis.call('BLPOP', KEYS[1], 0)",
        b"1",
        b"no_key",
      ],
    )
    .await?;
    sleep(Duration::from_millis(100));

    // 断言经纪内已登记该会话的观察者
    assert!(
      broker.try_get_observer(a_id as usize).is_some(),
      "会话 a 应在经纪中登记观察者"
    );

    Ok::<(), Error>(())
  })?;

  let t0 = Instant::now();
  server.stop();
  let elapsed = t0.elapsed();
  assert!(
    elapsed < Duration::from_secs(4),
    "停机排空耗时 {:?} 超过 4 秒护栏（未被即刻取消，落到 5 秒超时强收）",
    elapsed
  );

  Ok(())
}

/// 连接注册前移治理面：未发字节的空闲连接（会话未建立）入 CLIENT LIST
/// 且可被 CLIENT KILL 秒断；零字节即断的短命连接进 received/disposed 统计
///
/// 行为原型为 C# GarnetServerTcp.HandleNewConnection（`activeHandlers.TryAdd`
/// 即刻注册先于 handler.Start，TLS 握手与首字节读取均在其内）的 wedb 自有
/// 治理变体，C# 侧无同位测试；实现对位锚点由 wnode::server 的
/// run_tcp_accept_loop 单点持有（rust 以 accept 成功分支预注册承接，
/// 连接接收计数前移到容量门前，短命连接全部进统计）
#[test]
fn idle_connection_before_first_byte_is_listed_and_killable() -> aok::Result<()> {
  let _lock = CLIENT_TESTS_LOCK.lock();
  let dir = tempdir()?;
  let provider = Arc::new(open_provider(
    dir.path().join("node.db"),
    test_store_config(),
    RespServerSessionOptions::default(),
  )?);

  let (server, addr) = start_server(provider.clone());
  let rt = Runtime::new()?;
  rt.block_on(async {
    // 治理锚定连接 a（发 PING 建会话）；空闲连接 b 连接成功后不发任何
    // 字节——会话永不建立，条目仅由 accept 侧预注册供给
    let mut a = TcpStream::connect(addr).await?;
    send_cmd(&mut a, &[b"PING"]).await?;
    assert_eq!(read_reply(&mut a).await, b"+PONG\r\n");
    let mut b = TcpStream::connect(addr).await?;

    // 预注册条目入表（accept 为异步路径，轮询收敛）
    assert!(
      wait_until(200, || provider.registry.active_consumers().len() >= 2),
      "空闲连接预注册条目未入表"
    );

    // 空闲连接入 CLIENT LIST（无会话行以 view 默认投影渲染：flags=N db=0
    // resp=2——C# ActiveConsumers 对 Session 为 null 的条目不可见，rust
    // 投影模型承接为可见）
    send_cmd(&mut a, &[b"CLIENT", b"LIST"]).await?;
    let list = bulk_body(&read_reply(&mut a).await);
    let lines: Vec<&str> = list.trim_end_matches('\n').split('\n').collect();
    assert_eq!(lines.len(), 2, "空闲连接须入 CLIENT LIST: {list}");
    assert!(
      lines.iter().all(|l| l.contains(" flags=N db=0 resp=2 ")),
      "两行 flags/db/resp 字段完整: {list}"
    );

    // b 行 addr（排除 a 自身行）
    let a_id = client_id(&mut a).await?.to_string();
    let b_addr = addr_of(
      lines
        .iter()
        .find(|l| !l.contains(&format!("id={a_id} ")))
        .expect("空闲连接行"),
    )
    .to_string();

    // KILL 空闲连接：握手段挂起读挂终止令牌，秒断（对齐已建会话连接的
    // KILL 语义）
    send_cmd(&mut a, &[b"CLIENT", b"KILL", b"ADDR", b_addr.as_bytes()]).await?;
    assert_eq!(int_reply(&read_reply(&mut a).await), 1);
    assert!(read_reply(&mut b).await.is_empty(), "空闲连接被杀须断开");

    // 零字节即断的短命连接进统计：received/disposed 配对（a + b + 短命）
    drop(TcpStream::connect(addr).await?);
    let totals = wait_totals(&provider.registry, 200, |t| t == (3, 3, 0));
    // a 仍持连接：received=3（a/b/短命）、disposed=2（被杀 b + 即断短命）、
    // active=1（a）——received - disposed = active 不变量
    assert_eq!(totals, (3, 2, 1), "短命连接须计入 received/disposed");

    Ok::<(), Error>(())
  })?;
  server.stop();
  Ok(())
}

/// KILL 终止域全覆盖锚：大应答在途写出停滞同样入 KILL 取消域——对位 C#
/// GarnetTcpNetworkSender.TryClose（:285）`socket.Close()` "should cause all
/// outstanding requests to fail"（:291 注释自认）：C# 直关套接字令在途 send
/// 即刻失败并走 Dispose 摘除注册表条目；rust 修复前写出段无取消挂点，被杀
/// 连接可无限期滞留注册表。形态：a 发 MGET 组装 ~18MB 大应答后停读不收
/// （应答超双方内核收发缓冲上界之和，服务端命令臂 write_all 被零窗口按住），
/// b 发 CLIENT KILL ID → ① 1 秒级内 a 条目自注册表消失（CONNECTION Totals
/// 活跃数退场、CLIENT LIST 不再列出）；② 服务端套接字关闭（a 端收尽残帧
/// 读到 EOF）；③ 全部断连后 totals 收敛 (2, 2, 0)
#[test]
fn client_kill_stalled_large_write_session() -> aok::Result<()> {
  let _lock = CLIENT_TESTS_LOCK.lock();
  let dir = tempdir()?;
  // 3MB 级大记录须大预算存储配置（缺省 16MB 预算推导 256KB 页放不下单
  // 记录，SET 报通用存储错误；256MB 预算 → 4MB 页，先例见
  // database_manager_multi_sublog_checkpoint）
  let provider = Arc::new(open_provider(
    dir.path().join("node.db"),
    test_store_config_with_budget(256u64 << 20),
    RespServerSessionOptions::default(),
  )?);

  let (server, addr) = start_server(provider.clone());
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut a = TcpStream::connect(addr).await?;
    let mut b = TcpStream::connect(addr).await?;

    let a_id = client_id(&mut a).await?;

    // 6×3MB 经 MGET 组装 ~18MB 应答：超双方内核收发缓冲上界之和
    // （本机 kern.ipc.maxsockbuf=8MB 双端封顶 16MB；MB 级应答可能被
    // autotune 缓冲吞下导致假绿，巨量聚合才保证 write_all 停滞）
    let chunk = vec![b'x'; 3 << 20];
    let mut mget_args: Vec<&[u8]> = vec![b"MGET"];
    let mut names: Vec<String> = Vec::new();
    for i in 0..6 {
      let key = format!("big_{i}");
      send_cmd(&mut b, &[b"SET", key.as_bytes(), &chunk]).await?;
      assert_eq!(read_reply(&mut b).await, b"+OK\r\n");
      names.push(key);
    }
    for k in &names {
      mget_args.push(k.as_bytes());
    }
    send_cmd(&mut a, &mget_args).await?;
    // 让 a 侧泵将命令臂写出推进至 write_all 挂起（b 侧任务不受牵连）
    sleep(Duration::from_millis(200));

    kill_by_id(&mut b, a_id, "首杀须成功").await?;

    // 1 秒级注册表条目消失：a 条目注销、活跃数只余 b
    let totals = wait_totals(&provider.registry, 100, |t| {
      t.2 == 1 && provider.registry.get(a_id).is_none()
    });
    assert!(
      totals.2 == 1 && provider.registry.get(a_id).is_none(),
      "被杀停滞写出连接须 1 秒级自注册表消失，当前 totals={totals:?}"
    );

    // CLIENT LIST 面同步：注销的 a 不再被列出
    send_cmd(&mut b, &[b"CLIENT", b"LIST"]).await?;
    let list = bulk_body(&read_reply(&mut b).await);
    assert!(
      !list.contains(&format!("id={a_id} ")),
      "被杀注销的 a 不得再出现在 CLIENT LIST"
    );

    // 服务端套接字关闭：a 收尽残帧即见 EOF（收场尾巴 shutdown 的 FIN）
    let mut sink = vec![0u8; 64 << 10];
    let eof = timeout(Duration::from_millis(1_000), async {
      loop {
        let BufResult(res, returned) = a.read(sink).await;
        sink = returned;
        match res {
          Ok(0) | Err(_) => break,
          Ok(_) => {}
        }
      }
    })
    .await
    .is_ok();
    assert!(eof, "KILL 停滞写出连接须关闭服务端套接字（a 端读到 EOF）");

    // 全部断连后 totals 收敛：received=2、disposed=2、active=0
    drop(a);
    drop(b);
    let final_totals = wait_totals(&provider.registry, 100, |t| t == (2, 2, 0));
    assert_eq!(
      final_totals,
      (2, 2, 0),
      "被杀与正常断连后 disposed/active 计数须收敛（received - disposed = active 不变量）"
    );

    Ok::<(), Error>(())
  })?;
  server.stop();
  Ok(())
}

/// CLIENT LIST/KILL TYPE 非 ASCII 字面回显逐字节折 '?'（lossy vs ASCII 折叠对齐票）：
/// 对标 C# ParseUtils.ReadString = Encoding.ASCII.GetString——é（0xC3 0xA9）逐字节
/// 各折一个回显 "??"，孤立 0xE9 折 "?"（整错误帧逐字节相等断言，含帧长）；
/// 修复前 rust 走 from_utf8_lossy 对前者原样回 é、对后者回 U+FFFD，三形互不等
#[test]
fn client_kill_list_type_non_ascii_echo_folds_to_question_mark() -> aok::Result<()> {
  let _lock = CLIENT_TESTS_LOCK.lock();
  let dir = tempdir()?;
  let provider = Arc::new(open_provider(
    dir.path().join("node.db"),
    test_store_config(),
    RespServerSessionOptions::default(),
  )?);

  let (server, addr) = start_server(provider);
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut a = TcpStream::connect(addr).await?;
    send_cmd(&mut a, &[b"PING"]).await?;
    assert_eq!(read_reply(&mut a).await, b"+PONG\r\n");

    let e_accute = [0xC3u8, 0xA9]; // 合法二字节序列 → 逐字节折 "??"
    let lone_e9 = [0xE9u8]; // 非法孤立字节 → 折单 "?"（lossy 旧形为 EF BF BD）

    // LIST TYPE 臂（network_clientlist 未知回显）
    send_cmd(&mut a, &[b"CLIENT", b"LIST", b"TYPE", &e_accute]).await?;
    assert_eq!(
      read_reply(&mut a).await,
      b"-ERR Unknown client type '??'\r\n",
      "LIST TYPE é 须逐字节折 ??（C# Encoding.ASCII 口径）"
    );
    send_cmd(&mut a, &[b"CLIENT", b"LIST", b"TYPE", &lone_e9]).await?;
    assert_eq!(
      read_reply(&mut a).await,
      b"-ERR Unknown client type '?'\r\n",
      "LIST TYPE 孤立 0xE9 须折 ?（非 U+FFFD）"
    );

    // KILL TYPE 臂（parse_kill_filters 未知回显）
    send_cmd(&mut a, &[b"CLIENT", b"KILL", b"TYPE", &e_accute]).await?;
    assert_eq!(
      read_reply(&mut a).await,
      b"-ERR Unknown client type '??'\r\n",
      "KILL TYPE é 须逐字节折 ??（C# Encoding.ASCII 口径）"
    );
    send_cmd(&mut a, &[b"CLIENT", b"KILL", b"TYPE", &lone_e9]).await?;
    assert_eq!(
      read_reply(&mut a).await,
      b"-ERR Unknown client type '?'\r\n",
      "KILL TYPE 孤立 0xE9 须折 ?（非 U+FFFD）"
    );

    Ok::<(), Error>(())
  })?;
  server.stop();
  Ok(())
}

/// CLIENT KILL USER 滤值与 ACL 存名折叠双向锁（lossy vs ASCII 折叠对齐票）：
/// ACL SETUSER café 存名按 ACL 域既有口径折为 caf??（user_name 单源直读句柄），
/// 滤值侧改齐 wbase::ascii_sanitize 后 CLIENT KILL USER café 与 CLIENT KILL USER
/// caf?? 均折为 caf?? Ordinal 全等命中回 :count 1——修复前滤值走 lossy 保 café
/// 与存名恒不等 killed:0 永杀不掉（C# 折-折同杀，IsMatch :466）
#[test]
fn client_kill_user_non_ascii_folds_matches_stored_name() -> aok::Result<()> {
  let _lock = CLIENT_TESTS_LOCK.lock();
  let dir = tempdir()?;
  let acl = Arc::new(AccessControlList::new("")?);
  let provider = Arc::new(
    open_provider(
      dir.path().join("node.db"),
      test_store_config(),
      RespServerSessionOptions {
        default_user: "default".into(),
        max_databases: 16,
        ..RespServerSessionOptions::default()
      },
    )?
    .with_acl(Arc::clone(&acl)),
  );

  let (server, addr) = start_server(provider.clone());
  let rt = Runtime::new()?;
  rt.block_on(async {
    let cafe = b"caf\xC3\xA9"; // café：é = 0xC3 0xA9，ASCII 折叠形为 caf??
    let folded = b"caf??";

    let mut admin = TcpStream::connect(addr).await?;
    send_cmd(&mut admin, &[b"PING"]).await?;
    assert_eq!(read_reply(&mut admin).await, b"+PONG\r\n");

    // 非 ASCII 名建档（ns 0 default 超管）；口令 pw
    send_cmd(
      &mut admin,
      &[b"ACL", b"SETUSER", cafe, b"on", b">pw", b"+@all"],
    )
    .await?;
    assert_eq!(read_reply(&mut admin).await, b"+OK\r\n");

    // 轮询等待注册表活跃连接数收敛到 want（会话泵建连注册/被杀注销均为异步）
    let wait_active = |want: usize| {
      assert!(
        wait_until(200, || provider.registry.active_consumers().len() == want),
        "注册表活跃连接数未收敛到 {want}"
      );
    };
    wait_active(1);

    // —— 第一轮：目标以原形 café 认证，KILL USER café（原形滤值）须命中 ——
    let mut t = TcpStream::connect(addr).await?;
    send_cmd(&mut t, &[b"AUTH", cafe, b"pw"]).await?;
    assert_eq!(
      read_reply(&mut t).await,
      b"+OK\r\n",
      "café 认证须命中折名 caf??"
    );
    let _t_id = client_id(&mut t).await?;
    wait_active(2);

    // 他者视角 LIST 须见 user=caf??（存名单源折叠形）
    send_cmd(&mut admin, &[b"CLIENT", b"LIST"]).await?;
    let list = bulk_body(&read_reply(&mut admin).await);
    assert!(
      list.contains("user=caf??"),
      "CLIENT LIST user 字段须为折叠存名 caf??: {list}"
    );

    send_cmd(&mut admin, &[b"CLIENT", b"KILL", b"USER", cafe]).await?;
    assert_eq!(
      int_reply(&read_reply(&mut admin).await),
      1,
      "KILL USER café 须折为 caf?? 与存名全等命中（修复前 lossy 保 café 恒 killed:0）"
    );
    assert!(read_reply(&mut t).await.is_empty(), "目标连接须被杀断开");
    wait_active(1);

    // —— 第二轮：目标仍以原形 café 认证，KILL USER caf??（折叠形滤值）须命中 ——
    let mut t2 = TcpStream::connect(addr).await?;
    send_cmd(&mut t2, &[b"AUTH", cafe, b"pw"]).await?;
    assert_eq!(read_reply(&mut t2).await, b"+OK\r\n");
    let _t2_id = client_id(&mut t2).await?;
    wait_active(2);

    send_cmd(&mut admin, &[b"CLIENT", b"KILL", b"USER", folded]).await?;
    assert_eq!(
      int_reply(&read_reply(&mut admin).await),
      1,
      "KILL USER caf??（折叠形）须与存名 caf?? 全等命中（折后全等双向锁）"
    );
    assert!(read_reply(&mut t2).await.is_empty(), "目标连接须被杀断开");
    wait_active(1);

    Ok::<(), Error>(())
  })?;
  server.stop();
  Ok(())
}
