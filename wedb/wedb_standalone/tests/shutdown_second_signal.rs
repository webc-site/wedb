//! 优雅停机窗口期二次信号强杀逃生通道端到端回归（task/ing/zcode-r21-standalone）
//!
//! 缺陷：首信号进入优雅停机后，未触发信号监听任务只入队取消，future 实际
//! 析构（SignalListener::drop → SigDfl 恢复）被 stop() 同步长段（向量收敛
//! 30s×2 / pubsub 收敛 / join 排空无上界）饿死——窗口期二次信号（同种或
//! 异种）被仍在 slab 的 handler 吞掉，进程继续优雅退出（退出码 0）。
//! 修复：wait_shutdown_signal drop 监听任务后 yield_now 让渡一轮，SIG_DFL
//! 恢复先于返回（处置位断言的确定性回归见
//! wnode/tests/signal_default_disposition_restore.rs）。
//!
//! 本文件按票面验证点做真进程端到端：首信号后等「捕获停机信号」日志（该
//! 日志晚于 wait_shutdown_signal 返回，此刻 SIG_DFL 必已恢复），随后送达
//! 二次信号——同种（SIGINT→SIGINT）与异种（SIGINT→SIGTERM）两形态，断言
//! 进程被信号按默认处置杀死（信号致死而非 0 退出）。C# host
//! （main/GarnetServer/Program.cs Main）无任何信号注册，任何信号按默认
//! 处置必死，修复后窗口期行为对齐原型。
//!
//! 端到端时序竞争说明：二次信号必须抢在优雅停机自然退出前送达；空库停机
//! 全程为数十毫秒级，日志观测轮询粒度为毫秒级，极端调度抖动可能错过窗口
//! （表现同为 0 退出）。MAX_ATTEMPTS 重试只覆盖「错过」——若恢复回归失效
//! （信号被吞），每次尝试都以 0 退出，重试后必然失败，不掩盖回归。

use std::{
  env::temp_dir,
  fs,
  io::Read,
  net::TcpListener,
  os::unix::process::ExitStatusExt,
  path::{Path, PathBuf},
  process::{Child, Command, ExitStatus, Stdio, id},
  sync::{Arc, Mutex},
  thread::{sleep, spawn},
  time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use nix::{
  sys::signal::{Signal, kill},
  unistd::Pid,
};

/// 就绪横幅（wait_for_shutdown 的 log::info，非 quiet 默认开启）
const READINESS: &str = "网络监听就绪";
/// 首信号捕获日志（晚于 wait_shutdown_signal 返回，SIG_DFL 已恢复）
const CAUGHT: &str = "捕获停机信号";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const CAUGHT_TIMEOUT: Duration = Duration::from_secs(10);
const EXIT_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_ATTEMPTS: usize = 3;
/// 日志轮询粒度
const POLL: Duration = Duration::from_millis(5);

fn free_port() -> u16 {
  TcpListener::bind("127.0.0.1:0")
    .unwrap()
    .local_addr()
    .unwrap()
    .port()
}

fn unique_dir() -> PathBuf {
  let dir = temp_dir().join(format!(
    "wedb-r21-second-signal-{}-{}",
    id(),
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_nanos()
  ));
  fs::create_dir_all(&dir).unwrap();
  dir
}

/// 拉起单机服务端子进程，stderr 异步收集进共享缓冲
fn spawn_server(dir: &Path) -> (Child, Arc<Mutex<String>>) {
  let mut child = Command::new(env!("CARGO_BIN_EXE_wedb-standalone"))
    .args([
      "--port",
      &free_port().to_string(),
      "--dir",
      &dir.to_string_lossy(),
      "--log-level",
      "info",
    ])
    .stdout(Stdio::null())
    .stderr(Stdio::piped())
    .spawn()
    .expect("拉起 wedb-standalone 失败");
  let stderr = child.stderr.take().expect("stderr 管道");
  let log = Arc::new(Mutex::new(String::new()));
  let sink = Arc::clone(&log);
  spawn(move || {
    let mut reader = stderr;
    let mut buf = [0u8; 4096];
    loop {
      match reader.read(&mut buf) {
        Ok(0) | Err(_) => break,
        Ok(n) => sink
          .lock()
          .unwrap()
          .push_str(&String::from_utf8_lossy(&buf[..n])),
      }
    }
  });
  (child, log)
}

fn wait_for_log(log: &Mutex<String>, marker: &str, timeout: Duration) -> bool {
  let deadline = Instant::now() + timeout;
  while Instant::now() < deadline {
    if log.lock().unwrap().contains(marker) {
      return true;
    }
    sleep(POLL);
  }
  false
}

fn wait_exit(child: &mut Child, timeout: Duration) -> Option<ExitStatus> {
  let deadline = Instant::now() + timeout;
  loop {
    match child.try_wait() {
      // Ok(None) = 仍在运行，继续轮询至超时（不可误判为终态）
      Ok(Some(status)) => return Some(status),
      Ok(None) if Instant::now() >= deadline => return None,
      Ok(None) => sleep(POLL),
      Err(_) => return None,
    }
  }
}

/// 首信号进入优雅停机，等捕获日志（SIG_DFL 已恢复）后送达二次信号，
/// 返回进程终态；Err 为时序竞争错失（非回归证据）
fn first_then_second(first: Signal, second: Signal) -> Result<ExitStatus, String> {
  let dir = unique_dir();
  let (mut child, log) = spawn_server(&dir);
  let outcome = (|| {
    if !wait_for_log(&log, READINESS, STARTUP_TIMEOUT) {
      return Err("服务未在时限内就绪".into());
    }
    kill(Pid::from_raw(child.id() as i32), first).expect("首信号送达失败");
    if !wait_for_log(&log, CAUGHT, CAUGHT_TIMEOUT) {
      return Err("首信号未在时限内被捕获".into());
    }
    kill(Pid::from_raw(child.id() as i32), second).expect("二次信号送达失败");
    wait_exit(&mut child, EXIT_TIMEOUT).ok_or_else(|| "二次信号后进程未在时限内退出".to_string())
  })();
  let _ = child.kill();
  let _ = child.wait();
  let _ = fs::remove_dir_all(&dir);
  outcome
}

fn assert_second_signal_kills(first: Signal, second: Signal, label: &str) {
  let mut last = String::new();
  for _ in 0..MAX_ATTEMPTS {
    match first_then_second(first, second) {
      Ok(status) => {
        assert_eq!(
          status.signal(),
          Some(second as i32),
          "{label}: 二次信号应按默认处置杀死进程（SIG_DFL 逃生通道），实际终态 {status:?}"
        );
        return;
      }
      Err(e) => last = e,
    }
  }
  panic!("{label}: 连续 {MAX_ATTEMPTS} 次尝试未观测到二次信号致死（末次: {last}）");
}

/// 同种二次信号：SIGINT → SIGINT 窗口期送达必杀
#[test]
fn same_kind_second_signal_kills_during_graceful_stop() {
  assert_second_signal_kills(
    Signal::SIGINT,
    Signal::SIGINT,
    "同种二次信号 (SIGINT→SIGINT)",
  );
}

/// 异种二次信号：SIGINT → SIGTERM 窗口期送达必杀
#[test]
fn other_kind_second_signal_kills_during_graceful_stop() {
  assert_second_signal_kills(
    Signal::SIGINT,
    Signal::SIGTERM,
    "异种二次信号 (SIGINT→SIGTERM)",
  );
}
