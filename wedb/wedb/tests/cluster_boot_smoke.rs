//! 集群 bin 裸启动冒烟测试
//!
//! 验证集群节点在单机裸启动（无预置集群对端）时：
//! 1. `cluster_provider.start()` 与 gossip spawn 处于 compio runtime 内，
//!    绝不触发 "not in a compio runtime" panic；
//! 2. 支持通过 ShutdownCoordinator 优雅停机；
//! 3. 实际构建的 wedb 二进制裸启动正常运行并不崩溃。

use std::{
  io::Error,
  process::Command,
  thread::{sleep, spawn},
  time::Duration,
};

use aok::Result;
use tempfile::tempdir;
use wconf::ConfigFileArgs;
use wedb::{ClusterArgs, server::boot::run_cluster_server};
use wnode::ShutdownCoordinator;

/// 进程内通过 ServerBootstrap 与 run_cluster_server 验证裸启动
#[test]
fn test_cluster_boot_inprocess_smoke() -> Result<()> {
  let dir = tempdir()?;
  let dir_str = dir.path().to_string_lossy().to_string();

  let args = ClusterArgs::from_args_iter([
    "wedb",
    "--port",
    "0",
    "--dir",
    &dir_str,
    "--gossip-delay-secs",
    "1",
  ])
  .map_err(|e| Error::other(e.to_string()))?;

  let coordinator = ShutdownCoordinator::new();
  let coord_clone = coordinator.clone();

  let handle = spawn(move || run_cluster_server(args, Some(coord_clone)));

  // 等待服务与 Gossip 任务安全进入 compio runtime
  sleep(Duration::from_millis(600));
  assert!(
    !handle.is_finished(),
    "服务应处于运行态，不得因 panic 提前崩溃"
  );

  // 触发优雅关机
  coordinator.stop();
  let res = handle.join().map_err(|_| Error::other("线程异常退出"))?;
  assert!(res.is_ok(), "服务应优雅退出: {res:?}");

  Ok(())
}

/// 验证编译产物 wedb 二进制裸启动不发生 panic
#[test]
fn test_cluster_bin_bare_boot_smoke() -> Result<()> {
  let dir = tempdir()?;
  let dir_str = dir.path().to_string_lossy().to_string();

  let bin_path = env!("CARGO_BIN_EXE_wedb");
  let mut child = Command::new(bin_path)
    .arg("--port")
    .arg("0")
    .arg("--dir")
    .arg(&dir_str)
    .spawn()?;

  // 等待观察子进程是否崩溃
  sleep(Duration::from_millis(800));

  // 若存在 compio runtime 启动 panic，进程在数十毫秒内即已退出
  let status = child.try_wait()?;
  assert!(
    status.is_none(),
    "集群 bin 裸启动不应提前崩溃退出: {status:?}"
  );

  // 发送 SIGTERM 验证优雅关机
  #[cfg(unix)]
  {
    let _ = Command::new("kill")
      .arg("-15")
      .arg(child.id().to_string())
      .status();
  }
  #[cfg(not(unix))]
  {
    let _ = child.kill();
  }

  let exit_status = child.wait()?;
  #[cfg(unix)]
  {
    use std::os::unix::process::ExitStatusExt;
    // 优雅关机退出码为 0，或被 SIGTERM (15) 信号中断
    assert!(
      exit_status.success() || exit_status.signal() == Some(15),
      "集群 bin 应正常退出: {exit_status:?}"
    );
  }
  #[cfg(not(unix))]
  {
    assert!(
      exit_status.success(),
      "集群 bin 应正常退出: {exit_status:?}"
    );
  }

  Ok(())
}
