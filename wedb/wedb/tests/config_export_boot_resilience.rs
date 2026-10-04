//! 验证 --config-export-path 指向只读或父目录不存在路径时服务照常启动（zcode-r59-cfgio）
//!
//! 契约对齐：C# TryParseCommandLineArguments 调用 TryExportServerOptions 后丢弃返回值，
//! 导出失败尽力而为仅记录日志，服务照常启动。
//! 验证当 --config-export-path 指向不存在父目录或只读路径时：
//! 1. ClusterArgs::from_args_iter 正常解析成功，降级告警不拒启；
//! 2. 服务经 run_cluster_server 照常启动并可优雅停机。

use std::{
  fs::{self, File},
  io::Error,
  thread::{sleep, spawn},
  time::Duration,
};

use aok::Result;
use tempfile::tempdir;
use wconf::ConfigFileArgs;
use wedb::{ClusterArgs, server::boot::run_cluster_server};
use wnode::ShutdownCoordinator;

/// 验证父目录不存在时服务照常启动
#[test]
fn test_config_export_nonexistent_parent_dir_boot_resilience() -> Result<()> {
  let dir = tempdir()?;
  let dir_str = dir.path().to_string_lossy().to_string();
  let invalid_export_path = dir.path().join("missing_dir/nested/export.nt");

  let args = ClusterArgs::from_args_iter([
    "wedb",
    "--port",
    "0",
    "--dir",
    &dir_str,
    "--gossip-delay-secs",
    "1",
    "--config-export-path",
    invalid_export_path.to_str().unwrap(),
  ])
  .map_err(|e| Error::other(e.to_string()))?;

  let coordinator = ShutdownCoordinator::new();
  let coord_clone = coordinator.clone();

  let handle = spawn(move || run_cluster_server(args, Some(coord_clone)));

  sleep(Duration::from_millis(600));
  assert!(
    !handle.is_finished(),
    "导出路径父目录不存在时服务应照常启动，不得崩溃或拒启"
  );

  coordinator.stop();
  let res = handle.join().map_err(|_| Error::other("线程异常退出"))?;
  assert!(res.is_ok(), "服务应优雅退出: {res:?}");

  Ok(())
}

/// 验证导出路径为只读文件时服务照常启动
#[test]
fn test_config_export_readonly_path_boot_resilience() -> Result<()> {
  let dir = tempdir()?;
  let dir_str = dir.path().to_string_lossy().to_string();
  let readonly_export_path = dir.path().join("readonly_export.nt");

  // 创建只读文件
  File::create(&readonly_export_path)?;
  let mut perms = fs::metadata(&readonly_export_path)?.permissions();
  perms.set_readonly(true);
  fs::set_permissions(&readonly_export_path, perms)?;

  let args = ClusterArgs::from_args_iter([
    "wedb",
    "--port",
    "0",
    "--dir",
    &dir_str,
    "--gossip-delay-secs",
    "1",
    "--config-export-path",
    readonly_export_path.to_str().unwrap(),
  ])
  .map_err(|e| Error::other(e.to_string()))?;

  let coordinator = ShutdownCoordinator::new();
  let coord_clone = coordinator.clone();

  let handle = spawn(move || run_cluster_server(args, Some(coord_clone)));

  sleep(Duration::from_millis(600));
  assert!(
    !handle.is_finished(),
    "导出路径只读时服务应照常启动，不得崩溃或拒启"
  );

  coordinator.stop();
  let res = handle.join().map_err(|_| Error::other("线程异常退出"))?;
  assert!(res.is_ok(), "服务应优雅退出: {res:?}");

  Ok(())
}
