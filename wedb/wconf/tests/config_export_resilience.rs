//! 验证 NodeArgs 配置导出路径只读或不存在父目录时降级不拒启（zcode-r59-cfgio）

use std::{
  env::temp_dir,
  fs::{self, File},
  process,
  time::SystemTime,
};

use aok::Result;
use wconf::{ConfigFileArgs, NodeArgs};

#[test]
fn test_node_args_config_export_nonexistent_parent_dir() -> Result<()> {
  let ts = SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap()
    .as_millis();
  let missing_path = temp_dir().join(format!(
    "wedb-missing-dir-{}-{}/export.toml",
    process::id(),
    ts
  ));

  // 确保父目录绝对不存在
  if missing_path.parent().unwrap().exists() {
    let _ = fs::remove_dir_all(missing_path.parent().unwrap());
  }

  let args = NodeArgs::from_args_iter([
    "wedb",
    "--config-export-path",
    missing_path.to_str().unwrap(),
  ]);

  assert!(args.is_ok(), "配置导出父目录不存在时应降级继续，不得拒启");
  let args = args.unwrap();
  assert_eq!(
    args.config_export_path.as_deref(),
    Some(missing_path.as_path())
  );

  Ok(())
}

#[test]
fn test_node_args_config_export_readonly_file() -> Result<()> {
  let ts = SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap()
    .as_millis();
  let readonly_path = temp_dir().join(format!(
    "wedb-readonly-export-{}-{}.toml",
    process::id(),
    ts
  ));

  File::create(&readonly_path)?;
  let mut perms = fs::metadata(&readonly_path)?.permissions();
  perms.set_readonly(true);
  fs::set_permissions(&readonly_path, perms)?;

  let args = NodeArgs::from_args_iter([
    "wedb",
    "--config-export-path",
    readonly_path.to_str().unwrap(),
  ]);

  // 清理前恢复写权限以便删除
  #[cfg(unix)]
  {
    use std::os::unix::fs::PermissionsExt;
    let _ = fs::set_permissions(&readonly_path, fs::Permissions::from_mode(0o644));
  }
  #[cfg(not(unix))]
  {
    let mut restore_perms = fs::metadata(&readonly_path)?.permissions();
    restore_perms.set_readonly(false);
    let _ = fs::set_permissions(&readonly_path, restore_perms);
  }
  let _ = fs::remove_file(&readonly_path);

  assert!(args.is_ok(), "配置导出路径只读时应降级继续，不得拒启");
  let args = args.unwrap();
  assert_eq!(
    args.config_export_path.as_deref(),
    Some(readonly_path.as_path())
  );

  Ok(())
}
