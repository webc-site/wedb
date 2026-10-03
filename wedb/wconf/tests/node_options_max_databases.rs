//! max_databases 启动期定界测试（对标 C# Options.cs:687 MaxDatabases 的
//! IntRangeValidation(1, 256, isRequired: true)，校验挂在三层合并漏斗
//! from_layered_matches 的导出之前，命令行与配置文件两路同受约束）
//!
//! 自研依据: max_databases 投影（C# 对应 GarnetServerOptions MaxDatabases）

use std::{env::temp_dir, fs, path::PathBuf};

use wconf::{ConfigFileArgs, NodeArgs};

/// 写临时 toml 配置文件（进程内唯一名，测试结束自清理）
fn temp_config(name: &str, content: &str) -> PathBuf {
  let path = temp_dir().join(format!("wedb-wconf-maxdb-{name}.toml"));
  fs::write(&path, content).unwrap();
  path
}

#[test]
fn test_max_databases_range_validation() {
  // 文件配 64 合法，原样投影进 runtime_server_options
  let file = temp_config("legal", "max_databases = 64\n");
  let args = NodeArgs::from_args_iter(["wedb", "--config", file.to_str().unwrap()]).unwrap();
  fs::remove_file(&file).ok();
  assert_eq!(args.max_databases, 64);
  assert_eq!(args.runtime_server_options().max_databases, 64);

  // 端点 1 / 256 合法
  assert!(NodeArgs::from_args_iter(["wedb", "--max-databases", "1"]).is_ok());
  assert!(NodeArgs::from_args_iter(["wedb", "--max-databases", "256"]).is_ok());

  // 命令行越界被拒
  for bad in ["0", "257", "1000000"] {
    assert!(
      NodeArgs::from_args_iter(["wedb", "--max-databases", bad]).is_err(),
      "max-databases {bad} 应被启动期定界拒绝"
    );
  }

  // 配置文件负值同样被拒（漏斗对文件基线同样校验）
  let file = temp_config("negative", "max_databases = -3\n");
  assert!(NodeArgs::from_args_iter(["wedb", "--config", file.to_str().unwrap()]).is_err());
  fs::remove_file(&file).ok();
}
