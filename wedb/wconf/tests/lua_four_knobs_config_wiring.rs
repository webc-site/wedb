//! 票 wlua-lua-options-config-disconnect：Lua 四旋钮（内存模式/内存限额/
//! 日志模式/沙箱白名单）+ 超时值域的配置端真接线全链用例。
//!
//! 对标 garnet libs/host/Configuration/Options.cs:642/:647/:651/:655/:664 五旋钮
//! 与 :1029 `new LuaOptions(...)` 单套装配段：CLI 显式项、toml 文件导入、
//! 导出-回导往返三面 + 启动校验拒启面（ForbiddenWithOption Native 互斥、
//! MemorySizeValidation、IntRangeValidation(10, int.MaxValue) 0 特例）。
//!
//! 修复前必红：NodeArgs 无此四字段，无论 CLI/文件如何设值，attach 装配点
//! `LuaOptions { ..default() }` 恒落 Native/0/Enable/空，四旋钮断言即失败。

use std::{env::temp_dir, fs, path::PathBuf, process};

use wconf::{ConfigFileArgs, LuaLoggingMode, LuaMemoryManagementMode, NodeArgs, NodeOptionsError};

/// 写临时 toml 配置文件（路径掺测试名与进程 id，杜绝并发互踩）
fn temp_config(name: &str, content: &str) -> PathBuf {
  let path = temp_dir().join(format!("wedb-lua-knobs-{name}-{}.toml", process::id()));
  fs::write(&path, content).unwrap();
  path
}

/// 四旋钮的文件基线值（与默认、CLI 覆盖三方互异）
const FILE_BASELINE: &str = "\
enable_lua = true
lua_script_timeout_ms = 5000
lua_memory_management_mode = \"tracked\"
lua_script_memory_limit = \"8mb\"
lua_logging_mode = \"silent\"
lua_allowed_functions = [\"redis.call\", \"cjson.encode\"]
";

/// 四旋钮的 CLI 显式新值（kebab 长名与 C# 同名）
fn cli_overrides() -> Vec<&'static str> {
  vec![
    "--lua-memory-management-mode",
    "managed",
    "--lua-script-memory-limit",
    "16mb",
    "--lua-logging-mode",
    "disable",
    "--lua-allowed-functions",
    "redis.call,redis.pcall",
    "--lua-script-timeout-ms",
    "9000",
  ]
}

/// 第一段：--config 文件基线 + CLI 显式项 → 显式给出即覆盖
#[test]
fn test_cli_explicit_overrides_file_baseline() {
  let file = temp_config("cli-ovr", FILE_BASELINE);
  let mut argv: Vec<&str> = vec!["wedb", "--config", file.to_str().unwrap()];
  argv.extend(cli_overrides());
  let args = NodeArgs::from_args_iter(&argv).unwrap();
  fs::remove_file(&file).ok();

  assert_eq!(
    args.lua_memory_management_mode,
    LuaMemoryManagementMode::Managed
  );
  assert_eq!(args.lua_script_memory_limit.as_deref(), Some("16mb"));
  assert_eq!(args.lua_memory_limit_bytes(), Some(16 * 1024 * 1024));
  assert_eq!(args.lua_logging_mode, LuaLoggingMode::Disable);
  assert_eq!(
    args.lua_allowed_functions,
    vec!["redis.call".to_string(), "redis.pcall".to_string()]
  );
  assert_eq!(args.lua_script_timeout_ms, 9000);
}

/// 第二段：仅 --config 无 CLI 显式项 → toml 蛇形键导入原样生效（证文件面接线）
#[test]
fn test_toml_file_import() {
  let file = temp_config("file-only", FILE_BASELINE);
  let args = NodeArgs::from_args_iter(["wedb", "--config", file.to_str().unwrap()]).unwrap();
  fs::remove_file(&file).ok();

  assert_eq!(
    args.lua_memory_management_mode,
    LuaMemoryManagementMode::Tracked
  );
  assert_eq!(args.lua_memory_limit_bytes(), Some(8 * 1024 * 1024));
  assert_eq!(args.lua_logging_mode, LuaLoggingMode::Silent);
  assert_eq!(
    args.lua_allowed_functions,
    vec!["redis.call".to_string(), "cjson.encode".to_string()]
  );
  assert_eq!(args.lua_script_timeout_ms, 5000);
}

/// 第三段：导出-回导往返 → 四旋钮落盘再回导逐值全等（导入面与导出面同一字段集）
#[test]
fn test_export_reimport_roundtrip() {
  let file = temp_config("roundtrip", FILE_BASELINE);
  let args = NodeArgs::from_args_iter(["wedb", "--config", file.to_str().unwrap()]).unwrap();
  fs::remove_file(&file).ok();

  let out = temp_dir().join(format!("wedb-lua-knobs-export-{}.toml", process::id()));
  args.export_config(&out).expect("导出失败");
  let reloaded = NodeArgs::from_file(&out).expect("回导失败");
  fs::remove_file(&out).ok();

  assert_eq!(
    reloaded.lua_memory_management_mode,
    args.lua_memory_management_mode
  );
  assert_eq!(
    reloaded.lua_script_memory_limit,
    args.lua_script_memory_limit
  );
  assert_eq!(reloaded.lua_logging_mode, args.lua_logging_mode);
  assert_eq!(reloaded.lua_allowed_functions, args.lua_allowed_functions);
  assert_eq!(reloaded.lua_script_timeout_ms, args.lua_script_timeout_ms);
}

/// 缺省态零漂移锁：不传四旗标时逐值等于 C# 生效默认（Native / 无限制 /
/// Enable / 空白名单 / 超时 0），杜绝缺省常量被顺手改
#[test]
fn test_defaults_no_drift() {
  let args = NodeArgs::from_args_iter(["wedb"]).unwrap();
  assert_eq!(
    args.lua_memory_management_mode,
    LuaMemoryManagementMode::Native
  );
  assert_eq!(args.lua_script_memory_limit, None);
  assert_eq!(args.lua_memory_limit_bytes(), None);
  assert_eq!(args.lua_logging_mode, LuaLoggingMode::Enable);
  assert!(args.lua_allowed_functions.is_empty());
  assert_eq!(args.lua_script_timeout_ms, 0);
}

/// ForbiddenWithOption(Native)：限额与 Native 模式同时设置拒启；切 Tracked 即放行
#[test]
fn test_memory_limit_forbidden_with_native_mode() {
  let err = NodeArgs::from_args_iter(["wedb", "--lua-script-memory-limit", "8mb"]).unwrap_err();
  assert!(
    matches!(err, NodeOptionsError::LuaMemoryLimitWithNative),
    "Native + 限额须拒启, actual: {err:?}"
  );

  // Tracked + 限额：放行且字节量投影到位
  let args = NodeArgs::from_args_iter([
    "wedb",
    "--lua-memory-management-mode",
    "tracked",
    "--lua-script-memory-limit",
    "8mb",
  ])
  .unwrap();
  assert_eq!(args.lua_memory_limit_bytes(), Some(8 * 1024 * 1024));
}

/// MemorySizeValidation(false)：限额尺寸串非空却无法整体解析即启动拒启
#[test]
fn test_memory_limit_size_parse_reject() {
  let err = NodeArgs::from_args_iter([
    "wedb",
    "--lua-memory-management-mode",
    "tracked",
    "--lua-script-memory-limit",
    "lots",
  ])
  .unwrap_err();
  assert!(
    matches!(err, NodeOptionsError::InvalidSizeStr(name, _) if name == "lua-script-memory-limit"),
    "非法尺寸串须拒启, actual: {err:?}"
  );
}

/// IntRangeValidation(10, int.MaxValue, isRequired: false)：0 = 禁用合法，
/// 负值与 0 < 值 < 10 拒启，10 及以上放行
#[test]
fn test_script_timeout_range_reject() {
  // 0 = 禁用（缺省）合法
  assert!(NodeArgs::from_args_iter(["wedb", "--lua-script-timeout-ms=0"]).is_ok());
  // 恰在下界 10 合法
  assert!(NodeArgs::from_args_iter(["wedb", "--lua-script-timeout-ms=10"]).is_ok());
  for bad in ["-5", "1", "9"] {
    let arg = format!("--lua-script-timeout-ms={bad}");
    let err = NodeArgs::from_args_iter(["wedb", arg.as_str()]).unwrap_err();
    assert!(
      matches!(err, NodeOptionsError::LuaScriptTimeoutOutOfRange(v) if v == bad.parse::<i64>().unwrap()),
      "超时 {bad} 须拒启, actual: {err:?}"
    );
  }
}
