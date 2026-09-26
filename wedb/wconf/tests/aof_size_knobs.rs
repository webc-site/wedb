//! AOF 三面尺寸旋钮入口面测试（对标 C# Options.cs:211-221 三面 [Option]
//! 旗标 + [MemorySizeValidation] 定界，与 Options.cs:924-926 投影进
//! GarnetServerOptions 的装配段）
//!
//! 覆盖三层优先级（结构体缺省 → toml 文件 → 命令行显式项）与
//! 启动期旗标级拒启；投影断言取生效字符串原文（wconf 不折算字节，组合互校验
//! 唯一真源在 wnode `AofSettings::from_options`，本面禁第二套解析器）。
//!
//! 自研依据: AOF 容量旋钮投影（C# 对应 ServerSettingsManager AofMemorySize 校验）

use std::{env::temp_dir, fs, path::PathBuf};

use wconf::{ConfigFileArgs, DEFAULT_HLOG_PAGE_SIZE, NodeArgs, NodeOptionsError};

/// 写临时 toml 配置文件（进程内唯一名，测试结束自清理）
fn temp_config(name: &str, content: &str) -> PathBuf {
  let path = temp_dir().join(format!("wedb-wconf-aof-size-{name}.toml"));
  fs::write(&path, content).unwrap();
  path
}

/// 未显式配置时三项投影仍为 RuntimeServerOptions 缺省原文（128m/32m/1g），
/// 证伪 NodeArgs 携带第二套缺省常量；主存页容量投影缺省同一真源
/// （DEFAULT_HLOG_PAGE_SIZE，票 wnode-aof-main-page-bits-unwired）
#[test]
fn unset_knobs_keep_single_default_truth() {
  let opts = NodeArgs::default().runtime_server_options();
  assert_eq!(opts.aof_memory_size.as_deref(), Some("128m"));
  assert_eq!(opts.aof_page_size.as_deref(), Some("32m"));
  assert_eq!(opts.aof_segment_size.as_deref(), Some("1g"));
  assert_eq!(
    opts.hlog_page_size, DEFAULT_HLOG_PAGE_SIZE,
    "主存页容量投影缺省须与 DEFAULT_HLOG_PAGE_SIZE 同源"
  );
}

/// CLI 三面旗标覆盖缺省：原样字符串经 runtime_server_options() 抵达
#[test]
fn cli_knobs_override_defaults() {
  let args = NodeArgs::from_args_iter([
    "wedb",
    "--aof-memory",
    "64mb",
    "--aof-page-size",
    "16mb",
    "--aof-segment-size",
    "512mb",
  ])
  .unwrap();
  let opts = args.runtime_server_options();
  assert_eq!(opts.aof_memory_size.as_deref(), Some("64mb"));
  assert_eq!(opts.aof_page_size.as_deref(), Some("16mb"));
  assert_eq!(opts.aof_segment_size.as_deref(), Some("512mb"));
}

/// toml 文件覆盖缺省（键名 = 字段名）
#[test]
fn toml_knobs_override_defaults() {
  let file = temp_config(
    "file",
    "aof_memory_size = \"64mb\"\naof_page_size = \"16mb\"\naof_segment_size = \"512mb\"\n",
  );
  let args = NodeArgs::from_args_iter(["wedb", "--config", file.to_str().unwrap()]).unwrap();
  fs::remove_file(&file).ok();
  let opts = args.runtime_server_options();
  assert_eq!(opts.aof_memory_size.as_deref(), Some("64mb"));
  assert_eq!(opts.aof_page_size.as_deref(), Some("16mb"));
  assert_eq!(opts.aof_segment_size.as_deref(), Some("512mb"));
}

/// 三层优先级逐字段：CLI 显式项覆盖文件同键，CLI 未显式项保留文件值
#[test]
fn cli_overrides_toml_per_field() {
  let file = temp_config(
    "layered",
    "aof_memory_size = \"256mb\"\naof_page_size = \"64mb\"\naof_segment_size = \"2gb\"\n",
  );
  let args = NodeArgs::from_args_iter([
    "wedb",
    "--config",
    file.to_str().unwrap(),
    "--aof-page-size",
    "32mb",
  ])
  .unwrap();
  fs::remove_file(&file).ok();
  let opts = args.runtime_server_options();
  assert_eq!(
    opts.aof_page_size.as_deref(),
    Some("32mb"),
    "CLI 显式项覆盖文件值"
  );
  assert_eq!(
    opts.aof_memory_size.as_deref(),
    Some("256mb"),
    "CLI 未显式覆盖时文件值保留"
  );
  assert_eq!(opts.aof_segment_size.as_deref(), Some("2gb"));
}

/// 旗标级可解析性定界（对标 C# [MemorySizeValidation] 的入口侧）：非尺寸
/// 文本经 validate()（from_args_iter 漏斗末端）启动期拒启，CLI 与文件两路同受约束
#[test]
fn unparseable_size_rejected_at_startup() {
  for flag in ["--aof-memory", "--aof-page-size", "--aof-segment-size"] {
    let err = NodeArgs::from_args_iter(["wedb", flag, "foo"]).unwrap_err();
    assert!(
      matches!(err, NodeOptionsError::InvalidSizeStr(_, _)),
      "{flag} foo 应启动期拒启: {err:?}"
    );
  }
  let file = temp_config("bad", "aof_memory_size = \"foo\"\n");
  assert!(
    NodeArgs::from_args_iter(["wedb", "--config", file.to_str().unwrap()]).is_err(),
    "文件面非尺寸文本同样拒启"
  );
  fs::remove_file(&file).ok();
}
