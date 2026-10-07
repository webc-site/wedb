#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 票 wconf-override-explicit-full-coverage：NodeArgs 全字段 CLI 显式覆盖守护
//! （既有 cli_override_explicit_fields.rs 仅覆盖 11 个关键字段，本测试以反射
//! 面把覆盖判定钉死在 derive 单源上，验收标准「新增字段漏改必被测试抓到」）。
//!
//! 三处手写同步点的守护分工：
//!
//! 1. struct 定义与 `Default::default()`：满字段结构体字面量，新增字段缺位即
//!    E0063 编译错，编译器天然守护，无需测试；
//!
//! 2. `override_explicit` 的 over! 清单（本测试主靶）：合并基线取 default、
//!    cli 取全量显式值，二者 TOML 导出串必须全等——over! 漏掉任一序列化可见
//!    字段，该字段留在基线缺省值，串不等即红；
//!
//! 3. 本测试 CLI 清单自身：遍历 `NodeArgs::command()` 的全部已声明 arg
//!    （derive 单源，新增字段自动入列），逐 id 断言
//!    `value_source == CommandLine`——新增字段未同步扩充清单即红，
//!    测试自身不可漏。
//!
//! 例外白名单：`config` / `config_export_path` 为 CLI 专属元选项
//!（`#[toml(skip)]`，见 `ConfigFileArgs::from_layered_matches`——合并前拷出、
//! 合并后回填），刻意不进 over! 清单，本测试单独断言合并后保持缺省。
//!
//! 自研依据: 命令行覆盖仅显式字段（toml 配置契约，C# 无对应）

use clap::{CommandFactory, FromArgMatches, parser::ValueSource};
use wconf::NodeArgs;

/// 全量 CLI 显式清单：每个已声明 arg 一个与缺省值互异的非默认值
///（新值全部对过 wconf 内 DEFAULT_* 常量与字段缺省，逐项人工核对）
fn full_cli_args() -> Vec<&'static str> {
  vec![
    "--bind",
    "192.168.7.9",
    "--port",
    "16379",
    "--unixsocket",
    "/tmp/wedb-guard.sock",
    "--unixsocketperm",
    "644",
    "--dir",
    "guard-data",
    "--wal-dir",
    "guard-wal",
    "--checkpoint-dir",
    "guard-ckpt",
    "--requirepass",
    "guard-pass",
    "--tls-cert",
    "guard-cert.pem",
    "--tls-key",
    "guard-key.pem",
    "--tls-client-cert-required",
    "false",
    "--tls-client-target-host",
    "guard-host",
    "--tls-server-cert-required",
    "false",
    "--tls-issuer-cert",
    "guard-issuer.pem",
    "--cert-refresh-freq",
    "9",
    "--threads",
    "5",
    "--network-connection-limit",
    "100",
    "--aof",
    "true",
    "--disable-pubsub",
    "true",
    "--recover",
    "true",
    "--aof-commit-ms",
    "7",
    "--aof-commit-wait",
    "true",
    "--repl-diskless-sync",
    "true",
    "--fast-aof-truncate",
    "true",
    "--on-demand-checkpoint",
    "false",
    "--file-logger",
    "guard.log",
    "--log-level",
    "debug",
    "--quiet",
    "true",
    "--disable-console-logger",
    "true",
    "--slowlog-log-slower-than",
    "123456",
    "--slowlog-max-len",
    "129",
    "--max-databases",
    "17",
    "--protected-mode",
    "false",
    "--enable-debug-command",
    "yes",
    "--object-scan-count-limit",
    "1001",
    "--expired-object-collection-freq",
    "12",
    "--expired-key-deletion-scan-freq",
    "45",
    "--metrics-sampling-freq",
    "14",
    "--latency-monitor",
    "true",
    "--commandstats-monitor",
    "true",
    "--enable-lua",
    "true",
    "--lua-script-timeout-ms",
    "15000",
    "--lua-memory-management-mode",
    "managed",
    "--lua-script-memory-limit",
    "64mb",
    "--lua-logging-mode",
    "silent",
    "--lua-allowed-functions",
    "fa,fb",
    "--enable-vector-set-preview",
    "true",
    "--aof-size-limit",
    "64mb",
    "--aof-memory",
    "256m",
    "--aof-page-size",
    "64m",
    "--aof-segment-size",
    "2g",
    "--aof-size-limit-enforce-frequency",
    "7",
    "--index-max-size",
    "128mb",
    "--index-resize-frequency",
    "61",
    "--index-resize-threshold",
    "51",
    "--repl-sync-timeout",
    "31",
    "--repl-attach-timeout",
    "120",
    "--replica-sync-delay",
    "25",
    "--aof-sync-max-lag-bytes",
    "4096",
    "--aof-tail-witness-freq",
    "50",
    "--cluster-replication-reestablishment-timeout",
    "33",
    "--vector-set-quantization-task-count",
    "8",
    "--compaction-type",
    "Shift",
    "--compaction-max-segments",
    "33",
    "--sg-get",
    "false",
    "--aof-replay-max-lag-bytes",
    "5",
    "--repl-diskless-sync-delay",
    "6",
    "--cluster-announce-hostname",
    "guard-node",
    "--cluster-username",
    "guard-user",
    "--cluster-password",
    "guard-secret",
    "--config",
    "guard.toml",
    "--config-export-path",
    "guard-export.toml",
    // hlog 配置段（flatten 嵌套，arg id 带 hlog_ 前缀或独立长名）
    "--hlog-page-size",
    "33554432",
    "--hlog-memory-size",
    "8589934592",
    "--hlog-mutable-percent",
    "66",
    "--read-cache",
    "true",
    "--read-cache-memory-size",
    "2147483648",
    "--tree-cache-budget",
    "1073741824",
    "--reviv",
    "true",
    "--reviv-fraction",
    "0.9",
    "--copy-reads-to-tail",
    "true",
  ]
}

/// CLI 专属元选项白名单（toml(skip)，刻意不进 over!，见模块文档）
const CLI_ONLY_META_ARGS: [&str; 2] = ["config", "config_export_path"];

#[test]
fn override_explicit_covers_every_declared_arg() {
  let args: Vec<String> = ["wedb-guard".to_string()]
    .into_iter()
    .chain(full_cli_args().into_iter().map(str::to_string))
    .collect();

  let matches = NodeArgs::command()
    .try_get_matches_from(&args)
    .expect("全量 CLI 清单必须可解析");
  let cli = NodeArgs::from_arg_matches(&matches).expect("matches 反物化必须成功");

  // 步骤一（测试清单自身完备性）：derive 单源的已声明 arg 全集里，除 help/version
  // 伪项外，每个 id 必须被本测试显式给出——新增字段漏配清单在此必红
  for arg in NodeArgs::command().get_arguments() {
    let id = arg.get_id().as_str();
    if id == "help" || id == "version" {
      continue;
    }
    assert_eq!(
      matches.value_source(id),
      Some(ValueSource::CommandLine),
      "arg {id} 未被测试 CLI 清单显式给出：NodeArgs 新增字段必须同步扩充 full_cli_args 并纳入 override_explicit 的 over! 清单"
    );
  }

  // 步骤二（over! 清单完备性）：default 基线经 override_explicit 合并后，TOML
  // 导出串须与 cli 逐字段全等——over! 漏掉任一序列化可见字段，该字段留基线
  // 缺省值，串不等即红
  let mut merged = NodeArgs::default();
  merged.override_explicit(&matches, cli.clone());
  let merged_toml = merged.to_toml_string().expect("合并面 TOML 导出必须成功");
  let cli_toml = cli.to_toml_string().expect("cli 面 TOML 导出必须成功");
  assert_eq!(
    merged_toml, cli_toml,
    "override_explicit 的 over! 清单与 TOML 可见字段集不等：漏项字段的 CLI 显式值被基线静默丢弃"
  );

  // 步骤三（白名单契约钉死）：config / config_export_path 刻意不进 over!，
  // 合并后保持缺省（from_layered_matches 在外层拷出回填，不经 override 面）
  for id in CLI_ONLY_META_ARGS {
    assert_ne!(
      matches.value_source(id),
      None,
      "元选项 {id} 必须仍是已声明 arg（结构变更须同步本白名单与 over! 排除面）"
    );
  }
  assert!(
    merged.config.is_none() && merged.config_export_path.is_none(),
    "CLI 专属元选项不得经 override_explicit 混入合并面"
  );
}

#[test]
fn full_cli_list_actually_deviates_from_default() {
  // sanity：全量清单与 default 基线的 TOML 导出必须互异——两串相等即清单全体
  // 失效（如序列化形态变更致全部值巧合同串），守护退化为空转
  let args: Vec<String> = ["wedb-guard".to_string()]
    .into_iter()
    .chain(full_cli_args().into_iter().map(str::to_string))
    .collect();
  let matches = NodeArgs::command()
    .try_get_matches_from(&args)
    .expect("全量 CLI 清单必须可解析");
  let cli = NodeArgs::from_arg_matches(&matches).expect("matches 反物化必须成功");
  let default_toml = NodeArgs::default()
    .to_toml_string()
    .expect("default TOML 导出必须成功");
  let cli_toml = cli.to_toml_string().expect("cli TOML 导出必须成功");
  assert_ne!(
    default_toml, cli_toml,
    "全量 CLI 清单与 default 基线零差异，守护用例失效"
  );
}
