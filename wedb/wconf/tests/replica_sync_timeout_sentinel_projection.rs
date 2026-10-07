#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 票 wconf-replica-sync-timeout-zero-infinite-sentinel-projection：
//! `replica_sync_timeout_secs` 零/负值投影折无限超时哨兵 u64::MAX 的全链用例。
//!
//! 对标 C# garnet/libs/host/Configuration/Options.cs:995
//! `ReplicaSyncTimeout = ReplicaSyncTimeout <= 0 ? Timeout.InfiniteTimeSpan :
//! TimeSpan.FromSeconds(ReplicaSyncTimeout)`，并镜像其 test 臂
//! garnet/test/standalone/Garnet.test/TestUtils.cs:900 的同一折算；三面
//! （结构体直构 / CLI `--repl-sync-timeout` / toml 配置文件）物化后
//! 经 runtime_server_options() 断言折算口径。
//!
//! 修复前必红：投影臂为 `self.replica_sync_timeout_secs as u64` 直强转，
//! 0/-1 折出 0/巨负回绕值而非 u64::MAX，消费端
//! `Duration::from_secs(0)` 使副本一致读立即报 ConsistentReadTimeout，
//! 本文件哨兵断言即失败。
//!
//! 自研依据: 副本同步超时哨兵投影（C# 对应 ReplicaSyncTimeout 校验）

use std::{env::temp_dir, fs, path::PathBuf, process, time::Duration};

use wconf::{ConfigFileArgs, NodeArgs, node_options::DEFAULT_REPLICA_SYNC_TIMEOUT_SECS};

/// 写临时 toml 配置文件（路径掺测试名与进程 id，杜绝并发互踩）
fn temp_config(name: &str, content: &str) -> PathBuf {
  let path = temp_dir().join(format!(
    "wedb-repl-sync-timeout-{name}-{}.toml",
    process::id()
  ));
  fs::write(&path, content).unwrap();
  path
}

/// 结构体直构三臂：0 与负值折 u64::MAX 哨兵，正值原样透传
#[test]
fn test_replica_sync_timeout_sentinel_projection() {
  for raw in [0_i32, -1, i32::MIN] {
    let args = NodeArgs {
      replica_sync_timeout_secs: raw,
      ..Default::default()
    };
    let opts = args.runtime_server_options();
    assert_eq!(
      opts.replica_sync_timeout_secs,
      u64::MAX,
      "{raw} 须折无限超时哨兵（对标 Options.cs:995 <=0 ? InfiniteTimeSpan）"
    );
    assert_eq!(
      opts.replica_sync_timeout(),
      None,
      "{raw} 折叠访问器须返 None（对标 Options.cs:995 <=0 ? InfiniteTimeSpan）"
    );
  }
  for raw in [1_i32, 5, 77, i32::MAX] {
    let args = NodeArgs {
      replica_sync_timeout_secs: raw,
      ..Default::default()
    };
    let opts = args.runtime_server_options();
    assert_eq!(
      opts.replica_sync_timeout_secs, raw as u64,
      "正值 {raw} 须原样投影"
    );
    assert_eq!(
      opts.replica_sync_timeout(),
      Some(Duration::from_secs(raw as u64)),
      "正值 {raw} 折叠访问器须返 Some(Duration)"
    );
  }
}

/// CLI 面：--repl-sync-timeout 0 物化后投影为哨兵；正值与未给即默认三态
#[test]
fn test_replica_sync_timeout_sentinel_via_cli() {
  let args = NodeArgs::from_args_iter(["wedb", "--repl-sync-timeout", "0"]).unwrap();
  assert_eq!(args.replica_sync_timeout_secs, 0);
  assert_eq!(
    args.runtime_server_options().replica_sync_timeout_secs,
    u64::MAX,
    "CLI 显式 0 须折无限超时哨兵"
  );
  assert_eq!(
    args.runtime_server_options().replica_sync_timeout(),
    None,
    "CLI 显式 0 折叠访问器须返 None"
  );

  let args = NodeArgs::from_args_iter(["wedb", "--repl-sync-timeout", "-1"]).unwrap();
  assert_eq!(
    args.runtime_server_options().replica_sync_timeout_secs,
    u64::MAX,
    "CLI 负值同臂归哨兵（C# <=0 判据）"
  );
  assert_eq!(
    args.runtime_server_options().replica_sync_timeout(),
    None,
    "CLI 负值折叠访问器须返 None"
  );

  // 显式正值即生效、未给即取默认（C# TestUtils.cs:900 正值臂）
  let args = NodeArgs::from_args_iter(["wedb", "--repl-sync-timeout", "9"]).unwrap();
  assert_eq!(args.replica_sync_timeout_secs, 9);
  assert_eq!(
    args.runtime_server_options().replica_sync_timeout(),
    Some(Duration::from_secs(9)),
    "CLI 显式正值须返回 Some(Duration)"
  );
  let args = NodeArgs::from_args_iter(["wedb"]).unwrap();
  assert_eq!(
    args.runtime_server_options().replica_sync_timeout_secs,
    DEFAULT_REPLICA_SYNC_TIMEOUT_SECS as u64,
    "未给即取 C# defaults.conf:355 缺省 5 秒"
  );
  assert_eq!(
    args.runtime_server_options().replica_sync_timeout(),
    Some(Duration::from_secs(
      DEFAULT_REPLICA_SYNC_TIMEOUT_SECS as u64
    )),
    "缺省折叠访问器须返 5 秒"
  );
}

/// toml 配置面：`replica_sync_timeout_secs = 0` 物化后投影为哨兵
#[test]
fn test_replica_sync_timeout_sentinel_via_config_file() {
  let file = temp_config("zero", "replica_sync_timeout_secs = 0\n");
  let args = NodeArgs::from_args_iter(["wedb", "--config", file.to_str().unwrap()]).unwrap();
  fs::remove_file(&file).ok();
  assert_eq!(args.replica_sync_timeout_secs, 0);
  assert_eq!(
    args.runtime_server_options().replica_sync_timeout_secs,
    u64::MAX,
    "配置文件零值须折无限超时哨兵"
  );
  assert_eq!(
    args.runtime_server_options().replica_sync_timeout(),
    None,
    "配置文件零值折叠访问器须返 None"
  );

  // 三层合并回归：文件 0 被 CLI 显式正值覆盖后取正值（覆盖与折算互不串扰）
  let file = temp_config("mixed", "replica_sync_timeout_secs = 0\n");
  let args = NodeArgs::from_args_iter([
    "wedb",
    "--config",
    file.to_str().unwrap(),
    "--repl-sync-timeout",
    "12",
  ])
  .unwrap();
  fs::remove_file(&file).ok();
  assert_eq!(args.runtime_server_options().replica_sync_timeout_secs, 12);
  assert_eq!(
    args.runtime_server_options().replica_sync_timeout(),
    Some(Duration::from_secs(12)),
    "CLI 覆盖后折叠访问器须返 Some(12s)"
  );
}

/// 消费侧折算见证（wnode garnet_append_only_file.rs 即此式）：u64::MAX 秒
/// 经 replica_sync_timeout 折为 None（不挂计时器、永等放行），与 0 秒「立即超时」严格可分辨——修复前
/// 哨兵位投影成 0 时本断言即红
#[test]
fn test_sentinel_seconds_never_fire_at_consumer() {
  let args = NodeArgs {
    replica_sync_timeout_secs: 0,
    ..Default::default()
  };
  let runtime_opts = args.runtime_server_options();
  assert_eq!(
    runtime_opts.replica_sync_timeout(),
    None,
    "消费端折叠访问器须返 None 永等（杜绝 Duration::from_secs(u64::MAX) 溢出）"
  );
  let read_timeout = Duration::from_secs(runtime_opts.replica_sync_timeout_secs);
  assert!(read_timeout > Duration::from_secs(86400 * 365));
  assert_ne!(read_timeout, Duration::ZERO);
}
