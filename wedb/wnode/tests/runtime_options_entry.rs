//! 启动配置到运行时配置面的装配测试
//!
//! 验证 NodeArgs 运行时选项投影（Options.cs:GetServerOptions 装配段对标）
//! 经 StorageSessionProvider::with_runtime_server_options 播种后：
//! RuntimeServerConfig 槽位生效 + 慢日志容器按 SlowLogMaxEntries 装配
//! （对标 C# StoreWrapper.cs:243 slowLogContainer 构造）+ 只读回显五字段
//! （DIR/LOGDIR/UNIXSOCKET/APPENDONLY/AOF_SIZE_LIMIT）CONFIG GET 格式化断言。

use std::{path::PathBuf, sync::Arc};

use tempfile::tempdir;
use wconf::{NodeArgs, RuntimeServerOptions, ServerConfigType, size::pretty_size};
use wmetric::{SlowLogContainer, SlowLogEntry};
use wnode::{
  aof::AofSettings, resp::resp_session_consumer::RespSessionConsumer,
  service::StorageSessionProvider,
};
use wresp::command::RespCommand;
use wtest_base::test_store_config;

fn make_entry(id: i64) -> SlowLogEntry {
  SlowLogEntry {
    id,
    timestamp: 0,
    duration: 0,
    command: RespCommand::Invalid,
    arguments: None,
    client_ip_port: String::new(),
    client_name: String::new(),
  }
}

#[test]
fn runtime_options_seeded_into_provider() {
  let dir = tempdir().unwrap();
  let data_path = dir.path().join("data").join("config-entry.db");

  let node_args = NodeArgs {
    slow_log_threshold: 1500,
    slow_log_max_entries: 7,
    object_scan_count_limit: 333,
    max_databases: 4,
    dir: PathBuf::from("/ro/data"),
    unixsocket: Some("/ro/x.sock".into()),
    aof: true,
    aof_size_limit: Some("64mb".into()),
    ..Default::default()
  };

  let provider = StorageSessionProvider::open_with_config(
    test_store_config(),
    &data_path,
    |network_sender_id, api| {
      Some(RespSessionConsumer::new(
        network_sender_id,
        Default::default(),
        Arc::new(api),
      ))
    },
  )
  .unwrap()
  .with_runtime_server_options(node_args.runtime_server_options());

  // RuntimeServerConfig 槽位播种生效（批入口阈值折算 / SCAN 上限读取源）
  assert_eq!(
    provider
      .runtime_config
      .get_int(ServerConfigType::SlowlogLogSlowerThan),
    1500
  );
  assert_eq!(
    provider
      .runtime_config
      .get_int(ServerConfigType::ObjectScanCountLimit),
    333
  );
  assert_eq!(
    provider
      .runtime_config
      .resp_format(ServerConfigType::Databases),
    "4"
  );

  // 只读回显五字段端到端：启动投影 → 格式器直读选项（CONFIG GET/INFO 共用回落），
  // 对应 C# RuntimeServerConfig.cs:122-134 SetReadOnly 直读 serverOptions
  assert_eq!(
    provider.runtime_config.resp_format(ServerConfigType::Dir),
    "/ro/data"
  );
  assert_eq!(
    provider
      .runtime_config
      .resp_format(ServerConfigType::Logdir),
    "/ro/data/wal"
  );
  assert_eq!(
    provider
      .runtime_config
      .resp_format(ServerConfigType::UnixSocket),
    "/ro/x.sock"
  );
  assert_eq!(
    provider
      .runtime_config
      .resp_format(ServerConfigType::AppendOnly),
    "yes"
  );
  assert_eq!(
    provider
      .runtime_config
      .resp_format(ServerConfigType::AofSizeLimit),
    "64mb"
  );

  // 慢日志容器按 SlowLogMaxEntries 装配：第 8 条入队淘汰首条（环形裁剪）
  let container: &SlowLogContainer = &provider.slow_log_container;
  assert_eq!(container.count(), 0);
  for i in 0..8 {
    container.add(make_entry(i));
  }
  assert_eq!(container.count(), 7);
  // 环形裁剪淘汰的是最早的条目
  assert_eq!(container.get_entries(7)[0].id, 1);
}

#[test]
fn slow_log_container_default_capacity() {
  // 缺省装配容量 = GarnetServerOptions.cs:292 SlowLogMaxEntries 默认 128
  let dir = tempdir().unwrap();
  let data_path = dir.path().join("data").join("config-default.db");
  let provider = StorageSessionProvider::open_with_config(
    test_store_config(),
    &data_path,
    |network_sender_id, api| {
      Some(RespSessionConsumer::new(
        network_sender_id,
        Default::default(),
        Arc::new(api),
      ))
    },
  )
  .unwrap();
  let container: &SlowLogContainer = &provider.slow_log_container;
  for i in 0..200 {
    container.add(make_entry(i));
  }
  assert_eq!(container.count(), 128);
  assert_eq!(container.get_entries(128)[0].id, 72);
}

/// AOF 尺寸旋钮 CLI 入口端到端：NodeArgs（aof + aof_segment_size "64m"）经
/// runtime_server_options() 单点投影、open_with_config_and_aof 内
/// AofSettings::from_options 唯一体检口装配，设备实持段容量即生效值
///（对标 Options.cs:924-926 投影 → GarnetServerOptions.cs:1050 GetAofSettings
/// 装载段）。物理口径见 wnode/tests/service_aof.rs:127 既有例，本例只补
/// 入口面这一跳，不重复物理断言
#[test]
fn cli_aof_segment_size_reaches_device() {
  let dir = tempdir().unwrap();
  let data_path = dir.path().join("data").join("aof-size-entry.db");
  let node_args = NodeArgs {
    aof: true,
    aof_segment_size: Some("64m".into()),
    ..Default::default()
  };
  let provider = StorageSessionProvider::open_with_config_and_aof(
    test_store_config(),
    &data_path,
    None,
    node_args.runtime_server_options(),
    |network_sender_id, api| {
      Some(RespSessionConsumer::new(
        network_sender_id,
        Default::default(),
        Arc::new(api),
      ))
    },
  )
  .unwrap();
  let wal = provider.wal().expect("AOF 点亮即有物理日志");
  assert_eq!(
    wal.device.segment_size(),
    64 * 1024 * 1024,
    "CLI 指定 aof-segment-size 须经投影 + 装配抵达设备段容量"
  );
}

/// AOF 门面全量投影端到端：调用方 options 的复制背压预算
///（aof-sync-max-lag-bytes）直入门面背压闸，不经窄化二次投影
///（对标 C# EnableAOF 装配段完整 serverOptions 直入 GarnetAppendOnlyFile；
/// 回归锚：旧装配以 `NodeArgs{..default}` 窄化投影仅回填 commit 频率，
/// 预算丢失后背压闸回落缺省 -1 恒禁用）
#[test]
fn aof_facade_receives_full_options_projection() {
  let dir = tempdir().unwrap();
  let data_path = dir.path().join("data").join("aof-facade-projection.db");
  let node_args = NodeArgs {
    aof: true,
    aof_sync_max_lag_bytes: 1 << 20,
    ..Default::default()
  };
  let provider = StorageSessionProvider::open_with_config_and_aof(
    test_store_config(),
    &data_path,
    None,
    node_args.runtime_server_options(),
    |network_sender_id, api| {
      Some(RespSessionConsumer::new(
        network_sender_id,
        Default::default(),
        Arc::new(api),
      ))
    },
  )
  .unwrap();
  let backpressure = provider
    .aof()
    .expect("AOF 点亮即有门面")
    .log()
    .backpressure()
    .expect("门面构造期建背压闸");
  assert!(
    backpressure.enabled(),
    "aof-sync-max-lag-bytes 须经全量投影抵达背压闸（窄化投影即回落 -1 禁用）"
  );
  assert_eq!(
    backpressure.publish_delta_bytes(),
    (1 << 20) / 8,
    "背压推进告警阈值取配置生效值（单子日志预算 = 预算/子日志数）"
  );
}

/// 实配投影接线（票 wnode-aof-main-page-bits-unwired）：`--hlog-page-size 4m`
/// 后 AOF 页 8m 合法放行（编译期 16m 常量口径误拒），4m 页按实配下界拒启
/// 并引导 --hlog-page-size——C# GetAofSettings 改 PageSize 后拒启的同位语义
///（自 `wnode/src/aof/aof_settings.rs` 内联测模块迁入，纯 pub API 面）
#[test]
fn main_page_floor_follows_configured_hlog_page_size() {
  const MAIN_PAGE: usize = 4 * 1024 * 1024;
  let base = RuntimeServerOptions {
    aof_memory_size: Some("64m".into()),
    hlog_page_size: MAIN_PAGE,
    ..RuntimeServerOptions::default()
  };

  let ok = RuntimeServerOptions {
    aof_page_size: Some("8m".into()),
    ..base.clone()
  };
  let settings = AofSettings::from_options(&ok).expect("页 = 实配主存页两倍须放行");
  assert_eq!(settings.page_size_bytes, 8 * 1024 * 1024);

  let floor = RuntimeServerOptions {
    aof_page_size: Some("4m".into()),
    ..base
  };
  let msg = AofSettings::from_options(&floor)
    .expect_err("页等于实配主存页（不足两倍）必须拒启")
    .to_string();
  assert!(
    msg.contains(&pretty_size((MAIN_PAGE * 2) as i64)) && msg.contains("--hlog-page-size"),
    "文案须按实配主存页给出下界并引导 --hlog-page-size: {msg}"
  );
}

/// 缺省组合合法且尺寸非 2 的幂时就近下取（C# PreviousPowerOf2 同口径）
///（自 `wnode/src/aof/aof_settings.rs` 内联测模块迁入，纯 pub API 面）
#[test]
fn accepts_defaults_and_rounds_down() {
  let defaults = AofSettings::from_options(&RuntimeServerOptions::default())
    .expect("缺省组合（128m/32m/1g）须通过");
  assert_eq!(defaults.memory_size_bytes, 128 * 1024 * 1024);
  assert_eq!(defaults.page_size_bytes, 32 * 1024 * 1024);
  assert_eq!(defaults.segment_size_bytes, 1024 * 1024 * 1024);

  let rounded = RuntimeServerOptions {
    aof_memory_size: Some("2500m".into()),
    aof_page_size: Some("33m".into()),
    aof_segment_size: Some("1500m".into()),
    ..RuntimeServerOptions::default()
  };
  let settings = AofSettings::from_options(&rounded).expect("合法组合须通过");
  assert_eq!(settings.memory_size_bytes, 2 * 1024 * 1024 * 1024);
  assert_eq!(settings.page_size_bytes, 32 * 1024 * 1024);
  assert_eq!(settings.segment_size_bytes, 1024 * 1024 * 1024);
}
