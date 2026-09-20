//! 启动配置到运行时配置面的装配测试
//!
//! 验证 NodeArgs 运行时选项投影（Options.cs:GetServerOptions 装配段对标）
//! 经 StorageSessionProvider::with_runtime_server_options 播种后：
//! RuntimeServerConfig 槽位生效 + 慢日志容器按 SlowLogMaxEntries 装配
//! （对标 C# StoreWrapper.cs:243 slowLogContainer 构造）+ 只读回显五字段
//! （DIR/LOGDIR/UNIXSOCKET/APPENDONLY/AOF_SIZE_LIMIT）CONFIG GET 格式化断言。

use std::{path::PathBuf, sync::Arc};

use tempfile::tempdir;
use wconf::{NodeArgs, ServerConfigType};
use wmetric::{SlowLogContainer, SlowLogEntry};
use wnode::{resp::resp_session_consumer::RespSessionConsumer, service::StorageSessionProvider};
use wresp::command::RespCommand;
use wtest_base::test_store_config;

fn make_entry(id: i32) -> SlowLogEntry {
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

  // RuntimeServerConfig 槽位播种生效（handle_slow_log / SCAN 上限读取源）
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
