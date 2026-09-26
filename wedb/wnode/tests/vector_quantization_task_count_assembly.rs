//! Vector Set 量化任务数装配级专测
//!
//! 验证：
//! 1. CLI 显式 `--vector-set-quantization-task-count 2` 装配后断言 VectorManager.quantization_task_count == 2
//! 2. 负值折叠走自动档与默认档同形
//! 3. 缺省仍折 available_parallelism

use std::{
  sync::{Arc, atomic::Ordering},
  thread::available_parallelism,
};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wconf::{ConfigFileArgs, NodeArgs, RuntimeServerOptions};
use wnode::service::StorageSessionProvider;
use wnode_test::{session_factory, start_server};
use wtest_base::test_store_config;

#[test]
fn vector_set_quantization_task_count_cli_explicit_override() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let default_parallelism = available_parallelism().map(|n| n.get()).unwrap_or(4);

  // 1. CLI 显式 --vector-set-quantization-task-count 2（recover 生产装配链）
  let node = NodeArgs::from_args_iter([
    "wedb",
    "--vector-set-quantization-task-count",
    "2",
    "--recover",
  ])
  .expect("parse args");
  assert_eq!(node.vector_set_quantization_task_count, 2);

  let provider = rt.block_on(async {
    StorageSessionProvider::open_from_args(
      &node,
      dir.path().join("quant_explicit.db"),
      session_factory,
    )
    .await
    .expect("open from args")
  });
  assert_eq!(
    provider
      .vector_manager
      .quantization_task_count
      .load(Ordering::Relaxed),
    2
  );

  // 2. 负值折叠走自动档（max(0) 折叠为 0，与默认档同形）
  let node_negative = NodeArgs::from_args_iter([
    "wedb",
    "--vector-set-quantization-task-count",
    "-3",
    "--recover",
  ])
  .expect("parse args");
  assert_eq!(node_negative.vector_set_quantization_task_count, -3);

  let provider_negative = rt.block_on(async {
    StorageSessionProvider::open_from_args(
      &node_negative,
      dir.path().join("quant_neg.db"),
      session_factory,
    )
    .await
    .expect("open from args")
  });
  assert_eq!(
    provider_negative
      .vector_manager
      .quantization_task_count
      .load(Ordering::Relaxed),
    default_parallelism
  );

  // 3. 缺省仍折 available_parallelism
  let node_default = NodeArgs::from_args_iter(["wedb", "--recover"]).expect("parse args");
  assert_eq!(node_default.vector_set_quantization_task_count, 0);

  let provider_default = rt.block_on(async {
    StorageSessionProvider::open_from_args(
      &node_default,
      dir.path().join("quant_default.db"),
      session_factory,
    )
    .await
    .expect("open from args")
  });
  assert_eq!(
    provider_default
      .vector_manager
      .quantization_task_count
      .load(Ordering::Relaxed),
    default_parallelism
  );
}

#[test]
fn vector_set_quantization_task_count_aof_recover_chain() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("aof_node").join("node.db");
  let default_parallelism = available_parallelism().map(|n| n.get()).unwrap_or(4);

  // 初始代：点亮 AOF 并提交一条记录，确保 WAL 具备可恢复段
  let init_provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions::default(),
      session_factory,
    )
    .expect("init aof provider"),
  );
  let (server, _addr) = start_server(Arc::clone(&init_provider));
  rt.block_on(async {
    let session = init_provider.store().new_session().expect("session");
    session.upsert(b"init-k", b"init-v").await.expect("upsert");
    init_provider
      .aof()
      .expect("aof enabled")
      .log()
      .commit_async()
      .await;
  });
  server.stop();
  drop(server);
  drop(init_provider);

  // AOF + recover 显式 2
  let node_explicit = NodeArgs::from_args_iter([
    "wedb",
    "--vector-set-quantization-task-count",
    "2",
    "--recover",
    "--aof",
  ])
  .expect("parse args");
  assert_eq!(node_explicit.vector_set_quantization_task_count, 2);

  let provider_explicit = rt.block_on(async {
    StorageSessionProvider::open_from_args(&node_explicit, &data_path, session_factory)
      .await
      .expect("open recovered with aof")
  });
  assert_eq!(
    provider_explicit
      .vector_manager
      .quantization_task_count
      .load(Ordering::Relaxed),
    2
  );
  drop(provider_explicit);

  // AOF + recover 负值折叠
  let node_negative = NodeArgs::from_args_iter([
    "wedb",
    "--vector-set-quantization-task-count",
    "-5",
    "--recover",
    "--aof",
  ])
  .expect("parse args");
  let provider_negative = rt.block_on(async {
    StorageSessionProvider::open_from_args(&node_negative, &data_path, session_factory)
      .await
      .expect("open recovered with aof negative")
  });
  assert_eq!(
    provider_negative
      .vector_manager
      .quantization_task_count
      .load(Ordering::Relaxed),
    default_parallelism
  );
  drop(provider_negative);

  // AOF + recover 缺省
  let node_default = NodeArgs::from_args_iter(["wedb", "--recover", "--aof"]).expect("parse args");
  let provider_default = rt.block_on(async {
    StorageSessionProvider::open_from_args(&node_default, &data_path, session_factory)
      .await
      .expect("open recovered with aof default")
  });
  assert_eq!(
    provider_default
      .vector_manager
      .quantization_task_count
      .load(Ordering::Relaxed),
    default_parallelism
  );
}

/// 案一回归：冷启动正常路径（无 --recover / 无 --aof，四臂 (false,false)）下
/// `--vector-set-quantization-task-count` 必须真透传至 VectorManager，不再被
/// 三件套漏斗 `open_node_with_config` 硬编码 0 静默丢弃回退核数（假旋钮）
#[test]
fn vector_set_quantization_task_count_cold_start_passthrough() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");

  // 冷启动显式 2：断言精准为 2，而非回退 host 核数
  let node = NodeArgs::from_args_iter(["wedb", "--vector-set-quantization-task-count", "2"])
    .expect("parse args");
  assert_eq!(node.vector_set_quantization_task_count, 2);
  let provider = rt.block_on(async {
    StorageSessionProvider::open_from_args(
      &node,
      dir.path().join("cold_explicit.db"),
      session_factory,
    )
    .await
    .expect("open cold start")
  });
  assert_eq!(
    provider
      .vector_manager
      .quantization_task_count
      .load(Ordering::Relaxed),
    2
  );
  drop(provider);

  // 冷启动缺省：仍折 available_parallelism
  let default_parallelism = available_parallelism().map(|n| n.get()).unwrap_or(4);
  let node_default = NodeArgs::from_args_iter(["wedb"]).expect("parse args");
  assert_eq!(node_default.vector_set_quantization_task_count, 0);
  let provider_default = rt.block_on(async {
    StorageSessionProvider::open_from_args(
      &node_default,
      dir.path().join("cold_default.db"),
      session_factory,
    )
    .await
    .expect("open cold start default")
  });
  assert_eq!(
    provider_default
      .vector_manager
      .quantization_task_count
      .load(Ordering::Relaxed),
    default_parallelism
  );
}
