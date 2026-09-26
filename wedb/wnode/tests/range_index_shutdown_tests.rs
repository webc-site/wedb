//! 范围索引停机收口回归：stop() 显式释放在线树（对标 C# 关停链
//! libs/server/StoreWrapper.cs:Dispose 的 `rangeIndexManager?.Dispose()`，
//! Provider.Dispose 段——连接排空之后、databaseManager 引擎兜底析构之前）
//!
//! 1. 在线树经 server.stop() 释放，live_indexes 清空，不依赖 store 析构
//!    （嵌入式/复用进程形态 stop() 返回即释放树句柄与 native 页缓存）；
//! 2. 重复收口安全：显式收口、GarnetServer drop 的 stop 重入、
//!    WedbStore::drop 的 dispose 兜底三者并存不 panic（wbftree manager
//!    逐树 `take` 幂等语义）。

use std::sync::Arc;

use tempfile::tempdir;
use wbftree::{StorageBackendType, TreeTuning};
use wconf::RuntimeServerOptions;
use wnode::{SessionProviderFace, service::StorageSessionProvider};
use wnode_test::{session_factory, start_server};
use wtest_base::test_store_config;

/// stop() 收口在线树：live_indexes 清空，且不依赖 store 析构
#[test]
fn stop_disposes_live_range_index_trees() {
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("ri_shutdown.db");

  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions::default(),
      session_factory,
    )
    .expect("open with aof"),
  );
  let (server, _addr) = start_server(Arc::clone(&provider));

  // 构造在线树（引擎直建并持句柄；测试期间 store 不析构）
  let engine = Arc::clone(provider.store().range_index());
  engine
    .create_bftree(
      b"ri_shutdown",
      StorageBackendType::Memory,
      TreeTuning::default(),
    )
    .expect("create online tree");
  assert_eq!(engine.live_index_count(), 1, "预置：在线树已注册");

  server.stop();

  assert_eq!(
    engine.live_index_count(),
    0,
    "stop() 收口后在线树必须释放（不依赖 store 析构）"
  );

  // 重复收口安全：显式收口重入（幂等）不 panic
  provider.dispose_range_index();
  assert_eq!(engine.live_index_count(), 0, "重复收口后仍为空");
}
