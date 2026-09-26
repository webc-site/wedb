//! GarnetServer dispose_async 契约与 session_provider 访问器单测
//!
//! 验证：
//! 1. `session_provider()` 访问器正确返回底层 `&Arc<P>`；
//! 2. `server.dispose_async().await` 先关停网络，再将 AOF 环形缓冲未提交帧刷盘，
//!    重启 recover 能正确回放数据；
//! 3. 对偶语义锁：裸 `server.stop()`（或 Drop）仅为网络/资源收口，不执行 AOF 异步刷盘。

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::tempdir;
use waof::AofEntryType;
use wconf::RuntimeServerOptions;
use wnode::service::StorageSessionProvider;
use wnode_test::{session_factory, start_server};
use wtest_base::test_store_config;
use wval::{KeyTag, NamespaceDbCodec};

fn entry_key(user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// 验证 session_provider 访问器透出底层会话提供者句柄
#[test]
fn test_server_session_provider_accessor() {
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("accessor.db");

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
  assert!(
    Arc::ptr_eq(server.session_provider(), &provider),
    "session_provider() 须与入参 provider 指向同一 Arc 实例"
  );
  server.stop();
}

/// 验证 dispose_async 将 AOF 环形缓冲区未提交帧完整落盘并可恢复
#[test]
fn test_server_dispose_async_flushes_uncommitted_frames() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("dispose_async.db");

  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions {
        commit_frequency_ms: 10_000,
        ..RuntimeServerOptions::default()
      },
      session_factory,
    )
    .expect("open with aof"),
  );
  let (server, _addr) = start_server(Arc::clone(&provider));

  let aof = provider.aof().expect("aof enabled");
  let _ = aof
    .enqueue_raw(
      AofEntryType::StoreUpsert,
      (1, 1),
      &entry_key(b"test_key"),
      b"test_val",
      &[],
    )
    .unwrap();
  let tail = aof.log().tail_address().max();
  assert!(tail > 0);
  let sublog = aof.log().get_sub_log(0);
  assert!(
    sublog.flushed_until_address() < tail,
    "预置：dispose 前未提交帧未落设备"
  );

  // 统一收口单点：dispose_async 关停网络并刷盘
  rt.block_on(server.dispose_async())
    .expect("dispose_async 成功");
  assert!(
    sublog.flushed_until_address() >= tail,
    "dispose_async 后未提交帧必须落设备"
  );

  let shutdown_tail = aof.log().tail_address().max();
  drop(server);
  drop(provider);

  // 重开恢复装配：验证未提交帧被完整回放
  let provider2 = Arc::new(
    rt.block_on(StorageSessionProvider::open_recovered_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions::default(),
      false,
      session_factory,
    ))
    .expect("open recovered"),
  );
  let recovered = provider2.recovered_aof_tail().expect("恢复位点已点亮");
  assert_eq!(
    recovered.get(0),
    Some(shutdown_tail),
    "恢复位点须等于停机前尾"
  );

  let session = provider2.store().new_session().expect("session");
  assert_eq!(
    rt.block_on(session.read(b"test_key"))
      .expect("read recovered key"),
    Some(b"test_val".to_vec()),
    "dispose_async 收口的未提交帧须随恢复重放落库"
  );
}

/// 对偶语义锁：裸 stop() 仅收口资源，不刷 AOF 尾
#[test]
fn test_server_stop_bare_does_not_flush_uncommitted_frames() {
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("bare_stop.db");

  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions {
        commit_frequency_ms: 10_000,
        ..RuntimeServerOptions::default()
      },
      session_factory,
    )
    .expect("open with aof"),
  );
  let (server, _addr) = start_server(Arc::clone(&provider));

  let aof = provider.aof().expect("aof enabled");
  let _ = aof
    .enqueue_raw(
      AofEntryType::StoreUpsert,
      (1, 1),
      &entry_key(b"bare_key"),
      b"bare_val",
      &[],
    )
    .unwrap();
  let tail = aof.log().tail_address().max();
  assert!(tail > 0);
  let sublog = aof.log().get_sub_log(0);
  assert!(
    sublog.flushed_until_address() < tail,
    "预置：未提交帧未落设备"
  );

  // 裸 stop() 仅停连接/排空缓冲，不执行 AOF 异步刷盘
  server.stop();
  assert!(
    sublog.flushed_until_address() < tail,
    "裸 stop() 之后未提交帧仍未落设备（语义边界声明符合预期）"
  );
}
