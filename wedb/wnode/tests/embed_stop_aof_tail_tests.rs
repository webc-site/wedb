//! 直嵌 GarnetServer 停机契约：dispose_async 落盘停机 vs 裸 drop 资源收口
//!
//! 对标 C# 句柄式嵌入停机契约（libs/host/GarnetServer.cs:InternalDispose
//! Phase 3 Provider.Dispose → GarnetDatabase.Dispose → GarnetAppendOnlyFile.
//! Dispose：Dispose 同步链内完成 AOF 落盘）：
//! 1. server.dispose_async() = stop() + AOF 尾刷唯一收口单点（信号路径
//!    wait_for_shutdown 同点合一），环形缓冲未提交帧落设备、重开恢复可重放；
//! 2. 裸 drop = 资源收口不含 AOF 尾刷（server.rs 文档契约），未提交帧弃置，
//!    对偶语义锁防隐式兜底回归。

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::tempdir;
use waof::AofEntryType;
use wconf::RuntimeServerOptions;
use wnode::service::StorageSessionProvider;
use wnode_test::{session_factory, start_server};
use wtest_base::test_store_config;
use wval::{KeyTag, NamespaceDbCodec};

/// 条目入账键编码器（与主写入面 service.rs::physical_key 同一单点编码器
/// NamespaceDbCodec::encode_tagged_key）：AOF keyed 条目键一律为引擎物理键
/// [NsVarint][DbVarint][KeyTag][用户键]，回放端 KeyContextGuard 据此解域
fn entry_key(user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// 直嵌形态落盘停机：dispose_async（stop + 尾刷单点）后未提交帧入盘，
/// 重开恢复位点追平、帧重放落库可读（wedb_test::node 同款用法形态）
#[test]
fn embedded_dispose_async_flushes_tail_for_recovery() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("embed_dispose.db");

  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions {
        commit_frequency_ms: 50,
        ..RuntimeServerOptions::default()
      },
      session_factory,
    )
    .expect("open with aof"),
  );
  let (server, _addr) = start_server(Arc::clone(&provider));

  // 直驱入队未提交帧（aof_commit_ms=50 形态无周期提交、不经存储监听）
  let aof = provider.aof().expect("aof enabled");
  let _ = aof
    .enqueue_raw(
      AofEntryType::StoreUpsert,
      (1, 1),
      &entry_key(b"embed-tail"),
      b"v1",
      &[],
    )
    .unwrap();
  let tail = aof.log().tail_address().max();
  assert!(tail > 0);
  let sublog = aof.log().get_sub_log(0);
  assert!(
    sublog.flushed_until_address() < tail,
    "预置：dispose_async 前未提交帧未落设备"
  );

  // 直嵌落盘停机唯一公开入口：stop 排空 + 主运行时直驱尾刷
  let _ = rt.block_on(server.dispose_async());
  assert!(
    sublog.flushed_until_address() >= tail,
    "dispose_async 后未提交帧已落设备"
  );

  drop(server);
  drop(provider);

  // 重开恢复装配：未提交帧随尾刷可恢复重放，键落库可读
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
  let session = provider2.store().new_session().expect("session");
  assert_eq!(
    rt.block_on(session.read(b"embed-tail")).expect("read"),
    Some(b"v1".to_vec()),
    "dispose_async 收口的未提交帧须随恢复重放落库"
  );
}

/// 对偶语义锁：裸 drop = 资源收口（stop）不含 AOF 尾刷，环形缓冲未提交帧
/// 弃置不入盘（文档契约，无隐式兜底；落盘停机须走 dispose_async）
#[test]
fn bare_drop_discards_uncommitted_frames() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("embed_bare_drop.db");

  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions {
        commit_frequency_ms: 50,
        ..RuntimeServerOptions::default()
      },
      session_factory,
    )
    .expect("open with aof"),
  );
  let (server, _addr) = start_server(Arc::clone(&provider));

  let aof = provider.aof().expect("aof enabled");
  // 先提交一条基线帧，确保 WAL 段在物理设备上就绪
  let _ = aof
    .enqueue_raw(
      AofEntryType::StoreUpsert,
      (1, 1),
      &entry_key(b"embed-committed"),
      b"v0",
      &[],
    )
    .unwrap();
  rt.block_on(aof.log().commit_async());

  let _ = aof
    .enqueue_raw(
      AofEntryType::StoreUpsert,
      (1, 1),
      &entry_key(b"embed-drop"),
      b"lost",
      &[],
    )
    .unwrap();
  let tail = aof.log().tail_address().max();
  assert!(tail > 0);
  let sublog = aof.log().get_sub_log(0);
  assert!(
    sublog.flushed_until_address() < tail,
    "预置：drop 前未提交帧未落设备"
  );

  // 裸 drop：仅资源收口，AOF 尾随环形缓冲弃置
  drop(server);
  drop(provider);

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
  let session = provider2.store().new_session().expect("session");
  assert_eq!(
    rt.block_on(session.read(b"embed-committed")).expect("read"),
    Some(b"v0".to_vec()),
    "已提交帧在恢复后可读"
  );
  assert_eq!(
    rt.block_on(session.read(b"embed-drop")).expect("read"),
    None,
    "裸 drop 弃置未提交帧（落盘停机须走 dispose_async / wait_for_shutdown）"
  );
}
