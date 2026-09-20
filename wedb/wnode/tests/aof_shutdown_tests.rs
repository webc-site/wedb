//! AOF 关停对偶回归：背压闸门停机放行 + 未提交帧刷盘收口
//!
//! 对标 C# 关停链（libs/host/GarnetServer.cs:InternalDispose Phase 3
//! Provider.Dispose → GarnetDatabase.Dispose → GarnetAppendOnlyFile.Dispose
//! = backpressure?.Dispose + Log.Dispose）：
//! 1. 闸门启用 + 复制水位钉 0 + 真实入队超预算时，滞留追加方必须在
//!    server.stop() 的有限时间内放行（C# AofBackpressure.Dispose：
//!    Release all stalled appenders permanently (server shutdown)；rust
//!    join 无超时上界，放行必须先于 join）；
//! 2. 停机 dispose 路径收口未提交帧：不经手工 flush，重开恢复装配后
//!    恢复位点与停机前尾一致。

use std::{
  sync::{Arc, mpsc::channel},
  thread,
  time::Duration,
};

use compio::runtime::Runtime;
use tempfile::tempdir;
use waof::AofEntryType;
use wnode::service::StorageSessionProvider;
use wnode_test::{session_factory, start_server};
use wtest_base::test_store_config;
use wval::{KeyTag, NamespaceDbCodec};

/// 条目入账键编码器（与主写入面 service.rs::physical_key 同一单点编码器
/// NamespaceDbCodec::encode_tagged_key）：AOF keyed 条目键一律为引擎物理键
/// [NsVarint][DbVarint][KeyTag][用户键]，回放端 KeyContextGuard 据此解域，
/// 裸用户键即被判「物理键损坏」。
fn entry_key(user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// 停机放行：闸门启用 + 水位钉 0 + 尾超预算的滞留追加方随 stop() 释放
#[test]
fn stop_releases_stalled_appenders() {
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("bp_shutdown.db");

  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      Some(50),
      session_factory,
    )
    .expect("open with aof"),
  );
  let (server, _addr) = start_server(Arc::clone(&provider));

  let aof = provider.aof().expect("aof enabled");
  let gate = aof.backpressure().expect("闸门随 GarnetLog 恒装配");
  // 启用闸门（预放行入队：此刻水位 i64::MAX，入队不被门控）
  gate.set_budget(64);
  let _ = aof
    .enqueue_raw(
      AofEntryType::StoreUpsert,
      (1, 1),
      &entry_key(b"stall"),
      &[0u8; 4096],
      &[],
    )
    .unwrap();
  let tail = aof.tail_address();
  assert!(
    tail > 64,
    "入队须推尾越预算（live tail 自愈判定以实时尾为准）"
  );
  // 模拟附着副本停止推进：发布水位钉 0
  gate.publish_shipped_address(0, 0);

  let (done_tx, done_rx) = channel();
  let gate_for_thread = Arc::clone(gate);
  thread::spawn(move || {
    // 同步慢路径滞留（poll 内阻塞，等价命令处理中的追加方）
    gate_for_thread.wait(0, tail);
    let _ = done_tx.send(());
  });
  // 给滞留线程留出注册监听的窗口
  thread::sleep(Duration::from_millis(200));
  assert!(
    done_rx.recv_timeout(Duration::ZERO).is_err(),
    "预置：stop 前追加方须仍滞留"
  );

  server.stop();

  done_rx
    .recv_timeout(Duration::from_secs(5))
    .expect("server.stop() 须在有限时间内放行滞留追加方");
}

/// 停机收口：不经手工 flush，dispose 路径刷盘后重开恢复位点与停机前尾一致
#[test]
fn dispose_flushes_uncommitted_frames_for_recovery() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("dispose_flush.db");

  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      Some(50),
      session_factory,
    )
    .expect("open with aof"),
  );
  let (server, _addr) = start_server(Arc::clone(&provider));

  // aof_commit_ms=50（auto_commit 关）且直驱入队不经存储监听：无提交发生
  let aof = provider.aof().expect("aof enabled");
  let _ = aof
    .enqueue_raw(
      AofEntryType::StoreUpsert,
      (1, 1),
      &entry_key(b"flush"),
      b"value",
      &[],
    )
    .unwrap();
  let tail = aof.tail_address();
  assert!(tail > 0);
  let sublog = aof.log().get_sub_log(0);
  assert!(
    sublog.flushed_until_address() < tail,
    "预置：dispose 前未提交帧未落设备"
  );

  // 停机 dispose 路径（wait_for_shutdown 排空后的收口动作）
  rt.block_on(aof.dispose_async());
  assert!(
    sublog.flushed_until_address() >= tail,
    "dispose 后未提交帧已落设备"
  );

  // 停机时刻尾地址：dispose 收口除刷盘外还追加提交指纹帧（waof/src/wal/log.rs
  // commit 元数据帧，非 AOF 语义条目、回放侧按帧长+指纹识别后跳过），故停机
  // 前尾以此刻实时尾为准，恢复位点须逐字节追平
  let shutdown_tail = aof.tail_address();
  server.stop();
  drop(server);
  drop(provider);

  // 重开恢复装配：恢复位点须等于停机前尾（未提交帧随收口可恢复）
  let provider2 = Arc::new(
    rt.block_on(StorageSessionProvider::open_recovered_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      None,
      false,
      session_factory,
    ))
    .expect("open recovered"),
  );
  let recovered = provider2
    .recovered_aof_tail()
    .expect("恢复位点点亮（空日志判据 begin == tail，非空即越过 0）");
  assert_eq!(
    recovered.get(0),
    Some(shutdown_tail),
    "恢复位点须等于停机前尾"
  );

  // 内容判据：位点相等不足以证明未提交帧被消化——收口帧须随恢复重放真正
  // 落库可读（物理键解域后按用户键点查 string 域）
  let session = provider2.store().new_session().expect("session");
  assert_eq!(
    rt.block_on(session.read(b"flush"))
      .expect("read recovered key"),
    Some(b"value".to_vec()),
    "dispose 收口的未提交帧须随恢复重放落库"
  );
}
