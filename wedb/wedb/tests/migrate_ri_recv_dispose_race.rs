#![recursion_limit = "256"]
//! RI 迁移接收态 dispose 竞态组件测试
//!
//! 对标 C# test/cluster/Garnet.test.cluster/RangeIndexMigrationReceiveStateTests.cs
//! 两法（dispose 与在途 ProcessRecord 的竞态，接收节点一侧网络线程 dispose
//! ClusterSession、另一侧迁移块仍在处理——dispose 绝不与在途
//! ProcessRecord 并发运行、绝不中途打断它，清理让渡至 worker 收尾）：
//! 1. ProcessRecordAfterDispose_Throws → process_record_after_dispose_is_rejected；
//! 2. DisposeDuringProcessRecord_DefersCleanupToWorker →
//!    dispose_during_in_flight_process_record_defers_cleanup。
//!
//! rust 侧机制形态差异（行为等价设计）：C# 经 CooperativeDisposeGuard + 异常
//! 注入停泊实现交错；rust 接收态宿主为会话字段
//! `Arc<AsyncLockMutex<RangeIndexMigrationReceiveState>>`（见
//! wedb/src/server/cluster_session/mod.rs 的 range_index_receive_state），互斥
//! 由异步锁结构性承担——持锁窗口即在途窗口，dispose 经同一把锁排队，天然
//! 「不并发、不打断、清理让渡收尾侧」；dispose 后的记录处理以返回 `false`
//! 拒绝（rust 无异常面，C# ObjectDisposedException 的等价承接形态）。

use std::{
  fs::read_dir,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use aok::{OK, Void};
use async_lock::Mutex as AsyncLockMutex;
use compio::{runtime::spawn, time::sleep};
use wedb_test::store_node::{StoreNode, open_store};
use wnode::range_index::{RangeIndexMigrationReceiveState, TreeStreamMeta};
use wtest_base::wait_for;
use wval::GarnetObjectType;

/// 流键（部分流永不完成，反序列化器停驻接收态）
const STREAM_KEY: &[u8] = b"ri:race";
/// 流声明文件字节数（远大于实际发送量 → 流悬停在 ReceivingFileData 态）
const DECLARED_FILE_BYTES: i64 = 1000;
/// 实际随块发送的文件字节
const SENT_FILE_BYTES: usize = 10;

/// 构造部分流块：`[4B keyLen][key][8B fileSize][部分文件字节]`
/// （fileSize ≫ 已发字节，反序列化器停在 ReceivingFileData、临时文件已开）
fn partial_chunk() -> Vec<u8> {
  let mut chunk = Vec::with_capacity(4 + STREAM_KEY.len() + 8 + SENT_FILE_BYTES);
  chunk.extend_from_slice(&(STREAM_KEY.len() as i32).to_le_bytes());
  chunk.extend_from_slice(STREAM_KEY);
  chunk.extend_from_slice(&DECLARED_FILE_BYTES.to_le_bytes());
  chunk.resize(chunk.len() + SENT_FILE_BYTES, 0);
  chunk
}

/// 纯 RI 流元（无成员挂 TTL、无键级 TTL）
fn ri_meta() -> TreeStreamMeta {
  TreeStreamMeta {
    obj_type: GarnetObjectType::RangeIndex,
    next_expiry: i64::MAX,
    expire_ticks: 0,
  }
}

/// 迁移临时目录在册 .bftree 文件数（对标 C# CountTempFiles）
fn temp_file_count(mgr: &wbftree::RangeIndexManager) -> usize {
  read_dir(mgr.migration_temp_dir())
    .map(|entries| {
      entries
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|x| x == "bftree"))
        .count()
    })
    .unwrap_or(0)
}

/// 法一 ProcessRecordAfterDispose_Throws：dispose 后状态机不在接收态，
/// 后续记录处理被拒绝（rust 无异常面，以返回 false 承接 C#
/// ObjectDisposedException 断言）
#[compio::test]
async fn process_record_after_dispose_is_rejected() -> Void {
  let StoreNode { _dir, store } = open_store("ri_race_disposed.db");
  let mut state = RangeIndexMigrationReceiveState::new(store.range_index().clone());

  state.dispose();
  assert!(
    !state.is_receiving(),
    "dispose 后状态机不得处于接收态（C# IsReceiving 假断言）"
  );

  let session = store.new_session().expect("存储会话");
  let ok = state
    .process_record(&partial_chunk(), ri_meta(), &session, false)
    .await;
  assert!(
    !ok,
    "dispose 后的记录处理必须被拒绝（C# ObjectDisposedException 的 rust 承接形态）"
  );
  assert!(!state.is_receiving(), "被拒绝的记录不得留下半开接收态");
  OK
}

/// 法二 DisposeDuringProcessRecord_DefersCleanupToWorker：dispose 与在途
/// ProcessRecord 交叠——dispose 必须被在途窗（互斥锁）挡住不并发清理，
/// worker 出窗后清理恰一次落地（临时快照文件删净、接收态复位），且
/// dispose 后续到记录处理被拒绝
#[compio::test]
async fn dispose_during_in_flight_process_record_defers_cleanup() -> Void {
  // 在途窗四相标志（单线程运行时 + FIFO 异步锁，定序无竞态）
  let processing = Arc::new(AtomicBool::new(false));
  let processed = Arc::new(AtomicBool::new(false));
  let release_now = Arc::new(AtomicBool::new(false));
  let disposer_done = Arc::new(AtomicBool::new(false));

  let StoreNode { _dir, store } = open_store("ri_race_overlap.db");
  let mgr = store.range_index().clone();
  let state = Arc::new(AsyncLockMutex::new(RangeIndexMigrationReceiveState::new(
    Arc::clone(&mgr),
  )));
  let chunk = partial_chunk();
  let session = store.new_session().expect("存储会话");

  // ===== worker：锁窗即在途窗（结构等价 C# 持 CooperativeDisposeGuard）=====
  let worker_state = Arc::clone(&state);
  let worker_processing = Arc::clone(&processing);
  let worker_processed = Arc::clone(&processed);
  let worker_release = Arc::clone(&release_now);
  let worker = spawn(async move {
    let mut guard = worker_state.lock().await;
    worker_processing.store(true, Ordering::Release);
    let ok = guard
      .process_record(&chunk, ri_meta(), &session, false)
      .await;
    assert!(ok, "在途 process_record 处理部分流块不得失败");
    worker_processed.store(true, Ordering::Release);
    // 主任务完成「dispose 被挡」观测前，worker 不得出窗
    while !worker_release.load(Ordering::Acquire) {
      sleep(Duration::from_millis(5)).await;
    }
    drop(guard); // 在途窗收口（C# worker finally 段的对位时点）
  });

  // ===== 主任务：等 worker 深入在途窗（临时快照文件已建 = 反序列化器在飞）=====
  assert!(
    wait_for(
      || { processing.load(Ordering::Acquire) && temp_file_count(&mgr) == 1 },
      Duration::from_secs(5),
    )
    .await,
    "worker 应抵达在途窗且临时快照文件应已建（对标 C# CountTempFiles==1 断言）"
  );
  assert!(
    state.try_lock().is_none(),
    "在途窗开启期间互斥锁必须被 worker 持有（C# IsReceiving 真断言的结构对位）"
  );

  // ===== dispose 与在途 ProcessRecord 赛跑：只能停泊在锁上 =====
  let disposer_state = Arc::clone(&state);
  let disposer_flag = Arc::clone(&disposer_done);
  let disposer = spawn(async move {
    let mut guard = disposer_state.lock().await;
    guard.dispose();
    disposer_flag.store(true, Ordering::Release);
  });
  // 数拍让渡：dispose 必须仍被在途窗挡住（绝不并发清理、绝不打断在途处理）
  sleep(Duration::from_millis(30)).await;
  assert!(
    !disposer_done.load(Ordering::Acquire),
    "在途窗内 dispose 不得完成：清理必须让渡至 worker 出窗（C# 防并发核心断言）"
  );
  assert!(
    processed.load(Ordering::Acquire),
    "dispose 停泊期间在途 process_record 应已不受干扰地完成"
  );

  // ===== 放行 worker 出窗：dispose 落地，清理恰一次归属收尾侧 =====
  release_now.store(true, Ordering::Release);
  assert!(
    wait_for(
      || disposer_done.load(Ordering::Acquire),
      Duration::from_secs(5)
    )
    .await,
    "worker 出窗后被让渡的 dispose 应完成"
  );
  worker.await.expect("worker 任务不应 panic");
  disposer.await.expect("disposer 任务不应 panic");
  assert_eq!(
    temp_file_count(&mgr),
    0,
    "延迟清理应删除临时快照文件恰一次（C# CountTempFiles==0 断言）"
  );
  {
    let guard = state.lock().await;
    assert!(
      !guard.is_receiving(),
      "复位应清空反序列化器（C# IsReceiving 假断言）"
    );
  }

  // ===== dispose 后续到记录处理必须被拒绝 =====
  let session_after = store.new_session().expect("收尾读会话");
  let mut guard = state.lock().await;
  let ok = guard
    .process_record(&partial_chunk(), ri_meta(), &session_after, false)
    .await;
  assert!(
    !ok,
    "dispose 后续到记录处理必须被拒绝（C# 二次 ObjectDisposedException 断言）"
  );
  OK
}
