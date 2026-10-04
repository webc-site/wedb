//! 复制同步取消安全守卫（RAII Drop）语义回归
//!
//! compio thread-per-core 下副本断连 → 网络泵慢臂 RaceEnd::Disposed → 同步
//! 编排执行体 future 被丢弃（wnode consume 泵自陈「future drop 即取消」），
//! 错误臂清理代码不可达（第 9 轮审查洞 1~3）。本组测试直接构造守卫对象，或
//! 手动 poll 至挂起点后整体丢弃 future（RaceEnd::Disposed 等价形态），断言
//! 取消臂清理：钉线出册（safe_truncate_aof 闸门解除）、批量窗标志复位、
//! 未收敛会话判败唤醒，以及 disarm 与正常臂清理的幂等共存。

use std::{
  sync::Arc,
  task::{Context, Waker},
};

use waof::AofAddress;
use wbase::future::{block_on, yield_now};
use wedb::server::{
  replication::{
    aof_sync_driver::{AofSyncDriver, AofSyncDriverStore},
    diskless_replication::{
      DisklessSyncSession, PinDriversGuard, ReplicationSyncManager, SyncBatchGuard,
      SyncSessionGuard, SyncStatus,
    },
    replica_sync_session::PinTruncationGuard,
    sync_metadata::SyncMetadata,
  },
  worker::NodeRole,
};

const LOCAL: u128 = 0x10CA1;

fn replica_meta(node_id: u128) -> SyncMetadata {
  SyncMetadata {
    full_sync: true,
    origin_node_role: NodeRole::Replica,
    origin_node_id: node_id,
    current_primary_repl_id: "replid".to_string(),
    current_store_version: 0,
    current_aof_begin_address: AofAddress::create(1, 0),
    current_aof_tail_address: AofAddress::create(1, 100),
    checkpoint_entry: None,
  }
}

fn driver(remote_node_id: u128, start: i64) -> Arc<AofSyncDriver> {
  Arc::new(AofSyncDriver::new(
    LOCAL,
    remote_node_id,
    1,
    &AofAddress::create(1, start),
    None,
  ))
}

fn reattach_ok(mgr: &ReplicationSyncManager, store: &Arc<AofSyncDriverStore>, node_id: u128) {
  assert!(
    mgr
      .add_replica_sync_session(
        "127.0.0.1:0".to_string(),
        replica_meta(node_id),
        1,
        Arc::clone(store),
      )
      .is_ok(),
    "节点 {node_id:#x} 重新入册须可行（批量窗复位 + 会话已摘除）"
  );
}

/// 预锁会话装配（对标 stream_sync 预锁段：钉线入库 + 会话回挂实例）
fn pinned_session(
  mgr: &ReplicationSyncManager,
  store: &Arc<AofSyncDriverStore>,
  node_id: u128,
  pin_start: i64,
) -> Arc<DisklessSyncSession> {
  let s = mgr
    .add_replica_sync_session(
      "127.0.0.1:0".to_string(),
      replica_meta(node_id),
      1,
      Arc::clone(store),
    )
    .expect("首入册成功");
  let d = driver(node_id, pin_start);
  assert!(store.try_add_replication_driver(Arc::clone(&d), false));
  s.add_aof_sync_task(d);
  s
}

/// 洞 1（会话登记守卫）：会话编排 future 在挂起点被整体丢弃（RaceEnd::
/// Disposed 等价形态）——守卫随 frame 丢弃，钉线出册 + 会话摘除 +
/// sync_in_progress 复位，diskless attach 不再永久被拒
#[test]
fn session_guard_future_drop_at_suspend_unpins_and_reopens_window() {
  let store = Arc::new(AofSyncDriverStore::new(1));
  let mgr = ReplicationSyncManager::new();
  let s = pinned_session(&mgr, &store, 0xA1, 10);
  assert_eq!(store.count(), 1, "前置：预锁钉线在册");

  // 手动 poll 至让渡挂起点后丢弃执行体 future：取消臂即守卫 Drop
  let mut fut = Box::pin(async {
    let _guard = SyncSessionGuard::new(&mgr, &s);
    yield_now().await;
  });
  let mut cx = Context::from_waker(Waker::noop());
  assert!(
    fut.as_mut().poll(&mut cx).is_pending(),
    "首 poll 挂起于让渡点"
  );
  drop(fut);

  assert_eq!(store.count(), 0, "取消臂必退钉出册");
  // 标志恒真按 "sync session creation failed" 拒绝、会话残留按
  // "already exists" 拒绝——重入册可行即双重复位齐证
  reattach_ok(&mgr, &store, 0xA1);
}

/// 洞 1（批窗守卫）：开窗执行体取消——未收敛会话全体判败（终态广播唤醒
/// follower 侧 wait_for_sync_completion 等待者，a 的预锁钉线经既有
/// set_status(FAILED) 实例摘除链出册）+ 清册关窗；disarm 与正常臂幂等共存
/// （终态不覆写、已清册面零扰动）
#[test]
fn batch_guard_drop_fails_pending_sessions_and_disarm_coexists() {
  let store = Arc::new(AofSyncDriverStore::new(1));
  let mgr = ReplicationSyncManager::new();
  let a = pinned_session(&mgr, &store, 0xB1, 10);
  let b = mgr
    .add_replica_sync_session(
      "127.0.0.1:0".to_string(),
      replica_meta(0xB2),
      1,
      Arc::clone(&store),
    )
    .expect("次入册成功");

  let batch = SyncBatchGuard::new(&mgr, vec![Arc::clone(&a), Arc::clone(&b)]);
  assert_eq!(batch.sessions().len(), 2);
  drop(batch); // 未 disarm = future 取消

  assert_eq!(
    a.status_info().sync_status,
    SyncStatus::Failed,
    "未收敛会话判败唤醒等待者"
  );
  assert_eq!(b.status_info().sync_status, SyncStatus::Failed);
  assert_eq!(store.count(), 0, "a 的预锁钉线经判败链出册");
  reattach_ok(&mgr, &store, 0xB1);

  // 正常臂幂等共存：先终态收敛再 disarm，drop 不覆写终态
  let c = mgr
    .add_replica_sync_session(
      "127.0.0.1:0".to_string(),
      replica_meta(0xC1),
      1,
      Arc::clone(&store),
    )
    .expect("重入册成功");
  c.set_status(SyncStatus::Success, None);
  let mut batch2 = SyncBatchGuard::new(&mgr, vec![Arc::clone(&c)]);
  batch2.disarm();
  drop(batch2);
  assert_eq!(
    c.status_info().sync_status,
    SyncStatus::Success,
    "disarm 后终态不覆写"
  );
}

/// 洞 2（批量预锁钉线守卫）：取消臂逐实例出册（已被正常臂置换的在役实例
/// 不误删）；disarm 与正常臂幂等共存
#[test]
fn pin_drivers_guard_drop_unpins_instance_matched_and_disarm_coexists() {
  let store = Arc::new(AofSyncDriverStore::new(1));
  let d1 = driver(0xD1, 10);
  let d2 = driver(0xD2, 20);
  assert!(store.try_add_replication_drivers(&[Arc::clone(&d1), Arc::clone(&d2)], false));
  // d1 已被正常臂以新实例原地置换（begin_aof_sync 原地置换对位）
  let d1_live = driver(0xD1, 55);
  assert!(store.try_add_replication_driver(Arc::clone(&d1_live), false));

  let mut guard = PinDriversGuard::new(&store, vec![Arc::clone(&d1), Arc::clone(&d2)]);
  guard.disarm();
  drop(guard);
  assert_eq!(store.count(), 2, "disarm 后正常臂持有面零扰动");

  // 取消臂：未 disarm 丢弃 → 预锁实例逐个出册，被置换的在役实例留存
  let guard = PinDriversGuard::new(&store, vec![d1, d2]);
  drop(guard);
  assert_eq!(store.count(), 1, "仅预锁实例出册");
  assert_eq!(
    store.drivers()[0].remote_node_id(),
    0xD1,
    "被置换的在役驱动留存"
  );
}

/// 洞 3（磁盘链预锁钉线守卫）：钉线在册期 safe_truncate_aof 被预锁位点
/// 钳制，取消臂（快照传送窗内执行体丢弃）退钉后截断闸门解除；disarm 与
/// 正常成功臂（在役置换）幂等共存，退场不误杀在役流
#[test]
fn pin_truncation_guard_drop_releases_truncation_clamp_and_disarm_coexists() {
  let store = Arc::new(AofSyncDriverStore::new(1));
  let pin = driver(0xE1, 5);
  assert!(store.try_add_replication_driver(Arc::clone(&pin), false));
  // 截断目标 50（非 100）：钉线出册前先抬一轮截断线，后续在役起点 77 须
  // 高于已截断位点方能通过 start_gate 挂载（与生产 data_loss_check 同判据）
  let clamped = block_on(store.safe_truncate_aof(&AofAddress::create(1, 50)));
  assert_eq!(clamped.get(0), Some(5), "钉线在册期截断被钳制于预锁位点");

  // 取消臂：future 未经 disarm 丢弃 → 按节点退钉出册
  let guard = PinTruncationGuard::new(&store, 0xE1);
  drop(guard);
  assert_eq!(store.count(), 0, "取消臂必按节点退钉");
  let unclamped = block_on(store.safe_truncate_aof(&AofAddress::create(1, 50)));
  assert_eq!(unclamped.get(0), Some(50), "退钉后截断闸门解除");

  // 正常成功臂幂等共存：预锁已被在役驱动以授予位点原地置换后 disarm，
  // 守卫退场不得误杀在役流
  let live = driver(0xE1, 77);
  assert!(store.try_add_replication_driver(Arc::clone(&live), false));
  let mut guard = PinTruncationGuard::new(&store, 0xE1);
  guard.disarm();
  drop(guard);
  assert_eq!(store.count(), 1, "disarm 后在役驱动留存");
}
