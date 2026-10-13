#![recursion_limit = "256"]
//! REPLICAOF / CLUSTER REPLICATE 慢路径执行体泵丢弃赎回守卫回归测试（票
//! wedb-cluster-replicate-sync-pump-drop-recovery-lock-orphan）。
//!
//! 背景（对标 garnet C# 一手锚）：两处发起臂均在网络线程内联 BlockingWait
//! （ReplicaOfCommand.cs:88-94、RespClusterReplicationCommands.cs:101-107）
//! 驱动同步发起链，执行序列不可被客户端断连拆解；收尾 catch
//! （ReplicaDiskbasedSync.cs:188-196 TryResetReplica）与 finally
//! （:197-214 EndRecovery）异常与正常两路都必达。rust 前台臂经
//! pending_slow 慢路径承接，执行体在 try_add_replica_async 四项登记
//! （恢复锁 / 角色翻转持久化 / 挂起主任务 / 拆旧推流驱动）之后的
//! await 窗暴露于网络泵 probe_race 丢弃面（RaceEnd::Disposed 败侧
//! future drop）——裸 drop 即 finish_replica_sync 永不可达：恢复锁滞留
//! ClusterReplicate、角色滞留副本，复制面被 begin_recovery 状态矩阵拒死
//! 无自愈通路（唯一出路进程重启）。
//!
//! 收口：queue_try_replicate_sync 执行体首挂 ReplicateSyncGuard
//! （assembly.rs，同 cluster_session/failover.rs:EpochDrainGuard 先例的
//! Drop 取消收口 + disarm 幂等形态）——未 disarm 丢弃即 spawn 补跑
//! finish_replica_sync 失败收尾（复位主 + 释放恢复锁）。
//!
//! 反证基线（revert-proof）：
//! - 删除守卫（或装配点）→ 主用例丢弃后恢复锁恒滞留 ClusterReplicate，
//!   wait_for 超时断言转红；
//! - 守卫误装在 poll 前（未构造即赎回）→ 形态边界用例纪元被误推进转红；
//! - disarm 丢失（正常臂也补跑）→ 完成链用例对 -ERR / 锁释放断言的
//!   双重收尾可在日志观测，状态断言维持（行为面由 disarm 保零变化）。

use std::{
  sync::Arc,
  task::{Context, Poll, Waker},
  time::Duration,
};

use compio::time::sleep;
use wbase::hash_slot::CLUSTER_SLOT_COUNT;
use wedb::server::{
  cluster::IClusterProvider, cluster_provider::ClusterProvider, cluster_session::ClusterSession,
  replication::recovery_status::RecoveryStatus, worker::NodeRole,
};
use wedb_test::{
  cluster_consumer_fresh::cluster_consumer_fresh, two_primary_provider::two_primary_provider,
};
use wnode::{
  ClusterSessionFace, MessageConsumerFace, RespSessionConsumer,
  resp::resp_server_session::RespServerSessionOptions,
};
use wtest_base::{resp_frame_str, wait_for};

/// 排空窗注入档：恒不追平夹具下 bump_and_wait 自挂起起至少等待此时长，
/// 令 poll 一次必 Pending（丢弃点确定性落在登记后的 await 窗内）
const DRAIN_TIMEOUT_MS: u64 = 200;

/// 泵一帧（同步段），返回即时应答（慢路径挂起时为空）
fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut out = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut out);
  assert_eq!(remaining, Some(0), "帧应被完整消费");
  out
}

/// 恒不追平夹具（failover_epoch_drain_failclose.rs 同款）：注册会话先行
/// 批首纪元快照，其后原语自 bump 恒落后 → 排空等待必挂满超时窗
fn park_lagging_session(cp: &Arc<ClusterProvider>) -> Arc<ClusterSession> {
  cp.bump_current_epoch();
  let lag = cp.create_cluster_session();
  lag.acquire_current_epoch();
  assert_eq!(lag.local_current_epoch(), cp.current_epoch());
  cp.set_cluster_node_timeout_ms(DRAIN_TIMEOUT_MS);
  lag
}

/// 双主装配（DE11 本地主 + DE12 远端主 :7001，本地全量槽）
fn provider() -> Arc<ClusterProvider> {
  two_primary_provider(None, CLUSTER_SLOT_COUNT, CLUSTER_SLOT_COUNT, &[], None)
}

/// 主用例：执行体在纪元排空 await 窗（四项登记已完成）被泵丢弃 →
/// 守卫补跑失败收尾。三面断言：
/// - 丢弃时点滞留半途态确证（恢复锁 ClusterReplicate + 角色已翻副本），
///   即票面危害现场——守卫缺失时此态即终态；
/// - 补跑后恢复锁释放（NoRecovery）、角色复位 Primary；
/// - 拒死链解除：REPLICAOF NO ONE 的 begin_recovery(ReplicaOfNoOne)
///   可再度成功（滞留时被状态矩阵拒）。
#[compio::test]
async fn pump_dropped_wait_redeems_recovery_lock_and_role() {
  let cp = provider();
  let m = cp.cluster_manager().unwrap();
  let rm = cp.replication_manager().expect("rm 在场");
  let lag = park_lagging_session(&cp);
  let mut consumer =
    cluster_consumer_fresh(&cp, "pump_drop.db", RespServerSessionOptions::default());

  // REPLICAOF 127.0.0.1 7001 → 前台发起挂慢路径（C# 网络线程 BlockingWait
  // 的 pending_slow 承接）
  let frame = resp_frame_str(&["REPLICAOF", "127.0.0.1", "7001"]);
  assert!(pump(&mut consumer, &frame).is_empty(), "慢路径无即时应答");
  let slow = consumer.take_slow_wait().expect("REPLICAOF 须挂慢路径");

  // 手动 poll 一次：驱动至纪元排空挂起窗（try_add 四项登记同步完成）。
  // Box::pin 持有所有权——Pin<&mut> 借用形的 drop 不落到底层 future，守卫
  // 丢弃链会断（std::pin::pin! 栈借用形态在此不可用）
  let mut fut = Box::pin(slow.resolve());
  let mut cx = Context::from_waker(Waker::noop());
  match fut.as_mut().poll(&mut cx) {
    Poll::Pending => {}
    Poll::Ready(v) => panic!("排空窗内不得就绪: {}", String::from_utf8_lossy(&v)),
  }

  // 丢弃时点滞留半途态确证（revert 删守卫时此态即终态，后续断言齐红）
  assert_eq!(
    rm.recovery_status(),
    RecoveryStatus::ClusterReplicate,
    "丢弃时恢复锁须滞留 ClusterReplicate（半途态现场）"
  );
  assert_eq!(
    m.current_config().local_node_role(),
    NodeRole::Replica,
    "丢弃时角色已翻副本（flush_config 已持久化）"
  );

  // 泵丢弃（RaceEnd::Disposed 的 future drop 等价注入）→ 守卫补跑
  drop(fut);
  assert!(
    wait_for(
      || rm.recovery_status() == RecoveryStatus::NoRecovery,
      Duration::from_secs(5)
    )
    .await,
    "守卫须补跑失败收尾释放恢复锁（无守卫即恒滞留 ClusterReplicate）"
  );

  // 角色复位主 + 拒死链解除：REPLICAOF NO ONE 可再度取锁
  assert_eq!(
    m.current_config().local_node_role(),
    NodeRole::Primary,
    "补跑 catch 臂须复位主角色"
  );
  assert!(
    rm.begin_recovery(RecoveryStatus::ReplicaOfNoOne, false),
    "补跑后复制面须可再度进入恢复（滞留时被 ERR_RECOVERY_LOCK 拒死）"
  );
  rm.end_recovery(RecoveryStatus::NoRecovery, false);
  drop(lag);
}

/// 对照：完成链（无 lagging 会话，排空即刻达成，attach 因本地 wal 未接线
/// 失败）守卫 disarm 退场——-ERR 应答、锁释放、角色复位全部由链内
/// finish_replica_sync 既有收口承担，行为与守卫引入前零变化
#[compio::test]
async fn completed_failure_chain_unaffected_by_guard() {
  let cp = provider();
  let m = cp.cluster_manager().unwrap();
  let rm = cp.replication_manager().expect("rm 在场");
  let mut consumer =
    cluster_consumer_fresh(&cp, "pump_done.db", RespServerSessionOptions::default());

  let frame = resp_frame_str(&["REPLICAOF", "127.0.0.1", "7001"]);
  assert!(pump(&mut consumer, &frame).is_empty());
  let slow = consumer.take_slow_wait().expect("REPLICAOF 须挂慢路径");
  let ack = slow.resolve().await;
  let text = String::from_utf8_lossy(&ack).to_string();
  assert!(
    text.starts_with("-ERR"),
    "wal 未接线 attach 必失败回 -ERR（绝不先回 OK）: {text}"
  );

  assert_eq!(
    rm.recovery_status(),
    RecoveryStatus::NoRecovery,
    "链内 finally 臂须释放恢复锁（disarm 后守卫零参与）"
  );
  assert_eq!(
    m.current_config().local_node_role(),
    NodeRole::Primary,
    "链内 catch 臂须复位主角色"
  );
  assert!(
    rm.begin_recovery(RecoveryStatus::ClusterReplicate, false),
    "完成链收场后锁须可再取"
  );
  rm.end_recovery(RecoveryStatus::NoRecovery, false);
}

/// 形态边界：未 poll 即丢弃（async 块未执行、守卫未构造、四项登记未发生）
/// → 零赎回零推进。观测面取配置纪元：补跑含 try_reset_replica（内含
/// bump_local_node_config_epoch），守卫若误在未构造时也赎回即纪元被推进
#[compio::test]
async fn unpolled_drop_constructs_no_guard_and_advances_nothing() {
  let cp = provider();
  let m = cp.cluster_manager().unwrap();
  let rm = cp.replication_manager().expect("rm 在场");
  let epoch_before = m.current_config().local_node_config_epoch();
  let mut consumer =
    cluster_consumer_fresh(&cp, "pump_unpolled.db", RespServerSessionOptions::default());

  let frame = resp_frame_str(&["REPLICAOF", "127.0.0.1", "7001"]);
  assert!(pump(&mut consumer, &frame).is_empty());
  let slow = consumer.take_slow_wait().expect("REPLICAOF 须挂慢路径");
  drop(slow);

  // 让出调度：若误补跑（spawn 异步）此刻已落账
  sleep(Duration::from_millis(100)).await;
  assert_eq!(
    rm.recovery_status(),
    RecoveryStatus::NoRecovery,
    "未 poll 丢弃零登记：状态不得变化"
  );
  assert_eq!(
    m.current_config().local_node_role(),
    NodeRole::Primary,
    "未 poll 丢弃零翻转：角色不得变化"
  );
  assert_eq!(
    m.current_config().local_node_config_epoch(),
    epoch_before,
    "未 poll 丢弃零赎回：纪元不得被补跑推进（守卫仅在首次 poll 后构造）"
  );
}
