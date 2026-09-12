//! 读取一致性管理器（对标 libs/server/AOF/ReadConsistency/
//! ReadConsistencyManager.cs:ReadConsistencyManager）
//!
//! 多物理子日志拓扑下的前缀一致读协议：按虚拟子日志维护 key → 序列号草图
//! 与前沿，读侧在跨子日志读取前等待目标子日志回放越过会话序列号；可选
//! 漂移约束（主动扫描 + 反应式栅栏）收敛领先/滞后子日志间的时间差。

use std::{
  sync::atomic::{AtomicI64, Ordering},
  time::Duration,
};

use super::{
  replay_align_barrier::ReplayAlignBarrier,
  replica_read_session_context::ReplicaReadSessionContext,
  virtual_sublog_replay_state::VirtualSublogReplayState,
};
use crate::aof::garnet_log::GarnetLog;

/// 读取一致性管理器。
pub struct ReadConsistencyManager {
  /// 管理器版本（副本重挂主节点时递增；C# CurrentVersion）。
  current_version: AtomicI64,
  /// 物理子日志数。
  physical_sublog_count: usize,
  /// 回放任务数。
  replay_task_count: usize,
  /// 每虚拟子日志回放状态（C# vsrs）。
  vsrs: Vec<VirtualSublogReplayState>,
  /// 主动漂移扫描是否启用（阈值与频率均 > 0 且多虚拟子日志）。
  proactive_replay_drift_check_enabled: bool,
  /// 允许的最大漂移（序列号单位；-1 = 关闭栅栏）。
  replay_drift_threshold: i64,
  /// 反应式漂移约束是否启用（阈值 >= 0 且多虚拟子日志）。
  reactive_replay_drift_check_enabled: bool,
  /// 漂移扫描窗口步长（max(1, freq × threshold) × 虚拟子日志数）。
  replay_drift_interval: i64,
  /// 跨子日志回放对齐栅栏。
  pub replay_barrier: ReplayAlignBarrier,
}

impl ReadConsistencyManager {
  /// 构造（C# 主构造的显式参数形态：拓扑 + 漂移配置）。
  pub fn new(
    current_version: i64,
    physical_sublog_count: usize,
    replay_task_count: usize,
    replay_drift_threshold: i64,
    replay_drift_check_freq: i64,
  ) -> Self {
    let virtual_sublog_count = physical_sublog_count.max(1) * replay_task_count.max(1);
    let window_length = (replay_drift_check_freq.max(0) * replay_drift_threshold.max(0)).max(1);
    let proactive =
      replay_drift_check_freq > 0 && replay_drift_threshold >= 0 && virtual_sublog_count > 1;
    let reactive = replay_drift_threshold >= 0 && virtual_sublog_count > 1;
    // 主动扫描按窗口轮转分派给各子日志（C# BuildVirtualSublogReplayStates）
    let vsrs = (0..virtual_sublog_count)
      .map(|idx| {
        VirtualSublogReplayState::new(if proactive {
          (idx as i64) * window_length
        } else {
          i64::MAX
        })
      })
      .collect();
    let replay_drift_interval = window_length * virtual_sublog_count as i64;
    Self {
      current_version: AtomicI64::new(current_version),
      physical_sublog_count: physical_sublog_count.max(1),
      replay_task_count: replay_task_count.max(1),
      vsrs,
      proactive_replay_drift_check_enabled: proactive,
      replay_drift_threshold,
      reactive_replay_drift_check_enabled: reactive,
      replay_drift_interval,
      replay_barrier: ReplayAlignBarrier::new(virtual_sublog_count, None),
    }
  }

  /// 管理器版本。
  #[inline]
  pub fn current_version(&self) -> i64 {
    self.current_version.load(Ordering::Acquire)
  }

  /// 虚拟子日志数。
  pub fn virtual_sublog_count(&self) -> usize {
    self.vsrs.len()
  }

  /// key 哈希（GarnetLog::HASH 同源；本域独立暴露供无日志句柄侧使用）。
  #[inline]
  pub fn key_hash(&self, key: &[u8]) -> i64 {
    GarnetLog::hash(key)
  }

  /// 哈希 → 虚拟子日志下标（C# appendOnlyFile.Log.GetVirtualSublogIdx 同式）。
  #[inline]
  pub fn virtual_sublog_idx_of_hash(&self, hash: i64) -> usize {
    ((hash as u64) % (self.physical_sublog_count as u64)) as usize * self.replay_task_count
      + ((hash as u64) / (self.physical_sublog_count as u64) % (self.replay_task_count as u64))
        as usize
  }

  /// 物理子日志 × 回放任务 → 虚拟子日志下标（C# GetVirtualSublogIdx）。
  #[inline]
  pub fn get_virtual_sublog_idx(&self, sublog_idx: usize, replay_idx: usize) -> usize {
    sublog_idx * self.replay_task_count + replay_idx
  }

  /// 虚拟子日志状态。
  #[inline]
  pub fn vsr(&self, virtual_sublog_idx: usize) -> &VirtualSublogReplayState {
    &self.vsrs[virtual_sublog_idx.min(self.vsrs.len() - 1)]
  }

  /// libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:GetKeySequenceNumber
  ///
  /// key 的序列号；`frontier` = true 时取前沿（key 序列号与子日志最大值的较大者）。
  pub fn get_key_sequence_number(&self, key: &[u8], frontier: bool) -> i64 {
    let hash = GarnetLog::hash(key);
    if frontier {
      self.get_sublog_frontier_sequence_number(hash)
    } else {
      self.get_key_sequence_number_by_hash(hash)
    }
  }

  /// key 序列号（哈希形态）。
  pub fn get_key_sequence_number_by_hash(&self, hash: i64) -> i64 {
    self
      .vsr(self.virtual_sublog_idx_of_hash(hash))
      .get_key_sequence_number(hash)
  }

  /// libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:GetSublogFrontierSequenceNumber
  pub fn get_sublog_frontier_sequence_number(&self, hash: i64) -> i64 {
    self
      .vsr(self.virtual_sublog_idx_of_hash(hash))
      .get_frontier_sequence_number(hash)
  }

  /// libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:GetPhysicalSublogMaxReplayedSequenceNumber
  ///
  /// 各物理子日志最大已回放序列号快照（跨其全部虚拟子日志取 max）。
  pub fn get_physical_sublog_max_replayed_sequence_number(&self) -> Vec<i64> {
    (0..self.physical_sublog_count)
      .map(|physical_sublog_idx| self.get_physical_sublog_max(physical_sublog_idx))
      .collect()
  }

  /// libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:GetPhysicalSublogMax
  ///
  /// 单物理子日志跨全部虚拟子日志的最大已回放序列号。
  pub fn get_physical_sublog_max(&self, physical_sublog_idx: usize) -> i64 {
    let start_idx = self.get_virtual_sublog_idx(physical_sublog_idx, 0);
    (0..self.replay_task_count)
      .map(|rt| self.vsr(start_idx + rt).max())
      .max()
      .unwrap_or(0)
  }

  /// libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:GetPhysicalSublogMaxSequenceVector
  ///
  /// 逗号分隔的各物理子日志最大序列号（诊断面）。
  pub fn get_physical_sublog_max_sequence_vector(&self) -> String {
    let mut sb = String::new();
    let mut buf = itoa::Buffer::new();
    for idx in 0..self.physical_sublog_count {
      if !sb.is_empty() {
        sb.push(',');
      }
      sb.push_str(buf.format(self.get_physical_sublog_max(idx)));
    }
    sb
  }

  /// libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:GetPhysicalSublogMaxDriftSequenceVector
  ///
  /// 各物理子日志相对全局最大值的漂移量（诊断面）。
  pub fn get_physical_sublog_max_drift_sequence_vector(&self) -> String {
    let maxes: Vec<i64> = (0..self.physical_sublog_count)
      .map(|idx| self.get_physical_sublog_max(idx))
      .collect();
    let max_sequence_number = maxes.iter().copied().max().unwrap_or(0);
    let mut sb = String::new();
    let mut buf = itoa::Buffer::new();
    for m in maxes {
      if !sb.is_empty() {
        sb.push(',');
      }
      sb.push_str(buf.format(max_sequence_number - m));
    }
    sb
  }

  /// libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:UpdatePhysicalSublogMaxSequenceNumber
  ///
  /// 推进物理子日志全部虚拟子日志的最大序列号。
  pub fn update_physical_sublog_max_sequence_number(
    &self,
    physical_sublog_idx: usize,
    sequence_number: i64,
  ) {
    let start_idx = self.get_virtual_sublog_idx(physical_sublog_idx, 0);
    for rt in 0..self.replay_task_count {
      self
        .vsr(start_idx + rt)
        .update_max_sequence_number(sequence_number);
    }
  }

  /// libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:AdvanceVirtualSublogTime
  ///
  /// 主侧带内脉冲推进空闲虚拟子日志时间，并向活动对齐轮登记非阻塞到场。
  pub fn advance_virtual_sublog_time(&self, virtual_sublog_idx: usize, sequence_number: i64) {
    let vsr = self.vsr(virtual_sublog_idx);
    vsr.update_max_sequence_number(sequence_number);
    self
      .replay_barrier
      .signal_arrival(virtual_sublog_idx, vsr.max());
  }

  /// libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:UpdateVirtualSublogMaxSequenceNumber
  pub fn update_virtual_sublog_max_sequence_number(
    &self,
    virtual_sublog_idx: usize,
    sequence_number: i64,
  ) {
    self
      .vsr(virtual_sublog_idx)
      .update_max_sequence_number(sequence_number);
  }

  /// libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:UpdateVirtualSublogKeySequenceNumber
  ///
  /// 回放线程推进 key 时间戳：先发前沿（防栅栏死锁）→ 主动漂移扫描（窗口
  /// 轮转）→ 栅栏对齐等待 → 延迟发布 key 草图（deferred-KRT 顺序）。
  pub fn update_virtual_sublog_key_sequence_number(
    &self,
    virtual_sublog_idx: usize,
    key_hash: i64,
    sequence_number: i64,
  ) {
    let vsr = self.vsr(virtual_sublog_idx);
    // 前沿先行发布：保证栅栏到达的确定性
    vsr.update_max_sequence_number(sequence_number);

    // 主动漂移扫描：跨入本子日志所辖窗口时扫描并按需开栅栏轮
    if self.proactive_replay_drift_check_enabled
      && sequence_number >= vsr.next_drift_check_window_lower_bound()
    {
      let interval = self.replay_drift_interval;
      let mut next = vsr.next_drift_check_window_lower_bound() + interval;
      if next <= sequence_number {
        next += ((sequence_number - next) / interval + 1) * interval;
      }
      vsr.set_next_drift_check_window_lower_bound(next);
      self.bound_replay_drift();
    }

    // 与活动轮对齐：领先子日志在目标处暂停（无轮次时单次读 + 比较）
    self
      .replay_barrier
      .signal_arrival_and_wait(virtual_sublog_idx, vsr.max());

    // key 草图延后发布：停等期间读者仍见旧值
    vsr.update_key_sequence_number(key_hash, sequence_number);
  }

  /// 按哈希路由的 key 序列号推进（存储过程/无子日志下标侧入口）。
  pub fn update_key_sequence_number_by_hash(&self, key_hash: i64, sequence_number: i64) {
    let idx = self.virtual_sublog_idx_of_hash(key_hash);
    self
      .vsr(idx)
      .update_key_sequence_number(key_hash, sequence_number);
  }

  /// libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:CheckConsistencyManagerVersion
  ///
  /// 会话上下文与当前版本同步：首次或版本变更即重置会话状态。
  pub fn check_consistency_manager_version(
    &self,
    replica_read_session_context: &mut ReplicaReadSessionContext,
  ) {
    if replica_read_session_context.session_version() != self.current_version() {
      replica_read_session_context.set_session_version(self.current_version());
      replica_read_session_context.set_last_virtual_sublog_idx(-1);
      replica_read_session_context.set_maximum_session_sequence_number(0);
      replica_read_session_context.reset_cached_sublog_max();
    }
  }

  /// libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:VerifyKeyFreshness
  ///
  /// 读前新鲜度校验：跨子日志且会话序列号不低于缓存前沿时等待回放推进。
  pub fn verify_key_freshness(
    &self,
    key_hash: i64,
    replica_read_session_context: &mut ReplicaReadSessionContext,
    timeout: Duration,
  ) {
    let virtual_sublog_idx = self.virtual_sublog_idx_of_hash(key_hash);
    let last_idx = replica_read_session_context.last_virtual_sublog_idx();
    let init_or_same_sublog = last_idx == -1 || last_idx as usize == virtual_sublog_idx;
    let mssn = replica_read_session_context.maximum_session_sequence_number();

    // 预取草图槽（读后更新路径）
    let vsr = self.vsr(virtual_sublog_idx);
    vsr.prefetch_key_sequence_number(key_hash);

    if !init_or_same_sublog
      && mssn >= replica_read_session_context.cached_sublog_max(virtual_sublog_idx)
    {
      // 刷新缓存视图
      let sketch_max_value = vsr.max();
      replica_read_session_context.set_cached_sublog_max(virtual_sublog_idx, sketch_max_value);

      // 乐观无锁检查
      if mssn >= sketch_max_value {
        // 即将阻塞：值得约束则开栅栏轮
        self.bound_replay_drift();
        if !vsr.wait_for_sequence_number(mssn, &replica_read_session_context.waiter(), timeout) {
          // 超时：C# 抛 TimeoutException 中止一致读；rust 由调用方按超时处置
        }
        replica_read_session_context.set_cached_sublog_max(virtual_sublog_idx, vsr.max());
      }
    }

    // 留待读后更新
    replica_read_session_context.set_last_virtual_sublog_idx(virtual_sublog_idx as i32);
    replica_read_session_context.set_last_hash(key_hash);
  }

  /// libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:BoundReplayDrift
  ///
  /// 扫描全部虚拟子日志前沿；漂移超阈值即按领先者开对齐轮。
  pub fn bound_replay_drift(&self) {
    if !self.reactive_replay_drift_check_enabled {
      return;
    }
    if self.replay_barrier.in_progress() {
      return;
    }
    let mut min_frontier = i64::MAX;
    let mut max_frontier = i64::MIN;
    for vsr in &self.vsrs {
      let frontier = vsr.max();
      min_frontier = min_frontier.min(frontier);
      max_frontier = max_frontier.max(frontier);
    }
    if max_frontier - min_frontier <= self.replay_drift_threshold {
      return;
    }
    self.replay_barrier.try_open_round(max_frontier);
  }

  /// libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:PreSingleKeyConsistentRead
  ///
  /// 单 key 一致读前半协议：版本检查 + 新鲜度校验（store.Read 之前执行）。
  pub fn pre_single_key_consistent_read(
    &self,
    hash: i64,
    replica_read_session_context: &mut ReplicaReadSessionContext,
    timeout: Duration,
  ) {
    self.check_consistency_manager_version(replica_read_session_context);
    self.verify_key_freshness(hash, replica_read_session_context, timeout);
  }

  /// libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:PostSingleKeyConsistentRead
  ///
  /// 单 key 一致读后半协议：store.Read 后以 key 序列号推进会话序列号
  ///（确保 manager 追踪值为过估）。
  pub fn post_single_key_consistent_read(
    &self,
    replica_read_session_context: &mut ReplicaReadSessionContext,
  ) {
    let key_sequence_number =
      self.get_key_sequence_number_by_hash(replica_read_session_context.last_hash());
    replica_read_session_context.advance_maximum_session_sequence_number(key_sequence_number);
  }

  /// libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:PreBatchKeyConsistentRead
  ///
  /// 批量一致读前半协议：新鲜度校验 + 记录键哈希与会话序列号。
  /// 返回键哈希（读后校验复用）。
  pub fn pre_batch_key_consistent_read(
    &self,
    key: &[u8],
    batch_read_context: &mut ReplicaReadSessionContext,
    timeout: Duration,
  ) -> i64 {
    let hash = GarnetLog::hash(key);
    self.verify_key_freshness(hash, batch_read_context, timeout);
    let key_sequence_number = self.get_key_sequence_number_by_hash(batch_read_context.last_hash());
    batch_read_context.advance_maximum_session_sequence_number(key_sequence_number);
    hash
  }

  /// libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:PostBatchKeyConsistentReadValidate
  ///
  /// 批量读后校验：key 序列号未越过批快照边界则前缀一致。
  pub fn post_batch_key_consistent_read_validate(
    &self,
    hash: i64,
    batch_read_context: &ReplicaReadSessionContext,
  ) -> bool {
    let key_sequence_number = self.get_key_sequence_number_by_hash(hash);
    key_sequence_number <= batch_read_context.maximum_session_sequence_number()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn manager() -> ReadConsistencyManager {
    // 2 物理 × 2 回放 = 4 虚拟子日志；漂移关闭（单机测试确定性）
    ReadConsistencyManager::new(1, 2, 2, -1, 0)
  }

  #[test]
  fn version_check_resets_context_once() {
    let m = manager();
    let mut ctx = ReplicaReadSessionContext::default();
    m.check_consistency_manager_version(&mut ctx);
    assert_eq!(ctx.session_version(), 1);
    ctx.set_maximum_session_sequence_number(50);
    m.check_consistency_manager_version(&mut ctx);
    assert_eq!(ctx.maximum_session_sequence_number(), 50, "同版本不重置");
  }

  #[test]
  fn update_and_read_key_sequence_number() {
    let m = manager();
    let key = b"rk";
    assert_eq!(m.get_key_sequence_number(key, false), 0);
    m.update_key_sequence_number_by_hash(GarnetLog::hash(key), 11);
    assert_eq!(m.get_key_sequence_number(key, false), 11);
    assert!(m.get_key_sequence_number(key, true) >= 11);
  }

  #[test]
  fn physical_sublog_max_vector_and_drift() {
    let m = manager();
    m.update_physical_sublog_max_sequence_number(0, 30);
    m.update_physical_sublog_max_sequence_number(1, 10);
    assert_eq!(m.get_physical_sublog_max(0), 30);
    assert_eq!(m.get_physical_sublog_max(1), 10);
    assert_eq!(m.get_physical_sublog_max_sequence_vector(), "30,10");
    assert_eq!(m.get_physical_sublog_max_drift_sequence_vector(), "0,20");
    assert_eq!(
      m.get_physical_sublog_max_replayed_sequence_number(),
      vec![30, 10]
    );
  }

  #[test]
  fn virtual_sublog_routing_matches_formula() {
    let m = manager();
    for hash in [0i64, 1, 12345, i64::MAX, i64::MIN] {
      let expected = ((hash as u64) % 2) as usize * 2 + ((hash as u64) / 2 % 2) as usize;
      assert_eq!(m.virtual_sublog_idx_of_hash(hash), expected);
      assert!(m.virtual_sublog_idx_of_hash(hash) < 4);
    }
    assert_eq!(m.get_virtual_sublog_idx(1, 2), 4);
  }

  #[test]
  fn consistent_read_protocol_single_key() {
    let m = manager();
    let key = b"proto";
    let hash = GarnetLog::hash(key);
    m.update_key_sequence_number_by_hash(hash, 5);

    let mut ctx = ReplicaReadSessionContext::default();
    m.pre_single_key_consistent_read(hash, &mut ctx, Duration::from_millis(10));
    // 同子日志首读：无需等待即可推进
    m.post_single_key_consistent_read(&mut ctx);
    assert!(ctx.maximum_session_sequence_number() >= 5);
  }

  #[test]
  fn batch_protocol_cross_sublog_wait_and_validate() {
    let m = manager();
    let k1 = b"b1";
    let k2 = b"b2";
    // 两键路由到不同子日志（穷举到一对）
    let (h1, h2) = (GarnetLog::hash(k1), GarnetLog::hash(k2));
    if m.virtual_sublog_idx_of_hash(h1) == m.virtual_sublog_idx_of_hash(h2) {
      return;
    }
    m.update_key_sequence_number_by_hash(h1, 3);

    let mut ctx = ReplicaReadSessionContext::default();
    let got1 = m.pre_batch_key_consistent_read(k1, &mut ctx, Duration::from_millis(10));
    // 第二键所在子日志已回放超越会话序列号：等待后通过
    m.update_virtual_sublog_key_sequence_number(m.virtual_sublog_idx_of_hash(h2), h2, 8);
    let got2 = m.pre_batch_key_consistent_read(k2, &mut ctx, Duration::from_millis(500));
    assert!(m.post_batch_key_consistent_read_validate(got1, &ctx));
    assert!(m.post_batch_key_consistent_read_validate(got2, &ctx));
    assert!(ctx.maximum_session_sequence_number() >= 3);
  }

  #[test]
  fn drift_bounding_opens_barrier_round() {
    // 阈值 5：漂移 > 5 即开轮
    let m = ReadConsistencyManager::new(1, 2, 1, 5, 0);
    assert!(!m.replay_barrier.in_progress());
    m.update_virtual_sublog_max_sequence_number(0, 100);
    m.bound_replay_drift();
    assert!(m.replay_barrier.in_progress(), "漂移 100 应开轮");
    // 轮次进行中不重复开
    m.bound_replay_drift();
    assert!(m.replay_barrier.in_progress());
  }

  #[test]
  fn advance_virtual_sublog_time_signals_barrier() {
    let m = ReadConsistencyManager::new(1, 1, 2, 5, 0);
    m.replay_barrier.try_open_round(10);
    // 推进至目标：到场计数（另一虚拟子日志亦到场后放行）
    m.advance_virtual_sublog_time(0, 12);
    m.advance_virtual_sublog_time(1, 12);
    assert!(!m.replay_barrier.in_progress());
  }
}
