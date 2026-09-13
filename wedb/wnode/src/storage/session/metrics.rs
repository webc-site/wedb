//! 会话级存储指标（对标 libs/server/Storage/Session/Metrics.cs，C# 为 StorageSession partial）

use std::sync::atomic::Ordering::Relaxed;

use wdev::Device;
use wkv::ConsistentReadFunctions;

use super::storage_session::StorageSession;

impl<'a, D: Device, CR: ConsistentReadFunctions> StorageSession<'a, D, CR> {
  /// 会话命中计数 +1
  ///
  /// libs/server/Storage/Session/Metrics.cs:incr_session_found
  pub fn incr_session_found(&self) {
    self.session_found.fetch_add(1, Relaxed);
  }

  /// 会话未命中计数 +1
  ///
  /// libs/server/Storage/Session/Metrics.cs:incr_session_notfound
  pub fn incr_session_notfound(&self) {
    self.session_notfound.fetch_add(1, Relaxed);
  }

  /// 会话 pending（异步闭环）计数 +1
  ///
  /// libs/server/Storage/Session/Metrics.cs:incr_session_pending
  pub fn incr_session_pending(&self) {
    self.session_pending.fetch_add(1, Relaxed);
  }

  /// 开始 pending 等待计时（记录当前毫秒时间戳）
  ///
  /// libs/server/Storage/Session/Metrics.cs:StartPendingMetrics
  pub fn start_pending_metrics(&self) {
    let now_ms = coarsetime::Clock::now_since_epoch().as_millis();
    self.pending_start_ms.store(now_ms, Relaxed);
  }

  /// 结束 pending 等待计时并累计到总等待时长
  ///
  /// libs/server/Storage/Session/Metrics.cs:StopPendingMetrics
  pub fn stop_pending_metrics(&self) {
    let start = self.pending_start_ms.swap(0, Relaxed);
    if start != 0 {
      let now_ms = coarsetime::Clock::now_since_epoch().as_millis();
      self
        .pending_total_ms
        .fetch_add(now_ms.saturating_sub(start), Relaxed);
    }
  }
}
