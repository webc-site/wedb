use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// RespServerSession 发出的性能指标快照（对标
/// libs/server/Metrics/GarnetSessionMetrics.cs:GarnetSessionMetrics 的字段面）。
///
/// C# 为每会话实例的普通字段累加（单写者，无锁）；rust 会话体与存储执行域跨
/// compio 任务共享同一指标对象，写面由 [`SessionMetricsHandle`]（原子承接）
/// 承担，本结构为纯数据快照与监视器聚合体（add/reset 对位 C# Add/Reset）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GarnetSessionMetrics {
  /// 网络入字节累计。
  pub total_net_input_bytes: u64,
  /// 网络出字节累计。
  pub total_net_output_bytes: u64,
  /// 已处理命令累计。
  pub total_commands_processed: u64,
  /// pending 累计。
  pub total_pending: u64,
  /// 命中累计。
  pub total_found: u64,
  /// 未命中累计。
  pub total_notfound: u64,
  /// 集群命令处理累计。
  pub total_cluster_commands_processed: u64,
  /// 写命令执行计数。
  pub total_write_commands_processed: u64,
  /// 读命令执行计数。
  pub total_read_commands_processed: u64,
  /// 收到的事务命令计数。
  pub total_transactions_commands_received: u64,
  /// 全部 resp 服务器会话 try consume 触发的异常总数。
  pub total_number_resp_server_session_exceptions: u64,
  /// 成功执行的事务总数（C# 字段名与注释语义相反，此处保持同名同值行为）。
  pub total_transaction_commands_execution_failed: u64,
}

impl GarnetSessionMetrics {
  /// libs/server/Metrics/GarnetSessionMetrics.cs:Add
  ///
  /// 将另一份会话指标并入本实例。
  pub fn add(&mut self, add: &GarnetSessionMetrics) {
    self.total_net_input_bytes = self
      .total_net_input_bytes
      .wrapping_add(add.total_net_input_bytes);
    self.total_net_output_bytes = self
      .total_net_output_bytes
      .wrapping_add(add.total_net_output_bytes);
    self.total_commands_processed = self
      .total_commands_processed
      .wrapping_add(add.total_commands_processed);
    self.total_pending = self.total_pending.wrapping_add(add.total_pending);
    self.total_found = self.total_found.wrapping_add(add.total_found);
    self.total_notfound = self.total_notfound.wrapping_add(add.total_notfound);
    self.total_cluster_commands_processed = self
      .total_cluster_commands_processed
      .wrapping_add(add.total_cluster_commands_processed);
    self.total_write_commands_processed = self
      .total_write_commands_processed
      .wrapping_add(add.total_write_commands_processed);
    self.total_read_commands_processed = self
      .total_read_commands_processed
      .wrapping_add(add.total_read_commands_processed);
    self.total_transactions_commands_received = self
      .total_transactions_commands_received
      .wrapping_add(add.total_transactions_commands_received);
    self.total_transaction_commands_execution_failed = self
      .total_transaction_commands_execution_failed
      .wrapping_add(add.total_transaction_commands_execution_failed);
    self.total_number_resp_server_session_exceptions = self
      .total_number_resp_server_session_exceptions
      .wrapping_add(add.total_number_resp_server_session_exceptions);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:Reset
  ///
  /// 清零全部计数。
  pub fn reset(&mut self) {
    *self = Self::default();
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_net_input_bytes
  #[inline]
  pub fn get_total_net_input_bytes(&self) -> u64 {
    self.total_net_input_bytes
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_net_output_bytes
  #[inline]
  pub fn get_total_net_output_bytes(&self) -> u64 {
    self.total_net_output_bytes
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_commands_processed
  #[inline]
  pub fn get_total_commands_processed(&self) -> u64 {
    self.total_commands_processed
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_pending
  #[inline]
  pub fn get_total_pending(&self) -> u64 {
    self.total_pending
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_found
  #[inline]
  pub fn get_total_found(&self) -> u64 {
    self.total_found
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_notfound
  #[inline]
  pub fn get_total_notfound(&self) -> u64 {
    self.total_notfound
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_cluster_commands_processed
  #[inline]
  pub fn get_total_cluster_commands_processed(&self) -> u64 {
    self.total_cluster_commands_processed
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_write_commands_processed
  #[inline]
  pub fn get_total_write_commands_processed(&self) -> u64 {
    self.total_write_commands_processed
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_read_commands_processed
  #[inline]
  pub fn get_total_read_commands_processed(&self) -> u64 {
    self.total_read_commands_processed
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_transaction_commands_received
  #[inline]
  pub fn get_total_transaction_commands_received(&self) -> u64 {
    self.total_transactions_commands_received
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_transaction_commands_execution_failed
  #[inline]
  pub fn get_total_transaction_commands_execution_failed(&self) -> u64 {
    self.total_transaction_commands_execution_failed
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_number_resp_server_session_exceptions
  #[inline]
  pub fn get_total_number_resp_server_session_exceptions(&self) -> u64 {
    self.total_number_resp_server_session_exceptions
  }
}

/// 会话指标共享句柄（对标 C# GarnetSessionMetrics 的类引用语义（文件映射在
/// [`GarnetSessionMetrics`] 结构文档处一处声明）：C# 每会话一个对象，
/// RespServerSession 与 StorageSession 共持同一实例直写计数；rust 会话体与
/// 存储执行域跨 compio 任务共享，以同数同序的
/// [`AtomicU64`] 字段 + `&self` Relaxed 写口承接 C# 普通字段累加）。
///
/// 本句柄即会话计数真值源：读面统一经 [`Self::snapshot`] 收口为
/// [`GarnetSessionMetrics`] 纯数据（监视器聚合 / INFO 呈现），不设第二套累加器；
/// 采样关闭（C# trackStats false → sessionMetrics 为 null）时会话与存储会话两侧
/// 句柄均为 None，写口不可达，零开销与 C# `sessionMetrics?.` 空条件调用同形。
#[derive(Debug, Default)]
pub struct SessionMetricsHandle {
  /// 网络入字节累计。
  total_net_input_bytes: AtomicU64,
  /// 网络出字节累计。
  total_net_output_bytes: AtomicU64,
  /// 已处理命令累计。
  total_commands_processed: AtomicU64,
  /// pending 累计。
  total_pending: AtomicU64,
  /// 命中累计。
  total_found: AtomicU64,
  /// 未命中累计。
  total_notfound: AtomicU64,
  /// 集群命令处理累计。
  total_cluster_commands_processed: AtomicU64,
  /// 写命令执行计数。
  total_write_commands_processed: AtomicU64,
  /// 读命令执行计数。
  total_read_commands_processed: AtomicU64,
  /// 收到的事务命令计数。
  total_transactions_commands_received: AtomicU64,
  /// 全部 resp 服务器会话 try consume 触发的异常总数。
  total_number_resp_server_session_exceptions: AtomicU64,
  /// 成功执行的事务总数（C# 字段名与注释语义相反，此处保持同名同值行为）。
  total_transaction_commands_execution_failed: AtomicU64,
}

impl SessionMetricsHandle {
  /// 全部计数 Relaxed 装载，收口为纯数据快照（读面唯一出口）。
  pub fn snapshot(&self) -> GarnetSessionMetrics {
    GarnetSessionMetrics {
      total_net_input_bytes: self.total_net_input_bytes.load(Relaxed),
      total_net_output_bytes: self.total_net_output_bytes.load(Relaxed),
      total_commands_processed: self.total_commands_processed.load(Relaxed),
      total_pending: self.total_pending.load(Relaxed),
      total_found: self.total_found.load(Relaxed),
      total_notfound: self.total_notfound.load(Relaxed),
      total_cluster_commands_processed: self.total_cluster_commands_processed.load(Relaxed),
      total_write_commands_processed: self.total_write_commands_processed.load(Relaxed),
      total_read_commands_processed: self.total_read_commands_processed.load(Relaxed),
      total_transactions_commands_received: self.total_transactions_commands_received.load(Relaxed),
      total_number_resp_server_session_exceptions: self
        .total_number_resp_server_session_exceptions
        .load(Relaxed),
      total_transaction_commands_execution_failed: self
        .total_transaction_commands_execution_failed
        .load(Relaxed),
    }
  }

  /// INFO RESET STATS 的活跃会话原地复位（consumer 镜像点单一调用方）：
  /// 全部计数清零，复位后自零位重新累计；原子承接面与 [`Self::snapshot`]
  /// 同一字段集，杜绝快照/复位两套字段漂移。
  ///
  /// C# `GarnetSessionMetrics.cs` 的 Reset 一枚 1:1 挂载在值语义的
  /// [`GarnetSessionMetrics::reset`]，本原子镜像臂是 rust 侧共享计数承接面
  /// （C# 由会话独占计数，无对位方法），不复挂
  pub fn reset(&self) {
    self.total_net_input_bytes.store(0, Relaxed);
    self.total_net_output_bytes.store(0, Relaxed);
    self.total_commands_processed.store(0, Relaxed);
    self.total_pending.store(0, Relaxed);
    self.total_found.store(0, Relaxed);
    self.total_notfound.store(0, Relaxed);
    self.total_cluster_commands_processed.store(0, Relaxed);
    self.total_write_commands_processed.store(0, Relaxed);
    self.total_read_commands_processed.store(0, Relaxed);
    self.total_transactions_commands_received.store(0, Relaxed);
    self
      .total_number_resp_server_session_exceptions
      .store(0, Relaxed);
    self
      .total_transaction_commands_execution_failed
      .store(0, Relaxed);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_net_input_bytes
  ///
  /// 累加网络入字节。
  #[inline]
  pub fn incr_total_net_input_bytes(&self, bytes: u64) {
    self.total_net_input_bytes.fetch_add(bytes, Relaxed);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_net_output_bytes
  ///
  /// 累加网络出字节。
  #[inline]
  pub fn incr_total_net_output_bytes(&self, bytes: u64) {
    self.total_net_output_bytes.fetch_add(bytes, Relaxed);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_commands_processed
  ///
  /// 累加已处理命令数。
  #[inline]
  pub fn incr_total_commands_processed(&self, cmds: u64) {
    self.total_commands_processed.fetch_add(cmds, Relaxed);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_pending
  ///
  /// 累加 pending 操作数。
  #[inline]
  pub fn incr_total_pending(&self, count: u64) {
    self.total_pending.fetch_add(count, Relaxed);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_found
  ///
  /// 累加命中数。
  #[inline]
  pub fn incr_total_found(&self, count: u64) {
    self.total_found.fetch_add(count, Relaxed);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_notfound
  ///
  /// 累加未命中数。
  #[inline]
  pub fn incr_total_notfound(&self, count: u64) {
    self.total_notfound.fetch_add(count, Relaxed);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_cluster_commands_processed
  ///
  /// 累加集群命令处理数。
  #[inline]
  pub fn incr_total_cluster_commands_processed(&self, count: u64) {
    self
      .total_cluster_commands_processed
      .fetch_add(count, Relaxed);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:add_total_write_commands_processed
  ///
  /// 累加写命令数。
  #[inline]
  pub fn add_total_write_commands_processed(&self, count: u64) {
    self
      .total_write_commands_processed
      .fetch_add(count, Relaxed);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:add_total_read_commands_processed
  ///
  /// 累加读命令数。
  #[inline]
  pub fn add_total_read_commands_processed(&self, count: u64) {
    self.total_read_commands_processed.fetch_add(count, Relaxed);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_transaction_commands_received
  ///
  /// 累加收到的事务命令数。
  #[inline]
  pub fn incr_total_transaction_commands_received(&self, count: u64) {
    self
      .total_transactions_commands_received
      .fetch_add(count, Relaxed);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_transaction_execution_failed
  ///
  /// 累加执行失败（C# 注释：成功）的事务数。
  #[inline]
  pub fn incr_total_transaction_execution_failed(&self, count: u64) {
    self
      .total_transaction_commands_execution_failed
      .fetch_add(count, Relaxed);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_number_resp_server_session_exceptions
  ///
  /// 累加会话异常数。
  #[inline]
  pub fn incr_total_number_resp_server_session_exceptions(&self, count: u64) {
    self
      .total_number_resp_server_session_exceptions
      .fetch_add(count, Relaxed);
  }
}

#[cfg(test)]
mod tests {
  use super::{GarnetSessionMetrics, SessionMetricsHandle};

  #[test]
  fn handle_incr_snapshot_roundtrip() {
    let h = SessionMetricsHandle::default();
    h.incr_total_net_input_bytes(100);
    h.incr_total_net_output_bytes(200);
    h.incr_total_commands_processed(7);
    h.incr_total_pending(2);
    h.incr_total_found(5);
    h.incr_total_notfound(2);
    h.incr_total_cluster_commands_processed(1);
    h.add_total_write_commands_processed(3);
    h.add_total_read_commands_processed(4);
    h.incr_total_transaction_commands_received(1);
    h.incr_total_transaction_execution_failed(0);
    h.incr_total_number_resp_server_session_exceptions(0);

    let m = h.snapshot();
    assert_eq!(m.get_total_net_input_bytes(), 100);
    assert_eq!(m.get_total_net_output_bytes(), 200);
    assert_eq!(m.get_total_commands_processed(), 7);
    assert_eq!(m.get_total_pending(), 2);
    assert_eq!(m.get_total_found(), 5);
    assert_eq!(m.get_total_notfound(), 2);
    assert_eq!(m.get_total_cluster_commands_processed(), 1);
    assert_eq!(m.get_total_write_commands_processed(), 3);
    assert_eq!(m.get_total_read_commands_processed(), 4);
    assert_eq!(m.get_total_transaction_commands_received(), 1);
    assert_eq!(m.get_total_transaction_commands_execution_failed(), 0);
    assert_eq!(m.get_total_number_resp_server_session_exceptions(), 0);
  }

  #[test]
  fn add_aggregates_every_counter() {
    let mut base = SessionMetricsHandle::default().snapshot();
    base.add(&GarnetSessionMetrics {
      total_commands_processed: 10,
      total_found: 6,
      ..Default::default()
    });
    base.add(&GarnetSessionMetrics {
      total_commands_processed: 3,
      total_found: 1,
      total_net_input_bytes: 64,
      ..Default::default()
    });
    assert_eq!(base.get_total_commands_processed(), 13);
    assert_eq!(base.get_total_found(), 7);
    assert_eq!(base.get_total_net_input_bytes(), 64);

    base.reset();
    assert_eq!(base, GarnetSessionMetrics::default());
  }
}
