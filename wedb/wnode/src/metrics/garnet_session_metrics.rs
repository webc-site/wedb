/// RespServerSession 发出的性能指标（对标
/// libs/server/Metrics/GarnetSessionMetrics.cs:GarnetSessionMetrics）。
///
/// C# 为每会话实例的普通字段累加（单写者，无锁）；Rust 以同构 `u64` 字段 +
/// `&mut self` 方法承接，构造即 `Reset`（对齐 C# 构造函数）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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
    self.incr_total_net_input_bytes(add.get_total_net_input_bytes());
    self.incr_total_net_output_bytes(add.get_total_net_output_bytes());
    self.incr_total_commands_processed(add.get_total_commands_processed());
    self.incr_total_pending(add.get_total_pending());
    self.incr_total_found(add.get_total_found());
    self.incr_total_notfound(add.get_total_notfound());

    self.incr_total_cluster_commands_processed(add.get_total_cluster_commands_processed());

    self.add_total_write_commands_processed(add.get_total_write_commands_processed());
    self.add_total_read_commands_processed(add.get_total_read_commands_processed());
    self.incr_total_transaction_commands_received(add.get_total_transaction_commands_received());
    self.incr_total_transaction_execution_failed(
      add.get_total_transaction_commands_execution_failed(),
    );

    self.incr_total_number_resp_server_session_exceptions(
      add.get_total_number_resp_server_session_exceptions(),
    );
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:Reset
  ///
  /// 清零全部计数。
  pub fn reset(&mut self) {
    *self = Self::default();
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_net_input_bytes
  ///
  /// 累加网络入字节。
  #[inline]
  pub fn incr_total_net_input_bytes(&mut self, bytes: u64) {
    self.total_net_input_bytes = self.total_net_input_bytes.wrapping_add(bytes);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_net_input_bytes
  #[inline]
  pub fn get_total_net_input_bytes(&self) -> u64 {
    self.total_net_input_bytes
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_net_output_bytes
  ///
  /// 累加网络出字节。
  #[inline]
  pub fn incr_total_net_output_bytes(&mut self, bytes: u64) {
    self.total_net_output_bytes = self.total_net_output_bytes.wrapping_add(bytes);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_net_output_bytes
  #[inline]
  pub fn get_total_net_output_bytes(&self) -> u64 {
    self.total_net_output_bytes
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_commands_processed
  ///
  /// 累加已处理命令数。
  #[inline]
  pub fn incr_total_commands_processed(&mut self, cmds: u64) {
    self.total_commands_processed = self.total_commands_processed.wrapping_add(cmds);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_commands_processed
  #[inline]
  pub fn get_total_commands_processed(&self) -> u64 {
    self.total_commands_processed
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_pending
  ///
  /// 累加 pending 操作数。
  #[inline]
  pub fn incr_total_pending(&mut self, count: u64) {
    self.total_pending = self.total_pending.wrapping_add(count);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_pending
  #[inline]
  pub fn get_total_pending(&self) -> u64 {
    self.total_pending
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_found
  ///
  /// 累加命中数。
  #[inline]
  pub fn incr_total_found(&mut self, count: u64) {
    self.total_found = self.total_found.wrapping_add(count);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_found
  #[inline]
  pub fn get_total_found(&self) -> u64 {
    self.total_found
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_notfound
  ///
  /// 累加未命中数。
  #[inline]
  pub fn incr_total_notfound(&mut self, count: u64) {
    self.total_notfound = self.total_notfound.wrapping_add(count);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_notfound
  #[inline]
  pub fn get_total_notfound(&self) -> u64 {
    self.total_notfound
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_cluster_commands_processed
  ///
  /// 累加集群命令处理数。
  #[inline]
  pub fn incr_total_cluster_commands_processed(&mut self, count: u64) {
    self.total_cluster_commands_processed =
      self.total_cluster_commands_processed.wrapping_add(count);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_cluster_commands_processed
  #[inline]
  pub fn get_total_cluster_commands_processed(&self) -> u64 {
    self.total_cluster_commands_processed
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:add_total_write_commands_processed
  ///
  /// 累加写命令数。
  #[inline]
  pub fn add_total_write_commands_processed(&mut self, count: u64) {
    self.total_write_commands_processed = self.total_write_commands_processed.wrapping_add(count);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_write_commands_processed
  #[inline]
  pub fn get_total_write_commands_processed(&self) -> u64 {
    self.total_write_commands_processed
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:add_total_read_commands_processed
  ///
  /// 累加读命令数。
  #[inline]
  pub fn add_total_read_commands_processed(&mut self, count: u64) {
    self.total_read_commands_processed = self.total_read_commands_processed.wrapping_add(count);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_transaction_commands_received
  ///
  /// 累加收到的事务命令数。
  #[inline]
  pub fn incr_total_transaction_commands_received(&mut self, count: u64) {
    self.total_transactions_commands_received = self
      .total_transactions_commands_received
      .wrapping_add(count);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_transaction_execution_failed
  ///
  /// 累加执行失败（C# 注释：成功）的事务数。
  #[inline]
  pub fn incr_total_transaction_execution_failed(&mut self, count: u64) {
    self.total_transaction_commands_execution_failed = self
      .total_transaction_commands_execution_failed
      .wrapping_add(count);
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

  /// libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_number_resp_server_session_exceptions
  ///
  /// 累加会话异常数。
  #[inline]
  pub fn incr_total_number_resp_server_session_exceptions(&mut self, count: u64) {
    self.total_number_resp_server_session_exceptions = self
      .total_number_resp_server_session_exceptions
      .wrapping_add(count);
  }

  /// libs/server/Metrics/GarnetSessionMetrics.cs:get_total_number_resp_server_session_exceptions
  #[inline]
  pub fn get_total_number_resp_server_session_exceptions(&self) -> u64 {
    self.total_number_resp_server_session_exceptions
  }
}

#[cfg(test)]
mod tests {
  use super::GarnetSessionMetrics;

  #[test]
  fn incr_and_get_roundtrip() {
    let mut m = GarnetSessionMetrics::default();
    m.incr_total_net_input_bytes(100);
    m.incr_total_net_output_bytes(200);
    m.incr_total_commands_processed(7);
    m.incr_total_pending(2);
    m.incr_total_found(5);
    m.incr_total_notfound(2);
    m.incr_total_cluster_commands_processed(1);
    m.add_total_write_commands_processed(3);
    m.add_total_read_commands_processed(4);
    m.incr_total_transaction_commands_received(1);
    m.incr_total_transaction_execution_failed(0);
    m.incr_total_number_resp_server_session_exceptions(0);

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
    let mut base = GarnetSessionMetrics::default();
    base.incr_total_commands_processed(10);
    base.incr_total_found(6);

    let mut delta = GarnetSessionMetrics::default();
    delta.incr_total_commands_processed(3);
    delta.incr_total_found(1);
    delta.incr_total_net_input_bytes(64);

    base.add(&delta);
    assert_eq!(base.get_total_commands_processed(), 13);
    assert_eq!(base.get_total_found(), 7);
    assert_eq!(base.get_total_net_input_bytes(), 64);

    base.reset();
    assert_eq!(base, GarnetSessionMetrics::default());
  }
}
