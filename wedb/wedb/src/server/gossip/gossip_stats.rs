use std::sync::atomic::{AtomicI64, Ordering};

use wresp::metrics::MetricsItem;

/// libs/cluster/Server/Gossip/GossipStats.cs:GossipStats
#[derive(Debug, Default)]
pub struct GossipStats {
  /// 接收到的 meet 请求数
  pub meet_requests_recv: AtomicI64,
  /// 成功处理的 meet 请求数
  pub meet_requests_succeed: AtomicI64,
  /// 失败的 meet 请求数
  pub meet_requests_failed: AtomicI64,
  /// 成功发起派发的 gossip 请求数（派发面口径，对标 Gossip.cs:455：
  /// TryGossip 返回 true 即计，不等应答）
  pub gossip_success_count: AtomicI64,
  /// 发送失败的 gossip 请求数
  pub gossip_failed_count: AtomicI64,
  /// 超时的 gossip 请求数
  pub gossip_timeout_count: AtomicI64,
  /// 携带完整配置数组的 gossip 请求数
  pub gossip_full_send: AtomicI64,
  /// 空载荷（ping 心跳）gossip 请求数
  pub gossip_empty_send: AtomicI64,
  /// gossip 发送字节累计
  pub gossip_bytes_send: AtomicI64,
  /// gossip 接收字节累计
  pub gossip_bytes_recv: AtomicI64,
}

impl GossipStats {
  pub fn new() -> Self {
    Self::default()
  }

  /// libs/cluster/Server/Gossip/GossipStats.cs:UpdateMeetRequestsRecv
  #[inline]
  pub fn update_meet_requests_recv(&self) {
    self.meet_requests_recv.fetch_add(1, Ordering::Relaxed);
  }

  /// libs/cluster/Server/Gossip/GossipStats.cs:UpdateMeetRequestsSucceed
  #[inline]
  pub fn update_meet_requests_succeed(&self) {
    self.meet_requests_succeed.fetch_add(1, Ordering::Relaxed);
  }

  /// libs/cluster/Server/Gossip/GossipStats.cs:UpdateMeetRequestsFailed
  #[inline]
  pub fn update_meet_requests_failed(&self) {
    self.meet_requests_failed.fetch_add(1, Ordering::Relaxed);
  }

  /// libs/cluster/Server/Gossip/GossipStats.cs:UpdateGossipBytesSend
  #[inline]
  pub fn update_gossip_bytes_send(&self, byte_count: i64) {
    self
      .gossip_bytes_send
      .fetch_add(byte_count, Ordering::Relaxed);
  }

  /// libs/cluster/Server/Gossip/GossipStats.cs:UpdateGossipBytesRecv
  #[inline]
  pub fn update_gossip_bytes_recv(&self, byte_count: i64) {
    self
      .gossip_bytes_recv
      .fetch_add(byte_count, Ordering::Relaxed);
  }

  /// to_metrics_items 指标名表（10 计数器 + gossip_open_connections 尾项；
  /// 禁用臂与正常臂共用单表，杜绝字面量双份维护）
  const METRIC_NAMES: [&str; 11] = [
    "meet_requests_recv",
    "meet_requests_succeed",
    "meet_requests_failed",
    "gossip_success_count",
    "gossip_failed_count",
    "gossip_timeout_count",
    "gossip_full_send",
    "gossip_empty_send",
    "gossip_bytes_send",
    "gossip_bytes_recv",
    "gossip_open_connections",
  ];

  pub fn to_metrics_items(
    &self,
    metrics_disabled: bool,
    open_connections: usize,
  ) -> Vec<MetricsItem> {
    if metrics_disabled {
      return Self::METRIC_NAMES
        .iter()
        .map(|name| MetricsItem::new(*name, "0"))
        .collect();
    }
    // 尾项 open_connections（usize）与计数器（i64）按名表序对位
    let values = [
      self.meet_requests_recv.load(Ordering::Relaxed),
      self.meet_requests_succeed.load(Ordering::Relaxed),
      self.meet_requests_failed.load(Ordering::Relaxed),
      self.gossip_success_count.load(Ordering::Relaxed),
      self.gossip_failed_count.load(Ordering::Relaxed),
      self.gossip_timeout_count.load(Ordering::Relaxed),
      self.gossip_full_send.load(Ordering::Relaxed),
      self.gossip_empty_send.load(Ordering::Relaxed),
      self.gossip_bytes_send.load(Ordering::Relaxed),
      self.gossip_bytes_recv.load(Ordering::Relaxed),
      open_connections as i64,
    ];
    Self::METRIC_NAMES
      .iter()
      .zip(values)
      .map(|(name, value)| MetricsItem::from_i64(*name, value))
      .collect()
  }

  pub fn reset(&self) {
    self.meet_requests_recv.store(0, Ordering::Relaxed);
    self.meet_requests_succeed.store(0, Ordering::Relaxed);
    self.meet_requests_failed.store(0, Ordering::Relaxed);
    self.gossip_success_count.store(0, Ordering::Relaxed);
    self.gossip_failed_count.store(0, Ordering::Relaxed);
    self.gossip_timeout_count.store(0, Ordering::Relaxed);
    self.gossip_full_send.store(0, Ordering::Relaxed);
    self.gossip_empty_send.store(0, Ordering::Relaxed);
    self.gossip_bytes_send.store(0, Ordering::Relaxed);
    self.gossip_bytes_recv.store(0, Ordering::Relaxed);
  }
}
