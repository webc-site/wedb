use std::sync::atomic::{AtomicI64, Ordering};

use wnode::MetricsItem;

/// libs/cluster/Server/Gossip/GossipStats.cs:GossipStats
#[derive(Debug, Default)]
pub struct GossipStats {
  /// 接收到的 meet 请求数
  pub meet_requests_recv: AtomicI64,
  /// 成功处理的 meet 请求数
  pub meet_requests_succeed: AtomicI64,
  /// 失败的 meet 请求数
  pub meet_requests_failed: AtomicI64,
  /// 成功发送的 gossip 请求数
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

  #[inline]
  pub fn update_meet_requests_recv(&self) {
    self.meet_requests_recv.fetch_add(1, Ordering::Relaxed);
  }

  #[inline]
  pub fn update_meet_requests_succeed(&self) {
    self.meet_requests_succeed.fetch_add(1, Ordering::Relaxed);
  }

  #[inline]
  pub fn update_meet_requests_failed(&self) {
    self.meet_requests_failed.fetch_add(1, Ordering::Relaxed);
  }

  #[inline]
  pub fn update_gossip_bytes_send(&self, byte_count: i64) {
    self
      .gossip_bytes_send
      .fetch_add(byte_count, Ordering::Relaxed);
  }

  #[inline]
  pub fn update_gossip_bytes_recv(&self, byte_count: i64) {
    self
      .gossip_bytes_recv
      .fetch_add(byte_count, Ordering::Relaxed);
  }

  pub fn to_metrics_items(
    &self,
    metrics_disabled: bool,
    open_connections: usize,
  ) -> Vec<MetricsItem> {
    if metrics_disabled {
      return vec![
        MetricsItem::new("meet_requests_recv", "0"),
        MetricsItem::new("meet_requests_succeed", "0"),
        MetricsItem::new("meet_requests_failed", "0"),
        MetricsItem::new("gossip_success_count", "0"),
        MetricsItem::new("gossip_failed_count", "0"),
        MetricsItem::new("gossip_timeout_count", "0"),
        MetricsItem::new("gossip_full_send", "0"),
        MetricsItem::new("gossip_empty_send", "0"),
        MetricsItem::new("gossip_bytes_send", "0"),
        MetricsItem::new("gossip_bytes_recv", "0"),
        MetricsItem::new("gossip_open_connections", "0"),
      ];
    }
    vec![
      MetricsItem::new(
        "meet_requests_recv",
        self.meet_requests_recv.load(Ordering::Relaxed).to_string(),
      ),
      MetricsItem::new(
        "meet_requests_succeed",
        self
          .meet_requests_succeed
          .load(Ordering::Relaxed)
          .to_string(),
      ),
      MetricsItem::new(
        "meet_requests_failed",
        self
          .meet_requests_failed
          .load(Ordering::Relaxed)
          .to_string(),
      ),
      MetricsItem::new(
        "gossip_success_count",
        self
          .gossip_success_count
          .load(Ordering::Relaxed)
          .to_string(),
      ),
      MetricsItem::new(
        "gossip_failed_count",
        self.gossip_failed_count.load(Ordering::Relaxed).to_string(),
      ),
      MetricsItem::new(
        "gossip_timeout_count",
        self
          .gossip_timeout_count
          .load(Ordering::Relaxed)
          .to_string(),
      ),
      MetricsItem::new(
        "gossip_full_send",
        self.gossip_full_send.load(Ordering::Relaxed).to_string(),
      ),
      MetricsItem::new(
        "gossip_empty_send",
        self.gossip_empty_send.load(Ordering::Relaxed).to_string(),
      ),
      MetricsItem::new(
        "gossip_bytes_send",
        self.gossip_bytes_send.load(Ordering::Relaxed).to_string(),
      ),
      MetricsItem::new(
        "gossip_bytes_recv",
        self.gossip_bytes_recv.load(Ordering::Relaxed).to_string(),
      ),
      MetricsItem::new("gossip_open_connections", open_connections.to_string()),
    ]
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
