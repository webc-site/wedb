use wbase::pool::{DEFAULT_MAX_RECEIVE_BUFFER_SIZE, NetworkBufferSettings};

/// 最大单次 AOF 分包大小：1MB（对标 Garnet maxChunkSize: 1 << 20）
pub const MAX_CHUNK_SIZE: usize = 1 << 20;

/// 流复制网络配置构造器（1:1 对标 Garnet ReplicationNetworkBufferSettings.cs）
pub struct ReplicationNetworkBufferSettings;

impl ReplicationNetworkBufferSettings {
  pub const RSS_SEND_BUFFER_SIZE: usize = 1 << 20;
  pub const RSS_INITIAL_RECEIVE_BUFFER_SIZE: usize = 1 << 12;

  pub const IRS_SEND_BUFFER_SIZE: usize = 1 << 17;
  pub const IRS_INITIAL_RECEIVE_BUFFER_SIZE: usize = 1 << 17;

  pub const AOF_SYNC_INITIAL_RECEIVE_BUFFER_SIZE: usize = 1 << 17;

  /// 副本同步会话网络缓冲区设置（对标 C# GetRSSNetworkBufferSettings）
  #[inline]
  pub const fn rss_settings() -> NetworkBufferSettings {
    NetworkBufferSettings::new(
      Self::RSS_SEND_BUFFER_SIZE,
      Self::RSS_INITIAL_RECEIVE_BUFFER_SIZE,
      DEFAULT_MAX_RECEIVE_BUFFER_SIZE,
    )
  }

  /// 发起副本同步网络缓冲区设置（对标 C# GetIRSNetworkBufferSettings）
  #[inline]
  pub const fn irs_settings() -> NetworkBufferSettings {
    NetworkBufferSettings::new(
      Self::IRS_SEND_BUFFER_SIZE,
      Self::IRS_INITIAL_RECEIVE_BUFFER_SIZE,
      DEFAULT_MAX_RECEIVE_BUFFER_SIZE,
    )
  }

  /// AOF 同步任务网络缓冲区设置（对标 C# GetAofSyncNetworkBufferSettings）
  #[inline]
  pub const fn aof_sync_settings(aof_page_size_bits: u32) -> NetworkBufferSettings {
    let send_size = 2 << aof_page_size_bits;
    NetworkBufferSettings::new(
      send_size,
      Self::AOF_SYNC_INITIAL_RECEIVE_BUFFER_SIZE,
      DEFAULT_MAX_RECEIVE_BUFFER_SIZE,
    )
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_replication_network_buffer_settings() {
    let rss = ReplicationNetworkBufferSettings::rss_settings();
    assert_eq!(rss.send_buffer_size, 1 << 20);
    assert_eq!(rss.initial_receive_buffer_size, 1 << 12);

    let irs = ReplicationNetworkBufferSettings::irs_settings();
    assert_eq!(irs.send_buffer_size, 1 << 17);

    let aof = ReplicationNetworkBufferSettings::aof_sync_settings(24);
    assert_eq!(aof.send_buffer_size, 2 << 24);

    let inclusive = NetworkBufferSettings::get_inclusive(&[rss, irs, aof]);
    assert_eq!(inclusive.send_buffer_size, 2 << 24);
    assert_eq!(inclusive.initial_receive_buffer_size, 1 << 12);

    let pool = inclusive.create_buffer_pool(16);
    assert!(pool.validate(&rss));
    assert!(pool.validate(&irs));
    assert!(pool.validate(&aof));
  }
}
