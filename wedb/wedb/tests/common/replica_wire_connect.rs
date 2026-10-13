//! 副本发送通道建连单源（CLIENT 握手 + APPENDLOG init 往返）
//!
//! 收口 appendlog_reject_disconnect / replica_background_replay /
//! replication_end_to_end 三册逐字同形的 `TcpSessionWire::connect` 装配：
//! sublog 0、无凭据、共享默认缓冲池、30s 建连限时、TLS 位 None。宿主册直挂
//!（沿用 primary_assets 先例）：
//!
//! ```text
//! #[path = "common/replica_wire_connect.rs"]
//! mod replica_wire_connect;
//! use replica_wire_connect::connect_replica_wire;
//! ```

use std::{io, sync::Arc, time::Duration};

use wbase::pool::{DEFAULT_BUFFER_SIZE, DEFAULT_MAX_ENTRIES_PER_LEVEL, LimitedFixedBufferPool};
use wedb::server::replication::replica_wire::TcpSessionWire;

/// 副本发送通道建连（`connect` 内已确认 init +OK；返回健康通道句柄）
pub async fn connect_replica_wire(addr: &str, primary_id: u128) -> io::Result<Arc<TcpSessionWire>> {
  TcpSessionWire::connect(
    addr,
    primary_id,
    0,
    (None, None),
    LimitedFixedBufferPool::new(DEFAULT_BUFFER_SIZE, DEFAULT_MAX_ENTRIES_PER_LEVEL),
    Some(Duration::from_secs(30)),
    #[cfg(feature = "tls")]
    None,
  )
  .await
}
