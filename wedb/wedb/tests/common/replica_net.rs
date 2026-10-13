//! 复制链路测试网络面单源（共享缓冲池 + RESP 客户端往返；对标 wedb/tests
//! common 收口先例，replica_auth_handshake / replica_sync_timeout 共享）
//!
//! 宿主册直挂：
//!
//! ```text
//! #[path = "common/replica_net.rs"]
//! mod replica_net;
//! use replica_net::{client_roundtrip, network_pool};
//! ```

use std::sync::Arc;

use wbase::pool::{DEFAULT_BUFFER_SIZE, DEFAULT_MAX_ENTRIES_PER_LEVEL, LimitedFixedBufferPool};
use wconn::client::{DEFAULT_OUTSTANDING_TASKS, GarnetClient, NO_TIMEOUT_MILLIS};

/// 共享复制网络缓冲池（逐连接新建，与 connect_replica_wire 夹具同构）
pub fn network_pool() -> Arc<LimitedFixedBufferPool> {
  LimitedFixedBufferPool::new(DEFAULT_BUFFER_SIZE, DEFAULT_MAX_ENTRIES_PER_LEVEL)
}

/// RESP 客户端往返字符串应答（`auth` 为 Some 即带认证建连；服务端 -ERR 应答
/// 映射为 Err 文本，供拒绝臂断言）
pub async fn client_roundtrip(
  endpoint: &str,
  auth: Option<(&str, &str)>,
  command: &[&str],
) -> Result<String, String> {
  let (user, pwd) = auth.map_or((None, None), |(u, p)| (Some(u), Some(p)));
  let mut client = GarnetClient::new(
    endpoint.to_string(),
    user.map(str::to_string),
    pwd.map(str::to_string),
    Some("test".into()),
    DEFAULT_OUTSTANDING_TASKS,
    NO_TIMEOUT_MILLIS,
  )
  .unwrap();
  client.connect_async().await.map_err(|e| e.to_string())?;
  client
    .execute_for_string_result_async(command)
    .await
    .map_err(|e| e.to_string())
}
