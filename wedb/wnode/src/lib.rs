//! WeDB 节点网络运行时与通用宿主服务生命周期基座 (`wnode`)
//!
//! 1:1 对标微软 Garnet 生产级服务生命周期，驱动纯 `compio` 全异步事件循环。
//! 包含：
//! - 跨平台停机信号竞速监听 (Ctrl+C、SIGTERM、docker/systemd stop) 与强杀逃生通道
//! - 优雅停机协调器 (ShutdownCoordinator) 与驱动级秒级打断
//! - 统一网络流抽象 (ConnectionStream: TCP / Unix Domain Socket)
//! - 多端点监听解析 (ServerEndpoint: TCP / UDS)
//! - 多核端口复用监听 (SO_REUSEPORT) 与 UDS 自动资源闭环治理
//! - 网络缓冲区池化管理 (LimitedFixedBufferPool, PooledBuffer)
//! - 慢客户端背压流控 (NetworkSenderThrottle)
//! - 统一网络服务与会话抽象 (WireFormat, SessionProviderFace, MessageConsumerFace)
//! - 统一节点宿主服务器门面 (GarnetServer)
//! - 通用信息指标模型 (MetricsItem)

pub mod aof;
pub mod api {
  pub mod garnet_status {
    pub use crate::types::GarnetStatus;
  }
}
pub mod args;
pub mod buffer_pool;
pub mod cluster_provider;
pub mod cluster_session;
pub mod conf;
pub mod endpoint;
pub mod error;
pub mod inputs;
pub mod key_spec;
pub mod net;
pub mod resp;
pub mod role_info;
pub mod server;
pub mod servers;
pub mod service;
pub mod session_parse_state_extensions;
pub mod shutdown;
pub mod signal;
pub mod storage;
pub mod task;
pub mod throttle;
pub mod tls;
pub mod traits;
pub mod types;

use std::sync::Arc;

pub use aof::{
  AofProcessor, AofReplayError, GarnetAppendOnlyFile, GarnetLog, InMemorySublog, LogRecord,
  RangeIndexSessionFace, ReplayInput, ShardedLog, SingleLog, Sublog, SublogBackend,
};
pub use args::{DEFAULT_BIND, DEFAULT_DIR, DEFAULT_PORT, NodeArgs, ServerArgs};
pub use buffer_pool::{
  DEFAULT_BUFFER_SIZE, DEFAULT_MAX_POOL_SIZE, DEFAULT_MAX_RECEIVE_BUFFER_SIZE,
  DEFAULT_SEND_BUFFER_SIZE, LimitedFixedBufferPool, NetworkBufferSettings, PooledBuffer,
  SEND_BUFFER_OVERHEAD_RESERVE,
};
pub use cluster_provider::{ClusterProvider, NoopClusterProvider};
pub use cluster_session::{ClusterSession, ClusterSessionFace, ClusterSlotVerificationInput};
pub use conf::Conf;
pub use endpoint::ServerEndpoint;
pub use error::{Error, Result};
pub use inputs::{CustomProcedureInput, StringInput, UnifiedInput};
use resp::objects::collection_item_source::CollectionItemSource;
use wobject::itembroker::item_broker_face::{BlockedWait as BaseBlockedWait, SharedItemBroker};

pub type ItemBroker = SharedItemBroker<CollectionItemSource<wdev::SegmentedDevice>>;
pub type BlockedWait = BaseBlockedWait<Arc<ItemBroker>>;
pub use key_spec::{
  SimpleRespKeySpec, SimpleRespKeySpecBeginSearch, SimpleRespKeySpecFindKeys,
  extract_keys_and_flags_from_slice, extract_keys_from_slice,
};
#[cfg(unix)]
pub use net::UdsGuard;
pub use net::{
  ConnectionStream, DirectWriter, NetworkHandler, SessionReader, TCP_LISTEN_BACKLOG, bind_reuseport,
};
pub use resp::RespSessionConsumer;
pub use role_info::RoleInfo;
pub use server::{GarnetServer, NodeServerBuilder, ServerBootstrap, run_node};
pub use servers::{MetricsApi, RegisterApi};
pub use shutdown::ShutdownCoordinator;
pub use signal::{SIGINT_LABEL, SIGTERM_LABEL, SIGTERM_NUM, wait_shutdown_signal};
pub use storage::{StorageScriptingApi, StorageSession};
pub use task::{TaskManager, TaskPlacementCategory, TaskType};
pub use throttle::{NetworkSenderThrottle, ThrottleClosed};
pub use tls::IGarnetTlsOptions;
pub use traits::{MessageConsumerFace, ServerEnumerate, SessionProviderFace, WireFormat};
pub use types::GarnetStatus;
