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
pub mod bitmap;
pub mod buffer_pool;
pub mod cluster_provider;
pub mod conf;
pub mod config;
pub mod custom;
pub mod databases;
pub mod endpoint;
pub mod error;
pub mod hyperloglog;
pub mod input_header;
pub mod inputs;
pub mod key_spec;
pub mod metrics;
pub mod net;
pub mod objects;
pub mod resp;
pub mod server;
pub mod servers;
pub mod service;
pub mod session_parse_state;
pub mod session_parse_state_extensions;
pub mod shutdown;
pub mod signal;
pub mod storage;
pub mod task;
pub mod throttle;
pub mod tls;
pub mod traits;
pub mod types;
pub mod units;

pub use aof::{
  AofHeader, AofProcessor, AofReplayError, GarnetAppendOnlyFile, GarnetLog, InMemorySublog,
  LogRecord, RangeIndexReplayerFace, RangeIndexSessionFace, ReplayInput, SequenceNumberGenerator,
  ShardedLog, SingleLog, Sublog, SublogBackend,
};
pub use args::{DEFAULT_BIND, DEFAULT_DIR, DEFAULT_PORT, NodeArgs, ServerArgs};
pub use buffer_pool::{
  DEFAULT_BUFFER_SIZE, DEFAULT_MAX_POOL_SIZE, LimitedFixedBufferPool, PooledBuffer,
};
pub use cluster_provider::{ClusterProvider, NoopClusterProvider};
pub use conf::Conf;
pub use config::{
  ConfigError, ConfigKind, ConfigMeta, ConfigTimeUnit, LogCompactionType, RuntimeServerConfig,
  RuntimeServerOptions, ServerConfig, ServerConfigType,
};
pub use endpoint::ServerEndpoint;
pub use error::{Error, Result};
pub use input_header::RespInputHeader;
pub use inputs::{CustomProcedureInput, ObjectInput, StringInput, UnifiedInput};
pub use key_spec::{
  ALL_FLAGS, KeySpecificationFlags, SimpleRespKeySpec, SimpleRespKeySpecBeginSearch,
  SimpleRespKeySpecFindKeys, extract_keys_and_flags_from_slice, extract_keys_from_slice,
};
pub use metrics::{InfoCommandUtils, InfoMetricsType, MetricsItem, format_info_section};
#[cfg(unix)]
pub use net::UdsGuard;
pub use net::{
  ConnectionStream, DirectWriter, NetworkHandler, SessionReader, TCP_LISTEN_BACKLOG, bind_reuseport,
};
pub use server::{GarnetServer, NodeServerBuilder, ServerBootstrap, run_node};
pub use servers::{
  GarnetServerOptions, MetricsApi, RegisterApi, ServerOptions, StoreApi, StoreApiFace,
};
pub use session_parse_state::SessionParseState;
pub use shutdown::ShutdownCoordinator;
pub use signal::{SIGINT_LABEL, SIGTERM_LABEL, SIGTERM_NUM, wait_shutdown_signal};
pub use storage::{StorageScriptingApi, StorageSession};
pub use task::{TaskManager, TaskPlacementCategory, TaskType};
pub use throttle::{NetworkSenderThrottle, ThrottleClosed};
pub use tls::IGarnetTlsOptions;
pub use traits::{MessageConsumerFace, ServerEnumerate, SessionProviderFace, WireFormat};
pub use types::{GarnetObjectType, GarnetStatus, RespCommand, RespInputFlags, StoreType};
pub use units::{
  log2_exact, next_power_of_2, parse_size, parse_size_bytes, pretty_size, previous_power_of_2,
  try_parse_size, try_parse_size_bytes,
};
