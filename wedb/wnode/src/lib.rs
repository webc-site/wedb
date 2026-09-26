//! WeDB 节点网络运行时与通用宿主服务生命周期基座 (`wnode`)
//!
//! 1:1 对标微软 Garnet 生产级服务生命周期，驱动纯 `compio` 全异步事件循环。
//! 包含：
//! - 跨平台停机信号竞速监听 (Ctrl+C、SIGTERM、docker/systemd stop) 与强杀逃生通道
//! - 优雅停机协调器 (ShutdownCoordinator) 与驱动级秒级打断
//! - 统一网络流抽象 (ConnectionStream: TCP / Unix Domain Socket)
//! - 多端点监听解析 (ServerEndpoint: TCP / UDS)
//! - 多核端口复用监听 (SO_REUSEPORT) 与 UDS 自动资源闭环治理
//! - 统一网络服务与会话抽象 (WireFormat, SessionProviderFace, MessageConsumerFace)
//! - 统一节点宿主服务器门面 (GarnetServer)

pub mod aof;
pub mod cluster_provider;
pub mod cluster_session;
pub mod config_owner;
pub mod database;
pub mod datadir_lock;
pub mod endpoint;
pub mod error;
pub mod logging;
pub mod net;
pub mod primary_tasks;
pub mod range_index;
pub mod resp;
pub mod role_info;
pub mod server;
pub mod servers;
pub mod service;
pub mod session_parse_state_extensions;
pub mod shutdown;
pub mod signal;
pub mod storage;
pub mod traits;
pub mod types;

pub use aof::{
  AofProcessor, AofReplayError, AofWriteContext, GarnetAppendOnlyFile, GarnetLog, RecordShape,
  ReplayInput,
};
pub use cluster_provider::{ClusterProvider, ClusterProviderHandle, NoopClusterProvider};
pub use cluster_session::{
  ClusterSession, ClusterSessionFace, ClusterSlotVerificationInput, SlotVerifyGate,
};
pub use datadir_lock::DataDirLock;
pub use endpoint::ServerEndpoint;
pub use error::{Error, Result};
pub use logging::{FileLoggerProvider, LoggingBuilder, MemoryForwardLogger};
#[cfg(unix)]
pub use net::UdsGuard;
pub use net::bind_reuseport;
pub use primary_tasks::PrimaryTasks;
pub use resp::RespSessionConsumer;
pub use role_info::RoleInfo;
pub use server::{GarnetServer, ServerBootstrap};
pub use service::open_node_with_config;
pub use shutdown::ShutdownCoordinator;
pub use signal::{SIGINT_LABEL, wait_shutdown_signal};
pub use storage::StorageSession;
pub use traits::{MessageConsumerFace, PeerSource, SessionProviderFace, WireFormat};
pub use types::GarnetStatus;
