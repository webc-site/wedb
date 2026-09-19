//! WeDB 服务端配置与选项库 (`wconf`)
//!
//! 对标微软 Garnet 服务配置架构 (Garnet.server/Config/)。
//!
//! 纯粹负责配置项定义、序列化/反序列化及合法性校验，零非必要依赖。

pub mod config_kind;
pub mod config_meta;
pub mod config_time_unit;
pub mod connection_protection_option;
pub mod error;
pub mod log_compaction_type;
pub mod node_options;
pub mod runtime_server_config;
pub mod runtime_server_options;
pub mod server_config_type;
pub mod size;
pub use config_kind::ConfigKind;
pub use config_meta::{ConfigMeta, ConfigReconcile, ConfigUpdateAction, EnumMeta};
pub use config_time_unit::ConfigTimeUnit;
pub use connection_protection_option::ConnectionProtectionOption;
pub use error::ConfigError;
pub use log_compaction_type::LogCompactionType;
pub use node_options::{
  ConfigFileArgs, DEFAULT_BIND, DEFAULT_BIND_ANY, DEFAULT_DIR, DEFAULT_HLOG_PAGE_SIZE,
  DEFAULT_LOG_FLUSH_INTERVAL, DEFAULT_MAX_DATABASES, DEFAULT_OBJECT_SCAN_COUNT_LIMIT, DEFAULT_PORT,
  DEFAULT_READ_CACHE_MEMORY_SIZE, DEFAULT_RESP_VERSION, DEFAULT_SLOW_LOG_MAX_ENTRIES,
  DEFAULT_SLOW_LOG_THRESHOLD, HlogOptions, HlogProjection, MAX_DATABASES_MAX, MAX_DATABASES_MIN,
  NodeArgs, NodeOptionsError, ServerArgs,
};
pub use runtime_server_config::RuntimeServerConfig;
pub use runtime_server_options::RuntimeServerOptions;
pub use server_config_type::ServerConfigType;
