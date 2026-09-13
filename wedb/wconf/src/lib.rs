//! WeDB 服务端配置与选项库 (`wconf`)
//!
//! 对标微软 Garnet 服务配置架构 (Garnet.server/Config/)。
//!
//! 纯粹负责配置项定义、序列化/反序列化及合法性校验，零非必要依赖。

pub mod cluster_config;
pub mod config_kind;
pub mod config_meta;
pub mod config_name_comparer;
pub mod config_time_unit;
pub mod device_config;
pub mod error;
pub mod garnet_options;
pub mod log_compaction_type;
pub mod runtime_server_config;
pub mod runtime_server_options;
pub mod server_config;
pub mod server_config_type;
pub mod server_options;
pub mod units;

pub use cluster_config::{
  ClusterConfigError, ClusterConfigOptions, DEFAULT_BUS_PORT_OFFSET, DEFAULT_CLUSTER_TIMEOUT_SECS,
  DEFAULT_REPLICA_SYNC_DELAY_MS, MAX_HASH_SLOT_VALUE,
};
pub use config_kind::ConfigKind;
pub use config_meta::{
  ConfigMeta, ConfigOwner, ConfigUpdateAction, ConfigUpdateOwner, EnumMeta, TestOwner,
};
pub use config_name_comparer::ConfigNameComparer;
pub use config_time_unit::ConfigTimeUnit;
pub use device_config::{
  DeviceConfigError, DeviceOptions, DeviceType, IoBackend, LocalMemoryDeviceOptions,
  NativeDeviceOptions,
};
pub use error::ConfigError;
pub use garnet_options::{
  AofLogSettings, DEFAULT_INDEX_MEMORY_SIZE, DEFAULT_MAX_INLINE_KEY_SIZE,
  DEFAULT_MAX_INLINE_VALUE_SIZE, DEFAULT_PUB_SUB_PAGE_SIZE, GarnetServerOptions,
  MUTABLE_PERCENT_RANGE, OptionsError, StoreSettings, USE_DEFAULT_INITIAL_IO_RECORD_SIZE,
};
pub use log_compaction_type::LogCompactionType;
pub use runtime_server_config::RuntimeServerConfig;
pub use runtime_server_options::RuntimeServerOptions;
pub use server_config::ServerConfig;
pub use server_config_type::ServerConfigType;
pub use server_options::{DEFAULT_RESP_VERSION, MIN_PAGE_SIZE_BYTES, ServerOptions};
pub use units::{
  log2_exact, next_power_of_2, parse_size, parse_size_bytes, pretty_size, previous_power_of_2,
  try_parse_size, try_parse_size_bytes,
};
