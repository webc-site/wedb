//! 配置域（对标 libs/server/Config 与 LogCompactionType.cs）。

pub mod config_kind;
pub mod config_meta;
pub mod config_name_comparer;
pub mod config_time_unit;
pub mod error;
pub mod log_compaction_type;
pub mod runtime_server_config;
pub mod runtime_server_options;
pub mod server_config;
pub mod server_config_type;

pub use config_kind::ConfigKind;
pub use config_meta::{ConfigMeta, ConfigOwner, ConfigUpdateOwner};
pub use config_time_unit::ConfigTimeUnit;
pub use error::ConfigError;
pub use log_compaction_type::LogCompactionType;
pub use runtime_server_config::RuntimeServerConfig;
pub use runtime_server_options::RuntimeServerOptions;
pub use server_config::ServerConfig;
pub use server_config_type::ServerConfigType;
