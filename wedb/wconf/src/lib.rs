//! WeDB 服务端配置与选项库 (`wconf`)
//!
//! 对标微软 Garnet 服务配置架构 (Garnet.server/Config/)。
//!
//! 纯粹负责配置项定义、序列化/反序列化及合法性校验，除 `wbase` 基座外零依赖。

/// 配置枚举解析五件套表驱动展开：成员表 + 错误文案 → `from_raw`/`try_parse`/
/// `FromStr`/`FromToml`/`ToToml` 同形生成。
///
/// 收口三处手写样板（lua_option_modes 两枚举 + connection_protection_option）
/// 为单点；语义与手写逐行等价：十进制数值反查（对标 Enum.IsDefined + 强转）、
/// 成员名忽略大小写解析（对标 CommandLineParser 的 Enum.TryParse(_, ignoreCase)
/// 与 TypeConverters.cs:225 `Enum.Parse(strVal, true)` 共通语义），错误文案同时
/// 作 CLI 提示面与 TOML 解析错误面。仅限本 crate 内部使用。
macro_rules! config_option_enum_impls {
  // $err 为含一个 `{}` 占位的完整错误文案（占位符接非法取值原文）
  ($name:ident, $err:literal, [$($variant:ident => $member:literal),+ $(,)?]) => {
    impl $name {
      /// 判别值反查已声明成员（对标 Enum.IsDefined + 强转语义）
      #[inline]
      pub fn from_raw(raw: u8) -> Option<Self> {
        Self::try_from(raw).ok()
      }

      /// 按成员名或十进制数值解析（忽略大小写，仅接受已声明成员）
      pub fn try_parse(value: &str) -> Option<Self> {
        if let Ok(raw) = value.parse::<u8>() {
          return Self::from_raw(raw);
        }
        $(
          if value.eq_ignore_ascii_case($member) {
            return Some(Self::$variant);
          }
        )+
        None
      }
    }

    /// 命令行值解析入口（clap value_parser；错误文案即 CLI 提示面）
    impl std::str::FromStr for $name {
      type Err = String;

      fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_parse(s).ok_or_else(|| format!($err, s))
      }
    }

    impl<'de> toml_spanner::FromToml<'de> for $name {
      fn from_toml(
        ctx: &mut toml_spanner::Context<'de>,
        item: &toml_spanner::Item<'de>,
      ) -> Result<Self, toml_spanner::Failed> {
        let Some(s) = item.as_str() else {
          return Err(ctx.report_expected_but_found(&"a string", item));
        };
        Self::try_parse(s).ok_or_else(|| ctx.report_custom_error(format!($err, s), item))
      }
    }

    impl toml_spanner::ToToml for $name {
      fn to_toml<'a>(
        &'a self,
        arena: &'a toml_spanner::Arena,
      ) -> Result<toml_spanner::Item<'a>, toml_spanner::ToTomlError> {
        Ok(toml_spanner::Item::string(arena.alloc_str(&self.to_string())))
      }
    }
  };
}

pub(crate) mod config_kind;
pub(crate) mod config_meta;
pub(crate) mod config_time_unit;
pub(crate) mod connection_protection_option;
pub mod error;
pub(crate) mod lua_option_modes;
pub mod node_options;
pub mod runtime_server_config;
pub mod runtime_server_options;
pub(crate) mod server_config_type;
pub mod size;
pub use config_kind::ConfigKind;
pub use config_meta::ConfigReconcile;
pub use connection_protection_option::ConnectionProtectionOption;
pub use error::ConfigError;
pub use lua_option_modes::{LuaLoggingMode, LuaMemoryManagementMode};
pub use node_options::{
  ConfigFileArgs, DATA_FILE, DEFAULT_BIND, DEFAULT_BIND_ANY, DEFAULT_DIR, DEFAULT_HLOG_PAGE_SIZE,
  DEFAULT_LOG_FLUSH_INTERVAL, DEFAULT_MAX_DATABASES, DEFAULT_NETWORK_BUFFER_MEMORY_BUDGET,
  DEFAULT_OBJECT_SCAN_COUNT_LIMIT, DEFAULT_ON_DEMAND_CHECKPOINT, DEFAULT_PORT,
  DEFAULT_READ_CACHE_MEMORY_SIZE, DEFAULT_RESP_VERSION, DEFAULT_SLOW_LOG_MAX_ENTRIES,
  DEFAULT_SLOW_LOG_THRESHOLD, HlogOptions, HlogProjection, NodeArgs, NodeOptionsError, ServerArgs,
  format_bind_endpoint,
};
pub use runtime_server_config::RuntimeServerConfig;
pub use runtime_server_options::RuntimeServerOptions;
pub use server_config_type::ServerConfigType;
