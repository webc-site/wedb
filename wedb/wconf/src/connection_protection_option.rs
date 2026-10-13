//! 自研依据: 连接保护选项（C# 对应 ConnectionProtectionOption）
use num_enum::{IntoPrimitive, TryFromPrimitive};
use strum::Display;

/// 连接保护选项（对标 libs/server/Auth/Settings/ConnectionProtectionOption.cs:ConnectionProtectionOption）。
///
/// 命令级连接保护门（如 DEBUG）的三档值域，判别值与 C# 声明一致。
/// C# redis.conf 专属别名 all（RedisTypes.cs:19 All=2 与 Yes 同值）不设：
/// 本仓配置文件唯一格式为 TOML，无 redis.conf 兼容层。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Display, TryFromPrimitive, IntoPrimitive)]
#[strum(serialize_all = "lowercase")]
#[repr(u8)]
pub enum ConnectionProtectionOption {
  /// 全部连接拒绝（C# 枚举零值，缺省档）
  #[default]
  No = 0,
  /// 仅本地连接放行（回环 / Unix 套接字）
  Local = 1,
  /// 全部连接放行
  Yes = 2,
}

config_option_enum_impls!(
  ConnectionProtectionOption,
  "取值须为 no/local/yes 之一，当前为 {}",
  [No => "no", Local => "local", Yes => "yes"]
);
