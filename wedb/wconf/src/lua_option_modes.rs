//! Lua 脚本内存/日志模式枚举（对标 libs/server/Lua/LuaOptions.cs 的
//! LuaMemoryManagementMode / LuaLoggingMode 两枚宿主配置侧枚举）。
//!
//! 自研依据: 配置端值域镜像（C# host 层 Options 以 server 层枚举作 CLI 值域，
//! rust 分层下 wconf 不得反向依赖 wlua，故在本层落同形镜像枚举，attach 单点投影
//! 进 wlua 消费端枚举）。判别值与 C# 声明一致。
use num_enum::{IntoPrimitive, TryFromPrimitive};
use strum::Display;

/// 内存管理模式（对标 LuaOptions.cs:LuaMemoryManagementMode）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Display, TryFromPrimitive, IntoPrimitive)]
#[strum(serialize_all = "lowercase")]
#[repr(u8)]
pub enum LuaMemoryManagementMode {
  /// 默认分配器，宿主不感知分配（C# 枚举零值，缺省档，无法施加脚本限额）
  #[default]
  Native = 0,
  /// 宿主感知的原生分配（限额不精确）
  Tracked = 1,
  /// 预分配自由表分配器
  Managed = 2,
}

config_option_enum_impls!(
  LuaMemoryManagementMode,
  "取值须为 native/tracked/managed 之一，当前为 {}",
  [Native => "native", Tracked => "tracked", Managed => "managed"]
);

/// redis.log 行为模式（对标 LuaOptions.cs:LuaLoggingMode）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Display, TryFromPrimitive, IntoPrimitive)]
#[strum(serialize_all = "lowercase")]
#[repr(u8)]
pub enum LuaLoggingMode {
  /// redis.log 透传记录（C# 生效默认，defaults.conf:509 在册）
  #[default]
  Enable = 0,
  /// redis.log 成功但无操作
  Silent = 1,
  /// redis.log 报错（日志被禁用）
  Disable = 2,
}

config_option_enum_impls!(
  LuaLoggingMode,
  "取值须为 enable/silent/disable 之一，当前为 {}",
  [Enable => "enable", Silent => "silent", Disable => "disable"]
);
