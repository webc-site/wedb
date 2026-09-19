//! Lua 会话选项（对标 libs/server/Lua/LuaOptions.cs:LuaOptions）。

/// 注册进 Lua 的全局选项集（超时 / 内存上限 / 计费视图）。
#[derive(Debug, Clone)]
pub struct LuaOptions {
  /// 脚本超时（毫秒；0 = 无限制）。
  pub timeout_millis: i64,
  /// 内存限制（字节；0 = 无限制）。
  pub lua_memory_limit_bytes: i64,
  /// 是否允许非事务化对副本/主地址的随机访问（对齐 C# LuaOptions 默认）。
  pub allow_local_reads: bool,
  /// redis.log 行为（C# 默认 Silent）。
  pub log_mode: LuaLoggingMode,
  /// 内存管理模式（C# 默认 Native）。
  pub memory_mode: LuaMemoryManagementMode,
  /// 允许导出进沙箱的函数集（空 = 默认集）。
  pub allowed_functions: Vec<String>,
}

impl Default for LuaOptions {
  /// C# 默认：超时 0、内存无限制、Silent 日志、Native 内存模式。
  fn default() -> Self {
    Self {
      timeout_millis: 0,
      lua_memory_limit_bytes: 0,
      allow_local_reads: true,
      log_mode: LuaLoggingMode::Silent,
      memory_mode: LuaMemoryManagementMode::Native,
      allowed_functions: Vec::new(),
    }
  }
}

impl LuaOptions {
  /// 最小有效内存限制（1 KiB）。
  pub const MIN_MEMORY_LIMIT_BYTES: i64 = 1_024;
  /// 最大有效内存限制（2 GiB）。
  pub const MAX_MEMORY_LIMIT_BYTES: i64 = i32::MAX as i64;

  /// libs/server/Lua/LuaOptions.cs:GetMemoryLimitBytes
  ///
  /// 有效内存上限：未配置（<= 0）、Native 模式、或超出 [1K, 2GB] 范围时返回 None。
  #[must_use]
  pub fn get_memory_limit_bytes(&self) -> Option<usize> {
    if self.lua_memory_limit_bytes <= 0 {
      return None;
    }
    if self.memory_mode == LuaMemoryManagementMode::Native {
      log::warn!(
        "Lua script memory limit is ignored when mode = {:?}",
        self.memory_mode
      );
      return None;
    }
    if self.lua_memory_limit_bytes < Self::MIN_MEMORY_LIMIT_BYTES
      || self.lua_memory_limit_bytes > Self::MAX_MEMORY_LIMIT_BYTES
    {
      log::warn!(
        "Lua script memory limit is out of range [1K, 2GB] = {} and will be ignored",
        self.lua_memory_limit_bytes
      );
      return None;
    }
    Some(self.lua_memory_limit_bytes as usize)
  }
}

/// libs/server/Lua/LuaOptions.cs:LuaMemoryManagementMode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum LuaMemoryManagementMode {
  /// 默认分配器，宿主不感知分配。
  #[default]
  Native = 0,
  /// 宿主感知的原生分配（限额不精确）。
  Tracked = 1,
  /// 预分配自由表分配器。
  Managed = 2,
}

/// libs/server/Lua/LuaOptions.cs:LuaLoggingMode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum LuaLoggingMode {
  /// redis.log 透传记录（DEBUG/INFO/WARN/ERROR 映射）。
  #[default]
  Enable = 0,
  /// redis.log 成功但无操作。
  Silent = 1,
  /// redis.log 报错（日志被禁用）。
  Disable = 2,
}

#[cfg(test)]
mod tests {
  use super::{LuaLoggingMode, LuaMemoryManagementMode, LuaOptions};

  #[test]
  fn memory_limit_semantics() {
    let default = LuaOptions::default();
    assert_eq!(default.get_memory_limit_bytes(), None);
    assert_eq!(default.log_mode, LuaLoggingMode::Silent);
    assert_eq!(default.memory_mode, LuaMemoryManagementMode::Native);

    // Native 模式下即使指定了限额，也会被忽略返回 None
    let native_limited = LuaOptions {
      lua_memory_limit_bytes: 1024 * 1024,
      memory_mode: LuaMemoryManagementMode::Native,
      ..LuaOptions::default()
    };
    assert_eq!(native_limited.get_memory_limit_bytes(), None);

    // Tracked / Managed 模式下有效
    let tracked_limited = LuaOptions {
      lua_memory_limit_bytes: 1024 * 1024,
      memory_mode: LuaMemoryManagementMode::Tracked,
      ..LuaOptions::default()
    };
    assert_eq!(tracked_limited.get_memory_limit_bytes(), Some(1024 * 1024));

    // 小于 1024 或大于 2GB 范围忽略
    let too_small = LuaOptions {
      lua_memory_limit_bytes: 512,
      memory_mode: LuaMemoryManagementMode::Tracked,
      ..LuaOptions::default()
    };
    assert_eq!(too_small.get_memory_limit_bytes(), None);

    let too_large = LuaOptions {
      lua_memory_limit_bytes: (i32::MAX as i64) + 1,
      memory_mode: LuaMemoryManagementMode::Managed,
      ..LuaOptions::default()
    };
    assert_eq!(too_large.get_memory_limit_bytes(), None);
  }
}
