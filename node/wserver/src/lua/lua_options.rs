//! Lua 会话选项（对标 libs/server/Lua/LuaOptions.cs:LuaOptions）。
//!
/// 注册进 Lua 的全局选项集（超时 / 内存上限 / 计费视图）。
#[derive(Debug, Clone)]
pub struct LuaOptions {
  /// 脚本超时（毫秒；0 = 无限制）。
  pub timeout_millis: i64,
  /// 内存限制（字节；0 = 无限制）。
  pub lua_memory_limit_bytes: i64,
  /// 是否允许非事务化对副本/主地址的随机访问（对齐 C# LuaOptions 默认）。
  pub allow_local_reads: bool,
}

impl Default for LuaOptions {
  /// C# 默认：超时 0、内存无限制。
  fn default() -> Self {
    Self {
      timeout_millis: 0,
      lua_memory_limit_bytes: 0,
      allow_local_reads: true,
    }
  }
}

impl LuaOptions {
  /// libs/server/Lua/LuaOptions.cs:GetMemoryLimitBytes
  ///
  /// 有效内存上限：未配置（<= 0）时返回 None。
  pub fn get_memory_limit_bytes(&self) -> Option<usize> {
    if self.lua_memory_limit_bytes > 0 {
      Some(self.lua_memory_limit_bytes as usize)
    } else {
      None
    }
  }
}

#[cfg(test)]
mod tests {
  use super::LuaOptions;

  #[test]
  fn memory_limit_semantics() {
    let default = LuaOptions::default();
    assert_eq!(default.get_memory_limit_bytes(), None);

    let limited = LuaOptions {
      lua_memory_limit_bytes: 1024 * 1024,
      ..LuaOptions::default()
    };
    assert_eq!(limited.get_memory_limit_bytes(), Some(1024 * 1024));
  }
}
