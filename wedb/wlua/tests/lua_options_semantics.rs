//! Lua 会话选项语义集成测（r7 自 `wlua/src/options.rs` 内联测模块迁入，
//! 依赖面全 pub：LuaOptions 字段 + get_memory_limit_bytes）。

use wlua::{LuaLoggingMode, LuaMemoryManagementMode, LuaOptions};

#[test]
fn memory_limit_semantics() {
  let default = LuaOptions::default();
  assert_eq!(default.get_memory_limit_bytes(), None);
  assert_eq!(default.log_mode, LuaLoggingMode::Enable);
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
