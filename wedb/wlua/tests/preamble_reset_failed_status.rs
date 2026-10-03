//! preamble 参数重置失败文案分流集成测。
//!
//! 对标 libs/server/Lua/LuaRunner.Functions.cs 两失败臂（UnsafeRunPreambleForRunner /
//! UnsafeRunPreambleForSession）按 LuaStatus 分流的四文案与 libs/server/Resp/
//! CmdStrings.cs:545-548 常量文本；C# 内联 switch 无独立测试面（garnet/test
//! 无覆盖），rust 收单源 LuaRunner::parameter_reset_failed 纯函数后表驱动
//! 四状态全覆盖，配 pcall_status 底层状态真值锚与 reset 臂触发后 runner
//! 复用终态（失败臂不置 NeedsDispose，置位唯一在 allocator 域）。

use wlua::{
  LUA_ERRERR, LUA_ERRMEM, LUA_ERRRUN, LUA_ERRSYNTAX, LUA_OK, LUA_YIELD, LuaLoggingMode, LuaOptions,
  LuaRunner, LuaState, RespObject,
};

/// 四状态→四文案映射（逐字节对齐 CmdStrings.cs:545-548；ErrErr/Yield/OK
/// 落 Other 臂，对齐 C# switch 的 `or _` 兜底）。
#[test]
fn parameter_reset_failed_text_mapping() {
  let cases: &[(i32, &[u8])] = &[
    (
      LUA_ERRSYNTAX,
      b"Resetting parameters to Lua script failed: Syntax",
    ),
    (
      LUA_ERRMEM,
      b"Resetting parameters to Lua script failed: Memory",
    ),
    (
      LUA_ERRRUN,
      b"Resetting parameters to Lua script failed: Runtime",
    ),
    (
      LUA_ERRERR,
      b"Resetting parameters to Lua script failed: Other",
    ),
    (
      LUA_YIELD,
      b"Resetting parameters to Lua script failed: Other",
    ),
    (LUA_OK, b"Resetting parameters to Lua script failed: Other"),
  ];
  for &(status, text) in cases {
    assert_eq!(
      LuaRunner::parameter_reset_failed(status),
      text,
      "status {status} 文案分流"
    );
  }
}

/// pcall_status 状态出参真值锚：error() 脚本回 ErrRun，正常脚本回 Ok
/// （TryResetParameters failingStatus 出参的底层依赖）。
#[test]
fn pcall_status_carries_underlying_status() {
  let mut state = LuaState::new();
  state.load_string("error('boom')").unwrap();
  assert_eq!(state.pcall_status(0), Err(LUA_ERRRUN));
  state.clear_stack();

  state.load_string("return 1").unwrap();
  assert!(state.pcall_status(0).is_ok());
  state.clear_stack();
}

/// reset 臂触发（KEYS 收缩）后 runner 复用且 needs_dispose 终态 false
/// （对齐 C#：preamble 成功/失败臂皆不置 NeedsDispose，runner 留缓存）。
#[test]
fn reset_shrunk_keys_runner_reused_needs_dispose_false() {
  let opts = LuaOptions {
    log_mode: LuaLoggingMode::Silent,
    ..Default::default()
  };
  let mut runner = LuaRunner::with_options(&opts, b"return #KEYS", "0.0.0").unwrap();
  let mut out = Vec::new();
  runner.compile_for_runner(&mut out).unwrap();

  // 首跑 3 键：key_length = 3。
  let keys: Vec<Vec<u8>> = (1..=3).map(|i| format!("k{i}").into_bytes()).collect();
  let RespObject::Integer(first) = runner.run_for_runner(Some(keys), None).unwrap() else {
    panic!("首跑应回整数");
  };
  assert_eq!(first, 3);
  assert!(!runner.needs_dispose());

  // 二跑 1 键：key_length(3) > 1 触发 reset_keys_and_argv pcall（成功臂）。
  let RespObject::Integer(second) = runner
    .run_for_runner(Some(vec![b"k1".to_vec()]), None)
    .unwrap()
  else {
    panic!("二跑应回整数");
  };
  assert_eq!(second, 1, "reset 后 KEYS 收缩生效");
  assert!(
    !runner.needs_dispose(),
    "reset 臂后 runner 应复用，不置 needs_dispose"
  );

  // 三跑无键：reset 再触发后空 KEYS 仍可复用。
  let RespObject::Integer(third) = runner.run_for_runner(None, None).unwrap() else {
    panic!("三跑应回整数");
  };
  assert_eq!(third, 0);
  assert!(!runner.needs_dispose());
}
