//! `LuaState` 栈/表/pcall 语义层集成测试

use std::{
  ffi::c_void,
  sync::atomic::{AtomicI32, Ordering},
};

use wlua::{
  ConstantStrings, Error, LUA_OK, LUA_YIELD, LuaState, LuaTrackedAllocator, clear_callback_context,
  set_callback_context,
};

#[test]
fn push_pop_and_types() {
  let mut state = LuaState::new();
  state.push_integer(7);
  state.push_number(3.5);
  state.push_boolean(true);
  state.push_nil();
  state.push_buffer(b"hello");
  assert_eq!(state.get_top(), 5);
  assert_eq!(state.type_name(-1), Some("string"));
  assert_eq!(state.type_name(-2), Some("nil"));
  assert_eq!(state.check_number(-5), Some(7.0));
  assert!(state.to_boolean(-3));
  assert!(!state.to_boolean(-2));
  assert_eq!(state.raw_len(-1), 5);
  state.pop(5);
  assert!(state.expect_lua_stack_empty());
}

#[test]
fn load_and_pcall() {
  let mut state = LuaState::new();
  // 返回两个值。
  state.load_string("return 1, 'x'").unwrap();
  state.pcall(0).unwrap();
  assert_eq!(state.get_top(), 2);
  assert_eq!(state.check_number(-2), Some(1.0));
  assert_eq!(state.type_name(-1), Some("string"));
  state.clear_stack();

  // 运行错误 → 错误串压栈 + 状态非 OK（C# LuaStatus.ErrRun 语义）。
  state.load_string("error('boom')").unwrap();
  assert!(state.pcall(0).is_err());
  assert_eq!(state.get_top(), 1);
  // Luau 的错误串可能带位置前缀，仅校验包含错误消息。
  assert!(String::from_utf8_lossy(&state.known_string_to_buffer(-1).unwrap()).contains("boom"));
  state.clear_stack();
}

#[test]
fn table_ops_and_globals() {
  let mut state = LuaState::new();
  state.create_table(4, 0);
  state.push_integer(42);
  state.raw_set_integer(-2, 1);
  // 表位于 -1（42 已随 raw_set_integer 弹出）。
  state.raw_get_integer(-1, 1);
  assert_eq!(state.check_number(-1), Some(42.0));
  state.pop(1);
  assert_eq!(state.raw_len(-1), 1);

  state.clear_stack();
  state.push_buffer(b"answer");
  assert!(state.set_global(b"ANSWER"));
  assert!(state.get_global(b"ANSWER"));
  assert_eq!(state.known_string_to_buffer(-1).unwrap(), b"answer");
  state.clear_stack();

  // 缺失全局：压 nil 且报不存在。
  assert!(!state.get_global(b"NO_SUCH_GLOBAL_WLUA"));
  assert_eq!(state.type_name(-1), Some("nil"));
  state.pop(1);
}

#[test]
fn rotate_orders_stack() {
  let mut state = LuaState::new();
  for i in 1..=3 {
    state.push_integer(i);
  }
  // lua_rotate(L, 1, 1)：栈顶 1 个元素滚到区间开头 → [3, 1, 2]。
  state.rotate(1, 1);
  assert_eq!(state.check_number(1), Some(3.0));
  assert_eq!(state.check_number(2), Some(1.0));
  // 反向旋转归位 → [1, 2, 3]。
  state.rotate(1, -1);
  assert_eq!(state.check_number(1), Some(1.0));
  assert_eq!(state.check_number(3), Some(3.0));
  state.pop(2);
  assert_eq!(state.get_top(), 1);
}

#[test]
fn pcall_n_pads_and_truncates() {
  let mut state = LuaState::new();
  state.load_string("return 1, 2").unwrap();
  state.pcall_n(0, 3).unwrap();
  assert_eq!(state.get_top(), 3);
  // 引用 id 0 = REFNIL（nil 值引用），非有效引用。
  assert!(!state.push_ref(0));
  state.clear_stack();

  state.load_string("return 1, 2").unwrap();
  state.pcall_n(0, 1).unwrap();
  assert_eq!(state.get_top(), 1);
  assert_eq!(state.check_number(-1), Some(1.0));
}

#[test]
fn next_and_raw_get_semantics() {
  let mut state = LuaState::new();
  state.create_table(0, 2);
  state.push_buffer(b"k");
  state.push_integer(7);
  state.raw_set(-3);
  assert_eq!(state.get_top(), 1);

  // lua_next 语义：迭代尽头只耗键、不压值（净 -1）。
  state.push_nil();
  let mut seen = 0;
  while state.lua_next() {
    seen += 1;
    state.pop(1);
  }
  assert_eq!(seen, 1);
  assert_eq!(state.get_top(), 1);

  // lua_rawget 语义：栈顶键被查得值原位替换。
  state.push_buffer(b"k");
  state.raw_get(-2);
  assert_eq!(state.get_top(), 2);
  assert_eq!(state.check_number(-1), Some(7.0));
}

#[test]
fn number_to_string_in_place() {
  let mut state = LuaState::new();
  state.push_integer(42);
  assert!(state.try_number_to_string());
  assert_eq!(state.known_string_to_buffer(-1).unwrap(), b"42");
  state.pop(1);

  // 非数值槽位：拒绝转换且保持原值。
  state.push_buffer(b"x");
  assert!(!state.try_number_to_string());
  assert_eq!(state.known_string_to_buffer(-1).unwrap(), b"x");
}

/// 实验验证（协程化改造前置语义探针）：cont == NULL 的 C 函数内
/// `lua_yield(l, n)` 挂起协程，再次 `lua_resume` 时该 C 函数以 resume
/// 前压入的参数值作为自身返回值收帧——redis.call 协程化挂起协议的
/// 语义基石（ldo.cpp:lua_yield 返回 -1 + LOP_CALL 负返回出口 +
/// resume() 的 `luau_poscall(L, firstArg)` 收帧路径）。
#[test]
fn yield_resume_c_function_semantics() {
  static CALLS: AtomicI32 = AtomicI32::new(0);

  /// 首调压 2 个让渡值并挂起；续跑后以 resume 参数个数收帧返回。
  fn yielding_host(state: &mut LuaState, _ctx: &mut u8) -> i32 {
    match CALLS.fetch_add(1, Ordering::SeqCst) {
      0 => {
        state.push_number(42.0);
        state.push_number(7.0);
        state.script_yield(2)
      }
      _ => state.get_top() as i32,
    }
  }

  let mut state = LuaState::new();
  state.register_host_fn::<u8>(b"host_fn", yielding_host);
  state.load_string("return host_fn(1, 2)").unwrap();
  let function_ref = state.try_ref();
  assert!(function_ref > 0);

  // 回调窗口上下文（蹦床取上下文的必需项；真实链路由 CallbackGuard 挂载）
  let ctx: u8 = 0;
  // SAFETY：测试窗口内挂载回调上下文（指针随测试栈帧存活）。
  unsafe { set_callback_context((&raw const ctx).cast_mut().cast()) };

  let co = state.new_thread();
  let co_view = &mut unsafe { LuaState::view(co) };
  assert!(co_view.push_ref(function_ref), "function pushed on thread");

  // 首段 resume：C 函数挂起，让渡值 (42, 7) 留在协程栈
  assert_eq!(co_view.resume(0), LUA_YIELD);
  assert_eq!(co_view.get_top(), 2);
  assert_eq!(co_view.check_number(1), Some(42.0));
  assert_eq!(co_view.check_number(2), Some(7.0));

  // 弹让渡值，压 resume 传值——它们成为 C 函数的返回值
  co_view.pop(2);
  co_view.push_number(100.0);
  co_view.push_number(200.0);
  co_view.push_number(300.0);
  assert_eq!(co_view.resume(3), LUA_OK);
  assert_eq!(co_view.get_top(), 3);
  assert_eq!(co_view.check_number(1), Some(100.0));
  assert_eq!(co_view.check_number(3), Some(300.0));

  // SAFETY：与 set_callback_context 配对摘除。
  unsafe { clear_callback_context((&raw const ctx).cast_mut().cast()) };
}

/// 实验补充：pcall 包裹路径的 yield 放行性（Luau 经 baseCcalls 维持
/// 可让渡不变式，redis.call 在脚本 pcall 内挂起不得误报
/// "attempt to yield across metamethod/C-call boundary"）。
#[test]
fn yield_across_pcall_is_yieldable() {
  static CALLS: AtomicI32 = AtomicI32::new(0);

  fn yielding_host(state: &mut LuaState, _ctx: &mut u8) -> i32 {
    match CALLS.fetch_add(1, Ordering::SeqCst) {
      0 => {
        state.push_number(5.0);
        state.script_yield(1)
      }
      _ => state.get_top() as i32,
    }
  }

  let mut state = LuaState::new();
  state.register_host_fn::<u8>(b"host_fn", yielding_host);
  state
    .load_string("local ok, a = pcall(function() return host_fn() end) return ok, a")
    .unwrap();
  let function_ref = state.try_ref();

  let ctx: u8 = 0;
  // SAFETY：测试窗口内挂载回调上下文（指针随测试栈帧存活）。
  unsafe { set_callback_context((&raw const ctx).cast_mut().cast()) };

  let co = state.new_thread();
  let co_view = &mut unsafe { LuaState::view(co) };
  assert!(co_view.push_ref(function_ref));
  assert_eq!(co_view.resume(0), LUA_YIELD);
  co_view.pop(1);
  co_view.push_number(99.0);
  assert_eq!(co_view.resume(1), LUA_OK);
  assert_eq!(co_view.get_top(), 2);
  assert!(co_view.to_boolean(1), "pcall ok");
  assert_eq!(co_view.check_number(2), Some(99.0));

  // SAFETY：与 set_callback_context 配对摘除。
  unsafe { clear_callback_context((&raw const ctx).cast_mut().cast()) };
}

#[test]
fn view_shares_stack_and_refs() {
  let mut state = LuaState::new();
  state.push_integer(1);
  assert!(state.try_ref() > 0);

  // 视图共享真实栈与注册表：引用互通。
  let mut view = unsafe { LuaState::view(state.raw()) };
  assert_eq!(view.get_top(), 0);
  assert!(view.push_ref(1));
  assert_eq!(view.check_number(-1), Some(1.0));
  view.push_integer(2);
  assert_eq!(state.get_top(), 2);
}

#[test]
fn allocator_quota_stops_runaway_script() {
  let mut state = LuaState::with_allocator(LuaTrackedAllocator::new(1024 * 1024));
  state
    .load_string("local t = {} for i = 1, 100000 do t[i] = ('x'):rep(64) end")
    .unwrap();
  let err = state.pcall(0).unwrap_err();
  assert!(matches!(err, Error::Runtime(_)), "配额拒绝应折算为运行错误");
  state.clear_stack();
  // 配额内小脚本照常执行。
  state.load_string("return 'ok'").unwrap();
  state.pcall(0).unwrap();
  assert_eq!(state.known_string_to_buffer(-1).unwrap(), b"ok");
}

#[test]
fn host_fn_and_panic_containment() {
  struct Host;
  let mut host = Host;
  let host_ptr = (&raw mut host).cast::<c_void>();
  // SAFETY：指针在 pcall 窗口内存活，窗口后清除。
  unsafe { set_callback_context(host_ptr) };
  let mut state = LuaState::new();
  assert!(
    state.register_host_fn(b"garnet_add", |state: &mut LuaState, _host: &mut Host| {
      let a = state.check_number(1).unwrap_or_default();
      let b = state.check_number(2).unwrap_or_default();
      state.pop(2);
      state.push_number(a + b);
      1
    })
  );

  state.load_string("return garnet_add(2, 3)").unwrap();
  state.pcall(0).unwrap();
  assert_eq!(state.check_number(-1), Some(5.0));
  state.clear_stack();

  // panic 兜底：崩出域函数即折算 Lua 错误，进程不受影响。
  assert!(
    state.register_host_fn(b"garnet_boom", |_state, _host: &mut Host| {
      panic!("host panic");
    })
  );
  state.load_string("return garnet_boom()").unwrap();
  let err = state.pcall(0).unwrap_err();
  assert!(
    err
      .to_string()
      .contains("internal error in Lua host function")
  );
  state.clear_stack();
  // SAFETY：与开头 set_callback_context 配对清除（窗口守卫纪律）。
  unsafe { clear_callback_context(host_ptr) };
}

/// 常量串经生产链（push_buffer + try_ref + push_ref）转注册表往返
#[test]
fn constant_string_ref_roundtrip() {
  let mut state = LuaState::new();
  state.push_buffer(ConstantStrings::OK_LOWER);
  let id = state.try_ref();
  assert!(state.expect_lua_stack_empty());
  assert!(state.push_ref(id));
  assert_eq!(state.known_string_to_buffer(-1).unwrap(), b"ok");
}
