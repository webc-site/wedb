//! `LuaState` 栈/表/pcall 语义层纯 pub API 面集成测
//!
//! r329 自 `wlua/src/lib.rs` 内联测模块迁入；触 `crate::sys`（两 yield 系测）、
//! pub(crate) `Error` 枚举（配额测）、pub(crate) `LuaState::view`
//! （view 共享测）与 pub(crate) 回调上下文（host_fn 测）者留守内联。

use wlua::LuaState;

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
