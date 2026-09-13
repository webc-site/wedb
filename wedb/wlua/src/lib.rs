//! wlua：Luau C API 直连薄封装。
//!
//! 以 vendored 官方 Luau（luau0-src，cc 编译静态链接）为底座，提供：
//! - [`LuaState`]：栈操作/表/注册表引用/pcall/装载的 C API 语义层；
//! - [`ILuaAllocator`]：`lua_Alloc` 直挂的自定义分配器（内存配额）；
//! - VM safepoint 中断回调（脚本超时钩子）；
//! - 宿主函数注册（C 蹦床 + catch_unwind，panic 不跨 FFI）。
//!
//! 对标 garnet libs/server/Lua 的绑定层；RESP 值编解码留在会话域。

#![cfg_attr(docsrs, feature(doc_cfg))]

mod allocator;
pub mod api;
pub mod cache;
pub mod commands;
mod context;
mod error;
pub mod functions;
pub mod functions_struct;
pub mod hash_key;
pub mod limited_allocator;
pub mod loader;
pub mod managed_allocator;
pub mod options;
pub mod runner;
pub mod sender;
mod state;
pub mod strings;
mod sys;
pub mod timeout;
pub mod tracked_allocator;

pub use allocator::{ILuaAllocator, LuaAllocator};
pub use api::*;
pub use cache::*;
pub use commands::*;
pub use context::{callback_context, clear_callback_context, set_callback_context};
pub use error::{Error, Result};
pub use functions::*;
pub use functions_struct::*;
pub use hash_key::{SHA1_HEX_LEN, ScriptHashKey};
pub use limited_allocator::{BLOCK_HEADER_SIZE, BlockRef, LuaLimitedManagedAllocator};
pub use loader::*;
pub use managed_allocator::LuaManagedAllocator;
pub use options::{LuaLoggingMode, LuaMemoryManagementMode, LuaOptions};
pub use runner::*;
pub use sender::*;
pub use state::{LuaState, now_monotonic_millis};
pub use strings::*;
pub use timeout::{LuaTimeoutManager, TimeoutCookie};
pub use tracked_allocator::LuaTrackedAllocator;

#[cfg(test)]
mod tests {
  use std::ffi::c_void;

  use super::{Error, LuaState, now_monotonic_millis};

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
  fn rotate_and_stack_top() {
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
    state.update_stack_top(5);
    assert_eq!(state.get_top(), 5);
    assert_eq!(state.type_name(4), Some("nil"));
    state.update_stack_top(1);
    assert_eq!(state.get_top(), 1);
  }

  #[test]
  fn view_shares_stack_and_refs() {
    let mut state = LuaState::new();
    state.push_integer(1);
    assert!(state.try_ref() > 0);

    // 视图共享真实栈与注册表：引用互通。
    let mut view = LuaState::view(state.raw());
    assert_eq!(view.get_top(), 0);
    assert!(view.push_ref(1));
    assert_eq!(view.check_number(-1), Some(1.0));
    view.push_integer(2);
    assert_eq!(state.get_top(), 2);
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

  #[test]
  fn allocator_quota_stops_runaway_script() {
    let mut state = LuaState::with_allocator(super::LuaTrackedAllocator::new(1024 * 1024));
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
    use super::{clear_callback_context, set_callback_context};

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

  #[test]
  fn timeout_interrupt_raises_error() {
    let mut state = LuaState::new();
    // 截止已过：下一 safepoint（循环回边）即中断。
    state.try_set_hook(Some(now_monotonic_millis()));
    state
      .load_string("local i = 0; while true do i = i + 1 end")
      .unwrap();
    let err = state.pcall(0).unwrap_err();
    assert!(err.to_string().contains("exceeded configured timeout"));
    state.clear_stack();
    // 撤销截止后脚本可正常跑完。
    state.try_set_hook(None);
    state.load_string("return 42").unwrap();
    state.pcall(0).unwrap();
    assert_eq!(state.check_number(-1), Some(42.0));
  }

  #[test]
  fn stack_capacity_and_cfunction_and_const_string() {
    let mut state = LuaState::new();
    assert!(state.try_ensure_minimum_stack_capacity(50));

    // 测试 push_constant_string
    state.push_buffer(b"hello_constant");
    let ref_id = state.try_ref();
    assert!(state.push_constant_string(ref_id));
    assert_eq!(state.known_string_to_buffer(-1).unwrap(), b"hello_constant");
    state.unref(ref_id);
    state.clear_stack();

    // 测试 push_cfunction
    use std::ffi::c_int;

    use crate::sys::{lua_State, lua_pushnumber};

    unsafe extern "C" fn test_fn(l: *mut lua_State) -> c_int {
      unsafe {
        lua_pushnumber(l, 123.0);
      }
      1
    }
    state.push_cfunction(test_fn);
    state.set_global(b"my_c_fn");
    state.load_string("return my_c_fn()").unwrap();
    state.pcall(0).unwrap();
    assert_eq!(state.check_number(-1), Some(123.0));
  }
}
