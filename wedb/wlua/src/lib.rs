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
mod api;
mod cache;
mod commands;
mod context;
mod error;
// 门面模块（宿主回调函数族，对位 LuaRunner.Functions.cs）：仅经模块路径可达，
// 不在 crate 根二次导出。
pub(crate) use context::{clear_callback_context, set_callback_context};
pub(crate) use error::Error;
pub use managed_allocator::LuaManagedAllocator;
pub mod functions;
mod functions_struct;
mod hash_key;
mod limited_allocator;
mod loader;
mod managed_allocator;
mod options;
mod runner;
mod state;
mod strings;
mod sys;
mod timeout;
mod tracked_allocator;

// 外部实际需要的入口（生命周期/执行入口/配置类型）逐项显式导出；
// 内部辅助与数据结构收在私有模块，仅 functions 门面按模块路径暴露。
pub(crate) use allocator::ILuaAllocator;
pub use allocator::LuaAllocator;
pub use api::{ScriptApiError, ScriptingApi};
pub use cache::{LuaScriptHandle, RunnerCreateOptions, SessionScriptCache};
pub use commands::{LuaCommands, LuaSessionContext, StoreScriptCache};
pub use hash_key::ScriptHashKey;
pub use limited_allocator::LuaLimitedManagedAllocator;
pub use options::{LuaLoggingMode, LuaMemoryManagementMode, LuaOptions};
pub use runner::{LuaRunner, RespObject};
pub use state::{Deadline, LuaState};
pub use timeout::{LuaTimeoutManager, TIMEOUT_TRIGGERED};
pub use tracked_allocator::LuaTrackedAllocator;

#[cfg(test)]
mod tests {
  use std::ffi::c_void;

  use super::{Error, LuaState};
  use crate::sys::{LUA_OK, LUA_YIELD};

  /// 实验验证（协程化改造前置语义探针）：cont == NULL 的 C 函数内
  /// `lua_yield(l, n)` 挂起协程，再次 `lua_resume` 时该 C 函数以 resume
  /// 前压入的参数值作为自身返回值收帧——redis.call 协程化挂起协议的
  /// 语义基石（ldo.cpp:lua_yield 返回 -1 + LOP_CALL 负返回出口 +
  /// resume() 的 `luau_poscall(L, firstArg)` 收帧路径）。
  #[test]
  fn yield_resume_c_function_semantics() {
    use std::sync::atomic::{AtomicI32, Ordering};

    use crate::sys;

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
    unsafe { crate::set_callback_context((&raw const ctx).cast_mut().cast()) };

    let co = state.new_thread();
    let co_view = &mut LuaState::view(co);
    assert!(co_view.push_ref(function_ref), "function pushed on thread");

    // 首段 resume：C 函数挂起，让渡值 (42, 7) 留在协程栈
    assert_eq!(co_view.resume(0), sys::LUA_YIELD);
    assert_eq!(co_view.get_top(), 2);
    assert_eq!(co_view.check_number(1), Some(42.0));
    assert_eq!(co_view.check_number(2), Some(7.0));

    // 弹让渡值，压 resume 传值——它们成为 C 函数的返回值
    co_view.pop(2);
    co_view.push_number(100.0);
    co_view.push_number(200.0);
    co_view.push_number(300.0);
    assert_eq!(co_view.resume(3), sys::LUA_OK);
    assert_eq!(co_view.get_top(), 3);
    assert_eq!(co_view.check_number(1), Some(100.0));
    assert_eq!(co_view.check_number(3), Some(300.0));

    // SAFETY：与 set_callback_context 配对摘除。
    unsafe { crate::clear_callback_context((&raw const ctx).cast_mut().cast()) };
  }

  /// 实验补充：pcall 包裹路径的 yield 放行性（Luau 经 baseCcalls 维持
  /// 可让渡不变式，redis.call 在脚本 pcall 内挂起不得误报
  /// "attempt to yield across metamethod/C-call boundary"）。
  #[test]
  fn yield_across_pcall_is_yieldable() {
    use std::sync::atomic::{AtomicI32, Ordering};

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
    unsafe { crate::set_callback_context((&raw const ctx).cast_mut().cast()) };

    let co = state.new_thread();
    let co_view = &mut LuaState::view(co);
    assert!(co_view.push_ref(function_ref));
    assert_eq!(co_view.resume(0), LUA_YIELD);
    co_view.pop(1);
    co_view.push_number(99.0);
    assert_eq!(co_view.resume(1), LUA_OK);
    assert_eq!(co_view.get_top(), 2);
    assert!(co_view.to_boolean(1), "pcall ok");
    assert_eq!(co_view.check_number(2), Some(99.0));

    // SAFETY：与 set_callback_context 配对摘除。
    unsafe { crate::clear_callback_context((&raw const ctx).cast_mut().cast()) };
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
}
