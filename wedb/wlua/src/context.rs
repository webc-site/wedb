//! 宿主回调上下文与 C 蹦床：panic 不跨 FFI 的纪律边界。
//!
//! C# 侧以 ThreadStatic `this` + UnmanagedCallersOnly 蹦床承接 lua_CFunction；
//! 本模块以 thread-local 上下文指针 + `catch_unwind` 蹦床承接同语义：上下文
//! 由宿主在回调窗口内挂入（RAII 守卫保证清除），蹦床取上下文驱动域函数。

use std::{
  cell::Cell,
  ffi::c_void,
  mem::transmute,
  panic::{AssertUnwindSafe, catch_unwind},
  ptr,
};

use crate::sys;

thread_local! {
  /// 回调窗口上下文（仅同一调用线程内有效；窗口外为空）。
  static CALLBACK_CONTEXT: Cell<*mut c_void> = const { Cell::new(ptr::null_mut()) };
}

/// 设置回调窗口上下文（窗口内有效；重复设置视为程序性错误）。
///
/// # Safety
/// 指针须在回调窗口期间保持有效（窗口结束后不得再解引用）。
pub unsafe fn set_callback_context(context: *mut c_void) {
  CALLBACK_CONTEXT.with(|cell| {
    debug_assert!(cell.get().is_null(), "Expected null context");
    cell.set(context);
  });
}

/// 清除回调窗口上下文。
///
/// # Safety
/// `context` 须为 [`set_callback_context`] 挂入的指针。
pub unsafe fn clear_callback_context(context: *mut c_void) {
  CALLBACK_CONTEXT.with(|cell| {
    debug_assert_eq!(cell.get(), context, "Expected context to match");
    cell.set(ptr::null_mut());
  });
}

/// 取当前回调上下文（未设置返回空）。
pub fn callback_context() -> *mut c_void {
  CALLBACK_CONTEXT.with(Cell::get)
}

/// 宿主函数蹦床：取上值中的域函数指针与窗口上下文执行，panic 兜底为 Lua 错误。
///
/// # Safety（lua_CFunction 契约）
/// - 经 [`super::LuaState::register_host_fn`] 以 `lua_pushcclosurek` 注册，
///   上值 1 为域函数指针（lightuserdata 形态）。
/// - panic 不跨 FFI：`catch_unwind` 收敛后压错误串并 `lua_error` 长跳转至
///   宿主 `lua_pcall`；跳越帧（蹦床自身）内除 POD 外无存活析构对象。
pub(crate) unsafe extern "C" fn host_trampoline<C: 'static>(l: *mut sys::lua_State) -> i32 {
  let result = catch_unwind(AssertUnwindSafe(|| {
    let host = callback_context();
    assert!(!host.is_null(), "no lua callback context");
    // SAFETY：上值 1 由 register_host_fn 压入，类型即域函数指针。
    let function = unsafe {
      let raw = sys::lua_tolightuserdatatagged(l, sys::lua_upvalueindex(1), 0);
      transmute::<*mut c_void, fn(&mut crate::LuaState, &mut C) -> i32>(raw)
    };
    // SAFETY：host 非空且由窗口守卫保证窗口内有效（C 与注册时一致）。
    let host = unsafe { &mut *host.cast::<C>() };
    let mut state = crate::LuaState::view(l);
    function(&mut state, host)
  }));
  match result {
    Ok(count) => count,
    Err(_) => {
      log::error!("lua host function panicked; converting to Lua error");
      // SAFETY：压串后长跳转；本帧内无析构对象，longjmp 纪律见上。
      unsafe {
        sys::lua_pushlstring(l, HOST_PANIC_MSG.as_ptr().cast(), HOST_PANIC_MSG.len());
        sys::lua_error(l);
      }
    }
  }
}

/// panic 兜底错误文案。
const HOST_PANIC_MSG: &[u8] = b"ERR internal error in Lua host function";
