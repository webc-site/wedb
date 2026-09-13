//! Luau C API FFI 声明（vendored 官方 Luau 0.736，`lua.h`/`lualib.h`/`luacode.h` 最小子集）。
//!
//! 布局关键：[`lua_Callbacks`] 与 [`lua_CompileOptions`] 按 C 头文件字段序完整
//! 转写（部分结构体声明 = 未定义行为）；未消费的回调字段以单元函数指针占位，
//! 所有函数指针同布局，不影响结构体大小与对齐。
//!
//! 常量 [`LUA_REGISTRYINDEX`]/[`LUA_GLOBALSINDEX`] 依赖构建期
//! `LUAI_MAXCSTACK`（luau0-src 默认 1_000_000，见 build.rs——勿改 Build 配置）。

use std::ffi::{c_char, c_int, c_void};

/// lua 虚拟机实例（opaque）。
#[repr(C)]
pub struct lua_State {
  _private: [u8; 0],
}

/// 调试激活记录（opaque；仅以指针形态出现在回调签名中）。
#[repr(C)]
pub struct lua_Debug {
  _private: [u8; 0],
}

/// C 函数（宿主回调）形态。
pub type LuaCFunction = unsafe extern "C" fn(l: *mut lua_State) -> c_int;

/// 可恢复调用的续延（本封装不使用，恒 None）。
pub type LuaContinuation = Option<unsafe extern "C" fn(l: *mut lua_State, status: c_int) -> c_int>;

/// 自定义分配器：`nsize == 0` 释放 `ptr` 并返回 NULL；否则分配/扩缩容。
pub type LuaAlloc = unsafe extern "C" fn(
  ud: *mut c_void,
  ptr: *mut c_void,
  osize: usize,
  nsize: usize,
) -> *mut c_void;

/// VM 动态回调集（lua.h `lua_Callbacks`，字段序与头文件一致）。
#[repr(C)]
pub struct lua_Callbacks {
  /// Luau 不会覆写的宿主指针。
  pub userdata: *mut c_void,
  /// safepoint（循环回边/调用返回/GC）触发；可由任意线程设置。
  pub interrupt: Option<unsafe extern "C" fn(l: *mut lua_State, gc: c_int)>,
  /// 无保护错误（长跳转无落点）时调用。
  pub panic: Option<unsafe extern "C" fn(l: *mut lua_State, errcode: c_int)>,
  /// 线程创建（LP = 父）/销毁（LP = NULL）。
  pub userthread: Option<unsafe extern "C" fn(lp: *mut lua_State, l: *mut lua_State)>,
  /// 字符串创建时分配原子 id。
  pub useratom:
    Option<unsafe extern "C" fn(l: *mut lua_State, s: *const c_char, len: usize) -> i16>,
  /// BREAK 指令命中。
  pub debugbreak: Option<unsafe extern "C" fn(l: *mut lua_State, ar: *mut lua_Debug)>,
  /// 单步模式逐指令。
  pub debugstep: Option<unsafe extern "C" fn(l: *mut lua_State, ar: *mut lua_Debug)>,
  /// 他线程 break 中断本线程。
  pub debuginterrupt: Option<unsafe extern "C" fn(l: *mut lua_State, ar: *mut lua_Debug)>,
  /// 保护调用出错。
  pub debugprotectederror: Option<unsafe extern "C" fn(l: *mut lua_State)>,
  /// 堆对象分配后回调。
  pub onallocate: Option<
    unsafe extern "C" fn(
      l: *mut lua_State,
      block: *mut c_void,
      osize: usize,
      nsize: usize,
      memcat: u8,
      tt: c_int,
      tag: c_int,
    ),
  >,
  /// lua_resume 前。
  pub preresume: Option<unsafe extern "C" fn(l: *mut lua_State)>,
  /// lua_resume 后。
  pub postresume: Option<unsafe extern "C" fn(l: *mut lua_State)>,
  /// 堆对象释放前回调。
  pub onfree: Option<unsafe extern "C" fn(l: *mut lua_State, block: *mut c_void)>,
}

/// 编译选项（luacode.h `lua_CompileOptions`，字段序与头文件一致）。
#[repr(C)]
pub struct lua_CompileOptions {
  /// 优化档位（0 关闭；1 基线；2 妨害调试的深优化）。
  pub optimization_level: c_int,
  /// 调试信息档位（0 无；1 行号+函数名；2 全量局部/上值名）。
  pub debug_level: c_int,
  /// 类型信息档位（供原生代码生成）。
  pub type_info_level: c_int,
  /// 覆盖率档位。
  pub coverage_level: c_int,
  /// 向量构造备用全局库。
  pub vector_lib: *const c_char,
  /// 向量构造备用构造函数名。
  pub vector_ctor: *const c_char,
  /// 向量备用类型名。
  pub vector_type: *const c_char,
  /// 向量分量精度（0 = f32）。
  pub vector_precision: c_int,
  /// 可变全局名表（NULL 结尾；关闭这些全局的 import 优化）。
  pub mutable_globals: *const *const c_char,
  /// 类型信息收录的 userdata 类型（NULL 结尾）。
  pub userdata_types: *const *const c_char,
  /// 成员类型/常量已知的库（NULL 结尾）。
  pub libraries_with_known_members: *const *const c_char,
  /// 库成员类型回调。
  pub library_member_type_cb: Option<unsafe extern "C" fn()>,
  /// 库成员常量回调。
  pub library_member_constant_cb: Option<unsafe extern "C" fn()>,
  /// 禁用内建 fastcall 的库函数名表（NULL 结尾）。
  pub disabled_builtins: *const *const c_char,
}

/// 栈/注册表伪下标（构建期 LUAI_MAXCSTACK = 1_000_000 折算）。
pub const LUA_REGISTRYINDEX: c_int = -1_000_000 - 2000;
/// 全局表伪下标。
pub const LUA_GLOBALSINDEX: c_int = -1_000_000 - 2002;
/// C 函数第 `i` 个上值（1 基）。
pub const fn lua_upvalueindex(i: c_int) -> c_int {
  LUA_GLOBALSINDEX - i
}
/// pcall/call 的多返回值档位。
pub const LUA_MULTRET: c_int = -1;
/// 保护调用成功状态。
pub const LUA_OK: c_int = 0;
/// 注册表引用哨兵：空引用（nil 值引用以 0 = REFNIL 表达，lua_unref 幂等）。
pub const LUA_NOREF: c_int = -1;

/// 值类型码（lua.h `lua_Type` 枚举序）。
pub const LUA_TNONE: c_int = -1;
pub const LUA_TNIL: c_int = 0;
pub const LUA_TBOOLEAN: c_int = 1;
pub const LUA_TLIGHTUSERDATA: c_int = 2;
pub const LUA_TNUMBER: c_int = 3;
/// 0.736 新增 int64 子类型（`lua_pushinteger64` 产出）。
pub const LUA_TINTEGER: c_int = 4;
pub const LUA_TVECTOR: c_int = 5;
pub const LUA_TSTRING: c_int = 6;
pub const LUA_TTABLE: c_int = 7;
pub const LUA_TFUNCTION: c_int = 8;

unsafe extern "C" {
  // ---- 状态机与库 ----
  pub fn lua_newstate(allocator: LuaAlloc, ud: *mut c_void) -> *mut lua_State;
  pub fn lua_close(l: *mut lua_State);
  pub fn luaL_newstate() -> *mut lua_State;
  pub fn luaL_openlibs(l: *mut lua_State);

  // ---- 栈基础 ----
  pub fn lua_gettop(l: *mut lua_State) -> c_int;
  pub fn lua_settop(l: *mut lua_State, idx: c_int);
  pub fn lua_remove(l: *mut lua_State, idx: c_int);
  pub fn lua_insert(l: *mut lua_State, idx: c_int);
  pub fn lua_pushvalue(l: *mut lua_State, idx: c_int);
  pub fn lua_checkstack(l: *mut lua_State, sz: c_int) -> c_int;

  // ---- 压栈 ----
  pub fn lua_pushnil(l: *mut lua_State);
  pub fn lua_pushnumber(l: *mut lua_State, n: f64);
  pub fn lua_pushboolean(l: *mut lua_State, b: c_int);
  pub fn lua_pushlstring(l: *mut lua_State, s: *const c_char, len: usize);
  pub fn lua_pushlightuserdatatagged(l: *mut lua_State, p: *mut c_void, tag: c_int);
  pub fn lua_pushcclosurek(
    l: *mut lua_State,
    f: LuaCFunction,
    debugname: *const c_char,
    nup: c_int,
    cont: LuaContinuation,
  );

  // ---- 取值与类型 ----
  pub fn lua_type(l: *mut lua_State, idx: c_int) -> c_int;
  pub fn lua_tonumberx(l: *mut lua_State, idx: c_int, isnum: *mut c_int) -> f64;
  pub fn lua_toboolean(l: *mut lua_State, idx: c_int) -> c_int;
  pub fn lua_tolstring(l: *mut lua_State, idx: c_int, len: *mut usize) -> *const c_char;
  pub fn lua_objlen(l: *mut lua_State, idx: c_int) -> usize;
  pub fn lua_tolightuserdatatagged(l: *mut lua_State, idx: c_int, tag: c_int) -> *mut c_void;
  pub fn lua_replace(l: *mut lua_State, idx: c_int);
  pub fn luaL_tolstring(l: *mut lua_State, idx: c_int, len: *mut usize) -> *const c_char;

  // ---- 表 ----
  pub fn lua_createtable(l: *mut lua_State, narr: c_int, nrec: c_int);
  pub fn lua_getfield(l: *mut lua_State, idx: c_int, k: *const c_char) -> c_int;
  pub fn lua_setfield(l: *mut lua_State, idx: c_int, k: *const c_char);
  pub fn lua_rawget(l: *mut lua_State, idx: c_int) -> c_int;
  pub fn lua_rawset(l: *mut lua_State, idx: c_int);
  pub fn lua_rawgeti(l: *mut lua_State, idx: c_int, n: c_int) -> c_int;
  pub fn lua_rawseti(l: *mut lua_State, idx: c_int, n: c_int);
  pub fn lua_next(l: *mut lua_State, idx: c_int) -> c_int;

  // ---- 注册表引用 ----
  pub fn lua_ref(l: *mut lua_State, idx: c_int) -> c_int;
  pub fn lua_unref(l: *mut lua_State, r: c_int);

  // ---- 调用与错误 ----
  pub fn lua_pcall(l: *mut lua_State, nargs: c_int, nresults: c_int, errfunc: c_int) -> c_int;
  pub fn lua_error(l: *mut lua_State) -> !;

  // ---- 内存与回调 ----
  pub fn lua_totalbytes(l: *mut lua_State, category: c_int) -> usize;
  pub fn lua_callbacks(l: *mut lua_State) -> *mut lua_Callbacks;

  // ---- 装载（编译 + 加载字节码） ----
  pub fn luau_load(
    l: *mut lua_State,
    chunkname: *const c_char,
    data: *const c_char,
    size: usize,
    env: c_int,
  ) -> c_int;
  pub fn luau_compile(
    source: *const c_char,
    size: usize,
    options: *mut lua_CompileOptions,
    outsize: *mut usize,
  ) -> *mut c_char;
  pub fn free(ptr: *mut c_void);
}
