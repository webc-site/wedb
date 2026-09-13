//! Lua 状态薄封装：直连 Luau C API 的栈语义层
//! （对标 libs/server/Lua/LuaStateWrapper.cs:LuaStateWrapper）。
//!
//! 每个方法与 Luau `lua.h` C API 一一对应；指针生命周期论证集中在各
//! `unsafe` 块的 SAFETY 注释。GC 锚定纪律：一切值仅经真实 Lua 栈
//! （下标定位，栈上即锚定）或注册表引用（[`Self::try_ref`]）持有，
//! 不存在栈外句柄，故无悬垂窗口。

use std::{
  cell::{Cell, UnsafeCell},
  ffi::{CString, c_int, c_void},
  marker::PhantomData,
  mem, ptr,
  slice::from_raw_parts,
};

use super::{
  allocator::{AllocatorSlot, ILuaAllocator, LuaAllocator, alloc_shim},
  context,
  error::{Error, Result, runtime},
  sys,
};

/// 超时中断错误文案（run_common 对 "ERR " 前缀原样透传）。
pub const TIMEOUT_ERROR: &[u8] = b"ERR Lua script exceeded configured timeout";

/// 超时截止槽（单调毫秒；挂给 VM 中断回调读取）。
type Deadline = Cell<Option<i64>>;

/// 当前单调毫秒（coarsetime；超时截止的时钟基准）。
pub fn now_monotonic_millis() -> i64 {
  coarsetime::Clock::now_since_epoch().as_millis() as i64
}

/// Lua 状态：VM 指针 + 可选自定义分配器 + 超时截止。
///
/// `!Send`：VM 与宿主回调窗口同线程，不得跨线程移交。
pub struct LuaState {
  /// VM 指针（构造即锚定；owned 时 Drop 负责 lua_close）。
  l: *mut sys::lua_State,
  /// true = 持有 VM（Drop 关闭）；view 形态为 false。
  owned: bool,
  /// 自定义分配器槽（`lua_Alloc` 的 ud 指向此堆址；先 lua_close 后弃 Box）。
  allocator: Option<Box<AllocatorSlot>>,
  /// 超时截止槽（曾设过钩子后恒为 Some）。
  deadline: Option<Box<Deadline>>,
  /// 是否发生过不可恢复的状态异常（对标 C# LuaStateWrapper.NeedsDispose）。
  needs_dispose: bool,
  /// 栈指针非 Send/Sync 标记。
  _marker: PhantomData<*mut sys::lua_State>,
}

impl Drop for LuaState {
  fn drop(&mut self) {
    // 必须先关 VM（VM 销毁期间的分配走 allocator 槽）再弃槽本体。
    if self.owned {
      // SAFETY：l 由 new/with_allocator 创建且仅此一处关闭（owned 唯一）。
      unsafe { sys::lua_close(self.l) };
    }
  }
}

impl Default for LuaState {
  fn default() -> Self {
    Self::new()
  }
}

impl LuaState {
  /// 构造：新建 VM 并装载标准库（默认分配器）。
  pub fn new() -> Self {
    // SAFETY：luaL_newstate 失败仅返回 NULL（内存耗尽属进程级致命）。
    let l = unsafe { sys::luaL_newstate() };
    assert!(!l.is_null(), "luaL_newstate failed");
    // SAFETY：l 为刚创建的有效 VM。
    unsafe { sys::luaL_openlibs(l) };
    Self {
      l,
      owned: true,
      allocator: None,
      deadline: None,
      needs_dispose: false,
      _marker: PhantomData,
    }
  }

  /// 构造：以宿主分配器创建 VM（内存上限经配额拒绝承载）。
  pub fn with_allocator(allocator: impl Into<LuaAllocator>) -> Self {
    let slot: Box<AllocatorSlot> = Box::new(UnsafeCell::new(allocator.into()));
    // SAFETY：ud 指向随 self 存活的堆槽；Drop 序（先 lua_close 后弃 Box）
    // 保证 alloc_shim 的 ud 解引用不晚于槽销毁。
    let l = unsafe { sys::lua_newstate(alloc_shim, (&raw const *slot).cast_mut().cast()) };
    assert!(!l.is_null(), "lua_newstate failed");
    // SAFETY：l 为刚创建的有效 VM。
    unsafe { sys::luaL_openlibs(l) };
    Self {
      l,
      owned: true,
      allocator: Some(slot),
      deadline: None,
      needs_dispose: false,
      _marker: PhantomData,
    }
  }

  /// 以既有 VM 建临时视图（宿主回调侧入口）：共享真实栈与注册表，
  /// 不持有 VM（无 Drop 关闭）、不接管分配器与截止。
  pub(crate) fn view(l: *mut sys::lua_State) -> Self {
    Self {
      l,
      owned: false,
      allocator: None,
      deadline: None,
      needs_dispose: false,
      _marker: PhantomData,
    }
  }

  /// VM 裸指针（回调装配使用）。
  pub fn raw(&self) -> *mut sys::lua_State {
    self.l
  }

  /// libs/server/Lua/LuaStateWrapper.cs:ExpectLuaStackEmpty
  ///
  /// 栈是否为空（debug 断言面；回调帧入口即帧参数计数）。
  pub fn expect_lua_stack_empty(&self) -> bool {
    self.get_top() == 0
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TryEnsureMinimumStackCapacity
  ///
  /// 确保栈容量满足至少 `additional_capacity` 个额外槽位。
  pub fn try_ensure_minimum_stack_capacity(&mut self, additional_capacity: usize) -> bool {
    // SAFETY：sys::lua_checkstack 校验并按需扩容栈空间
    unsafe { sys::lua_checkstack(self.l, additional_capacity as c_int) != 0 }
  }

  /// 保护模式调用：弹出函数与 `nargs` 个参数，压回全部返回值（MULTRET）。
  pub fn pcall(&mut self, nargs: usize) -> Result<()> {
    self.pcall_n(nargs, usize::MAX).map(|_| ())
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PCall
  ///
  /// 成功时结果按 `nresults` 截断/补 nil（`usize::MAX` = MULTRET 全保留），
  /// 返回压栈结果数；失败时错误对象压栈并返回 Err（错误文案）。
  pub fn pcall_n(&mut self, nargs: usize, nresults: usize) -> Result<usize> {
    let nargs = nargs as c_int;
    let nres = if nresults == usize::MAX {
      sys::LUA_MULTRET
    } else {
      nresults as c_int
    };
    // SAFETY：栈平衡——pcall 消费 (1 + nargs) 槽，按 nres 回填；
    // errfunc = 0：错误对象原样入栈（串错误即错误串）。
    let below = unsafe { sys::lua_gettop(self.l) } - nargs - 1;
    let status = unsafe { sys::lua_pcall(self.l, nargs, nres, 0) };
    if status == sys::LUA_OK {
      // SAFETY：读取刚调用的有效 VM 栈高。
      let pushed = unsafe { sys::lua_gettop(self.l) } - below;
      Ok(pushed.max(0) as usize)
    } else {
      Err(runtime(self.error_object_text()))
    }
  }

  /// 错误对象文本（栈顶；非串对象折算类型名）。
  fn error_object_text(&self) -> String {
    let mut len = 0usize;
    // SAFETY：仅读栈顶；返回指针随栈上对象存活至出栈，读取即刻完成。
    let text = unsafe { sys::lua_tolstring(self.l, -1, &mut len) };
    if text.is_null() {
      return format!(
        "non-string error object ({})",
        self.type_name(-1).unwrap_or("?")
      );
    }
    // SAFETY：text/len 指向栈上 Lua 串，拷贝在出栈前完成。
    let bytes = unsafe { from_raw_parts(text.cast::<u8>(), len) };
    String::from_utf8_lossy(bytes).into_owned()
  }

  /// libs/server/Lua/LuaStateWrapper.cs:Type
  ///
  /// `idx` 处元素类型名（int64/vector 归一为 "number"，对齐宿主判定面）。
  pub fn type_name(&self, idx: i32) -> Option<&'static str> {
    // SAFETY：idx 为可接受下标（越界时 TNONE 返回 None）。
    let tp = unsafe { sys::lua_type(self.l, idx as c_int) };
    match tp {
      sys::LUA_TNONE => None,
      sys::LUA_TNIL => Some("nil"),
      sys::LUA_TBOOLEAN => Some("boolean"),
      sys::LUA_TLIGHTUSERDATA => Some("userdata"),
      sys::LUA_TNUMBER | sys::LUA_TINTEGER | sys::LUA_TVECTOR => Some("number"),
      sys::LUA_TSTRING => Some("string"),
      sys::LUA_TTABLE => Some("table"),
      sys::LUA_TFUNCTION => Some("function"),
      _ => Some("userdata"),
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TryPushBuffer
  ///
  /// 压入字节缓冲为字符串（进入/退出 infallible 区间，超限由配额分配器处置）。
  pub fn push_buffer(&mut self, buffer: &[u8]) -> bool {
    self.enter_infallible_allocation_region();
    // SAFETY：buffer 在调用期间存活；VM 侧复制内容。
    unsafe { sys::lua_pushlstring(self.l, buffer.as_ptr().cast(), buffer.len()) };
    self.try_exit_infallible_allocation_region()
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PushNil
  pub fn push_nil(&mut self) {
    // SAFETY：有效 VM；纯压栈。
    unsafe { sys::lua_pushnil(self.l) };
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PushNumber
  pub fn push_number(&mut self, number: f64) {
    // SAFETY：有效 VM；纯压栈。
    unsafe { sys::lua_pushnumber(self.l, number) };
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PushInteger
  ///
  /// Luau 数值皆为双精度（lua_pushinteger 即 cast_num），i64 经 f64 承载
  /// 与 VM 自身语义一致。
  pub fn push_integer(&mut self, integer: i64) {
    // SAFETY：有效 VM；纯压栈。
    unsafe { sys::lua_pushnumber(self.l, integer as f64) };
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PushBoolean
  pub fn push_boolean(&mut self, boolean: bool) {
    // SAFETY：有效 VM；纯压栈。
    unsafe { sys::lua_pushboolean(self.l, c_int::from(boolean)) };
  }

  /// libs/server/Lua/LuaStateWrapper.cs:Pop
  ///
  /// 弹出 `count` 个栈元素。
  pub fn pop(&mut self, count: usize) {
    // SAFETY：settop 负向收缩；count 超栈高时落在 0（等效清空）。
    unsafe { sys::lua_settop(self.l, -(count as c_int) - 1) };
  }

  /// libs/server/Lua/LuaStateWrapper.cs:Remove
  ///
  /// 移除 `idx` 处元素，其上元素整体下移。
  pub fn remove(&mut self, idx: i32) {
    // SAFETY：idx 有效性由调用方保证（与 C API 相同约束）。
    unsafe { sys::lua_remove(self.l, idx as c_int) };
  }

  /// libs/server/Lua/LuaStateWrapper.cs:RawSetInteger
  ///
  /// 表\[idx] = 栈顶值（表位于 `table_idx`，整型键；栈顶值弹出）。
  pub fn raw_set_integer(&mut self, table_idx: i32, key: i64) {
    // SAFETY：键为 1 基小序号折算 c_int（调用点约束）；table_idx 指向表。
    unsafe { sys::lua_rawseti(self.l, table_idx as c_int, key as c_int) };
  }

  /// libs/server/Lua/LuaStateWrapper.cs:RawSet
  ///
  /// 表\[key] = 栈顶值（键/值取自栈顶两元素并弹出）。
  pub fn raw_set(&mut self, table_idx: i32) {
    // SAFETY：table_idx 在弹键值前解析（C API 契约）；须指向表。
    unsafe { sys::lua_rawset(self.l, table_idx as c_int) };
  }

  /// libs/server/Lua/LuaStateWrapper.cs:RawGetInteger
  ///
  /// 表\[idx] 值压栈（缺键压 nil；`table_idx` 须指向表，调用点均先行校验）。
  pub fn raw_get_integer(&mut self, table_idx: i32, key: i64) {
    // SAFETY：同上；键为 1 基小序号折算 c_int。
    unsafe { sys::lua_rawgeti(self.l, table_idx as c_int, key as c_int) };
  }

  /// libs/server/Lua/LuaStateWrapper.cs:RawGet（键取自栈顶形态）
  ///
  /// 以栈顶元素为键查 `table_idx` 表：弹出键，压入查得值（缺键 nil）。
  pub fn raw_get(&mut self, table_idx: i32) {
    // SAFETY：table_idx 在弹键前解析（C API 契约）；须指向表。
    unsafe { sys::lua_rawget(self.l, table_idx as c_int) };
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TryRef
  /// libs/server/Lua/NativeMethods.cs:Ref
  ///
  /// 引用栈顶元素到注册表并弹出，返回引用 id（nil → REFNIL，失败哨兵
  /// NOREF，两者均 <= 0）。C# 侧经 NativeMethods.Ref（luaL_ref 的 P/Invoke
  /// 托管转发，唯一调用方即 TryRef）两跳实现；Rust 无 P/Invoke 隔层，
  /// 本方法直接经 `sys::lua_ref` FFI 单跳完成同一功能。
  pub fn try_ref(&mut self) -> i32 {
    self.enter_infallible_allocation_region();
    // SAFETY：-1 为当前栈顶；引用后值由注册表持有（GC 锚定转移）。
    let id = unsafe { sys::lua_ref(self.l, -1) };
    self.pop(1);
    if !self.try_exit_infallible_allocation_region() {
      log::warn!("Lua 退出不可失败内存分配区返回 false");
    }
    id
  }

  /// libs/server/Lua/LuaStateWrapper.cs:Unref
  pub fn unref(&mut self, ref_id: i32) {
    if ref_id > sys::LUA_NOREF {
      // SAFETY：ref_id 由 try_ref 产出；unref 对 NOREF/REFNIL 幂等空操作。
      unsafe { sys::lua_unref(self.l, ref_id) };
    }
  }

  /// 引用压栈（对标 C# RawGetInteger(LuaRegistry.Index, id) 伪索引形态）。
  ///
  /// 返回压入值是否非 nil（失效/nil 引用为 false）。
  pub fn push_ref(&mut self, ref_id: i32) -> bool {
    // SAFETY：注册表伪下标 + try_ref 产出的 id。
    unsafe { sys::lua_rawgeti(self.l, sys::LUA_REGISTRYINDEX, ref_id) };
    self.type_name(-1) != Some("nil")
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PushConstantString
  ///
  /// 将注册表中的常量字符串压栈（1:1 对标 C# RawGetInteger(LuaType.String, LuaRegistry.Index, constStringRegistryIndex)）。
  pub fn push_constant_string(&mut self, const_string_registry_index: i32) -> bool {
    self.push_ref(const_string_registry_index)
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TryCreateTable
  ///
  /// 压入新表（`narr`/`nrec` 仅容量提示）。
  pub fn create_table(&mut self, narr: usize, nrec: usize) -> bool {
    self.enter_infallible_allocation_region();
    // SAFETY：容量提示折算 c_int（表自增长，不因提示值溢出）。
    unsafe { sys::lua_createtable(self.l, narr as c_int, nrec as c_int) };
    self.try_exit_infallible_allocation_region()
  }

  /// libs/server/Lua/LuaStateWrapper.cs:GetGlobal
  ///
  /// 全局压栈；返回全局是否存在（nil → false）。
  pub fn get_global(&mut self, name: &[u8]) -> bool {
    let Some(name) = cstr(name) else {
      return false;
    };
    // SAFETY：name 以 NUL 结尾且在调用期间存活。
    unsafe { sys::lua_getfield(self.l, sys::LUA_GLOBALSINDEX, name.as_ptr()) };
    self.type_name(-1) != Some("nil")
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TrySetGlobal
  ///
  /// 栈顶值写入全局（值弹出）；名字含 NUL 时消费栈顶并返回 false。
  pub fn set_global(&mut self, name: &[u8]) -> bool {
    let Some(name) = cstr(name) else {
      self.pop(1);
      return false;
    };
    self.enter_infallible_allocation_region();
    // SAFETY：栈顶值转移至全局表；name 存活至调用返回。
    unsafe { sys::lua_setfield(self.l, sys::LUA_GLOBALSINDEX, name.as_ptr()) };
    self.try_exit_infallible_allocation_region()
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TryRegister
  ///
  /// 注册宿主函数为全局：C 蹦床 + 域函数指针上值（panic 兜底见
  /// `context::host_trampoline`）。
  pub fn register_host_fn<C: 'static>(
    &mut self,
    name: &[u8],
    function: fn(&mut LuaState, &mut C) -> i32,
  ) -> bool {
    let Some(name) = cstr(name) else {
      return false;
    };
    self.enter_infallible_allocation_region();
    // SAFETY：函数指针经 lightuserdata 上值锚定（GC 不回收 lightuserdata）；
    // debugname 由 name 持有至 pushcclosurek 返回。
    unsafe {
      sys::lua_pushlightuserdatatagged(self.l, function as *mut c_void, 0);
      sys::lua_pushcclosurek(
        self.l,
        context::host_trampoline::<C>,
        name.as_ptr(),
        1,
        None,
      );
    }
    self.set_global(name.as_bytes());
    self.try_exit_infallible_allocation_region()
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PushCFunction
  ///
  /// 将原生 C 函数压栈（1:1 对标 C# NativeMethods.PushCFunction）。
  pub fn push_cfunction(&mut self, function: sys::LuaCFunction) {
    // SAFETY：无闭包上值压入原生 C 函数
    unsafe {
      sys::lua_pushcclosurek(self.l, function, ptr::null(), 0, None);
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:LoadBuffer
  ///
  /// 编译缓冲为函数并压栈；编译/装载失败弹出错误串并返回 Err。
  pub fn load_buffer(&mut self, buffer: &[u8], chunk_name: &str) -> Result<()> {
    let Ok(chunk_name) = CString::new(chunk_name) else {
      return Err(Error::Misuse("chunk name contains NUL"));
    };
    let mut options = compile_options();
    let mut size = 0usize;
    // SAFETY：buffer/编译选项在调用期间存活；产物以 free 归还。
    let bytecode = unsafe {
      sys::luau_compile(
        buffer.as_ptr().cast(),
        buffer.len(),
        &mut options,
        &mut size,
      )
    };
    if bytecode.is_null() {
      // 编译器内存耗尽：无产物可装载。
      return Err(runtime("compilation failed"));
    }
    // 编译失败时产物为编码错误，luau_load 装载失败并压错误串（单一出口）。
    // SAFETY：bytecode/size 为刚产出且有效的编译产物；env = 0 默认全局表。
    let status = unsafe {
      let st = sys::luau_load(self.l, chunk_name.as_ptr(), bytecode, size, 0);
      sys::free(bytecode.cast());
      st
    };
    if status == sys::LUA_OK {
      return Ok(());
    }
    let message = self.error_object_text();
    self.pop(1);
    Err(runtime(message))
  }

  /// libs/server/Lua/LuaStateWrapper.cs:LoadString
  pub fn load_string(&mut self, source: &str) -> Result<()> {
    self.load_buffer(source.as_bytes(), "=load_string")
  }

  /// 数值 → 字符串（栈顶槽位就地转换，Luau 数值文本化语义快捷方法）。
  pub fn try_number_to_string(&mut self) -> bool {
    self.try_number_to_string_at(-1)
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TryNumberToString
  ///
  /// 指定栈槽位数值 → 字符串（1:1 对标 C# `TryNumberToString(int stackIndex, out ReadOnlySpan<byte> str)`）。
  ///
  /// redis.call/log 等多参回调中目标参数未必在栈顶，故提供槽位形态。
  pub fn try_number_to_string_at(&mut self, idx: i32) -> bool {
    if self.type_name(idx) != Some("number") {
      return false;
    }
    // lua_replace 语义 = "以栈顶覆写 idx 并弹栈"，对相对下标 -1 自指即弹栈；
    // 故先折算绝对下标（1 基），保证 push+replace 对任意槽位成立。
    // 相对下标折算：abs = top + idx + 1（-1 = 栈顶）。
    let abs = if idx > 0 {
      Some(idx)
    } else {
      (self.get_top() as i32)
        .checked_add(idx)
        .and_then(|v| v.checked_add(1))
        .filter(|v| *v > 0)
    };
    let Some(abs) = abs else {
      return false;
    };
    self.enter_infallible_allocation_region();
    // SAFETY：数值分支（已校验类型）不触发 __tostring 元方法，不抛错；
    // 产物串压于栈顶。
    unsafe { sys::luaL_tolstring(self.l, idx as c_int, ptr::null_mut()) };
    // SAFETY：abs 有效（类型判定刚命中），栈顶串覆写 abs 槽并弹栈。
    unsafe { sys::lua_replace(self.l, abs as c_int) };
    self.try_exit_infallible_allocation_region()
  }

  /// libs/server/Lua/LuaStateWrapper.cs:KnownStringToBuffer
  ///
  /// 取 `idx` 字符串到缓冲（非串返回 None）。
  pub fn known_string_to_buffer(&self, idx: i32) -> Option<Vec<u8>> {
    if self.type_name(idx) != Some("string") {
      return None;
    }
    let mut len = 0usize;
    // SAFETY：串值在栈上（GC 锚定），指针仅在本函数内读取。
    let text = unsafe { sys::lua_tolstring(self.l, idx as c_int, &mut len) };
    if text.is_null() {
      return None;
    }
    // SAFETY：text/len 指向栈上 Lua 串，拷贝在出栈前完成。
    let bytes = unsafe { from_raw_parts(text.cast::<u8>(), len) };
    Some(bytes.to_vec())
  }

  /// libs/server/Lua/LuaStateWrapper.cs:CheckNumber
  ///
  /// 数值/可转换串 → f64（lua_tonumberx 语义；不可转换返回 None）。
  pub fn check_number(&self, idx: i32) -> Option<f64> {
    let mut isnum = 0;
    // SAFETY：仅读 idx 槽；布尔/nil 等返回 isnum = 0。
    let value = unsafe { sys::lua_tonumberx(self.l, idx as c_int, &mut isnum) };
    (isnum != 0).then_some(value)
  }

  /// libs/server/Lua/LuaStateWrapper.cs:ToBoolean
  pub fn to_boolean(&self, idx: i32) -> bool {
    // SAFETY：仅读 idx 槽。
    unsafe { sys::lua_toboolean(self.l, idx as c_int) != 0 }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:RawLen
  ///
  /// 字符串/表长度（其余类型 0）。
  pub fn raw_len(&self, idx: i32) -> i64 {
    // SAFETY：objlen 对非串/表返回 0，无越界。
    unsafe { sys::lua_objlen(self.l, idx as c_int) as i64 }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:Next
  ///
  /// 表迭代（lua_next 语义）：弹出栈顶键，压入下一键值对并返回 true；
  /// 迭代结束仅消耗键、不压任何值（净 -1），返回 false。
  /// （命名避开 `Iterator::next` 的 trait 同名，避免误导为可迭代对象。）
  pub fn lua_next(&mut self) -> bool {
    // SAFETY：栈顶为键、其下为表（C API 遍历约定）；nil 键即起始。
    unsafe { sys::lua_next(self.l, -2) != 0 }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PushValue
  ///
  /// 复制 `idx` 处元素压栈。
  pub fn push_value(&mut self, idx: i32) {
    // SAFETY：idx 有效（调用方约束）；复制值随新槽锚定。
    unsafe { sys::lua_pushvalue(self.l, idx as c_int) };
  }

  /// libs/server/Lua/LuaStateWrapper.cs:Rotate
  ///
  /// `idx`..栈顶 区间旋转 `n` 位（正 n 把栈顶元素滚向 `idx`）。
  /// Luau 无 lua_rotate：正向以 n 次 lua_insert、反向以
  /// pushvalue+remove 等价承接（区间长度先行取模，杜绝超长循环）。
  pub fn rotate(&mut self, idx: i32, n: i32) {
    let base = if idx > 0 {
      idx as usize - 1
    } else {
      self.get_top().saturating_sub((-idx) as usize)
    };
    if base >= self.get_top() {
      return;
    }
    let len = (self.get_top() - base) as i32;
    let n = if len <= 1 { 0 } else { n % len };
    if n == 0 {
      return;
    }
    for _ in 0..n.abs() {
      if n > 0 {
        // 正向：栈顶元素滚到 idx。
        // SAFETY：base+1 在 1..=top 区间内。
        unsafe { sys::lua_insert(self.l, base as c_int + 1) };
      } else {
        // 反向：idx 处元素滚到栈顶。
        // SAFETY：同上；复制后移除原槽，净效果为单元素搬家。
        unsafe {
          sys::lua_pushvalue(self.l, base as c_int + 1);
          sys::lua_remove(self.l, base as c_int + 1);
        }
      }
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TrySetHook / 超时
  ///
  /// `deadline` 为 Some 时经 VM safepoint 中断回调（循环回边/调用返回/GC）
  /// 轮询截止，超限即抛出超时错误（可被脚本 pcall）；None 撤销生效截止。
  pub fn try_set_hook(&mut self, deadline_monotonic_millis: Option<i64>) {
    match (&mut self.deadline, deadline_monotonic_millis) {
      (Some(slot), d) => slot.set(d),
      (None, Some(d)) => {
        let slot = Box::new(Deadline::new(Some(d)));
        // SAFETY：callbacks 指向 VM 全局回调集（VM 存活期内有效）；
        // userdata 为 Luau 承诺不覆写的宿主槽，指向随 self 存活的截止槽。
        unsafe {
          let callbacks = sys::lua_callbacks(self.l);
          (*callbacks).userdata = (&raw const *slot).cast_mut().cast();
          (*callbacks).interrupt = Some(interrupt_trampoline);
        }
        self.deadline = Some(slot);
      }
      (None, None) => {}
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:ClearStack
  pub fn clear_stack(&mut self) {
    // SAFETY：settop(0) 清空回调帧/宿主帧的栈上槽。
    unsafe { sys::lua_settop(self.l, 0) };
  }

  /// libs/server/Lua/LuaStateWrapper.cs:UpdateStackTop
  ///
  /// 显式设置栈高（lua_settop 正语义：截断；不足补 nil）。
  pub fn update_stack_top(&mut self, new_top: usize) {
    // SAFETY：扩高槽由 VM 以 nil 填充（内建 checkstack 语义）。
    unsafe { sys::lua_settop(self.l, new_top as c_int) };
  }

  /// 栈高。
  pub fn get_top(&self) -> usize {
    // SAFETY：仅读栈高。
    unsafe { sys::lua_gettop(self.l) as usize }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:EnterInfallibleAllocationRegion
  pub fn enter_infallible_allocation_region(&mut self) {
    if let Some(slot) = &self.allocator {
      // SAFETY：槽借用仅限本函数帧，不与 VM 分配回调交叠。
      unsafe { ILuaAllocator::enter_infallible_allocation_region(&mut *slot.get()) };
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TryExitInfallibleAllocationRegion
  pub fn try_exit_infallible_allocation_region(&mut self) -> bool {
    let Some(slot) = &self.allocator else {
      return true;
    };
    // SAFETY：同上。
    let success = unsafe { ILuaAllocator::try_exit_infallible_allocation_region(&mut *slot.get()) };
    if !success {
      self.needs_dispose = true;
    }
    success
  }

  /// libs/server/Lua/LuaStateWrapper.cs:NeedsDispose
  pub fn needs_dispose(&self) -> bool {
    self.needs_dispose
  }

  /// VM 内存用量（当前内存目录 0；自定义分配器下含宿主簿记视图）。
  pub fn used_memory(&self) -> usize {
    // SAFETY：仅读 VM 统计。
    unsafe { sys::lua_totalbytes(self.l, 0) }
  }
}

/// 中断回调：safepoint 触发，超截止即抛超时错误（可被 pcall 捕获）。
/// 满足 Luau 中断回调 C 签名规范，保留 _gc 参数
///
/// # Safety（回调契约）
/// - userdata 指向随 LuaState 存活的截止槽（try_set_hook 注册）。
/// - lua_error 长跳转：本帧内除 POD 外无存活析构对象。
/// - `_count`: 满足 Luau C ABI 中断钩子签名规范，超时由截止时间槽判断，无需消费此形参。
unsafe extern "C" fn interrupt_trampoline(l: *mut sys::lua_State, _count: c_int) {
  // SAFETY：callbacks 指针有效；userdata 由 try_set_hook 置入。
  let slot: *const Deadline = unsafe {
    let callbacks = sys::lua_callbacks(l);
    (*callbacks).userdata.cast()
  };
  if slot.is_null() {
    return;
  }
  // SAFETY：slot 随 LuaState 存活，仅本线程读写（中断同线程触发）。
  let expired = unsafe { (*slot).get() }.is_some_and(|deadline| now_monotonic_millis() >= deadline);
  if expired {
    // SAFETY：压串后长跳转至宿主 lua_pcall；跳越帧无析构对象。
    unsafe {
      sys::lua_pushlstring(l, TIMEOUT_ERROR.as_ptr().cast(), TIMEOUT_ERROR.len());
      sys::lua_error(l);
    }
  }
}

/// 编译选项（对齐 Luau 默认：基线优化 + 行号级调试信息）。
fn compile_options() -> sys::lua_CompileOptions {
  // SAFETY：全字段为 c_int / 指针，零值合法（未启用回调与扩展表）。
  let mut options: sys::lua_CompileOptions = unsafe { mem::zeroed() };
  options.optimization_level = 1;
  options.debug_level = 1;
  options
}

/// 字节名 → NUL 结尾 C 串（含 NUL 的名字非法）。
fn cstr(bytes: &[u8]) -> Option<CString> {
  CString::new(bytes).ok()
}
