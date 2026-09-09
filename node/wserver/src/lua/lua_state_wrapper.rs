//! Lua 状态机封装：以 mlua（luau）承接 C API 栈语义
//! （对标 libs/server/Lua/LuaStateWrapper.cs:LuaStateWrapper）。
//!
//! C# 直接操作 Lua C 栈；mlua 为高层 API，故在封装内维护显式
//! `Vec<Value>` 栈镜像：push/pop/rotate/settop/next 等映射到栈镜像，
//! 表与全局操作落到 mlua，编译/执行经 `load`/`pcall`。
//!
//! 栈镜像与注册表引用存于 VM 的 app data（[`LuaInterp`]）：宿主回调
//! （redis.call 等，由 mlua 闭包承接）只能拿到 `&Lua`，经
//! `app_data_mut` 取同一份栈镜像，避免与外层 `&mut` 借用重叠。

use std::str;

use gxhash::HashMap;
use mlua::{Lua, MultiValue, Value, Variadic};

use super::i_lua_allocator::ILuaAllocator;

/// 栈元素。
pub type StackValue = Value;

/// 解释器可变内态：C API 栈镜像 + 注册表引用（存为 VM app data）。
#[derive(Default)]
pub struct LuaInterp {
  /// C API 栈镜像（栈顶 = 末尾）。
  pub stack: Vec<StackValue>,
  /// 注册表引用：id → RegistryKey。
  pub refs: HashMap<i32, mlua::RegistryKey>,
  /// 引用 id 分配器。
  pub next_ref_id: i32,
  /// 超时截止（单调毫秒；None = 未设）。经 VM 中断钩子轮询。
  pub deadline_monotonic_millis: Option<i64>,
}

/// Lua 状态封装：VM + 栈镜像（镜像本体在 app data）。
pub struct LuaStateWrapper {
  /// luau VM（内持 LuaInterp app data）。
  lua: Lua,
  /// 分配器（内存语义钩子；mlua 侧以 memory_limit 承载）。
  allocator: Option<Box<dyn ILuaAllocator>>,
}

impl Default for LuaStateWrapper {
  fn default() -> Self {
    Self::new()
  }
}

impl LuaStateWrapper {
  /// 构造：新建 luau VM 并装载安全基库。
  pub fn new() -> Self {
    let lua = Lua::new();
    lua.set_app_data(LuaInterp::default());
    Self {
      lua,
      allocator: None,
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:LuaStateWrapper（视图构造）
  ///
  /// 以既有 VM 建临时视图（宿主回调侧入口）：app data 共享同一份栈镜像。
  pub fn view(lua: &Lua) -> Self {
    if lua.app_data_ref::<LuaInterp>().is_none() {
      lua.set_app_data(LuaInterp::default());
    }
    Self {
      lua: lua.clone(),
      allocator: None,
    }
  }

  /// VM 引用。
  pub fn lua(&self) -> &Lua {
    &self.lua
  }

  /// 内态访问。
  pub fn interp(&self) -> mlua::AppDataRef<'_, LuaInterp> {
    self
      .lua
      .app_data_ref::<LuaInterp>()
      .expect("app data 已在构造时装载")
  }

  /// 内态可变访问。
  pub fn interp_mut(&self) -> mlua::AppDataRefMut<'_, LuaInterp> {
    self
      .lua
      .app_data_mut::<LuaInterp>()
      .expect("app data 已在构造时装载")
  }

  /// libs/server/Lua/LuaStateWrapper.cs:ExpectLuaStackEmpty
  ///
  /// 断言栈已空（返回是否为空，测试/调试用）。
  pub fn expect_lua_stack_empty(&self) -> bool {
    self.interp().stack.is_empty()
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TryEnsureMinimumStackCapacity
  pub fn try_ensure_minimum_stack_capacity(&mut self, min_capacity: usize) -> bool {
    self.interp_mut().stack.reserve(min_capacity);
    true
  }

  /// libs/server/Lua/LuaStateWrapper.cs:CallFromLuaEntered
  ///
  /// 以受管状态调用已装载的函数（参数来自栈顶 `nargs` 个，回填结果）。
  pub fn call_from_lua_entered(&mut self, nargs: usize) -> Result<(), mlua::Error> {
    self.known_call_from_lua_entered(nargs)
  }

  /// libs/server/Lua/LuaStateWrapper.cs:KnownCallFromLuaEntered
  ///
  /// 已知可调用形态的执行路径：弹出函数与参数，压回全部结果。
  pub fn known_call_from_lua_entered(&mut self, nargs: usize) -> Result<(), mlua::Error> {
    let total = nargs + 1;
    let args: Vec<StackValue> = {
      let mut interp = self.interp_mut();
      args_from_stack(&mut interp.stack, total)
    };
    let Some((first, rest)) = args.split_first() else {
      return Err(mlua::Error::RuntimeError(
        "attempt to call a non-function object".into(),
      ));
    };
    let function = as_function(first)?;
    let results: MultiValue = function.call(Variadic::from_iter(rest.iter().cloned()))?;
    let mut interp = self.interp_mut();
    interp.stack.extend(results.into_vec());
    Ok(())
  }

  /// libs/server/Lua/LuaStateWrapper.cs:Type
  ///
  /// 栈顶第 `idx`（-1 为栈顶）元素的类型名。
  pub fn type_name(&self, idx: i32) -> Option<&'static str> {
    self.peek(idx).as_ref().map(value_type_name)
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TryPushBuffer
  ///
  /// 压入字节缓冲为字符串。
  pub fn try_push_buffer(&mut self, buffer: &[u8]) -> bool {
    let Ok(s) = self.lua.create_string(buffer) else {
      return false;
    };
    self.interp_mut().stack.push(Value::String(s));
    true
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PushNil
  pub fn push_nil(&mut self) {
    self.interp_mut().stack.push(Value::Nil);
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PushNumber
  pub fn push_number(&mut self, number: f64) {
    self.interp_mut().stack.push(Value::Number(number));
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PushInteger
  pub fn push_integer(&mut self, integer: i64) {
    self.interp_mut().stack.push(Value::Integer(integer));
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PushBoolean
  pub fn push_boolean(&mut self, boolean: bool) {
    self.interp_mut().stack.push(Value::Boolean(boolean));
  }

  /// libs/server/Lua/LuaStateWrapper.cs:Pop
  ///
  /// 弹出 `count` 个栈元素。
  pub fn pop(&mut self, count: usize) {
    let mut interp = self.interp_mut();
    let keep = interp.stack.len().saturating_sub(count);
    interp.stack.truncate(keep);
  }

  /// libs/server/Lua/LuaStateWrapper.cs:Remove
  ///
  /// 移除 `idx` 处元素，其上元素整体下移。
  pub fn remove(&mut self, idx: i32) {
    let Some(abs) = self.abs_index(idx) else {
      return;
    };
    self.interp_mut().stack.remove(abs);
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PCall
  ///
  /// 保护模式调用栈顶 `nargs` 参的函数：成功压入全部结果，失败压入错误串。
  pub fn pcall(&mut self, nargs: usize) -> Result<(), mlua::Error> {
    self.pcall_n(nargs, usize::MAX).map(|_| ())
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PCall（nresults 形态）
  ///
  /// 成功时结果按 `nresults` 截断/补 nil（`usize::MAX` = 全保留）；
  /// 失败时压入错误串并返回 Err（状态非 OK）。
  pub fn pcall_n(&mut self, nargs: usize, nresults: usize) -> Result<usize, mlua::Error> {
    let total = nargs + 1;
    let args: Vec<StackValue> = {
      let mut interp = self.interp_mut();
      args_from_stack(&mut interp.stack, total)
    };
    let Some((first, rest)) = args.split_first() else {
      return Err(mlua::Error::RuntimeError(
        "attempt to call a non-function object".into(),
      ));
    };
    let Ok(function) = as_function(first) else {
      return Err(mlua::Error::RuntimeError(
        "attempt to call a non-function object".into(),
      ));
    };
    let called: Result<MultiValue, mlua::Error> =
      function.call(Variadic::from_iter(rest.iter().cloned()));
    let mut interp = self.interp_mut();
    match called {
      Ok(results) => {
        let mut values = results.into_vec();
        if nresults != usize::MAX {
          // lua_pcall 语义：不足补 nil，超出仅保留前 nresults 个。
          values.truncate(nresults);
          values.resize(nresults, Value::Nil);
        }
        let count = values.len();
        interp.stack.extend(values);
        Ok(count)
      }
      Err(error) => {
        let message = error_message(&error);
        let Ok(s) = self.lua.create_string(message) else {
          return Err(error);
        };
        interp.stack.push(Value::String(s));
        Err(error)
      }
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:RawSetInteger
  ///
  /// 表[idx] = value（表位于 `table_idx`，整型键）。
  pub fn raw_set_integer(&mut self, table_idx: i32, key: i64, value: StackValue) -> bool {
    let Some(Value::Table(table)) = self.peek(table_idx) else {
      return false;
    };
    table.raw_set(key, value).is_ok()
  }

  /// libs/server/Lua/LuaStateWrapper.cs:RawSet
  ///
  /// 表[key] = value（键/值取自栈顶两元素并弹出）。
  pub fn raw_set(&mut self, table_idx: i32) -> bool {
    // lua_settable 语义：表下标在弹键值之前解析；app data 守卫不可重入，逐次弹出。
    let Some(Value::Table(table)) = self.peek(table_idx) else {
      return false;
    };
    let Some(value) = self.interp_mut().stack.pop() else {
      return false;
    };
    let Some(key) = self.interp_mut().stack.pop() else {
      return false;
    };
    table.raw_set(key, value).is_ok()
  }

  /// libs/server/Lua/LuaStateWrapper.cs:RawGetInteger
  pub fn raw_get_integer(&mut self, table_idx: i32, key: i64) -> bool {
    let Some(Value::Table(table)) = self.peek(table_idx) else {
      return false;
    };
    match table.raw_get(key) {
      Ok(value) => {
        self.interp_mut().stack.push(value);
        true
      }
      Err(_) => false,
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:RawGet（键取自栈顶形态）
  ///
  /// 以栈顶元素为键查 `table_idx` 表：弹出键，压入查得值
  /// （lua_rawget 语义，键被结果原位替换）。
  pub fn raw_get_top(&mut self, table_idx: i32) -> bool {
    // lua_rawget 语义：表下标在弹键之前解析。
    let Some(Value::Table(table)) = self.peek(table_idx) else {
      return false;
    };
    let Some(key) = self.interp_mut().stack.pop() else {
      return false;
    };
    match table.raw_get(key) {
      Ok(value) => {
        self.interp_mut().stack.push(value);
        true
      }
      Err(_) => false,
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TryRef
  ///
  /// 引用栈顶元素到注册表，返回引用 id。
  pub fn try_ref(&mut self) -> Option<i32> {
    let value = self.interp_mut().stack.pop()?;
    let key = self.lua.create_registry_value(value).ok()?;
    let id = {
      let mut interp = self.interp_mut();
      interp.next_ref_id += 1;
      interp.next_ref_id
    };
    self.interp_mut().refs.insert(id, key);
    Some(id)
  }

  /// libs/server/Lua/LuaStateWrapper.cs:Unref
  pub fn unref(&mut self, ref_id: i32) {
    if let Some(key) = self.interp_mut().refs.remove(&ref_id) {
      self.lua.remove_registry_value(key).ok();
    }
  }

  /// 引用取值（Runner 的 script lookup 路径）。
  pub fn ref_value(&self, ref_id: i32) -> Option<StackValue> {
    let interp = self.interp();
    let key = interp.refs.get(&ref_id)?;
    self.lua.registry_value::<Value>(key).ok()
  }

  /// 引用压栈（对标 C# RawGetInteger(LuaRegistry.Index, id) 伪索引形态）。
  pub fn push_ref(&mut self, ref_id: i32) -> bool {
    match self.ref_value(ref_id) {
      Some(value) => {
        self.interp_mut().stack.push(value);
        true
      }
      None => false,
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TryCreateTable
  ///
  /// 压入新表（`narr`/`nrec` 仅容量提示）。
  pub fn try_create_table(&mut self, narr: usize, nrec: usize) -> bool {
    match self.lua.create_table_with_capacity(narr, nrec) {
      Ok(table) => {
        self.interp_mut().stack.push(Value::Table(table));
        true
      }
      Err(_) => false,
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:GetGlobal
  pub fn get_global(&mut self, name: &[u8]) -> bool {
    let Ok(name) = str::from_utf8(name) else {
      return false;
    };
    match self.lua.globals().get(name) {
      Ok(value) => {
        self.interp_mut().stack.push(value);
        true
      }
      Err(_) => false,
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TrySetGlobal
  pub fn try_set_global(&mut self, name: &[u8]) -> bool {
    let (Some(value), Ok(name)) = (self.interp_mut().stack.pop(), str::from_utf8(name)) else {
      return false;
    };
    self.lua.globals().set(name, value).is_ok()
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TryRegister
  ///
  /// 注册宿主函数为全局（对标 C# TryRegister(name, fn ptr)）。
  pub fn register_function<F, A, R>(&mut self, name: &[u8], function: F) -> bool
  where
    F: Fn(&Lua, A) -> Result<R, mlua::Error> + 'static,
    A: mlua::FromLuaMulti,
    R: mlua::IntoLuaMulti,
  {
    let Ok(name) = str::from_utf8(name) else {
      return false;
    };
    let Ok(f) = self.lua.create_function(function) else {
      return false;
    };
    self.lua.globals().set(name, f).is_ok()
  }

  /// libs/server/Lua/LuaStateWrapper.cs:LoadBuffer
  ///
  /// 编译缓冲中的代码块为函数并压栈；编译失败返回 Err。
  pub fn load_buffer(&mut self, buffer: &[u8], chunk_name: &str) -> Result<(), mlua::Error> {
    let Ok(source) = str::from_utf8(buffer) else {
      return Err(mlua::Error::RuntimeError("non-utf8 chunk".into()));
    };
    let chunk = self.lua.load(source).set_name(chunk_name);
    let function = chunk.into_function()?;
    self.interp_mut().stack.push(Value::Function(function));
    Ok(())
  }

  /// libs/server/Lua/LuaStateWrapper.cs:LoadString
  pub fn load_string(&mut self, source: &str) -> Result<(), mlua::Error> {
    let function = self.lua.load(source).into_function()?;
    self.interp_mut().stack.push(Value::Function(function));
    Ok(())
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TryNumberToString
  ///
  /// 数值 → 字符串（栈顶槽位就地转换，luau 语义：%v 格式）。
  pub fn try_number_to_string(&mut self) -> bool {
    self.try_number_to_string_at(-1)
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TryNumberToString（指定槽位形态）
  ///
  /// C# 以 `TryNumberToString(stackIndex, out str)` 在参数原位转换；
  /// redis.call/log 等多参回调中目标参数未必在栈顶，故提供槽位形态。
  pub fn try_number_to_string_at(&mut self, idx: i32) -> bool {
    let number = match self.peek(idx) {
      Some(Value::Number(n)) => n,
      Some(Value::Integer(i)) => i as f64,
      _ => return false,
    };
    let Ok(s) = self.lua.create_string(format_number_text(number)) else {
      return false;
    };
    let Some(abs) = self.abs_index(idx) else {
      return false;
    };
    let mut interp = self.interp_mut();
    // abs 已校验且 peek 刚命中，槽位必然存在。
    interp.stack[abs] = Value::String(s);
    true
  }

  /// libs/server/Lua/LuaStateWrapper.cs:KnownStringToBuffer
  ///
  /// 取栈顶字符串到缓冲。
  pub fn known_string_to_buffer(&self, idx: i32) -> Option<Vec<u8>> {
    match self.peek(idx)? {
      Value::String(s) => Some(s.as_bytes().to_vec()),
      _ => None,
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:CheckNumber
  pub fn check_number(&self, idx: i32) -> Option<f64> {
    match self.peek(idx)? {
      Value::Number(n) => Some(n),
      Value::Integer(i) => Some(i as f64),
      Value::String(s) => str::from_utf8(&s.as_bytes()).ok()?.parse().ok(),
      _ => None,
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:ToBoolean
  pub fn to_boolean(&self, idx: i32) -> bool {
    !matches!(
      self.peek(idx),
      None | Some(Value::Nil) | Some(Value::Boolean(false))
    )
  }

  /// libs/server/Lua/LuaStateWrapper.cs:RawLen
  pub fn raw_len(&self, idx: i32) -> i64 {
    match self.peek(idx) {
      Some(Value::Table(t)) => t.raw_len() as i64,
      Some(Value::String(s)) => s.as_bytes().len() as i64,
      _ => 0,
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PushCFunction / TryRegister
  ///
  /// mlua 侧函数压栈。
  pub fn push_c_function(&mut self, function: mlua::Function) {
    self.interp_mut().stack.push(Value::Function(function));
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PushConstantString
  pub fn push_constant_string(&mut self, constant: &[u8]) -> bool {
    self.try_push_buffer(constant)
  }

  /// libs/server/Lua/LuaStateWrapper.cs:Next
  ///
  /// 表迭代（lua_next 语义）：弹出栈顶键，压入下一键值对并返回 true；
  /// 迭代结束仅消耗键、不压任何值（净 -1），返回 false。
  #[allow(clippy::should_implement_trait)]
  pub fn next(&mut self) -> bool {
    let key = self.interp_mut().stack.pop();
    let Some(key) = key else {
      return false;
    };
    let Some(Value::Table(table)) = self.peek(-1) else {
      self.interp_mut().stack.push(key);
      return false;
    };
    // mlua 无 C 栈式 next：以 pairs 快照承接——收集键序，推进到当前键
    // 的下一键值对（nil 键 = 起始）。回复表规模小，快照开销可忽略。
    let pairs: Vec<(StackValue, StackValue)> = table
      .clone()
      .pairs::<StackValue, StackValue>()
      .filter_map(Result::ok)
      .collect();
    let start = if matches!(key, Value::Nil) {
      0
    } else {
      pairs
        .iter()
        .position(|(k, _)| *k == key)
        .map_or(pairs.len(), |pos| pos + 1)
    };
    match pairs.get(start) {
      Some((next_key, value)) => {
        let mut interp = self.interp_mut();
        interp.stack.push(next_key.clone());
        interp.stack.push(value.clone());
        true
      }
      None => false,
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PushValue
  ///
  /// 复制 `idx` 处元素压栈。
  pub fn push_value(&mut self, idx: i32) {
    if let Some(value) = self.peek(idx) {
      self.interp_mut().stack.push(value);
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:Rotate
  ///
  /// `idx`..栈顶 区间旋转 `n` 位（lua_rotate 语义：正 n 把栈顶 n 个元素
  /// 滚动到 `idx` 处，即 slice rotate_right(n)）。
  pub fn rotate(&mut self, idx: i32, n: i32) {
    let start = self.abs_index(idx);
    let Some(start) = start else { return };
    let mut interp = self.interp_mut();
    if n == 0 || interp.stack.len() <= start {
      return;
    }
    let len = interp.stack.len() - start;
    let n = ((n % len as i32) + len as i32) as usize % len;
    if n == 0 {
      return;
    }
    interp.stack[start..].rotate_right(n);
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TrySetHook / 超时
  ///
  /// luau 以 VM 中断钩子（`Lua::set_interrupt`）承接 C# 调试钩子：
  /// `deadline` 为 Some 时安装轮询，超限即抛出超时错误（可被脚本 pcall）。
  pub fn try_set_hook(&mut self, deadline_monotonic_millis: Option<i64>) {
    self.interp_mut().deadline_monotonic_millis = deadline_monotonic_millis;
    self.lua.set_interrupt(move |lua: &Lua| {
      let expired = lua
        .app_data_ref::<LuaInterp>()
        .and_then(|interp| interp.deadline_monotonic_millis)
        .is_some_and(|deadline| now_monotonic_millis() >= deadline);
      if expired {
        Err(mlua::Error::RuntimeError(
          "ERR Lua script exceeded configured timeout".into(),
        ))
      } else {
        Ok(mlua::VmState::Continue)
      }
    });
  }

  /// 当前时限。
  pub fn deadline(&self) -> Option<i64> {
    self.interp().deadline_monotonic_millis
  }

  /// libs/server/Lua/LuaStateWrapper.cs:AssertLuaStackIndexInBounds
  ///
  /// 下标是否落在栈内。
  pub fn assert_lua_stack_index_in_bounds(&self, idx: i32) -> bool {
    self.abs_index(idx).is_some()
  }

  /// libs/server/Lua/LuaStateWrapper.cs:AssertLuaStackExpected
  ///
  /// 栈顶元素类型是否符合期望。
  pub fn assert_lua_stack_expected(&self, idx: i32, expected: &str) -> bool {
    self.type_name(idx) == Some(expected)
  }

  /// libs/server/Lua/LuaStateWrapper.cs:AssertLuaStackNotFull
  pub fn assert_lua_stack_not_full(&self) -> bool {
    self.interp().stack.len() < i32::MAX as usize
  }

  /// libs/server/Lua/LuaStateWrapper.cs:AssertLuaStackNotEmpty
  pub fn assert_lua_stack_not_empty(&self) -> bool {
    !self.interp().stack.is_empty()
  }

  /// libs/server/Lua/LuaStateWrapper.cs:LuaAtPanic
  ///
  /// luau 无 panic 路径（内存越界以错误值回报），钩子恒为空操作。
  pub fn lua_at_panic(&mut self) -> i32 {
    0
  }

  /// libs/server/Lua/LuaStateWrapper.cs:LuaAllocateBytes
  ///
  /// VM 内存用量（luau 内存统计）。
  pub fn lua_allocate_bytes(&self) -> usize {
    self.lua.used_memory()
  }

  /// libs/server/Lua/LuaStateWrapper.cs:ClearStack
  pub fn clear_stack(&mut self) {
    self.interp_mut().stack.clear();
  }

  /// libs/server/Lua/LuaStateWrapper.cs:UpdateStackTop
  ///
  /// 显式设置栈高（C# lua_settop 的正语义：截断；不足补 nil）。
  pub fn update_stack_top(&mut self, new_top: usize) {
    self.interp_mut().stack.resize(new_top, Value::Nil);
  }

  /// 栈高。
  pub fn get_top(&self) -> usize {
    self.interp().stack.len()
  }

  /// libs/server/Lua/LuaStateWrapper.cs:EnterInfallibleAllocationRegion
  pub fn enter_infallible_allocation_region(&mut self) {
    if let Some(allocator) = &mut self.allocator {
      allocator.enter_infallible_allocation_region();
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TryExitInfallibleAllocationRegion
  pub fn try_exit_infallible_allocation_region(&mut self) -> bool {
    self
      .allocator
      .as_mut()
      .is_none_or(|a| a.try_exit_infallible_allocation_region())
  }

  /// 附加分配器（内存语义钩子）。
  pub fn set_allocator(&mut self, allocator: Box<dyn ILuaAllocator>) {
    self.allocator = Some(allocator);
  }

  /// 相对下标 → 绝对下标（-1 = 栈顶；1 = 栈底）。
  fn abs_index(&self, idx: i32) -> Option<usize> {
    if idx > 0 {
      usize::try_from(idx).ok().map(|i| i - 1)
    } else {
      usize::try_from(-idx)
        .ok()
        .and_then(|i| self.interp().stack.len().checked_sub(i))
    }
  }

  /// 栈值视图（mlua 句柄为引用形态，克隆仅引用计数 +1）。
  fn peek(&self, idx: i32) -> Option<StackValue> {
    let i = self.abs_index(idx)?;
    self.interp().stack.get(i).cloned()
  }
}

/// 从栈顶取 `total` 个元素（顺序保持）。
fn args_from_stack(stack: &mut Vec<StackValue>, total: usize) -> Vec<StackValue> {
  stack.split_off(stack.len().saturating_sub(total))
}

/// mlua 错误 → C# 语义的原始 Lua 错误串（剥离 mlua 前缀）。
pub fn error_message(error: &mlua::Error) -> String {
  match error {
    mlua::Error::RuntimeError(msg) => msg.clone(),
    other => {
      let text = other.to_string();
      text
        .strip_prefix("runtime error: ")
        .map_or_else(|| text.clone(), str::to_owned)
    }
  }
}

/// 当前单调毫秒（coarsetime）。
pub fn now_monotonic_millis() -> i64 {
  coarsetime::Clock::now_since_epoch().as_millis() as i64
}

/// 数值 → 文本（luaL_tolstring 的整值直写形态）。
fn format_number_text(number: f64) -> String {
  if number == number.trunc() && number.abs() < 1e15 {
    format!("{}", number as i64)
  } else {
    format!("{number}")
  }
}

/// 栈值 → mlua 函数。
fn as_function(value: &StackValue) -> Result<mlua::Function, mlua::Error> {
  match value {
    Value::Function(function) => Ok(function.clone()),
    other => Err(mlua::Error::RuntimeError(format!(
      "attempt to call a {} value",
      value_type_name(other)
    ))),
  }
}

/// 值类型名（对齐 lua_type 的名字面）。
pub fn value_type_name(value: &StackValue) -> &'static str {
  match value {
    Value::Nil => "nil",
    Value::Boolean(_) => "boolean",
    Value::Integer(_) | Value::Number(_) => "number",
    Value::String(_) => "string",
    Value::Table(_) => "table",
    Value::Function(_) => "function",
    Value::LightUserData(_) | Value::UserData(_) => "userdata",
    Value::Thread(_) => "thread",
    _ => "userdata",
  }
}

#[cfg(test)]
mod tests {
  use super::{LuaStateWrapper, Value};

  #[test]
  fn push_pop_and_types() {
    let mut state = LuaStateWrapper::new();
    state.push_integer(7);
    state.push_number(3.5);
    state.push_boolean(true);
    state.push_nil();
    state.try_push_buffer(b"hello");
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
    let mut state = LuaStateWrapper::new();
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
    // luau 的错误串可能带位置前缀，仅校验包含错误消息。
    assert!(String::from_utf8_lossy(&state.known_string_to_buffer(-1).unwrap()).contains("boom"));
  }

  #[test]
  fn table_ops_and_globals() {
    let mut state = LuaStateWrapper::new();
    assert!(state.try_create_table(4, 0));
    state.push_integer(42);
    assert!(state.raw_set_integer(-2, 1, Value::Integer(42)));
    // 表位于 -2（42 在其上）。
    assert!(state.raw_get_integer(-2, 1));
    assert_eq!(state.check_number(-1), Some(42.0));
    state.pop(1);
    assert_eq!(state.raw_len(-2), 1);

    state.clear_stack();
    state.try_push_buffer(b"answer");
    assert!(state.try_set_global(b"ANSWER"));
    assert!(state.get_global(b"ANSWER"));
    assert_eq!(state.known_string_to_buffer(-1).unwrap(), b"answer");
  }

  #[test]
  fn rotate_and_stack_top() {
    let mut state = LuaStateWrapper::new();
    for i in 1..=3 {
      state.push_integer(i);
    }
    // lua_rotate(L, 1, 1)：栈顶 1 个元素滚到区间开头 → [3, 1, 2]。
    state.rotate(1, 1);
    assert_eq!(state.check_number(1), Some(3.0));
    assert_eq!(state.check_number(2), Some(1.0));
    state.update_stack_top(5);
    assert_eq!(state.get_top(), 5);
    state.update_stack_top(1);
    assert_eq!(state.get_top(), 1);
  }

  #[test]
  fn view_shares_interp_and_refs() {
    let mut state = LuaStateWrapper::new();
    state.push_integer(1);
    assert!(state.try_ref().is_some());

    // 视图共享 app data：栈与引用互通。
    let mut view = LuaStateWrapper::view(state.lua());
    assert_eq!(view.get_top(), 0);
    assert!(view.push_ref(1));
    assert_eq!(view.check_number(-1), Some(1.0));
    view.push_integer(2);
    assert_eq!(state.get_top(), 2);
  }

  #[test]
  fn pcall_n_pads_and_truncates() {
    let mut state = LuaStateWrapper::new();
    state.load_string("return 1, 2").unwrap();
    state.pcall_n(0, 3).unwrap();
    assert_eq!(state.get_top(), 3);
    assert!(state.ref_value(0).is_none());
    state.clear_stack();

    state.load_string("return 1, 2").unwrap();
    state.pcall_n(0, 1).unwrap();
    assert_eq!(state.get_top(), 1);
    assert_eq!(state.check_number(-1), Some(1.0));
  }

  #[test]
  fn next_and_raw_get_top_semantics() {
    let mut state = LuaStateWrapper::new();
    assert!(state.try_create_table(0, 2));
    state.push_constant_string(b"k");
    state.push_integer(7);
    assert!(state.raw_set(-3));
    assert_eq!(state.get_top(), 1);

    // lua_next 语义：迭代尽头只耗键、不压值（净 -1）。
    state.push_nil();
    let mut seen = 0;
    while state.next() {
      seen += 1;
      state.pop(1);
    }
    assert_eq!(seen, 1);
    assert_eq!(state.get_top(), 1);

    // lua_rawget 语义：栈顶键被查得值原位替换。
    state.push_constant_string(b"k");
    assert!(state.raw_get_top(-2));
    assert_eq!(state.get_top(), 2);
    assert_eq!(state.check_number(-1), Some(7.0));
  }
}
