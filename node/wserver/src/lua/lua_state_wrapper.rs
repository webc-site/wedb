//! Lua 状态机封装：以 mlua（luau）承接 C API 栈语义
//! （对标 libs/server/Lua/LuaStateWrapper.cs:LuaStateWrapper）。
//!
//! C# 直接操作 Lua C 栈；mlua 为高层 API，故在封装内维护显式
//! `Vec<Value>` 栈镜像：push/pop/rotate/settop/next 等映射到栈镜像，
//! 表与全局操作落到 mlua，编译/执行经 `load`/`pcall`。

use std::{collections::HashMap, str};

use mlua::{Lua, MultiValue, Value, Variadic};

use super::i_lua_allocator::ILuaAllocator;

/// 栈元素。
pub type StackValue = Value;

/// Lua 状态封装：VM + 栈镜像。
pub struct LuaStateWrapper {
  /// luau VM。
  lua: Lua,
  /// 注册表引用：id → RegistryKey。
  refs: HashMap<i32, mlua::RegistryKey>,
  /// 引用 id 分配器。
  next_ref_id: i32,
  /// C API 栈镜像（栈顶 = 末尾）。
  stack: Vec<StackValue>,
  /// 超时截止（单调毫秒；None = 未设）。
  deadline_monotonic_millis: Option<i64>,
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
    Self {
      lua,
      stack: Vec::new(),
      refs: HashMap::new(),
      next_ref_id: 0,
      deadline_monotonic_millis: None,
      allocator: None,
    }
  }

  /// VM 引用。
  pub fn lua(&self) -> &Lua {
    &self.lua
  }

  /// libs/server/Lua/LuaStateWrapper.cs:ExpectLuaStackEmpty
  ///
  /// 断言栈已空（返回是否为空，测试/调试用）。
  pub fn expect_lua_stack_empty(&self) -> bool {
    self.stack.is_empty()
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TryEnsureMinimumStackCapacity
  pub fn try_ensure_minimum_stack_capacity(&mut self, min_capacity: usize) -> bool {
    self
      .stack
      .reserve(min_capacity.saturating_sub(self.stack.len()));
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
    let args: Vec<StackValue> = self.stack.split_off(self.stack.len().saturating_sub(total));
    let function = as_function(&args[0])?;
    let results: MultiValue = function.call(Variadic::from_iter(args.into_iter().skip(1)))?;
    for v in results.into_vec() {
      self.stack.push(v);
    }
    Ok(())
  }

  /// libs/server/Lua/LuaStateWrapper.cs:Type
  ///
  /// 栈顶第 `idx`（-1 为栈顶）元素的类型名。
  pub fn type_name(&self, idx: i32) -> Option<&'static str> {
    self.peek(idx).map(value_type_name)
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TryPushBuffer
  ///
  /// 压入字节缓冲为字符串。
  pub fn try_push_buffer(&mut self, buffer: &[u8]) -> bool {
    let Ok(s) = self.lua.create_string(buffer) else {
      return false;
    };
    self.stack.push(Value::String(s));
    true
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PushNil
  pub fn push_nil(&mut self) {
    self.stack.push(Value::Nil);
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PushNumber
  pub fn push_number(&mut self, number: f64) {
    self.stack.push(Value::Number(number));
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PushInteger
  pub fn push_integer(&mut self, integer: i64) {
    self.stack.push(Value::Integer(integer));
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PushBoolean
  pub fn push_boolean(&mut self, boolean: bool) {
    self.stack.push(Value::Boolean(boolean));
  }

  /// libs/server/Lua/LuaStateWrapper.cs:Pop
  ///
  /// 弹出 `count` 个栈元素。
  pub fn pop(&mut self, count: usize) {
    let keep = self.stack.len().saturating_sub(count);
    self.stack.truncate(keep);
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PCall
  ///
  /// 保护模式调用栈顶 `nargs-1` 参的函数：成功压入结果，失败压入错误串。
  pub fn pcall(&mut self, nargs: usize) -> Result<(), mlua::Error> {
    let total = nargs + 1;
    let args: Vec<StackValue> = self.stack.split_off(self.stack.len().saturating_sub(total));
    let Ok(function) = as_function(&args[0]) else {
      return Ok(());
    };
    let called: Result<MultiValue, mlua::Error> =
      function.call(Variadic::from_iter(args.into_iter().skip(1)));
    match called {
      Ok(results) => {
        for v in results.into_vec() {
          self.stack.push(v);
        }
        Ok(())
      }
      Err(error) => {
        let message = self.lua.create_string(error.to_string())?;
        self.stack.push(Value::String(message));
        Ok(())
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
    let (Some(value), Some(key)) = (self.stack.pop(), self.stack.pop()) else {
      return false;
    };
    let Some(Value::Table(table)) = self.peek(table_idx) else {
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
        self.stack.push(value);
        true
      }
      Err(_) => false,
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TryRef
  ///
  /// 引用栈顶元素到注册表，返回引用 id。
  pub fn try_ref(&mut self) -> Option<i32> {
    let value = self.stack.pop()?;
    let key = self.lua.create_registry_value(value).ok()?;
    self.next_ref_id += 1;
    let id = self.next_ref_id;
    self.refs.insert(id, key);
    Some(id)
  }

  /// libs/server/Lua/LuaStateWrapper.cs:Unref
  pub fn unref(&mut self, ref_id: i32) {
    if let Some(key) = self.refs.remove(&ref_id) {
      self.lua.remove_registry_value(key).ok();
    }
  }

  /// 引用取值（Runner 的 script lookup 路径）。
  pub fn ref_value(&self, ref_id: i32) -> Option<StackValue> {
    let key = self.refs.get(&ref_id)?;
    self.lua.registry_value::<Value>(key).ok()
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TryCreateTable
  ///
  /// 压入新表（`narr`/`nrec` 仅容量提示）。
  pub fn try_create_table(&mut self, narr: usize, nrec: usize) -> bool {
    match self.lua.create_table_with_capacity(narr, nrec) {
      Ok(table) => {
        self.stack.push(Value::Table(table));
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
        self.stack.push(value);
        true
      }
      Err(_) => false,
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TrySetGlobal
  pub fn try_set_global(&mut self, name: &[u8]) -> bool {
    let (Some(value), Ok(name)) = (self.stack.pop(), str::from_utf8(name)) else {
      return false;
    };
    self.lua.globals().set(name, value).is_ok()
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
    self.stack.push(Value::Function(function));
    Ok(())
  }

  /// libs/server/Lua/LuaStateWrapper.cs:LoadString
  pub fn load_string(&mut self, source: &str) -> Result<(), mlua::Error> {
    let function = self.lua.load(source).into_function()?;
    self.stack.push(Value::Function(function));
    Ok(())
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TryNumberToString
  ///
  /// 数值 → 字符串（luau 语义：%v 格式）。
  pub fn try_number_to_string(&mut self) -> bool {
    let Some(Some(number)) = self.stack.last().map(|v| match v {
      Value::Number(n) => Some(*n),
      Value::Integer(i) => Some(*i as f64),
      _ => None,
    }) else {
      return false;
    };
    let text = if number == number.trunc() && number.abs() < 1e15 {
      format!("{}", number as i64)
    } else {
      format!("{number}")
    };
    let Ok(s) = self.lua.create_string(text) else {
      return false;
    };
    *self.stack.last_mut().expect("last 已校验") = Value::String(s);
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
      Value::Number(n) => Some(*n),
      Value::Integer(i) => Some(*i as f64),
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
    self.stack.push(Value::Function(function));
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PushConstantString
  pub fn push_constant_string(&mut self, constant: &[u8]) -> bool {
    self.try_push_buffer(constant)
  }

  /// libs/server/Lua/LuaStateWrapper.cs:Next
  ///
  /// 表迭代：栈顶为上次返回的键，压入下一键值对；迭代结束压入 nil。
  #[allow(clippy::should_implement_trait)]
  pub fn next(&mut self) -> bool {
    let Some(key) = self.stack.pop() else {
      return false;
    };
    let Some(Value::Table(table)) = self.peek(-1) else {
      self.stack.push(key);
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
    if let Some((next_key, value)) = pairs.get(start) {
      self.stack.push(next_key.clone());
      self.stack.push(value.clone());
      true
    } else {
      self.stack.push(Value::Nil);
      false
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:PushValue
  ///
  /// 复制 `idx` 处元素压栈。
  pub fn push_value(&mut self, idx: i32) {
    if let Some(value) = self.peek(idx) {
      self.stack.push(value.clone());
    }
  }

  /// libs/server/Lua/LuaStateWrapper.cs:Rotate
  ///
  /// `idx`..栈顶 区间旋转 `n` 位（正左移，负右移）。
  pub fn rotate(&mut self, idx: i32, n: i32) {
    let start = self.abs_index(idx);
    let Some(start) = start else { return };
    if n == 0 || self.stack.len() <= start {
      return;
    }
    let len = self.stack.len() - start;
    let n = ((n % len as i32) + len as i32) as usize % len;
    if n == 0 {
      return;
    }
    self.stack[start..].rotate_left(n);
  }

  /// libs/server/Lua/LuaStateWrapper.cs:TrySetHook / 超时
  ///
  /// luau 无调试钩子 API；时限以 deadline 承载，执行器轮询检查。
  pub fn try_set_hook(&mut self, deadline_monotonic_millis: Option<i64>) {
    self.deadline_monotonic_millis = deadline_monotonic_millis;
  }

  /// 当前时限。
  pub fn deadline(&self) -> Option<i64> {
    self.deadline_monotonic_millis
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
    self.stack.len() < i32::MAX as usize
  }

  /// libs/server/Lua/LuaStateWrapper.cs:AssertLuaStackNotEmpty
  pub fn assert_lua_stack_not_empty(&self) -> bool {
    !self.stack.is_empty()
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
    self.stack.clear();
  }

  /// libs/server/Lua/LuaStateWrapper.cs:UpdateStackTop
  ///
  /// 显式设置栈高（C# lua_settop 的正语义：截断；不足补 nil）。
  pub fn update_stack_top(&mut self, new_top: usize) {
    self.stack.resize(new_top, Value::Nil);
  }

  /// 栈高。
  pub fn get_top(&self) -> usize {
    self.stack.len()
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
        .and_then(|i| self.stack.len().checked_sub(i))
    }
  }

  fn peek(&self, idx: i32) -> Option<&StackValue> {
    self.abs_index(idx).and_then(|i| self.stack.get(i))
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

    // 运行错误 → 错误串压栈。
    state.load_string("error('boom')").unwrap();
    state.pcall(0).unwrap();
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
    state.rotate(1, 1);
    assert_eq!(state.check_number(1), Some(2.0));
    state.update_stack_top(5);
    assert_eq!(state.get_top(), 5);
    state.update_stack_top(1);
    assert_eq!(state.get_top(), 1);
  }
}
