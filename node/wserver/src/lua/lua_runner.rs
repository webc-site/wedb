//! Lua 脚本执行器（对标 libs/server/Lua/LuaRunner.cs:LuaRunner）。
//!
//! C# 以 KeraLua C 指针 + UnmanagedCallersOnly trampoline 承接宿主回调；
//! mlua 侧回调为 Rust 闭包，参数经 app data 栈镜像传递。会话上下文经
//! thread-local [`HostShared`] 传递（对标 LuaRunnerTrampolines.CallbackContext）。

use std::{
  cell::{Cell, RefCell},
  error::Error,
  fmt, mem, ptr,
  rc::Rc,
  str,
};

use gxhash::HashSet;
use wresp::read::{
  try_read_as_span, try_read_error_as_span, try_read_signed_array_length,
  try_read_signed_map_length, try_read_signed_set_length, try_read_span_with_length_header,
  try_read_unsigned_array_length, try_read_verbatim_string_length,
};

use super::{
  lua_options::{LuaLoggingMode, LuaOptions},
  lua_runner__functions::LuaRunner_Functions,
  lua_runner__loader::LuaRunner_Loader,
  lua_runner__strings::ConstantStrings,
  lua_state_wrapper::{LuaStateWrapper, now_monotonic_millis},
  scratch_buffer_network_sender::ScratchBufferBuilder,
  script_hash_key::ScriptHashKey,
  scripting_api::ScriptingApi,
  session_script_cache::SessionScriptCache,
};
use crate::{
  objects::types::object_output::ObjectOutput,
  storage::session::common::array_key_iteration_functions::cluster_slot,
  transaction::txn_key_entry::{LockType, TxnKeyEntries},
};

/// 沙箱初始 KEYS/ARGV 数组容量（C# InitialKeysCapacity/InitialArgvCapacity）。
const INITIAL_KEYS_CAPACITY: usize = 5;
const INITIAL_ARGV_CAPACITY: usize = 5;

/// redis 默认版本号（C# 构造默认 "0.0.0.0"）。
pub const DEFAULT_REDIS_VERSION: &str = "0.0.0.0";

/// 脚本运行错误的 RESP 前缀。
const ERR_PREFIX: &[u8] = b"ERR ";

/// 会话命令面共享句柄（回调与 runner 两侧安全互访）。
pub type ScriptSession = Rc<RefCell<dyn ScriptingApi>>;

/// RESP 输出通道（对标 IResponseAdapter：会话缓冲或 runner 内存缓冲）。
pub struct RespOut<'a> {
  /// 响应写入缓冲。
  pub buf: &'a mut Vec<u8>,
  /// RESP 协议版本。
  pub protocol_version: u8,
}

impl<'a> RespOut<'a> {
  /// 会话侧输出。
  pub fn session(buf: &'a mut Vec<u8>, protocol_version: u8) -> Self {
    Self {
      buf,
      protocol_version,
    }
  }

  /// runner 侧输出（恒 RESP2，对标 RunnerAdapter.RespProtocolVersion = 2）。
  pub fn runner(buf: &'a mut Vec<u8>) -> Self {
    Self {
      buf,
      protocol_version: 2,
    }
  }

  /// libs/server/Lua/LuaRunner.cs:SendAndReset（IResponseAdapter 适配点）
  ///
  /// Vec 自增长，无缓冲翻转（C# 会话/runner 适配器的刷新点恒真成功）。
  pub fn send_and_reset(&mut self) {}

  /// 写 RESP 整数（`:<n>\r\n`）。
  pub fn write_int64(&mut self, value: i64) {
    self.buf.push(b':');
    self.write_decimal(value);
  }

  /// 写 RESP bulk string（`$len\r\n<bytes>\r\n`）。
  pub fn write_bulk_string(&mut self, value: &[u8]) {
    self.buf.push(b'$');
    self.write_decimal(value.len() as i64);
    self.buf.extend_from_slice(value);
    self.buf.extend_from_slice(b"\r\n");
  }

  /// 写 RESP error（`-<msg>\r\n`，msg 自带 ERR 前缀）。
  pub fn write_error(&mut self, msg: &[u8]) {
    self.buf.push(b'-');
    self.buf.extend_from_slice(msg);
    self.buf.extend_from_slice(b"\r\n");
  }

  /// 直写字节。
  pub fn write_direct(&mut self, bytes: &[u8]) {
    self.buf.extend_from_slice(bytes);
  }

  /// RESP2 null（`$-1\r\n`）。
  pub fn write_null(&mut self) {
    self.buf.extend_from_slice(b"$-1\r\n");
  }

  /// RESP3 null（`_\r\n`）。
  pub fn write_resp3_null(&mut self) {
    self.buf.extend_from_slice(b"_\r\n");
  }

  /// 数组头（`*<n>\r\n`）。
  pub fn write_array_len(&mut self, len: usize) {
    self.buf.push(b'*');
    self.write_decimal(len as i64);
  }

  /// map 头（`%<n>\r\n`）。
  pub fn write_map_len(&mut self, len: usize) {
    self.buf.push(b'%');
    self.write_decimal(len as i64);
  }

  /// set 头（`~<n>\r\n`）。
  pub fn write_set_len(&mut self, len: usize) {
    self.buf.push(b'~');
    self.write_decimal(len as i64);
  }

  /// RESP3 double（`,<num>\r\n`）。
  pub fn write_double(&mut self, value: f64) {
    self.buf.push(b',');
    self
      .buf
      .extend_from_slice(ObjectOutput::format_double(value).as_bytes());
    self.buf.extend_from_slice(b"\r\n");
  }

  /// RESP3 boolean（`#t\r\n` / `#f\r\n`）。
  pub fn write_bool(&mut self, value: bool) {
    self
      .buf
      .extend_from_slice(if value { b"#t\r\n" } else { b"#f\r\n" });
  }

  /// 十进制正文 + CRLF。
  fn write_decimal(&mut self, value: i64) {
    let mut buffer = itoa::Buffer::new();
    self.buf.extend_from_slice(buffer.format(value).as_bytes());
    self.buf.extend_from_slice(b"\r\n");
  }
}

/// RunForRunner 的结构化返回（C# object 形态）。
#[derive(Debug, Clone, PartialEq)]
pub enum RespObject {
  /// `+` 简单串。
  SimpleString(String),
  /// `:` 整数。
  Integer(i64),
  /// `$` 批量串。
  BulkString(Vec<u8>),
  /// `$-1` 空。
  Null,
  /// `*` 数组。
  Array(Vec<RespObject>),
}

/// 宿主回调共享态：C# `this` 中除 LuaStateWrapper 外的可变部分
/// （会话引用、preamble 参数、scratch 面、编译产物索引）。
pub struct HostShared {
  /// 编译后的用户函数注册表引用（-1 = 未编译）。
  pub function_registry_index: i32,
  /// redis.log 行为。
  pub log_mode: LuaLoggingMode,
  /// 事务模式（garnet_call 走事务面）。
  pub txn_mode: bool,
  /// 事务键集（txn 模式下脚本 KEYS 逐个登记）。
  pub txn_key_entries: TxnKeyEntries,
  /// 会话引用（run/回调窗口期间有效；None = runner 模式）。
  /// 裸指针承接 C# 的 respServerSession 成员引用语义：仅在窗口内解引用。
  pub session: Option<ScriptSessionPtr>,
  /// RESP 请求拼装缓冲（C# scratchBufferBuilder 的命令拼装面）。
  pub scratch: ScratchBufferBuilder,
  /// redis.call 响应接收面。
  pub sender: super::scratch_buffer_network_sender::ScratchBufferNetworkSender,
  /// runner 模式 preamble 参数（KEYS）。
  pub preamble_keys: Option<Vec<Vec<u8>>>,
  /// runner 模式 preamble 参数（ARGV）。
  pub preamble_argv: Option<Vec<Vec<u8>>>,
  /// session 模式 preamble 参数（numkeys 开头；preamble 消费后即清）。
  pub preamble_args: Vec<Vec<u8>>,
  /// preamble 未消费参数量（KEYS+ARGV 合计）。
  pub preamble_key_and_argv_count: i32,
  /// preamble KEYS 数量。
  pub preamble_n_keys: i32,
  /// 运行中致命损伤标记（NeedsDispose）。
  pub needs_dispose: bool,
}

impl HostShared {
  /// 构造。
  pub fn new(log_mode: LuaLoggingMode, txn_mode: bool) -> Self {
    Self {
      function_registry_index: -1,
      log_mode,
      txn_mode,
      txn_key_entries: TxnKeyEntries::new(16),
      session: None,
      scratch: ScratchBufferBuilder::default(),
      sender: super::scratch_buffer_network_sender::ScratchBufferNetworkSender::new(),
      preamble_keys: None,
      preamble_argv: None,
      preamble_args: Vec::new(),
      preamble_key_and_argv_count: 0,
      preamble_n_keys: 0,
      needs_dispose: false,
    }
  }
}

thread_local! {
  /// 宿主回调上下文（对标 LuaRunnerTrampolines.CallbackContext 的 ThreadStatic）。
  static CALLBACK_CONTEXT: Cell<*mut HostShared> = const { Cell::new(ptr::null_mut()) };
}

/// libs/server/Lua/LuaRunner.Functions.cs:SetCallbackContext
///
/// 设置回调期间可用的宿主上下文（仅同一调用线程内有效）。
pub fn set_callback_context(context: *mut HostShared) {
  CALLBACK_CONTEXT.with(|cell| {
    debug_assert!(cell.get().is_null(), "Expected null context");
    cell.set(context);
  });
}

/// libs/server/Lua/LuaRunner.Functions.cs:ClearCallbackContext
pub fn clear_callback_context(context: *mut HostShared) {
  CALLBACK_CONTEXT.with(|cell| {
    debug_assert_eq!(cell.get(), context, "Expected context to match");
    cell.set(ptr::null_mut());
  });
}

/// 取当前回调上下文（未设置即程序性错误，以 panic 上抛为 Lua 错误）。
pub fn callback_context() -> *mut HostShared {
  let context = CALLBACK_CONTEXT.with(Cell::get);
  if context.is_null() {
    panic!("no lua callback context");
  }
  context
}

/// 回调上下文守卫（对标 C# SetCallbackContext / finally ClearCallbackContext 配对）。
///
/// C# 在 CompileFor*/RunFor* 期间把 `this` 挂进 ThreadStatic 槽供 trampoline
/// 取回；Rust 侧以 RAII 守卫保证 panic 路径也能清槽。
struct CallbackGuard(*mut HostShared);

impl CallbackGuard {
  /// 进入回调上下文窗口。
  fn enter(host: *mut HostShared) -> Self {
    let ptr: *mut HostShared = host;
    set_callback_context(ptr);
    Self(ptr)
  }
}

impl Drop for CallbackGuard {
  fn drop(&mut self) {
    clear_callback_context(self.0);
  }
}

/// 会话指针（生存期擦除形态：仅回调窗口内解引用，窗口外恒 None）。
#[repr(transparent)]
pub struct ScriptSessionPtr(*mut (dyn ScriptingApi + 'static));

impl ScriptSessionPtr {
  /// 从带生存期的会话引用构造（窗口内使用，窗口结束即清除）。
  ///
  /// SAFETY（调用方）：窗口结束后不得再解引用。
  pub(crate) fn erase<'a>(session: &'a mut (dyn ScriptingApi + 'a)) -> Self {
    Self(unsafe {
      mem::transmute::<*mut (dyn ScriptingApi + 'a), *mut (dyn ScriptingApi + 'static)>(
        session as *mut (dyn ScriptingApi + 'a),
      )
    })
  }

  /// 解引用（回调窗口内）。
  pub(crate) fn get(&mut self) -> &mut (dyn ScriptingApi + 'static) {
    unsafe { &mut *self.0 }
  }
}

/// 宿主函数通用形态：操作栈镜像 + 宿主上下文，返回栈上结果数。
pub type HostFn = fn(&mut LuaStateWrapper, &mut HostShared) -> i32;

/// Lua 脚本执行器。
pub struct LuaRunner {
  /// 注册表引用：sandbox_env。
  sandbox_env_registry_index: i32,
  /// 注册表引用：load_sandboxed。
  load_sandboxed_registry_index: i32,
  /// 注册表引用：reset_keys_and_argv。
  reset_keys_and_argv_registry_index: i32,
  /// 注册表引用：request_timeout。
  request_timeout_registry_index: i32,

  /// redis.log 行为。
  log_mode: LuaLoggingMode,
  /// 允许导出函数集。
  allowed_functions: HashSet<String>,
  /// 脚本源码。
  source: Vec<u8>,
  /// 事务模式。
  txn_mode: bool,

  /// 宿主共享态（堆定址，回调经 thread-local 指针访问）。
  host: Box<HostShared>,

  /// KEYS 数组容量。
  keys_arr_capacity: usize,
  /// ARGV 数组容量。
  argv_arr_capacity: usize,
  /// 当前 KEYS 长度。
  key_length: usize,
  /// 当前 ARGV 长度。
  argv_length: usize,

  /// VM。
  state: LuaStateWrapper,
}

/// 构造失败（对标 GarnetException 文案）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LuaRunnerInitError(pub String);

impl fmt::Display for LuaRunnerInitError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(&self.0)
  }
}

impl Error for LuaRunnerInitError {}

impl LuaRunner {
  /// libs/server/Lua/LuaRunner.cs:LuaRunner（构造）
  ///
  /// 建 KEYS/ARGV 全局表、注册宿主函数族、灌入 redis 版本全局量，
  /// 执行 loader block 并引用沙箱关键对象。
  pub fn new(
    log_mode: LuaLoggingMode,
    mem_limit_bytes: Option<usize>,
    allowed_functions: HashSet<String>,
    source: Vec<u8>,
    txn_mode: bool,
    redis_version: &str,
  ) -> Result<Self, LuaRunnerInitError> {
    let mut state = LuaStateWrapper::new();
    if let Some(limit) = mem_limit_bytes
      && let Err(e) = state.lua().set_memory_limit(limit)
    {
      return Err(LuaRunnerInitError(format!(
        "Could not initialize Lua VM: {e}"
      )));
    }

    // KEYS / ARGV 全局表（显式容量，后续按需重建）。
    if !state.try_create_table(INITIAL_KEYS_CAPACITY, 0) || !state.try_set_global(b"KEYS") {
      return Err(LuaRunnerInitError(
        "Insufficient space in Lua VM for KEYS".into(),
      ));
    }
    if !state.try_create_table(INITIAL_ARGV_CAPACITY, 0) || !state.try_set_global(b"ARGV") {
      return Err(LuaRunnerInitError(
        "Insufficient space in Lua VM for ARGV".into(),
      ));
    }

    // Lua 5.1 兼容 + 运行时库 + redis 接口（宿主实现）。
    Self::register(&mut state, b"garnet_atan2", LuaRunner_Functions::atan2)?;
    Self::register(&mut state, b"garnet_cosh", LuaRunner_Functions::cosh)?;
    Self::register(&mut state, b"garnet_frexp", LuaRunner_Functions::frexp)?;
    Self::register(&mut state, b"garnet_ldexp", LuaRunner_Functions::ldexp)?;
    Self::register(&mut state, b"garnet_log10", LuaRunner_Functions::log10)?;
    Self::register(&mut state, b"garnet_pow", LuaRunner_Functions::pow)?;
    Self::register(&mut state, b"garnet_sinh", LuaRunner_Functions::sinh)?;
    Self::register(&mut state, b"garnet_tanh", LuaRunner_Functions::tanh)?;
    Self::register(&mut state, b"garnet_maxn", LuaRunner_Functions::maxn)?;
    Self::register(
      &mut state,
      b"garnet_loadstring",
      LuaRunner_Functions::load_string,
    )?;

    Self::register(
      &mut state,
      b"garnet_cjson_encode",
      LuaRunner_Functions::c_json_encode,
    )?;
    Self::register(
      &mut state,
      b"garnet_cjson_decode",
      LuaRunner_Functions::c_json_decode,
    )?;
    Self::register(
      &mut state,
      b"garnet_bit_tobit",
      LuaRunner_Functions::bit_to_bit,
    )?;
    Self::register(
      &mut state,
      b"garnet_bit_tohex",
      LuaRunner_Functions::bit_to_hex,
    )?;
    // garnet_bitop implements bnot, bor, band, xor, etc. but isn't directly exposed
    Self::register(&mut state, b"garnet_bitop", LuaRunner_Functions::bitop)?;
    Self::register(
      &mut state,
      b"garnet_bit_bswap",
      LuaRunner_Functions::bit_bswap,
    )?;
    Self::register(
      &mut state,
      b"garnet_cmsgpack_pack",
      LuaRunner_Functions::c_msg_pack_pack,
    )?;
    Self::register(
      &mut state,
      b"garnet_cmsgpack_unpack",
      LuaRunner_Functions::c_msg_pack_unpack,
    )?;
    Self::register(
      &mut state,
      b"garnet_struct_pack",
      LuaRunner_Functions::struct_pack,
    )?;
    Self::register(
      &mut state,
      b"garnet_struct_unpack",
      LuaRunner_Functions::struct_unpack,
    )?;
    Self::register(
      &mut state,
      b"garnet_struct_size",
      LuaRunner_Functions::struct_size,
    )?;
    Self::register(&mut state, b"garnet_call", LuaRunner_Functions::garnet_call)?;
    Self::register(&mut state, b"garnet_sha1hex", LuaRunner_Functions::sha1_hex)?;
    Self::register(&mut state, b"garnet_log", LuaRunner_Functions::log)?;
    Self::register(
      &mut state,
      b"garnet_acl_check_cmd",
      LuaRunner_Functions::acl_check_command,
    )?;
    Self::register(&mut state, b"garnet_setresp", LuaRunner_Functions::set_resp)?;
    Self::register(
      &mut state,
      b"garnet_unpack_trampoline",
      LuaRunner_Functions::unpack_trampoline,
    )?;
    Self::register(
      &mut state,
      b"garnet_request_timeout",
      LuaRunner_Functions::request_timeout_fn,
    )?;
    Self::register(&mut state, b"garnet_load", LuaRunner_Functions::load_chunk)?;

    let mut runner = Self {
      sandbox_env_registry_index: -1,
      load_sandboxed_registry_index: -1,
      reset_keys_and_argv_registry_index: -1,
      request_timeout_registry_index: -1,
      log_mode,
      allowed_functions,
      source,
      txn_mode,
      host: Box::new(HostShared::new(log_mode, txn_mode)),
      keys_arr_capacity: INITIAL_KEYS_CAPACITY,
      argv_arr_capacity: INITIAL_ARGV_CAPACITY,
      key_length: 0,
      argv_length: 0,
      state,
    };

    // redis 版本全局量。
    if !runner.state.try_push_buffer(redis_version.as_bytes())
      || !runner.state.try_set_global(b"garnet_REDIS_VERSION")
    {
      return Err(LuaRunnerInitError(
        "Insufficient space in Lua VM for redis version global".into(),
      ));
    }

    let redis_version_num = Self::redis_version_num(redis_version);
    runner.state.push_integer(redis_version_num);
    if !runner.state.try_set_global(b"garnet_REDIS_VERSION_NUM") {
      return Err(LuaRunnerInitError(
        "Insufficient space in Lua VM for redis version number global".into(),
      ));
    }

    // loader block：构建沙箱环境与关键函数。
    let loader_block = LuaRunner_Loader::prepare_loader_block_bytes(&runner.allowed_functions);
    if runner
      .state
      .load_buffer(loader_block.as_bytes(), "@loader_block")
      .is_err()
    {
      return Err(LuaRunnerInitError("Could not initialize Lua VM".into()));
    }
    if runner.state.pcall_n(0, usize::MAX).is_err() {
      let err_msg = runner.state.known_string_to_buffer(-1).map_or_else(
        || "No error provided".to_string(),
        |buff| String::from_utf8_lossy(&buff).into_owned(),
      );
      return Err(LuaRunnerInitError(format!(
        "Could not initialize Lua sandbox state: {err_msg}"
      )));
    }

    // 引用沙箱关键对象。
    macro_rules! ref_global {
      ($field:ident, $name:expr, $err:expr) => {{
        if !runner.state.get_global($name) {
          return Err(LuaRunnerInitError($err.into()));
        }
        runner.$field = runner.state.try_ref().unwrap_or(-1);
        if runner.$field == -1 {
          return Err(LuaRunnerInitError($err.into()));
        }
      }};
    }
    ref_global!(
      sandbox_env_registry_index,
      b"sandbox_env",
      "Insufficient space in VM for sandbox_env ref"
    );
    ref_global!(
      load_sandboxed_registry_index,
      b"load_sandboxed",
      "Insufficient space in VM for load_sandboxed ref"
    );
    ref_global!(
      reset_keys_and_argv_registry_index,
      b"reset_keys_and_argv",
      "Insufficient space in VM for reset_keys_and_argv ref"
    );
    ref_global!(
      request_timeout_registry_index,
      b"request_timeout",
      "Insufficient space in VM for request_timeout ref"
    );

    debug_assert!(runner.state.expect_lua_stack_empty());
    Ok(runner)
  }

  /// libs/server/Lua/LuaRunner.cs:LuaRunner（options 构造重载）
  pub fn with_options(
    options: &LuaOptions,
    source: &[u8],
    txn_mode: bool,
    redis_version: &str,
  ) -> Result<Self, LuaRunnerInitError> {
    Self::new(
      options.log_mode,
      options.get_memory_limit_bytes(),
      options.allowed_functions.iter().cloned().collect(),
      source.to_vec(),
      txn_mode,
      redis_version,
    )
  }

  /// C# `Version.Parse` 的 major<<16|minor<<8|build 折算。
  fn redis_version_num(redis_version: &str) -> i64 {
    let mut parts = redis_version
      .split('.')
      .map(|p| p.parse::<i64>().unwrap_or(0).clamp(0, u8::MAX as i64));
    let major = parts.next().unwrap_or(0);
    let minor = parts.next().unwrap_or(0);
    let build = parts.next().unwrap_or(0);
    (major << 16) | (minor << 8) | build
  }

  /// libs/server/Lua/LuaRunner.cs:Register（构造内局部函数）
  ///
  /// 注册宿主函数为全局；闭包即蹦床：参数压栈镜像 → 执行 → 取回结果。
  /// 末尾清空镜像栈，对标 Lua C 调用帧丢弃语义（仅保留返回值）。
  fn register(
    state: &mut LuaStateWrapper,
    name: &[u8],
    function: HostFn,
  ) -> Result<(), LuaRunnerInitError> {
    let registration = state.register_function(name, move |lua, args: mlua::MultiValue| {
      let host = unsafe { &mut *callback_context() };
      let mut view = LuaStateWrapper::view(lua);
      for value in args {
        view.push_stack_value(value);
      }
      let count = function(&mut view, host);
      let results = view.pop_values(count);
      // C 帧丢弃：未消费的参数残留不外溢到下一次回调。
      view.clear_stack();
      Ok(mlua::MultiValue::from_vec(results))
    });
    if !registration {
      return Err(LuaRunnerInitError(format!(
        "Insufficient space in VM for {} global",
        String::from_utf8_lossy(name)
      )));
    }
    Ok(())
  }

  /// libs/server/Lua/LuaRunner.cs:NeedsDispose
  ///
  /// 运行中遭遇致命损伤时为 true，应在最近时机重建 runner。
  pub fn needs_dispose(&self) -> bool {
    self.host.needs_dispose
  }

  /// libs/server/Lua/LuaRunner.cs:CompileForRunner
  ///
  /// 面向宿主的编译；错误以 Err 返回（C# 抛 GarnetException）。
  pub fn compile_for_runner(&mut self, out: &mut Vec<u8>) -> Result<(), String> {
    debug_assert!(self.state.expect_lua_stack_empty());

    let _guard = CallbackGuard::enter(self.host.as_mut());
    self.compile_common(out);

    // 读回响应中的错误（C# RespReadUtils.TryReadErrorAsSpan）。
    let mut cursor: &[u8] = out;
    let mut err_span: &[u8] = &[];
    if matches!(try_read_error_as_span(&mut err_span, &mut cursor), Ok(true)) {
      return Err(String::from_utf8_lossy(err_span).into_owned());
    }
    if self.host.function_registry_index == -1 {
      return Err("Internal Lua Error".into());
    }
    Ok(())
  }

  /// libs/server/Lua/LuaRunner.cs:CompileForSession
  ///
  /// 面向会话的编译；错误以 RESP error 写入 `out`，返回是否成功。
  pub fn compile_for_session(&mut self, out: &mut Vec<u8>) -> bool {
    debug_assert!(self.state.expect_lua_stack_empty());

    let _guard = CallbackGuard::enter(self.host.as_mut());
    self.compile_common(out);
    if out.first() == Some(&b'-') {
      return false;
    }
    self.host.function_registry_index != -1
  }

  /// libs/server/Lua/LuaRunner.cs:ResetCompilation
  pub fn reset_compilation(&mut self) {
    if self.host.function_registry_index != -1 {
      self.state.unref(self.host.function_registry_index);
      self.host.function_registry_index = -1;
    }
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:CompileCommon
  ///
  /// 编译脚本：经 load_sandboxed 装载，(err, func) 双值返回。
  fn compile_common(&mut self, out: &mut Vec<u8>) {
    debug_assert_eq!(
      self.host.function_registry_index, -1,
      "Shouldn't compile multiple times"
    );

    if !self.state.push_ref(self.load_sandboxed_registry_index)
      || !self.state.try_push_buffer(&self.source)
    {
      resp_out_error(out, ConstantStrings::OUT_OF_MEMORY);
      return;
    }

    let call_res = self.state.pcall_n(1, 2);

    // On success the stack will have two things on it:
    //  1. The error (nil if not error)
    //  2. The function (nil if error)

    if call_res.is_ok() && self.state.get_top() == 2 && !self.state.to_boolean(1) {
      // No error, success!
      match self.state.try_ref() {
        Some(index) => self.host.function_registry_index = index,
        None => {
          // Uh-oh, couldn't save the function under the registry
          resp_out_error(out, ConstantStrings::OUT_OF_MEMORY);
        }
      }
    } else {
      let err_str = if self.state.get_top() >= 1
        && let Some(buff) = self.state.known_string_to_buffer(1)
      {
        // We control the definition of load_sandboxed, so we know this will be the error
        format!("Compilation error: {}", String::from_utf8_lossy(&buff))
      } else {
        "Compilation error, cause unknown".to_string()
      };

      out.clear();
      out.push(b'-');
      out.extend_from_slice(err_str.as_bytes());
      out.extend_from_slice(b"\r\n");
    }

    // C# 形态中 CompileCommon 以 C 函数返回 0，帧内栈槽随帧丢弃；
    // Rust 栈镜像持久，需显式清空。
    self.state.clear_stack();
  }

  /// libs/server/Lua/LuaRunner.cs:LuaWrappedError
  ///
  /// 清栈后压 `non_error_returns` 个 nil，再压错误串；返回结果数。
  /// （C# 的 const string 注册表索引形态在 Rust 侧统一为静态字节。）
  pub fn lua_wrapped_error(&mut self, non_error_returns: usize, error_msg: &[u8]) -> i32 {
    lua_wrapped_error_view(&mut self.state, non_error_returns, error_msg)
  }

  /// libs/server/Lua/LuaRunner.cs:ProcessRespResponse
  ///
  /// 解析单条 RESP(2|3) 响应为栈上 Lua 值；返回结果数。
  pub fn process_resp_response(&mut self, resp_protocol_version: u8, resp: &[u8]) -> i32 {
    process_resp_response_view(&mut self.state, resp_protocol_version, resp)
  }

  /// libs/server/Lua/LuaRunner.cs:ProcessSingleRespTerm
  pub fn process_single_resp_term(&mut self, resp_protocol_version: u8, cursor: &mut &[u8]) -> i32 {
    process_single_resp_term_view(&mut self.state, resp_protocol_version, cursor)
  }

  /// libs/server/Lua/LuaRunner.cs:RunForSession
  ///
  /// 以会话参数（numkeys 开头）执行已编译函数，响应写入 `out`。
  pub fn run_for_session(
    &mut self,
    args: &[Vec<u8>],
    session: &mut dyn ScriptingApi,
    out: &mut Vec<u8>,
  ) {
    self.host.preamble_args = args.to_vec();
    // C# RunForSession(count)：count = parseState.Count - 1（去 script 位），
    // 即本切片长度（numkeys + keys... + argv...）。
    self.host.preamble_key_and_argv_count = args.len() as i32;
    self.host.session = Some(ScriptSessionPtr::erase(session));

    let _guard = CallbackGuard::enter(self.host.as_mut());

    // Every invocation starts in RESP2
    session.update_resp_protocol_version(2);

    self.reset_timeout();

    // preamble：装配 KEYS/ARGV（C# 经 RunPreambleForSession C 函数），
    // 随后执行已编译函数 —— redis.call 回调经窗口上下文访问会话面
    // （窗口已由 CallbackGuard 安装）。
    let preamble_res = self.run_preamble_for_session();

    if let Err(err) = preamble_res {
      let mut resp = RespOut::session(out, 2);
      resp.write_error(err);
      return;
    }

    let mut resp = RespOut::session(out, 2);
    if self.txn_mode && self.host.preamble_n_keys > 0 {
      self.run_in_transaction(&mut resp);
    } else {
      self.run_common(&mut resp);
    }
    self.host.session = None;
  }

  /// libs/server/Lua/LuaRunner.cs:RunForRunner
  ///
  /// 以 (keys, argv) 执行并把响应解析为对象（宿主形态）。
  pub fn run_for_runner(
    &mut self,
    keys: Option<Vec<Vec<u8>>>,
    argv: Option<Vec<Vec<u8>>>,
  ) -> Result<RespObject, String> {
    let _guard = CallbackGuard::enter(self.host.as_mut());

    self.reset_timeout();

    self.host.preamble_keys = keys.clone();
    self.host.preamble_argv = argv;
    let mut response = Vec::with_capacity(64);
    // 回调窗口已由 CallbackGuard 安装。
    self.run_preamble_for_runner()?;
    self.host.preamble_keys = None;
    self.host.preamble_argv = None;

    let mut resp = RespOut::runner(&mut response);
    if self.txn_mode && keys.as_ref().is_some_and(|k| !k.is_empty()) {
      // runner 模式无会话事务面，键锁语义由 TxnKeyEntries 承接。
      self.host.txn_key_entries.lock_all_keys();
      self.run_common(&mut resp);
      self.host.txn_key_entries.unlock_all_keys();
    } else {
      self.run_common(&mut resp);
    }

    let mut cursor: &[u8] = &response;
    let mut err_span: &[u8] = &[];
    if matches!(try_read_error_as_span(&mut err_span, &mut cursor), Ok(true)) {
      return Err(String::from_utf8_lossy(err_span).into_owned());
    }

    let mut cursor: &[u8] = &response;
    let ret = self.map_resp_to_object(&mut cursor)?;
    debug_assert!(cursor.is_empty(), "Should have fully consumed response");
    Ok(ret)
  }

  /// libs/server/Lua/LuaRunner.cs:MapRespToObject
  fn map_resp_to_object(&mut self, cursor: &mut &[u8]) -> Result<RespObject, String> {
    match cursor.first() {
      Some(b'+') => {
        let mut simple_str: &[u8] = &[];
        if !matches!(try_read_as_span(&mut simple_str, cursor), Ok(true)) {
          return Err("Unexpected simple string".into());
        }
        Ok(RespObject::SimpleString(
          String::from_utf8_lossy(simple_str).into_owned(),
        ))
      }
      Some(b':') => {
        let Some(int64) = read_resp_int(cursor) else {
          return Err("Unexpected integer".into());
        };
        Ok(RespObject::Integer(int64))
      }
      // Error ('-') is handled before call to MapRespToObject
      Some(b'$') => {
        if cursor.len() >= 5 && &cursor[1..5] == b"-1\r\n" {
          *cursor = &cursor[5..];
          return Ok(RespObject::Null);
        }
        let mut bulk_str: &[u8] = &[];
        if !matches!(
          try_read_span_with_length_header(&mut bulk_str, cursor),
          Ok(true)
        ) {
          return Err("Unexpected bulk string".into());
        }
        Ok(RespObject::BulkString(bulk_str.to_vec()))
      }
      Some(b'*') => {
        let mut item_count = 0i32;
        if !matches!(
          try_read_unsigned_array_length(&mut item_count, cursor),
          Ok(true)
        ) {
          return Err("Unexpected array".into());
        }
        let mut array = Vec::with_capacity(item_count as usize);
        for _ in 0..item_count {
          array.push(self.map_resp_to_object(cursor)?);
        }
        Ok(RespObject::Array(array))
      }
      other => Err(format!(
        "Unexpected sigil {}",
        other.map(|c| *c as char).unwrap_or('\0')
      )),
    }
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:UnsafeRunPreambleForRunner
  ///
  /// runner 模式 preamble：重置并灌入 KEYS/ARGV。
  pub fn run_preamble_for_runner(&mut self) -> Result<(), String> {
    let keys = self.host.preamble_keys.clone().unwrap_or_default();
    let argv = self.host.preamble_argv.clone().unwrap_or_default();

    if self.try_reset_parameters(keys.len(), argv.len()).is_err() {
      self.host.needs_dispose = true;
      return Err("Resetting parameters to Lua script failed: Other".into());
    }

    if !keys.is_empty() {
      if self.keys_arr_capacity < keys.len() && !self.try_recreate_keys(keys.len()) {
        return Err(String::from_utf8_lossy(ConstantStrings::OUT_OF_MEMORY).into_owned());
      }

      self.fill_array_global(ConstantStrings::KEYS, &keys);
    }

    if !argv.is_empty() {
      if self.argv_arr_capacity < argv.len() && !self.try_recreate_argv(argv.len()) {
        return Err(String::from_utf8_lossy(ConstantStrings::OUT_OF_MEMORY).into_owned());
      }

      self.fill_array_global(ConstantStrings::ARGV, &argv);
    }

    Ok(())
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:UnsafeRunPreambleForSession
  ///
  /// session 模式 preamble：args[0] 为 numkeys，随后 KEYS*、ARGV*。
  pub fn run_preamble_for_session(&mut self) -> Result<(), &'static [u8]> {
    // take 所有权避免与 host 可变借用重叠（参数仅 preamble 消费一次）。
    let args = mem::take(&mut self.host.preamble_args);

    let mut offset = 1usize;
    let n_keys = args
      .first()
      .and_then(|k| str::from_utf8(k).ok()?.parse::<i32>().ok())
      .unwrap_or_default();
    self.host.preamble_n_keys = n_keys;
    self.host.preamble_key_and_argv_count -= 1;

    let n_argv = self.host.preamble_key_and_argv_count - n_keys;
    if self
      .try_reset_parameters(n_keys as usize, n_argv as usize)
      .is_err()
    {
      self.host.needs_dispose = true;
      return Err(ConstantStrings::PARAMETER_RESET_FAILED_OTHER);
    }

    if n_keys > 0 {
      if self.keys_arr_capacity < n_keys as usize && !self.try_recreate_keys(n_keys as usize) {
        return Err(ConstantStrings::INSUFFICIENT_LUA_STACK_SPACE);
      }

      let end = (offset + n_keys as usize).min(args.len());
      let keys: Vec<Vec<u8>> = args[offset..end].to_vec();
      if self.txn_mode {
        for key in &keys {
          self
            .host
            .txn_key_entries
            .add_key(i64::from(cluster_slot(key)), LockType::Exclusive);
        }
      }
      self.fill_array_global(ConstantStrings::KEYS, &keys);
      self.key_length = n_keys as usize;

      offset = end;
    }

    if n_argv > 0 {
      if self.argv_arr_capacity < n_argv as usize && !self.try_recreate_argv(n_argv as usize) {
        return Err(ConstantStrings::OUT_OF_MEMORY);
      }

      let end = (offset + n_argv as usize).min(args.len());
      let argv: Vec<Vec<u8>> = args[offset..end].to_vec();
      self.fill_array_global(ConstantStrings::ARGV, &argv);
      self.argv_length = n_argv as usize;
    }

    Ok(())
  }

  /// 装配 sandbox_env.<KEYS|ARGV> 数组全局（preamble 共用形态）。
  fn fill_array_global(&mut self, name: &[u8], values: &[Vec<u8>]) {
    // 取目标数组表压栈。
    _ = self.state.push_ref(self.sandbox_env_registry_index);
    let sandbox_at = self.state.get_top() as i32;
    self.state.push_constant_string(name);
    _ = self.state.raw_get_top(sandbox_at);
    self.state.remove(sandbox_at);

    for (i, value) in values.iter().enumerate() {
      // equivalent to KEYS[i+1] = value
      let _ = self.state.try_push_buffer(value);
      if let Some(value) = self.state.pop_value() {
        self.state.raw_set_integer(1, i as i64 + 1, value);
      }
    }

    // Remove 数组表
    self.state.pop(1);
  }

  /// libs/server/Lua/LuaRunner.cs:RunInTransaction
  ///
  /// 事务窗口内执行 RunCommon（键锁经 TxnKeyEntries，事务面经会话）。
  fn run_in_transaction(&mut self, resp: &mut RespOut) {
    if let Some(session) = self.host.session.as_mut() {
      let session = session.get();
      session.begin_transaction();
      session.set_transaction_mode(true);
    }
    self.host.txn_key_entries.lock_all_keys();

    self.run_common(resp);

    self.host.txn_key_entries.unlock_all_keys();
    if let Some(session) = self.host.session.as_mut() {
      let session = session.get();
      session.set_transaction_mode(false);
      session.end_transaction();
    }
  }

  /// libs/server/Lua/LuaRunner.cs:ResetTimeout
  ///
  /// 清时限（下一次运行前调用）。
  pub fn reset_timeout(&mut self) {
    self.state.try_set_hook(None);
  }

  /// libs/server/Lua/LuaRunner.cs:RequestTimeout
  ///
  /// 请求当前执行立即超时（中断钩子在下一检查点抛出超时错误）。
  pub fn request_timeout(&mut self) {
    self.state.try_set_hook(Some(now_monotonic_millis()));
  }

  /// libs/server/Lua/LuaRunner.cs:TryResetParameters
  fn try_reset_parameters(&mut self, n_keys: usize, n_args: usize) -> Result<(), mlua::Error> {
    if self.key_length > n_keys || self.argv_length > n_args {
      if !self.state.push_ref(self.reset_keys_and_argv_registry_index) {
        return Err(mlua::Error::RuntimeError(
          "reset_keys_and_argv ref missing".into(),
        ));
      }

      self.state.push_integer(n_keys as i64 + 1);
      self.state.push_integer(n_args as i64 + 1);

      self.state.pcall(2)?;
    }

    self.key_length = n_keys;
    self.argv_length = n_args;

    Ok(())
  }

  /// libs/server/Lua/LuaRunner.cs:TryRecreateKEYS
  fn try_recreate_keys(&mut self, length: usize) -> bool {
    // Get sandbox_env and "KEYS" on the stack
    if !self.state.push_ref(self.sandbox_env_registry_index) {
      return false;
    }
    let sandbox_env_index = self.state.get_top() as i32;
    self.state.push_constant_string(ConstantStrings::KEYS);

    // Make new KEYS
    if !self.state.try_create_table(length, 0) {
      return false;
    }

    // Save it (existing slot update, no allocation impact)
    self.state.raw_set(sandbox_env_index);

    // Get sandbox_env off the stack
    self.state.pop(1);

    self.keys_arr_capacity = length;
    true
  }

  /// libs/server/Lua/LuaRunner.cs:TryRecreateARGV
  fn try_recreate_argv(&mut self, length: usize) -> bool {
    if !self.state.push_ref(self.sandbox_env_registry_index) {
      return false;
    }
    let sandbox_env_index = self.state.get_top() as i32;
    self.state.push_constant_string(ConstantStrings::ARGV);

    if !self.state.try_create_table(length, 0) {
      return false;
    }

    self.state.raw_set(sandbox_env_index);
    self.state.pop(1);

    self.argv_arr_capacity = length;
    true
  }

  /// libs/server/Lua/LuaRunner.cs:RunCommon
  ///
  /// 执行已编译函数并写出响应（错误经 RESP error 形态）。
  fn run_common(&mut self, resp: &mut RespOut) {
    // Every invocation starts in RESP2（会话协议版本回置由 RunForSession 完成）。
    if !self.state.push_ref(self.host.function_registry_index) {
      resp.write_error(b"ERR An error occurred while invoking a Lua script");
      return;
    }

    let call_res = self.state.pcall_n(0, 1);
    if call_res.is_ok() {
      // The actual call worked, handle the response
      self.write_response(resp);
      debug_assert!(self.state.expect_lua_stack_empty());
      return;
    }

    // An error was raised
    match self.state.get_top() {
      0 => {
        resp.write_error(b"ERR An error occurred while invoking a Lua script");
      }
      1 => {
        // PCall will put error in a string
        let Some(err_buf) = self.state.known_string_to_buffer(1) else {
          log::error!("Got an unexpected number of values back from a pcall error");
          resp.write_error(b"ERR Unexpected error response");
          self.state.clear_stack();
          return;
        };

        if err_buf.starts_with(ERR_PREFIX) {
          // Response came back with a ERR, already - just pass it along
          resp.write_error(&err_buf);
        } else {
          // Otherwise, this is probably a Lua error - and those aren't very descriptive
          // So slap some more information in
          resp.write_direct(b"-ERR Lua encountered an error: ");
          resp.write_direct(&err_buf);
          resp.write_direct(b"\r\n");
        }

        self.state.pop(1);
      }
      _ => {
        log::error!("Got an unexpected number of values back from a pcall error");
        resp.write_error(b"ERR Unexpected error response");
        self.state.clear_stack();
      }
    }
    debug_assert!(self.state.expect_lua_stack_empty());
  }

  /// libs/server/Lua/LuaRunner.cs:WriteResponse
  ///
  /// 栈顶（若有）值 → RESP 回复写入 `resp`。
  pub fn write_response(&mut self, resp: &mut RespOut) {
    if self.state.get_top() == 0 {
      // 顶层 null 无需栈空间，恒可写出。
      if resp.protocol_version == 3 {
        _ = Self::try_write_resp3_null(self, resp, &mut None);
      } else {
        _ = Self::try_write_resp2_null(self, resp, &mut None);
      }
      return;
    }

    // Copy the value in case of a trial serialization (Vec 单趟直写)
    self.state.push_value(1);

    let mut err: Option<&'static [u8]> = None;
    let written = Self::try_write_single_item(self, resp, &mut err);

    if err.is_none() && written {
      // Remove the extra value copy we pushed（原值随写出弹空）
      self.state.pop(1);
    }

    if let Some(err) = err {
      // An error was encountered, so write it out
      self.state.clear_stack();
      self.state.push_constant_string(err);
      let err_buff = self.state.known_string_to_buffer(1).unwrap_or_default();
      resp.write_error(&err_buff);
      self.state.pop(1);
    }
  }

  /// libs/server/Lua/LuaRunner.cs:TryWriteSingleItem
  ///
  /// 写出栈顶单项并弹栈；返回是否完整写出（Vec 无界，恒真），
  /// 遭遇不可序列化错误时置 `err`。
  pub fn try_write_single_item(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let cur_top = runner.state.get_top() as i32;
    let Some(ret_type) = runner.state.type_name(cur_top) else {
      *err = Some(ConstantStrings::UNEXPECTED_ERROR);
      return false;
    };
    let is_nullish = matches!(ret_type, "nil" | "userdata" | "function" | "thread");

    if is_nullish {
      return if resp.protocol_version == 3 {
        Self::try_write_resp3_null(runner, resp, err)
      } else {
        Self::try_write_resp2_null(runner, resp, err)
      };
    }

    match ret_type {
      "number" => Self::try_write_number(runner, resp, err),
      "string" => Self::try_write_string(runner, resp, err),
      "boolean" => {
        if resp.protocol_version == 3 {
          // RESP3 has a proper boolean type
          Self::try_write_resp3_boolean(runner, resp, err)
        } else {
          // RESP2 booleans are weird: false = null (bulk nil), true = 1
          if runner.state.to_boolean(cur_top) {
            runner.state.pop(1);
            runner.state.push_integer(1);
            Self::try_write_number(runner, resp, err)
          } else {
            Self::try_write_resp2_null(runner, resp, err)
          }
        }
      }
      "table" => {
        // Redis does not respect metatables, so RAW access is ok here

        runner.state.push_constant_string(ConstantStrings::DOUBLE);
        let is_double =
          runner.state.raw_get_top(cur_top) && runner.state.type_name(-1) == Some("number");
        if is_double {
          let fit = if resp.protocol_version == 3 {
            Self::try_write_double(runner, resp, err)
          } else {
            // Force double to string for RESP2
            if !runner.state.try_number_to_string() {
              *err = Some(ConstantStrings::OUT_OF_MEMORY);
              return false;
            }
            Self::try_write_string(runner, resp, err)
          };
          // Remove table from stack
          runner.state.pop(1);
          return fit;
        }
        // Remove whatever we read from the table under the "double" key
        runner.state.pop(1);

        runner.state.push_constant_string(ConstantStrings::MAP);
        let is_map =
          runner.state.raw_get_top(cur_top) && runner.state.type_name(-1) == Some("table");
        if is_map {
          let fit = if resp.protocol_version == 3 {
            Self::try_write_map(runner, resp, err)
          } else {
            Self::try_write_map_to_array(runner, resp, err)
          };
          // remove table from stack
          runner.state.pop(1);
          return fit;
        }
        runner.state.pop(1);

        runner.state.push_constant_string(ConstantStrings::SET);
        let is_set =
          runner.state.raw_get_top(cur_top) && runner.state.type_name(-1) == Some("table");
        if is_set {
          let fit = if resp.protocol_version == 3 {
            Self::try_write_set(runner, resp, err)
          } else {
            Self::try_write_set_to_array(runner, resp, err)
          };
          // remove table from stack
          runner.state.pop(1);
          return fit;
        }
        runner.state.pop(1);

        // If the key "ok" is in there, we need to short circuit
        runner.state.push_constant_string(ConstantStrings::OK_LOWER);
        let is_ok =
          runner.state.raw_get_top(cur_top) && runner.state.type_name(-1) == Some("string");
        if is_ok {
          let fit = Self::try_write_string(runner, resp, err);
          // Remove table from stack
          runner.state.pop(1);
          return fit;
        }
        runner.state.pop(1);

        // If the key "err" is in there, we need to short circuit
        runner.state.push_constant_string(ConstantStrings::ERR);
        let is_err =
          runner.state.raw_get_top(cur_top) && runner.state.type_name(-1) == Some("string");
        if is_err {
          let fit = Self::try_write_error(runner, resp, err);
          // Remove table from stack
          runner.state.pop(1);
          return fit;
        }
        runner.state.pop(1);

        // Map this table to an array
        Self::try_write_table_to_array(runner, resp, err)
      }
      _ => {
        // All types should have been handled
        *err = Some(ConstantStrings::UNEXPECTED_ERROR);
        false
      }
    }
  }

  /// libs/server/Lua/LuaRunner.cs:TryWriteResp2Null
  pub fn try_write_resp2_null(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let _ = err;
    resp.write_null();
    runner.state.pop(1);
    true
  }

  /// libs/server/Lua/LuaRunner.cs:TryWriteResp3Null
  pub fn try_write_resp3_null(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let _ = err;
    resp.write_resp3_null();
    runner.state.pop(1);
    true
  }

  /// libs/server/Lua/LuaRunner.cs:TryWriteNumber
  pub fn try_write_number(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let _ = err;
    let cur_top = runner.state.get_top() as i32;
    // Redis unconditionally converts all "number" replies to integer replies
    let num = runner.state.check_number(cur_top).unwrap_or_default() as i64;
    resp.write_int64(num);
    runner.state.pop(1);
    true
  }

  /// libs/server/Lua/LuaRunner.cs:TryWriteString
  pub fn try_write_string(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let _ = err;
    let cur_top = runner.state.get_top() as i32;
    let buf = runner
      .state
      .known_string_to_buffer(cur_top)
      .unwrap_or_default();
    resp.write_bulk_string(&buf);
    runner.state.pop(1);
    true
  }

  /// libs/server/Lua/LuaRunner.cs:TryWriteResp3Boolean
  pub fn try_write_resp3_boolean(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let _ = err;
    let cur_top = runner.state.get_top() as i32;
    // In RESP3 there is a dedicated boolean type
    resp.write_bool(runner.state.to_boolean(cur_top));
    runner.state.pop(1);
    true
  }

  /// libs/server/Lua/LuaRunner.cs:TryWriteDouble
  pub fn try_write_double(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let _ = err;
    let cur_top = runner.state.get_top() as i32;
    let num = runner.state.check_number(cur_top).unwrap_or_default();
    resp.write_double(num);
    runner.state.pop(1);
    true
  }

  /// libs/server/Lua/LuaRunner.cs:TryWriteMap
  pub fn try_write_map(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let table_ix = runner.state.get_top() as i32;
    let mut map_size = 0usize;

    // Push nil key as "first key"
    runner.state.push_nil();
    while runner.state.next() {
      // Now we have value at top of stack, and key one below it
      map_size += 1;
      // Remove value, we don't need it
      runner.state.pop(1);
    }

    // Write the map header
    resp.write_map_len(map_size);

    // Write the values out by traversing the table again
    runner.state.push_nil();
    while runner.state.next() {
      // Copy key to top of stack
      runner.state.push_value(table_ix + 1);

      // Write (and remove) key out
      if !Self::try_write_single_item(runner, resp, err) && err.is_some() {
        return false;
      }

      // Write (and remove) value out
      if !Self::try_write_single_item(runner, resp, err) && err.is_some() {
        return false;
      }
    }

    // Remove the table
    runner.state.pop(1);
    true
  }

  /// libs/server/Lua/LuaRunner.cs:TryWriteMapToArray
  pub fn try_write_map_to_array(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let table_ix = runner.state.get_top() as i32;
    let mut map_size = 0usize;

    runner.state.push_nil();
    while runner.state.next() {
      map_size += 1;
      runner.state.pop(1);
    }

    let array_size = map_size * 2;

    // Write the array header
    resp.write_array_len(array_size);

    runner.state.push_nil();
    while runner.state.next() {
      runner.state.push_value(table_ix + 1);

      if !Self::try_write_single_item(runner, resp, err) && err.is_some() {
        return false;
      }

      if !Self::try_write_single_item(runner, resp, err) && err.is_some() {
        return false;
      }
    }

    runner.state.pop(1);
    true
  }

  /// libs/server/Lua/LuaRunner.cs:TryWriteSet
  pub fn try_write_set(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let table_ix = runner.state.get_top() as i32;
    let mut set_size = 0usize;

    runner.state.push_nil();
    while runner.state.next() {
      set_size += 1;
      runner.state.pop(1);
    }

    // Write the set header
    resp.write_set_len(set_size);

    runner.state.push_nil();
    while runner.state.next() {
      // Remove the value, it's ignored
      runner.state.pop(1);

      // Make a copy of the key
      runner.state.push_value(table_ix + 1);

      // Write (and remove) key copy out
      if !Self::try_write_single_item(runner, resp, err) && err.is_some() {
        return false;
      }
    }

    runner.state.pop(1);
    true
  }

  /// libs/server/Lua/LuaRunner.cs:TryWriteSetToArray
  pub fn try_write_set_to_array(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let table_ix = runner.state.get_top() as i32;
    let mut set_size = 0usize;

    runner.state.push_nil();
    while runner.state.next() {
      set_size += 1;
      runner.state.pop(1);
    }

    resp.write_array_len(set_size);

    runner.state.push_nil();
    while runner.state.next() {
      runner.state.pop(1);
      runner.state.push_value(table_ix + 1);

      if !Self::try_write_single_item(runner, resp, err) && err.is_some() {
        return false;
      }
    }

    runner.state.pop(1);
    true
  }

  /// libs/server/Lua/LuaRunner.cs:TryWriteError
  pub fn try_write_error(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let _ = err;
    let cur_top = runner.state.get_top() as i32;
    let err_buff = runner
      .state
      .known_string_to_buffer(cur_top)
      .unwrap_or_default();
    resp.write_error(&err_buff);
    runner.state.pop(1);
    true
  }

  /// libs/server/Lua/LuaRunner.cs:TryWriteTableToArray
  pub fn try_write_table_to_array(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let table_top = runner.state.get_top() as i32;

    // Lua # operator - this MAY stop at nils (raw length)
    let max_len = runner.state.raw_len(table_top) as usize;

    // Find the TRUE length by scanning for nils
    let mut true_len = 0usize;
    while true_len < max_len {
      let is_nil = if runner.state.raw_get_integer(table_top, true_len as i64 + 1) {
        runner.state.type_name(-1) == Some("nil")
      } else {
        true
      };
      runner.state.pop(1);

      if is_nil {
        break;
      }
      true_len += 1;
    }

    resp.write_array_len(true_len);

    for i in 1..=true_len {
      // Push item at index i onto the stack
      _ = runner.state.raw_get_integer(table_top, i as i64);

      // Write the item out, removing it from the stack
      if !Self::try_write_single_item(runner, resp, err) && err.is_some() {
        return false;
      }
    }

    // Remove the table
    runner.state.pop(1);
    true
  }

  /// libs/server/Lua/LuaRunner.cs:TryProbeSupport
  ///
  /// 探测 VM 可用性（C# 经 NativeMethods.Version P/Invoke）。
  pub fn try_probe_support() -> Result<(), String> {
    let mut state = LuaStateWrapper::new();
    state
      .load_string("return 42")
      .and_then(|_| state.pcall_n(0, 1))
      .map_err(|e| super::lua_state_wrapper::error_message(&e))?;
    if state.check_number(-1) == Some(42.0) {
      Ok(())
    } else {
      Err("Lua VM did not return expected value".into())
    }
  }

  /// libs/server/Lua/LuaRunner.cs:InitializeNoScriptDetails
  ///
  /// 构建 NoScript 命令位图（start, bitmap）。resp 域 RespCommandsInfo 为
  /// 并行转写域：此处以 NoScript 命令名字典序索引承接位图形状，待 resp
  /// 域就绪后切换为 RespCommand 枚举位图。
  pub fn initialize_no_script_details() -> (i32, Vec<u64>) {
    const NO_SCRIPT_COMMANDS: &[&str] = &[
      "EVAL",
      "EVALSHA",
      "EVALSHA_RO",
      "EVAL_RO",
      "FCALL",
      "FCALL_RO",
      "FLUSHALL",
      "FLUSHDB",
      "FUNCTION",
      "PSUBSCRIBE",
      "SCRIPT",
      "SUBSCRIBE",
      "SWAPDB",
    ];

    let start = 0i32;
    let bits = u64::BITS as usize;
    let mut bitmap = vec![0u64; NO_SCRIPT_COMMANDS.len().div_ceil(bits).max(1)];
    for (index, member) in NO_SCRIPT_COMMANDS.iter().enumerate() {
      bitmap[index / bits] |= 1u64 << (index % bits);
      let _ = member;
    }

    (start, bitmap)
  }

  /// 脚本 SHA1 摘要键（SCRIPT/EVAL 调度路径使用）。
  pub fn script_digest(&self) -> ScriptHashKey {
    SessionScriptCache::get_script_digest(&self.source)
  }

  /// 宿主共享态访问（LuaCommands 调度入口）。
  pub fn host_mut(&mut self) -> &mut HostShared {
    &mut self.host
  }

  /// 脚本源码。
  pub fn source(&self) -> &[u8] {
    &self.source
  }

  /// redis.log 行为。
  pub fn log_mode(&self) -> LuaLoggingMode {
    self.log_mode
  }
}

/// 直接写 RESP error 到输出缓冲（compile 路径）。
fn resp_out_error(out: &mut Vec<u8>, msg: &[u8]) {
  out.clear();
  out.push(b'-');
  out.extend_from_slice(msg);
  out.extend_from_slice(b"\r\n");
}

/// RESP `:<int>\r\n` 解析。
fn read_resp_int(cursor: &mut &[u8]) -> Option<i64> {
  let end = cursor.iter().position(|&b| b == b'\r')?;
  if cursor.get(end + 1) != Some(&b'\n') {
    return None;
  }
  let text = str::from_utf8(&cursor[1..end]).ok()?;
  let value = text.parse().ok()?;
  *cursor = &cursor[end + 2..];
  Some(value)
}

/// 查找 CRLF 位置。
fn find_crlf(data: &[u8]) -> Option<usize> {
  data.windows(2).position(|w| w == b"\r\n")
}

/// LuaWrappedError 栈视图形态（宿主回调侧入口）。
pub fn lua_wrapped_error_view(
  state: &mut LuaStateWrapper,
  non_error_returns: usize,
  error_msg: &[u8],
) -> i32 {
  state.clear_stack();
  for _ in 0..non_error_returns {
    state.push_nil();
  }

  if !state.try_push_buffer(error_msg) {
    // Don't have enough space to provide the actual error, which is itself an OOM error
    return lua_wrapped_error_view(state, non_error_returns, ConstantStrings::OUT_OF_MEMORY);
  }

  (non_error_returns + 1) as i32
}

/// libs/server/Lua/LuaRunner.cs:ProcessRespResponse（栈视图形态）
pub fn process_resp_response_view(
  state: &mut LuaStateWrapper,
  resp_protocol_version: u8,
  resp: &[u8],
) -> i32 {
  let mut cursor = resp;
  let ret = process_single_resp_term_view(state, resp_protocol_version, &mut cursor);

  if !cursor.is_empty() {
    log::error!("RESP3 Response not fully consumed, this should never happen");
    return lua_wrapped_error_view(state, 1, ConstantStrings::UNEXPECTED_ERROR);
  }

  ret
}

/// libs/server/Lua/LuaRunner.cs:ProcessSingleRespTerm（栈视图形态）
pub fn process_single_resp_term_view(
  state: &mut LuaStateWrapper,
  resp_protocol_version: u8,
  cursor: &mut &[u8],
) -> i32 {
  let Some(&indicator) = cursor.first() else {
    log::error!("Unexpected response, this should never happen");
    return lua_wrapped_error_view(state, 1, ConstantStrings::UNEXPECTED_ERROR);
  };

  match indicator {
    // Simple reply (Common)
    b'+' => {
      *cursor = &cursor[1..];
      let mut result_span: &[u8] = &[];
      if matches!(try_read_as_span(&mut result_span, cursor), Ok(true)) {
        // Construct a table = { 'ok': value }
        if !state.try_create_table(0, 1) {
          *cursor = &[];
          return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
        }
        state.push_constant_string(ConstantStrings::OK_LOWER);

        if !state.try_push_buffer(result_span) {
          return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
        }

        state.raw_set(1);
        return 1;
      }
      default_resp_term_view(state)
    }

    // Integer (Common)
    b':' => {
      if let Some(number) = read_resp_int(cursor) {
        state.push_integer(number);
        return 1;
      }
      default_resp_term_view(state)
    }

    // Error (Common)
    b'-' => {
      *cursor = &cursor[1..];
      let mut err_span: &[u8] = &[];
      if matches!(try_read_as_span(&mut err_span, cursor), Ok(true)) {
        if err_span == ConstantStrings::RESP_ERR_GENERIC_UNK_CMD {
          // Gets a special response
          return lua_wrapped_error_view(state, 1, ConstantStrings::ERR_UNKNOWN);
        }

        return lua_wrapped_error_view(state, 1, err_span);
      }
      default_resp_term_view(state)
    }

    // Bulk string or null bulk string (Common)
    b'$' => {
      // "$-1\r\n" → RESP2 null bulk → false
      if cursor.len() >= 5 && &cursor[1..5] == b"-1\r\n" {
        // Bulk null strings are mapped to FALSE
        state.push_boolean(false);
        *cursor = &cursor[5..];
        return 1;
      }
      let mut bulk_span: &[u8] = &[];
      if matches!(
        try_read_span_with_length_header(&mut bulk_span, cursor),
        Ok(true)
      ) {
        if !state.try_push_buffer(bulk_span) {
          return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
        }
        return 1;
      }
      default_resp_term_view(state)
    }

    // Array (Common)
    b'*' => {
      let mut array_item_count = 0i32;
      if matches!(
        try_read_signed_array_length(&mut array_item_count, cursor),
        Ok(true)
      ) {
        if array_item_count == -1 {
          state.push_boolean(false);
        } else {
          let count = array_item_count as usize;
          if !state.try_create_table(count, 0) {
            *cursor = &[];
            return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
          }
          let table_index = state.get_top() as i32;

          for item_ix in 0..count {
            // Pushes the item to the top of the stack
            _ = process_single_resp_term_view(state, resp_protocol_version, cursor);

            // Store the item into the table
            if let Some(value) = state.pop_value() {
              state.raw_set_integer(table_index, item_ix as i64 + 1, value);
            }
          }
        }

        return 1;
      }
      default_resp_term_view(state)
    }

    // Map (RESP3 only)
    b'%' if resp_protocol_version == 3 => {
      let mut map_pair_count = 0i32;
      if matches!(
        try_read_signed_map_length(&mut map_pair_count, cursor),
        Ok(true)
      ) && map_pair_count >= 0
      {
        // Response is a two level table, where { map = { ... } }
        if !state.try_create_table(0, 1) {
          *cursor = &[];
          return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
        }
        let parent_index = state.get_top() as i32;

        state.push_constant_string(ConstantStrings::MAP);
        if !state.try_create_table(0, map_pair_count as usize) {
          *cursor = &[];
          return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
        }
        let sub_index = parent_index + 2;

        for _ in 0..map_pair_count {
          // Read key
          _ = process_single_resp_term_view(state, resp_protocol_version, cursor);
          // Read value
          _ = process_single_resp_term_view(state, resp_protocol_version, cursor);

          // Set t[k] = v
          state.raw_set(sub_index);
        }

        // Store the sub-table into the parent table
        state.raw_set(parent_index);
        return 1;
      }
      default_resp_term_view(state)
    }

    // Null (RESP3 only)
    b'_' if resp_protocol_version == 3 => {
      if cursor.len() >= 3 && &cursor[1..3] == b"\r\n" {
        *cursor = &cursor[3..];
        state.push_nil();
        return 1;
      }
      default_resp_term_view(state)
    }

    // Set (RESP3 only)
    b'~' if resp_protocol_version == 3 => {
      let mut set_item_count = 0i32;
      if matches!(
        try_read_signed_set_length(&mut set_item_count, cursor),
        Ok(true)
      ) && set_item_count >= 0
      {
        // Response is a two level table, where { set = { ... } }
        if !state.try_create_table(0, 1) {
          *cursor = &[];
          return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
        }
        let parent_index = state.get_top() as i32;

        state.push_constant_string(ConstantStrings::SET);
        if !state.try_create_table(0, set_item_count as usize) {
          *cursor = &[];
          return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
        }
        let sub_index = parent_index + 2;

        for _ in 0..set_item_count {
          // Read value, which we use as key
          _ = process_single_resp_term_view(state, resp_protocol_version, cursor);
          // Unconditionally the value under the key is true
          state.push_boolean(true);

          // Set t[value] = true
          state.raw_set(sub_index);
        }

        // Store the sub-table into the parent table
        state.raw_set(parent_index);
        return 1;
      }
      default_resp_term_view(state)
    }

    // Boolean (RESP3 only)
    b'#' if resp_protocol_version == 3 => {
      if cursor.len() >= 4 {
        let as_int = &cursor[0..4];
        if as_int == b"#t\r\n" {
          *cursor = &cursor[4..];
          state.push_boolean(true);
          return 1;
        } else if as_int == b"#f\r\n" {
          *cursor = &cursor[4..];
          state.push_boolean(false);
          return 1;
        }
      }
      default_resp_term_view(state)
    }

    // Double (RESP3 only)
    b',' if resp_protocol_version == 3 => {
      if let Some(end_of_double_ix) = find_crlf(cursor) {
        let double_span = &cursor[..end_of_double_ix + 2];
        let body = &double_span[1..double_span.len() - 2];
        let parsed = match body {
          b"inf" => Some(f64::INFINITY),
          b"nan" => Some(f64::NAN),
          b"-inf" => Some(f64::NEG_INFINITY),
          b"-nan" => Some(f64::NAN),
          text => str::from_utf8(text).ok().and_then(|t| t.parse().ok()),
        };
        if let Some(parsed) = parsed {
          *cursor = &cursor[double_span.len()..];

          // Create table like { double = <parsed> }
          if !state.try_create_table(0, 1) {
            *cursor = &[];
            return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
          }
          state.push_constant_string(ConstantStrings::DOUBLE);
          state.push_number(parsed);
          state.raw_set(1);
          return 1;
        }
      }
      default_resp_term_view(state)
    }

    // Big number (RESP3 only)
    b'(' if resp_protocol_version == 3 => {
      if let Some(end_of_big_num) = find_crlf(cursor) {
        let big_num_span = &cursor[..end_of_big_num + 2];
        if big_num_span.len() >= 4 {
          let big_num_buf = &big_num_span[1..big_num_span.len() - 2];
          if big_num_buf.iter().all(u8::is_ascii_digit) {
            *cursor = &cursor[big_num_span.len()..];

            // Create table like { big_number = <bigNumBuf> }
            if !state.try_create_table(0, 1) {
              *cursor = &[];
              return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
            }
            state.push_constant_string(ConstantStrings::BIG_NUMBER);

            if !state.try_push_buffer(big_num_buf) {
              *cursor = &[];
              return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
            }

            state.raw_set(1);
            return 1;
          }
        }
      }
      default_resp_term_view(state)
    }

    // Verbatim strings (RESP3 only)
    b'=' if resp_protocol_version == 3 => {
      let mut verbatim_string_length = 0i32;
      if matches!(
        try_read_verbatim_string_length(&mut verbatim_string_length, cursor),
        Ok(true)
      ) && verbatim_string_length >= 4
      {
        let verbatim = verbatim_string_length as usize;
        if cursor.len() >= verbatim + 2 {
          let format = cursor[0..3].to_vec();
          let data = cursor[4..verbatim].to_vec();

          let advanced = *cursor;
          *cursor = &cursor[verbatim..];
          if &cursor[0..2] != b"\r\n" {
            *cursor = advanced;
            return default_resp_term_view(state);
          }
          *cursor = &cursor[2..];

          // create table like { format = <format>, string = <data> }
          if !state.try_create_table(0, 2) {
            *cursor = &[];
            return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
          }

          state.push_constant_string(ConstantStrings::FORMAT);
          if !state.try_push_buffer(&format) {
            return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
          }
          state.raw_set(1);

          state.push_constant_string(ConstantStrings::STRING);
          if !state.try_push_buffer(&data) {
            return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
          }
          state.raw_set(1);

          return 1;
        }
      }
      default_resp_term_view(state)
    }

    _ => default_resp_term_view(state),
  }
}

/// default 分支（意外响应 → UnexpectedError）。
fn default_resp_term_view(state: &mut LuaStateWrapper) -> i32 {
  log::error!("Unexpected response, this should never happen");
  lua_wrapped_error_view(state, 1, ConstantStrings::UNEXPECTED_ERROR)
}

/// 栈值辅助面（回调蹦床参数入口）。
impl LuaStateWrapper {
  /// libs/server/Lua/LuaStateWrapper.cs:PushStackValue（回调参数压栈）
  pub fn push_stack_value(&mut self, value: mlua::Value) {
    self.interp_mut().stack.push(value);
  }

  /// 弹出栈顶值。
  pub fn pop_value(&mut self) -> Option<mlua::Value> {
    self.interp_mut().stack.pop()
  }

  /// 弹出 `count` 个值（自底向上序）。
  pub fn pop_values(&mut self, count: i32) -> Vec<mlua::Value> {
    if count <= 0 {
      return Vec::new();
    }
    let mut interp = self.interp_mut();
    let start = interp.stack.len().saturating_sub(count as usize);
    interp.stack.split_off(start)
  }
}
