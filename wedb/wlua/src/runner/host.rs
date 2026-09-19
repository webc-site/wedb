//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs
//!
//! 宿主回调共享态与会话指针（lock_guard 域）：C# `this` 可变部分的 Rust
//! 形态——thread-local 上下文、RAII 回调守卫与静态函数表会话指针。

use std::marker::PhantomData;

use wresp::command::RespCommand;
use wtxn::{txn_key_entry::TxnKeyEntries, txn_lock_table::TxnLockTable};

use crate::{LuaState, api::ScriptingApi, options::LuaLoggingMode};

/// 宿主回调共享态：C# `this` 中除 LuaState 外的可变部分
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
  /// RESP 请求拼装缓冲（C# scratchBufferBuilder 的命令拼装面；
  /// 拼装经 wresp::resp_memory_writer::RespWriter 直写，clear 保留容量承接 C# Reset 零分配复用）。
  pub scratch: Vec<u8>,
  /// redis.call 响应接收缓冲（脚本窗口内复用，消费后即清，容量保留）。
  pub response: Vec<u8>,
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
  /// 构造（`lock_table` = 所属引擎实例锁表句柄，对标 C# `LuaRunner` 经
  /// `storeWrapper` 取该 store 的 `LockTable`）
  pub fn new(log_mode: LuaLoggingMode, txn_mode: bool, lock_table: TxnLockTable) -> Self {
    Self {
      function_registry_index: -1,
      log_mode,
      txn_mode,
      txn_key_entries: TxnKeyEntries::new(16, lock_table),
      session: None,
      scratch: Vec::new(),
      response: Vec::new(),
      preamble_keys: None,
      preamble_argv: None,
      preamble_args: Vec::new(),
      preamble_key_and_argv_count: 0,
      preamble_n_keys: 0,
      needs_dispose: false,
    }
  }
}

/// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:SetCallbackContext
///
/// 设置回调期间可用的宿主上下文（仅同一调用线程内有效）。
pub fn set_callback_context(context: *mut HostShared) {
  // SAFETY：指针在回调窗口内有效（窗口结束即清）；wlua 侧仅作指针转存。
  unsafe { crate::set_callback_context(context.cast()) };
}

/// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:ClearCallbackContext
pub fn clear_callback_context(context: *mut HostShared) {
  // SAFETY：与 set_callback_context 配对（窗口守卫保证）。
  unsafe { crate::clear_callback_context(context.cast()) };
}

/// 取当前回调上下文（未设置即程序性错误，以 panic 上抛为 Lua 错误）。
pub fn callback_context() -> *mut HostShared {
  crate::callback_context().cast()
}

/// 回调上下文守卫（对标 C# SetCallbackContext / finally ClearCallbackContext 配对）。
///
/// C# 在 CompileFor*/RunFor* 期间把 `this` 挂进 ThreadStatic 槽供 trampoline
/// 取回；Rust 侧以 RAII 守卫保证 panic 路径也能清槽。
pub(super) struct CallbackGuard(*mut HostShared);

impl CallbackGuard {
  /// 进入回调上下文窗口。
  pub(super) fn enter(host: *mut HostShared) -> Self {
    let ptr: *mut HostShared = host;
    set_callback_context(ptr);
    Self(ptr)
  }
}

impl Drop for CallbackGuard {
  fn drop(&mut self) {
    clear_callback_context(self.0);
    // SAFETY：回调窗口退出时清空会话裸指针，防止异常展开导致指针悬垂。
    unsafe {
      (*self.0).session = None;
    }
  }
}

type VtableGetFn = unsafe fn(*mut (), &[u8]) -> Result<Option<Vec<u8>>, &'static str>;
type VtableSetFn = unsafe fn(*mut (), &[u8], &[u8]) -> Result<(), &'static str>;

/// 脚本命令面函数表（静态函数指针，彻底消除动态分发 ScriptingApi）
pub struct ScriptingApiVtable {
  dispatch_resp: unsafe fn(*mut (), &[u8], &mut Vec<u8>),
  get: VtableGetFn,
  set: VtableSetFn,
  resp_protocol_version: unsafe fn(*const ()) -> u8,
  update_resp_protocol_version: unsafe fn(*mut (), u8),
  parse_resp_command_buffer: unsafe fn(*mut (), &[u8]) -> Option<RespCommand>,
  check_acl_permissions: unsafe fn(*const (), RespCommand) -> bool,
  set_transaction_mode: unsafe fn(*mut (), bool),
  begin_transaction: unsafe fn(*mut ()),
  end_transaction: unsafe fn(*mut ()),
}

impl ScriptingApiVtable {
  /// 为具体类型 `S` 构建静态函数表
  pub const fn of<S: ScriptingApi>() -> Self {
    Self {
      dispatch_resp: |ptr, req, response| unsafe {
        (*ptr.cast::<S>()).dispatch_resp(req, response)
      },
      get: |ptr, key| unsafe { (*ptr.cast::<S>()).get(key) },
      set: |ptr, key, val| unsafe { (*ptr.cast::<S>()).set(key, val) },
      resp_protocol_version: |ptr| unsafe { (*ptr.cast::<S>()).resp_protocol_version() },
      update_resp_protocol_version: |ptr, ver| unsafe {
        (*ptr.cast::<S>()).update_resp_protocol_version(ver)
      },
      parse_resp_command_buffer: |ptr, buf| unsafe {
        (*ptr.cast::<S>()).parse_resp_command_buffer(buf)
      },
      check_acl_permissions: |ptr, cmd| unsafe { (*ptr.cast::<S>()).check_acl_permissions(cmd) },
      set_transaction_mode: |ptr, en| unsafe { (*ptr.cast::<S>()).set_transaction_mode(en) },
      begin_transaction: |ptr| unsafe { (*ptr.cast::<S>()).begin_transaction() },
      end_transaction: |ptr| unsafe { (*ptr.cast::<S>()).end_transaction() },
    }
  }
}

struct VtableOf<S>(PhantomData<fn() -> S>);
impl<S: ScriptingApi> VtableOf<S> {
  const VTABLE: ScriptingApiVtable = ScriptingApiVtable::of::<S>();
}

/// 会话指针（生存期擦除形态：仅回调窗口内解引用，窗口外恒 None）。
#[derive(Clone, Copy)]
pub struct ScriptSessionPtr {
  ptr: *mut (),
  vtable: &'static ScriptingApiVtable,
}

impl ScriptSessionPtr {
  /// 从带生存期的会话引用构造（窗口内使用，窗口结束即清除）。
  ///
  /// SAFETY（调用方）：窗口结束后不得再解引用。
  pub(crate) fn erase<S: ScriptingApi>(session: &mut S) -> Self {
    Self {
      ptr: (session as *mut S).cast(),
      vtable: &VtableOf::<S>::VTABLE,
    }
  }

  /// 解引用（回调窗口内）。
  pub(crate) fn get(&mut self) -> ScriptSessionRef<'_> {
    ScriptSessionRef {
      ptr: self.ptr,
      vtable: self.vtable,
    }
  }
}

/// 会话命令面借用视图
pub struct ScriptSessionRef<'a> {
  ptr: *mut (),
  vtable: &'a ScriptingApiVtable,
}

impl ScriptingApi for ScriptSessionRef<'_> {
  fn dispatch_resp(&mut self, request: &[u8], response: &mut Vec<u8>) {
    unsafe { (self.vtable.dispatch_resp)(self.ptr, request, response) }
  }

  fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, &'static str> {
    unsafe { (self.vtable.get)(self.ptr, key) }
  }

  fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), &'static str> {
    unsafe { (self.vtable.set)(self.ptr, key, value) }
  }

  fn resp_protocol_version(&self) -> u8 {
    unsafe { (self.vtable.resp_protocol_version)(self.ptr) }
  }

  fn update_resp_protocol_version(&mut self, version: u8) {
    unsafe { (self.vtable.update_resp_protocol_version)(self.ptr, version) }
  }

  fn parse_resp_command_buffer(&mut self, buffer: &[u8]) -> Option<RespCommand> {
    unsafe { (self.vtable.parse_resp_command_buffer)(self.ptr, buffer) }
  }

  fn check_acl_permissions(&self, command: RespCommand) -> bool {
    unsafe { (self.vtable.check_acl_permissions)(self.ptr, command) }
  }

  fn set_transaction_mode(&mut self, enabled: bool) {
    unsafe { (self.vtable.set_transaction_mode)(self.ptr, enabled) }
  }

  fn begin_transaction(&mut self) {
    unsafe { (self.vtable.begin_transaction)(self.ptr) }
  }

  fn end_transaction(&mut self) {
    unsafe { (self.vtable.end_transaction)(self.ptr) }
  }
}

/// 宿主函数通用形态：操作栈镜像 + 宿主上下文，返回栈上结果数。
pub type HostFn = fn(&mut LuaState, &mut HostShared) -> i32;
