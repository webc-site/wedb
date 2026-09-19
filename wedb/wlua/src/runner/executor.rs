//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs
//!
//! 编译与执行域：脚本装载、preamble 装配（KEYS/ARGV）、事务包络与
//! RunCommon 执行主循环（对标 LuaRunner.cs Compile/Run 部分）。

use std::{mem, sync::Arc};

use wbase::num::strict_i32;
use wresp::{read::try_read_error_as_span, resp_memory_writer::RespWriter};
use wtxn::{TxnKeyEntryComparison, txn_key_entry::LockType};

use super::{
  ERR_LUA_INVOKE_FAILED, ERR_PREFIX, ERR_UNEXPECTED_RESPONSE, LuaRunner, RespObject, RespOut,
  ScriptSessionPtr, host::CallbackGuard, resp_convert::resp_out_error,
};
use crate::{Error, api::ScriptingApi, state::Deadline, strings::ConstantStrings};

impl LuaRunner {
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:CompileForRunner
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:CompileForSession
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:CompileCommon
  ///
  /// 编译脚本：经 load_sandboxed 装载，(err, func) 双值返回。
  fn compile_common(&mut self, out: &mut Vec<u8>) {
    debug_assert_eq!(
      self.host.function_registry_index, -1,
      "Shouldn't compile multiple times"
    );

    if !self.state.push_ref(self.load_sandboxed_registry_index) {
      resp_out_error(out, ConstantStrings::OUT_OF_MEMORY);
      return;
    }
    self.state.push_buffer(&self.source);

    let call_res = self.state.pcall_n(1, 2);

    // On success the stack will have two things on it:
    //  1. The error (nil if not error)
    //  2. The function (nil if error)

    if call_res.is_ok() && self.state.get_top() == 2 && !self.state.to_boolean(1) {
      // No error, success!（引用 id <= 0 = nil/NOREF，即注册失败）
      let index = self.state.try_ref();
      if index > 0 {
        self.host.function_registry_index = index;
      } else {
        // Uh-oh, couldn't save the function under the registry
        resp_out_error(out, ConstantStrings::OUT_OF_MEMORY);
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
      RespWriter::new_ref(out).write_error(&err_str);
    }

    // C# 形态中 CompileCommon 以 C 函数返回 0，帧内栈槽随帧丢弃；
    // Rust 栈镜像持久，需显式清空。
    self.state.clear_stack();
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:RunForSession
  ///
  /// 以会话参数（numkeys 开头）执行已编译函数，响应写入 `out`。
  pub fn run_for_session<S: ScriptingApi>(
    &mut self,
    args: &[Vec<u8>],
    session: &mut S,
    out: &mut Vec<u8>,
  ) {
    // C# RunForSession(count)：count = parseState.Count - 1（去 script 位），
    // 即本切片长度（numkeys + keys... + argv...）。
    self.host.preamble_key_and_argv_count = args.len() as i32;
    self.host.session = Some(ScriptSessionPtr::erase(session));

    let _guard = CallbackGuard::enter(self.host.as_mut());

    // Every invocation starts in RESP2
    session.update_resp_protocol_version(2);

    // C# ResetTimeout 的清残留语义由调用方 arm/disarm 承接（commands.rs
    // try_execute_script：arm 覆盖写截止、结束 disarm 清 0），此处不再触碰。

    // preamble：装配 KEYS/ARGV（C# 经 RunPreambleForSession C 函数），
    // 随后执行已编译函数 —— redis.call 回调经窗口上下文访问会话面
    // （窗口已由 CallbackGuard 安装）。
    let preamble_res = self.run_preamble_for_session_slice(args);

    if let Err(err) = preamble_res {
      // err 为 preamble 的 `&'static [u8]` 常量文案；仍走字节入参口，
      // 清洗由 wresp 错误帧唯一成帧点保证
      self.host.session = None;
      let resp = RespOut::session(out, 2);
      RespWriter::new_ref(resp.buf).write_error_bytes(err);
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:RunForRunner
  ///
  /// 以 (keys, argv) 执行并把响应解析为对象（宿主形态）。
  pub fn run_for_runner(
    &mut self,
    keys: Option<Vec<Vec<u8>>>,
    argv: Option<Vec<Vec<u8>>>,
  ) -> Result<RespObject, String> {
    let _guard = CallbackGuard::enter(self.host.as_mut());

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

  /// runner 模式 preamble：重置并灌入 KEYS/ARGV（对应 UnsafeRunPreambleForRunner 实现）
  pub fn run_preamble_for_runner(&mut self) -> Result<(), String> {
    let keys = self.host.preamble_keys.clone().unwrap_or_default();
    let argv = self.host.preamble_argv.clone().unwrap_or_default();

    if self.try_reset_parameters(keys.len(), argv.len()).is_err() {
      self.host.needs_dispose = true;
      return Err(
        String::from_utf8_lossy(ConstantStrings::PARAMETER_RESET_FAILED_OTHER).into_owned(),
      );
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

  /// session 模式 preamble（切片直接借用，零克隆）：`args[0]` 为 numkeys，随后 KEYS*、ARGV*
  pub fn run_preamble_for_session_slice(&mut self, args: &[Vec<u8>]) -> Result<(), &'static [u8]> {
    let mut offset = 1usize;
    let n_keys = args.first().and_then(|k| strict_i32(k)).unwrap_or_default();
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
      let keys = &args[offset..end];
      if self.txn_mode {
        // C# LuaKeys/GetKeysAndArguments 走 TxnKeyManager.AddKey(GetKeyHash(key))：
        // 入的是锁条带哈希而非集群槽位（旧路以 CRC16 槽充当，属错用；库级
        // 定槽下槽位与键无关，更不可作键哈希）。锁表登记面统一走
        // TxnKeyEntryComparison::key_hash 单点
        for key in keys {
          self
            .host
            .txn_key_entries
            .add_key(TxnKeyEntryComparison::key_hash(key), LockType::Exclusive);
        }
      }
      self.fill_array_global(ConstantStrings::KEYS, keys);
      self.key_length = n_keys as usize;

      offset = end;
    }

    if n_argv > 0 {
      if self.argv_arr_capacity < n_argv as usize && !self.try_recreate_argv(n_argv as usize) {
        return Err(ConstantStrings::OUT_OF_MEMORY);
      }

      let end = (offset + n_argv as usize).min(args.len());
      let argv = &args[offset..end];
      self.fill_array_global(ConstantStrings::ARGV, argv);
      self.argv_length = n_argv as usize;
    }

    Ok(())
  }

  /// session 模式 preamble：`args[0]` 为 numkeys，随后 KEYS*、ARGV*（对应 UnsafeRunPreambleForSession 实现）
  pub fn run_preamble_for_session(&mut self) -> Result<(), &'static [u8]> {
    let args = mem::take(&mut self.host.preamble_args);
    self.run_preamble_for_session_slice(&args)
  }

  /// 装配 sandbox_env.<KEYS|ARGV> 数组全局（preamble 共用形态）。
  fn fill_array_global(&mut self, name: &[u8], values: &[Vec<u8>]) {
    // 取目标数组表压栈（注册表项由本 runner 创建，恒为表）。
    _ = self.state.push_ref(self.sandbox_env_registry_index);
    let sandbox_at = self.state.get_top() as i32;
    self.state.push_buffer(name);
    self.state.raw_get(sandbox_at);
    self.state.remove(sandbox_at);

    for (i, value) in values.iter().enumerate() {
      // equivalent to KEYS[i+1] = value（值压栈后随 raw_set_integer 弹出）
      self.state.push_buffer(value);
      self.state.raw_set_integer(1, i as i64 + 1);
    }

    // Remove 数组表
    self.state.pop(1);
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:RunInTransaction
  ///
  /// 事务窗口内执行 RunCommon（键锁经 TxnKeyEntries，事务面经会话）。
  fn run_in_transaction(&mut self, resp: &mut RespOut) {
    if let Some(session) = self.host.session.as_mut() {
      let mut session = session.get();
      session.begin_transaction();
      session.set_transaction_mode(true);
    }
    self.host.txn_key_entries.lock_all_keys();

    self.run_common(resp);

    self.host.txn_key_entries.unlock_all_keys();
    if let Some(session) = self.host.session.as_mut() {
      let mut session = session.get();
      session.set_transaction_mode(false);
      session.end_transaction();
    }
  }

  /// 换挂会话共享截止槽（超时管理器登记形态；C# RequestTimeout 的
  /// sethook 落点由 luau VM safepoint 中断回调承接）。
  ///
  /// run 开始前由会话缓存调用（arm_timeout）：槽值写入与到期激活见
  /// timeout.rs（tick 线程 CAS）与本 state 层中断回调。
  pub fn hook_shared_deadline(&mut self, slot: Arc<Deadline>) {
    self.state.hook_shared_deadline(slot);
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryResetParameters
  fn try_reset_parameters(&mut self, n_keys: usize, n_args: usize) -> Result<(), crate::Error> {
    if self.key_length > n_keys || self.argv_length > n_args {
      if !self.state.push_ref(self.reset_keys_and_argv_registry_index) {
        return Err(Error::Misuse("reset_keys_and_argv ref missing"));
      }

      self.state.push_integer(n_keys as i64 + 1);
      self.state.push_integer(n_args as i64 + 1);

      self.state.pcall(2)?;
    }

    self.key_length = n_keys;
    self.argv_length = n_args;

    Ok(())
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryRecreateKEYS
  fn try_recreate_keys(&mut self, length: usize) -> bool {
    // Get sandbox_env and "KEYS" on the stack
    if !self.state.push_ref(self.sandbox_env_registry_index) {
      return false;
    }
    let sandbox_env_index = self.state.get_top() as i32;
    self.state.push_buffer(ConstantStrings::KEYS);

    // Make new KEYS
    self.state.create_table(length, 0);

    // Save it (existing slot update, no allocation impact)
    self.state.raw_set(sandbox_env_index);

    // Get sandbox_env off the stack
    self.state.pop(1);

    self.keys_arr_capacity = length;
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryRecreateARGV
  fn try_recreate_argv(&mut self, length: usize) -> bool {
    if !self.state.push_ref(self.sandbox_env_registry_index) {
      return false;
    }
    let sandbox_env_index = self.state.get_top() as i32;
    self.state.push_buffer(ConstantStrings::ARGV);

    self.state.create_table(length, 0);
    self.state.raw_set(sandbox_env_index);
    self.state.pop(1);

    self.argv_arr_capacity = length;
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:RunCommon
  ///
  /// 执行已编译函数并写出响应（错误经 RESP error 形态）。
  fn run_common(&mut self, resp: &mut RespOut) {
    // Every invocation starts in RESP2（会话协议版本回置由 RunForSession 完成）。
    if !self.state.push_ref(self.host.function_registry_index) {
      RespWriter::new_ref(resp.buf).write_error_bytes(ERR_LUA_INVOKE_FAILED);
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
        RespWriter::new_ref(resp.buf).write_error_bytes(ERR_LUA_INVOKE_FAILED);
      }
      1 => {
        // PCall will put error in a string
        let Some(err_buf) = self.state.known_string_to_buffer(1) else {
          log::error!("Got an unexpected number of values back from a pcall error");
          RespWriter::new_ref(resp.buf).write_error_bytes(ERR_UNEXPECTED_RESPONSE);
          self.state.clear_stack();
          return;
        };

        // err_buf 是脚本可控文本（error() 载荷 / 运行时 traceback 均可带 CRLF）。
        // 清洗已下沉到 wresp 错误帧唯一成帧点（sanitize_error_bytes：CRLF 切断 +
        // MAX_ERROR_MSG_LEN 帽，按字节边界），故此处不再 `from_utf8_lossy` 预处理，
        // 直接以原始字节经字节入参口成帧，避免静默改写脚本文本字节（对标 C#
        // LuaRunner.cs:RunCommon 的 TryWriteError 单口）
        if err_buf.starts_with(ERR_PREFIX) {
          // Response came back with a ERR, already - just pass it along
          RespWriter::new_ref(resp.buf).write_error_bytes(&err_buf);
        } else {
          // Otherwise, this is probably a Lua error - and those aren't very descriptive
          // So slap some more information in
          let prefix: &[u8] = b"ERR Lua encountered an error: ";
          let mut msg = Vec::with_capacity(prefix.len() + err_buf.len());
          msg.extend_from_slice(prefix);
          msg.extend_from_slice(&err_buf);
          RespWriter::new_ref(resp.buf).write_error_bytes(&msg);
        }

        self.state.pop(1);
      }
      _ => {
        log::error!("Got an unexpected number of values back from a pcall error");
        RespWriter::new_ref(resp.buf).write_error_bytes(ERR_UNEXPECTED_RESPONSE);
        self.state.clear_stack();
      }
    }
    debug_assert!(self.state.expect_lua_stack_empty());
  }
}
