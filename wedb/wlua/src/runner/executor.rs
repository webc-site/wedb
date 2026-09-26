//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs
//!
//! 编译与执行域：脚本装载、preamble 装配（KEYS/ARGV）与
//! RunCommon 执行主循环（对标 LuaRunner.cs Compile/Run 部分）。
//!
//! 执行模型（协程化）：脚本函数不再 `lua_pcall` 同步跑死，改由主线程
//! `lua_newthread` 派生协程 + `lua_resume` 续跑。redis.call 命中阻塞/慢
//! 路径时回调内挂起协程（[`sys::lua_yield`]，让渡标记压协程栈），宿主
//! await 驱动挂起体后以应答转换值续跑——VM 同步绑定内不再有内联收割。

use std::{ffi::c_int, ptr::null_mut, sync::Arc};

use wbase::num::strict_i32;
use wresp::{read::try_read_error_as_span, resp_memory_writer::RespWriter};

use super::{
  ERR_LUA_INVOKE_FAILED, ERR_PREFIX, ERR_UNEXPECTED_RESPONSE, LuaRunner, RespObject, RespOut,
  ScriptSessionPtr, host::CallbackGuard, resp_convert::resp_out_error,
};
use crate::{Error, api::ScriptingApi, state::Deadline, strings::ConstantStrings, sys};

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
  ///
  /// 返回脚本挂起态：`Some(tag)` = redis.call 命中阻塞/慢路径，脚本协程
  /// 挂起中（挂起体已让渡会话侧，宿主 await 驱动后经
  /// [`Self::continue_session`] 续跑）；`None` = 脚本完成或出错（应答已
  /// 写 `out`，runner 可复用）。
  ///
  /// libs/server/Lua/LuaRunner.cs:ResetTimeout（run 前清残留 sethook）的
  /// 等价承接：截止槽在 run 开始 arm / 结束 disarm（覆写语义，见
  /// commands.rs start_execute_script），旧钩子无残留可清，rust 无独立
  /// 清钩子面。
  ///
  /// 收尾守卫（对标 C# LuaRunner.cs:1560-1563 RunCommon finally 的
  /// ExpectLuaStackEmpty 与 Redis evalGenericCommand 收尾的
  /// lua_settop(0)+lua_gc(GCSTEP,1)）：挂起窗口主栈锚（协程线程值）与
  /// 协程帧原样保留，故仅完成/出错路径清栈并步进增量 GC。
  pub fn run_for_session<S: ScriptingApi>(
    &mut self,
    args: &[&[u8]],
    session: &mut S,
    out: &mut Vec<u8>,
  ) -> Option<i32> {
    let phase = self.run_for_session_inner(args, session, out);
    if phase.is_none() {
      self.finish_run();
    }
    phase
  }

  fn run_for_session_inner<S: ScriptingApi>(
    &mut self,
    args: &[&[u8]],
    session: &mut S,
    out: &mut Vec<u8>,
  ) -> Option<i32> {
    // C# RunForSession(count)：count = parseState.Count - 1（去 script 位），
    // 即本切片长度（numkeys + keys... + argv...）。
    self.host.preamble_key_and_argv_count = args.len() as i32;
    self.host.session = Some(ScriptSessionPtr::erase(session));

    let _guard = CallbackGuard::enter(self.host.as_mut());

    // 每次调用自 RESP2 起（C# RunCommon 的重置施加在 SessionScriptCache
    // 独立内嵌 processor 上；rust 会话共享，窗口值由外层 run_lua_command 入口
    // 保存/收尾恢复，此处改写仅作用于脚本期——redis.call 应答解析面
    // （resp_protocol_version）与 setresp 的可见域）。最终应答按入口版本成帧
    // （C# RespResponseAdapter 读未被脚本触碰的外层会话版本）
    let entry_protocol_version = session.resp_protocol_version();
    session.update_resp_protocol_version(2);

    // preamble：装配 KEYS/ARGV（C# 经 RunPreambleForSession C 函数），
    // 随后执行已编译函数 —— redis.call 回调经窗口上下文访问会话面
    // （窗口已由 CallbackGuard 安装）。
    let preamble_res = self.run_preamble_for_session_slice(args);

    if let Err(err) = preamble_res {
      // err 为 preamble 的 `&'static [u8]` 常量文案；仍走字节入参口，
      // 清洗由 wresp 错误帧唯一成帧点保证
      self.host.session = None;
      let resp = RespOut::session(out, entry_protocol_version);
      RespWriter::new_ref(resp.buf).write_error_bytes(err);
      return None;
    }

    let mut resp = RespOut::session(out, entry_protocol_version);
    let phase = self.run_common(&mut resp);
    self.host.session = None;
    phase
  }

  /// 挂起续段：压入当前挂起的应答转换值后 `lua_resume` 续跑，直至完成
  /// （应答写 `out`，返回 `None`）或再次挂起（返回 `Some(tag)`）。
  ///
  /// 仅在 [`Self::run_for_session`] 返回挂起后由宿主驱动循环调用；协议
  /// 版本承接挂起时快照（wnode 侧窗口重开时已回贴会话），不再重置 RESP2
  /// ——setresp 的脚本期窗口值跨挂起有效。
  pub fn continue_session<S: ScriptingApi>(
    &mut self,
    reply: (&[u8], u8),
    session: &mut S,
    out: &mut Vec<u8>,
  ) -> Option<i32> {
    debug_assert!(!self.script_thread.is_null(), "无挂起中的脚本协程");
    self.host.session = Some(ScriptSessionPtr::erase(session));
    let _guard = CallbackGuard::enter(self.host.as_mut());

    let entry_protocol_version = session.resp_protocol_version();
    let mut resp = RespOut::session(out, entry_protocol_version);

    let phase = {
      // SAFETY：script_thread 由 run 窗口派生且挂起期存活（主栈锚 +
      // 会话缓存持有 runner），续段窗口独占使用。
      let _swap = self.state.swap_thread(self.script_thread);

      // 应答字节 → Lua 值压协程栈（redis.call 的返回值形态）
      let count = super::process_resp_response_view(&mut self.state, reply.1, reply.0);
      self.pending_resume_args = count.max(0) as usize;

      let status = self.state.resume(self.pending_resume_args);
      self.settle_resume(status, &mut resp)
    };

    if phase.is_none() {
      self.finish_run();
    }
    phase
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:RunForRunner
  ///
  /// 以 (keys, argv) 执行并把响应解析为对象（宿主形态）。
  /// 收尾守卫同 [`Self::run_for_session`]（preamble `?` 早退与
  /// run_common 各路径统一清栈 + GC 步进）。
  pub fn run_for_runner(
    &mut self,
    keys: Option<Vec<Vec<u8>>>,
    argv: Option<Vec<Vec<u8>>>,
  ) -> Result<RespObject, String> {
    let res = self.run_for_runner_inner(keys, argv);
    self.finish_run();
    res
  }

  fn run_for_runner_inner(
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
    let phase = self.run_common(&mut resp);
    // runner 模式无会话，redis.call 落 no_session_response，脚本不可挂起
    debug_assert!(phase.is_none(), "runner 模式脚本不应挂起");

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

  /// run 域统一收尾：清栈归零 + 增量 GC 步进（对标 Redis 脚本收尾
  /// lua_settop(lua,0) + lua_gc(lua,LUA_GCSTEP,1)，C# finally
  /// ExpectLuaStackEmpty 的生产语义承接；全仓清栈仅此一个执行口，不留双轨）。
  /// 协程线程锚（主栈栈位）随清栈释放，运行线程指针归空。
  fn finish_run(&mut self) {
    self.script_thread = null_mut();
    self.state.clear_stack();
    self.state.gc_step(1);
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
  pub fn run_preamble_for_session_slice(&mut self, args: &[&[u8]]) -> Result<(), &'static [u8]> {
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

  /// 装配 sandbox_env.<KEYS|ARGV> 数组全局（preamble 共用形态）。
  ///
  /// 泛型承接双调用面：session 切片视图 `&[&[u8]]` 与 runner 托管形态
  /// `&[Vec<u8>]`（C# RunPreambleForRunner 的 keys/argv 列表）皆按
  /// `AsRef<[u8]>` 零拷贝压栈，不强制 owned。
  fn fill_array_global<T: AsRef<[u8]>>(&mut self, name: &[u8], values: &[T]) {
    // 取目标数组表压栈（注册表项由本 runner 创建，恒为表）。
    _ = self.state.push_ref(self.sandbox_env_registry_index);
    let sandbox_at = self.state.get_top() as i32;
    self.state.push_buffer(name);
    self.state.raw_get(sandbox_at);
    self.state.remove(sandbox_at);

    // 移除 sandbox_env 后，数组表浮至当前栈顶；以动态下标定位，
    // 消除绝对下标 1 的硬编码假设（基底栈若有残留漂移，表不在 1 位）。
    let table_idx = self.state.get_top() as i32;
    for (i, value) in values.iter().enumerate() {
      // equivalent to KEYS[i+1] = value（值压栈后随 raw_set_integer 弹出）
      self.state.push_buffer(value.as_ref());
      self.state.raw_set_integer(table_idx, i as i64 + 1);
    }

    // Remove 数组表
    self.state.pop(1);
  }

  /// libs/server/Lua/LuaRunner.cs:RequestTimeout
  ///
  /// C# 即 `state.TrySetHook(&RequestTimeout, LuaHookMask.Count, 1)` 一行
  /// sethook；rust 等价物是换挂会话共享截止槽（超时管理器登记形态），
  /// 到期激活由 luau VM safepoint 中断回调承接（state.rs
  /// interrupt_trampoline）。
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
  ///
  /// 协程化执行（对标 pcall 同步形态的 resume 投影）：主线程派生协程线程
  /// 并以主栈栈位锚定，函数经注册表引用压入协程栈，`lua_resume` 续跑。
  /// `LUA_YIELD` = redis.call 命中阻塞/慢路径挂起，读让渡标记原样上返；
  /// 线程锚与协程帧跨挂起窗口存活，[`Self::finish_run`] 由完成路径收尾。
  fn run_common(&mut self, resp: &mut RespOut) -> Option<i32> {
    // 派生协程线程：值压主栈锚定（挂起窗口跨同步首段/续段；完成路径
    // finish_run 清栈收锚）。派生先于 push_ref——锚缺失即挂起窗口失活。
    let thread = self.state.new_thread();
    self.script_thread = thread;

    // 状态层换岗至协程线程：回调机器（global->cb）/分配器/注册表均 VM 级
    // 共享，切换仅改栈操作落点（见 state.rs swap_thread 语义依据）
    let _swap = self.state.swap_thread(thread);

    // 每次调用自 RESP2 起（重置由 RunForSession 入口完成，仅作用脚本期窗口）。
    if !self.state.push_ref(self.host.function_registry_index) {
      RespWriter::new_ref(resp.buf).write_error_bytes(ERR_LUA_INVOKE_FAILED);
      return None;
    }

    let status = self.state.resume(0);
    self.settle_resume(status, resp)
  }

  /// resume 状态解释（首段/续段共用单点）：`LUA_OK` → 终值写出完成；
  /// `LUA_YIELD` → 读让渡标记（redis.call 收尾压入，协程栈相对 1 位）挂起
  /// 返回；其余 → 错误帧写出（错误对象压栈形态与 pcall errfunc=0 一致）。
  fn settle_resume(&mut self, status: c_int, resp: &mut RespOut) -> Option<i32> {
    match status {
      sys::LUA_YIELD => {
        let tag = self.state.check_number(1).unwrap_or_default();
        Some(tag as i32)
      }
      sys::LUA_OK => {
        // 脚本窗口终值回读（对标 C# TryWriteSingleItem 布尔臂实时读
        // respServerSession.respProtocolVersion：RunCommon 起始压回 2，脚本内
        // setresp 改写共享会话，响应写出时刻即终值；runner 模式无会话，保持
        // 默认 2 落双 2 象限）
        if let Some(session) = self.host.session.as_mut() {
          resp.script_version = session.get().resp_protocol_version();
        }
        // The actual call worked, handle the response
        self.write_response(resp);
        debug_assert!(self.state.expect_lua_stack_empty());
        None
      }
      _ => {
        self.finish_err(resp);
        None
      }
    }
  }

  /// 丢弃挂起中的脚本协程（驱动方失联的兜底复位：协程线程锚随主栈清栈
  /// 释放，运行线程指针归空）。无挂起协程时空操作。
  pub fn abort_suspended(&mut self) {
    if self.script_thread.is_null() {
      return;
    }
    self.finish_run();
  }

  /// resume 错误收尾：错误对象 → RESP error 帧（原 pcall 错误臂单点）。
  fn finish_err(&mut self, resp: &mut RespOut) {
    // An error was raised
    let top = self.state.get_top();
    if top == 0 {
      RespWriter::new_ref(resp.buf).write_error_bytes(ERR_LUA_INVOKE_FAILED);
    } else {
      let Some(err_buf) = self.state.known_string_to_buffer(-1) else {
        log::error!("Got an unexpected error value from lua resume: top={top}");
        RespWriter::new_ref(resp.buf).write_error_bytes(ERR_UNEXPECTED_RESPONSE);
        self.state.clear_stack();
        return;
      };

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

      self.state.clear_stack();
    }
    debug_assert!(self.state.expect_lua_stack_empty());
  }
}
