//! Lua 命令面：EVAL / EVALSHA / SCRIPT 全语义
//! （对标 libs/server/Lua/LuaCommands.cs:RespServerSession 的 Lua 分片）。
//!
//! C# 入口挂接在 RespServerSession 上直写网络缓冲；Rust 以
//! [`LuaSessionContext`] 承载会话面（参数、缓存、输出缓冲、命令 API），
//! resp 域（并行转写域）后续把其会话类型适配到 [`ScriptingApi`] 接线。

use std::sync::Arc;

use wbase::{map::ConcurrentMap, num::strict_i64, time::now_ms_i64};
use wresp::{
  cmd_strings::{
    GENERIC_ERR_WRONG_NUM_ARGS, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, RESP_ERR_NO_SCRIPT,
    RESP_ERR_SCRIPT_FLUSH_OPTIONS, RESP_OK,
  },
  resp_memory_writer::RespWriter,
};

use crate::{
  api::ScriptingApi,
  cache::{LuaScriptHandle, RunnerCreateOptions, SessionScriptCache},
  hash_key::{SHA1_HEX_LEN, ScriptHashKey},
  options::LuaOptions,
  runner::RespOut,
  timeout::TimeoutRegistration,
};

/// 全局脚本缓存（对标 storeWrapper.storeScriptCache 的
/// ConcurrentDictionary<ScriptHashKey, LuaScriptHandle>）。
#[derive(Default)]
pub struct StoreScriptCache {
  /// 摘要 → 共享脚本句柄。
  map: ConcurrentMap<ScriptHashKey, Arc<LuaScriptHandle>>,
}

impl StoreScriptCache {
  /// TryGetValue
  pub fn try_get(&self, key: &ScriptHashKey) -> Option<Arc<LuaScriptHandle>> {
    self.map.pin().get(key).cloned()
  }

  /// 全局登记的唯一收口（承接 libs/server/Lua/LuaCommands.cs 的 TryEVAL /
  /// NetworkScriptLoad 中重复书写的 TryAdd 失败后 Dispose 逻辑）：
  /// 键已存在时不覆写先到权威句柄（papaya try_insert 承接
  /// ConcurrentDictionary.TryAdd 语义，insert 会顶掉既有值，不合规），
  /// 并对败方新句柄就地 dispose 后返回 false；成功登记返回 true。
  /// 竞态败方当次请求仍以本地 runner 正常应答，清退延后到下次检索。
  pub fn try_add_or_toss(&self, key: ScriptHashKey, handle: Arc<LuaScriptHandle>) -> bool {
    if self.map.pin().try_insert(key, Arc::clone(&handle)).is_ok() {
      return true;
    }
    handle.dispose();
    false
  }

  /// TryRemove
  pub fn try_remove(&self, key: &ScriptHashKey) -> Option<Arc<LuaScriptHandle>> {
    self.map.pin().remove(key).cloned()
  }

  /// ContainsKey
  pub fn contains_key(&self, key: &ScriptHashKey) -> bool {
    self.map.pin().contains_key(key)
  }

  /// 全部摘要（SCRIPT FLUSH 遍历 + 逐句柄销毁）。
  pub fn keys(&self) -> Vec<ScriptHashKey> {
    self.map.pin().keys().cloned().collect()
  }
}

/// Lua 会话上下文：LuaCommands 的会话面
/// （对标 RespServerSession 的 parseState / sessionScriptCache / storeWrapper /
/// dcurr 输出缓冲的组合）。
pub struct LuaSessionContext<'a, S: ScriptingApi> {
  /// parseState 参数切片视图（EVAL 为 [script, numkeys, keys..., argv...]；
  /// 零拷贝直通接收缓冲，对标 C# parseState.GetArgSliceByRef 形态）。
  pub args: &'a [&'a [u8]],
  /// 响应输出缓冲（dcurr 等价物，RESP2/3 直写）。
  pub out: &'a mut Vec<u8>,
  /// 会话级脚本缓存（'s = 缓存内 runner 的会话面借用，独立于本上下文借用 'a）。
  pub session_cache: &'a mut SessionScriptCache,
  /// 全局脚本缓存。
  pub store_cache: &'a StoreScriptCache,
  /// 会话命令 API（redis.call 落地面）。
  pub session: &'a mut S,
  /// redis 版本号全局量。
  pub redis_version: &'a str,
  /// 会话 Lua 选项。
  pub lua_options: &'a LuaOptions,
}

impl<'a, S: ScriptingApi> LuaSessionContext<'a, S> {
  /// runner 构造选项（逐字段下传形态）。
  fn runner_options(&self) -> RunnerCreateOptions {
    RunnerCreateOptions {
      mem_mode: Some(self.lua_options.memory_mode),
      mem_limit_bytes: self.lua_options.get_memory_limit_bytes(),
      log_mode: Some(self.lua_options.log_mode),
      allowed_functions: if self.lua_options.allowed_functions.is_empty() {
        None
      } else {
        Some(self.lua_options.allowed_functions.iter().cloned().collect())
      },
      redis_version: self.redis_version.to_string(),
    }
  }
}

enum EvalshaResolution {
  Resolved(ScriptHashKey),
  LoadFailed,
  NotFound,
}

pub struct LuaCommands;

impl LuaCommands {
  fn validate_numkeys<S: ScriptingApi>(
    ctx: &mut LuaSessionContext<'_, S>,
    cmd_name: &str,
  ) -> Option<usize> {
    let count = ctx.args.len();
    if count < 2 {
      Self::abort_with_wrong_number_of_arguments(ctx, cmd_name);
      return None;
    }

    let Some(n) = strict_i64(ctx.args[1]) else {
      Self::abort_with_error_message(ctx, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes());
      return None;
    };
    if !(0..=(count as i64 - 2)).contains(&n) {
      Self::abort_with_error_message(ctx, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes());
      return None;
    }
    Some(count)
  }

  fn resolve_evalsha_script_key<S: ScriptingApi>(
    ctx: &mut LuaSessionContext<'_, S>,
  ) -> EvalshaResolution {
    // 摘要直借参数视图（零拷贝）；miss 重试的栈上 40 字节小写副本一次备好
    // （C# AsciiUtils.ToLowerInPlace 原地改写接收缓冲，rust 参数为只读视图，
    //  等价落栈，不触碰接收窗，无堆分配）
    let mut lower_buf = [0_u8; SHA1_HEX_LEN];
    if ctx.args[0].len() == SHA1_HEX_LEN {
      lower_buf.copy_from_slice(ctx.args[0]);
      lower_buf.make_ascii_lowercase();
    }
    let mut digest: &[u8] = ctx.args[0];
    let mut converted_to_lower = false;

    while digest.len() == SHA1_HEX_LEN {
      let Some(script_key) = ScriptHashKey::from_hex(digest) else {
        break;
      };

      if ctx.session_cache.try_get_runner(&script_key).is_some() {
        return EvalshaResolution::Resolved(script_key);
      }

      if let Some(global_script_handle) = ctx.store_cache.try_get(&script_key) {
        let mut handle = Some(Arc::clone(&global_script_handle));
        let source = global_script_handle.script_data().to_vec();
        let options = ctx.runner_options();
        let mut load_out = Vec::new();
        let loaded = ctx.session_cache.try_load_runner(
          &source,
          &script_key,
          &mut handle,
          &options,
          &mut load_out,
        );
        if loaded.is_none() {
          // TryLoad will have written an error out, if any
          ctx.out.extend_from_slice(&load_out);
          // Note we DON'T dispose the script handle because this is just the session cache
          _ = ctx.store_cache.try_remove(&script_key);
          return EvalshaResolution::LoadFailed;
        }
        return EvalshaResolution::Resolved(script_key);
      }

      if !converted_to_lower {
        // On a miss (which should be rare) make sure the hash is lower case and try again.
        digest = &lower_buf;
        converted_to_lower = true;
        continue;
      }

      break;
    }

    EvalshaResolution::NotFound
  }

  /// libs/server/Lua/LuaCommands.cs:TryEVALSHA
  ///
  /// EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
  pub fn try_evalsha<S: ScriptingApi>(ctx: &mut LuaSessionContext<'_, S>) -> bool {
    let Some(count) = Self::validate_numkeys(ctx, "EVALSHA") else {
      return true;
    };

    let script_key = match Self::resolve_evalsha_script_key(ctx) {
      EvalshaResolution::Resolved(key) => key,
      EvalshaResolution::LoadFailed => return true,
      EvalshaResolution::NotFound => {
        let resp = RespOut::session(ctx.out, 2);
        RespWriter::new_ref(resp.buf).write_error_bytes(RESP_ERR_NO_SCRIPT);
        return true;
      }
    };

    Self::run_script_for_session(ctx, count, &script_key);
    true
  }

  /// libs/server/Lua/LuaCommands.cs:TryEVAL
  ///
  /// EVAL script numkeys [key [key ...]] [arg [arg ...]]
  pub fn try_eval<S: ScriptingApi>(ctx: &mut LuaSessionContext<'_, S>) -> bool {
    let Some(count) = Self::validate_numkeys(ctx, "EVAL") else {
      return true;
    };

    // 脚本源直传参数视图（零拷贝；C# script = ref parseState.GetArgSliceByRef(0)）。
    let script = ctx.args[0];
    let on_stack_script_key = SessionScriptCache::get_script_digest(script);
    let mut session_script_handle = ctx.store_cache.try_get(&on_stack_script_key);
    let options = ctx.runner_options();
    let mut load_out = Vec::new();
    let loaded = ctx.session_cache.try_load_runner(
      script,
      &on_stack_script_key,
      &mut session_script_handle,
      &options,
      &mut load_out,
    );

    let Some((_, created)) = loaded else {
      // TryLoad will have written any errors out
      ctx.out.extend_from_slice(&load_out);
      return true;
    };

    // Add script to the store dictionary IF we didn't already have it cached
    //
    // This may strike you as odd, but it is how Redis behaves
    if let Some(new_handle) = created {
      // Some other session may have loaded the script meanwhile; on a race the
      // new handle is tossed and the global copy wins on next invocation
      //（toss 臂统一收敛至 StoreScriptCache::try_add_or_toss 单点；当次请求
      // 仍以本地 runner 正常执行，仅下次检索才清退失效句柄）。
      ctx
        .store_cache
        .try_add_or_toss(on_stack_script_key, new_handle);
    }

    Self::run_script_for_session(ctx, count, &on_stack_script_key);
    true
  }

  /// libs/server/Lua/LuaCommands.cs:NetworkScriptExists
  ///
  /// SCRIPT|EXISTS：逐摘要输出 0/1 数组。
  pub fn network_script_exists<S: ScriptingApi>(ctx: &mut LuaSessionContext<'_, S>) -> bool {
    if ctx.args.is_empty() {
      return Self::abort_with_wrong_number_of_arguments(ctx, "script|exists");
    }

    // Returns an array where each element is a 0 if the script does not exist, and a 1 if it does
    let resp = RespOut::session(ctx.out, 2);
    RespWriter::new_ref(resp.buf).write_array_length(ctx.args.len());

    for sha1 in ctx.args {
      let mut exists = 0;

      // Length check is required, as ScriptHashKey makes a hard assumption
      if let Some(key) = ScriptHashKey::from_hex(sha1) {
        exists = i64::from(ctx.store_cache.contains_key(&key));
      }

      RespWriter::new_ref(resp.buf).write_int64(exists);
    }

    true
  }

  /// libs/server/Lua/LuaCommands.cs:NetworkScriptFlush
  ///
  /// SCRIPT|FLUSH：可选 ASYNC/SYNC 参数校验后清空全局缓存。
  pub fn network_script_flush<S: ScriptingApi>(ctx: &mut LuaSessionContext<'_, S>) -> bool {
    if ctx.args.len() > 1 {
      return Self::abort_with_error_message(ctx, RESP_ERR_SCRIPT_FLUSH_OPTIONS);
    } else if ctx.args.len() == 1 {
      // We ignore this, but should validate it
      // 大小写无关比较承接 C# AsciiUtils.ToUpperInPlace + SequenceEqual
      // （视图只读，不物化大写副本）
      let arg = ctx.args[0];
      if !arg.eq_ignore_ascii_case(b"ASYNC") && !arg.eq_ignore_ascii_case(b"SYNC") {
        return Self::abort_with_error_message(ctx, RESP_ERR_SCRIPT_FLUSH_OPTIONS);
      }
    }

    // Flush store script cache
    //
    // Disposing each script handle (that we actually remove) along the way
    // to signal to session level caches that the script needs to be discarded
    for digest in ctx.store_cache.keys() {
      if let Some(script_handle) = ctx.store_cache.try_remove(&digest) {
        script_handle.dispose();
      }
    }

    let resp = RespOut::session(ctx.out, 2);
    RespWriter::new_ref(resp.buf).write_direct(RESP_OK);

    true
  }

  /// libs/server/Lua/LuaCommands.cs:NetworkScriptLoad
  ///
  /// SCRIPT|LOAD：编译登记脚本并输出摘要。
  pub fn network_script_load<S: ScriptingApi>(ctx: &mut LuaSessionContext<'_, S>) -> bool {
    if ctx.args.len() != 1 {
      return Self::abort_with_wrong_number_of_arguments(ctx, "script|load");
    }

    // 脚本源直传参数视图（零拷贝；C# NetworkScriptLoad 直读 parseState 位）。
    let source = ctx.args[0];
    let digest = SessionScriptCache::get_script_digest(source);

    let mut session_script_handle = ctx.store_cache.try_get(&digest);
    let options = ctx.runner_options();
    let mut load_out = Vec::new();
    let loaded = ctx.session_cache.try_load_runner(
      source,
      &digest,
      &mut session_script_handle,
      &options,
      &mut load_out,
    );

    let Some((_, created)) = loaded else {
      // TryLoad will write any errors out
      ctx.out.extend_from_slice(&load_out);
      return true;
    };

    // Add script to the global store dictionary if not already in there
    if let Some(new_handle) = created {
      // Some other caller may have added the script already; on a race the
      // new handle is dead but we'll load it from the shared cache next time
      //（toss 臂统一收敛至 StoreScriptCache::try_add_or_toss 单点）。
      ctx.store_cache.try_add_or_toss(digest, new_handle);
    }

    let resp = RespOut::session(ctx.out, 2);
    RespWriter::new_ref(resp.buf).write_bulk_string(digest.as_str().as_bytes());

    true
  }

  /// libs/server/Lua/LuaCommands.cs:RunScriptForSession
  ///
  /// 以当前会话执行已解析脚本；失败时把 runner 移出会话缓存。
  ///
  /// 返回脚本挂起态（协程化承接）：`Some(tag)` = 脚本内 redis.call 命中
  /// 阻塞/慢路径，宿主 await 驱动挂起体后经
  /// [`Self::continue_execute_script`] 续跑；`None` = 已完成。
  pub fn run_script_for_session<S: ScriptingApi>(
    ctx: &mut LuaSessionContext<'_, S>,
    count: usize,
    script_key: &ScriptHashKey,
  ) -> Option<i32> {
    // We assume here that ExecuteScript does not raise exceptions.
    // 会话缓存正在运行同一脚本（重入/销毁竞态）时按 C# StartRunningScript 语义跳过。
    if ctx.session_cache.is_running(script_key) {
      return None;
    }
    ctx.session_cache.start_running_script(script_key);

    let phase = Self::start_execute_script(ctx, count - 1, script_key);

    ctx.session_cache.stop_running_script(script_key);
    phase
  }

  /// libs/server/Lua/LuaCommands.cs:TryExecuteScript
  ///
  /// 脚本执行首段（同步）：调用脚本至完成或首次挂起；返回 `Some(tag)` =
  /// 挂起（协程化承接：redis.call 命中阻塞/慢路径，挂起体已让渡会话侧，
  /// 宿主 await 驱动后经 [`Self::continue_execute_script`] 续跑）；
  /// `None` = 已完成（含出错），runner 完成清退判定已就地承接。
  ///
  /// 超时装挂对齐 C# StartRunningScript/StopRunningScript 配对：arm 置
  /// 共享截止槽 = 单调 now + timeout（SetCookie 形态，`now_ms_i64` 免壁钟
  /// NTP 阶跃回拨干扰），完成臂 disarm 清 0（SetCookie(0) 形态）；挂起
  /// 窗口截止保持武装（与内联收割同语义），续段完成臂统一撤销。到期
  /// 激活由 LuaTimeoutManager.tick 承接。
  pub fn start_execute_script<S: ScriptingApi>(
    ctx: &mut LuaSessionContext<'_, S>,
    count: usize,
    script_key: &ScriptHashKey,
  ) -> Option<i32> {
    let timeout = ctx.session_cache.timeout_handle();
    // 执行期直取（对标 C# RunScriptForSession 直接持 TryLoad/TryGetFromDigest
    // 返回的 runner 引用）：不做 is_disposed 清退判定，败方句柄 dispose 后
    // 当次请求仍正常应答，清退延后到下次检索（try_get_runner）。
    let runner = ctx.session_cache.get_runner_mut(script_key)?;

    // 装挂超时（先取句柄再借 runner，规避缓存与 runner 的双重可变借用）。
    if let Some((registration, timeout_millis)) = &timeout {
      runner.hook_shared_deadline(registration.shared_deadline());
      registration.arm(now_ms_i64(), *timeout_millis);
    }

    // TryExecuteScript(count - 1)：去掉 numkeys 位。
    let args = &ctx.args[1..(count + 1).min(ctx.args.len())];
    let phase = runner.run_for_session(args, ctx.session, ctx.out);

    if let Some(tag) = phase {
      // 挂起登记（续跑入口凭键取回 runner）
      ctx.session_cache.note_suspended(script_key);
      return Some(tag);
    }

    Self::finish_execute_script(ctx, script_key, &timeout);
    None
  }

  /// 脚本执行续段（同步单步）：驱动挂起脚本协程至完成或再次挂起。`reply`
  /// 为当前挂起的应答字节（RESP 帧）与协议版本——外层先 await 驱动挂起体
  /// （挂起必有未决命令，不得空拍续跑），再经本口转 Lua 值压协程栈作
  /// redis.call 返回值后续跑。
  ///
  /// 完成臂承接超时 disarm 与 runner 完成清退判定（镜像首段）。
  pub fn continue_execute_script<S: ScriptingApi>(
    ctx: &mut LuaSessionContext<'_, S>,
    reply: (&[u8], u8),
  ) -> Option<i32> {
    let timeout = ctx.session_cache.timeout_handle();
    let script_key = ctx.session_cache.suspended_key()?;
    let Some(runner) = ctx.session_cache.get_runner_mut(&script_key) else {
      ctx.session_cache.clear_suspended();
      return None;
    };

    let phase = runner.continue_session(reply, ctx.session, ctx.out);

    if phase.is_none() {
      Self::finish_execute_script(ctx, &script_key, &timeout);
    }
    phase
  }

  /// 脚本完成臂（首段/续段共用）：撤销截止 + 完成清退判定（needs_dispose
  /// 的 runner 移出会话缓存——句柄不 dispose，仅下次检索清退，C# 口径）。
  fn finish_execute_script<S: ScriptingApi>(
    ctx: &mut LuaSessionContext<'_, S>,
    script_key: &ScriptHashKey,
    timeout: &Option<(Arc<TimeoutRegistration>, i64)>,
  ) {
    // 撤销截止：正常完成/超时中断后皆清，杜绝残留误伤下一 run。
    if let Some((registration, _)) = timeout {
      registration.disarm();
    }
    ctx.session_cache.clear_suspended();

    let keep = ctx
      .session_cache
      .get_runner_mut(script_key)
      .is_none_or(|runner| !runner.needs_dispose());
    if !keep {
      // Note we DON'T dispose the script handle because this is just the session cache
      ctx.session_cache.remove_runner(script_key);
    }
  }

  /// AbortWithWrongNumberOfArguments（resp 域形态，文案对齐）。
  fn abort_with_wrong_number_of_arguments<S: ScriptingApi>(
    ctx: &mut LuaSessionContext<'_, S>,
    command: &str,
  ) -> bool {
    let text = GENERIC_ERR_WRONG_NUM_ARGS.replace("{0}", command);
    Self::abort_with_error_message(ctx, text.as_bytes())
  }

  /// AbortWithErrorMessage。
  ///
  /// `message` 为已拼好的错误字节（含命令名回显面），净化在 wresp 错误帧唯一
  /// 成帧点内生效，本口不再预处理也不裸成帧。
  fn abort_with_error_message<S: ScriptingApi>(
    ctx: &mut LuaSessionContext<'_, S>,
    message: &[u8],
  ) -> bool {
    let resp = RespOut::session(ctx.out, 2);
    RespWriter::new_ref(resp.buf).write_error_bytes(message);
    true
  }

  /// 丢弃挂起中的脚本（驱动方失联的兜底复位：挂起 runner 复位 + 挂起登记
  /// 清除；挂起体的取消由会话侧承接）。无挂起登记时空操作。
  pub fn abort_suspended_script(session_cache: &mut SessionScriptCache) {
    let Some(script_key) = session_cache.suspended_key() else {
      return;
    };
    session_cache.clear_suspended();
    if let Some(runner) = session_cache.get_runner_mut(&script_key) {
      runner.abort_suspended();
    }
  }
}
