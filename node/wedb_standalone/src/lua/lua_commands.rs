//! Lua 命令面：EVAL / EVALSHA / SCRIPT 全语义
//! （对标 libs/server/Lua/LuaCommands.cs:RespServerSession 的 Lua 分片）。
//!
//! C# 入口挂接在 RespServerSession 上直写网络缓冲；Rust 以
//! [`LuaSessionContext`] 承载会话面（参数、缓存、输出缓冲、命令 API），
//! resp 域（并行转写域）后续把其会话类型适配到 [`ScriptingApi`] 接线。

use std::{str, sync::Arc};

use whasher::GxPapayaMap;

use super::{
  lua_options::LuaOptions,
  lua_runner::RespOut,
  scratch_buffer_network_sender::ScratchBufferNetworkSender,
  script_hash_key::ScriptHashKey,
  scripting_api::ScriptingApi,
  session_script_cache::{LuaScriptHandle, RunnerCreateOptions, SHA1_LEN, SessionScriptCache},
};
use crate::resp::{
  cmd_strings::GENERIC_ERR_WRONG_NUM_ARGS, parser::session_parse_state::strict_i64,
};

/// EVAL/EVALSHA numkeys 非法时报错文案（C# CmdStrings
/// RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER）
const ERR_VALUE_NOT_INTEGER: &[u8] = b"ERR value is not an integer or out of range.";
/// SCRIPT FLUSH 选项非法文案（本域两处复用）。
const ERR_SCRIPT_FLUSH_OPTION: &[u8] = b"ERR SCRIPT FLUSH only support SYNC|ASYNC option";

/// 全局脚本缓存（对标 storeWrapper.storeScriptCache 的
/// ConcurrentDictionary<ScriptHashKey, LuaScriptHandle>）。
#[derive(Default)]
pub struct StoreScriptCache {
  /// 摘要 → 共享脚本句柄。
  map: GxPapayaMap<ScriptHashKey, Arc<LuaScriptHandle>>,
}

impl StoreScriptCache {
  /// TryGetValue
  pub fn try_get(&self, key: &ScriptHashKey) -> Option<Arc<LuaScriptHandle>> {
    self.map.pin().get(key).cloned()
  }

  /// TryAdd
  pub fn try_add(&self, key: ScriptHashKey, handle: Arc<LuaScriptHandle>) -> bool {
    self.map.pin().insert(key, handle).is_none()
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
pub struct LuaSessionContext<'a> {
  /// parseState 参数（EVAL 为 [script, numkeys, keys..., argv...]）。
  pub args: &'a [Vec<u8>],
  /// 响应输出缓冲（dcurr 等价物，RESP2/3 直写）。
  pub out: &'a mut Vec<u8>,
  /// 会话级脚本缓存（'s = 缓存内 runner 的会话面借用，独立于本上下文借用 'a）。
  pub session_cache: &'a mut SessionScriptCache,
  /// 全局脚本缓存。
  pub store_cache: &'a StoreScriptCache,
  /// 会话命令 API（redis.call 落地面）。
  pub session: &'a mut dyn ScriptingApi,
  /// 是否启用 Lua（serverOptions.EnableLua）。
  pub lua_enabled: bool,
  /// 脚本是否以事务形态执行（serverOptions.LuaTransactionMode）。
  pub txn_mode: bool,
  /// redis 版本号全局量。
  pub redis_version: &'a str,
  /// 会话 Lua 选项。
  pub lua_options: &'a LuaOptions,
}

impl<'a> LuaSessionContext<'a> {
  /// runner 构造选项（逐字段下传形态）。
  fn runner_options(&self) -> RunnerCreateOptions {
    RunnerCreateOptions {
      mem_limit_bytes: self.lua_options.get_memory_limit_bytes(),
      log_mode: Some(self.lua_options.log_mode),
      allowed_functions: if self.lua_options.allowed_functions.is_empty() {
        None
      } else {
        Some(self.lua_options.allowed_functions.iter().cloned().collect())
      },
      txn_mode: self.txn_mode,
      redis_version: self.redis_version.to_string(),
    }
  }
}

pub struct LuaCommands;

impl LuaCommands {
  /// libs/server/Lua/LuaCommands.cs:TryEVALSHA
  ///
  /// EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
  #[allow(clippy::too_many_lines)]
  pub fn try_evalsha(ctx: &mut LuaSessionContext) -> bool {
    if !Self::check_lua_enabled(ctx) {
      return true;
    }

    let count = ctx.args.len();
    if count < 2 {
      return Self::abort_with_wrong_number_of_arguments(ctx, "EVALSHA");
    }

    let Some(n) = strict_i64(&ctx.args[1]) else {
      return Self::abort_with_error_message(ctx, ERR_VALUE_NOT_INTEGER);
    };
    if !(0..=(count as i64 - 2)).contains(&n) {
      return Self::abort_with_error_message(ctx, ERR_VALUE_NOT_INTEGER);
    }

    // Length check is mandatory, as ScriptHashKey assumes correct length.
    // C# tryAgain 标签形态：会话未命中 → 全局装载 → 小写化重试一次。
    let mut digest = ctx.args[0].clone();
    let mut converted_to_lower = false;
    let mut resolved: Option<ScriptHashKey> = None;

    while digest.len() == SHA1_LEN {
      let Some(script_key) = ScriptHashKey::from_hex(&digest) else {
        break;
      };

      if ctx.session_cache.try_get_runner(&script_key).is_some() {
        resolved = Some(script_key);
        break;
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
          return true;
        }
        resolved = Some(script_key);
        break;
      }

      if !converted_to_lower {
        // On a miss (which should be rare) make sure the hash is lower case and try again.
        //
        // We assume that hashes will be sent in the same format as we return them (lower)
        // most of the time, so optimize for that.
        digest.make_ascii_lowercase();
        converted_to_lower = true;
        continue;
      }

      break;
    }

    let Some(script_key) = resolved else {
      let mut resp = RespOut::session(ctx.out, 2);
      resp.write_error(b"NOSCRIPT No matching script. Please use EVAL.");
      return true;
    };

    Self::run_script_for_session(ctx, count, &script_key);
    true
  }

  /// libs/server/Lua/LuaCommands.cs:TryEVAL
  ///
  /// EVAL script numkeys [key [key ...]] [arg [arg ...]]
  pub fn try_eval(ctx: &mut LuaSessionContext) -> bool {
    if !Self::check_lua_enabled(ctx) {
      return true;
    }

    let count = ctx.args.len();
    if count < 2 {
      return Self::abort_with_wrong_number_of_arguments(ctx, "EVAL");
    }

    let Some(n) = strict_i64(&ctx.args[1]) else {
      return Self::abort_with_error_message(ctx, ERR_VALUE_NOT_INTEGER);
    };
    if !(0..=(count as i64 - 2)).contains(&n) {
      return Self::abort_with_error_message(ctx, ERR_VALUE_NOT_INTEGER);
    }

    let script = ctx.args[0].clone();
    let on_stack_script_key = SessionScriptCache::get_script_digest(&script);
    let global_script_handle = ctx.store_cache.try_get(&on_stack_script_key);

    let mut session_script_handle = global_script_handle.clone();
    let options = ctx.runner_options();
    let mut load_out = Vec::new();
    let loaded = ctx.session_cache.try_load_runner(
      &script,
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
      _ = ctx
        .store_cache
        .try_add(on_stack_script_key.clone(), new_handle);
    }

    Self::run_script_for_session(ctx, count, &on_stack_script_key);
    true
  }

  /// libs/server/Lua/LuaCommands.cs:NetworkScriptExists
  ///
  /// SCRIPT|EXISTS：逐摘要输出 0/1 数组。
  pub fn network_script_exists(ctx: &mut LuaSessionContext) -> bool {
    if !Self::check_lua_enabled(ctx) {
      return true;
    }

    if ctx.args.is_empty() {
      return Self::abort_with_wrong_number_of_arguments(ctx, "script|exists");
    }

    // Returns an array where each element is a 0 if the script does not exist, and a 1 if it does
    let mut resp = RespOut::session(ctx.out, 2);
    resp.write_array_len(ctx.args.len());

    for sha1 in ctx.args {
      let mut exists = 0;

      // Length check is required, as ScriptHashKey makes a hard assumption
      if let Some(key) = ScriptHashKey::from_hex(sha1) {
        exists = i64::from(ctx.store_cache.contains_key(&key));
      }

      resp.write_int64(exists);
    }

    true
  }

  /// libs/server/Lua/LuaCommands.cs:NetworkScriptFlush
  ///
  /// SCRIPT|FLUSH：可选 ASYNC/SYNC 参数校验后清空全局缓存。
  pub fn network_script_flush(ctx: &mut LuaSessionContext) -> bool {
    if !Self::check_lua_enabled(ctx) {
      return true;
    }

    if ctx.args.len() > 1 {
      return Self::abort_with_error_message(ctx, ERR_SCRIPT_FLUSH_OPTION);
    } else if ctx.args.len() == 1 {
      // We ignore this, but should validate it
      let arg = ctx.args[0].to_ascii_uppercase();
      if arg != b"ASYNC" && arg != b"SYNC" {
        return Self::abort_with_error_message(ctx, ERR_SCRIPT_FLUSH_OPTION);
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

    let mut resp = RespOut::session(ctx.out, 2);
    resp.write_direct(b"+OK\r\n");

    true
  }

  /// libs/server/Lua/LuaCommands.cs:NetworkScriptLoad
  ///
  /// SCRIPT|LOAD：编译登记脚本并输出摘要。
  pub fn network_script_load(ctx: &mut LuaSessionContext) -> bool {
    if !Self::check_lua_enabled(ctx) {
      return true;
    }

    if ctx.args.len() != 1 {
      return Self::abort_with_wrong_number_of_arguments(ctx, "script|load");
    }

    let source = ctx.args[0].clone();
    let digest = SessionScriptCache::get_script_digest(&source);

    let global_script_handle = ctx.store_cache.try_get(&digest);
    let mut session_script_handle = global_script_handle.clone();
    let options = ctx.runner_options();
    let mut load_out = Vec::new();
    let loaded = ctx.session_cache.try_load_runner(
      &source,
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
      _ = ctx.store_cache.try_add(digest.clone(), new_handle);
    }

    let mut resp = RespOut::session(ctx.out, 2);
    resp.write_bulk_string(digest.as_str().as_bytes());

    true
  }

  /// libs/server/Lua/LuaCommands.cs:CheckLuaEnabled
  ///
  /// 启用时返回 true；否则写出错误并返回 false。
  pub fn check_lua_enabled(ctx: &mut LuaSessionContext) -> bool {
    if !ctx.lua_enabled {
      let mut resp = RespOut::session(ctx.out, 2);
      resp.write_error(b"ERR This instance has Lua scripting support disabled");
      return false;
    }

    true
  }

  /// libs/server/Lua/LuaCommands.cs:RunScriptForSession
  ///
  /// 以当前会话执行已解析脚本；失败时把 runner 移出会话缓存。
  pub fn run_script_for_session(
    ctx: &mut LuaSessionContext,
    count: usize,
    script_key: &ScriptHashKey,
  ) {
    // We assume here that ExecuteScript does not raise exceptions.
    // 会话缓存正在运行同一脚本（重入/销毁竞态）时按 C# StartRunningScript 语义跳过。
    if ctx.session_cache.is_running(script_key) {
      return;
    }
    ctx.session_cache.start_running_script(script_key);

    Self::try_execute_script(ctx, count - 1, script_key);

    ctx.session_cache.stop_running_script(script_key);
  }

  /// libs/server/Lua/LuaCommands.cs:TryExecuteScript
  ///
  /// 调用脚本执行；返回 false 表示 runner 应被丢弃。
  pub fn try_execute_script(
    ctx: &mut LuaSessionContext,
    count: usize,
    script_key: &ScriptHashKey,
  ) -> bool {
    let Some(runner) = ctx.session_cache.try_get_runner(script_key) else {
      return false;
    };

    // TryExecuteScript(count - 1)：去掉 numkeys 位。
    let args = ctx.args[1..(count + 1).min(ctx.args.len())].to_vec();
    runner.run_for_session(&args, ctx.session, ctx.out);

    let keep = !runner.needs_dispose();
    if !keep {
      // Note we DON'T dispose the script handle because this is just the session cache
      ctx.session_cache.remove_runner(script_key);
    }
    keep
  }

  /// AbortWithWrongNumberOfArguments（resp 域形态，文案对齐）。
  fn abort_with_wrong_number_of_arguments(ctx: &mut LuaSessionContext, command: &str) -> bool {
    let text = GENERIC_ERR_WRONG_NUM_ARGS.replace("{0}", command);
    Self::abort_with_error_message(ctx, text.as_bytes())
  }

  /// AbortWithErrorMessage。
  fn abort_with_error_message(ctx: &mut LuaSessionContext, message: &[u8]) -> bool {
    let mut resp = RespOut::session(ctx.out, 2);
    resp.write_error(message);
    true
  }
}

/// 摘要便捷构造（供 resp 域接线使用）。
pub fn script_digest(source: &[u8]) -> ScriptHashKey {
  SessionScriptCache::get_script_digest(source)
}

/// 哑会话：恒定命令面（benchmark / 测试形态，对标 respServerSession == null）。
#[derive(Default)]
pub struct NoopScriptingApi;

impl ScriptingApi for NoopScriptingApi {
  fn dispatch_resp(&mut self, _request: &[u8], _sender: &mut ScratchBufferNetworkSender) {}

  fn get(&mut self, _key: &[u8]) -> Result<Option<Vec<u8>>, &'static str> {
    Ok(None)
  }

  fn set(&mut self, _key: &[u8], _value: &[u8]) -> Result<(), &'static str> {
    Ok(())
  }

  fn resp_protocol_version(&self) -> u8 {
    2
  }

  fn update_resp_protocol_version(&mut self, _version: u8) {}

  fn check_acl_permissions(&self, _command: &str) -> bool {
    true
  }
}
