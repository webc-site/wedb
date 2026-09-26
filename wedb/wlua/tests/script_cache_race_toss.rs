//! EVAL / SCRIPT LOAD 全局登记竞态败方句柄 toss 契约回归测试。
//!
//! 在 garnet 中的相对路径:libs/server/Lua/LuaCommands.cs:TryEVAL /
//! NetworkScriptLoad（TryAdd 失败分支 `sessionScriptHandle.Dispose()`）与
//! libs/server/Lua/SessionScriptCache.cs:TryGetFromDigest（失效清退）、
//! LuaCommands.cs:NetworkScriptFlush（失效传播闭环）。
//!
//! 并发模拟编排（同摘要先后两次登记，不依赖真线程竞态时序）：
//! 胜方先到登记，败方以同摘要再登记——登记必须失败、先到权威句柄不被
//! 覆写、败方句柄 dispose 后当次请求仍以本地 runner 正常应答，仅在下次
//! 检索清退并经全局恢复；SCRIPT FLUSH 后胜败双方会话的 EVALSHA 均须
//! NOSCRIPT（票面危害一闭环判据）。

use std::sync::Arc;

use wlua::{
  LuaCommands, LuaOptions, LuaSessionContext, RunnerCreateOptions, ScriptApiError, ScriptingApi,
  SessionScriptCache, StoreScriptCache,
};
use wresp::command::RespCommand;

/// 被测脚本：`return 7` → RESP2 整数帧。
const SRC: &[u8] = b"return 7";
const INT7: &[u8] = b":7\r\n";
const NOSCRIPT: &[u8] = b"NOSCRIPT";

/// 字节子序列包含判定（响应帧断言用）。
fn has(hay: &[u8], needle: &[u8]) -> bool {
  hay.windows(needle.len()).any(|w| w == needle)
}

/// 最小会话面：本文件场景脚本不含 redis.call，无任何落地调用
/// （对标 C# 会话 simple 脚本路径下 basicGarnetApi 从不触达的形态）。
#[derive(Default)]
struct NullSession;

impl ScriptingApi for NullSession {
  fn dispatch_resp(&mut self, _request: &[u8], _response: &mut Vec<u8>) {}

  fn get(&mut self, _key: &[u8]) -> Result<Option<Vec<u8>>, ScriptApiError> {
    Ok(None)
  }

  fn set(&mut self, _key: &[u8], _value: &[u8]) -> Result<(), ScriptApiError> {
    Ok(())
  }

  fn resp_protocol_version(&self) -> u8 {
    2
  }

  fn update_resp_protocol_version(&mut self, _version: u8) {}

  fn parse_resp_command_buffer(&mut self, _buffer: &[u8]) -> Option<RespCommand> {
    None
  }

  fn check_acl_permissions(&self, _command: RespCommand) -> bool {
    true
  }
}

/// 以生产入口执行 EVAL。
fn eval_script(store: &StoreScriptCache, cache: &mut SessionScriptCache, out: &mut Vec<u8>) {
  let args: [&[u8]; 2] = [SRC, b"0"];
  let lua_options = LuaOptions::default();
  let mut session = NullSession;
  let mut ctx = LuaSessionContext {
    args: &args,
    out,
    session_cache: cache,
    store_cache: store,
    session: &mut session,
    redis_version: "0.0.0",
    lua_options: &lua_options,
  };
  LuaCommands::try_eval(&mut ctx);
}

/// 以生产入口执行 SCRIPT LOAD。
fn script_load(store: &StoreScriptCache, cache: &mut SessionScriptCache, out: &mut Vec<u8>) {
  let args: [&[u8]; 1] = [SRC];
  let lua_options = LuaOptions::default();
  let mut session = NullSession;
  let mut ctx = LuaSessionContext {
    args: &args,
    out,
    session_cache: cache,
    store_cache: store,
    session: &mut session,
    redis_version: "0.0.0",
    lua_options: &lua_options,
  };
  LuaCommands::network_script_load(&mut ctx);
}

/// 以生产入口执行 SCRIPT FLUSH。
fn script_flush(store: &StoreScriptCache, cache: &mut SessionScriptCache, out: &mut Vec<u8>) {
  let args: [&[u8]; 0] = [];
  let lua_options = LuaOptions::default();
  let mut session = NullSession;
  let mut ctx = LuaSessionContext {
    args: &args,
    out,
    session_cache: cache,
    store_cache: store,
    session: &mut session,
    redis_version: "0.0.0",
    lua_options: &lua_options,
  };
  LuaCommands::network_script_flush(&mut ctx);
}

/// 以生产入口执行 EVALSHA。
fn eval_sha(
  store: &StoreScriptCache,
  cache: &mut SessionScriptCache,
  digest: &str,
  out: &mut Vec<u8>,
) {
  let args: [&[u8]; 2] = [digest.as_bytes(), b"0"];
  let lua_options = LuaOptions::default();
  let mut session = NullSession;
  let mut ctx = LuaSessionContext {
    args: &args,
    out,
    session_cache: cache,
    store_cache: store,
    session: &mut session,
    redis_version: "0.0.0",
    lua_options: &lua_options,
  };
  LuaCommands::try_evalsha(&mut ctx);
}

/// 判据一（确定性判别测试）：预插权威句柄 A 后，同摘要新句柄 B 经
/// try_add_or_toss 登记必须返回 false、B 就地被 dispose（is_disposed
/// 为 true）、全局持有句柄仍为 A。若移除生产方法 try_add_or_toss 内部
/// 的 dispose 调用，本测试必红（反向注入判别力）。
#[test]
fn try_add_or_toss_disposes_loser_and_keeps_winner() {
  let store = StoreScriptCache::default();
  let digest = SessionScriptCache::get_script_digest(SRC);

  // 胜方 A：生产 SCRIPT LOAD 登记。
  let mut cache_a = SessionScriptCache::default();
  let mut out = Vec::new();
  script_load(&store, &mut cache_a, &mut out);
  assert!(
    has(&out, digest.as_str().as_bytes()),
    "SCRIPT LOAD 应答异常: {out:?}"
  );
  let handle_a = store
    .try_get(&digest)
    .expect("SCRIPT LOAD 后全局应持有权威句柄");
  assert!(!handle_a.is_disposed());

  // 败方 B：独立会话在竞态窗口（全局检索落空）内新建同摘要句柄。
  let mut cache_b = SessionScriptCache::default();
  let options = RunnerCreateOptions::default();
  let mut load_out = Vec::new();
  let handle_b = {
    let mut global_handle = None;
    let loaded = cache_b.try_load_runner(SRC, &digest, &mut global_handle, &options, &mut load_out);
    let (_, created) = loaded.expect("竞态窗口装载应成功");
    created.expect("竞态窗口新建句柄应上升登记")
  };
  assert!(!Arc::ptr_eq(&handle_b, &handle_a), "败方应为独立新句柄");

  // 同摘要再登记：必须失败、败方就地 dispose、先到权威句柄不被覆写
  //（C# ConcurrentDictionary.TryAdd 键已存在不修改 + 败方 Dispose 契约）。
  assert!(
    !store.try_add_or_toss(digest, Arc::clone(&handle_b)),
    "败方登记必须失败（键已存在）"
  );
  assert!(
    handle_b.is_disposed(),
    "登记失败的败方句柄必须被 try_add_or_toss 就地 dispose"
  );
  let held = store.try_get(&digest).expect("全局缓存不应为空");
  assert!(
    Arc::ptr_eq(&held, &handle_a),
    "papaya insert 把先到权威句柄覆写成了败方句柄"
  );
}

/// 判据二（票面「测试验证点」闭环）：败方 try_add_or_toss 登记失败就地 toss 销毁，
/// 当次 EVAL 仍须以本地 runner 正常应答（无响应帧即挂死）；全局仍持有
/// 胜方句柄；下次检索清退败方并经全局恢复；SCRIPT FLUSH 后胜败双方
/// EVALSHA 均 NOSCRIPT、无旧 runner 残留。
#[test]
fn race_loser_toss_lifecycle_and_flush_closure() {
  let store = StoreScriptCache::default();
  let digest = SessionScriptCache::get_script_digest(SRC);
  let hex = digest.as_str().to_owned();

  // 1. 胜方 A 先到：生产 SCRIPT LOAD 登记。
  let mut cache_a = SessionScriptCache::default();
  let mut out = Vec::new();
  script_load(&store, &mut cache_a, &mut out);
  let handle_a = store.try_get(&digest).expect("胜方权威句柄应在全局缓存");

  // 2. 败方 B 竞态窗口装载（全局检索落空形态）。
  let mut cache_b = SessionScriptCache::default();
  let options = RunnerCreateOptions::default();
  let mut load_out = Vec::new();
  let handle_b = {
    let mut global_handle = None;
    let loaded = cache_b.try_load_runner(SRC, &digest, &mut global_handle, &options, &mut load_out);
    let (_, created) = loaded.expect("竞态窗口装载应成功");
    created.expect("竞态窗口新建句柄应上升登记")
  };

  // 3. 同摘要登记失败 → 单点 try_add_or_toss 就地 toss 败方句柄
  //（生产 try_eval/network_script_load 收敛后的同款契约）。
  assert!(
    !store.try_add_or_toss(digest, Arc::clone(&handle_b)),
    "败方登记必须失败"
  );
  assert!(
    handle_b.is_disposed(),
    "登记失败的败方句柄必须被 try_add_or_toss 就地 dispose"
  );

  // 4. 当次 EVAL 仍须以本地 runner 正常执行并写出响应帧
  //（C# TryEVAL 在 Dispose 败方句柄后仍以 TryLoad 返回的 runner 走
  // RunScriptForSession；清退只发生在下次检索）。
  let mut out_run = Vec::new();
  {
    let args: [&[u8]; 2] = [SRC, b"0"];
    let lua_options = LuaOptions::default();
    let mut session = NullSession;
    let mut ctx = LuaSessionContext {
      args: &args,
      out: &mut out_run,
      session_cache: &mut cache_b,
      store_cache: &store,
      session: &mut session,
      redis_version: "0.0.0",
      lua_options: &lua_options,
    };
    LuaCommands::run_script_for_session(&mut ctx, args.len(), &digest);
  }
  assert!(
    has(&out_run, INT7),
    "败方当次 EVAL 应以本地 runner 正常应答（缺响应帧即客户端挂死）: {out_run:?}"
  );

  // 5. 下次检索：已 toss 的败方句柄条目被清退。
  assert!(
    cache_b.try_get_runner(&digest).is_none(),
    "检索期应清退 disposed 败方句柄条目"
  );

  // 6. 全局权威仍是胜方 A（覆写缺陷判据），经 EVALSHA 从全局恢复执行。
  let held = store.try_get(&digest).expect("全局缓存不应为空");
  assert!(
    Arc::ptr_eq(&held, &handle_a),
    "全局权威句柄被败方覆写（ScriptHashKey papaya insert）"
  );
  let mut out_recover = Vec::new();
  eval_sha(&store, &mut cache_b, &hex, &mut out_recover);
  assert!(
    has(&out_recover, INT7),
    "清退后经全局 A 恢复执行应有响应帧: {out_recover:?}"
  );

  // 7. SCRIPT FLUSH（生产入口）→ 胜败双方会话后续 EVALSHA 均 NOSCRIPT。
  let mut out_flush = Vec::new();
  script_flush(&store, &mut cache_a, &mut out_flush);
  assert!(
    out_flush.starts_with(b"+OK"),
    "FLUSH 应答异常: {out_flush:?}"
  );

  let mut out_a = Vec::new();
  eval_sha(&store, &mut cache_a, &hex, &mut out_a);
  assert!(
    has(&out_a, NOSCRIPT) && !has(&out_a, INT7),
    "胜方会话 flush 后应 NOSCRIPT: {out_a:?}"
  );
  let mut out_b = Vec::new();
  eval_sha(&store, &mut cache_b, &hex, &mut out_b);
  assert!(
    has(&out_b, NOSCRIPT) && !has(&out_b, INT7),
    "败方会话 flush 后应 NOSCRIPT: {out_b:?}"
  );
  assert!(
    cache_a.is_empty() && cache_b.is_empty(),
    "flush 后不得残留旧 runner"
  );
}

/// 判据三（票面「危害一」闭环）：先到会话完成生产 EVAL 登记后，
/// 并发败方的同摘要再登记不得顶出其权威句柄；SCRIPT FLUSH 后先到
/// 会话的后续 EVALSHA 必须 NOSCRIPT，不得仍能执行已清退脚本。
#[test]
fn script_flush_invalidates_first_session_after_racy_second_add() {
  let store = StoreScriptCache::default();
  let digest = SessionScriptCache::get_script_digest(SRC);
  let hex = digest.as_str().to_owned();

  // 1. 先到会话 B 生产 EVAL：全局登记其权威句柄 B0。
  let mut cache_b = SessionScriptCache::default();
  let mut out = Vec::new();
  eval_script(&store, &mut cache_b, &mut out);
  assert!(has(&out, INT7), "先到 EVAL 应答异常: {out:?}");
  let handle_b0 = store
    .try_get(&digest)
    .expect("先到会话应已登记全局权威句柄");

  // 2. 后到会话 A 竞态窗口登记同摘要：失败并 toss（票面时序）。
  let mut cache_a = SessionScriptCache::default();
  let options = RunnerCreateOptions::default();
  let mut load_out = Vec::new();
  let handle_a1 = {
    let mut global_handle = None;
    let loaded = cache_a.try_load_runner(SRC, &digest, &mut global_handle, &options, &mut load_out);
    let (_, created) = loaded.expect("后到竞态窗口装载应成功");
    created.expect("后到竞态窗口新建句柄应上升登记")
  };
  assert!(
    !store.try_add_or_toss(digest, Arc::clone(&handle_a1)),
    "后到登记必须失败"
  );
  assert!(
    handle_a1.is_disposed(),
    "登记失败的败方句柄必须被 try_add_or_toss 就地 dispose"
  );
  assert!(
    !handle_b0.is_disposed(),
    "先到权威句柄不得被后到登记销毁或顶出"
  );

  // 3. 生产 SCRIPT FLUSH：逐个移除全局句柄并 dispose。
  let mut out_flush = Vec::new();
  script_flush(&store, &mut cache_a, &mut out_flush);
  assert!(
    out_flush.starts_with(b"+OK"),
    "FLUSH 应答异常: {out_flush:?}"
  );
  assert!(store.try_get(&digest).is_none(), "FLUSH 后全局应清空");

  // 4. 先到会话的后续 EVALSHA 必须 NOSCRIPT（危害一闭环判据），无旧 runner 残留。
  let mut out_b = Vec::new();
  eval_sha(&store, &mut cache_b, &hex, &mut out_b);
  assert!(
    has(&out_b, NOSCRIPT) && !has(&out_b, INT7),
    "SCRIPT FLUSH 后先到会话仍可执行已清退脚本: {out_b:?}"
  );
  assert!(cache_b.is_empty(), "flush 后先到会话不得残留旧 runner");
}
