//! 动态参数切片借用直通验证：大载荷 KEYS/ARGV、多参数、二进制字节与
//! 大脚本源均经 `&[&[u8]]` 视图零拷贝进入 preamble 与编译装载。
//!
//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:
//! UnsafeRunPreambleForSession（parseState.GetArgSliceByRef → state.TryPushBuffer
//! 直通接收缓冲，全程不物化参数副本）与 libs/server/Lua/LuaCommands.cs:
//! TryEVAL（script = ref parseState.GetArgSliceByRef(0) + stackalloc digest）、
//! TryEVALSHA（miss 时 AsciiUtils.ToLowerInPlace 重试的大小写无关契约）。
//!
//! 判据：16 参数（超栈内联容量的多参形态）、4096 字节键/实参载荷、含
//! 0x00/0xFF 的二进制字节、128KB 脚本源逐字节保真，响应帧与原协议一致。

use wlua::{
  LuaCommands, LuaOptions, LuaSessionContext, ScriptApiError, ScriptingApi, SessionScriptCache,
  StoreScriptCache,
};
use wresp::command::RespCommand;

const INT7: &[u8] = b":7\r\n";
const NOSCRIPT: &[u8] = b"NOSCRIPT";

/// 字节子序列包含判定（响应帧断言用）。
fn has(hay: &[u8], needle: &[u8]) -> bool {
  hay.windows(needle.len()).any(|w| w == needle)
}

/// 最小会话面：本文件场景脚本不含 redis.call，无任何落地调用。
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

/// 以生产入口执行 EVAL（args 为调用方持有的切片视图数组）。
fn eval_script(
  store: &StoreScriptCache,
  cache: &mut SessionScriptCache,
  args: &[&[u8]],
) -> Vec<u8> {
  let lua_options = LuaOptions::default();
  let mut session = NullSession;
  let mut out = Vec::new();
  let mut ctx = LuaSessionContext {
    args,
    out: &mut out,
    session_cache: cache,
    store_cache: store,
    session: &mut session,
    redis_version: "0.0.0",
    lua_options: &lua_options,
  };
  LuaCommands::try_eval(&mut ctx);
  out
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

/// 大载荷多参数直通：16 KEYS + 16 ARGV（各 4096 字节，含二进制字节）逐项保真。
#[test]
fn large_payload_keys_argv_pass_through_by_ref() {
  // 校验脚本：KEYS[i] = 'K'*4096 .. i，ARGV 前 15 项 = 'A'*4096 .. i，
  // ARGV[16] 为含 0x00/0xFF 的二进制串（string.char 对照）。
  const SRC: &[u8] = b"
    if #KEYS ~= 16 then return 'kn:' .. #KEYS end
    for i = 1, 16 do
      if KEYS[i] ~= string.rep('K', 4096) .. tostring(i) then return 'k' .. i end
    end
    if #ARGV ~= 16 then return 'an:' .. #ARGV end
    for i = 1, 15 do
      if ARGV[i] ~= string.rep('A', 4096) .. tostring(i) then return 'a' .. i end
    end
    if ARGV[16] ~= string.char(0, 255, 128, 1, 254) then return 'raw' end
    return 'OK'
  ";

  // 视图底座：载荷缓冲持本测试全程，视图数组仅借其切片
  // （被测路径全程零拷贝；缓冲构造是测试输入的装配，非被测面）
  let keys: Vec<Vec<u8>> = (1..=16)
    .map(|i| {
      let mut k = vec![b'K'; 4096];
      k.extend_from_slice(i.to_string().as_bytes());
      k
    })
    .collect();
  let mut argvs: Vec<Vec<u8>> = (1..=15)
    .map(|i| {
      let mut a = vec![b'A'; 4096];
      a.extend_from_slice(i.to_string().as_bytes());
      a
    })
    .collect();
  argvs.push(vec![0_u8, 255, 128, 1, 254]);

  let mut args: Vec<&[u8]> = Vec::with_capacity(2 + keys.len() + argvs.len());
  args.push(SRC);
  args.push(b"16");
  args.extend(keys.iter().map(Vec::as_slice));
  args.extend(argvs.iter().map(Vec::as_slice));

  let store = StoreScriptCache::default();
  let mut cache = SessionScriptCache::default();
  let out = eval_script(&store, &mut cache, &args);
  assert_eq!(out, b"$2\r\nOK\r\n", "大载荷多参数应逐字节保真: {out:?}");
}

/// 大脚本源直通：128KB 源码经视图装载编译，执行结果与原协议一致。
#[test]
fn large_script_source_passes_through_by_ref() {
  let source = format!("-- {}\nreturn 'BIG'", "x".repeat(128 * 1024));
  let args: [&[u8]; 2] = [source.as_bytes(), b"0"];

  let store = StoreScriptCache::default();
  let mut cache = SessionScriptCache::default();
  let out = eval_script(&store, &mut cache, &args);
  assert_eq!(out, b"$3\r\nBIG\r\n", "大脚本源应完整装载执行: {out:?}");
}

/// EVALSHA 大小写无关契约：登记后以大写 hex 调用（含会话缓存整体 miss
/// 形态）仍命中执行，对标 C# TryEVALSHA miss 时 ToLowerInPlace 重试语义。
#[test]
fn uppercase_evalsha_digest_resolves() {
  const SRC: &[u8] = b"return 7";

  let store = StoreScriptCache::default();
  let mut cache_loader = SessionScriptCache::default();
  let digest = SessionScriptCache::get_script_digest(SRC);
  let hex_upper = digest.as_str().to_ascii_uppercase();

  // 全局登记（生产 SCRIPT LOAD 形态）；执行会话与查询会话分离，
  // 保证 EVALSHA 走「会话 miss → 全局检索」的完整解析链。
  let args_load: [&[u8]; 1] = [SRC];
  let mut load_out = Vec::new();
  let mut loader_session = NullSession;
  let mut ctx = LuaSessionContext {
    args: &args_load,
    out: &mut load_out,
    session_cache: &mut cache_loader,
    store_cache: &store,
    session: &mut loader_session,
    redis_version: "0.0.0",
    lua_options: &LuaOptions::default(),
  };
  LuaCommands::network_script_load(&mut ctx);
  assert!(
    load_out.starts_with(b"$40\r\n"),
    "SCRIPT LOAD 应答异常: {load_out:?}"
  );

  // 空会话以大写 hex 调用：必须命中（NOSCRIPT 即失败）。
  let mut cache_query = SessionScriptCache::default();
  let mut out = Vec::new();
  eval_sha(&store, &mut cache_query, &hex_upper, &mut out);
  assert!(
    has(&out, INT7) && !has(&out, NOSCRIPT),
    "大写摘要 EVALSHA 应命中执行: {out:?}"
  );
}
