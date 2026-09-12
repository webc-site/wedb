//! Lua 命令域集成测试（真实 wkv 库驱动 EVAL / EVALSHA / SCRIPT 全链路）
//!
//! 覆盖：redis.call 直连存储（SET/GET/INCR）、KEYS/ARGV 装配、脚本缓存
//! 复用（EVALSHA 命中 / SCRIPT LOAD / EXISTS / FLUSH）、cjson 编解码、
//! 错误形态（NOSCRIPT / 编译失败 / 未启用 Lua / NOPERM）。

use std::sync::Arc;

use aok::OK;
use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wlua::{
  LuaCommands, LuaOptions, LuaSessionContext, ScriptingApi, SessionScriptCache, StoreScriptCache,
};
use wnode::{
  databases::garnet_database::DEFAULT_VERSION_MAP_SIZE,
  storage::{session::storage_session::StorageSession, storage_scripting_api::StorageScriptingApi},
};
use wtxn::WatchVersionMap;

type TestStore = Arc<WedbStore<SegmentedDevice>>;

/// 独立 WATCH 版本表（生产由 GarnetDatabase 持有；会话级测试用默认桶数实例）
fn test_version_map() -> Arc<WatchVersionMap> {
  Arc::new(WatchVersionMap::new(DEFAULT_VERSION_MAP_SIZE))
}

/// 打开临时文件库
fn open_store(tag: &str) -> aok::Result<(TempDir, TestStore)> {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag))?);
  let config = StoreConfig::new(16384, 65536, 64, 0.5)?;
  let store = Arc::new(WedbStore::open(config, device)?);
  Ok((dir, store))
}

/// EVAL script numkeys keys... argv...
fn eval_args(script: &str, keys: &[&[u8]], argv: &[&[u8]]) -> Vec<Vec<u8>> {
  let mut args = vec![
    script.as_bytes().to_vec(),
    keys.len().to_string().into_bytes(),
  ];
  args.extend(keys.iter().map(|k| k.to_vec()));
  args.extend(argv.iter().map(|a| a.to_vec()));
  args
}

/// EVALSHA sha1 numkeys keys... argv...
fn evalsha_args(digest: &str, keys: &[&[u8]], argv: &[&[u8]]) -> Vec<Vec<u8>> {
  let mut args = vec![
    digest.as_bytes().to_vec(),
    keys.len().to_string().into_bytes(),
  ];
  args.extend(keys.iter().map(|k| k.to_vec()));
  args.extend(argv.iter().map(|a| a.to_vec()));
  args
}

/// 会话面测试标尺：EVAL / EVALSHA / SCRIPT 子命令统一驱动。
///
/// 两个独立生命周期：'s = 缓存内 runner 的会话面借用，'a = 标尺自身借用
/// （&mut SessionScriptCache<'s> 的不变性要求两者分离才能逐次重借用）。
struct Harness<'a, A: ScriptingApi> {
  cache: &'a mut SessionScriptCache,
  store: &'a StoreScriptCache,
  api: &'a mut A,
  options: &'a LuaOptions,
}

impl<'a, A: ScriptingApi> Harness<'a, A> {
  fn with<F>(&mut self, args: &[Vec<u8>], out: &mut Vec<u8>, entry: F) -> bool
  where
    F: FnOnce(&mut LuaSessionContext<'_, A>) -> bool,
  {
    // 会话缓冲按命令重置（对标 dcurr 复位）。
    out.clear();
    let mut ctx = LuaSessionContext {
      args,
      out,
      session_cache: self.cache,
      store_cache: self.store,
      session: self.api,
      lua_enabled: true,
      txn_mode: false,
      redis_version: "7.4.0",
      lua_options: self.options,
    };
    entry(&mut ctx)
  }

  fn eval(&mut self, args: &[Vec<u8>], out: &mut Vec<u8>) -> bool {
    self.with(args, out, LuaCommands::try_eval)
  }

  fn evalsha(&mut self, args: &[Vec<u8>], out: &mut Vec<u8>) -> bool {
    self.with(args, out, LuaCommands::try_evalsha)
  }

  fn script_load(&mut self, args: &[Vec<u8>], out: &mut Vec<u8>) -> bool {
    self.with(args, out, LuaCommands::network_script_load)
  }

  fn script_exists(&mut self, args: &[Vec<u8>], out: &mut Vec<u8>) -> bool {
    self.with(args, out, LuaCommands::network_script_exists)
  }

  fn script_flush(&mut self, args: &[Vec<u8>], out: &mut Vec<u8>) -> bool {
    self.with(args, out, LuaCommands::network_script_flush)
  }
}

/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:CanDoEvalSetGet
/// redis.call 直连存储端到端：SET → GET → INCR。
#[test]
fn test_eval_redis_call_roundtrip() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_store("lua-eval.db")?;
    let session = store.new_session()?;
    let storage = StorageSession::new(session.enter_batch(), test_version_map());
    let mut api = StorageScriptingApi::new(&storage, None);

    let mut cache = SessionScriptCache::default();
    let store_cache = StoreScriptCache::default();
    let options = LuaOptions::default();
    let mut out = Vec::new();
    let mut harness = Harness {
      cache: &mut cache,
      store: &store_cache,
      api: &mut api,
      options: &options,
    };

    // SET via redis.call + KEYS/ARGV
    let args = eval_args(
      "return redis.call('SET', KEYS[1], ARGV[1])",
      &[b"lua:k"],
      &[b"hello-lua"],
    );
    assert!(harness.eval(&args, &mut out));
    assert_eq!(out, b"$2\r\nOK\r\n");

    // GET via redis.call
    let args = eval_args("return redis.call('GET', KEYS[1])", &[b"lua:k"], &[]);
    assert!(harness.eval(&args, &mut out));
    assert_eq!(out, b"$9\r\nhello-lua\r\n");

    // GET miss → Lua false → RESP2 null（Redis 语义：false 而非 nil）
    let args = eval_args("return redis.call('GET', KEYS[1])", &[b"lua:miss"], &[]);
    assert!(harness.eval(&args, &mut out));
    assert_eq!(out, b"$-1\r\n");

    // INCR / INCRBY 经 RESP 分派（非特例路径）
    let args = eval_args("return redis.call('INCR', KEYS[1])", &[b"lua:cnt"], &[]);
    assert!(harness.eval(&args, &mut out));
    assert_eq!(out, b":1\r\n");

    let args = eval_args(
      "return redis.call('INCRBY', KEYS[1], 41)",
      &[b"lua:cnt"],
      &[],
    );
    assert!(harness.eval(&args, &mut out));
    assert_eq!(out, b":42\r\n");

    // 直读存储核对
    let direct = match StorageScriptingApi::get(&mut api, b"lua:k") {
      Ok(v) => v,
      Err(e) => panic!("直读失败: {e}"),
    };
    assert_eq!(direct, Some(b"hello-lua".to_vec()));
    OK
  })
}

/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:CanDoScriptFlush
/// 脚本缓存全链路：SCRIPT LOAD → 摘要 → EVALSHA 命中 → EXISTS → FLUSH。
#[test]
fn test_script_cache_lifecycle() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_store("lua-script.db")?;
    let session = store.new_session()?;
    let storage = StorageSession::new(session.enter_batch(), test_version_map());
    let mut api = StorageScriptingApi::new(&storage, None);

    let mut cache = SessionScriptCache::default();
    let store_cache = StoreScriptCache::default();
    let options = LuaOptions::default();
    let mut out = Vec::new();
    let mut harness = Harness {
      cache: &mut cache,
      store: &store_cache,
      api: &mut api,
      options: &options,
    };

    // SCRIPT LOAD
    let load_args = vec![b"return tonumber(ARGV[1]) + 1".to_vec()];
    assert!(harness.script_load(&load_args, &mut out));
    assert!(out.starts_with(b"$40\r\n"));
    let digest = String::from_utf8_lossy(&out[5..45]).to_string();

    // SCRIPT EXISTS（存在 + 不存在）
    let exists_args = vec![digest.clone().into_bytes(), vec![b'0'; 40]];
    assert!(harness.script_exists(&exists_args, &mut out));
    assert_eq!(out, b"*2\r\n:1\r\n:0\r\n");

    // EVALSHA 命中：返回 ARGV[1] + 1
    let args = evalsha_args(&digest, &[], &[b"41"]);
    assert!(harness.evalsha(&args, &mut out));
    assert_eq!(out, b":42\r\n");

    // EVALSHA 未命中 → NOSCRIPT
    let fake = "f".repeat(40);
    let args = evalsha_args(&fake, &[], &[]);
    assert!(harness.evalsha(&args, &mut out));
    assert_eq!(out, b"-NOSCRIPT No matching script. Please use EVAL.\r\n");

    // EVALSHA 摘要长度不合法 → NOSCRIPT
    let args = evalsha_args("short", &[], &[]);
    assert!(harness.evalsha(&args, &mut out));
    assert!(out.starts_with(b"-NOSCRIPT"));

    // SCRIPT FLUSH（ASYNC 选项合法）→ +OK
    let flush_args = vec![b"ASYNC".to_vec()];
    assert!(harness.script_flush(&flush_args, &mut out));
    assert_eq!(out, b"+OK\r\n");

    // FLUSH 后 EXISTS → 0
    let exists_args = vec![digest.into_bytes()];
    assert!(harness.script_exists(&exists_args, &mut out));
    assert_eq!(out, b"*1\r\n:0\r\n");

    // 非法 FLUSH 选项
    let bad_args = vec![b"NOW".to_vec()];
    assert!(harness.script_flush(&bad_args, &mut out));
    assert!(out.starts_with(b"-ERR SCRIPT FLUSH"));
    OK
  })
}

/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:CJson
/// cjson 编解码 + 沙箱语义（多返回值取首个、错误形态）。
#[test]
fn test_eval_cjson_and_semantics() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_store("lua-cjson.db")?;
    let session = store.new_session()?;
    let storage = StorageSession::new(session.enter_batch(), test_version_map());
    let mut api = StorageScriptingApi::new(&storage, None);

    let mut cache = SessionScriptCache::default();
    let store_cache = StoreScriptCache::default();
    let options = LuaOptions::default();
    let mut out = Vec::new();
    let mut harness = Harness {
      cache: &mut cache,
      store: &store_cache,
      api: &mut api,
      options: &options,
    };

    // cjson.encode：对象 → JSON
    let args = eval_args("return cjson.encode({a = 'x', b = 3})", &[], &[]);
    assert!(harness.eval(&args, &mut out));
    let body = String::from_utf8_lossy(&out).to_string();
    assert!(body.contains("\"a\":\"x\""), "got {body}");
    assert!(body.contains("\"b\":3"), "got {body}");

    // cjson.decode：JSON → 表 → 取字段
    let args = eval_args(
      "local t = cjson.decode(ARGV[1]) return t.k",
      &[],
      &[br#"{"k":"zz"}"#],
    );
    assert!(harness.eval(&args, &mut out));
    assert_eq!(out, b"$2\r\nzz\r\n");

    // redis.sha1hex（空串基准）
    let args = eval_args("return redis.sha1hex('')", &[], &[]);
    assert!(harness.eval(&args, &mut out));
    assert_eq!(out, b"$40\r\nda39a3ee5e6b4b0d3255bfef95601890afd80709\r\n");

    // 多返回值仅取首个（Redis EVAL 语义）
    let args = eval_args("return 1, 2", &[], &[]);
    assert!(harness.eval(&args, &mut out));
    assert_eq!(out, b":1\r\n");

    // nil → RESP2 null
    let args = eval_args("return nil", &[], &[]);
    assert!(harness.eval(&args, &mut out));
    assert_eq!(out, b"$-1\r\n");

    // 脚本错误 → -ERR Lua encountered an error: ...
    let args = eval_args("error('boom')", &[], &[]);
    assert!(harness.eval(&args, &mut out));
    assert!(
      out.starts_with(b"-ERR Lua encountered an error: "),
      "got {out:?}"
    );

    // 编译失败 → -ERR Compilation error
    let args = eval_args("return ]]", &[], &[]);
    assert!(harness.eval(&args, &mut out));
    assert!(out.starts_with(b"-Compilation error: "), "got {out:?}");

    // 参数个数错误
    let bad_arity = vec![b"return 1".to_vec()];
    assert!(harness.eval(&bad_arity, &mut out));
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'EVAL' command\r\n"
    );

    // numkeys 非整数
    let bad_numkeys = vec![b"return 1".to_vec(), b"nan".to_vec()];
    assert!(harness.eval(&bad_numkeys, &mut out));
    assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");
    OK
  })
}

/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:PermissionsEnforced
/// ACL 受限面：redis.call 的 NOPERM 形态。
#[test]
fn test_eval_acl_denied() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_store("lua-acl.db")?;
    let session = store.new_session()?;
    let storage = StorageSession::new(session.enter_batch(), test_version_map());
    let mut api = StorageScriptingApi::new(&storage, Some(vec!["GET".into()]));

    let mut cache = SessionScriptCache::default();
    let store_cache = StoreScriptCache::default();
    let options = LuaOptions::default();
    let mut out = Vec::new();
    let mut harness = Harness {
      cache: &mut cache,
      store: &store_cache,
      api: &mut api,
      options: &options,
    };

    // SET 无权限 → NOPERM 错误
    let args = eval_args("return redis.call('SET', KEYS[1], 'v')", &[b"k"], &[]);
    assert!(harness.eval(&args, &mut out));
    // redis.call 权限拒绝以 Lua error 形态上抛（error_wrapper），经 run_common 包装。
    assert!(
      out.starts_with(b"-ERR ") && out.windows(6).any(|w| w == b"NOPERM"),
      "got {}",
      String::from_utf8_lossy(&out)
    );

    // GET 有权限 → miss → Lua false → RESP2 null
    let args = eval_args("return redis.call('GET', KEYS[1])", &[b"k"], &[]);
    assert!(harness.eval(&args, &mut out));
    assert_eq!(out, b"$-1\r\n");
    OK
  })
}
