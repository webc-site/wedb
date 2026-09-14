//! Lua 命令域集成测试（真实 wkv 库驱动 EVAL / EVALSHA / SCRIPT 全链路）
//!
//! 对标 test/standalone/Garnet.test.scripting/LuaScriptTests.cs：
//! CanDoEvalSetGet / CanDoScriptFlush / CJson / PermissionsEnforced /
//! ComplexLuaTest1-3 / ScriptExistsErrors。
//!
//! 驱动形态：RespServerSession + StoreGarnetApi 生产装配路径整链路消费
//! （redis.call 经 RespScriptingApi 重入会话主循环，ACL 门控同步生效）。

use std::sync::Arc;

use parking_lot::Mutex;
use tempfile::TempDir;
use wacl::{AccessControlList, AclParser, GarnetAclAuthenticator};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::StoreGarnetApi,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};

type TestStore = Arc<WedbStore<SegmentedDevice>>;

/// 打开临时文件库
fn open_store(tag: &str) -> (TempDir, TestStore) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let mut config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  (dir, store)
}

/// 构造 enable_lua 会话（生产装配路径：StoreGarnetApi 注入）
fn lua_session(store: &TestStore) -> RespServerSession {
  let session = store.new_session().unwrap();
  let mut s = RespServerSession::new(
    1,
    RespServerSessionOptions {
      enable_lua: true,
      ..RespServerSessionOptions::default()
    },
  );
  s.set_garnet_api(StoreGarnetApi::new(session));
  s
}

/// RESP 数组帧
fn resp(parts: &[&[u8]]) -> Vec<u8> {
  let mut buf = format!("*{}\r\n", parts.len()).into_bytes();
  for p in parts {
    buf.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
    buf.extend_from_slice(p);
    buf.extend_from_slice(b"\r\n");
  }
  buf
}

/// 消费一条命令并取回响应
fn cmd(s: &mut RespServerSession, parts: Vec<Vec<u8>>) -> Vec<u8> {
  let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
  s.try_consume_messages(&resp(&refs)).expect("协议违规");
  s.take_output()
}

/// EVAL script numkeys keys... argv...
fn eval_parts(script: &str, keys: &[&[u8]], argv: &[&[u8]]) -> Vec<Vec<u8>> {
  let mut parts = vec![
    b"EVAL".to_vec(),
    script.as_bytes().to_vec(),
    keys.len().to_string().into_bytes(),
  ];
  parts.extend(keys.iter().map(|k| k.to_vec()));
  parts.extend(argv.iter().map(|a| a.to_vec()));
  parts
}

/// EVALSHA sha1 numkeys keys... argv...
fn evalsha_parts(digest: &str, keys: &[&[u8]], argv: &[&[u8]]) -> Vec<Vec<u8>> {
  let mut parts = vec![
    b"EVALSHA".to_vec(),
    digest.as_bytes().to_vec(),
    keys.len().to_string().into_bytes(),
  ];
  parts.extend(keys.iter().map(|k| k.to_vec()));
  parts.extend(argv.iter().map(|a| a.to_vec()));
  parts
}

/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:CanDoEvalSetGet
/// redis.call 直连存储端到端：SET → GET → INCR。
#[test]
fn test_eval_redis_call_roundtrip() {
  let (_dir, store) = open_store("lua-eval.db");
  let mut s = lua_session(&store);

  // SET via redis.call + KEYS/ARGV
  let out = cmd(
    &mut s,
    eval_parts(
      "return redis.call('SET', KEYS[1], ARGV[1])",
      &[b"lua:k"],
      &[b"hello-lua"],
    ),
  );
  assert_eq!(out, b"$2\r\nOK\r\n");

  // GET via redis.call
  let out = cmd(
    &mut s,
    eval_parts("return redis.call('GET', KEYS[1])", &[b"lua:k"], &[]),
  );
  assert_eq!(out, b"$9\r\nhello-lua\r\n");

  // GET miss → Lua false → RESP2 null（Redis 语义：false 而非 nil）
  let out = cmd(
    &mut s,
    eval_parts("return redis.call('GET', KEYS[1])", &[b"lua:miss"], &[]),
  );
  assert_eq!(out, b"$-1\r\n");

  // INCR / INCRBY 经 RESP 分派（非特例路径）
  let out = cmd(
    &mut s,
    eval_parts("return redis.call('INCR', KEYS[1])", &[b"lua:cnt"], &[]),
  );
  assert_eq!(out, b":1\r\n");

  let out = cmd(
    &mut s,
    eval_parts(
      "return redis.call('INCRBY', KEYS[1], 41)",
      &[b"lua:cnt"],
      &[],
    ),
  );
  assert_eq!(out, b":42\r\n");

  // 直读存储核对（同会话 RESP 面）
  let out = cmd(&mut s, vec![b"GET".to_vec(), b"lua:k".to_vec()]);
  assert_eq!(out, b"$9\r\nhello-lua\r\n");
}

/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:CanDoScriptFlush
/// 脚本缓存全链路：SCRIPT LOAD → 摘要 → EVALSHA 命中 → EXISTS → FLUSH。
#[test]
fn test_script_cache_lifecycle() {
  let (_dir, store) = open_store("lua-script.db");
  let mut s = lua_session(&store);

  // SCRIPT LOAD
  let out = cmd(
    &mut s,
    vec![
      b"SCRIPT".to_vec(),
      b"LOAD".to_vec(),
      b"return tonumber(ARGV[1]) + 1".to_vec(),
    ],
  );
  assert!(out.starts_with(b"$40\r\n"), "got {out:?}");
  let digest = String::from_utf8_lossy(&out[5..45]).to_string();

  // SCRIPT EXISTS（存在 + 不存在）
  let out = cmd(
    &mut s,
    vec![
      b"SCRIPT".to_vec(),
      b"EXISTS".to_vec(),
      digest.clone().into_bytes(),
      vec![b'0'; 40],
    ],
  );
  assert_eq!(out, b"*2\r\n:1\r\n:0\r\n");

  // EVALSHA 命中：返回 ARGV[1] + 1
  let out = cmd(&mut s, evalsha_parts(&digest, &[], &[b"41"]));
  assert_eq!(out, b":42\r\n");

  // EVALSHA 未命中 → NOSCRIPT
  let fake = "f".repeat(40);
  let out = cmd(&mut s, evalsha_parts(&fake, &[], &[]));
  assert_eq!(out, b"-NOSCRIPT No matching script. Please use EVAL.\r\n");

  // EVALSHA 摘要长度不合法 → NOSCRIPT
  let out = cmd(&mut s, evalsha_parts("short", &[], &[]));
  assert!(out.starts_with(b"-NOSCRIPT"), "got {out:?}");

  // SCRIPT FLUSH（ASYNC 选项合法）→ +OK
  let out = cmd(
    &mut s,
    vec![b"SCRIPT".to_vec(), b"FLUSH".to_vec(), b"ASYNC".to_vec()],
  );
  assert_eq!(out, b"+OK\r\n");

  // FLUSH 后 EXISTS → 0
  let out = cmd(
    &mut s,
    vec![b"SCRIPT".to_vec(), b"EXISTS".to_vec(), digest.into_bytes()],
  );
  assert_eq!(out, b"*1\r\n:0\r\n");

  // 非法 FLUSH 选项
  let out = cmd(
    &mut s,
    vec![b"SCRIPT".to_vec(), b"FLUSH".to_vec(), b"NOW".to_vec()],
  );
  assert!(out.starts_with(b"-ERR SCRIPT FLUSH"), "got {out:?}");
}

/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:CJson
/// cjson 编解码 + 沙箱语义（多返回值取首个、错误形态）。
#[test]
fn test_eval_cjson_and_semantics() {
  let (_dir, store) = open_store("lua-cjson.db");
  let mut s = lua_session(&store);

  // cjson.encode：对象 → JSON
  let out = cmd(
    &mut s,
    eval_parts("return cjson.encode({a = 'x', b = 3})", &[], &[]),
  );
  let body = String::from_utf8_lossy(&out).to_string();
  assert!(body.contains("\"a\":\"x\""), "got {body}");
  assert!(body.contains("\"b\":3"), "got {body}");

  // cjson.decode：JSON → 表 → 取字段
  let out = cmd(
    &mut s,
    eval_parts(
      "local t = cjson.decode(ARGV[1]) return t.k",
      &[],
      &[br#"{"k":"zz"}"#],
    ),
  );
  assert_eq!(out, b"$2\r\nzz\r\n");

  // redis.sha1hex（空串基准）
  let out = cmd(&mut s, eval_parts("return redis.sha1hex('')", &[], &[]));
  assert_eq!(out, b"$40\r\nda39a3ee5e6b4b0d3255bfef95601890afd80709\r\n");

  // 多返回值仅取首个（Redis EVAL 语义）
  let out = cmd(&mut s, eval_parts("return 1, 2", &[], &[]));
  assert_eq!(out, b":1\r\n");

  // nil → RESP2 null
  let out = cmd(&mut s, eval_parts("return nil", &[], &[]));
  assert_eq!(out, b"$-1\r\n");

  // 脚本错误 → -ERR Lua encountered an error: ...
  let out = cmd(&mut s, eval_parts("error('boom')", &[], &[]));
  assert!(
    out.starts_with(b"-ERR Lua encountered an error: "),
    "got {out:?}"
  );

  // 编译失败 → -ERR Compilation error
  let out = cmd(&mut s, eval_parts("return ]]", &[], &[]));
  assert!(out.starts_with(b"-Compilation error: "), "got {out:?}");

  // 参数个数错误
  let out = cmd(&mut s, vec![b"EVAL".to_vec(), b"return 1".to_vec()]);
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'EVAL' command\r\n"
  );

  // numkeys 非整数
  let out = cmd(
    &mut s,
    vec![b"EVAL".to_vec(), b"return 1".to_vec(), b"nan".to_vec()],
  );
  assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");
}

/// 挂载 ACL 认证器并完成 AUTH（C# ConnectionMultiplexer authUsername 形态）
fn acl_session(
  store: &TestStore,
  acl: &Arc<AccessControlList>,
  user: &str,
  pass: &str,
) -> RespServerSession {
  let mut s = lua_session(store);
  s.attach_acl(
    Some(Arc::new(Mutex::new(GarnetAclAuthenticator::new(
      Arc::clone(acl),
    )))),
    None,
  );
  let out = cmd(
    &mut s,
    vec![
      b"AUTH".to_vec(),
      user.as_bytes().to_vec(),
      pass.as_bytes().to_vec(),
    ],
  );
  assert_eq!(out, b"+OK\r\n", "AUTH {user} 必须成功");
  s
}

/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:PermissionsEnforced
/// ACL 受限面：allow 管理员 / deny（-get -cluster|myid）/ denyset（-set）。
#[test]
fn test_eval_acl_permissions() {
  let (_dir, store) = open_store("lua-acl.db");
  let acl = Arc::new(AccessControlList::new("", None).unwrap());
  AclParser::parse_acl_rule("user deny on >pw +@all -get -acl -cluster|myid", Some(&acl)).unwrap();
  AclParser::parse_acl_rule("user denyset on >pw2 +@all -set", Some(&acl)).unwrap();

  // allow 管理员连接（默认 +@all，无认证器挂载 → 门控全放行）
  let mut allow = lua_session(&store);
  let out = cmd(
    &mut allow,
    vec![b"SET".to_vec(), b"foo".to_vec(), b"bar".to_vec()],
  );
  assert_eq!(out, b"+OK\r\n");

  let out = cmd(
    &mut allow,
    eval_parts("return redis.call('GET', 'foo')", &[], &[]),
  );
  assert_eq!(out, b"$3\r\nbar\r\n");

  // allow CLUSTER MYID：不报 NOPERM
  let out = cmd(
    &mut allow,
    eval_parts("return redis.call('CLUSTER', 'MYID')", &[], &[]),
  );
  assert!(
    !out.windows(6).any(|w| w == b"NOPERM"),
    "got {}",
    String::from_utf8_lossy(&out)
  );

  // deny 连接：GET / CLUSTER MYID 均 NOPERM
  let mut deny = acl_session(&store, &acl, "deny", "pw");
  let out = cmd(
    &mut deny,
    eval_parts("return redis.call('GET', 'foo')", &[], &[]),
  );
  assert!(
    out.windows(6).any(|w| w == b"NOPERM"),
    "got {}",
    String::from_utf8_lossy(&out)
  );
  let out = cmd(
    &mut deny,
    eval_parts("return redis.call('CLUSTER', 'MYID')", &[], &[]),
  );
  assert!(
    out.windows(6).any(|w| w == b"NOPERM"),
    "got {}",
    String::from_utf8_lossy(&out)
  );

  // denyset 连接：SET 走 LuaRunner 独立快速路径，显式覆盖 → NOPERM；GET 放行
  let mut denyset = acl_session(&store, &acl, "denyset", "pw2");
  let out = cmd(
    &mut denyset,
    eval_parts("return redis.call('SET', 'foo', 'baz')", &[], &[]),
  );
  assert!(
    out.windows(6).any(|w| w == b"NOPERM"),
    "got {}",
    String::from_utf8_lossy(&out)
  );
  let out = cmd(
    &mut denyset,
    eval_parts("return redis.call('GET', 'foo')", &[], &[]),
  );
  assert_eq!(out, b"$3\r\nbar\r\n");
}

/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:ComplexLuaTest1
#[test]
fn complex_lua_test_1() {
  let (_dir, store) = open_store("lua-complex-1.db");
  let mut s = lua_session(&store);

  let script = "local setArgs = {}\nfor _, key in ipairs(KEYS) do\n  table.insert(setArgs, key)\nend\nunpack(KEYS)\nreturn redis.status_reply(table.concat(setArgs))";
  let out = cmd(
    &mut s,
    eval_parts(script, &[b"key1", b"key2"], &[b"value", b"1", b"60000"]),
  );
  assert_eq!(out, b"$8\r\nkey1key2\r\n");

  let out = cmd(
    &mut s,
    eval_parts(script, &[b"key1"], &[b"value", b"1", b"60000"]),
  );
  assert_eq!(out, b"$4\r\nkey1\r\n");

  let out = cmd(&mut s, eval_parts(script, &[], &[b"value", b"1", b"60000"]));
  assert_eq!(out, b"$0\r\n\r\n");
}

/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:ComplexLuaTest2
#[test]
fn complex_lua_test_2() {
  let (_dir, store) = open_store("lua-complex-2.db");
  let mut s = lua_session(&store);

  let script = "for _, key in ipairs(KEYS) do\n  redis.call('set', key, ARGV[1])\nend\nreturn redis.status_reply('OK')";
  let out = cmd(&mut s, eval_parts(script, &[b"key1", b"key2"], &[b"foo"]));
  assert_eq!(out, b"$2\r\nOK\r\n");

  let script2 = "local values = redis.call('mget', unpack(KEYS))\nfor i, _ in ipairs(KEYS) do\n  if values[i] ~= ARGV[1] then\n    return false\n  end\nend\nredis.call('del', unpack(KEYS))\nreturn redis.status_reply('OK')";
  let out = cmd(&mut s, eval_parts(script2, &[b"key3"], &[b"foo"]));
  assert_eq!(out, b"$-1\r\n");

  let out = cmd(&mut s, eval_parts(script2, &[b"key1", b"key2"], &[b"foo"]));
  assert_eq!(out, b"$2\r\nOK\r\n");
}

/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:ComplexLuaTest3
#[test]
fn complex_lua_test_3() {
  let (_dir, store) = open_store("lua-complex-3.db");
  let mut s = lua_session(&store);

  let script = "return redis.call('mget', unpack(KEYS))";
  let out = cmd(&mut s, eval_parts(script, &[b"key1", b"key2"], &[]));
  assert_eq!(out, b"*2\r\n$-1\r\n$-1\r\n");
}

/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:ScriptExistsErrors
#[test]
fn script_exists_errors() {
  let (_dir, store) = open_store("lua-exists-err.db");
  let mut s = lua_session(&store);

  let out = cmd(&mut s, vec![b"SCRIPT".to_vec(), b"EXISTS".to_vec()]);
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'script|exists' command\r\n"
  );
}
