//! Lua 命令域集成测试（真实 wkv 库驱动 EVAL / EVALSHA / SCRIPT 全链路）
//!
//! 对标 test/standalone/Garnet.test.scripting/LuaScriptTests.cs：
//! CanDoEvalSetGet / CanDoScriptFlush / CJson / PermissionsEnforced /
//! ComplexLuaTest1-3 / ScriptExistsErrors。
//!
//! 驱动形态：RespServerSession + StoreGarnetApi 生产装配路径整链路消费
//! （redis.call 经 RespScriptingApi 重入会话主循环，ACL 门控同步生效）。

use std::sync::Arc;

use compio::runtime::Runtime;
use parking_lot::Mutex;
use wacl::{AccessControlList, AclParser, GarnetAclAuthenticator};
use wcol::itembroker::{
  collection_item_broker::CollectionItemBroker, item_broker_face::SharedItemBroker,
};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::resp::{
  acl_store::AclStore, garnet_api::StoreGarnetApi,
  objects::collection_item_source::CollectionItemSource,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::drain_output;
use wpubsub::subscribe_broker::SubscribeBroker;
use wtest_base::open_test_store;

type TestStore = Arc<WedbStore<SegmentedDevice>>;

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
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(session)));
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
  feed(s, &resp(&refs)).expect("协议违规");
  drain_output(s)
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
/// 模拟泵直填一批字节（take → extend → return → consume 的会话侧等价）
fn feed(s: &mut RespServerSession, bytes: &[u8]) -> Option<usize> {
  s.recv_buffer.extend_from_slice(bytes);
  s.try_consume_messages()
}

#[test]
fn test_eval_redis_call_roundtrip() {
  let (_dir, store) = open_test_store("lua-eval.db").expect("open test store");
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
  let (_dir, store) = open_test_store("lua-script.db").expect("open test store");
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
  let (_dir, store) = open_test_store("lua-cjson.db").expect("open test store");
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

/// 脚本错误文案含 CRLF：错误帧一律经 wresp 错误帧唯一成帧点净化（CRLF 切断 +
/// MAX_ERROR_MSG_LEN 长度帽），一请求只出一帧、清洗后字节逐字节一致
/// （对标 C# LuaRunner.cs:RunCommon 的 TryWriteError 单口）。
/// 两个面分别覆盖 `&str` 入口与 `&[u8]` 入参站点：后者（表回显 err 键，
/// LuaRunner.cs:TryWriteError）无调用侧预处理，清洗完全由成帧点承担。
#[test]
fn lua_error_crlf_cannot_inject_frame() {
  let (_dir, store) = open_test_store("lua-error-crlf.db").expect("open test store");
  let mut s = lua_session(&store);

  // error() 载荷自带 CRLF + 伪造整数帧
  let out = cmd(
    &mut s,
    eval_parts("error('boom\\r\\n:4242\\r\\n')", &[], &[]),
  );
  let text = String::from_utf8_lossy(&out);
  assert!(!text.contains(":4242"), "CRLF 后的注入字节不得成帧: {text}");
  assert_eq!(
    out,
    b"-ERR Lua encountered an error: user_script:1: boom\r\n"
  );
  assert_eq!(
    out.iter().filter(|&&b| b == b'\n').count(),
    1,
    "got {out:?}"
  );

  // return {err = ...} 走字节入参站点，脚本原文 CRLF 之后被切断
  let out = cmd(
    &mut s,
    eval_parts("return {err='boom\\r\\n:4242\\r\\n'}", &[], &[]),
  );
  assert_eq!(out, b"-boom\r\n");
  assert_eq!(
    out.iter().filter(|&&b| b == b'\n').count(),
    1,
    "got {out:?}"
  );
}

/// 挂载 ACL 认证器并完成 AUTH（C# ConnectionMultiplexer authUsername 形态）
fn acl_session(
  store: &TestStore,
  acl: &Arc<AccessControlList>,
  user: &str,
  pass: &str,
) -> RespServerSession {
  let mut s = lua_session(store);
  s.attach_acl(Some(Arc::new(Mutex::new(GarnetAclAuthenticator::new(
    Arc::clone(acl),
  )))));
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
  let (_dir, store) = open_test_store("lua-acl.db").expect("open test store");
  let acl = Arc::new(AccessControlList::new("").unwrap());
  // deny / denyset 规则落存储记录（命名用户 AUTH 走 KeyTag::Acl 存储点查）
  let acl_write_session = store.new_session().unwrap();
  let acl_store = AclStore::new(&acl_write_session);
  for (name, rule) in [
    ("deny", "user deny on >pw +@all -get -acl -cluster|myid"),
    ("denyset", "user denyset on >pw2 +@all -set"),
  ] {
    let user = AclParser::parse_acl_rule(rule).unwrap();
    acl_store
      .write(0, name.as_bytes(), &user.to_bytes())
      .unwrap();
  }

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

/// redis.acl_check_cmd 有效性判定与权限检查收口会话侧单点后的三臂对照
/// （对标 LuaRunner.Functions.cs:AclCheckCommand :2803-3064）：
/// - 清单外命令（HSET）可用（旧硬清单口径直接报无效）
/// - 未知命令经目录单点判无效
/// - BITOP 无参全查臂（-bitop 用户 false）
/// - ACL 父命令不带子命令的逐子命令全查臂（-acl|setuser 用户 false，
///   带子命令走直检臂仍放行）
/// - arity 不足宽容补位（GET 无参 / SET 单参按 min 补空参后成帧）
#[test]
fn test_eval_acl_check_cmd_arms() {
  let (_dir, store) = open_test_store("lua-acl-check-cmd.db").expect("open test store");
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let acl_write_session = store.new_session().unwrap();
  let acl_store = AclStore::new(&acl_write_session);
  for (name, rule) in [
    ("denybitop", "user denybitop on >pw3 +@all -bitop"),
    ("denyaclsub", "user denyaclsub on >pw4 +@all -acl|setuser"),
  ] {
    let user = AclParser::parse_acl_rule(rule).unwrap();
    acl_store
      .write(0, name.as_bytes(), &user.to_bytes())
      .unwrap();
  }

  let check = |s: &mut RespServerSession, script: &str| -> String {
    String::from_utf8_lossy(&cmd(s, eval_parts(script, &[], &[]))).into_owned()
  };

  // 允许面（无认证器 → 位图门恒放行）：清单外命令与 arity 不足补位
  let mut allow = lua_session(&store);
  assert_eq!(
    check(
      &mut allow,
      "if redis.acl_check_cmd('HSET') then return 'Y' end return 'N'"
    ),
    "$1\r\nY\r\n"
  );
  assert_eq!(
    check(
      &mut allow,
      "if redis.acl_check_cmd('GET') then return 'Y' end return 'N'"
    ),
    "$1\r\nY\r\n"
  );
  assert_eq!(
    check(
      &mut allow,
      "if redis.acl_check_cmd('SET', 'k') then return 'Y' end return 'N'"
    ),
    "$1\r\nY\r\n"
  );

  // 未知命令：目录单点未命中 → 无效命令
  let out = check(&mut allow, "return redis.acl_check_cmd('NOSUCHCMD')");
  assert!(
    out.contains("ERR Invalid command passed to redis.acl_check_cmd()"),
    "got {out}"
  );

  // BITOP 无参全查臂：+@all -bitop 清位含展开集 → false
  let mut denybitop = acl_session(&store, &acl, "denybitop", "pw3");
  assert_eq!(
    check(
      &mut denybitop,
      "if redis.acl_check_cmd('BITOP') then return 'Y' end return 'N'"
    ),
    "$1\r\nN\r\n"
  );

  // ACL 父命令不带子命令：逐子命令全查，-acl|setuser 拖垮全查 → false
  let mut denyaclsub = acl_session(&store, &acl, "denyaclsub", "pw4");
  assert_eq!(
    check(
      &mut denyaclsub,
      "if redis.acl_check_cmd('ACL') then return 'Y' end return 'N'"
    ),
    "$1\r\nN\r\n"
  );
  // 带子命令（ACL CAT）走直检臂：acl|setuser 之外的子命令仍放行
  assert_eq!(
    check(
      &mut denyaclsub,
      "if redis.acl_check_cmd('ACL', 'CAT') then return 'Y' end return 'N'"
    ),
    "$1\r\nY\r\n"
  );
}

/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:ComplexLuaTest1
#[test]
fn complex_lua_test_1() {
  let (_dir, store) = open_test_store("lua-complex-1.db").expect("open test store");
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
  let (_dir, store) = open_test_store("lua-complex-2.db").expect("open test store");
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
  let (_dir, store) = open_test_store("lua-complex-3.db").expect("open test store");
  let mut s = lua_session(&store);

  let script = "return redis.call('mget', unpack(KEYS))";
  let out = cmd(&mut s, eval_parts(script, &[b"key1", b"key2"], &[]));
  assert_eq!(out, b"*2\r\n$-1\r\n$-1\r\n");
}

/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:ScriptExistsErrors
#[test]
fn script_exists_errors() {
  let (_dir, store) = open_test_store("lua-exists-err.db").expect("open test store");
  let mut s = lua_session(&store);

  let out = cmd(&mut s, vec![b"SCRIPT".to_vec(), b"EXISTS".to_vec()]);
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'script|exists' command\r\n"
  );
}

/// 构造共享 broker 的订阅会话（resp_pubsub.rs 装配形态）
fn subscriber(id: i64, broker: &Arc<SubscribeBroker>, channel: &[u8]) -> RespServerSession {
  let mut s = RespServerSession::new(id, RespServerSessionOptions::default());
  s.attach_pubsub(Arc::clone(broker));
  assert!(s.network_subscribe(false, &[channel]));
  drain_output(&mut s);
  s
}

/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:LuaPublishSucceeds_WhenCalledViaRedisCall
/// redis.call('PUBLISH', ...) 经 dispatch_resp 重入会话主循环：返回订阅者数，
/// 外部订阅者真实收到消息
#[test]
fn test_eval_lua_publish_reaches_subscriber() {
  let (_dir, store) = open_test_store("lua-pub-basic.db").expect("open test store");
  let broker = Arc::new(SubscribeBroker::new());
  let mut publisher = lua_session(&store);
  publisher.attach_pubsub(Arc::clone(&broker));
  let mut sub = subscriber(2, &broker, b"lua_pub_basic");

  let out = cmd(
    &mut publisher,
    eval_parts(
      "return redis.call('PUBLISH', KEYS[1], ARGV[1])",
      &[b"lua_pub_basic"],
      &[b"hello-from-lua"],
    ),
  );
  assert_eq!(out, b":1\r\n");

  // 订阅者邮箱收到消息帧（message + 通道 + 载荷）
  assert_eq!(sub.drain_pubsub_frames(), 1);
  assert_eq!(
    drain_output(&mut sub),
    b"*3\r\n$7\r\nmessage\r\n$13\r\nlua_pub_basic\r\n$14\r\nhello-from-lua\r\n"
  );
}

/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:LuaPublishReturnsSubscriberCount
/// 3 个订阅者在场时 Lua 内 PUBLISH 返回 3，且每个订阅者各收到一份
#[test]
fn test_eval_lua_publish_returns_subscriber_count() {
  let (_dir, store) = open_test_store("lua-pub-count.db").expect("open test store");
  let broker = Arc::new(SubscribeBroker::new());
  let mut publisher = lua_session(&store);
  publisher.attach_pubsub(Arc::clone(&broker));
  let subscribers: Vec<RespServerSession> = (2..5)
    .map(|id| subscriber(id, &broker, b"lua_pub_count"))
    .collect();
  let mut subscribers = subscribers;

  let out = cmd(
    &mut publisher,
    eval_parts(
      "return redis.call('PUBLISH', KEYS[1], ARGV[1])",
      &[b"lua_pub_count"],
      &[b"ping"],
    ),
  );
  assert_eq!(out, b":3\r\n");

  for sub in &mut subscribers {
    assert_eq!(sub.drain_pubsub_frames(), 1);
    assert_eq!(
      drain_output(sub),
      b"*3\r\n$7\r\nmessage\r\n$13\r\nlua_pub_count\r\n$4\r\nping\r\n"
    );
  }
}

/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:LuaSubscribe_StillNoScriptBlocked
/// SUBSCRIBE 被 no-script 位图拦截（Redis 协议兼容）
#[test]
fn test_eval_lua_subscribe_noscript_blocked() {
  let (_dir, store) = open_test_store("lua-pub-noscript.db").expect("open test store");
  let mut s = lua_session(&store);

  let out = cmd(
    &mut s,
    eval_parts(
      "return redis.call('SUBSCRIBE', KEYS[1])",
      &[b"any_channel"],
      &[],
    ),
  );
  assert!(
    out.windows(23).any(|w| w == b"not allowed from script"),
    "got {}",
    String::from_utf8_lossy(&out)
  );
}

/// 同批多命令帧（流水线形态一次接收）
fn batch_frames(batches: &[Vec<Vec<u8>>]) -> Vec<u8> {
  let mut buf = Vec::new();
  for parts in batches {
    let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
    buf.extend_from_slice(&resp(&refs));
  }
  buf
}

/// 脚本内命令落冷键慢路径：挂起体必须在重入内闭环并产真实应答，脚本窗口
/// 退出后会话无残留挂起（残留即被网络泵 resolve 成不属于任何命令的孤儿帧）
#[test]
fn eval_redis_call_cold_key_slow_path_handoff() {
  let (_dir, store) = open_test_store("lua-cold-call.db").expect("open test store");
  let rt = Runtime::new().expect("compio runtime");
  let mut s = lua_session(&store);

  assert_eq!(
    cmd(
      &mut s,
      vec![b"SET".to_vec(), b"lua:ck".to_vec(), b"cold".to_vec()]
    ),
    b"+OK\r\n"
  );
  // 刷并驱逐主日志：键转磁盘冷读，GET 快路径必降级挂起
  rt.block_on(store.flush_and_evict_all()).expect("flush and evict");

  let out = rt.block_on(async {
    cmd(
      &mut s,
      eval_parts("return redis.call('GET', KEYS[1])", &[b"lua:ck"], &[]),
    )
  });
  assert_eq!(out, b"$4\r\ncold\r\n", "冷键降级应答须在重入内闭环");
  assert!(
    s.take_slow_wait().is_none() && s.take_blocked_wait().is_none(),
    "脚本窗口退出后不得残留挂起体"
  );

  // 同脚本连发多条 redis.call：降级（冷键 GET）与非降级（SET/热键 GET）混合，
  // 逐条应答各自正确
  rt.block_on(store.flush_and_evict_all()).expect("flush and evict");
  let out = rt.block_on(async {
    cmd(
      &mut s,
      eval_parts(
        "local a = redis.call('GET', KEYS[1]) redis.call('SET', KEYS[2], 'hot') \
         return a .. '/' .. redis.call('GET', KEYS[2])",
        &[b"lua:ck", b"lua:hk"],
        &[],
      ),
    )
  });
  assert_eq!(out, b"$8\r\ncold/hot\r\n");
}

/// 同批 EVAL 夹在普通命令之间：前序命令应答不被脚本应答转换器误读、后续命令
/// 不随接收窗覆写丢失，整批应答逐条对齐且零孤儿帧
#[test]
fn eval_pipelined_batch_reply_alignment() {
  let (_dir, store) = open_test_store("lua-pipe-align.db").expect("open test store");
  let rt = Runtime::new().expect("compio runtime");
  let mut s = lua_session(&store);

  assert_eq!(
    cmd(
      &mut s,
      vec![b"SET".to_vec(), b"lua:ck".to_vec(), b"cold".to_vec()]
    ),
    b"+OK\r\n"
  );
  rt.block_on(store.flush_and_evict_all()).expect("flush and evict");

  let batch = batch_frames(&[
    vec![b"PING".to_vec()],
    eval_parts(
      "return redis.call('GET', KEYS[1])",
      &[b"lua:ck"],
      &[],
    ),
    vec![b"PING".to_vec()],
    vec![b"SET".to_vec(), b"lua:after".to_vec(), b"1".to_vec()],
  ]);
  let out = rt.block_on(async {
    feed(&mut s, &batch).expect("协议违规");
    drain_output(&mut s)
  });
  assert_eq!(
    out,
    b"+PONG\r\n$4\r\ncold\r\n+PONG\r\n+OK\r\n",
    "同批评答须逐条对齐，EVAL 帧之后的命令不得丢失"
  );
  assert!(
    s.take_slow_wait().is_none() && s.take_blocked_wait().is_none(),
    "本批收尾不得留挂起体给出网泵"
  );
}

/// 脚本内阻塞命令的挂起承接（C# 网络线程内联等待的等价物）：经纪注入下
/// BLPOP 在重入内闭环取到真实元素，而非空应答落 UnexpectedError
#[test]
fn eval_script_blpop_blocked_wait_handoff() {
  let (_dir, store) = open_test_store("lua-blpop-handoff.db").expect("open test store");
  let rt = Runtime::new().expect("compio runtime");
  let broker = Arc::new(SharedItemBroker::new(Arc::new(CollectionItemBroker::new(
    CollectionItemSource::new(store.new_session().unwrap()),
  ))));
  let mut s = lua_session(&store);
  s.set_item_broker(broker);

  assert_eq!(
    cmd(
      &mut s,
      vec![b"RPUSH".to_vec(), b"bq".to_vec(), b"v1".to_vec()]
    ),
    b":1\r\n"
  );

  let out = rt.block_on(async {
    cmd(
      &mut s,
      eval_parts(
        "local r = redis.call('BLPOP', KEYS[1], 10) return r[1] .. ':' .. r[2]",
        &[b"bq"],
        &[],
      ),
    )
  });
  assert_eq!(out, b"$5\r\nbq:v1\r\n", "阻塞命令须在重入内取到元素");
  assert!(
    s.take_blocked_wait().is_none() && s.take_slow_wait().is_none(),
    "脚本窗口退出后不得残留挂起体"
  );
  // 元素已被弹出：后续同批命令应答照常对齐
  assert_eq!(
    cmd(&mut s, vec![b"LLEN".to_vec(), b"bq".to_vec()]),
    b":0\r\n"
  );
}
