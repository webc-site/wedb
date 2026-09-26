//! Lua 命令域集成测试（真实 wkv 库驱动 EVAL / EVALSHA / SCRIPT 全链路）
//!
//! 对标 test/standalone/Garnet.test.scripting/LuaScriptTests.cs：
//! CanDoEvalSetGet / CanDoScriptFlush / CJson / PermissionsEnforced /
//! ComplexLuaTest1-3 / ScriptExistsErrors。
//!
//! 驱动形态：RespServerSession + StoreGarnetApi 生产装配路径整链路消费
//! （redis.call 经 RespScriptingApi 重入会话主循环，ACL 门控同步生效）。

use std::{future::Future, sync::Arc, time::Duration};

use compio::{
  runtime::{Runtime, spawn},
  time::sleep,
};
use tempfile::tempdir;
use wacl::{AccessControlList, AclParser, GarnetAclAuthenticator};
use wcol::itembroker::{
  collection_item_broker::CollectionItemBroker, item_broker_face::SharedItemBroker,
};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wlua::SessionScriptCache;
use wnode::{
  MessageConsumerFace, SessionProviderFace, WireFormat,
  resp::{
    acl_store::AclStore,
    garnet_api::StoreGarnetApi,
    objects::collection_item_source::CollectionItemSource,
    resp_server_session::{RespServerSession, RespServerSessionOptions},
    resp_session_consumer::RespSessionConsumer,
  },
  service::StorageSessionProvider,
};
use wnode_test::drain_output;
use wpubsub::subscribe_broker::SubscribeBroker;
use wtest_base::{open_test_store, test_store_config};

type TestStore = Arc<WedbStore<SegmentedDevice>>;

/// 构造 enable_lua 会话（生产装配路径：StoreGarnetApi 注入）
/// 同步测试壳内闭环 async ACL 存储访问链（全链 async 化的测试对位）
fn block_on<F: Future>(fut: F) -> F::Output {
  Runtime::new().unwrap().block_on(fut)
}

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
/// eval_parts → RESP 帧（视图收集折算，阻塞用例专用）
fn eval_frame(script: &str, keys: &[&[u8]]) -> Vec<u8> {
  let parts = eval_parts(script, keys, &[]);
  resp(&parts.iter().map(Vec::as_slice).collect::<Vec<_>>())
}

/// redis.call 直连存储端到端：SET → GET → INCR。
/// 模拟泵直填一批字节（take → extend → return → consume 的会话侧等价）；
/// 脚本挂起/阻塞/慢路径停车臂驱动后重入消费（泵消费循环的对位），停车
/// 臂（AUTH/HELLO/ACL 族与挂载刷新）经共享驱动器内联闭环
fn feed(s: &mut RespServerSession, bytes: &[u8]) -> Option<usize> {
  s.recv_buffer.extend_from_slice(bytes);
  let mut resp_buf = Vec::new();
  let remaining = s.try_consume_messages();
  s.take_output_into(&mut resp_buf);
  Runtime::new()
    .unwrap()
    .block_on(wnode_test::drive_pending_parks(s, &mut resp_buf, true));
  s.output.extend_from_slice(&resp_buf);
  remaining
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
  s.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(Arc::clone(acl)))));
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
    block_on(acl_store.write(0, name.as_bytes(), &user.to_bytes())).unwrap();
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

/// 脚本面与直令面同口径收敛（无第二套失效判据）：ACL 真源写口推进代数后，
/// 已认证连接的 EVAL 预门即重解析挂载句柄，脚本内 redis.call /
/// redis.acl_check_cmd 复用同一收敛后句柄，双向翻转即时生效
#[test]
fn test_eval_acl_live_propagation() {
  let (_dir, store) = open_test_store("lua-acl-live.db").expect("open test store");
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let writer_session = store.new_session().unwrap();
  let acl_store = AclStore::new(&writer_session);
  let denied = AclParser::parse_acl_rule("user live on >pw +@all -get").unwrap();
  let allowed = AclParser::parse_acl_rule("user live on >pw +@all").unwrap();
  block_on(acl_store.write(0, b"live", &denied.to_bytes())).unwrap();

  let mut victim = acl_session(&store, &acl, "live", "pw");
  assert_eq!(
    cmd(
      &mut victim,
      vec![b"SET".to_vec(), b"foo".to_vec(), b"bar".to_vec()]
    ),
    b"+OK\r\n"
  );

  // 基线：脚本内 GET 被拒
  let out = cmd(
    &mut victim,
    eval_parts("return redis.call('GET', 'foo')", &[], &[]),
  );
  assert!(
    out.windows(6).any(|w| w == b"NOPERM"),
    "got {}",
    String::from_utf8_lossy(&out)
  );
  let out = cmd(
    &mut victim,
    eval_parts(
      "if redis.acl_check_cmd('GET') then return 'Y' end return 'N'",
      &[],
      &[],
    ),
  );
  assert_eq!(out, b"$1\r\nN\r\n");

  // 真源改权（同 SETUSER 写口）：不重连不重认证，下一条 EVAL 即放行
  block_on(acl_store.write(0, b"live", &allowed.to_bytes())).unwrap();
  let out = cmd(
    &mut victim,
    eval_parts("return redis.call('GET', 'foo')", &[], &[]),
  );
  assert_eq!(out, b"$3\r\nbar\r\n");
  let out = cmd(
    &mut victim,
    eval_parts(
      "if redis.acl_check_cmd('GET') then return 'Y' end return 'N'",
      &[],
      &[],
    ),
  );
  assert_eq!(out, b"$1\r\nY\r\n");

  // 反向（撤权）同即：脚本面回落拒绝
  block_on(acl_store.write(0, b"live", &denied.to_bytes())).unwrap();
  let out = cmd(
    &mut victim,
    eval_parts("return redis.call('GET', 'foo')", &[], &[]),
  );
  assert!(
    out.windows(6).any(|w| w == b"NOPERM"),
    "撤权后脚本面仍放行: {}",
    String::from_utf8_lossy(&out)
  );
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
    block_on(acl_store.write(0, name.as_bytes(), &user.to_bytes())).unwrap();
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
  rt.block_on(store.flush_and_evict_all())
    .expect("flush and evict");

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
  rt.block_on(store.flush_and_evict_all())
    .expect("flush and evict");
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
  rt.block_on(store.flush_and_evict_all())
    .expect("flush and evict");

  let batch = batch_frames(&[
    vec![b"PING".to_vec()],
    eval_parts("return redis.call('GET', KEYS[1])", &[b"lua:ck"], &[]),
    vec![b"PING".to_vec()],
    vec![b"SET".to_vec(), b"lua:after".to_vec(), b"1".to_vec()],
  ]);
  let out = rt.block_on(async {
    feed(&mut s, &batch).expect("协议违规");
    drain_output(&mut s)
  });
  assert_eq!(
    out, b"+PONG\r\n$4\r\ncold\r\n+PONG\r\n+OK\r\n",
    "同批评答须逐条对齐，EVAL 帧之后的命令不得丢失"
  );
  assert!(
    s.take_slow_wait().is_none() && s.take_blocked_wait().is_none(),
    "本批收尾不得留挂起体给出网泵"
  );
}

type SharedBroker = Arc<SharedItemBroker<CollectionItemSource<SegmentedDevice>>>;

/// 脚本阻塞测试装配：共享存储 + 共享经纪 + 可选 enable_lua 会话（镜像
/// resp_blocking_commands 与 lua_session 的合并形态；collection notify 与
/// 慢路径升阶唤醒同单机装配，缺面则阻塞观察者无人唤醒）
fn lua_blocking_session(
  id: i64,
  store: &TestStore,
  broker: &SharedBroker,
  enable_lua: bool,
) -> RespServerSession {
  use wcol::itembroker::item_broker_face::ItemBrokerFinisher;

  let notify_broker = Arc::clone(broker);
  let wait_broker = Arc::clone(broker) as Arc<dyn ItemBrokerFinisher>;
  let api = Arc::new(
    StoreGarnetApi::new(store.new_session().unwrap())
      .with_collection_notify(Some(Arc::new(move |domain: (u64, u64), key: &[u8]| {
        notify_broker.handle_collection_update(domain, key)
      })))
      .with_item_broker_wait(Some(wait_broker)),
  );
  let mut s = RespServerSession::new(
    id,
    RespServerSessionOptions {
      enable_lua,
      ..RespServerSessionOptions::default()
    },
  );
  s.set_garnet_api(api);
  s.set_item_broker(Arc::clone(broker));
  s
}

/// 脚本内阻塞命令的挂起承接（C# 网络线程内联等待的等价物）：经纪注入下
/// BLPOP 在重入内闭环取到真实元素，而非空应答落 UnexpectedError
#[compio::test]
async fn eval_script_blpop_blocked_wait_handoff() {
  let (_dir, store) = open_test_store("lua-blpop-handoff.db").expect("open test store");
  let broker: SharedBroker = Arc::new(SharedItemBroker::new(Arc::new(CollectionItemBroker::new(
    CollectionItemSource::new(store.new_session().unwrap()),
  ))));
  let mut s = lua_blocking_session(1, &store, &broker, true);

  s.recv_buffer
    .extend_from_slice(&resp(&[&b"RPUSH"[..], &b"bq"[..], &b"v1"[..]]));
  let mut resp_buf = Vec::new();
  assert_eq!(s.try_consume_messages(), Some(0));
  s.take_output_into(&mut resp_buf);
  assert_eq!(resp_buf, b":1\r\n");
  resp_buf.clear();

  let parts = eval_parts(
    "local r = redis.call('BLPOP', KEYS[1], 10) return r[1] .. ':' .. r[2]",
    &[b"bq"],
    &[],
  );
  let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
  s.recv_buffer.extend_from_slice(&resp(&refs));
  assert!(s.try_consume_messages().is_some());
  s.take_output_into(&mut resp_buf);
  wnode_test::drive_pending_parks(&mut s, &mut resp_buf, true).await;
  s.output.extend_from_slice(&resp_buf);

  let out = drain_output(&mut s);
  assert_eq!(out, b"$5\r\nbq:v1\r\n", "阻塞命令须在重入内取到元素");
  assert!(
    s.take_blocked_wait().is_none() && s.take_slow_wait().is_none(),
    "脚本窗口退出后不得残留挂起体"
  );
  // 元素已被弹出：后续同批命令应答照常对齐
  s.recv_buffer
    .extend_from_slice(&resp(&[&b"LLEN"[..], &b"bq"[..]]));
  let mut llen_buf = Vec::new();
  assert_eq!(s.try_consume_messages(), Some(0));
  s.take_output_into(&mut llen_buf);
  assert_eq!(llen_buf, b":0\r\n");
}

/// 自述回归：Lua 脚本输入拒绝预编译字节码（rust 自有回归，无 C# 假锚）
#[test]
fn script_inputs_reject_precompiled_lua_bytecode() {
  let (_dir, store) = open_test_store("lua-reject-bytecode.db").expect("open test store");
  let mut s = lua_session(&store);

  let binary_chunk = b"\x1bLua\x54\x00\x19\x93\r\n\x1a\n";
  let key = b"binary-chunk-key";

  // EVAL must not execute arbitrary bytecode supplied as the script body
  let eval_out = cmd(
    &mut s,
    vec![
      b"EVAL".to_vec(),
      binary_chunk.to_vec(),
      b"1".to_vec(),
      key.to_vec(),
    ],
  );
  let eval_str = String::from_utf8_lossy(&eval_out);
  assert!(
    eval_str.contains("binary chunk"),
    "应报错包含 binary chunk: {eval_str}"
  );

  // 验证 key 不存在
  let get_out = cmd(&mut s, vec![b"GET".to_vec(), key.to_vec()]);
  assert_eq!(get_out, b"$-1\r\n");

  // SCRIPT LOAD must reject the same bytes without adding them to the global cache
  let load_out = cmd(
    &mut s,
    vec![b"SCRIPT".to_vec(), b"LOAD".to_vec(), binary_chunk.to_vec()],
  );
  let load_str = String::from_utf8_lossy(&load_out);
  assert!(
    load_str.contains("binary chunk"),
    "应报错包含 binary chunk: {load_str}"
  );

  let sha1 = SessionScriptCache::get_script_digest(binary_chunk);
  let exists_out = cmd(
    &mut s,
    vec![
      b"SCRIPT".to_vec(),
      b"EXISTS".to_vec(),
      sha1.as_str().as_bytes().to_vec(),
    ],
  );
  assert_eq!(exists_out, b"*1\r\n:0\r\n");

  // EVALSHA must reject execution since script was never cached
  let evalsha_out = cmd(
    &mut s,
    vec![
      b"EVALSHA".to_vec(),
      sha1.as_str().as_bytes().to_vec(),
      b"1".to_vec(),
      key.to_vec(),
    ],
  );
  assert_eq!(
    evalsha_out,
    b"-NOSCRIPT No matching script. Please use EVAL.\r\n"
  );
}

/// 自述回归：EVAL 使用完整源码长度含中间 null 字符（rust 自有回归，无 C# 假锚）
#[test]
fn eval_uses_full_source_length() {
  let (_dir, store) = open_test_store("lua-full-source-len.db").expect("open test store");
  let mut s = lua_session(&store);

  let source = b"return 1\0return 2";
  let eval_out = cmd(
    &mut s,
    vec![b"EVAL".to_vec(), source.to_vec(), b"0".to_vec()],
  );
  let eval_str = String::from_utf8_lossy(&eval_out);
  assert!(
    eval_str.starts_with("-Compilation error:"),
    "含有 NUL 的脚本须报错而非被阶段截断: {eval_str}"
  );
}

/// 回归（对标 C# LuaRunner.cs）：RESP3 会话 EVAL 后协议版本不变。
/// 脚本窗口的 RESP2 重置只作用 redis.call 应答解析面（rust 无内嵌 processor，
/// 共享会话字段由 run_lua_command 入口保存/收尾恢复），不穿透外层连接；
/// 最终应答按入口版本成帧——具名键表在 RESP3 下成 map 帧（协议钉死 2 时
/// 退化为 RESP2 数组展开）。
#[test]
fn eval_preserves_outer_resp_protocol() {
  let (_dir, store) = open_test_store("lua-resp3-window.db").expect("open test store");
  let mut s = lua_session(&store);
  s.resp_protocol_version = 3;

  let out = cmd(&mut s, eval_parts("return {map={a='b'}}", &[], &[]));
  assert!(out.starts_with(b"%1\r\n"), "got {out:?}");
  assert_eq!(s.resp_protocol_version, 3);

  // RESP2 会话对照：同脚本成数组展开帧（窗口重置不改变两态各自帧型）
  let mut s2 = lua_session(&store);
  let out = cmd(&mut s2, eval_parts("return {map={a='b'}}", &[], &[]));
  assert!(out.starts_with(b"*2\r\n"), "got {out:?}");
  assert_eq!(s2.resp_protocol_version, 2);
}

/// 回归（对标 C# LuaRunner.cs）：脚本内 setresp(3) 只作用脚本窗口，
/// EVAL 结束还原入口版本——RESP2 连接不因脚本内 setresp 永久升 RESP3。
#[test]
fn eval_setresp_window_is_restored() {
  let (_dir, store) = open_test_store("lua-setresp-window.db").expect("open test store");
  let mut s = lua_session(&store);

  let out = cmd(&mut s, eval_parts("redis.setresp(3) return 1", &[], &[]));
  assert_eq!(out, b":1\r\n");
  assert_eq!(s.resp_protocol_version, 2);
}

/// 回归（对标 C# SessionScriptCache.cs）：脚本内 redis.call('SELECT') 只作用脚本
/// 窗口（C# 落 SessionScriptCache 独立会话的 db 状态），EVAL 结束还原入口库
/// ——同连接后续命令不随脚本切库跨库写串。
#[test]
fn eval_select_stays_in_window() {
  let (_dir, store) = open_test_store("lua-select-window.db").expect("open test store");
  let mut s = lua_session(&store);

  // 窗口内 SELECT 3 后写键：脚本内后续 redis.call 落 db 3
  let out = cmd(
    &mut s,
    eval_parts(
      "redis.call('SELECT','3') redis.call('SET','lua:sel','d3') return 1",
      &[],
      &[],
    ),
  );
  assert_eq!(out, b":1\r\n");
  assert_eq!(s.active_db_id, 0);

  // 外层连接仍在原库 db 0：写入与 db 3 的脚本键互不可见
  let out = cmd(
    &mut s,
    vec![b"SET".to_vec(), b"lua:sel".to_vec(), b"d0".to_vec()],
  );
  assert_eq!(out, b"+OK\r\n");
  let out = cmd(&mut s, vec![b"GET".to_vec(), b"lua:sel".to_vec()]);
  assert_eq!(out, b"$2\r\nd0\r\n");

  // db 3 里是脚本窗口写的值
  let out = cmd(&mut s, vec![b"SELECT".to_vec(), b"3".to_vec()]);
  assert_eq!(out, b"+OK\r\n");
  let out = cmd(&mut s, vec![b"GET".to_vec(), b"lua:sel".to_vec()]);
  assert_eq!(out, b"$2\r\nd3\r\n");
  let _ = cmd(&mut s, vec![b"SELECT".to_vec(), b"0".to_vec()]);
  assert_eq!(s.active_db_id, 0);
}

/// 快路径 GET 错误帧折叠差分（C# api.GET 非 OK status——含 WRONGTYPE——一律
/// PushBoolean(false) 脚本继续，LuaRunner.Functions.cs:3246-3259）。rust 侧
/// C# 存储 API 直连对应面为会话重入回包，快路径把错误帧折回 false；网络面
/// 直连 GET 对 LIST 键仍产 -WRONGTYPE 帧（该契约不变）。本例取代旧
/// 「错误文本透传中止脚本」回归（该形态系转写分叉，未经 deviations 裁决）。
#[test]
fn eval_get_wrongtype_folds_to_false() {
  let (_dir, store) = open_test_store("lua-wrongtype-fold.db").expect("open test store");
  let mut s = lua_session(&store);

  // 造 WRONGTYPE：列表键上 GET，外层直连错误帧仅作网络面基线
  let out = cmd(
    &mut s,
    vec![b"LPUSH".to_vec(), b"lua:wt".to_vec(), b"1".to_vec()],
  );
  assert_eq!(out, b":1\r\n");
  let direct = cmd(&mut s, vec![b"GET".to_vec(), b"lua:wt".to_vec()]);
  assert!(direct.starts_with(b"-WRONGTYPE "), "got {direct:?}");

  // 脚本内 GET 命中错误键：折叠 false 后续跑（折叠缺失即错误帧上抛中断）
  let out = cmd(
    &mut s,
    eval_parts(
      "local v = redis.call('GET', KEYS[1]) if v == false then return 'continued' end return 'wrong'",
      &[b"lua:wt"],
      &[],
    ),
  );
  assert_eq!(out, b"$9\r\ncontinued\r\n");

  // 脚本直返 false → RESP2 null（与 GET miss 的 false 同形）
  let out = cmd(
    &mut s,
    eval_parts("return redis.call('GET', KEYS[1])", &[b"lua:wt"], &[]),
  );
  assert_eq!(out, b"$-1\r\n");
}

/// 生产装配形态双连接（provider.get_session → inject_dependencies 全链路，
/// 全局脚本缓存经基座单点实例注入两连接）
fn provider_lua_consumer_pair(
  db_name: &str,
) -> aok::Result<(RespSessionConsumer, RespSessionConsumer)> {
  let dir = tempdir()?;
  let provider = StorageSessionProvider::open_with_config(
    test_store_config(),
    dir.path().join(db_name),
    |sender_id, api| {
      Some(RespSessionConsumer::new(
        sender_id,
        RespServerSessionOptions {
          enable_lua: true,
          ..RespServerSessionOptions::default()
        },
        Arc::new(api),
      ))
    },
  )?;
  let c1 = provider
    .get_session(WireFormat::Ascii, 1)
    .expect("连接 1 会话创建");
  let c2 = provider
    .get_session(WireFormat::Ascii, 2)
    .expect("连接 2 会话创建");
  Ok((c1, c2))
}

/// 消费一条命令并取回响应（scratch 直填驱动）
fn consumer_cmd(c: &mut RespSessionConsumer, parts: &[&[u8]]) -> Vec<u8> {
  let frame = resp(parts);
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(&frame);
  c.return_recv_scratch(scratch);
  let mut out = Vec::new();
  assert_eq!(c.try_consume_messages_into(&mut out), Some(0));
  out
}

/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:CrossSessionEvalScriptCaching
/// 连接 1 EVAL 装载的脚本，连接 2 按摘要 EVALSHA 即时可用（Redis 跨连接
/// 脚本缓存契约：storeScriptCache 绑定存储实例而非会话）
#[test]
fn test_cross_session_eval_script_caching() {
  let rt = Runtime::new().expect("compio runtime");
  rt.block_on(async {
    let (mut c1, mut c2) =
      provider_lua_consumer_pair("lua-cross-session.db").expect("provider 装配");

    let script = "return 2;";
    // C# SHA1.HashData(Encoding.ASCII.GetBytes(script)) 的 hex 摘要
    let hash = SessionScriptCache::get_script_digest(script.as_bytes())
      .as_str()
      .to_owned();

    assert_eq!(
      consumer_cmd(&mut c1, &[b"EVAL", script.as_bytes(), b"0"]),
      b":2\r\n"
    );
    // 异连接按摘要命中
    assert_eq!(
      consumer_cmd(&mut c2, &[b"EVALSHA", hash.as_bytes(), b"0"]),
      b":2\r\n"
    );
    assert_eq!(
      consumer_cmd(&mut c1, &[b"EVALSHA", hash.as_bytes(), b"0"]),
      b":2\r\n"
    );
    // SCRIPT EXISTS 跨连接同源
    assert_eq!(
      consumer_cmd(&mut c2, &[b"SCRIPT", b"EXISTS", hash.as_bytes()]),
      b"*1\r\n:1\r\n"
    );
    Ok::<(), aok::Error>(())
  })
  .unwrap();
}

/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:MultiSessionScriptFlush
/// 连接 1 SCRIPT FLUSH 全服生效：两连接 EVALSHA 均转 NOSCRIPT
#[test]
fn test_multi_session_script_flush() {
  let rt = Runtime::new().expect("compio runtime");
  rt.block_on(async {
    let (mut c1, mut c2) = provider_lua_consumer_pair("lua-multi-flush.db").expect("provider 装配");

    assert_eq!(
      consumer_cmd(&mut c1, &[b"SCRIPT", b"FLUSH", b"SYNC"]),
      b"+OK\r\n"
    );

    // 连接 1 装载，两连接均可执行
    let load = consumer_cmd(&mut c1, &[b"SCRIPT", b"LOAD", b"return 2;"]);
    assert!(
      load.starts_with(b"$40\r\n"),
      "SCRIPT LOAD 应答摘要: {load:?}"
    );
    let digest = String::from_utf8_lossy(&load[5..45]).to_string();
    let evalsha =
      |c: &mut RespSessionConsumer| consumer_cmd(c, &[b"EVALSHA", digest.as_bytes(), b"0"]);
    assert_eq!(evalsha(&mut c1), b":2\r\n", "装载连接自身可执行");
    assert_eq!(evalsha(&mut c2), b":2\r\n", "异连接共享同一脚本缓存");

    // 连接 1 冲刷 → 两连接俱 NOSCRIPT
    assert_eq!(
      consumer_cmd(&mut c1, &[b"SCRIPT", b"FLUSH", b"SYNC"]),
      b"+OK\r\n"
    );
    for c in [&mut c1, &mut c2] {
      assert_eq!(
        evalsha(c),
        b"-NOSCRIPT No matching script. Please use EVAL.\r\n",
        "SCRIPT FLUSH 须全服生效"
      );
    }
    Ok::<(), aok::Error>(())
  })
  .unwrap();
}

/// 回归（对标 test/standalone/Garnet.test.scripting/LuaScriptTests.cs:LuaToResp3Conversions
/// SPEC BREAK 三项与 C# LuaRunner.cs:TryWriteSingleItem 布尔臂四象限）：
/// EVAL 布尔回值成帧由「脚本侧版本（脚本内 setresp 窗口终值，默认 2）×
/// 连接侧成帧版本（恒入口版本）」双维裁决——Redis 实际行为：脚本返回布尔
/// 恒 :1/nil，仅脚本 setresp(3) 且连接 RESP3 才回 #t/#f。
/// 四象限逐字节断言（:1/_、:1/:0、#t/#f、:1/$-1）。
#[test]
fn eval_boolean_framing_four_quadrants() {
  let (_dir, store) = open_test_store("lua-bool-quad.db").expect("open test store");

  // 脚本 2 + 连接 2（默认路径基线）：true → :1，false → $-1
  let mut s = lua_session(&store);
  assert_eq!(cmd(&mut s, eval_parts("return true", &[], &[])), b":1\r\n");
  assert_eq!(
    cmd(&mut s, eval_parts("return false", &[], &[])),
    b"$-1\r\n"
  );

  // 脚本 2 + 连接 3：true → :1，false → _（RESP3 null；C# SPEC BREAK）
  let mut s = lua_session(&store);
  s.resp_protocol_version = 3;
  assert_eq!(cmd(&mut s, eval_parts("return true", &[], &[])), b":1\r\n");
  assert_eq!(cmd(&mut s, eval_parts("return false", &[], &[])), b"_\r\n");
  assert_eq!(s.resp_protocol_version, 3, "外层连接版本不得被穿透");

  // 脚本 3 + 连接 2（SPEC BREAK!）：true → :1，false → :0
  let mut s = lua_session(&store);
  assert_eq!(
    cmd(&mut s, eval_parts("redis.setresp(3) return true", &[], &[])),
    b":1\r\n"
  );
  assert_eq!(
    cmd(
      &mut s,
      eval_parts("redis.setresp(3) return false", &[], &[])
    ),
    b":0\r\n"
  );
  assert_eq!(s.resp_protocol_version, 2, "setresp 只作用脚本窗口");

  // 脚本 3 + 连接 3：#t/#f（RESP3 专属布尔类型）
  let mut s = lua_session(&store);
  s.resp_protocol_version = 3;
  assert_eq!(
    cmd(&mut s, eval_parts("redis.setresp(3) return true", &[], &[])),
    b"#t\r\n"
  );
  assert_eq!(
    cmd(
      &mut s,
      eval_parts("redis.setresp(3) return false", &[], &[])
    ),
    b"#f\r\n"
  );
}

/// 回归（对标 C# LuaRunner.cs:TryWriteNumber 的 `(long)` 硬转换，x64
/// cvttsd2si 不定值语义）：EVAL number → integer 应答的截断基座在 NaN 与
/// 越界处一律回 :-9223372036854775808，不取 Rust `as` 饱和语义
/// （inf → i64::MAX、NaN → 0 的分叉）。
#[test]
fn eval_number_truncation_x64_semantics() {
  let (_dir, store) = open_test_store("lua-num-trunc.db").expect("open test store");
  let mut s = lua_session(&store);

  for script in ["return 1/0", "return 0/0", "return 1e300", "return -1e300"] {
    let out = cmd(&mut s, eval_parts(script, &[], &[]));
    assert_eq!(out, b":-9223372036854775808\r\n", "script: {script}");
  }

  // 域内常规值照常精确截断（2^63 恰可由 f64 表示，排他上界外 → i64::MIN）
  let out = cmd(&mut s, eval_parts("return 3.7", &[], &[]));
  assert_eq!(out, b":3\r\n");
}

/// 脚本内阻塞命令 → 协程挂起续跑端到端（对标 C# 脚本内 BLPOP 的回调栈
/// 同步收割语义，rust 协程化投影：EVAL 内 redis.call('BLPOP') 挂起脚本
/// 协程 → 消费返回（应答暂无）→ 对端 LPUSH 经共享经纪唤醒 → 泵同款
/// await 续跑至脚本完成，应答 = 阻塞结果）
#[compio::test]
async fn eval_blocking_call_suspends_and_resumes() {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("lua-blk.db")).unwrap());
  let mut config = test_store_config();
  config.gc.enabled = false;
  let store: TestStore = Arc::new(WedbStore::open(config, device).unwrap());
  let broker: SharedBroker = Arc::new(SharedItemBroker::new(Arc::new(CollectionItemBroker::new(
    CollectionItemSource::new(store.new_session().unwrap()),
  ))));
  let mut script_s = lua_blocking_session(1, &store, &broker, true);
  let mut push_s = lua_blocking_session(2, &store, &broker, false);

  // EVAL 内 BLPOP：消费返回即挂起（应答暂无，挂起态在会话）
  script_s.recv_buffer.extend_from_slice(&eval_frame(
    "return redis.call('BLPOP', KEYS[1], 5)",
    &[b"lua:blk"],
  ));
  let mut resp_buf = Vec::new();
  assert!(script_s.try_consume_messages().is_some(), "EVAL 应被消费");
  script_s.take_output_into(&mut resp_buf);
  assert!(script_s.has_script_suspend(), "脚本协程应挂起");
  assert!(resp_buf.is_empty(), "挂起期不应有应答产出");

  // 对端 50ms 后推入唤醒（共享经纪 + collection notify 单源）
  let push_frame = resp(&[&b"LPUSH"[..], &b"lua:blk"[..], &b"v1"[..]]);
  spawn(async move {
    sleep(Duration::from_millis(50)).await;
    push_s.recv_buffer.extend_from_slice(&push_frame);
    assert!(push_s.try_consume_messages().is_some(), "LPUSH 应被消费");
    push_s.take_output_into(&mut Vec::new());
  })
  .detach();

  // 泵同款续跑：await 驱动挂起体 → 协程续跑至脚本完成
  wnode_test::drive_pending_parks(&mut script_s, &mut resp_buf, true).await;
  script_s.output.extend_from_slice(&resp_buf);
  assert!(!script_s.has_script_suspend(), "续跑完成后挂起态应清空");
  assert_eq!(
    drain_output(&mut script_s),
    &b"*2\r\n$7\r\nlua:blk\r\n$2\r\nv1\r\n"[..],
    "EVAL 应答 = 脚本返回的阻塞结果数组"
  );
}

/// 一脚本两次阻塞命令：协程两度挂起两度续跑（续跑循环多轮形态，第二次
/// BLPOP 的元素已就位经经纪立即指派）
#[compio::test]
async fn eval_two_blocking_calls_resume_loop() {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("lua-blk2.db")).unwrap());
  let mut config = test_store_config();
  config.gc.enabled = false;
  let store: TestStore = Arc::new(WedbStore::open(config, device).unwrap());
  let broker: SharedBroker = Arc::new(SharedItemBroker::new(Arc::new(CollectionItemBroker::new(
    CollectionItemSource::new(store.new_session().unwrap()),
  ))));
  let mut script_s = lua_blocking_session(1, &store, &broker, true);
  let mut push_s = lua_blocking_session(2, &store, &broker, false);

  let script = "local a = redis.call('BLPOP', KEYS[1], 5) \
                local b = redis.call('BLPOP', KEYS[2], 5) \
                return {a[2], b[2]}";
  script_s
    .recv_buffer
    .extend_from_slice(&eval_frame(script, &[b"lua:bk1", b"lua:bk2"]));
  let mut resp_buf = Vec::new();
  assert!(script_s.try_consume_messages().is_some());
  script_s.take_output_into(&mut resp_buf);
  assert!(script_s.has_script_suspend(), "首个 BLPOP 应挂起协程");

  // 对端先后推入两键（第二键在脚本二度挂起前就位，立即指派路径）
  let push1 = resp(&[&b"LPUSH"[..], &b"lua:bk1"[..], &b"v1"[..]]);
  let push2 = resp(&[&b"LPUSH"[..], &b"lua:bk2"[..], &b"v2"[..]]);
  spawn(async move {
    sleep(Duration::from_millis(50)).await;
    for frame in [push1, push2] {
      push_s.recv_buffer.extend_from_slice(&frame);
      assert!(push_s.try_consume_messages().is_some());
      push_s.take_output_into(&mut Vec::new());
    }
  })
  .detach();

  wnode_test::drive_pending_parks(&mut script_s, &mut resp_buf, true).await;
  script_s.output.extend_from_slice(&resp_buf);
  assert!(!script_s.has_script_suspend());
  assert_eq!(
    drain_output(&mut script_s),
    &b"*2\r\n$2\r\nv1\r\n$2\r\nv2\r\n"[..],
    "两次阻塞结果的脚本返回值数组"
  );
}

/// 脚本内阻塞超时：挂起 → 超时空回 → 协程续跑完成（应答 = RESP2 null）
#[compio::test]
async fn eval_blocking_timeout_resumes_with_null() {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("lua-blk3.db")).unwrap());
  let mut config = test_store_config();
  config.gc.enabled = false;
  let store: TestStore = Arc::new(WedbStore::open(config, device).unwrap());
  let broker: SharedBroker = Arc::new(SharedItemBroker::new(Arc::new(CollectionItemBroker::new(
    CollectionItemSource::new(store.new_session().unwrap()),
  ))));
  let mut script_s = lua_blocking_session(1, &store, &broker, true);

  script_s.recv_buffer.extend_from_slice(&eval_frame(
    "return redis.call('BLPOP', KEYS[1], 0.1)",
    &[b"lua:blk3"],
  ));
  let mut resp_buf = Vec::new();
  assert!(script_s.try_consume_messages().is_some());
  script_s.take_output_into(&mut resp_buf);
  assert!(script_s.has_script_suspend(), "阻塞命令应挂起协程");

  wnode_test::drive_pending_parks(&mut script_s, &mut resp_buf, true).await;
  script_s.output.extend_from_slice(&resp_buf);
  assert!(!script_s.has_script_suspend());
  assert_eq!(
    drain_output(&mut script_s),
    &b"$-1\r\n"[..],
    "超时空回经脚本 false 折算 RESP2 null"
  );
}
