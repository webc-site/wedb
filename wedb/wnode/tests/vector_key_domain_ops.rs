//! 向量集合键域全命令对标测试（对标 Garnet UnifiedStore/ReadMethods.cs 与 MainStore/ReadMethods.cs:CheckRecordTypeMismatch）
//!
//! 验证 TYPE, EXISTS, OBJECT ENCODING, GET, GETDEL, GETEX, STRLEN, GETRANGE, SUBSTR, APPEND,
//! INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, DBSIZE, KEYS, SCAN 对向量集键的行为。

use std::{mem::forget, str::from_utf8, sync::Arc};

use compio::runtime::Runtime;
use itoa::Buffer;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace,
  database::{SingleDatabaseManager, garnet_database::GarnetDatabase},
  resp::{
    RespSessionConsumer,
    garnet_api::StoreGarnetApi,
    resp_server_session::RespServerSessionOptions,
    vector::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_manager_locking::{registry_key, registry_user_key},
      vector_store_callbacks::WedbVectorStoreCallbacks,
    },
  },
};
use wtest_base::test_store_config;
use wval::SessionPrefixBuf;
use wvector::Callbacks;

/// 会话消费者 + 向量登记表 + 存储句柄（物理域解析面 `store.vdb` 供换号断言）
fn consumer() -> (
  RespSessionConsumer,
  Arc<VectorManager>,
  Arc<WedbStore<SegmentedDevice>>,
) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("vec_domain.db")).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();

  let v_session = Arc::new(store.new_session().unwrap());
  let callbacks = Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new(v_session)));
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    callbacks,
  ));

  // FLUSH 族装配（域回收联动端到端）：单库管理器 + 向量回收注入
  let cp_dir = dir.path().join("cp");
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&store),
    Arc::clone(&store.device),
    cp_dir.clone(),
    None,
  ));
  let mgr = Arc::new(SingleDatabaseManager::new(cp_dir, db));
  mgr.attach_vector_manager(Arc::clone(&vm));

  let api = StoreGarnetApi::new(session)
    .with_vector_manager(Arc::clone(&vm))
    .with_database_manager(mgr);
  let consumer = RespSessionConsumer::new(1, RespServerSessionOptions::default(), Arc::new(api));
  forget(dir);
  (consumer, vm, store)
}

fn encode_frame(parts: &[&[u8]]) -> Vec<u8> {
  let est = parts.iter().map(|s| s.len() + 16).sum::<usize>() + 16;
  let mut out = Vec::with_capacity(est);
  let mut ibuf = Buffer::new();
  out.push(b'*');
  out.extend_from_slice(ibuf.format(parts.len()).as_bytes());
  out.extend_from_slice(b"\r\n");
  for p in parts {
    out.push(b'$');
    out.extend_from_slice(ibuf.format(p.len()).as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(p);
    out.extend_from_slice(b"\r\n");
  }
  out
}

fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> (Option<usize>, Vec<u8>) {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  (remaining, resp)
}

fn roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, req: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = pump(c, req);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {req:?}");
  if let Some(slow) = c.take_slow_wait() {
    let resolved = rt.block_on(slow.resolve());
    out.extend_from_slice(&resolved);
  }
  out
}

fn create_vectorset(rt: &Runtime, consumer: &mut RespSessionConsumer, key: &[u8]) {
  let vec3: Vec<u8> = [1.0f32, 2.0, 3.0]
    .iter()
    .flat_map(|v| v.to_le_bytes())
    .collect();
  let req = encode_frame(&[b"VADD", key, b"FP32", &vec3, b"elem1"]);
  let out = roundtrip(rt, consumer, &req);
  assert_eq!(out, b":1\r\n", "VADD 创建向量集失败");
}

/// 命令面剥域投影断言：RESP bulk 帧 `$<len>\r\n<user_key>\r\n` 逐字节在位。
///
/// 登记表条目为复合键 `[NsVarint][DbVarint] + 用户键`（registry_key 单点），
/// 而 KEYS/SCAN 投影契约恒为剥域用户键（C# 每库一实例 VectorManager
/// libs/server/Resp/Vector/VectorManager.cs:177 `dbId` 首参，天然无域前缀；
/// rust 单例经 for_each_domain_user_key → [`registry_user_key`] 单点剥域，
/// doc/zh/db.md §1.1 前缀刚性隔离的读端收口）。
///
/// 此处刻意不用裸子串比对：复合键以用户键为后缀，登记表域若泄漏进命令面，
/// 子串断言仍恒真；定长 bulk 头使「长度多出域前缀」即刻破形。
fn assert_projects_stripped_bulk(reply: &[u8], user_key: &[u8]) {
  let mut framed = Vec::with_capacity(user_key.len() + 8);
  framed.extend_from_slice(b"$");
  framed.extend_from_slice(Buffer::new().format(user_key.len()).as_bytes());
  framed.extend_from_slice(b"\r\n");
  framed.extend_from_slice(user_key);
  framed.extend_from_slice(b"\r\n");
  assert!(
    reply.windows(framed.len()).any(|w| w == framed.as_slice()),
    "命令面应投影剥域用户键 {user_key:?}（定长 bulk 帧 {framed:?}）：{:?}",
    from_utf8(reply)
  );
}

#[test]
fn vector_set_type_and_exists_and_object() {
  let rt = Runtime::new().unwrap();
  let (mut c, _vm, _store) = consumer();

  create_vectorset(&rt, &mut c, b"my_vectorset");
  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"SET", b"my_str", b"v"])),
    b"+OK\r\n"
  );

  // 1. TYPE
  let type_out = roundtrip(&rt, &mut c, &encode_frame(&[b"TYPE", b"my_vectorset"]));
  assert_eq!(type_out, b"+vectorset\r\n", "TYPE 向量集应返回 +vectorset");

  let type_str = roundtrip(&rt, &mut c, &encode_frame(&[b"TYPE", b"my_str"]));
  assert_eq!(type_str, b"+string\r\n");

  let type_missing = roundtrip(&rt, &mut c, &encode_frame(&[b"TYPE", b"missing_key"]));
  assert_eq!(type_missing, b"+none\r\n");

  // 2. EXISTS
  let exists_vec = roundtrip(&rt, &mut c, &encode_frame(&[b"EXISTS", b"my_vectorset"]));
  assert_eq!(exists_vec, b":1\r\n", "EXISTS 单个向量集应返回 :1");

  let exists_multi = roundtrip(
    &rt,
    &mut c,
    &encode_frame(&[b"EXISTS", b"my_vectorset", b"my_str", b"missing"]),
  );
  assert_eq!(exists_multi, b":2\r\n", "EXISTS 多键应包含向量集");

  // 3. OBJECT ENCODING
  let obj_enc = roundtrip(
    &rt,
    &mut c,
    &encode_frame(&[b"OBJECT", b"ENCODING", b"my_vectorset"]),
  );
  assert_eq!(
    obj_enc, b"$3\r\nraw\r\n",
    "OBJECT ENCODING 向量集应返回 raw"
  );

  // OBJECT REFCOUNT & IDLETIME
  let obj_ref = roundtrip(
    &rt,
    &mut c,
    &encode_frame(&[b"OBJECT", b"REFCOUNT", b"my_vectorset"]),
  );
  assert_eq!(obj_ref, b":1\r\n");

  let obj_idle = roundtrip(
    &rt,
    &mut c,
    &encode_frame(&[b"OBJECT", b"IDLETIME", b"my_vectorset"]),
  );
  assert_eq!(obj_idle, b":0\r\n");
}

#[test]
fn vector_set_wrongtype_on_string_and_rmw_commands() {
  let rt = Runtime::new().unwrap();
  let (mut c, _vm, _store) = consumer();

  create_vectorset(&rt, &mut c, b"vs_key");

  let commands: Vec<Vec<&[u8]>> = vec![
    vec![b"GET", b"vs_key"],
    vec![b"GETDEL", b"vs_key"],
    vec![b"GETEX", b"vs_key", b"EX", b"100"],
    vec![b"STRLEN", b"vs_key"],
    vec![b"GETRANGE", b"vs_key", b"0", b"-1"],
    vec![b"SUBSTR", b"vs_key", b"0", b"-1"],
    vec![b"APPEND", b"vs_key", b"suffix"],
    vec![b"INCR", b"vs_key"],
    vec![b"DECR", b"vs_key"],
    vec![b"INCRBY", b"vs_key", b"2"],
    vec![b"DECRBY", b"vs_key", b"2"],
    vec![b"INCRBYFLOAT", b"vs_key", b"1.5"],
    vec![b"SETRANGE", b"vs_key", b"0", b"new_val"],
  ];

  for cmd_args in commands {
    let cmd_name = from_utf8(cmd_args[0]).unwrap();
    let req = encode_frame(&cmd_args);
    let out = roundtrip(&rt, &mut c, &req);
    assert!(
      out.starts_with(b"-WRONGTYPE "),
      "命令 {cmd_name} 对向量集键应返回 WRONGTYPE，实际为: {:?}",
      String::from_utf8_lossy(&out)
    );
  }
}

#[test]
fn vector_set_dbsize_keys_and_scan() {
  let rt = Runtime::new().unwrap();
  let (mut c, _vm, _store) = consumer();

  // 写入两个普通字符串与两个向量集
  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"SET", b"str:1", b"v1"])),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"SET", b"str:2", b"v2"])),
    b"+OK\r\n"
  );
  create_vectorset(&rt, &mut c, b"vec:1");
  create_vectorset(&rt, &mut c, b"vec:2");

  // 1. DBSIZE 应统计全部 4 个键
  let dbsize_out = roundtrip(&rt, &mut c, &encode_frame(&[b"DBSIZE"]));
  assert_eq!(
    dbsize_out, b":4\r\n",
    "DBSIZE 应统计包含向量集在内的 4 个键"
  );

  // 2. KEYS * 应返回全部 4 个键
  let keys_all = roundtrip(&rt, &mut c, &encode_frame(&[b"KEYS", b"*"]));
  assert!(
    keys_all.starts_with(b"*4\r\n"),
    "KEYS * 应返回 4 个键: {:?}",
    String::from_utf8_lossy(&keys_all)
  );
  assert!(keys_all.windows(5).any(|w| w == b"vec:1"));
  assert!(keys_all.windows(5).any(|w| w == b"vec:2"));
  assert!(keys_all.windows(5).any(|w| w == b"str:1"));
  assert!(keys_all.windows(5).any(|w| w == b"str:2"));

  // KEYS vec:* 应返回 2 个向量集键
  let keys_vec = roundtrip(&rt, &mut c, &encode_frame(&[b"KEYS", b"vec:*"]));
  assert_eq!(keys_vec[0..4], *b"*2\r\n", "KEYS vec:* 应返回 2 个键");
  assert!(keys_vec.windows(5).any(|w| w == b"vec:1"));
  assert!(keys_vec.windows(5).any(|w| w == b"vec:2"));

  // 3. SCAN 0 应包含向量集键
  let scan_out = roundtrip(&rt, &mut c, &encode_frame(&[b"SCAN", b"0"]));
  assert!(scan_out.starts_with(b"*2\r\n"), "SCAN 格式应为 *2");
  assert!(
    scan_out.windows(5).any(|w| w == b"vec:1"),
    "SCAN 应包含 vec:1"
  );
  assert!(
    scan_out.windows(5).any(|w| w == b"vec:2"),
    "SCAN 应包含 vec:2"
  );

  // SCAN 0 MATCH vec:* 应仅匹配向量集
  let scan_match = roundtrip(
    &rt,
    &mut c,
    &encode_frame(&[b"SCAN", b"0", b"MATCH", b"vec:*"]),
  );
  assert!(scan_match.windows(5).any(|w| w == b"vec:1"));
  assert!(scan_match.windows(5).any(|w| w == b"vec:2"));
  assert!(!scan_match.windows(5).any(|w| w == b"str:1"));

  // SCAN 0 TYPE string 应不包含向量集
  let scan_type_str = roundtrip(
    &rt,
    &mut c,
    &encode_frame(&[b"SCAN", b"0", b"TYPE", b"string"]),
  );
  assert!(!scan_type_str.windows(5).any(|w| w == b"vec:1"));
  assert!(!scan_type_str.windows(5).any(|w| w == b"vec:2"));

  // 4. DEL 删除一个向量集后，DBSIZE 和 KEYS 同步减少
  let del_out = roundtrip(&rt, &mut c, &encode_frame(&[b"DEL", b"vec:1"]));
  assert_eq!(del_out, b":1\r\n", "DEL 向量集应返回 :1");

  let dbsize_after = roundtrip(&rt, &mut c, &encode_frame(&[b"DBSIZE"]));
  assert_eq!(dbsize_after, b":3\r\n", "DEL 后 DBSIZE 应为 3");

  let keys_after = roundtrip(&rt, &mut c, &encode_frame(&[b"KEYS", b"vec:*"]));
  assert_eq!(keys_after[0..4], *b"*1\r\n", "DEL 后 KEYS vec:* 仅剩 1 个");
}

/// TTL 族与 MEMORY USAGE 对存活向量集键的第四态判定（对标 C# UnifiedStore
/// ReadMethods.cs:31-47 Reader 无类型门 + :162-188 HandleTtl/HandleExpireTime 的
/// `HasExpiration ? Expiration : -1`、:103-124 HandleMemoryUsage 回 AllocatedSize）。
/// 修复前：探针只折叠 wkv 三域，存活向量键被当缺失键——TTL/PTTL/EXPIRETIME 回
/// -2、MEMORY USAGE 回 nil。修复后：存活向量键 TTL/PTTL/EXPIRETIME/PEXPIRETIME
/// 回 -1（存活无过期），MEMORY USAGE 回正整数；缺失键维持 -2 / nil。
#[test]
fn vector_set_ttl_family_and_memory_usage_fourth_state() {
  let rt = Runtime::new().unwrap();
  let (mut c, _vm, _store) = consumer();

  create_vectorset(&rt, &mut c, b"vs_ttl");

  // 存活向量键：TTL 族回 -1（存在但无过期），非缺失的 -2
  for cmd in [b"TTL".as_slice(), b"PTTL", b"EXPIRETIME", b"PEXPIRETIME"] {
    let out = roundtrip(&rt, &mut c, &encode_frame(&[cmd, b"vs_ttl"]));
    assert_eq!(
      out,
      b":-1\r\n",
      "存活向量键 {:?} 应回 -1（存活无过期），实际: {:?}",
      String::from_utf8_lossy(cmd),
      String::from_utf8_lossy(&out)
    );
  }

  // 存活向量键：MEMORY USAGE 回正整数（登记条目尺寸下限），非 nil
  let mu = roundtrip(
    &rt,
    &mut c,
    &encode_frame(&[b"MEMORY", b"USAGE", b"vs_ttl"]),
  );
  assert_eq!(
    mu[0],
    b':',
    "MEMORY USAGE 向量键应回整数: {:?}",
    String::from_utf8_lossy(&mu)
  );
  let mu_val: i64 = from_utf8(&mu[1..mu.len() - 2]).unwrap().parse().unwrap();
  assert!(mu_val > 0, "MEMORY USAGE 向量键应为正整数: {mu_val}");

  // 缺失键：TTL 族维持 -2，MEMORY USAGE 维持 nil（不回归）
  let ttl_missing = roundtrip(&rt, &mut c, &encode_frame(&[b"TTL", b"no_such_key"]));
  assert_eq!(ttl_missing, b":-2\r\n", "缺失键 TTL 应回 -2");
  let mu_missing = roundtrip(
    &rt,
    &mut c,
    &encode_frame(&[b"MEMORY", b"USAGE", b"no_such_key"]),
  );
  assert_eq!(mu_missing, b"$-1\r\n", "缺失键 MEMORY USAGE 应回 nil");

  // 普通字符串键：TTL 族 -1、MEMORY USAGE 正整数（探针三域路径不回归）
  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"SET", b"str_k", b"v"])),
    b"+OK\r\n"
  );
  let ttl_str = roundtrip(&rt, &mut c, &encode_frame(&[b"TTL", b"str_k"]));
  assert_eq!(ttl_str, b":-1\r\n", "存活字符串键 TTL 应回 -1");
}

/// (ns, db) 复合登记键隔离：db0/db1 同名向量键互不可见，TYPE/EXISTS/
/// DBSIZE/KEYS/SCAN 按会话库判定（doc/zh/db.md §1.1 前缀刚性隔离在登记表
/// 面的打通；C# 每库一实例 VectorManager 的 rust 单例复合键对偶）。
#[test]
fn vector_set_nsdb_isolation_across_dbs() {
  let rt = Runtime::new().unwrap();
  let (mut c, vm, _store) = consumer();
  let db0 = SessionPrefixBuf::new(0, 0);
  let db1 = SessionPrefixBuf::new(0, 1);

  // db0 建同名键 vs_shared
  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"SELECT", b"0"])),
    b"+OK\r\n"
  );
  create_vectorset(&rt, &mut c, b"vs_shared");
  // db1 建同名键 vs_shared + 独有键 vs_only_db1
  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"SELECT", b"1"])),
    b"+OK\r\n"
  );
  create_vectorset(&rt, &mut c, b"vs_shared");
  create_vectorset(&rt, &mut c, b"vs_only_db1");

  // 登记表两域各登记各的：复合键域隔离实证
  assert!(
    vm.read_stored_index(db0.as_slice(), b"vs_shared").is_some(),
    "db0 域应登记 vs_shared"
  );
  assert!(
    vm.read_stored_index(db1.as_slice(), b"vs_shared").is_some(),
    "db1 域应登记 vs_shared"
  );
  assert!(
    vm.read_stored_index(db0.as_slice(), b"vs_only_db1")
      .is_none(),
    "db1 独有键不得泄漏进 db0 域"
  );

  // db1 视角：EXISTS 只见本库 2 键；TYPE 同名键可达
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"EXISTS", b"vs_shared", b"vs_only_db1"])
    ),
    b":2\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"TYPE", b"vs_only_db1"])),
    b"+vectorset\r\n"
  );
  // DBSIZE / KEYS 只计本库
  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"DBSIZE"])),
    b":2\r\n",
    "DBSIZE 应只计 db1 的 2 个向量键"
  );
  let keys_db1 = roundtrip(&rt, &mut c, &encode_frame(&[b"KEYS", b"*"]));
  assert_eq!(keys_db1[0..4], *b"*2\r\n", "KEYS * 应只返回 db1 的 2 键");
  assert_projects_stripped_bulk(&keys_db1, b"vs_only_db1");
  assert_projects_stripped_bulk(&keys_db1, b"vs_shared");
  // SCAN 域内投影（游标 0 单页全量：本库两键均在，且为剥域用户键——
  //  定长 bulk 帧断言，携带登记域前缀即破形）
  let scan_db1 = roundtrip(&rt, &mut c, &encode_frame(&[b"SCAN", b"0"]));
  assert!(
    scan_db1.starts_with(b"*2\r\n$1\r\n0\r\n*2\r\n"),
    "SCAN 应答应为游标 0 + 两键页：{:?}",
    from_utf8(&scan_db1)
  );
  assert_projects_stripped_bulk(&scan_db1, b"vs_only_db1");
  assert_projects_stripped_bulk(&scan_db1, b"vs_shared");

  // 切回 db0：只见 1 键，db1 独有键不可见
  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"SELECT", b"0"])),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"DBSIZE"])),
    b":1\r\n",
    "db0 DBSIZE 应只见本库 1 个向量键"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"EXISTS", b"vs_only_db1"])),
    b":0\r\n",
    "db1 独有键在 db0 应不可见"
  );
  // del 面同样按域：删除 db0 的 vs_shared 不影响 db1 同名键
  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"DEL", b"vs_shared"])),
    b":1\r\n"
  );
  assert!(
    vm.read_stored_index(db1.as_slice(), b"vs_shared").is_some(),
    "删 db0 同名键不得影响 db1 登记"
  );
}

/// FLUSHDB 域回收联动：本库登记条目与 context 随换号回收，他库不受影响
///（C# 每库独立日志物理截断的 rust 复合键回收对偶，清库语义不架空）。
///
/// 域口径：wkv 清库为 O(1) 换号，逻辑 db1 在清库前后指向两个物理域，
/// 断言分别取清库前死域与清库后活域（`store.vdb.get_virtual_ids` 解析），
/// 死域整域清零即回收实证，重建键只允许落活域。
#[test]
fn vector_set_flushdb_registry_reclaim() {
  let rt = Runtime::new().unwrap();
  let (mut c, vm, store) = consumer();
  let db0 = SessionPrefixBuf::new(0, 0);

  // db0 与 db1 各建向量键
  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"SELECT", b"0"])),
    b"+OK\r\n"
  );
  create_vectorset(&rt, &mut c, b"vs_a");
  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"SELECT", b"1"])),
    b"+OK\r\n"
  );
  create_vectorset(&rt, &mut c, b"vs_b");

  // 清库前逻辑 db1 的物理域：登记条目正落于此域
  let (dead_vns, dead_vdb) = store.vdb.get_virtual_ids(0, 1);
  let dead = SessionPrefixBuf::new(dead_vns, dead_vdb);
  assert!(
    vm.read_stored_index(dead.as_slice(), b"vs_b").is_some(),
    "清库前本库登记应在位"
  );
  assert_eq!(
    vm.registry_domain_count(dead.as_slice()),
    1,
    "清库前本库应恰有一项登记"
  );
  assert_eq!(
    vm.registry_domain_count(db0.as_slice()),
    1,
    "db0 应恰有一项登记"
  );

  // FLUSHDB db1：仅本库死域回收
  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"FLUSHDB"])),
    b"+OK\r\n"
  );
  let (live_vns, live_vdb) = store.vdb.get_virtual_ids(0, 1);
  let live = SessionPrefixBuf::new(live_vns, live_vdb);
  assert_ne!(
    (live_vns, live_vdb),
    (dead_vns, dead_vdb),
    "FLUSHDB 应换号（逻辑库重指新物理域）"
  );
  assert_eq!(
    vm.registry_domain_count(dead.as_slice()),
    0,
    "FLUSHDB 后死域登记应整域回收"
  );
  assert_eq!(
    vm.registry_domain_count(live.as_slice()),
    0,
    "新域在重建前不得有登记条目"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"EXISTS", b"vs_b"])),
    b":0\r\n",
    "FLUSHDB 后本库键不可见"
  );
  assert!(
    vm.read_stored_index(db0.as_slice(), b"vs_a").is_some(),
    "FLUSHDB db1 不得回收 db0 登记"
  );

  // 换号后同键名重建走新域登记（死域不得再被写入）
  create_vectorset(&rt, &mut c, b"vs_b");
  assert!(
    vm.read_stored_index(live.as_slice(), b"vs_b").is_some(),
    "清库后同键名重建应落新域登记"
  );
  assert_eq!(
    vm.registry_domain_count(dead.as_slice()),
    0,
    "重建写入不得回落到死域"
  );

  // db0 全程不受影响
  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"SELECT", b"0"])),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"TYPE", b"vs_a"])),
    b"+vectorset\r\n",
    "他库向量键在 FLUSHDB 后应原样存活"
  );
}

/// 剥域单点确定性用例：[`registry_key`] 构造的复合键，[`registry_user_key`]
/// 还原结果逐字节等于用户键（零域/大域、空键、栈内与堆外两径）。
///
/// 比对取整切片相等而非「以用户键结尾」：复合键本就以用户键为后缀，包含式
/// 断言在「兜底返回带域前缀整键」时恒真，起不到门禁作用。
#[test]
fn registry_user_key_strips_to_exact_user_key() {
  let long_key = vec![b'v'; wval::STACK_KEY_CAP + 7];
  let cases: [&[u8]; 4] = [&[], b"k", b"my_vectorset", long_key.as_slice()];
  for (ns, db) in [(0u64, 0u64), (7, 3), (1 << 40, 1 << 33)] {
    let prefix = SessionPrefixBuf::new(ns, db);
    for key in cases {
      let rk = registry_key(prefix.as_slice(), key);
      assert!(
        rk.len() > key.len(),
        "复合键必带域前缀段（域 {ns}/{db} 键 {key:?}），否则本用例恒真"
      );
      assert_eq!(
        registry_user_key(rk.as_slice()),
        key,
        "域 {ns}/{db} 键 {key:?} 剥域应逐字节还原用户键"
      );
    }
  }
}

/// 畸形输入显式失败：剥域单点绝不把带域前缀的整键当用户键发出去。
///
/// 断言形态取 `should_panic`——catch/unwind 兜住后放过等于没有兜底方向。
#[test]
#[should_panic(expected = "不变量破坏")]
fn registry_user_key_fails_loud_on_malformed_composite() {
  // 首段 varint 只有延续位、无后继字节：registry_key 不可能产出该形态
  registry_user_key(&[0x80]);
}
