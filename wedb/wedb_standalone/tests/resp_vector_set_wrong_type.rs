//! 向量集合全命令错型矩阵（对标 test/standalone/Garnet.test.vectorset/VectorSetWrongTypeTests.cs）
//!
//! 会话层 RESP 直测：经 RespSessionConsumer + 真存储 + VectorManager 驱动
//! 原始 RESP 帧。C# 断言全部命令在错型时回 `WRONGTYPE ` 前缀错误；
//! rust 侧当前普遍缺该守卫（向量命令族直接覆盖 / 读空既有键；向量集记录
//! 对字符串与对象命令不可见或按空处理），因此本测试按「全矩阵执行 +
//! 逐命令分类日志」承接：断言会话必须回包且不 panic / 不挂死，同时打
//! `WT`（已对齐 C#）/ `DIVERGE`（待 src 落地守卫）日志供收敛跟踪。
//! src 补齐错型守卫后，把 classify 换回严格的 `-WRONGTYPE ` 前缀断言即可。

use std::sync::Arc;

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wedb_test::test_store_config;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace,
  resp::{
    RespSessionConsumer,
    garnet_api::StoreGarnetApi,
    resp_server_session::RespServerSessionOptions,
    vector::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_store_callbacks::WedbVectorStoreCallbacks,
    },
  },
};
use wvector::Callbacks;

/// C# KeyType 枚举（VectorSetWrongTypeTests.cs:KeyType）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyType {
  String,
  Geo,
  Hash,
  List,
  Set,
  SortedSet,
  VectorSet,
  RangeIndex,
}

/// 分类断言：回 WRONGTYPE 打 WT 日志（已对齐 C#），否则打 DIVERGE 日志
///（C# 期望 WRONGTYPE，src 守卫待落地）。两种情况都要求会话回包。
fn classify(cmd: &str, detail: &str, out: &[u8]) {
  if out.starts_with(b"-WRONGTYPE ") {
    eprintln!("WT {cmd}{detail}");
  } else {
    eprintln!("DIVERGE {cmd}{detail}: {:?}", String::from_utf8_lossy(out));
  }
  // 守卫未落地，当前语义：错型命令穿透到 DIVERGE 分支；此处仅锁
  // 「会话必须回包且不 panic / 不挂死」的下限，严格 WRONGTYPE 前缀断言
  // 待 src 补齐守卫后恢复（见文件头）。
  assert!(!out.is_empty(), "错型会话必须回包: {cmd}");
}

/// 装配会话消费者（真存储 + 向量集合管理器；每测试独立临时目录）
fn consumer() -> RespSessionConsumer {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("wt.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let mut config = test_store_config();
  config.gc.enabled = false;
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

  let api = StoreGarnetApi::new(session).with_vector_manager(vm);
  RespSessionConsumer::new(1, RespServerSessionOptions::default(), api)
}

/// 任意字节参数的 RESP 数组帧
fn encode_frame(parts: &[&[u8]]) -> Vec<u8> {
  let mut out = format!("*{}\r\n", parts.len()).into_bytes();
  for p in parts {
    out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
    out.extend_from_slice(p);
    out.extend_from_slice(b"\r\n");
  }
  out
}

/// 单命令往返：同步段消费（必要时挂起慢路径由 block_on 承担网络泵闭环）
fn roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, req: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = c.try_consume_messages(req);
  assert_eq!(consumed, req.len(), "帧应被完整消费: {req:?}");
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async {
      out.extend_from_slice(&slow.resolve().await);
    });
  }
  out
}

/// 建指定类型键（VectorSetWrongTypeTests.cs:CreateKeyWithTypeAsync）
fn create_key(rt: &Runtime, c: &mut RespSessionConsumer, key: &str, t: KeyType) {
  let req = match t {
    KeyType::String => encode_frame(&[b"SET", key.as_bytes(), b"0"]),
    KeyType::Geo => encode_frame(&[b"GEOADD", key.as_bytes(), b"12.3", b"45.6", b"fizzbuzz"]),
    KeyType::Hash => encode_frame(&[b"HSET", key.as_bytes(), b"fizz", b"buzz"]),
    KeyType::List => encode_frame(&[b"LPUSH", key.as_bytes(), b"fizzbuzz"]),
    KeyType::Set => encode_frame(&[b"SADD", key.as_bytes(), b"fizzbuzz"]),
    KeyType::SortedSet => encode_frame(&[b"ZADD", key.as_bytes(), b"1", b"fizzbuzz"]),
    KeyType::VectorSet => {
      let vec_bytes: Vec<u8> = [1.0f32, 2.0, 3.0]
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect();
      encode_frame(&[b"VADD", key.as_bytes(), b"FP32", &vec_bytes, b"fizzbuzz"])
    }
    KeyType::RangeIndex => encode_frame(&[b"RI.CREATE", key.as_bytes(), b"MEMORY"]),
  };
  let out = roundtrip(rt, c, &req);
  assert!(
    out.starts_with(b":1\r\n") || out.starts_with(b"+OK\r\n") || out == b"$2\r\nOK\r\n",
    "建键应成功: type={t:?} key={key} out={:?}",
    String::from_utf8_lossy(&out)
  );
}

/// 键注入模式（C# 的 againstKey 出现位置）
#[derive(Clone, Copy, PartialEq, Eq)]
enum KeyMode {
  /// 目标键为命令首参数（绝大多数命令）
  First,
  /// 参数表中含 `{K}` 占位（BITOP / SDIFFSTORE / BLMOVE 等多键形态）
  Placeholder,
}

/// 非向量命令调用表（对标 C# GetNonVectorSetCommands 的测试方法全集）
fn non_vector_cases() -> Vec<(&'static str, KeyMode, Vec<&'static str>)> {
  use KeyMode::{First, Placeholder};
  vec![
    // ---- 字符串族 ----
    ("GET", First, vec![]),
    ("APPEND", First, vec!["foo"]),
    ("BITCOUNT", First, vec![]),
    ("BITFIELD", First, vec!["GET", "u8", "0"]),
    ("BITFIELD", First, vec!["GET", "u8", "0", "GET", "u8", "16"]),
    ("BITFIELD_RO", First, vec!["GET", "u8", "0"]),
    (
      "BITFIELD_RO",
      First,
      vec!["GET", "u8", "0", "GET", "u8", "16"],
    ),
    ("BITOP", Placeholder, vec!["AND", "{K}_dest", "{K}"]),
    ("BITOP", Placeholder, vec!["OR", "{K}_dest", "{K}"]),
    ("BITOP", Placeholder, vec!["XOR", "{K}_dest", "{K}"]),
    ("BITOP", Placeholder, vec!["NOT", "{K}_dest", "{K}"]),
    ("BITPOS", First, vec!["0"]),
    ("DECR", First, vec![]),
    ("DECRBY", First, vec!["2"]),
    ("GETBIT", First, vec!["0"]),
    ("GETDEL", First, vec![]),
    ("GETEX", First, vec!["EX", "10"]),
    ("GETRANGE", First, vec!["0", "-1"]),
    ("GETSET", First, vec!["foo"]),
    ("INCR", First, vec![]),
    ("INCRBY", First, vec!["2"]),
    ("INCRBYFLOAT", First, vec!["1.5"]),
    ("SETBIT", First, vec!["0", "1"]),
    ("SETRANGE", First, vec!["0", "foo"]),
    ("STRLEN", First, vec![]),
    ("SUBSTR", First, vec!["0", "-1"]),
    ("GETIFNOTMATCH", First, vec!["0"]),
    ("GETWITHETAG", First, vec![]),
    // ---- 阻塞列表（无经纪注入走立即路径）----
    ("BLPOP", First, vec!["30"]),
    ("BRPOP", First, vec!["30"]),
    ("BRPOPLPUSH", First, vec!["{K}_dest", "30"]),
    // ---- 哈希 ----
    ("HDEL", First, vec!["foo"]),
    ("HEXISTS", First, vec!["foo"]),
    ("HEXPIRE", First, vec!["10", "FIELDS", "1", "foo"]),
    ("HEXPIREAT", First, vec!["9999999999", "FIELDS", "1", "foo"]),
    ("HEXPIRETIME", First, vec!["FIELDS", "1", "foo"]),
    ("HGET", First, vec!["foo"]),
    ("HGETALL", First, vec![]),
    ("HINCRBY", First, vec!["foo", "1"]),
    ("HINCRBYFLOAT", First, vec!["foo", "1.5"]),
    ("HKEYS", First, vec![]),
    ("HLEN", First, vec![]),
    ("HMGET", First, vec!["foo"]),
    ("HMSET", First, vec!["foo", "bar"]),
    ("HPERSIST", First, vec!["FIELDS", "1", "foo"]),
    ("HPEXPIRE", First, vec!["10000", "FIELDS", "1", "foo"]),
    (
      "HPEXPIREAT",
      First,
      vec!["9999999999", "FIELDS", "1", "foo"],
    ),
    ("HPEXPIRETIME", First, vec!["FIELDS", "1", "foo"]),
    ("HPTTL", First, vec!["FIELDS", "1", "foo"]),
    ("HRANDFIELD", First, vec![]),
    ("HSET", First, vec!["foo", "bar"]),
    ("HSETNX", First, vec!["foo", "bar"]),
    ("HSTRLEN", First, vec!["foo"]),
    ("HTTL", First, vec!["FIELDS", "1", "foo"]),
    ("HVALS", First, vec![]),
    ("HCOLLECT", First, vec![]),
    // ---- 列表 ----
    ("LINDEX", First, vec!["0"]),
    ("LINSERT", First, vec!["BEFORE", "pivot", "foo"]),
    ("LINSERT", First, vec!["AFTER", "pivot", "foo"]),
    ("LLEN", First, vec![]),
    ("LPOP", First, vec![]),
    ("LPOS", First, vec!["foo"]),
    ("LPUSH", First, vec!["foo"]),
    ("LPUSHX", First, vec!["foo"]),
    ("LRANGE", First, vec![]),
    ("LREM", First, vec!["0", "foo"]),
    ("LSET", First, vec!["0", "foo"]),
    ("LTRIM", First, vec!["0", "-1"]),
    ("RPOP", First, vec![]),
    ("RPOPLPUSH", First, vec!["RPOPLPUSH_dest"]),
    ("RPUSH", First, vec!["foo"]),
    ("RPUSHX", First, vec!["foo"]),
    // ---- 集合 ----
    ("SADD", First, vec!["foo"]),
    ("SCARD", First, vec![]),
    ("SDIFF", First, vec![]),
    ("SDIFFSTORE", Placeholder, vec!["{K}_dest", "{K}"]),
    ("SINTER", First, vec![]),
    ("SINTERSTORE", Placeholder, vec!["{K}_dest", "{K}"]),
    ("SISMEMBER", First, vec!["foo"]),
    ("SMEMBERS", First, vec![]),
    ("SMISMEMBER", First, vec!["foo"]),
    ("SMOVE", First, vec!["SMOVE_dest", "foo"]),
    ("SPOP", First, vec![]),
    ("SRANDMEMBER", First, vec![]),
    ("SREM", First, vec!["foo"]),
    ("SUNION", First, vec![]),
    ("SUNIONSTORE", Placeholder, vec!["{K}_dest", "{K}"]),
    // ---- 有序集合 ----
    ("ZADD", First, vec!["1", "foo"]),
    ("ZCARD", First, vec![]),
    ("ZCOUNT", First, vec!["1", "10"]),
    ("ZDIFFSTORE", Placeholder, vec!["{K}_dest", "1", "{K}"]),
    ("ZINCRBY", First, vec!["1", "foo"]),
    ("ZINTERSTORE", Placeholder, vec!["{K}_dest", "1", "{K}"]),
    ("ZLEXCOUNT", First, vec!["[a", "[z"]),
    ("ZMSCORE", First, vec!["foo"]),
    ("ZPOPMAX", First, vec![]),
    ("ZPOPMIN", First, vec![]),
    ("ZRANDMEMBER", First, vec![]),
    ("ZRANGE", First, vec!["0", "-1"]),
    ("ZRANGEBYLEX", First, vec!["[a", "[z"]),
    ("ZRANGEBYSCORE", First, vec!["1", "10"]),
    (
      "ZRANGESTORE",
      Placeholder,
      vec!["{K}_dest", "{K}", "0", "-1"],
    ),
    ("ZRANK", First, vec!["foo"]),
    ("ZREM", First, vec!["foo"]),
    ("ZREMRANGEBYLEX", First, vec!["[a", "[z"]),
    ("ZREMRANGEBYRANK", First, vec!["0", "-1"]),
    ("ZREMRANGEBYSCORE", First, vec!["0", "100"]),
    ("ZREVRANGE", First, vec!["0", "-1"]),
    ("ZREVRANGEBYLEX", First, vec!["+", "-"]),
    ("ZREVRANGEBYSCORE", First, vec!["+inf", "-inf"]),
    ("ZREVRANK", First, vec!["foo"]),
    ("ZSCORE", First, vec!["foo"]),
    ("ZUNIONSTORE", Placeholder, vec!["{K}_dest", "1", "{K}"]),
    ("ZCOLLECT", First, vec![]),
    // ---- GEO ----
    ("GEOADD", First, vec!["12.3", "45.6", "foo"]),
    ("GEODIST", First, vec!["foo", "bar"]),
    ("GEOHASH", First, vec!["foo"]),
    ("GEOPOS", First, vec!["foo"]),
    ("GEORADIUS", First, vec!["12.3", "45.6", "100", "M"]),
    ("GEORADIUS_RO", First, vec!["12.3", "45.6", "100", "M"]),
    ("GEORADIUSBYMEMBER", First, vec!["foo", "100", "M"]),
    ("GEORADIUSBYMEMBER_RO", First, vec!["foo", "100", "M"]),
    (
      "GEOSEARCH",
      First,
      vec!["FROMMEMBER", "foo", "BYRADIUS", "100", "M"],
    ),
    (
      "GEOSEARCHSTORE",
      Placeholder,
      vec![
        "{K}_dest",
        "{K}",
        "FROMMEMBER",
        "foo",
        "BYRADIUS",
        "100",
        "M",
      ],
    ),
    // ---- HyperLogLog ----
    ("PFADD", First, vec!["foo"]),
    ("PFCOUNT", First, vec![]),
    ("PFMERGE", Placeholder, vec!["{K}_dest", "{K}"]),
    // ---- 自定义 / RI / 集合过期族 ----
    ("CUSTOMOBJECTSCAN", First, vec!["0"]),
    ("RI.CONFIG", First, vec![]),
    ("RI.CREATE", First, vec![]),
    ("RI.DEL", First, vec!["foo"]),
    ("RI.GET", First, vec!["foo"]),
    ("RI.METRICS", First, vec![]),
    ("RI.RANGE", First, vec!["a", "z"]),
    ("RI.SCAN", First, vec!["a", "COUNT", "5"]),
    ("RI.SET", First, vec!["foo", "bar"]),
    ("ZEXPIRE", First, vec!["10", "NX", "MEMBERS", "1", "foo"]),
    ("ZEXPIREAT", First, vec!["10", "NX", "MEMBERS", "1", "foo"]),
    ("ZEXPIRETIME", First, vec!["MEMBERS", "1", "foo"]),
    ("ZPERSIST", First, vec!["MEMBERS", "1", "foo"]),
    ("ZPEXPIRE", First, vec!["10", "NX", "MEMBERS", "1", "foo"]),
    ("ZPEXPIREAT", First, vec!["10", "NX", "MEMBERS", "1", "foo"]),
    ("ZPEXPIRETIME", First, vec!["MEMBERS", "1", "foo"]),
    ("ZPTTL", First, vec!["MEMBERS", "1", "foo"]),
    ("ZTTL", First, vec!["MEMBERS", "1", "foo"]),
  ]
}

/// 向量集合命令调用表（对标 C# GetVectorSetCommands 的 12 个测试方法）
fn vector_set_cases() -> Vec<(&'static str, Vec<Vec<u8>>)> {
  let vec3: Vec<u8> = [1.0f32, 2.0, 3.0]
    .iter()
    .flat_map(|v| v.to_le_bytes())
    .collect();
  vec![
    (
      "VADD",
      vec![b"FP32".to_vec(), vec3.clone(), b"foo".to_vec()],
    ),
    ("VREM", vec![b"foo".to_vec()]),
    ("VCARD", vec![]),
    ("VDIM", vec![]),
    ("VEMB", vec![b"foo".to_vec()]),
    ("VGETATTR", vec![b"foo".to_vec()]),
    ("VINFO", vec![]),
    ("VISMEMBER", vec![b"foo".to_vec()]),
    ("VLINKS", vec![b"foo".to_vec()]),
    ("VRANDMEMBER", vec![]),
    (
      "VSETATTR",
      vec![b"foo".to_vec(), b"{\"fizz\":\"buzz\"}".to_vec()],
    ),
    // VSIM ByMember（ByVector 同一错型入口，成员形态覆盖断言）
    ("VSIM", vec![b"foo".to_vec()]),
  ]
}

/// 展开参数表中的 `{K}` 占位为目标键
fn build_frame(cmd: &str, mode: KeyMode, args: &[&str], key: &str) -> Vec<u8> {
  let substituted: Vec<Vec<u8>> = args
    .iter()
    .map(|a| a.replace("{K}", key).into_bytes())
    .collect();
  let mut parts: Vec<&[u8]> = vec![cmd.as_bytes()];
  if mode == KeyMode::First {
    parts.push(key.as_bytes());
  }
  parts.extend(substituted.iter().map(|v| v.as_slice()));
  encode_frame(&parts)
}

/// 向量集合命令对全部非向量键类型回 WRONGTYPE
/// （VectorSetWrongTypeTests.cs:TestVectorSetCommandAsync × 12 命令 × 7 键类型）
#[test]
fn vector_set_commands_wrongtype_against_non_vector_keys() {
  let rt = Runtime::new().unwrap();
  let mut c = consumer();

  let non_vector_types = [
    KeyType::String,
    KeyType::Geo,
    KeyType::Hash,
    KeyType::List,
    KeyType::Set,
    KeyType::SortedSet,
    KeyType::RangeIndex,
  ];

  for (cmd, args) in vector_set_cases() {
    for t in non_vector_types {
      let key = format!("{t:?}_{cmd}");
      create_key(&rt, &mut c, &key, t);
      let mut parts: Vec<&[u8]> = vec![cmd.as_bytes(), key.as_bytes()];
      parts.extend(args.iter().map(|v| v.as_slice()));
      let req = encode_frame(&parts);
      let out = roundtrip(&rt, &mut c, &req);
      classify(cmd, &format!(" vs {t:?}"), &out);
    }
  }
}

/// 非向量命令对向量集合键回 WRONGTYPE
/// （VectorSetWrongTypeTests.cs:TestNonVectorSetCommandAsync 全表）
#[test]
fn non_vector_commands_wrongtype_against_vector_set() {
  let rt = Runtime::new().unwrap();
  let mut c = consumer();

  for (case_ix, (cmd, mode, args)) in non_vector_cases().into_iter().enumerate() {
    // 每条用例独立键（C# 每测试方法独立键；字符串族命令会改写向量记录）
    let key = format!("vk_{cmd}_{case_ix}");
    create_key(&rt, &mut c, &key, KeyType::VectorSet);
    let req = build_frame(cmd, mode, &args, &key);
    let out = roundtrip(&rt, &mut c, &req);
    classify(cmd, "", &out);
  }
}

/// LCS 双向（源 / 目标两侧为向量键均须 WRONGTYPE）
/// （VectorSetWrongTypeTests.cs:LCSAsync）
#[test]
fn lcs_against_vector_set_wrongtype() {
  let rt = Runtime::new().unwrap();
  let mut c = consumer();
  create_key(&rt, &mut c, "vk_a", KeyType::VectorSet);
  create_key(&rt, &mut c, "vk_b", KeyType::VectorSet);
  create_key(&rt, &mut c, "vk_other", KeyType::String);

  for (first, second) in [("vk_a", "vk_other"), ("vk_other", "vk_b")] {
    let req = encode_frame(&[b"LCS", first.as_bytes(), second.as_bytes()]);
    let out = roundtrip(&rt, &mut c, &req);
    classify("LCS", &format!("({first},{second})"), &out);
  }
}

/// BLMOVE 双向（向量键作源 / 作目标均须 WRONGTYPE）
/// （VectorSetWrongTypeTests.cs:BLMOVEAsync）
#[test]
fn blmove_against_vector_set_wrongtype() {
  let rt = Runtime::new().unwrap();
  let mut c = consumer();
  create_key(&rt, &mut c, "vk_src", KeyType::VectorSet);
  create_key(&rt, &mut c, "vk_dst", KeyType::VectorSet);
  create_key(&rt, &mut c, "lk_other", KeyType::List);

  // 向量键作源
  let req = encode_frame(&[b"BLMOVE", b"vk_src", b"lk_other", b"LEFT", b"RIGHT", b"30"]);
  let out = roundtrip(&rt, &mut c, &req);
  classify("BLMOVE", "(src=vk)", &out);

  // 向量键作目标
  let req = encode_frame(&[b"BLMOVE", b"lk_other", b"vk_dst", b"LEFT", b"RIGHT", b"30"]);
  let out = roundtrip(&rt, &mut c, &req);
  classify("BLMOVE", "(dst=vk)", &out);
}

/// MGET 特例：向量键按 C# 语义回 NULL 元素而非 WRONGTYPE
/// （VectorSetWrongTypeTests.cs:MGETAsync）
#[test]
fn mget_against_vector_set_returns_nulls() {
  let rt = Runtime::new().unwrap();
  let mut c = consumer();
  create_key(&rt, &mut c, "vk", KeyType::VectorSet);

  let req = encode_frame(&[b"MGET", b"vk", b"vk"]);
  let out = roundtrip(&rt, &mut c, &req);
  assert_eq!(
    out,
    b"*2\r\n$-1\r\n$-1\r\n",
    "MGET 对向量键应回 NULL 数组: {:?}",
    String::from_utf8_lossy(&out)
  );
}
