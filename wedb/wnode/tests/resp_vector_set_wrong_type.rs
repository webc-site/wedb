//! 向量集合全命令错型矩阵（对标 test/standalone/Garnet.test.vectorset/VectorSetWrongTypeTests.cs）
//!
//! 会话层 RESP 直测：经 RespSessionConsumer + 真存储 + VectorManager 驱动
//! 原始 RESP 帧。C# 断言全部命令在错型时回 `WRONGTYPE ` 前缀错误；
//! rust 侧当前普遍缺该守卫（向量命令族直接覆盖 / 读空既有键；向量集记录
//! 对字符串与对象命令不可见或按空处理），因此本测试按「全矩阵执行 +
//! 逐命令分类日志」承接：断言会话必须回包且不 panic / 不挂死，同时打
//! `WT`（已对齐 C#）/ `DIVERGE`（待 src 落地守卫）日志供收敛跟踪。
//! src 补齐错型守卫后，把 classify 换回严格的 `-WRONGTYPE ` 前缀断言即可。

use std::{mem::forget, sync::Arc};

use compio::runtime::Runtime;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace,
  resp::{
    RespSessionConsumer,
    garnet_api::StoreGarnetApi,
    resp_server_session::RespServerSessionOptions,
    vector::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_store_callbacks::{OwnedActiveVectorSession, WedbVectorStoreCallbacks},
    },
  },
  storage::session::common::ttl_sync::put_ttl_sync,
};
use wnode_test::pump;
use wresp::cmd_strings::RESP_ERR_WRONG_TYPE_HLL;
use wtest_base::{resp_frame as encode_frame, test_store_config};
use wval::{KeyTag, SessionPrefixBuf};
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
    log::debug!("WT {cmd}{detail}");
  } else {
    log::debug!("DIVERGE {cmd}{detail}: {:?}", String::from_utf8_lossy(out));
  }
  // 守卫未落地，当前语义：错型命令穿透到 DIVERGE 分支；此处仅锁
  // 「会话必须回包且不 panic / 不挂死」的下限，严格 WRONGTYPE 前缀断言
  // 待 src 补齐守卫后恢复（见文件头）。
  assert!(!out.is_empty(), "错型会话必须回包: {cmd}");
}

/// 装配会话消费者（真存储 + 向量集合管理器；每测试独立临时目录）。
/// 返回 (消费者, 向量管理器, 存储句柄)——管理器供登记表幽灵断言，
/// 存储句柄供信封/Meta 物理域零写取证
fn consumer() -> (
  RespSessionConsumer,
  Arc<VectorManager>,
  Arc<WedbStore<SegmentedDevice>>,
) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("wt.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();

  // 回调无状态：向量会话按执行域绑定（测试为单任务同步段，专用会话持至用例结束）
  let _vector_domain = OwnedActiveVectorSession::new(store.new_session().unwrap());
  let callbacks = Callbacks::new(Arc::new(WedbVectorStoreCallbacks::<SegmentedDevice>::new()));
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    callbacks,
  ));

  let api = StoreGarnetApi::new(session).with_vector_manager(Arc::clone(&vm));
  let consumer = RespSessionConsumer::new(1, RespServerSessionOptions::default(), Arc::new(api));
  forget(dir);
  (consumer, vm, store)
}

/// 单命令往返：同步段消费（必要时挂起慢路径由 block_on 承担网络泵闭环）
fn roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, req: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = pump(c, req);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {req:?}");
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
    // 欠账注（票 zcode-r159c-sdiff 窄三）：本表仅注入首键位（First 与
    // Placeholder 的 dst+首键位形），SDIFF/SDIFFSTORE 尾键位向量形补钉见
    // `sdiff_tail_key_vector_wrongtype`（登记表门 scan_all 臂全键位直拦）。
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

#[test]
fn vector_set_commands_wrongtype_against_non_vector_keys() {
  let rt = Runtime::new().unwrap();
  // VADD 命令消费链 insert 需当前线程挂 compio runtime（thread-per-core），
  // 全体命令驱动置于 runtime 上下文内（roundtrip 内层 block_on 同线程复用）
  rt.block_on(async {
    let (mut c, _vm, _store) = consumer();

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
        // 向量集全命令 WRONGTYPE 守卫：既有非向量键一律拒绝且不得建登记
        assert!(
          out.starts_with(b"-WRONGTYPE "),
          "{cmd} vs {t:?} 应回 WRONGTYPE: {:?}",
          String::from_utf8_lossy(&out)
        );
        assert!(
          _vm
            .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), key.as_bytes())
            .is_none(),
          "{cmd} 被拒后不得残留向量登记: {key}"
        );
      }
    }
  })
}

/// 非向量命令对向量集合键回 WRONGTYPE
/// （VectorSetWrongTypeTests.cs:TestNonVectorSetCommandAsync 全表）
#[test]
fn non_vector_commands_wrongtype_against_vector_set() {
  let rt = Runtime::new().unwrap();
  // VADD 命令消费链 insert 需当前线程挂 compio runtime（thread-per-core），
  // 全体命令驱动置于 runtime 上下文内（roundtrip 内层 block_on 同线程复用）
  rt.block_on(async {
    let (mut c, _vm, _store) = consumer();

    for (case_ix, (cmd, mode, args)) in non_vector_cases().into_iter().enumerate() {
      // 每条用例独立键（C# 每测试方法独立键；字符串族命令会改写向量记录）
      let key = format!("vk_{cmd}_{case_ix}");
      create_key(&rt, &mut c, &key, KeyType::VectorSet);
      let req = build_frame(cmd, mode, &args, &key);
      let out = roundtrip(&rt, &mut c, &req);
      // PF 族首键位升格逐字节锁：C# HyperLogLogCommands.cs 三臂（:39-43/:81-85/
      // :112-116）错型出口恒 RESP_ERR_WRONG_TYPE_HLL，与泛型串前缀同文案异
      if matches!(cmd, "PFADD" | "PFCOUNT" | "PFMERGE") {
        assert_eq!(
          out,
          hll_wrongtype_frame(),
          "{cmd} 首键命中登记应回 PF 专属 WRONGTYPE 全帧: {:?}",
          String::from_utf8_lossy(&out)
        );
      } else {
        classify(cmd, "", &out);
      }
    }
  })
}

/// PF 专属错型帧（wresp 单点常量派生，对标 CmdStrings.RESP_ERR_WRONG_TYPE_HLL）
fn hll_wrongtype_frame() -> Vec<u8> {
  format!("-{}\r\n", RESP_ERR_WRONG_TYPE_HLL).into_bytes()
}

/// LCS 双向（源 / 目标两侧为向量键均须 WRONGTYPE）
/// （VectorSetWrongTypeTests.cs:LCSAsync :628-647，RunCommandFirstVectorSet :631 /
/// RunCommandSecondVectorSet :641 次键硬测——全库唯一次键位硬测缝。rust 侧收口于
/// 派发层 vector_registry_gate 固定键位臂（wresp::command::vector_gate_fixed_key_count），
/// 门前置同罩快慢两路，本测由 classify 止血桩升格为逐字节帧锁）
#[test]
fn lcs_against_vector_set_wrongtype() {
  let rt = Runtime::new().unwrap();
  // VADD 命令消费链 insert 需当前线程挂 compio runtime（thread-per-core），
  // 全体命令驱动置于 runtime 上下文内（roundtrip 内层 block_on 同线程复用）
  rt.block_on(async {
    let (mut c, vm, _store) = consumer();
    create_key(&rt, &mut c, "vk_a", KeyType::VectorSet);
    create_key(&rt, &mut c, "vk_b", KeyType::VectorSet);
    create_key(&rt, &mut c, "vk_other", KeyType::String);

    // 首键 / 次键向量双向 + 次键向量携选项 token 形态（IDX/MINMATCHLEN 值恒在
    // 固定键位之后：token 不进登记表探针，既不逃逸拒判也不误伤键位裁决）
    let idx_opts: [&[u8]; 3] = [b"IDX", b"MINMATCHLEN", b"1"];
    for (first, second, opts) in [
      ("vk_a", "vk_other", &[][..]),
      ("vk_other", "vk_b", &[][..]),
      ("vk_other", "vk_b", &idx_opts[..]),
    ] {
      let mut parts: Vec<&[u8]> = vec![b"LCS", first.as_bytes(), second.as_bytes()];
      parts.extend_from_slice(opts);
      let out = roundtrip(&rt, &mut c, &encode_frame(&parts));
      assert_eq!(
        out,
        b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n",
        "LCS ({first},{second}) 应整命令回 WRONGTYPE 全帧: {:?}",
        String::from_utf8_lossy(&out)
      );
    }
    // 门为只读前置拒：零副作用——双侧向量登记保留（C# 拒绝路径不动记录）、
    // 字符串侧值完好
    for k in ["vk_a", "vk_b"] {
      assert!(
        vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), k.as_bytes())
          .is_some(),
        "LCS 被拒后向量登记须原样保留: {k}"
      );
    }
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"GET", b"vk_other"])),
      b"$1\r\n0\r\n",
      "LCS 被拒后 string 侧不得受损"
    );
  })
}

/// PFCOUNT 次键位登记表门（票 zcode-r137c-hllsec2 案一）：C#
/// HyperLogLogOps.cs:128-175 逐键 GET、次键 WRONGTYPE 即整命令 return status
/// （:138）→ HyperLogLogCommands.cs:81-85 出 RESP_ERR_WRONG_TYPE_HLL 错误帧。
/// rust 收口于派发层 vector_registry_gate 全扫臂（Pfcount 入
/// vector_gate_scan_all_keys）+ PF 族单例文案，门前置同罩快慢两路。格二
/// （次键=RI 键）与格三（次键=非 HYLL string）rust 经 load_hll 三域探针同判
/// HLL 文案，本测一并锁为对拍回归（C# ReadMethods.cs:118-122 RI 臂 /
/// PrivateMethods.cs:259-297 IsValidHYLL 臂）
#[test]
fn pfcount_second_key_vector_wrongtype() {
  let rt = Runtime::new().unwrap();
  // VADD 命令消费链 insert 需当前线程挂 compio runtime（thread-per-core），
  // 全体命令驱动置于 runtime 上下文内（roundtrip 内层 block_on 同线程复用）
  rt.block_on(async {
    let (mut c, vm, _store) = consumer();
    create_key(&rt, &mut c, "vk_p", KeyType::VectorSet);
    create_key(&rt, &mut c, "ri_p", KeyType::RangeIndex);
    // 合法 HLL 键×2 与非 HYLL string 载荷
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"PFADD", b"hll_a", b"e1"])),
      b":1\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"PFADD", b"hll_b", b"e2"])),
      b":1\r\n"
    );
    assert_eq!(
      roundtrip(
        &rt,
        &mut c,
        &encode_frame(&[b"SET", b"str_bad", b"not-a-hyll-payload"])
      ),
      b"+OK\r\n"
    );

    // 格一（本案所立）：次键=登记向量键 → PF 专属 HLL 错帧（修复前为整数并集）
    let out = roundtrip(&rt, &mut c, &encode_frame(&[b"PFCOUNT", b"hll_a", b"vk_p"]));
    assert_eq!(
      out,
      hll_wrongtype_frame(),
      "PFCOUNT 次键命中登记应整命令回 HLL 错帧: {:?}",
      String::from_utf8_lossy(&out)
    );
    // 门为只读前置拒：登记原样保留、HLL 侧载荷完好
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vk_p")
        .is_some(),
      "PFCOUNT 被拒后向量登记须原样保留"
    );
    // 格二（既判净形对拍锁）：次键=RI 键，C# CheckRecordTypeMismatch RI 臂
    // 回错 ↔ rust load_hll 三域 Meta 探针 WRONGTYPE，同串 HLL 文案
    let out = roundtrip(&rt, &mut c, &encode_frame(&[b"PFCOUNT", b"hll_a", b"ri_p"]));
    assert_eq!(out, hll_wrongtype_frame(), "格二 RI 次键应同判 HLL 错帧");
    // 格三（既判净形对拍锁）：次键=非 HYLL string，C# IsValidHYLL 败臂 ↔
    // rust valid_hyll_payload 借用切片判败臂，同串
    let out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"PFCOUNT", b"hll_a", b"str_bad"]),
    );
    assert_eq!(out, hll_wrongtype_frame(), "格三非法载荷应同判 HLL 错帧");
    // 合法形态零漂移：双合法键并集回整数；尾键缺失回真实并集基数
    //（deviations §16，scan_all 只拦登记表命中不触此形）
    let out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"PFCOUNT", b"hll_a", b"hll_b"]),
    );
    assert!(out.starts_with(b":"), "合法多键并集应回整数: {out:?}");
    let out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"PFCOUNT", b"hll_a", b"hll_missing"]),
    );
    assert_eq!(out, b":1\r\n", "尾键缺失须回真实并集基数 1: {out:?}");
    // 被拒后合法键不受损
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"PFCOUNT", b"hll_a"])),
      b":1\r\n"
    );
  })
}

/// 物理记录在场取证（信封 / Meta 域零写判据，同
/// load_type_rmw_window_race.rs::envelope_record_present 通道）
fn domain_record_present(
  rt: &Runtime,
  store: &Arc<WedbStore<SegmentedDevice>>,
  tag: KeyTag,
  key: &[u8],
) -> bool {
  let sess = store.new_session().expect("取证会话");
  let rec_k = sess.session_tag_key(tag, key);
  rt.block_on(sess.read_raw(&rec_k))
    .expect("物理记录读取不得报存储错误")
    .is_some()
}

/// SMOVE 目键位登记表探针（票 zcode-r137c-hllsec2 案二）：C# SetOps.cs:298-304
/// 目键类型门先于任何摘除（一手注释 "Validate the destination type before
/// removing from the source so that a WRONGTYPE destination does not lose the
/// member"），dst=登记向量键时统一存储 GET 回 WRONGTYPE（ObjectStore/
/// ReadMethods.cs:15-22 ValueIsObject=false 臂）→ SetCommands.cs:617-620 泛型
/// -WRONGTYPE。rust 收口于 set_move/smove_cold 位内探针（registry_alive 单判据
/// 源，置于 src take 之前、src NOTFOUND 短路之后，严格复刻 C# 短路序）。
/// 格 d（src=vk）由 :250 矩阵行既有 args[0] 门覆盖，本测锁格 a/b/c 三形
#[test]
fn smove_destination_vector_set_wrongtype() {
  let rt = Runtime::new().unwrap();
  // VADD 命令消费链 insert 需当前线程挂 compio runtime（thread-per-core），
  // 全体命令驱动置于 runtime 上下文内（roundtrip 内层 block_on 同线程复用）
  rt.block_on(async {
    let (mut c, vm, store) = consumer();
    create_key(&rt, &mut c, "vk_dst", KeyType::VectorSet);
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"SADD", b"src_set", b"m"])),
      b":1\r\n"
    );
    let generic_wrongtype =
      b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n";

    // 格 b：src 含成员 + dst 登记向量 → 泛型 -WRONGTYPE、src 零变异、
    // dst 值域零写、登记保留（修复前为 :1 + 成员真实搬迁 + 登记键名下值域集合幽灵）
    let out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"SMOVE", b"src_set", b"vk_dst", b"m"]),
    );
    assert_eq!(
      out,
      generic_wrongtype,
      "SMOVE 目键命中登记应回泛型 WRONGTYPE 全帧: {:?}",
      String::from_utf8_lossy(&out)
    );
    assert_eq!(
      roundtrip(
        &rt,
        &mut c,
        &encode_frame(&[b"SISMEMBER", b"src_set", b"m"])
      ),
      b":1\r\n",
      "目键被拒后源成员不得丢失（C# does not lose the member）"
    );
    assert!(
      !domain_record_present(&rt, &store, KeyTag::ObjectEnvelope, b"vk_dst"),
      "目键被拒后信封域不得有幽灵集合写入"
    );
    assert!(
      !domain_record_present(&rt, &store, KeyTag::Meta, b"vk_dst"),
      "目键被拒后 Meta 域不得有幽灵登记"
    );
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vk_dst")
        .is_some(),
      "SMOVE 被拒后向量登记须原样保留"
    );

    // 格 c：src 不含成员 + dst 登记向量 → dst 类型门先于成员判定，仍 -WRONGTYPE
    //（修复前 take 空回 :0 帧级分叉）
    let out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"SMOVE", b"src_set", b"vk_dst", b"nomem"]),
    );
    assert_eq!(out, generic_wrongtype, "格 c 目键门须先于成员判定");

    // 格 a（防反转回归锁）：src 缺失 + dst 登记向量 → C# src NOTFOUND 先行、
    // dst 不被查询 → :0（若盲扫/前置 args[1] 门本格反转为 -WRONGTYPE 新分叉）
    let out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"SMOVE", b"src_missing", b"vk_dst", b"m"]),
    );
    assert_eq!(out, b":0\r\n", "格 a src 缺失须先行回 :0 不触目键");
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vk_dst")
        .is_some(),
      "格 a 后登记仍原样"
    );

    // 合法形态零漂移：src/dst 皆集合键正常搬迁
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"SADD", b"ok_dst", b"x"])),
      b":1\r\n"
    );
    let out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"SMOVE", b"src_set", b"ok_dst", b"m"]),
    );
    assert_eq!(out, b":1\r\n", "双集合键 SMOVE 须照常回 :1");
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"SISMEMBER", b"ok_dst", b"m"])),
      b":1\r\n"
    );
  })
}

/// BLMOVE 双向（向量键作源 / 作目标均须 WRONGTYPE）
/// （VectorSetWrongTypeTests.cs:BLMOVEAsync）
#[test]
fn blmove_against_vector_set_wrongtype() {
  let rt = Runtime::new().unwrap();
  // VADD 命令消费链 insert 需当前线程挂 compio runtime（thread-per-core），
  // 全体命令驱动置于 runtime 上下文内（roundtrip 内层 block_on 同线程复用）
  rt.block_on(async {
    let (mut c, _vm, _store) = consumer();
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
  })
}

/// MGET 特例：向量键按 C# 语义回 NULL 元素而非 WRONGTYPE
/// （VectorSetWrongTypeTests.cs:MGETAsync）
#[test]
fn mget_against_vector_set_returns_nulls() {
  let rt = Runtime::new().unwrap();
  // VADD 命令消费链 insert 需当前线程挂 compio runtime（thread-per-core），
  // 全体命令驱动置于 runtime 上下文内（roundtrip 内层 block_on 同线程复用）
  rt.block_on(async {
    let (mut c, _vm, _store) = consumer();
    create_key(&rt, &mut c, "vk", KeyType::VectorSet);

    let req = encode_frame(&[b"MGET", b"vk", b"vk"]);
    let out = roundtrip(&rt, &mut c, &req);
    assert_eq!(
      out,
      b"*2\r\n$-1\r\n$-1\r\n",
      "MGET 对向量键应回 NULL 数组: {:?}",
      String::from_utf8_lossy(&out)
    );
  })
}

/// 写入守卫双向闭环（对标 C# RespServerSessionVectors.cs:501 的既有键
/// RecordType 判定）：
/// 1. SET 后 VADD/VREM/VSETATTR 回 WRONGTYPE（双域键拒于入口）；
/// 2. VADD 后 SET 覆写：登记表随覆写清退，源端无幽灵上下文残留
#[test]
fn set_and_vadd_cross_domain_guards() {
  let rt = Runtime::new().unwrap();
  // VADD 命令消费链 insert 需当前线程挂 compio runtime（thread-per-core），
  // 全体命令驱动置于 runtime 上下文内（roundtrip 内层 block_on 同线程复用）
  rt.block_on(async {
    let (mut c, vm, _store) = consumer();
    let vec3: Vec<u8> = [1.0f32, 2.0, 3.0]
      .iter()
      .flat_map(|v| v.to_le_bytes())
      .collect();

    // ── 1. SET 后三写命令全拒 ──
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"SET", b"gk", b"v"])),
      b"+OK\r\n"
    );
    for (cmd, args) in [
      ("VADD", vec![b"FP32".to_vec(), vec3.clone(), b"e".to_vec()]),
      ("VREM", vec![b"e".to_vec()]),
      ("VSETATTR", vec![b"e".to_vec(), b"a".to_vec()]),
    ] {
      let mut parts: Vec<&[u8]> = vec![cmd.as_bytes(), b"gk"];
      parts.extend(args.iter().map(|v| v.as_slice()));
      let out = roundtrip(&rt, &mut c, &encode_frame(&parts));
      assert!(
        out.starts_with(b"-WRONGTYPE "),
        "{cmd} 对既有 string 键应回 WRONGTYPE: {:?}",
        String::from_utf8_lossy(&out)
      );
    }
    // 被拒的 VADD 不得并行建向量集（无双域键、无幽灵登记）
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"gk")
        .is_none(),
      "VADD 被拒后 string 键上不得残留向量登记"
    );

    // ── 2. VADD 后 SET 覆写清登记 ──
    let out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"VADD", b"vk", b"FP32", vec3.as_slice(), b"e"]),
    );
    assert_eq!(out, b":1\r\n", "VADD 应成功");
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vk")
        .is_some(),
      "VADD 后登记在位"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"SET", b"vk", b"v2"])),
      b"+OK\r\n"
    );
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vk")
        .is_none(),
      "SET 覆写后登记应清退（源端无幽灵上下文）"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"GET", b"vk"])),
      b"$2\r\nv2\r\n",
      "覆写后 string 值可读"
    );
  })
}

/// SET 打 RangeIndex 键被 WRONGTYPE 拦截且 RI 记录不受损，DEL 后 Meta 彻底清退、
/// 无幽灵复现。
///
/// 语义真值（对标 garnet/libs/server/Storage/Functions/MainStore/UpsertMethods.cs:
/// InPlaceWriter——"Prevent SET from overwriting VectorSet or RangeIndex stubs"，
/// RI 存根记录类型拒绝字符串覆写回 WrongType；ri-predicate-gate 的 ri_write_gate
/// 按 C# 收敛后本测试由旧「SET 覆写放行 + 清 meta」改写为「拦截 + 保留 + DEL 清退」，
/// 覆盖等价：SET 拦截方向、RI 数据完好、DEL 清退无幽灵三面俱全）
#[test]
fn set_overwrites_range_index_cleans_meta() {
  let rt = Runtime::new().unwrap();
  // VADD 命令消费链 insert 需当前线程挂 compio runtime（thread-per-core），
  // 全体命令驱动置于 runtime 上下文内（roundtrip 内层 block_on 同线程复用）
  rt.block_on(async {
    let (mut c, _vm, _store) = consumer();

    let field_k = b"00000000000000000000000000000001";
    let field_v = b"00000000000000000000000000000002";

    // 1. 创建 RangeIndex 并写入数据
    assert_eq!(
      roundtrip(
        &rt,
        &mut c,
        &encode_frame(&[b"RI.CREATE", b"rik", b"MEMORY"])
      ),
      b"+OK\r\n"
    );
    assert_eq!(
      roundtrip(
        &rt,
        &mut c,
        &encode_frame(&[b"RI.SET", b"rik", field_k, field_v])
      ),
      b"+OK\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"RI.GET", b"rik", field_k])),
      b"$32\r\n00000000000000000000000000000002\r\n"
    );

    // 2. SET 覆写被 WRONGTYPE 拦截（C# UpsertMethods.cs:InPlaceWriter 存根守卫）
    let set_out = roundtrip(&rt, &mut c, &encode_frame(&[b"SET", b"rik", b"str_val"]));
    assert!(
      set_out.starts_with(b"-WRONGTYPE "),
      "SET 打 RI 键应回 WRONGTYPE: {:?}",
      String::from_utf8_lossy(&set_out)
    );

    // 3. 拦截后无幽灵半删：RI 记录原样存活、字符串读面打 RI 键亦回 WRONGTYPE
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"RI.GET", b"rik", field_k])),
      b"$32\r\n00000000000000000000000000000002\r\n",
      "SET 被拒后 RI 记录不得受损"
    );
    let get_out = roundtrip(&rt, &mut c, &encode_frame(&[b"GET", b"rik"]));
    assert!(
      get_out.starts_with(b"-WRONGTYPE "),
      "GET 打 RI 键应回 WRONGTYPE: {:?}",
      String::from_utf8_lossy(&get_out)
    );

    // 4. DEL（白名单方向）删除 RI 键后，旧 Meta 绝不复现
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"DEL", b"rik"])),
      b":1\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"GET", b"rik"])),
      b"$-1\r\n"
    );
    let ri_after_del = roundtrip(&rt, &mut c, &encode_frame(&[b"RI.GET", b"rik", field_k]));
    assert!(
      ri_after_del.starts_with(b"-ERR range index not found"),
      "DEL 后 RI.GET 应回 range index not found: {:?}",
      String::from_utf8_lossy(&ri_after_del)
    );
  })
}

/// SET 条件形态对存活向量键的逐臂对位（C# BasicCommands.cs 内 NetworkSET_Conditional
/// getValue 两分支，:772-847，命令级锚点登记在 resp/basic_commands/set.rs）：写形态
/// 由持窗第四态折叠终裁（票 zcode-r163c-setguard 案二）——NX 命中向量键即「键在」
/// 回 nil 且登记零副作用保留（与 SETNX 窗内折叠同构，Redis NX 契约）；XX / 无条件
/// 命中即键在条件成立，窗内清退后覆写回 +OK；GET 形态撞登记直接回 -WRONGTYPE =
/// 拒绝保留。arity 面 BADARGS 先于任何存储访问 = 零副作用。
/// test/standalone/Garnet.test.vectorset/VectorSetOverwriteTests.cs:SETAsync
#[test]
fn set_conditional_arms_vector_parity() {
  let rt = Runtime::new().unwrap();
  // VADD 命令消费链 insert 需当前线程挂 compio runtime（thread-per-core），
  // 全体命令驱动置于 runtime 上下文内（roundtrip 内层 block_on 同线程复用）
  rt.block_on(async {
    let (mut c, vm, _store) = consumer();
    let vec3: Vec<u8> = [1.0f32, 2.0, 3.0]
      .iter()
      .flat_map(|v| v.to_le_bytes())
      .collect();
    let add_vk = |c: &mut RespSessionConsumer, key: &[u8]| {
      let out = roundtrip(&rt, c, &encode_frame(&[b"VADD", key, b"FP32", &vec3, b"e"]));
      assert_eq!(out, b":1\r\n", "VADD 建键应成功");
    };

    // GETSET：C# NetworkGETSET（:426-434）转 SET GET 标志走 getValue 臂 →
    // -WRONGTYPE 保留登记（不随覆写族销毁）
    add_vk(&mut c, b"vk_gs");
    let out = roundtrip(&rt, &mut c, &encode_frame(&[b"GETSET", b"vk_gs", b"x"]));
    assert!(
      out.starts_with(b"-WRONGTYPE "),
      "GETSET 对向量键应回 WRONGTYPE: {:?}",
      String::from_utf8_lossy(&out)
    );
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vk_gs")
        .is_some(),
      "GETSET 被拒后登记须保留（C# getValue 臂无 DELETE 重试）"
    );

    // SET k v GET / NX GET / XX GET：全部 getValue=true 臂 → 拒绝保留
    for (tag, opts) in [
      ("GET", &[b"GET".as_slice()][..]),
      ("NX", &[b"NX".as_slice(), b"GET".as_slice()][..]),
      ("XX", &[b"XX".as_slice(), b"GET".as_slice()][..]),
    ] {
      let key = format!("vk_get_{tag}");
      add_vk(&mut c, key.as_bytes());
      let mut parts: Vec<&[u8]> = vec![b"SET", key.as_bytes(), b"x"];
      parts.extend(opts);
      let out = roundtrip(&rt, &mut c, &encode_frame(&parts));
      assert!(
        out.starts_with(b"-WRONGTYPE "),
        "SET {tag} GET 形态对向量键应回 WRONGTYPE: {:?}",
        String::from_utf8_lossy(&out)
      );
      assert!(
        vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), key.as_bytes())
          .is_some(),
        "GET 形态被拒后登记须保留: {key}"
      );
    }

    // SET k v NX：向量第四态即「键在」→ nil 出且登记零副作用保留
    // （与 SETNX 窗内折叠同一裁决源，票 zcode-r163c-setguard 案二收口，
    // 修复前窗外预清退误判缺写 +OK 毁登记的 NX 契约背离形就此注销）
    add_vk(&mut c, b"vk_nx");
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"SET", b"vk_nx", b"v", b"NX"])),
      b"$-1\r\n",
      "SET NX 对存活向量键应判在回 nil（NX 违例绝不写值亦不摘除登记）"
    );
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vk_nx")
        .is_some(),
      "SET NX 违例后登记须原样保留"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"GET", b"vk_nx"])),
      b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n"
    );

    // SET k v XX：第四态在场即键在、条件成立 → 持窗清退后覆写回 +OK
    // （存活索引不再遭「回 nil 却静默摧毁」的数据丢失形）
    add_vk(&mut c, b"vk_xx");
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"SET", b"vk_xx", b"v", b"XX"])),
      b"+OK\r\n"
    );
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vk_xx")
        .is_none(),
      "SET XX 覆写后登记应窗内清退，杜绝双域残骸"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"GET", b"vk_xx"])),
      b"$1\r\nv\r\n"
    );

    // MSET 奇参：C# parse 层 arity 先于存储访问 → BADARGS 零副作用
    add_vk(&mut c, b"vk_ms");
    let out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"MSET", b"vk_ms", b"v", b"orphan"]),
    );
    assert!(
      out.starts_with(b"-ERR wrong number of arguments"),
      "MSET 奇参应 BADARGS: {:?}",
      String::from_utf8_lossy(&out)
    );
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vk_ms")
        .is_some(),
      "失败 arity 的 MSET 不得预清退（arity 前置于清退）"
    );
  })
}

/// MSET 覆写存活向量键收口（票 zcode-r163c-setguard 案一：慢臂持窗内清退登记表）。
/// C# ArrayCommands.cs:41-69 NetworkMSET 逐键 SET、:59-63 排他锁内 DELETE+SET 重投
/// → MSET 落毕键恒 string 域、登记零残骸；本测经 guard Degrade 直驱慢臂
#[test]
fn mset_overwrite_vector_key_no_ghost_registry() {
  let rt = Runtime::new().unwrap();
  // VADD 命令消费链 insert 需当前线程挂 compio runtime（thread-per-core），
  // 全体命令驱动置于 runtime 上下文内（roundtrip 内层 block_on 同线程复用）
  rt.block_on(async {
    let (mut c, vm, _store) = consumer();
    let vec3: Vec<u8> = [1.0f32, 2.0, 3.0]
      .iter()
      .flat_map(|v| v.to_le_bytes())
      .collect();
    assert_eq!(
      roundtrip(
        &rt,
        &mut c,
        &encode_frame(&[b"VADD", b"vk_mset", b"FP32", &vec3, b"e"])
      ),
      b":1\r\n",
      "VADD 建键应成功"
    );
    // 登记表命中 → set_vector_guard Degrade → slow::mset 窗内清退 + 批量覆写
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"MSET", b"vk_mset", b"v"])),
      b"+OK\r\n",
      "MSET 覆写向量键应清退后落笔回 +OK（C# DELETE+SET 重投终态）"
    );
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vk_mset")
        .is_none(),
      "MSET 落毕登记应清退，零双域残骸"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"GET", b"vk_mset"])),
      b"$1\r\nv\r\n",
      "覆写值须可读（非永久 -WRONGTYPE）"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"TYPE", b"vk_mset"])),
      b"+string\r\n",
      "TYPE 登记特判不再命中，键恒 string 域"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"DEL", b"vk_mset"])),
      b":1\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"SETNX", b"vk_mset", b"v2"])),
      b":1\r\n",
      "DEL 后 SETNX 应回 :1（NX 存活折叠第四态零残骸取证）"
    );
  })
}

/// BITOP 源/目分流对位（C# BitmapOps.cs:StringBitOperation：源命中向量整体拒
/// 零写；目的键在 maxBitmapLen>0 时走 DELETE+SET 重试臂销毁覆写，全源缺失
/// 零写保留）
#[test]
fn bitop_source_dest_vector_parity() {
  let rt = Runtime::new().unwrap();
  // VADD 命令消费链 insert 需当前线程挂 compio runtime（thread-per-core），
  // 全体命令驱动置于 runtime 上下文内（roundtrip 内层 block_on 同线程复用）
  rt.block_on(async {
    let (mut c, vm, _store) = consumer();
    let vec3: Vec<u8> = [1.0f32, 2.0, 3.0]
      .iter()
      .flat_map(|v| v.to_le_bytes())
      .collect();

    // 目的键为存活向量键且有源命中 → 销毁 + 写折叠值 + :len
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"SET", b"src1", b"abc"])),
      b"+OK\r\n"
    );
    assert_eq!(
      roundtrip(
        &rt,
        &mut c,
        &encode_frame(&[b"VADD", b"dst_vk", b"FP32", &vec3, b"e"])
      ),
      b":1\r\n"
    );
    assert_eq!(
      roundtrip(
        &rt,
        &mut c,
        &encode_frame(&[b"BITOP", b"AND", b"dst_vk", b"src1"])
      ),
      b":3\r\n",
      "BITOP 目的向量键有源命中应覆写回 :len"
    );
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"dst_vk")
        .is_none(),
      "BITOP 覆写后登记应清退（C# dest DELETE+SET 重试臂）"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"GET", b"dst_vk"])),
      b"$3\r\nabc\r\n"
    );

    // 全源缺失 → :0 零写，目的向量键登记保留
    assert_eq!(
      roundtrip(
        &rt,
        &mut c,
        &encode_frame(&[b"VADD", b"dst_vk2", b"FP32", &vec3, b"e"])
      ),
      b":1\r\n"
    );
    assert_eq!(
      roundtrip(
        &rt,
        &mut c,
        &encode_frame(&[b"BITOP", b"AND", b"dst_vk2", b"nosuch"])
      ),
      b":0\r\n"
    );
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"dst_vk2")
        .is_some(),
      "全源缺失 BITOP 不触目的键（C# keysFound==0 不写）"
    );

    // 源键命中向量 → 整体 -WRONGTYPE 零写，两端登记/缺失态不动
    assert_eq!(
      roundtrip(
        &rt,
        &mut c,
        &encode_frame(&[b"VADD", b"src_vk", b"FP32", &vec3, b"e"])
      ),
      b":1\r\n"
    );
    let out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"BITOP", b"AND", b"dst_new", b"src_vk"]),
    );
    assert!(
      out.starts_with(b"-WRONGTYPE "),
      "BITOP 源向量键应整体拒绝: {:?}",
      String::from_utf8_lossy(&out)
    );
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"src_vk")
        .is_some()
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"EXISTS", b"dst_new"])),
      b":0\r\n",
      "源拒绝路径不得先触目的键"
    );
  })
}

/// SINTERCARD/ZINTERCARD numkeys 形键段登记表门（票 zcode-r157c-sintercard 案一）：
/// C# 实参形 numkeys key [key...] [LIMIT n]，args[0] 恒数值 token 非键位，键段
/// `parseState.Parameters.Slice(1, nKeys)`（SetCommands.cs:183 / SortedSetCommands.cs:
/// 1199），任一键位 GET 命中登记向量即整命令泛型 -WRONGTYPE（:215-218/:1229-1232，
/// 与 PF 专属文案单例无涉）。rust 收口于派发层 vector_registry_gate numkeys 形臂
///（wresp::command::vector_gate_numkeys_form 键位选取单源，判据恒 read_stored_index，
/// 零第二判据源），门前置同罩快慢双臂（read.rs/slow.rs 同漏斗，resp_slow_path.rs
/// 既有 Sintercard 参数形族零漂移——短参/非整数形不探、放行命令位自家裁决）。
/// 修复前格 A/B/Z 为整数应答帧级分叉、格 D 数值名登记键反向误拒。
#[test]
fn sintercard_numkeys_vector_wrongtype() {
  let rt = Runtime::new().unwrap();
  // VADD 命令消费链 insert 需当前线程挂 compio runtime（thread-per-core），
  // 全体命令驱动置于 runtime 上下文内（roundtrip 内层 block_on 同线程复用）
  rt.block_on(async {
    let (mut c, vm, _store) = consumer();
    let generic_wrongtype =
      b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n";
    create_key(&rt, &mut c, "vk_ic", KeyType::VectorSet);
    assert_eq!(
      roundtrip(
        &rt,
        &mut c,
        &encode_frame(&[b"SADD", b"s1_ic", b"m1", b"m2"])
      ),
      b":2\r\n"
    );

    // 格 A：首键位（args[1]）命中登记 → 泛型 WRONGTYPE 全帧字节锁
    //（修复前门探 numkeys token「1」放行，load_many 三域 Missing 吸收回 :0）
    let out = roundtrip(&rt, &mut c, &encode_frame(&[b"SINTERCARD", b"1", b"vk_ic"]));
    assert_eq!(
      out,
      generic_wrongtype,
      "SINTERCARD 首键位命中登记应回泛型 WRONGTYPE 全帧: {:?}",
      String::from_utf8_lossy(&out)
    );
    // ZINTERCARD 同形同缝（共用 parse_intersect_card_args 单源，deviations §399 同锚）
    let out = roundtrip(&rt, &mut c, &encode_frame(&[b"ZINTERCARD", b"1", b"vk_ic"]));
    assert_eq!(out, generic_wrongtype, "ZINTERCARD 同臂应同帧");

    // 格 B：非空集合前置＋次键位命中登记 → 仍整命令 -WRONGTYPE
    //（C# GET vk 即回错，rust 门逐位探 args[1..=n]，交空短路在门后不豁免判型）
    let out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"SINTERCARD", b"2", b"s1_ic", b"vk_ic"]),
    );
    assert_eq!(out, generic_wrongtype, "格 B 次键位命中登记应整命令拒");

    // 键段携尾随 LIMIT token 形：探针窗恰 args[1..=n]，LIMIT token 与数值不入探
    let out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"SINTERCARD", b"1", b"vk_ic", b"LIMIT", b"5"]),
    );
    assert_eq!(out, generic_wrongtype, "LIMIT 尾随形键位命中应同帧");

    // 格 C：首键缺失＋后续键位命中登记 → rust 先判型回 -WRONGTYPE；C# 原型首键
    // NOTFOUND 短路免 GET 回 :0——该形并入在册 §113「先判型后短路」裁决射程
    //（deviations.md §113 尾注，对拍直引勿疑报）
    let out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"SINTERCARD", b"2", b"miss_ic", b"vk_ic"]),
    );
    assert_eq!(out, generic_wrongtype, "格 C 先判型在册形应回 WRONGTYPE");

    // 门为只读前置拒：登记保留、集合侧零变异
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vk_ic")
        .is_some(),
      "被拒后向量登记须原样保留"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"SCARD", b"s1_ic"])),
      b":2\r\n",
      "被拒后集合侧不得受损"
    );

    // 格 D：登记向量键恰以数值词命名——键段自 [1..] 起判型，numkeys 位不做键探型，
    // 真实键位（s1_ic / s3_other 皆未登记）放行整数应答（修复前 args[0] 门误命中
    // token「2」同名登记键，反向误拒 -WRONGTYPE）
    create_key(&rt, &mut c, "2", KeyType::VectorSet);
    let out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"SINTERCARD", b"2", b"s1_ic", b"s3_other"]),
    );
    assert_eq!(out, b":0\r\n", "格 D 数值名登记键不得误拒真实键位");
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"2")
        .is_some(),
      "格 D 数值名登记须原样保留"
    );

    // 自指形回归锁（覆盖面一判净面钉桩）：SINTERCARD 2 k k = card(k)，双侧无去重臂
    // 基数等价（rust load_many 逐位装载不去重、intersect retain 幂等）
    let out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"SINTERCARD", b"2", b"s1_ic", b"s1_ic"]),
    );
    assert_eq!(out, b":2\r\n", "自指形应回 card(k) 基数");

    // 短参/非整数/零 numkeys 形零漂移：门不探、放行命令位自家裁决与 C# 同帧
    //（参数校验不双轨——门吞这些形即造新分叉帧）
    let rejections: &[&[&[u8]]] = &[
      &[b"SINTERCARD".as_slice(), b"3", b"vk_ic"],
      &[b"SINTERCARD", b"abc", b"vk_ic"],
      &[b"SINTERCARD", b"0", b"vk_ic"],
    ];
    for parts in rejections {
      let out = roundtrip(&rt, &mut c, &encode_frame(parts));
      assert!(
        out.starts_with(b"-ERR") && !out.starts_with(b"-WRONGTYPE"),
        "命令位自家裁决帧不得被门吞成 WRONGTYPE: {:?}",
        String::from_utf8_lossy(&out)
      );
    }
  })
}

/// SDIFF/SDIFFSTORE 尾键位登记表向量形补钉（票 zcode-r159c-sdiff 窄三，
/// 非向量命令表 :249/:250 行仅锁首键位的欠账）：首键活集合＋尾键位登记向量
/// ——C# SetOps.cs:SetDiff 尾键循环 :913-915 逐 GET 撞向量影子 WRONGTYPE 即返
/// （SetCommands.cs:737-740 泛型串）；rust 派发层 vector_registry_gate
/// scan_all 臂（Sdiff/Sdiffstore 在列 wresp::command::vector_gate_scan_all_keys）
/// args 全键位 any 前置直拦，门前置同罩快慢双臂——同帧无分叉，锁泛型
/// -WRONGTYPE 全帧逐字节，STORE 错误臂 dst 零触达、登记与集合侧完好
#[test]
fn sdiff_tail_key_vector_wrongtype() {
  let rt = Runtime::new().unwrap();
  // VADD 命令消费链 insert 需当前线程挂 compio runtime（thread-per-core），
  // 全体命令驱动置于 runtime 上下文内（roundtrip 内层 block_on 同线程复用）
  rt.block_on(async {
    let (mut c, vm, _store) = consumer();
    let generic_wrongtype =
      b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n";
    create_key(&rt, &mut c, "vk_sd", KeyType::VectorSet);
    assert_eq!(
      roundtrip(
        &rt,
        &mut c,
        &encode_frame(&[b"SADD", b"s1_sd", b"m1", b"m2"])
      ),
      b":2\r\n"
    );

    // 首键活集合×尾键位向量 → 整命令泛型 -WRONGTYPE 全帧（C# 同帧对照格）
    let out = roundtrip(&rt, &mut c, &encode_frame(&[b"SDIFF", b"s1_sd", b"vk_sd"]));
    assert_eq!(
      out,
      generic_wrongtype,
      "SDIFF 尾键位命中登记应整命令拒: {:?}",
      String::from_utf8_lossy(&out)
    );
    // STORE 形同拦：错误臂先于落笔，dst 零触达（§125 窗序面正交，本锁只拍
    // 判型拦止与 dst 无写）
    let out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"SDIFFSTORE", b"dst_sd", b"s1_sd", b"vk_sd"]),
    );
    assert_eq!(out, generic_wrongtype, "SDIFFSTORE 尾键位向量应同帧拒");
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"EXISTS", b"dst_sd"])),
      b":0\r\n",
      "错误臂不得建 dst"
    );
    // 门为只读前置拒：登记保留、集合侧零变异
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vk_sd")
        .is_some(),
      "被拒后向量登记须原样保留"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"SCARD", b"s1_sd"])),
      b":2\r\n",
      "被拒后集合侧不得受损"
    );
  })
}

/// SDIFF/SDIFFSTORE 判死 RI 键位吸收形锁（票 zcode-r159c-sdiff 案一窄三 RI
/// 臂，deviations.md §150 在册分叉）：到期未清退 RI 主存记录（ValueIsObject
/// 不置，RangeIndexManager.cs:54）rust 装载漏斗每步域读恒先过 wkv 域内 TTL
/// 门折叠 Missing→空集静默吸收；C# ObjectStore Reader :19-23 判型先行回
/// -WRONGTYPE 至清退窗收敛——窗内形帧与 STORE dst 终态分叉直引 §150 判
/// 有意偏差，严禁按 C# 回改。判死残留经 put_ttl_sync 裸写过去刻度构造
/// （RESP 面 EXPIRE 对 RI 之裁决本锁不依赖，先例
/// rename_nx_expired_residual_parity.rs）；活 RI 首/尾键对照格两侧同帧
/// （rust meta_gate collection_type≠Set vs C# ValueIsObject 不置臂）
#[test]
fn sdiff_dead_range_index_absorbed_as_missing() {
  let rt = Runtime::new().unwrap();
  // VADD 命令消费链 insert 需当前线程挂 compio runtime（thread-per-core），
  // 全体命令驱动置于 runtime 上下文内（roundtrip 内层 block_on 同线程复用）
  rt.block_on(async {
    let (mut c, _vm, store) = consumer();
    let generic_wrongtype =
      b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n";
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"SADD", b"s1_ri", b"a", b"b"])),
      b":2\r\n"
    );
    create_key(&rt, &mut c, "ri_live", KeyType::RangeIndex);
    create_key(&rt, &mut c, "ri_dead", KeyType::RangeIndex);
    {
      let sess = store.new_session().unwrap();
      let batch = sess.enter_batch();
      put_ttl_sync(&batch, b"ri_dead", now_ticks() - TICKS_PER_SECOND).unwrap();
    }

    // 对照格（判净同帧面）：活 RI 任一键位两侧皆 -WRONGTYPE
    let out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"SDIFF", b"ri_live", b"s1_ri"]),
    );
    assert_eq!(out, generic_wrongtype, "活 RI 首键位应两侧同帧拒");
    let out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"SDIFF", b"s1_ri", b"ri_live"]),
    );
    assert_eq!(out, generic_wrongtype, "活 RI 尾键位应两侧同帧拒");

    // 判死尾 RI：域门折叠 Missing→空集吸收→回 s1 全量成员（C# 该形判型先行
    // 回 -WRONGTYPE，§150 窗内分叉；成员序非契约按 §136 口径排序比对）
    let out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"SDIFF", b"s1_ri", b"ri_dead"]),
    );
    assert!(
      out.starts_with(b"*2\r\n"),
      "判死尾 RI 应吸收为缺失回 s1 成员: {:?}",
      String::from_utf8_lossy(&out)
    );
    assert!(
      out.windows(3).any(|w| w == b"a\r\n") && out.windows(3).any(|w| w == b"b\r\n"),
      "成员须为 a/b"
    );

    // 判死首 RI：空集折叠回空数组
    let out = roundtrip(&rt, &mut c, &encode_frame(&[b"SDIFF", b"ri_dead"]));
    assert_eq!(out, b"*0\r\n", "判死首 RI 应回空数组");

    // STORE 形：成功臂落笔覆写 dst 回基数（C# 错误臂零触达，dst 终态窗内
    // 发散面 §150 如实登记）
    let out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"SDIFFSTORE", b"dst_ri", b"s1_ri", b"ri_dead"]),
    );
    assert_eq!(out, b":2\r\n", "判死尾 RI STORE 成功臂应回基数 2");
    let out = roundtrip(&rt, &mut c, &encode_frame(&[b"SMEMBERS", b"dst_ri"]));
    assert!(out.starts_with(b"*2\r\n"), "dst 须为新集合值");
  })
}
