//! COSCAN/CUSTOMOBJECTSCAN 对象域反转收口集成测试
//!（task/ing/wcol-coscan-object-domain-inversion.md，票面测试验证点五条）
//!
//! C# 原域：COSCAN 经 header.type == All 下传，仅自定义对象
//! `CustomObjectBase.Operate` 接受转对象 Scan（CustomObjectBase.cs:77-89）；
//! 内置三型严格类型检查对 All 一律 WrongType（HashObject.cs:225-231 /
//! SetObject.cs:126-135）。CUSTOMOBJECTSCAN 为其唯一解析文本名
//!（RespCommandHashLookupData.cs:264）。反转前 rust 恰好倒置（内置放行、
//! 自定义 WRONGTYPE、json 假桩吞错），本套件逐条钉住反转后口径：
//! a) roaring 键 COSCAN 全参数臂与 C# 对拍——恒 `[0, []]`
//!    （RoaringBitmapObject.cs:64-71 Scan 刻意留空：空收集 + 游标 0，忽略
//!    MATCH/COUNT/NOVALUES；doc 注释与实现矛盾以实现为准）；
//! b) hash/set/zset 键信封态与升阶态 COSCAN 均 WRONGTYPE（同步/冷双臂：
//!    内存态同步臂、flush_and_evict_all 冷化慢臂、Meta 升阶态）；
//! c) json 键 COSCAN 回错误帧而非空成功（NotImplementedException 裁量帧，
//!    文案登记 doc/zh/deviations.md §91）；
//! d) 自定义对象键 RESP 夹具矩阵（roaring/json × 裸/MATCH/COUNT/NOVALUES ×
//!    内存/冷化两态）；
//! e) HSCAN/SSCAN/ZSCAN 原生面回归——域收口仅削 COSCAN 越权面，原生扫描
//!    内存/冷化/升阶三态不受影响。

use std::sync::Arc;

use compio::runtime::Runtime;
use wcol::{SET_MEMBER_DUMMY_VALUE, types::member_ttl::encode_member};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wtest_base::open_test_store;
use wval::GarnetObjectType;

type TestStore = WedbStore<SegmentedDevice>;

/// SCAN 族应答游标段（外层 `*2` 的首元素 bulk：游标 0）
const SCAN_CURSOR_ZERO: &[u8] = b"*2\r\n$1\r\n0\r\n";
/// COSCAN 扫描帧（游标 0 + 空数组）：roaring 键全参数臂与缺键共用此帧形
const EMPTY_SCAN_FRAME: &[u8] = b"*2\r\n$1\r\n0\r\n*0\r\n";
/// WRONGTYPE 错误帧（内置三型 / 字符串键 / 升阶键统一收口）
const WRONGTYPE_FRAME: &[u8] =
  b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n";
/// JSON 域 COSCAN 裁量错误帧（NotImplementedException 默认消息，
/// doc/zh/deviations.md §91）
const NOT_IMPLEMENTED_FRAME: &[u8] = b"-ERR The method or operation is not implemented.\r\n";
/// 光标校验失败帧（C# SharedObjectCommands.ObjectScan 光标非负门）
const INVALID_CURSOR_FRAME: &[u8] = b"-ERR invalid cursor\r\n";

fn consumer_on(store: &Arc<TestStore>) -> RespSessionConsumer {
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// 单命令往返（慢路径挂起体经产线冲出口驱动闭环，store_overwrite_string_retire
/// 同款泵）
fn roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, args: &[&[u8]]) -> Vec<u8> {
  let mut req = format!("*{}\r\n", args.len()).into_bytes();
  for arg in args {
    req.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
    req.extend_from_slice(arg);
    req.extend_from_slice(b"\r\n");
  }
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(&req);
  c.return_recv_scratch(scratch);
  let mut out = Vec::new();
  let _ = c.try_consume_messages_into(&mut out);
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async { out.extend_from_slice(&slow.resolve().await) });
  }
  out
}

/// 手工升阶（entries 与 `IGarnetObject::export_entries` 同构的编码形态，
/// scan_family_dualstate_frames 同款）
fn promote(
  rt: &Runtime,
  store: &Arc<TestStore>,
  key: &[u8],
  obj_type: GarnetObjectType,
  entries: Vec<(Vec<u8>, Vec<u8>)>,
) {
  let sess = store.new_session().unwrap();
  rt.block_on(sess.promote_collection_to_bftree(key, obj_type, entries, i64::MAX, false))
    .unwrap();
  assert!(
    rt.block_on(sess.load_collection_stub(key))
      .unwrap()
      .is_some(),
    "键应处于 wbftree 分层态"
  );
}

/// 单成员集合的 SCAN 族应答帧（原生面回归共用：游标 0 + 条目数组；
/// hash 成员对 member/value，set 单 member，zset 成员对 member/score）
fn single_member_scan_frame(items: &[&[u8]]) -> Vec<u8> {
  let mut out = SCAN_CURSOR_ZERO.to_vec();
  out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
  for item in items {
    out.extend_from_slice(format!("${}\r\n", item.len()).as_bytes());
    out.extend_from_slice(item);
    out.extend_from_slice(b"\r\n");
  }
  out
}

/// a+d) roaring 键 COSCAN 全参数臂与 C# 对拍（内存态同步臂）
///
/// C# RoaringBitmapObject.Scan 刻意留空：恒空收集 + 游标 0，忽略 MATCH/COUNT/
/// NOVALUES——全臂逐字节同帧 `[0, []]`；扫描对位图零写副作用
#[test]
fn roaring_key_coscan_all_arms_csharp_parity() {
  let (_dir, store) = open_test_store("coscan-roaring-arms.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"R.SETBIT", b"rk", b"42", b"1"]),
    b":0\r\n"
  );

  let tails: &[&[&[u8]]] = &[
    &[b"0"],
    &[b"0", b"MATCH", b"bit*"],
    &[b"0", b"COUNT", b"1"],
    &[b"0", b"NOVALUES"],
    &[b"0", b"MATCH", b"bit*", b"COUNT", b"7", b"NOVALUES"],
  ];
  for tail in tails {
    let mut args: Vec<&[u8]> = vec![b"CUSTOMOBJECTSCAN", b"rk"];
    args.extend_from_slice(tail);
    assert_eq!(
      roundtrip(&rt, &mut c, &args),
      EMPTY_SCAN_FRAME,
      "roaring 键参数臂 {tail:?} 应与 C# 空扫描帧逐字节对拍"
    );
  }

  // 扫描只读：置位原样保留，无键回收副作用
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"R.GETBIT", b"rk", b"42"]),
    b":1\r\n"
  );
}

/// c+d) json 键 COSCAN 错误帧非空成功（内存态同步臂）
///
/// C# GarnetJsonObject.Scan 抛 NotImplementedException；rust 裁量错误帧收口
/// （doc/zh/deviations.md §91），合法参数臂一致拒绝；非法参数先于扫描语义走
/// 解析臂（C# ReadScanInput 先于抽象 Scan）；错误帧不删键
#[test]
fn json_key_coscan_error_frame_not_empty_success() {
  let (_dir, store) = open_test_store("coscan-json-error.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"JSON.SET", b"jk", b"$", b"{\"a\":1}"]),
    b"+OK\r\n"
  );

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"CUSTOMOBJECTSCAN", b"jk", b"0"]),
    NOT_IMPLEMENTED_FRAME
  );
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      &[
        b"CUSTOMOBJECTSCAN",
        b"jk",
        b"0",
        b"MATCH",
        b"p*",
        b"COUNT",
        b"3",
        b"NOVALUES"
      ]
    ),
    NOT_IMPLEMENTED_FRAME,
    "合法参数臂应一致落 NotImplemented 裁量帧"
  );

  // 解析臂先于扫描语义：COUNT 非整数走 ReadScanInput 错误帧（C# 同序）
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      &[b"CUSTOMOBJECTSCAN", b"jk", b"0", b"COUNT", b"xyz"]
    ),
    b"-ERR value is not an integer or out of range.\r\n"
  );

  // 错误帧不删键
  assert_eq!(roundtrip(&rt, &mut c, &[b"EXISTS", b"jk"]), b":1\r\n");
}

/// d) 缺键与字符串键收口（C# NOTFOUND → `[0, []]`；String 域 → WRONGTYPE）
#[test]
fn coscan_missing_and_string_keys() {
  let (_dir, store) = open_test_store("coscan-missing-string.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);

  // 缺键：C# ObjectScan NOTFOUND 分支 → 游标 0 + 空数组
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"CUSTOMOBJECTSCAN", b"missing", b"0"]),
    EMPTY_SCAN_FRAME
  );
  // 非法游标：光标门先于域判定
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"CUSTOMOBJECTSCAN", b"missing", b"-1"]),
    INVALID_CURSOR_FRAME
  );

  // String 域命中：COSCAN 原域不含用户字符串键
  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"sk", b"v"]), b"+OK\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"CUSTOMOBJECTSCAN", b"sk", b"0"]),
    WRONGTYPE_FRAME
  );
}

/// b 信封态 + e) 内置三型信封键 COSCAN WRONGTYPE（同步臂）与原生扫描回归
#[test]
fn builtin_envelope_keys_coscan_wrongtype_and_native_scan_intact() {
  let (_dir, store) = open_test_store("coscan-envelope-wrongtype.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"hk", b"f", b"v"]),
    b":1\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"SADD", b"sk", b"m"]), b":1\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZADD", b"zk", b"1", b"m"]),
    b":1\r\n"
  );

  // 反转收口：内置三型信封键（C# 严格类型检查对 All 挂 WrongType 旗）
  for key in [&b"hk"[..], &b"sk"[..], &b"zk"[..]] {
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"CUSTOMOBJECTSCAN", key, b"0"]),
      WRONGTYPE_FRAME,
      "信封键 {key:?} COSCAN 应 WRONGTYPE"
    );
  }

  // e) 原生面回归：域收口不削 HSCAN/SSCAN/ZSCAN（信封内存态扫描帧不变）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSCAN", b"hk", b"0"]),
    single_member_scan_frame(&[b"f", b"v"])
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SSCAN", b"sk", b"0"]),
    single_member_scan_frame(&[b"m"])
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZSCAN", b"zk", b"0"]),
    single_member_scan_frame(&[b"m", b"1"])
  );
}

/// b 冷臂 + d) flush_and_evict_all 冷化后 COSCAN（慢路径重放）全域同口径
///
/// 冷化使信封 / String / TTL 记录转磁盘候选，快臂 Deferred 降级 →
/// slow::coscan 重放；扫描、错误帧、WRONGTYPE、缺键帧与同步臂逐字节一致
#[test]
fn coscan_cold_arm_all_domains() {
  let (_dir, store) = open_test_store("coscan-cold-arm.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"R.SETBIT", b"rk", b"7", b"1"]),
    b":0\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"JSON.SET", b"jk", b"$", b"{\"a\":1}"]),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"hk", b"f", b"v"]),
    b":1\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"sk", b"v"]), b"+OK\r\n");

  // 刷盘冷化（store_overwrite_string_retire 同款触发器）
  rt.block_on(store.flush_and_evict_all()).unwrap();

  // 自定义对象冷臂：roaring 扫描帧 / json 错误帧与同步臂同帧
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"CUSTOMOBJECTSCAN", b"rk", b"0"]),
    EMPTY_SCAN_FRAME,
    "冷臂 roaring 扫描应与同步臂同帧"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"CUSTOMOBJECTSCAN", b"jk", b"0"]),
    NOT_IMPLEMENTED_FRAME,
    "冷臂 json 应与同步臂同帧"
  );
  // 内置信封 / 字符串冷臂：WRONGTYPE；缺键冷臂：[0, []]
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"CUSTOMOBJECTSCAN", b"hk", b"0"]),
    WRONGTYPE_FRAME
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"CUSTOMOBJECTSCAN", b"sk", b"0"]),
    WRONGTYPE_FRAME
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"CUSTOMOBJECTSCAN", b"coldmissing", b"0"]),
    EMPTY_SCAN_FRAME
  );

  // e) 原生面冷臂回归：HSCAN 冷化后仍正常扫描
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSCAN", b"hk", b"0"]),
    single_member_scan_frame(&[b"f", b"v"])
  );
}

/// b 升阶态 + e) Meta 域键 COSCAN WRONGTYPE（仅内置三型可升阶，新域直接
/// WRONGTYPE）与升阶原生扫描回归
#[test]
fn coscan_tiered_keys_wrongtype_and_native_scan_intact() {
  let (_dir, store) = open_test_store("coscan-tiered-wrongtype.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"hk", b"f", b"v"]),
    b":1\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"SADD", b"sk", b"m"]), b":1\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZADD", b"zk", b"1", b"m"]),
    b":1\r\n"
  );

  promote(
    &rt,
    &store,
    b"hk",
    GarnetObjectType::Hash,
    vec![(b"f".to_vec(), encode_member(b"v", None))],
  );
  promote(
    &rt,
    &store,
    b"sk",
    GarnetObjectType::Set,
    vec![(b"m".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec())],
  );
  promote(
    &rt,
    &store,
    b"zk",
    GarnetObjectType::SortedSet,
    vec![(b"m".to_vec(), encode_member(&1.0f64.to_be_bytes(), None))],
  );

  // 升阶键（Meta 域）COSCAN：删除内置放行臂后统一 WRONGTYPE，不再越权扫描
  for key in [&b"hk"[..], &b"sk"[..], &b"zk"[..]] {
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"CUSTOMOBJECTSCAN", key, b"0"]),
      WRONGTYPE_FRAME,
      "升阶键 {key:?} COSCAN 应 WRONGTYPE"
    );
  }

  // e) 原生面回归：升阶键树内扫描不受域收口影响（树内字典序恒定）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSCAN", b"hk", b"0"]),
    single_member_scan_frame(&[b"f", b"v"])
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SSCAN", b"sk", b"0"]),
    single_member_scan_frame(&[b"m"])
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZSCAN", b"zk", b"0"]),
    single_member_scan_frame(&[b"m", b"1"])
  );
}
