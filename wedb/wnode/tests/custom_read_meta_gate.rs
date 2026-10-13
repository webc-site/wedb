#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 自定义对象 Read 通道探测面 Meta 分层闸回归（票
//! wnode-custom-read-probe-meta-gate-missing-wrongtype-fork，P2）
//!
//! 缺陷面：probe_custom_read_sync / probe_custom_read_async 只探 ObjectEnvelope
//! 与 String 两域、缺装载族（obj_load_custom_sync / obj_load_custom）第一步
//! Meta 分层闸。分层存活键（数据迁 Meta 域 + wbftree、信封物理删除）两域皆缺，
//! Read 通道按缺失应答（R.GETBIT :0、R.BITCOUNT :0、R.BITPOS :-1、JSON.GET
//! nil），同键 RMW 通道与 C#（CustomObjectBase.Operate 键存活即判型，
//! CustomRespCommands.cs Read 臂 WRONGTYPE 出错误帧）恒 -WRONGTYPE——
//! review.md 4.2 多路径行为同构违例。
//!
//! 修复形态：Read 探测面在信封探测前补 Meta 闸步，复用既有单源
//! step_load_meta + meta_probe + probe_tag_sync/async（零新判据）；闸内出帧
//! 仍写丢弃型 sink（探测面零出帧纪律），四条 Read 消费臂（单键出帧臂、
//! 多键逐元素 nil 口径）零改动。
//!
//! 测试全真协议帧真存储，无 mock：分层键灌水采 get_slow_arm_object_wrongtype.rs
//! 先例形（HSET/SADD/ZADD 短条目越 wcol::TIERED_PROMOTE_THRESHOLD 计数阈就地
//! 升阶；600B 长载荷字节阈形仅 Hash 侧可升阶，set/zset 成员受 wbftree 单记录
//! 键长门拒升），升阶事实经 load_collection_stub 确证；过期未清退分层键按缺失
//! 应答为 deviations.md §150 判死吸收形锁面，严禁按 C# 回改。

use std::sync::Arc;

use compio::runtime::Runtime;
use itoa::Buffer as ItoaBuffer;
use wbase::time::now_ticks;
use wnode::{RespSessionConsumer, storage::session::common::ttl_sync::put_ttl_sync};
use wnode_test::{TestStore, metrics_env, roundtrip};

/// WRONGTYPE 错误帧全帧形（read 单键臂 write_error_raw(RESP_ERR_WRONG_TYPE)
/// 与 RMW 臂判定核同一字面，锁「同帧」逐字节一致）
const WRONGTYPE_FRAME: &[u8] =
  b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n";

/// 计数阈灌水形（get_slow_arm_object_wrongtype.rs seed_keys 先例同参）
const TOTAL: usize = wcol::TIERED_PROMOTE_THRESHOLD + 10;
const CHUNK: usize = 16384;

/// 灌族群形（Hash 字段+值、Set 成员、ZSet 分值+成员）
#[derive(Copy, Clone)]
enum Kind {
  Hash,
  Set,
  ZSet,
}

/// 单族分层键灌水：分块写入每片整数计数回帧当场红；终态 load_collection_stub
/// 确证 Meta 域存根在场（升阶即本锁面前提）、条目数吻合、O(1) 计数直读
fn pour(
  rt: &Runtime,
  c: &mut RespSessionConsumer,
  store: &Arc<TestStore>,
  cmd: &'static str,
  key: &'static str,
  len_cmd: &'static str,
  kind: Kind,
) {
  let mut buf = ItoaBuffer::new();
  for start in (1..=TOTAL).step_by(CHUNK) {
    let end = (start + CHUNK - 1).min(TOTAL);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity((end - start + 1) * 2 + 2);
    args.push(cmd.as_bytes().to_vec());
    args.push(key.as_bytes().to_vec());
    for i in start..=end {
      match kind {
        Kind::Hash => args.push(format!("f{i}").into_bytes()),
        Kind::ZSet => args.push(buf.format(i).as_bytes().to_vec()),
        Kind::Set => {}
      }
      // Hash 值与 Set/ZSet 成员统一短条目 `{i}`（成员域 m 前缀防撞 Set 域）
      if matches!(kind, Kind::Set | Kind::ZSet) {
        args.push(format!("m{i}").into_bytes());
      } else {
        args.push(buf.format(i).as_bytes().to_vec());
      }
    }
    let slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    let added = format!(":{}\r\n", end - start + 1);
    assert_eq!(
      roundtrip(rt, c, &slices),
      added.as_bytes(),
      "{cmd} 灌水片 {start}..={end} 应整片成功入账"
    );
  }
  // 升阶确证：Meta 域存根在场、条目数吻合
  let sess = store.new_session().unwrap();
  assert!(
    rt.block_on(sess.load_collection_stub(key.as_bytes()))
      .unwrap()
      .is_some_and(|(meta, _)| meta.size as usize == TOTAL),
    "{key} 应已升阶为分层态（Meta 域命中，本用例锁面前提）"
  );
  // 计数锚（分层态 HLEN/SCARD/ZCARD O(1) 直读 MetaValue.size）
  assert_eq!(
    roundtrip(rt, c, &[len_cmd.as_bytes(), key.as_bytes()]),
    format!(":{TOTAL}\r\n").as_bytes()
  );
}

/// 三族分层键：th（Hash）/ ts（Set）/ tz（ZSet）
fn seed_tiered(rt: &Runtime, c: &mut RespSessionConsumer, store: &Arc<TestStore>) {
  pour(rt, c, store, "HSET", "th", "HLEN", Kind::Hash);
  pour(rt, c, store, "SADD", "ts", "SCARD", Kind::Set);
  pour(rt, c, store, "ZADD", "tz", "ZCARD", Kind::ZSet);
}

/// 分层 Hash/Set/ZSet 键上 Read 臂恒 -WRONGTYPE 且与 RMW 臂同帧；
/// JSON.MGET 逐元素维持 nil；判型零写回（计数不漂移、读不建键）
#[test]
fn tiered_keys_custom_read_reply_wrongtype_same_frame_as_rmw() {
  let (rt, mut c, _api, _h, _dir, store) = metrics_env("custom-read-meta-gate-hot.db");
  seed_tiered(&rt, &mut c, &store);

  for key in [b"th".as_slice(), b"ts", b"tz"] {
    // Read 臂四探测点：修复前按缺失应答（:0 / :0 / :-1 / nil）
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"R.GETBIT", key, b"10"]),
      WRONGTYPE_FRAME,
      "分层键 {key:?} R.GETBIT 恒 -WRONGTYPE（修复前答 :0）"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"R.BITCOUNT", key]),
      WRONGTYPE_FRAME,
      "分层键 {key:?} R.BITCOUNT 恒 -WRONGTYPE（修复前答 :0）"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"R.BITPOS", key, b"1"]),
      WRONGTYPE_FRAME,
      "分层键 {key:?} R.BITPOS 恒 -WRONGTYPE（修复前答 :-1）"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"JSON.GET", key, b"$"]),
      WRONGTYPE_FRAME,
      "分层键 {key:?} JSON.GET 恒 -WRONGTYPE（修复前答 nil）"
    );
    // RMW 臂对照：既有 Meta 闸出帧与 Read 臂逐字节同帧
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"R.SETBIT", key, b"10", b"1"]),
      WRONGTYPE_FRAME,
      "分层键 {key:?} R.SETBIT（RMW 对照臂）同帧"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"JSON.SET", key, b"$", b"1"]),
      WRONGTYPE_FRAME,
      "分层键 {key:?} JSON.SET（RMW 对照臂）同帧"
    );
  }

  // 批量逐元素口径不变：分层键元素维持 nil（非错误帧入元素位）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"JSON.MGET", b"th", b"ts", b"tz", b"$.a"]),
    b"*3\r\n$-1\r\n$-1\r\n$-1\r\n",
    "JSON.MGET 分层键元素位维持 nil"
  );

  // 判型零副作用：读臂零写回零建键（三键计数不漂移）
  let total = format!(":{TOTAL}\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"HLEN", b"th"]), total.as_bytes());
  assert_eq!(roundtrip(&rt, &mut c, &[b"SCARD", b"ts"]), total.as_bytes());
  assert_eq!(roundtrip(&rt, &mut c, &[b"ZCARD", b"tz"]), total.as_bytes());
}

/// 回归不回退：字符串键 Read 臂仍按既有 String 域反探臂 -WRONGTYPE；内存态
/// 信封对象键（小 HSET 未升阶）仍按信封标签不符 -WRONGTYPE；本类型内存态
/// 信封键命中臂照常出帧（Meta 闸 Absent 放行，零行为改动）
#[test]
fn string_and_envelope_and_self_type_keys_regression() {
  let (rt, mut c, _api, _h, _dir, _store) = metrics_env("custom-read-meta-gate-regress.db");

  // 字符串键：既有 String 域反探臂（本票零改动）
  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"st", b"v"]), b"+OK\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"R.GETBIT", b"st", b"0"]),
    WRONGTYPE_FRAME
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"R.BITCOUNT", b"st"]),
    WRONGTYPE_FRAME
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"JSON.GET", b"st", b"$"]),
    WRONGTYPE_FRAME
  );

  // 内存态信封对象键（标签不符臂）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"eh", b"f", b"v"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"R.GETBIT", b"eh", b"0"]),
    WRONGTYPE_FRAME
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"JSON.GET", b"eh", b"$"]),
    WRONGTYPE_FRAME
  );

  // 本类型内存态信封键：命中喂 reader 臂零改动
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"R.SETBIT", b"rb", b"42", b"1"]),
    b":0\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"R.GETBIT", b"rb", b"42"]),
    b":1\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"R.BITCOUNT", b"rb"]), b":1\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"JSON.SET", b"js", b"$", b"{\"a\":1}"]),
    b"+OK\r\n"
  );
  let got = roundtrip(&rt, &mut c, &[b"JSON.GET", b"js", b"$.a"]);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"JSON.SET", b"jsref", b"$", b"{\"a\":1}"]),
    b"+OK\r\n"
  );
  assert_eq!(
    got,
    roundtrip(&rt, &mut c, &[b"JSON.GET", b"jsref", b"$.a"])
  );

  // 缺失键缺失形不回退：R.GETBIT :0 / R.BITCOUNT :0 / R.BITPOS :-1 / JSON.GET nil
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"R.GETBIT", b"nope", b"10"]),
    b":0\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"R.BITCOUNT", b"nope"]), b":0\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"R.BITPOS", b"nope", b"1"]),
    b":-1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"JSON.GET", b"nope", b"$"]),
    b"$-1\r\n"
  );
  // 缺失键读不建键
  assert_eq!(roundtrip(&rt, &mut c, &[b"EXISTS", b"nope"]), b":0\r\n");
}

/// 异步对位臂：分层键记录换出内存环成磁盘候选后，同步探测面 Meta 闸判降级、
/// 慢路径异步重放（probe_custom_read_async 补闸）出帧与热态逐字节同帧；
/// JSON.MGET 冷态元素位维持 nil（批量整批降级重放口径不变）
#[test]
fn cold_tiered_keys_slow_arm_replies_same_wrongtype_frame() {
  let (rt, mut c, _api, _h, _dir, store) = metrics_env("custom-read-meta-gate-cold.db");
  pour(&rt, &mut c, &store, "HSET", "th", "HLEN", Kind::Hash);

  // 热形基线
  let hot = roundtrip(&rt, &mut c, &[b"R.GETBIT", b"th", b"10"]);
  assert_eq!(hot, WRONGTYPE_FRAME);

  // 全库换出内存环：Meta 记录连带落盘为磁盘候选
  rt.block_on(store.flush_and_evict_all()).unwrap();

  // 慢臂重放（同步闸判 Degrade → probe_custom_read_async Meta 闸定音）：帧形逐字节等热形
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"R.GETBIT", b"th", b"10"]),
    hot,
    "冷分层键 R.GETBIT 慢臂须与热形同帧"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"JSON.GET", b"th", b"$"]),
    WRONGTYPE_FRAME,
    "冷分层键 JSON.GET 慢臂恒 -WRONGTYPE"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"R.BITCOUNT", b"th"]),
    WRONGTYPE_FRAME,
    "冷分层键 R.BITCOUNT 慢臂恒 -WRONGTYPE"
  );
  // 批量慢臂：分层键与缺失键元素位均维持 nil（整批降级重放口径不变）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"JSON.MGET", b"th", b"nope", b"$.a"]),
    b"*2\r\n$-1\r\n$-1\r\n",
    "冷分层键 JSON.MGET 慢臂元素位维持 nil"
  );
}

/// §150 锁面：过期未清退分层键经 wkv 域内 TTL 单点门判缺失，Read 通道仍按
/// 缺失应答（判死吸收形不回改为 WRONGTYPE，严禁按 C# Reader 门序回改）
#[test]
fn expired_tiered_key_still_answers_missing_shape() {
  let (rt, mut c, _api, _h, _dir, store) = metrics_env("custom-read-meta-gate-expired.db");
  pour(&rt, &mut c, &store, "HSET", "th", "HLEN", Kind::Hash);
  pour(&rt, &mut c, &store, "SADD", "ts", "SCARD", Kind::Set);

  // 过去刻度 TTL 直写（不经 EXPIRE 的过去即删语义）：Meta 记录在簿、TTL 门恒 Due
  let sess = store.new_session().unwrap();
  let batch = sess.enter_batch();
  put_ttl_sync(&batch, b"th", now_ticks().saturating_sub(1)).unwrap();

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"R.GETBIT", b"th", b"10"]),
    b":0\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"R.BITCOUNT", b"th"]), b":0\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"R.BITPOS", b"th", b"1"]),
    b":-1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"JSON.GET", b"th", b"$"]),
    b"$-1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"JSON.MGET", b"th", b"$.a"]),
    b"*1\r\n$-1\r\n",
    "过期未清退分层键批量元素位维持缺失形 nil"
  );
  // 未挂 TTL 的分层键判型臂不受波及（TTL 单点门按键裁决，无串扰）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"R.GETBIT", b"ts", b"10"]),
    WRONGTYPE_FRAME
  );
}
