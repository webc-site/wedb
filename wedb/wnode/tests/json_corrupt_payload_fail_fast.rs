#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! wext_json corrupt 载荷端到端 fail-fast 回归（票 wext-json-corrupt-payload-nil-fold-rewrite-breach-fail-fast）
//!
//! 钩子级七处收口见 wext_json/tests/json_corrupt_payload_fail_fast.rs；本文件走
//! 生产全链（RESP 帧解析 → 静态清单分派 → 信封装载 → CustomObjectFns 执行体），
//! 直塞「合法 JSON 标签 + 非 JSON 字节」信封物理记录构造损坏载荷键（绕过业务写
//! 路径，同 envelope_head_corrupt_reject 种子形态），钉死三形：
//! 1. 读族（JSON.GET/TYPE/RESP/STRLEN）回「ERR JSON object decode failed」错误帧，
//!    不再折叠 nil 成功应答；
//! 2. 变异族（JSON.DEL/CLEAR/NUMINCRBY/TOGGLE）回错误帧且 HLog tail 零推进 +
//!    AOF EnvelopeUpsert（custom 对象入账通道）零追加（旧缺陷形：updater 回
//!    true → is_empty 判非空 → Save 损坏载荷原样重写回库 + 每命令重复 AOF
//!    入账）；对照真写入必须 tail 前进且入账 +1，杜绝判据空跑；
//! 3. 多键读臂 error_element_to_nil 协议整形不变：corrupt 键在 JSON.MGET 元素位
//!    回 nil（错误帧禁入元素位），健康键元素逐字节如常；
//!    同通道 roaring R.GETBIT 同形对照恒错误帧不回退。

#![cfg(all(feature = "json", feature = "roaring"))]

use std::{
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  thread::sleep,
  time::Duration,
};

use compio::runtime::Runtime;
use tempfile::TempDir;
use wdev::SegmentedDevice;
use wkv::{StoreEvent, WedbStore};
use wnode_test::{consumer_on, err_frame, roundtrip};
use wval::{CustomObjectType, KeyTag};
fn open_store(tag: &str) -> (Arc<TestStore>, TempDir, Arc<AtomicU64>) {
  wnode_test::open_store_with_sink(tag, on_store_event)
}

type TestStore = WedbStore<SegmentedDevice>;

/// JSON 载荷解码失败错误帧（wext_json error.rs ERR_JSON_DECODE_FAILED 帧形）
fn json_decode_frame() -> Vec<u8> {
  err_frame("ERR JSON object decode failed")
}

/// roaring 同通道对照帧（wext_roaring ERR_DECODE，同形纪律先例，不回退）
fn roaring_decode_frame() -> Vec<u8> {
  err_frame("ERR RoaringBitmap object decode failed")
}

/// 自定义对象 AOF 入账计数器（StoreEvent::EnvelopeUpsert 单点拦查——
/// custom 对象 Save 经 notify_envelope_upsert 入 AOF ObjectStoreUpsert 条目，
/// service.rs 事件映射单源）
fn on_store_event(
  counter: &AtomicU64,
  _ver: i64,
  _aof_session_id: i32,
  event: StoreEvent<'_>,
) -> wkv::Result<()> {
  if matches!(event, StoreEvent::EnvelopeUpsert { .. }) {
    counter.fetch_add(1, Ordering::Relaxed);
  }
  Ok(())
}

/// 直塞原始信封物理记录：合法扩展标签 + 非 JSON 字节的损坏载荷
fn seed_corrupt(store: &Arc<TestStore>, key: &[u8], tag: CustomObjectType) {
  let mut raw = vec![tag.as_u8()];
  raw.extend_from_slice(b"not-json-payload!!");
  let sess = store.new_session().unwrap();
  let batch = sess.enter_batch();
  batch
    .try_upsert_tag_sync(key, KeyTag::ObjectEnvelope, &raw)
    .unwrap()
    .unwrap();
}

/// HLog 尾地址（写回判据：伪写回必追加信封新记录使 tail 前进）
fn hlog_tail(store: &Arc<TestStore>) -> u64 {
  store.new_session().unwrap().store.tail_address()
}

/// 落稳 HLog 布局与后台事件分发后取判据
fn quiesce() {
  sleep(Duration::from_millis(20));
}

/// 读族四头（含票面 json_get_reader、非共用头 json_type_reader/json_resp_reader
/// 与共用头 eval_json_target 消费族代表）corrupt 载荷恒回错误帧，非 nil
#[test]
fn corrupt_json_reads_reject_with_error_frame_not_nil() {
  let (store, _dir, _cnt) = open_store("json-corrupt-read.db");
  seed_corrupt(&store, b"cj", CustomObjectType::Json);
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);

  let frame = json_decode_frame();
  let cases: &[&[&[u8]]] = &[
    &[b"JSON.GET", b"cj", b"$"],
    &[b"JSON.GET", b"cj", b"$.a"],
    &[b"JSON.TYPE", b"cj", b"$"],
    &[b"JSON.RESP", b"cj", b"$"],
    &[b"JSON.STRLEN", b"cj", b"$"],
  ];
  for args in cases {
    let out = roundtrip(&rt, &mut c, args);
    assert_eq!(out, frame, "{args:?} corrupt 载荷须回错误帧（禁 nil 折叠）");
  }
}

/// 变异族 corrupt 载荷：错误帧 + 零重写零 AOF 入账；对照真写入 tail 前进且
/// AOF +1（杜绝把禁写判据写成空跑）
#[test]
fn corrupt_json_mutations_zero_rewrite_and_aof() {
  let (store, _dir, aof_rmw_count) = open_store("json-corrupt-mut.db");
  seed_corrupt(&store, b"cj", CustomObjectType::Json);
  let rt = Runtime::new().unwrap();

  let base_tail = hlog_tail(&store);
  let base_aof = aof_rmw_count.load(Ordering::Relaxed);
  let mut c = consumer_on(&store);

  let frame = json_decode_frame();
  let cases: &[&[&[u8]]] = &[
    &[b"JSON.DEL", b"cj", b"$"],
    &[b"JSON.CLEAR", b"cj", b"$"],
    &[b"JSON.NUMINCRBY", b"cj", b"$", b"1"],
    &[b"JSON.TOGGLE", b"cj", b"$"],
  ];
  for args in cases {
    let out = roundtrip(&rt, &mut c, args);
    assert_eq!(out, frame, "{args:?} corrupt 变异须回错误帧（禁 :0 折叠）");
  }

  quiesce();
  assert_eq!(
    hlog_tail(&store),
    base_tail,
    "corrupt 变异发生伪写回：损坏载荷被 Save 重写回库（HLog 追加新信封记录）"
  );
  assert_eq!(
    aof_rmw_count.load(Ordering::Relaxed),
    base_aof,
    "corrupt 变异向 AOF 追加了 ObjectStoreUpsert 条目（updater false 须放弃落库）"
  );

  // 反空跑对照：合法写入仍正常落库——tail 前进且 AOF +1
  let ok = roundtrip(&rt, &mut c, &[b"JSON.SET", b"gk", b"$", b"{\"a\":1}"]);
  assert_eq!(ok, b"+OK\r\n");
  quiesce();
  assert!(
    hlog_tail(&store) > base_tail,
    "合法 JSON.SET 未落库——对照失效（判据空跑）"
  );
  assert!(
    aof_rmw_count.load(Ordering::Relaxed) > base_aof,
    "合法 JSON.SET 未入 AOF 账——对照失效（判据空跑）"
  );

  // 损坏键态原样保留（错误帧路径零改写载荷），合法键读回逐字节如常
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"JSON.GET", b"gk", b"$.a"]),
    b"$3\r\n[1]\r\n"
  );
}

/// 多键读臂协议整形不变：corrupt 键元素位 nil（error_element_to_nil 禁错误帧入
/// 元素位），健康键元素逐字节如常
#[test]
fn json_mget_corrupt_element_stays_nil_shape() {
  let (store, _dir, _cnt) = open_store("json-corrupt-mget.db");
  seed_corrupt(&store, b"cj", CustomObjectType::Json);
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"JSON.SET", b"gk", b"$", b"{\"a\":1}"]),
    b"+OK\r\n"
  );

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"JSON.MGET", b"cj", b"gk", b"$.a"]),
    b"*2\r\n$-1\r\n$3\r\n[1]\r\n",
    "corrupt 键在 MGET 元素位须整形为 nil，健康元素不受累"
  );
}

/// 同通道 roaring R.GETBIT 同形对照：corrupt 载荷恒错误帧、nil/:0 折叠纪律
/// 在两条扩展通道齐平（wext_roaring ERR_DECODE 先例不回退）
#[test]
fn roaring_corrupt_decode_frame_parity() {
  let (store, _dir, _cnt) = open_store("json-corrupt-roaring.db");
  seed_corrupt(&store, b"rb", CustomObjectType::Roaring);
  let mut c = consumer_on(&store);
  let rt = Runtime::new().unwrap();
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"R.GETBIT", b"rb", b"42"]),
    roaring_decode_frame(),
    "R.GETBIT corrupt 载荷恒错误帧（同形对照锚）"
  );
}
