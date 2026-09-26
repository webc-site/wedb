//! 哈希读臂（HGETALL/HKEYS/HVALS）TTL 物理剔除固化回归
//!（票 wnode-hgetall-envelope-ttl-purge-not-solidified 验证点 a–f）
//!
//! 缺陷背景：三读臂旧形经 hash_load_sync+run_operate 装载即弃，装载期
//!（deserialize_from_slice）与出帧期（purge_expired_len）物理剔除实际发生却
//! 零落盘零删空自愈，违本仓「凡物理剔除必经 mutated_by_ttl 升格写回」固化
//! 契约（hash_commands/mod.rs 契约注、HTTL 既定模式）；第二实例：HINCRBY 族
//! 存量解析错臂出 '-' 帧被 should_write_back '-' 门整臂拒写，剔除矫正弃置。
//! 修法：快臂改走 hash_rmw_missing（run_sync_rmw 骨架 + on_missing 短路钩子）、
//! 慢臂 load_spec 三行改经 run_async_rmw 同钩同帧、'-' 门增
//! existed∧mutated_by_ttl 豁免支。C# 对照：HashObjectImpl.cs:HashGetAll/
//! HashGetKeysOrValues 纯读过滤不物理剔除（常驻对象层无此面），缺键
//! NOTFOUND 恒 RESP_EMPTYLIST（HashCommands.cs:148/:513）。
//!
//! 判据面（全走生产 run_sync_rmw / run_async_rmw / RESP 帧，无 mock）：
//! a) 到期剔除固化：直读信封探针断言陈旧字段物理消失、头计数/水位矫正，
//!    二次 HGETALL 逐字节一致；
//! b) 缺键 RESP2/RESP3 恒 `*0` 常量帧锁（on_missing 钩子路径）＋存在全剔
//!    RESP3 `%0`/RESP2 `*0` 且 EXISTS=0（删空自愈）；
//! c) 存量非数值 HINCRBY 错误帧逐字不变，到期字段不复活（'-' 门豁免面）；
//! d) 快慢双臂同键同命令应答与终态逐字节全等（flush_and_evict 磁盘驻留强制
//!    降级慢臂）；
//! f) 剔除零发生时 HLog tail 零推进、AOF ObjectStoreRMW 零追加。

use std::{
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  thread::sleep,
  time::Duration,
};

use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use wcol::{
  hash::hash_object::HashObject,
  object_payload::{
    GarnetObjectPayload, NO_EXPIRY_WATERMARK, count_of_blob, expiry_watermark_of_blob, obj_decode,
  },
};
use wdev::SegmentedDevice;
use wkv::{StoreEvent, StoreEventSink, StoreResult, WedbStore};
use wnode::{
  RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::{roundtrip, with_batch};
use wresp::cmd_strings::{RESP_ERR_HASH_VALUE_IS_NOT_INTEGER, write_error_raw};
use wtest_base::test_store_config;
use wval::{GarnetObjectType, KeyTag};

type TestStore = WedbStore<SegmentedDevice>;

/// HEXPIRE 成功回执逐字段码数组（C# HashCommands.cs:HashExpire 恒逐字段数组）
fn hexpire_ok(codes: &[i64]) -> Vec<u8> {
  let mut out = format!("*{}\r\n", codes.len()).into_bytes();
  for c in codes {
    out.extend_from_slice(format!(":{c}\r\n").as_bytes());
  }
  out
}

/// TTL 出账等待（HEXPIRE 1s + coarsetime 粒度余量）
const TTL_WAIT: Duration = Duration::from_millis(1200);

/// AOF ObjectStoreRMW 追加计数器（StoreEvent::ObjectRmw 单点拦查，
/// object_rmw_zero_mutation_no_writeback.rs 同形）
fn on_store_event(
  counter: &AtomicU64,
  _ver: i64,
  _aof_session_id: i32,
  event: StoreEvent<'_>,
) -> wkv::Result<()> {
  if matches!(event, StoreEvent::ObjectRmw(_)) {
    counter.fetch_add(1, Ordering::Relaxed);
  }
  Ok(())
}

fn open_store(tag: &str) -> (Arc<TestStore>, TempDir, Arc<AtomicU64>) {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let aof_rmw_count = Arc::new(AtomicU64::new(0));
  assert!(
    store.set_event_sink(StoreEventSink::new(
      Arc::clone(&aof_rmw_count),
      on_store_event
    )),
    "事件分发器注入失败（重复注入或时机过晚）"
  );
  (store, dir, aof_rmw_count)
}

fn consumer_on(store: &Arc<TestStore>) -> RespSessionConsumer {
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// 直读信封探针：`(头部计数, 到期水位, 在场字段升序表)`；键消亡回 `None`
///（store 侧 read_raw 物理记录 + obj_decode/from_blob，不走命令通道，
/// 剔除固化/删空自愈的白盒判据单源）
fn probe_envelope(
  rt: &Runtime,
  store: &Arc<TestStore>,
  key: &[u8],
) -> Option<(usize, i64, Vec<String>)> {
  let sess = store.new_session().expect("取证会话");
  let rec_k = sess.session_tag_key(KeyTag::ObjectEnvelope, key);
  let raw = rt
    .block_on(sess.read_raw(&rec_k))
    .expect("信封物理记录读取不得报存储错误")?;
  let payload = obj_decode(&raw, GarnetObjectType::Hash).expect("信封标签须为 Hash");
  let count = count_of_blob(payload).expect("计数头不得畸形");
  let watermark =
    expiry_watermark_of_blob(GarnetObjectType::Hash, payload).expect("水位头不得畸形");
  let obj = HashObject::from_blob(payload).expect("载荷解码不得失败（fail-fast 契约）");
  let mut fields: Vec<String> = obj
    .hash
    .keys()
    .map(|k| String::from_utf8_lossy(k).into_owned())
    .collect();
  fields.sort();
  Some((count, watermark, fields))
}

/// HLog 尾地址（写回判据：写回必追加信封新记录使 tail 前进）
fn hlog_tail(store: &Arc<TestStore>) -> u64 {
  store.new_session().unwrap().store.tail_address()
}

fn quiesce() {
  sleep(Duration::from_millis(20));
}

/// 种子：f1 无挂期存活，f2/f3 HEXPIRE 1s 到期
fn seed_purgeable(rt: &Runtime, store: &Arc<TestStore>, key: &[u8]) {
  let mut c = consumer_on(store);
  assert_eq!(
    roundtrip(
      rt,
      &mut c,
      &[b"HSET", key, b"f1", b"v1", b"f2", b"v2", b"f3", b"v3"]
    ),
    b":3\r\n"
  );
  assert_eq!(
    roundtrip(
      rt,
      &mut c,
      &[b"HEXPIRE", key, b"1", b"FIELDS", b"2", b"f2", b"f3"]
    ),
    hexpire_ok(&[1, 1])
  );
}

/// a) HGETALL 到期剔除固化：应答仅存活字段；信封探针断言 f2/f3 物理消失、
/// 头计数矫正为 1、水位归位无存活挂期旗标；二次 HGETALL 逐字节一致（幂等，
/// 剔除不再重复摊还）；HKEYS/HVALS 同臂同果
#[test]
fn hgetall_ttl_purge_solidifies_envelope() {
  let (store, _dir, _aof) = open_store("hgetall-solidify.db");
  let rt = Runtime::new().unwrap();
  seed_purgeable(&rt, &store, b"hs:k");
  sleep(TTL_WAIT);

  let mut c = consumer_on(&store);
  let want = b"*2\r\n$2\r\nf1\r\n$2\r\nv1\r\n";
  assert_eq!(roundtrip(&rt, &mut c, &[b"HGETALL", b"hs:k"]), want);

  // 探针：陈旧字段已物理剔除并随 mutated_by_ttl 升格写回固化
  let (count, watermark, fields) = probe_envelope(&rt, &store, b"hs:k").expect("键应在场");
  assert_eq!(count, 1, "头计数应矫正为存活字段数");
  assert_eq!(watermark, NO_EXPIRY_WATERMARK, "无存活挂期字段水位应归位");
  assert_eq!(fields, vec!["f1".to_string()], "到期字段须物理消失");

  // 二次 HGETALL：固化后零剔除，应答逐字节一致
  assert_eq!(roundtrip(&rt, &mut c, &[b"HGETALL", b"hs:k"]), want);
  // HKEYS/HVALS 同钩同臂同果
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HKEYS", b"hs:k"]),
    b"*1\r\n$2\r\nf1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HVALS", b"hs:k"]),
    b"*1\r\n$2\r\nv1\r\n"
  );
  // 矫正后信封不再劣化
  assert_eq!(
    probe_envelope(&rt, &store, b"hs:k"),
    Some((1, NO_EXPIRY_WATERMARK, vec!["f1".to_string()]))
  );
}

/// b-1) 缺键恒常量帧锁：HGETALL/HKEYS/HVALS 缺键 RESP2/RESP3 一律 `*0`
///（on_missing 短路钩子路径；C# HashCommands.cs:148/:513 NOTFOUND →
/// RESP_EMPTYLIST 协议恒定，RESP3 下不得出 `%0`）
#[test]
fn missing_key_constant_frame_lock() {
  for ver in [2u8, 3] {
    with_batch(move |s, batch| {
      s.resp_protocol_version = ver;
      let mut out = Vec::new();
      s.hash_get_all(&[b"nokey"], batch, &mut out).unwrap();
      assert_eq!(out, b"*0\r\n", "HGETALL 缺键 RESP{ver} 应恒常量空数组帧");
      out.clear();
      s.hash_keys(&[b"nokey"], batch, &mut out, true).unwrap();
      assert_eq!(out, b"*0\r\n", "HKEYS 缺键 RESP{ver} 应恒常量空数组帧");
      out.clear();
      s.hash_vals(&[b"nokey"], batch, &mut out).unwrap();
      assert_eq!(out, b"*0\r\n", "HVALS 缺键 RESP{ver} 应恒 *0（非 %0）");
    });
  }
}

/// b-2) 存在全剔：Present 臂经 run_op 出空图 RESP3 `%0`/RESP2 `*0`，空载荷
/// 经 obj_save_or_gc_raw→try_delete_sync 删空自愈（EXISTS=0、信封记录消亡）；
/// 此后键转缺键态，应答转常量帧 `*0`（与全剔帧可观测区分）
#[test]
fn purge_all_selfheals_and_missing_thereafter() {
  for ver in [2u8, 3] {
    // Present 全剔空图帧随协议（run_op 出帧）；自愈后缺键帧恒 *0（钩子常量）
    let purged_frame: &[u8] = if ver >= 3 { b"%0\r\n" } else { b"*0\r\n" };
    with_batch(move |s, batch| {
      s.resp_protocol_version = ver;
      let mut out = Vec::new();
      s.hash_set(&[b"he:k", b"only", b"v"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":1\r\n");
      out.clear();
      s.hash_expire(
        "HEXPIRE",
        &[b"he:k", b"1", b"FIELDS", b"1", b"only"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
      assert_eq!(out, hexpire_ok(&[1]));
      sleep(TTL_WAIT);

      // 全剔首读：Present 空图帧（HGETALL 为 map，RESP3=%0/RESP2=*0）+ 删空自愈
      out.clear();
      s.hash_get_all(&[b"he:k"], batch, &mut out).unwrap();
      assert_eq!(out, purged_frame, "存在全剔 RESP{ver} 空图帧走样");
      out.clear();
      s.network_exists(&[b"he:k"], batch, None, &mut out).unwrap();
      assert_eq!(out, b":0\r\n", "全剔删空自愈后 EXISTS 应为 0");
      // HVALS 数组头协议恒定 `*0`（C# HashGetKeysOrValues 用 WriteArrayLength，
      // 非 map）；此刻键已自愈删除，经 on_missing 钩子亦出 *0，双臂同帧
      out.clear();
      s.hash_keys(&[b"he:k"], batch, &mut out, false).unwrap();
      assert_eq!(out, b"*0\r\n", "HVALS 数组头 RESP{ver} 应恒 *0");

      // 信封物理记录消亡（删空自愈收口，与 HLEN 慢臂 envelope_length_correct 同果）
      let present = batch
        .try_read_tag_sync(b"he:k", KeyTag::ObjectEnvelope, |_| ())
        .unwrap();
      assert!(
        !matches!(present, StoreResult::Success(())),
        "信封应随删空自愈消亡，实为 {present:?}"
      );

      // 自愈后即缺键：RESP3 亦转 `*0` 常量帧（与 `%0` 全剔帧可区分）
      out.clear();
      s.hash_get_all(&[b"he:k"], batch, &mut out).unwrap();
      assert_eq!(out, b"*0\r\n", "自愈后缺键 RESP{ver} 应恒 *0（钩子常量帧）");
    });
  }
}

/// c) 第二实例（'-' 门豁免面）：存量非数值 + 他字段到期 → HINCRBY 错误帧
/// 逐字不变；探针断言到期字段已随豁免写回落盘、未复活
#[test]
fn hincrby_error_frame_purge_not_discarded() {
  let (store, _dir, _aof) = open_store("hincrby-dashgate.db");
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      &[b"HSET", b"hd:k", b"bad", b"notanumber", b"eph", b"x"]
    ),
    b":2\r\n"
  );
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      &[b"HEXPIRE", b"hd:k", b"1", b"FIELDS", b"1", b"eph"]
    ),
    hexpire_ok(&[1])
  );
  sleep(TTL_WAIT);

  // 错误帧逐字锁（修法只放行持久化，应答零变化）
  let mut want = Vec::new();
  write_error_raw(&mut want, RESP_ERR_HASH_VALUE_IS_NOT_INTEGER);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HINCRBY", b"hd:k", b"bad", b"1"]),
    want,
    "HINCRBY 存量非数值错误帧不得走样"
  );

  // 豁免支承接剔除矫正：装载期 delete_expired_items 已发生的 eph 物理剔除
  // 必固化，否则下次装载复活
  let (count, _, fields) = probe_envelope(&rt, &store, b"hd:k").expect("键应在场");
  assert_eq!(fields, vec!["bad".to_string()], "到期字段应已落盘剔除");
  assert_eq!(count, 1, "头计数应随豁免写回矫正");
  // 二次任意读命令不再重复剔除（幂等）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HGETALL", b"hd:k"]),
    b"*2\r\n$3\r\nbad\r\n$10\r\nnotanumber\r\n"
  );
}

/// d) 快慢双臂同形锁：同内容同命令，热键（run_sync_rmw 快臂）与磁盘驻留冷键
///（flush_and_evict 强制降级 → run_async_rmw 慢臂）应答逐字节全等、固化终态
/// 全等（订正点四：慢臂 load_spec 同改接 rmw 通道）
#[test]
fn fast_slow_arms_parity() {
  let (store, _dir, _aof) = open_store("hgetall-parity.db");
  let rt = Runtime::new().unwrap();
  seed_purgeable(&rt, &store, b"pa:hot");
  seed_purgeable(&rt, &store, b"pa:cold");
  sleep(TTL_WAIT);

  let mut c = consumer_on(&store);
  let hot_all = roundtrip(&rt, &mut c, &[b"HGETALL", b"pa:hot"]);
  let hot_keys = roundtrip(&rt, &mut c, &[b"HKEYS", b"pa:hot"]);
  let hot_vals = roundtrip(&rt, &mut c, &[b"HVALS", b"pa:hot"]);

  // 冷化 pa:cold（pa:hot 已固化收口，一并冷化不影响其断言）
  rt.block_on(store.flush_and_evict_all())
    .expect("冷化刷盘不得报错");
  let cold_all = roundtrip(&rt, &mut c, &[b"HGETALL", b"pa:cold"]);
  let cold_keys = roundtrip(&rt, &mut c, &[b"HKEYS", b"pa:cold"]);
  let cold_vals = roundtrip(&rt, &mut c, &[b"HVALS", b"pa:cold"]);

  assert_eq!(cold_all, hot_all, "HGETALL 快慢双臂应答须逐字节全等");
  assert_eq!(cold_keys, hot_keys, "HKEYS 快慢双臂应答须逐字节全等");
  assert_eq!(cold_vals, hot_vals, "HVALS 快慢双臂应答须逐字节全等");

  // 终态全等：双臂固化同果（头计数/水位/在场字段）
  let hot_state = probe_envelope(&rt, &store, b"pa:hot").expect("热臂键应在场");
  let cold_state = probe_envelope(&rt, &store, b"pa:cold").expect("慢臂键应在场");
  assert_eq!(hot_state.0, cold_state.0, "终态头计数应全等");
  assert_eq!(hot_state.1, cold_state.1, "终态水位应全等");
  assert_eq!(hot_state.2, cold_state.2, "终态在场字段应全等");

  // 冷键二次读（已热化固化态）与首读逐字节一致（剔除不再重复支付）
  let again = roundtrip(&rt, &mut c, &[b"HGETALL", b"pa:cold"]);
  assert_eq!(again, cold_all);
}

/// f) 剔除零发生不落库：无挂期字段的热键三连读 ×2 轮，HLog tail 零推进、
/// AOF ObjectStoreRMW 零追加（is_read_only 登记保持，无 mutated_by_ttl 即
/// `_` 臂判假）
#[test]
fn no_purge_reads_never_write_back() {
  let (store, _dir, aof_rmw_count) = open_store("hgetall-zerowrite.db");
  let rt = Runtime::new().unwrap();
  let mut seed = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut seed, &[b"HSET", b"zw:k", b"f", b"v"]),
    b":1\r\n"
  );
  drop(seed);
  quiesce();

  let base_tail = hlog_tail(&store);
  let base_aof = aof_rmw_count.load(Ordering::Relaxed);

  let mut c = consumer_on(&store);
  for _ in 0..2 {
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"HGETALL", b"zw:k"]),
      b"*2\r\n$1\r\nf\r\n$1\r\nv\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"HKEYS", b"zw:k"]),
      b"*1\r\n$1\r\nf\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"HVALS", b"zw:k"]),
      b"*1\r\n$1\r\nv\r\n"
    );
  }
  quiesce();
  assert_eq!(
    hlog_tail(&store),
    base_tail,
    "零剔除读臂发生伪写回：HLog 追加了新信封记录"
  );
  assert_eq!(
    aof_rmw_count.load(Ordering::Relaxed),
    base_aof,
    "零剔除读臂向 AOF 追加了 ObjectStoreRMW 增量条目"
  );

  // 反空跑对照：真剔除（到期固化）必须推 tail 且 +1 AOF，杜绝「一律禁写」虚设
  let mut seed2 = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut seed2, &[b"HSET", b"zw:t", b"g", b"1"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(
      &rt,
      &mut seed2,
      &[b"HEXPIRE", b"zw:t", b"1", b"FIELDS", b"1", b"g"]
    ),
    hexpire_ok(&[1])
  );
  drop(seed2);
  sleep(TTL_WAIT);
  let mut c2 = consumer_on(&store);
  assert_eq!(roundtrip(&rt, &mut c2, &[b"HGETALL", b"zw:t"]), b"*0\r\n");
  quiesce();
  assert!(
    hlog_tail(&store) > base_tail,
    "全剔删空自愈必须真实落盘（写回收口）"
  );
  assert!(
    aof_rmw_count.load(Ordering::Relaxed) > base_aof,
    "剔除升格写回应入账 AOF 增量条目"
  );
}
