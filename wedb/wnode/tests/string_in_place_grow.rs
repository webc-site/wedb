//! APPEND / SETRANGE 两臂双路一致性回归
//!
//! 对位 C# `libs/server/Storage/Functions/MainStore/RMWMethods.cs` 的同一 RMW
//! 状态机：槽位松弛容得下即 `InPlaceUpdater`（APPEND :799-834 / SETRANGE
//! :734-763，旧数据零复制、只拷新字节、原位改长），否则落回 `CopyUpdater`
//! （:1040 / :1402 整值尾部追加）。两路应答与读回值须逐字节一致，且原位臂
//! 不得改动槽位物理尺寸与 key 级 TTL。

use core::str;
use std::sync::Arc;

use parking_lot::Mutex;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wkv::{StoreEvent, StoreEventSink};
use wnode::{
  resp::resp_server_session::RespServerSession,
  storage::session::common::ttl_sync::{put_ttl_sync, ttl_of_sync},
};
use wnode_test::{Batch, err_frame, with_batch};
use wresp::cmd_strings::RESP_ERR_GENERIC;

fn resp_int(out: &[u8]) -> i64 {
  assert_eq!(out[0], b':', "应答须为整数帧：{out:?}");
  let text = str::from_utf8(&out[1..out.len() - 2]).unwrap();
  text.parse().unwrap()
}

fn set(s: &mut RespServerSession, batch: &Batch<'_>, key: &[u8], val: &[u8]) {
  let mut out = Vec::new();
  s.network_set(&[key, val], batch, None, &mut out).unwrap();
  assert_eq!(out, b"+OK\r\n");
}

fn get(s: &mut RespServerSession, batch: &Batch<'_>, key: &[u8]) -> Vec<u8> {
  let mut out = Vec::new();
  s.network_get(&[key], batch, &mut out).unwrap();
  assert_eq!(out[0], b'$', "读回须命中：{out:?}");
  let head_end = out.iter().position(|&c| c == b'\r').unwrap();
  let len: i64 = str::from_utf8(&out[1..head_end]).unwrap().parse().unwrap();
  assert!(len >= 0, "读回须命中：{out:?}");
  out[head_end + 2..head_end + 2 + len as usize].to_vec()
}

fn usage(s: &mut RespServerSession, batch: &Batch<'_>, key: &[u8]) -> i64 {
  let mut out = Vec::new();
  s.network_memory_usage(&[key], batch, None, &mut out)
    .unwrap();
  resp_int(&out)
}

fn append(s: &mut RespServerSession, batch: &Batch<'_>, key: &[u8], val: &[u8]) -> i64 {
  let mut out = Vec::new();
  s.network_append(&[key, val], batch, &mut out).unwrap();
  resp_int(&out)
}

fn set_range(
  s: &mut RespServerSession,
  batch: &Batch<'_>,
  key: &[u8],
  offset: usize,
  val: &[u8],
) -> i64 {
  let mut out = Vec::new();
  let off = offset.to_string();
  s.network_set_range(&[key, off.as_bytes(), val], batch, &mut out)
    .unwrap();
  resp_int(&out)
}

fn set_bit(
  s: &mut RespServerSession,
  batch: &Batch<'_>,
  key: &[u8],
  offset: usize,
  val: u8,
) -> i64 {
  let mut out = Vec::new();
  let off = offset.to_string();
  let v = [b'0' + val];
  s.network_string_set_bit(&[key, off.as_bytes(), &v], batch, &mut out)
    .unwrap();
  resp_int(&out)
}

/// 腾出槽位松弛富余：长值先落槽、短值原位覆写，被腾空的字节转为松弛填充
/// （MEMORY USAGE 不变即证槽位物理尺寸未动）
fn seed_slack(s: &mut RespServerSession, batch: &Batch<'_>, key: &[u8], short: &[u8]) -> i64 {
  set(s, batch, key, &[b'L'; 40]);
  set(s, batch, key, short);
  usage(s, batch, key)
}

/// 写监听（AOF / 复制旁路）入账的最小可观测量：物理键 + 通知载荷 + 墓碑位
#[derive(Debug, Clone, PartialEq, Eq)]
struct WriteNotice {
  key: Vec<u8>,
  val: Vec<u8>,
  tombstone: bool,
}

type NoticeLog = Arc<Mutex<Vec<WriteNotice>>>;

/// 记录 [`StoreEvent::Write`] 的旁路订阅者（与 `wkv/tests/etag_port.rs` 同一
/// 装配惯例：注入先于本测试的一切写操作，事件载荷按借用语义即时拷出）
fn write_notice_sink(log: NoticeLog) -> StoreEventSink {
  StoreEventSink::new(log, |log, _ver, _aof_session_id, event| {
    if let StoreEvent::Write {
      key,
      val,
      tombstone,
    } = event
    {
      log.lock().push(WriteNotice {
        key: key.to_vec(),
        val: val.to_vec(),
        tombstone,
      });
    }
    Ok(())
  })
}

/// 取本次操作新增的通知条目（订阅者以既有长度为基线，前序 SET 不入比对集）
fn latest_notices(log: &NoticeLog, base: usize) -> Vec<WriteNotice> {
  let guard = log.lock();
  assert_eq!(
    guard.len(),
    base + 1,
    "单次写操作须且仅须分发一条写通知，不得因原位臂多发、漏发或改口径"
  );
  guard[base..].to_vec()
}

/// APPEND 两臂应答与读回逐字节一致；原位臂不动槽位物理尺寸
///
/// 本用例只钉「两臂等价」这一层（应答整数、读回字节、槽位物理尺寸不变），
/// 不承担原位臂的最小写证伪：回落臂经 `try_rmw_sync` →
/// `try_upsert_raw_sync_unprotected` → `whlog::HybridLog::try_update_in_place` 时，
/// 同一容量门（`RecordHeader::can_update_with_slack`）允许引擎侧整值原位覆写，
/// 槽位照样不动，故 RESP 面上无可观测量区分「只拷新字节」与「物化整值再全量覆写」。
/// 零复制、零中间 Vec 的证伪在记录层与页层：
/// wrecord 的 `test_in_place_grow_frame_byte_diff_vs_whole_record_rewrite`
/// （分配探针 + 帧字节差分窗口）与 whlog 的
/// `test_grow_record_in_place_frame_byte_diff`（页内帧差分 + 整帧重发对照臂）
#[test]
fn append_in_place_and_tail_paths_agree() {
  with_batch(|s, batch| {
    let short = b"1234567890";
    let extra = b"ABCDE";

    // 甲：无富余可用 → 整值尾部追加臂
    let tail_key = b"ag-tail";
    set(s, batch, tail_key, short);
    let tail_usage_before = usage(s, batch, tail_key);
    assert_eq!(append(s, batch, tail_key, extra), 15);
    let tail_usage_after = usage(s, batch, tail_key);
    assert!(
      tail_usage_after > tail_usage_before,
      "尾部追加臂必落新记录：{tail_usage_before} -> {tail_usage_after}"
    );

    // 乙：槽位容得下 → 原位增长臂
    let in_place_key = b"ag-inplace";
    let slack_usage = seed_slack(s, batch, in_place_key, short);
    assert_eq!(append(s, batch, in_place_key, extra), 15);
    assert_eq!(
      usage(s, batch, in_place_key),
      slack_usage,
      "原位增长臂不得改变槽位物理尺寸"
    );

    assert_eq!(
      get(s, batch, tail_key),
      get(s, batch, in_place_key),
      "两臂读回值须逐字节一致"
    );
    assert_eq!(get(s, batch, in_place_key), b"1234567890ABCDE");
  });
}

/// 同键连续 APPEND 触发原位后，GET / MEMORY USAGE / TTL 三面皆正确
#[test]
fn consecutive_append_in_place_keeps_value_usage_ttl() {
  with_batch(|s, batch| {
    let key = b"ag-consec";
    let slack_usage = seed_slack(s, batch, key, b"1234567890");
    let expire_at = now_ticks() + 3600 * TICKS_PER_SECOND;
    put_ttl_sync(batch, key, expire_at).unwrap();

    assert_eq!(append(s, batch, key, b"ABCDE"), 15);
    assert_eq!(append(s, batch, key, b"FGHIJ"), 20);
    assert_eq!(append(s, batch, key, b"KLMNO"), 25);

    assert_eq!(get(s, batch, key), b"1234567890ABCDEFGHIJKLMNO");
    assert_eq!(usage(s, batch, key), slack_usage, "三度原位增长不动槽位");
    let kept = ttl_of_sync(batch, key)
      .unwrap()
      .value()
      .unwrap()
      .expect("原位增长不得清除既有 TTL");
    assert_eq!(kept, expire_at);
  });
}

/// 超富余的 APPEND 自然回落尾部臂，应答仍与整值语义一致
#[test]
fn append_beyond_slack_falls_back() {
  with_batch(|s, batch| {
    let key = b"ag-over";
    set(s, batch, key, &[b'L'; 20]);
    set(s, batch, key, b"x");
    let slack_usage = usage(s, batch, key);

    let big = vec![b'B'; 500];
    assert_eq!(append(s, batch, key, &big), 501);
    let after = usage(s, batch, key);
    assert!(
      after > slack_usage,
      "超容量须另起记录、不得就地吞槽：{slack_usage} -> {after}"
    );
    let got = get(s, batch, key);
    assert_eq!(got.len(), 501);
    assert_eq!(&got[..1], b"x");
    assert!(got[1..].iter().all(|&c| c == b'B'));
  });
}

/// SETRANGE 间隙补零两臂一致（原位臂的富余字节残留更早的长值，绝不零依赖）
#[test]
fn set_range_gap_zero_fill_matches_tail_path() {
  with_batch(|s, batch| {
    let short = b"1234567890";
    let gap = b"XY";

    let tail_key = b"sr-tail";
    set(s, batch, tail_key, short);
    let replied_tail = set_range(s, batch, tail_key, 20, gap);

    let in_place_key = b"sr-inplace";
    let slack_usage = seed_slack(s, batch, in_place_key, short);
    // 40 字节长值腾出的槽位容得下 offset 20 + 2
    let replied_in_place = set_range(s, batch, in_place_key, 20, gap);
    assert_eq!(replied_tail, replied_in_place);
    assert_eq!(replied_in_place, 22);
    assert_eq!(
      usage(s, batch, in_place_key),
      slack_usage,
      "SETRANGE 原位增长臂不得改变槽位物理尺寸"
    );

    let mut expected = Vec::new();
    expected.extend_from_slice(short);
    expected.resize(20, 0);
    expected.extend_from_slice(gap);
    assert_eq!(get(s, batch, tail_key), expected, "尾部臂间隙补零");
    assert_eq!(
      get(s, batch, in_place_key),
      expected,
      "原位臂间隙补零须与尾部臂逐字节一致"
    );
  });
}

/// SETRANGE 覆写旧值中段与覆写尾段（长度不变）两臂一致
#[test]
fn set_range_overwrite_paths_agree() {
  with_batch(|s, batch| {
    let base = b"0123456789";

    let tail_key = b"sro-tail";
    set(s, batch, tail_key, base);
    assert_eq!(set_range(s, batch, tail_key, 2, b"AB"), 10);
    assert_eq!(get(s, batch, tail_key), b"01AB456789");

    let in_place_key = b"sro-inplace";
    let slack_usage = seed_slack(s, batch, in_place_key, base);
    assert_eq!(set_range(s, batch, in_place_key, 2, b"AB"), 10);
    assert_eq!(get(s, batch, in_place_key), b"01AB456789");
    assert_eq!(usage(s, batch, in_place_key), slack_usage);

    // 尾段覆写不改长（对位 C# InPlaceUpdater「总长未超现值即不重排松弛」）
    assert_eq!(set_range(s, batch, in_place_key, 8, b"YZ"), 10);
    assert_eq!(get(s, batch, in_place_key), b"01AB4567YZ");
    assert_eq!(usage(s, batch, in_place_key), slack_usage);
  });
}

/// 零长度边界三连（对位 C# RMWMethods.cs:799 "If nothing to append, can avoid
/// copy update" 与 SETRANGE 空改段的 `new_len == old_len` 退化形态）：
/// 空载荷 APPEND 不改长、空值起手原位增长、空载荷越界 SETRANGE 纯补零，
/// 三面应答与读回皆须与整值臂逐字节一致——槽位富余里残留的是更早的 40
/// 字节长值，任何一处漏补零都会把 `L` 泄漏成读回值
#[test]
fn zero_length_payloads_agree_with_tail_paths() {
  with_batch(|s, batch| {
    // ① 空载荷 APPEND：经等长原位发布臂免复制回长（对位 C# :800 免 copy update
    // 短路），三面可观测量与整值臂逐字节一致
    let key = b"z-append";
    let slack_usage = seed_slack(s, batch, key, b"1234567890");
    assert_eq!(append(s, batch, key, b""), 10);
    assert_eq!(get(s, batch, key), b"1234567890", "空 APPEND 不得改动值");
    assert_eq!(usage(s, batch, key), slack_usage, "空 APPEND 不得换槽位");

    // ② 零长度现值起手：原位臂从 old_len=0 增长，富余首字节即新值
    let zero = b"z-zero";
    set(s, batch, zero, &[b'L'; 40]);
    set(s, batch, zero, b"");
    let zero_usage = usage(s, batch, zero);
    assert_eq!(get(s, batch, zero), b"", "空 SET 落成的零长记录读回空串");
    assert_eq!(append(s, batch, zero, b"X"), 1, "old_len=0 亦可原位增长");
    assert_eq!(get(s, batch, zero), b"X", "新增长度外的残留字节绝不可见");
    assert_eq!(usage(s, batch, zero), zero_usage);

    // ③ 空载荷越界 SETRANGE：整段新增区皆为补零区，两臂逐字节同值
    let tail = b"z-sr-tail";
    set(s, batch, tail, b"1234567890");
    assert_eq!(set_range(s, batch, tail, 24, b""), 24);
    let grow = b"z-sr-grow";
    let grow_usage = seed_slack(s, batch, grow, b"1234567890");
    assert_eq!(set_range(s, batch, grow, 24, b""), 24, "空段仍须按偏移延长");
    assert_eq!(
      usage(s, batch, grow),
      grow_usage,
      "补零延长走原位臂，不动槽位物理尺寸"
    );
    let mut expected = b"1234567890".to_vec();
    expected.resize(24, 0);
    assert_eq!(get(s, batch, tail), expected);
    assert_eq!(get(s, batch, grow), expected, "原位臂补零须与尾部臂同字节");

    // ④ 缺失键 + 空载荷：两臂同落「新建空值」臂，应答 0
    assert_eq!(append(s, batch, b"z-missing", b""), 0);
    assert_eq!(get(s, batch, b"z-missing"), Vec::<u8>::new());
  });
}

/// 写通知（AOF / 复制旁路入账字节）两臂对拍：原位臂在持页写锁期内通知的
/// 必须是**改长后的新全值**，与尾部臂的整值通知口径逐字节等价——AOF 与副本
/// 只认这条字节流，漏发、多发自不必说，把「新增段」当成整值发出去就会在
/// 回放端把旧值凭空抹掉
#[test]
fn write_notice_payload_is_identical_across_in_place_and_tail_arms() {
  with_batch(|s, batch| {
    let log: NoticeLog = Arc::new(Mutex::new(Vec::new()));
    assert!(
      batch
        .store()
        .set_event_sink(write_notice_sink(Arc::clone(&log))),
      "本测试独占存储事件订阅位，注入须成功"
    );

    let short = b"1234567890";
    let extra = b"ABCDE";
    let whole = b"1234567890ABCDE";

    // 甲：无富余 → 尾部整值臂通知
    let tail_key = b"ev-tail";
    let base = log.lock().len();
    set(s, batch, tail_key, short);
    let tail_set = latest_notices(&log, base);
    assert_eq!(tail_set[0].val, short, "SET 通知即整值");
    let base = log.lock().len();
    assert_eq!(append(s, batch, tail_key, extra), 15);
    let tail = latest_notices(&log, base).pop().unwrap();

    // 乙：槽位容得下 → 原位增长臂通知
    let grow_key = b"ev-grow";
    let base = log.lock().len();
    seed_slack(s, batch, grow_key, short);
    assert_eq!(
      log.lock().len(),
      base + 2,
      "播种富余的两度 SET 各发一条通知，原位覆写臂不重复分发"
    );
    let base = log.lock().len();
    assert_eq!(append(s, batch, grow_key, extra), 15);
    let grow = latest_notices(&log, base).pop().unwrap();

    assert_eq!(grow.val, whole, "原位臂通知须是新全值而非新增段");
    assert_eq!(
      grow.val, tail.val,
      "两臂写通知载荷须逐字节等价（AOF 回放口径单一）"
    );
    assert!(!grow.tombstone && !tail.tombstone);
    assert_eq!(
      grow.key.len(),
      tail.key.len(),
      "同物理键模板（会话前缀 + KeyTag + 用户键），仅用户键字节不同"
    );
    assert!(grow.key.ends_with(grow_key.as_slice()));
    assert!(tail.key.ends_with(tail_key.as_slice()));

    // SETRANGE 原位增长臂同口径：通知携带补零后的新全值
    let base = log.lock().len();
    assert_eq!(set_range(s, batch, grow_key, 20, b"XY"), 22);
    let mut expected = Vec::from(whole.as_slice());
    expected.resize(20, 0);
    expected.extend_from_slice(b"XY");
    assert_eq!(
      latest_notices(&log, base)[0].val,
      expected,
      "SETRANGE 原位增长通知须含间隙补零后的完整新值"
    );

    // 等长原位覆写（长度未变）也不得漏发
    let base = log.lock().len();
    assert_eq!(set_range(s, batch, grow_key, 0, b"QQ"), 22);
    expected[..2].copy_from_slice(b"QQ");
    assert_eq!(latest_notices(&log, base)[0].val, expected);
  });
}

/// 跨页硬上限（对标 whlog「单记录不跨页、无 oversized 路径」，本版本 C#
/// 同样没有）：超出槽位富余且整帧超页容量的 APPEND 必须整笔被拒，既有记录
/// 与后续小写入皆不受损
#[test]
fn append_beyond_page_ceiling_leaves_record_intact() {
  with_batch(|s, batch| {
    let key = b"cap-page";
    set(s, batch, key, b"1234567890");
    let usage_before = usage(s, batch, key);

    // 测试预算（16MB）推导的页容量远小于 4MB，整帧必超页
    let huge = vec![b'H'; 4 * 1024 * 1024];
    let mut out = Vec::new();
    let handled = s.network_append(&[key, &huge], batch, &mut out).unwrap();
    assert!(handled, "同步段须自行了断该请求，不得甩给异步闭环");
    assert_eq!(
      out,
      // 期望帧仍由单点常量派生（`write_resp_error` 对无大写前缀的裸消息统一
      // 冠以 `ERR `，与 `acl_tests.rs` 同一对拍口径）
      err_frame(&format!("ERR {RESP_ERR_GENERIC}")),
      "超页写入须整笔显式报错，绝不得报成功或静默截断"
    );

    assert_eq!(get(s, batch, key), b"1234567890", "被拒写入不得伤及旧值");
    assert_eq!(usage(s, batch, key), usage_before, "被拒写入不得换槽位");
    assert_eq!(append(s, batch, key, b"ZZ"), 12, "超限失败后该键仍可正常写");
    assert_eq!(get(s, batch, key), b"1234567890ZZ");
  });
}

/// SETBIT 原位改长 vs 尾部追加双路应答一致性回归（对标 APPEND/SETRANGE 同款双色路径）
#[test]
fn set_bit_in_place_and_tail_paths_agree() {
  with_batch(|s, batch| {
    let key_in_place = b"bit-in-place";
    let key_tail = b"bit-tail";

    // 播种槽位松弛富余：让 key_in_place 槽位足够容纳 40 字节
    set(s, batch, key_in_place, &[0u8; 40]);
    set(s, batch, key_in_place, &[0u8; 4]);
    let usage_in_place = usage(s, batch, key_in_place);

    // key_tail 紧凑分配，无松弛
    set(s, batch, key_tail, &[0u8; 4]);

    // 1. 在现有长度内置位（第 0 字节最高位，offset=0）
    assert_eq!(set_bit(s, batch, key_in_place, 0, 1), 0);
    assert_eq!(set_bit(s, batch, key_tail, 0, 1), 0);
    assert_eq!(get(s, batch, key_in_place), get(s, batch, key_tail));
    assert_eq!(
      usage(s, batch, key_in_place),
      usage_in_place,
      "原位改写未换槽位"
    );

    // 再次置同一位（验证原 bit 返回 1）
    assert_eq!(set_bit(s, batch, key_in_place, 0, 1), 1);
    assert_eq!(set_bit(s, batch, key_tail, 0, 1), 1);
    assert_eq!(get(s, batch, key_in_place), get(s, batch, key_tail));

    // 2. 原位增长（offset=100，需要 13 字节；松弛空间有 40 字节）
    // key_in_place 落在松弛内，走 InPlace 臂；key_tail 槽位不足，走 Fallback 尾部追加臂
    assert_eq!(set_bit(s, batch, key_in_place, 100, 1), 0);
    assert_eq!(set_bit(s, batch, key_tail, 100, 1), 0);
    assert_eq!(
      get(s, batch, key_in_place),
      get(s, batch, key_tail),
      "两臂结果值逐字节一致"
    );
    assert_eq!(
      usage(s, batch, key_in_place),
      usage_in_place,
      "原位改长未换槽位"
    );

    // 再次读取原 bit 验证为 1 并改写为 0
    assert_eq!(set_bit(s, batch, key_in_place, 100, 0), 1);
    assert_eq!(set_bit(s, batch, key_tail, 100, 0), 1);
    assert_eq!(get(s, batch, key_in_place), get(s, batch, key_tail));

    // 3. 超出松弛富余（offset=500，需要 63 字节 > 40 字节）：两路均回落全量尾部追加
    assert_eq!(set_bit(s, batch, key_in_place, 500, 1), 0);
    assert_eq!(set_bit(s, batch, key_tail, 500, 1), 0);
    assert_eq!(
      get(s, batch, key_in_place),
      get(s, batch, key_tail),
      "超松弛回落后两路一致"
    );
  });
}

/// SETRANGE 空写与间隙填零语义锁（不对齐 C# InPlace 臂 zeroInit:false 陈旧间隙缺陷，doc/zh/deviations.md §82）
#[test]
fn set_range_gap_zero_fill_empty_write_semantic_lock() {
  with_batch(|s, batch| {
    let k = b"k-emptyval-semantic-lock";
    set(s, batch, k, b"abc");

    // 1. SET k abc 后 SETRANGE k 10 "" 回 :10 且 GET k 逐字节等于 "abc" + 7 个 0x00
    assert_eq!(set_range(s, batch, k, 10, b""), 10);
    let mut expected10 = b"abc".to_vec();
    expected10.resize(10, 0); // "abc" + 7 个 0x00
    assert_eq!(
      get(s, batch, k),
      expected10,
      "SETRANGE k 10 \"\" 必须用 7 个 0x00 填充 [3..10) 间隙"
    );

    // 2. 衔接 SETRANGE k 11 x：offset 11 间隙补 1 字节 0x00（累计 8 个 0x00），回 :12
    //    GET k 逐字节等于 "abc" + 8 个 0x00 + "x"
    let k11 = b"k-emptyval-11";
    set(s, batch, k11, b"abc");
    assert_eq!(set_range(s, batch, k11, 10, b""), 10);
    assert_eq!(set_range(s, batch, k11, 11, b"x"), 12);
    let mut expected11 = b"abc".to_vec();
    expected11.resize(11, 0); // "abc" + 8 个 0x00
    expected11.push(b'x');
    assert_eq!(
      get(s, batch, k11),
      expected11,
      "SETRANGE k 11 x 必须包含 8 个 0x00 间隙"
    );

    // 3. SETRANGE k 12 x：offset 12 间隙补 2 字节 0x00（累计 9 个 0x00），回 :13
    //    GET k 逐字节等于 "abc" + 9 个 0x00 + "x"
    assert_eq!(set_range(s, batch, k, 12, b"x"), 13);
    let mut expected12 = b"abc".to_vec();
    expected12.resize(12, 0); // 7 个已有 0x00 + 2 个补零 = 9 个 0x00
    expected12.push(b'x');
    assert_eq!(
      get(s, batch, k),
      expected12,
      "SETRANGE k 12 x 必须包含 9 个 0x00 间隙且总长 13"
    );

    // 4. 原位臂与回退尾部臂在富余槽位（残留脏字节 'L'）上间隙填零一致性
    let in_place_key = b"k-emptyval-inplace";
    let slack_usage = seed_slack(s, batch, in_place_key, b"abc");
    assert_eq!(set_range(s, batch, in_place_key, 10, b""), 10);
    assert_eq!(
      usage(s, batch, in_place_key),
      slack_usage,
      "原位空写不改变物理槽位尺寸"
    );
    assert_eq!(
      get(s, batch, in_place_key),
      expected10,
      "原位臂空写间隙填零必须清除非零松弛残留"
    );
  });
}
