//! 分层 List 升阶导出必须经 member_ttl 唯一 codec（task/ing/wcol-list-promote-export-bypasses-member-ttl-codec-byte-loss）
//!
//! 判据源＝本仓单点 codec 契约自身（`wcol/src/types/member_ttl.rs` 模块头 +
//! `wnode/src/resp/objects/tiered_collection_ops/list.rs:39-43`「升阶与重灌同源……
//! 杜绝第二形态」）：树成员记录首字节恒为形态旗标（`0` = 裸载荷），读侧
//! `decode_member` 一律按旗标剥 1B/9B。`ListObject::export_entries`
//! （wcol/src/types/garnet_object.rs）若裸 `val.clone()` 导出，则升阶灌入的
//! 记录**无旗标**，与树内写臂（`common.rs tree_put` → `encode_member_into`）的
//! 有旗标记录混排同一棵树 ⇒ 同一 codec 读口把载荷首字节当旗标解释：
//!
//! - 首字节 `0x00` 的元素被剥 1B，且每轮「物化→重灌」再剥 1B（载荷逐轮衰减）；
//! - 首字节 `0x01` 且长度 ≥9 的元素被剥 9B，载荷第 2-9 字节被读成 .NET Ticks
//!   假刻度，经 `earliest_expiry`（common.rs:211）写进 `meta.next_expiry`，
//!   后续到期出账据此整批误删真实成员；
//! - 其余首字节走宽松臂原样放行 ⇒ ASCII 载荷全绿，只有二进制载荷踩雷（静默损坏）。
//!
//! 本载体因此以三类首字节元素（`0x00` / `0x01` 长 / ASCII）构造 List，经**真实
//! 自动升阶口**（体积维跨 `wcol::TIERED_PROMOTE_BYTES`，由 `apply_rmw_post_operate`
//! → `obj.should_promote()` → `export_entries` 驱动，零手工灌树）落树，再经
//! LPOP 穿透物化 → `apply_rmw_post_operate` 重灌臂两轮，逐轮断言逐字节等值。

use std::{mem::take, str::from_utf8, sync::Arc};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wresp::command::RespCommand;
use wval::MetaValue;

/// 元素数门限（65536）太远，本载体走体积维升阶：填充元素 1000B（树内记录
/// = 16B 序号键 + 1B 旗标 + 1000B 载荷 = 1017B，仍在集合升阶树
/// `TreeTuning::DEFAULT_RI_COLLECTION` 的 1024B 记录上限内）
const FILLER_LEN: usize = 1000;

/// `wbase::heap` 每条目记账口径：`round_up_ptr(len) + SLOT * 2`
const PER_ELEMENT_BYTES: usize = FILLER_LEN + 32;

/// 跨 4MB 体积门限所需的填充元素数（含余量，令升阶后两轮回灌仍留在迟滞死区之上）
fn filler_count() -> usize {
  wcol::TIERED_PROMOTE_BYTES / PER_ELEMENT_BYTES + 250
}

/// 首字节 `0x00` 的二进制元素（缺陷形下每轮「物化→重灌」剥 1 字节）
fn zero_element() -> Vec<u8> {
  let mut v = vec![0x00u8];
  v.extend_from_slice(b"zero-tail-marker");
  while v.len() < 64 {
    v.push(b'z');
  }
  v
}

/// 首字节 `0x01`、长度 ≥9 的二进制元素：其后 8 字节全 `0xFF` ⇒ 缺陷形下被
/// `decode_member` 读成 `ticks = -1` 的假过期刻度（必早于 `now_ticks()`，
/// 直接污染 `meta.next_expiry`）
fn expiry_shaped_element() -> Vec<u8> {
  let mut v = vec![0x01u8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
  v.extend_from_slice(b"one-tail-marker");
  while v.len() < 64 {
    v.push(b'y');
  }
  v
}

/// ASCII 元素（宽松臂放行，对照组：任何情况下都应等值）
fn ascii_element() -> Vec<u8> {
  b"plain-ascii-element".to_vec()
}

/// 头端垫块：LPUSH 进已升阶的树（走树内写臂 `tree_put`），供 LPOP 摘除——
/// LPOP 无树内臂，穿透即触发「物化 → 重灌」整轮（本载体第二轮的驱动方式）
fn pad_element() -> Vec<u8> {
  b"pad-head-block".to_vec()
}

struct Env {
  rt: Runtime,
  store: Arc<WedbStore<SegmentedDevice>>,
  api: GarnetApi,
  _dir: tempfile::TempDir,
}

fn env(tag: &str) -> Env {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  Env {
    rt: Runtime::new().unwrap(),
    store: store.clone(),
    api: Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
    _dir: dir,
  }
}

fn session_with(env: &Env) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(env.api.clone());
  s
}

fn exec(env: &Env, s: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  s.output.clear();
  env.api.exec(s, cmd, args);
  if !s.output.is_empty() {
    return take(&mut s.output);
  }
  let slow = s
    .take_slow_wait()
    .unwrap_or_else(|| panic!("命令 {cmd} 无输出且未挂起慢路径"));
  let out = env.rt.block_on(slow.resolve());
  s.output.clear();
  out
}

/// 键是否处于 wbftree 分层态（升阶/重灌发生的判据），顺带交出元记录
fn tiered_meta(env: &Env, key: &[u8]) -> Option<MetaValue> {
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .map(|(meta, _)| meta)
}

/// 严格解析 RESP2 批量字符串数组应答为字节元素清单（帧长与实体逐字节对账，
/// 长度漂移/缺 NUL 一律显式失败，杜绝「解析器宽容」吞掉剥字节证据）
fn parse_bulk_array(frame: &[u8]) -> Vec<Vec<u8>> {
  let mut items = Vec::new();
  let mut i = 0usize;
  let line_end = |buf: &[u8], from: usize| -> usize {
    buf[from..]
      .windows(2)
      .position(|w| w == b"\r\n")
      .map(|p| from + p)
      .expect("RESP 行终止符缺失")
  };
  assert_eq!(&frame[0..1], b"*", "应答应为数组帧，实得 {frame:?}");
  let mut n: usize = 0;
  let mut first = true;
  while i < frame.len() {
    let e = line_end(frame, i);
    let head = &frame[i..e];
    i = e + 2;
    if first {
      assert_eq!(&head[0..1], b"*");
      n = from_utf8(&head[1..]).unwrap().parse().unwrap();
      first = false;
      continue;
    }
    assert_eq!(&head[0..1], b"$", "数组第 {} 项非批量字符串帧", items.len());
    let len: usize = from_utf8(&head[1..]).unwrap().parse().unwrap();
    assert!(
      i + len + 2 <= frame.len(),
      "帧承诺 {len} 字节而实体只剩 {} 字节",
      frame.len() - i - 2
    );
    assert_eq!(&frame[i + len..i + len + 2], b"\r\n");
    items.push(frame[i..i + len].to_vec());
    i += len + 2;
  }
  assert_eq!(items.len(), n, "帧头承诺 {n} 项而实得 {} 项", items.len());
  items
}

fn lrange(env: &Env, s: &mut RespServerSession, key: &[u8], start: i64, stop: i64) -> Vec<Vec<u8>> {
  let a = start.to_string();
  let b = stop.to_string();
  let frame = exec(
    env,
    s,
    RespCommand::Lrange,
    &[key, a.as_bytes(), b.as_bytes()],
  );
  parse_bulk_array(&frame)
}

fn lindex(env: &Env, s: &mut RespServerSession, key: &[u8], idx: i64) -> Option<Vec<u8>> {
  let a = idx.to_string();
  let frame = exec(env, s, RespCommand::Lindex, &[key, a.as_bytes()]);
  if frame.starts_with(b"$-1") {
    return None;
  }
  assert_eq!(&frame[0..1], b"$", "LINDEX 应答非批量字符串帧：{frame:?}");
  let e = frame[1..]
    .windows(2)
    .position(|w| w == b"\r\n")
    .map(|p| 1 + p)
    .unwrap();
  let len: usize = from_utf8(&frame[1..e]).unwrap().parse().unwrap();
  assert_eq!(&frame[e + 2 + len..], b"\r\n");
  Some(frame[e + 2..e + 2 + len].to_vec())
}

fn llen(env: &Env, s: &mut RespServerSession, key: &[u8]) -> i64 {
  let frame = exec(env, s, RespCommand::Llen, &[key]);
  assert_eq!(&frame[0..1], b":");
  from_utf8(&frame[1..frame.len() - 2])
    .unwrap()
    .parse()
    .unwrap()
}

/// 三条断言逐元素复核（(a) 逐字节等值 / (c) 无假刻度由调用方另断）
fn assert_elements(env: &Env, s: &mut RespServerSession, expect: &[&[u8]], round: &str) {
  let got = lrange(env, s, b"binlist", 0, expect.len() as i64 - 1);
  assert_eq!(
    got.len(),
    expect.len(),
    "{round}: LRANGE 条数背离（成员丢失？）got={got:?}"
  );
  for (i, want) in expect.iter().enumerate() {
    assert_eq!(
      got[i],
      *want,
      "{round}: LRANGE 第 {i} 个元素（首字节 0x{:02X}，应长 {}）字节背离，实长 {}",
      want[0],
      want.len(),
      got[i].len()
    );
  }
  for (i, want) in expect.iter().enumerate() {
    assert_eq!(
      lindex(env, s, b"binlist", i as i64),
      Some(want.to_vec()),
      "{round}: LINDEX {i} 字节背离"
    );
  }
}

#[test]
fn tiered_list_promote_export_keeps_member_codec_frame() {
  let env = env("tiered-list-export-codec.db");
  let mut s = session_with(&env);

  let zero = zero_element();
  let shaped = expiry_shaped_element();
  let ascii = ascii_element();
  assert_eq!(zero[0], 0x00);
  assert_eq!(shaped[0], 0x01);
  assert!(shaped.len() >= 9);

  // 1. 先以三类元素入**内存信封**（务必让升阶那一刻由 `export_entries` 导出它们：
  //    升阶后再 LPUSH 进树只走树内写臂 `tree_put`（本就编码正确），会掩盖本缺陷）
  exec(
    &env,
    &mut s,
    RespCommand::Rpush,
    &[b"binlist", &zero, &shaped, &ascii],
  );
  let pad = pad_element();
  exec(&env, &mut s, RespCommand::Lpush, &[b"binlist", &pad]);

  // 2. 真实自动升阶：跨体积门限（RPUSH 收尾 apply_rmw_post_operate →
  //    obj.should_promote() → export_entries → promote_collection_to_bftree）
  let total = filler_count();
  let filler = vec![b'v'; FILLER_LEN];
  let mut pushed = 0usize;
  while pushed < total {
    let chunk = (total - pushed).min(500);
    let mut args: Vec<&[u8]> = Vec::with_capacity(chunk + 1);
    args.push(b"binlist");
    for _ in 0..chunk {
      args.push(&filler);
    }
    exec(&env, &mut s, RespCommand::Rpush, &args);
    pushed += chunk;
  }
  let meta = tiered_meta(&env, b"binlist").expect("升阶后键应处于 wbftree 分层态");
  assert_eq!(
    meta.size,
    (total + 4) as u64,
    "跨 {}/{PER_ELEMENT_BYTES} 条目（体积维 ≥ {}B）应已自动升阶为分层树",
    total,
    wcol::TIERED_PROMOTE_BYTES
  );

  // (a) 升阶后（导出臂产物）逐字节等值
  let promoted: Vec<&[u8]> = vec![&pad, &zero, &shaped, &ascii];
  assert_elements(&env, &mut s, &promoted, "升阶后");
  assert_eq!(llen(&env, &mut s, b"binlist"), (total + 4) as i64);

  // (c) List 恒无成员级 TTL：`0x01` 长元素不得被读成带刻度记录
  //     （`earliest_expiry` 一旦把载荷字节当刻度，meta.next_expiry 即离 MAX）
  assert_eq!(
    meta.next_expiry,
    i64::MAX,
    "升阶后水位必须为无刻度（i64::MAX），实得 {}——0x01 首字节载荷被 decode_member 读成假 TTL",
    meta.next_expiry
  );

  // 3. 第一轮「物化→重灌」：LPOP 无树内臂 ⇒ 穿透物化后 apply_rmw_post_operate
  //    以 export_entries 整树重灌（列表仍远高于降阶死区，故必为重灌而非降阶）
  let expect: Vec<&[u8]> = vec![&zero, &shaped, &ascii];
  assert_eq!(
    parse_bulk_array_one(&exec(&env, &mut s, RespCommand::Lpop, &[b"binlist"])),
    pad,
    "LPOP 应弹回头端垫块"
  );
  let meta = tiered_meta(&env, b"binlist").expect("重灌后仍处分层态（未跌回降阶死区）");
  assert_eq!(meta.size, (total + 3) as u64, "重灌后 size 应守恒");
  assert_elements(&env, &mut s, &expect, "第 1 轮物化→重灌后");
  assert_eq!(
    meta.next_expiry,
    i64::MAX,
    "第 1 轮重灌后水位仍须无刻度，实得 {}",
    meta.next_expiry
  );

  // 4. 第二轮「物化→重灌」：缺陷形态下逐轮再剥 1B/9B（载荷持续衰减直至消失）
  exec(&env, &mut s, RespCommand::Lpush, &[b"binlist", &pad]);
  assert_eq!(
    parse_bulk_array_one(&exec(&env, &mut s, RespCommand::Lpop, &[b"binlist"])),
    pad,
    "第二次 LPOP 应弹回垫块"
  );
  let meta = tiered_meta(&env, b"binlist").expect("二轮重灌后仍处分层态");
  assert_eq!(meta.size, (total + 3) as u64, "二轮重灌后 size 应守恒");
  assert_elements(&env, &mut s, &expect, "第 2 轮物化→重灌后");
  assert_eq!(
    meta.next_expiry,
    i64::MAX,
    "第 2 轮重灌后水位仍须无刻度，实得 {}",
    meta.next_expiry
  );
}

/// 单元素批量字符串应答（LPOP 返回形）
fn parse_bulk_array_one(frame: &[u8]) -> Vec<u8> {
  assert_eq!(&frame[0..1], b"$", "LPOP 应答非批量字符串帧：{frame:?}");
  let e = frame[1..]
    .windows(2)
    .position(|w| w == b"\r\n")
    .map(|p| 1 + p)
    .unwrap();
  let len: usize = from_utf8(&frame[1..e]).unwrap().parse().unwrap();
  assert_eq!(&frame[e + 2 + len..], b"\r\n");
  frame[e + 2..e + 2 + len].to_vec()
}
