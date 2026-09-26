//! 双键装载臂取闩序收敛回归（票 zcode-r135c-lockorder 案一，P2）
//!
//! 缺陷形态：多键读改写窗口双机制并存双序对撞——`rmw_window_sorted` 臂按
//! 主桶下标升序取闩（对标 C# TxnKeyEntryComparison.cs:23-24 与
//! ListOps.cs:229-231 双键登记后经同一桶序整组排序的单序铁律），而
//! LMOVE/SMOVE 形装载型双键臂（`try_sync_rmw_window_pair` /
//! `rmw_window_pair_async`）曾自立第二套**字节字典序**取闩序。两序作用在
//! 同一物理桶闩（`try_lock_key_bucket`）上：键对 (a,b) 若
//! bucket(a)<bucket(b) 且字典序 b<a，则 RPOPLPUSH a b 按 (b,a) 取闩、
//! RENAME a b 按 (a,b) 取闩，交叉互持互逐→双双耗光有界预算空转重放；且
//! 同步旧臂第一窗在手、第二窗失手时以**半保护态继续执行**（部分持窗盲写，
//! 比风暴更重）。
//!
//! 修法落点（本回归锁定判据）：pair 臂并入桶升序单机制
//! （`try_rmw_window_sorted`/`rmw_window_sorted`，双键退化为两槽计划，同桶
//! 碰撞折叠自带去重），失闩整体 RAII 放闩（全有全无），字典序比较臂删除。
//!
//! 夹具：确定性冲突键对筛选（同 rmw_window_sorted.rs 的 active_index 直算
//! 桶号）+ 外部件桶闩钉住 + 真实双线程 RESP 并发交叉，无 mock 无 sleep。

use std::{
  collections::{HashMap, hash_map::Entry},
  sync::{Arc, Barrier},
  thread,
};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use whasher::scoped_hash;
use wkv::WedbStore;
use wnode::{
  RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::roundtrip;
use wtest_base::open_test_store;
use wval::SessionPrefixBuf;

/// 并发交叉轮数
const ROUNDS: usize = 40;

fn consumer_on(store: &Arc<WedbStore<SegmentedDevice>>) -> RespSessionConsumer {
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// 存储忙应答帧判定（闩预算耗尽的统一漏斗 `-ERR slow path storage error`）
fn is_busy(resp: &[u8]) -> bool {
  resp.starts_with(b"-ERR") && String::from_utf8_lossy(resp).contains("slow path storage error")
}

/// 冲突键对：`bucket(a) < bucket(b)` 且字节字典序 `b < a`——双序对撞的
/// 确定性命中形（`tag~hi` 高位前缀 / `tag!lo` 低位前缀保证字典序反向，
/// 桶下标由哈希定，扫至首个命中即返）。桶号按双键臂 scoped 口径寻桶
///（`whasher::scoped_hash` 前缀种子；测试消费会话为缺省根域）
fn find_conflict_pair(
  store: &Arc<WedbStore<SegmentedDevice>>,
  tag: &str,
) -> (Vec<u8>, Vec<u8>, usize, usize) {
  let index = store.active_index();
  for i in 0..100_000usize {
    let a = format!("{tag}~hi{i}").into_bytes();
    let b = format!("{tag}!lo{i}").into_bytes();
    let ba = index.bucket_index_for_hash(scoped_hash(SessionPrefixBuf::ROOT.as_slice(), &a));
    let bb = index.bucket_index_for_hash(scoped_hash(SessionPrefixBuf::ROOT.as_slice(), &b));
    if ba < bb {
      assert!(b < a, "夹具前缀字典序前提破坏");
      return (a, b, ba, bb);
    }
  }
  panic!("未能筛出桶序/字典序冲突键对");
}

/// 钉住指定桶排他闩（外部件持闩，判据窗口内该桶恒忙）
fn pin_bucket(store: &Arc<WedbStore<SegmentedDevice>>, bucket: usize) {
  assert!(
    store.active_index().bucket(bucket).try_lock_exclusive(),
    "夹具钉闩前提：目标桶须空闲"
  );
}

fn unpin_bucket(store: &Arc<WedbStore<SegmentedDevice>>, bucket: usize) {
  store.active_index().bucket(bucket).unlock_exclusive();
}

/// 桶是否空闲（试取即放，零驻留探针）
fn bucket_free(store: &Arc<WedbStore<SegmentedDevice>>, bucket: usize) -> bool {
  let index = store.active_index();
  if index.bucket(bucket).try_lock_exclusive() {
    index.bucket(bucket).unlock_exclusive();
    true
  } else {
    false
  }
}

/// 案一判据一（确定性）：钉住**桶序首槽**（字典序旧臂的尾闩）后，装载型
/// 双键臂必须整体失败让位——旧字典序臂先取字典序首窗（本夹具的尾桶）得逞、
/// 第二窗失手仍携**半保护窗继续执行**并写出元素应答；收敛桶升序单机制后
/// 全有全无：同步臂回 `Ok(false)` → 异步重放臂预算耗尽 → 存储忙应答，
/// 源键零变异、尾槽桶零残留持闩
#[test]
fn pair_arm_first_slot_latch_pin_fails_closed() {
  let (_dir, store) = open_test_store("pair-latch-pin.db").unwrap();
  let rt = Runtime::new().unwrap();

  // RPOPLPUSH（list LMOVE 形同步臂）
  let (a, b, ba, bb) = find_conflict_pair(&store, "mv");
  {
    let mut c = consumer_on(&store);
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"RPUSH", &a, b"e1"]),
      b":1\r\n",
      "list 种子键对不可同桶（夹具前提）"
    );
    assert_ne!(ba, bb);
    pin_bucket(&store, ba);
    let resp = roundtrip(&rt, &mut c, &[b"RPOPLPUSH", &a, &b]);
    assert!(
      is_busy(&resp),
      "桶序首槽被占时双键臂必整体忙拒（旧字典序臂半保护盲写即此炸出），实得 {:?}",
      String::from_utf8_lossy(&resp)
    );
    assert!(
      bucket_free(&store, bb),
      "全有全无：首槽失手后尾槽不得残留持闩（旧臂尾窗泄漏面）"
    );
    unpin_bucket(&store, ba);
    assert_eq!(roundtrip(&rt, &mut c, &[b"LLEN", &a]), b":1\r\n");
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"RPOPLPUSH", &a, &b]),
      b"$2\r\ne1\r\n"
    );
    assert_eq!(roundtrip(&rt, &mut c, &[b"LLEN", &b]), b":1\r\n");
  }

  // SMOVE（set 形同一 pair 臂消费位）
  let (a2, b2, ba2, _bb2) = find_conflict_pair(&store, "st");
  {
    let mut c = consumer_on(&store);
    assert_eq!(roundtrip(&rt, &mut c, &[b"SADD", &a2, b"m1"]), b":1\r\n");
    pin_bucket(&store, ba2);
    let resp = roundtrip(&rt, &mut c, &[b"SMOVE", &a2, &b2, b"m1"]);
    assert!(
      is_busy(&resp),
      "SMOVE 双键臂同款全有全无判据炸出，实得 {:?}",
      String::from_utf8_lossy(&resp)
    );
    assert_eq!(roundtrip(&rt, &mut c, &[b"SCARD", &a2]), b":1\r\n");
    unpin_bucket(&store, ba2);
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SMOVE", &a2, &b2, b"m1"]),
      b":1\r\n"
    );
  }
}

/// 案一判据二（真实并发）：冲突键对上 RPOPLPUSH a b × RENAME a b 双线程
/// 交叉——双序并存时两臂按互逆序取闩互持互逐，收口单机制后恒同序：任一
/// 时刻至多一方缺闩让位，绝无「双双耗光预算」的互逐轮；忙拒方无对手补放
/// 必收敛，终态恒落串行可达点（b 恰含 e1 且 a 消失）
#[test]
fn pair_arm_vs_sorted_arm_no_cross_eviction() {
  let (_dir, store) = open_test_store("pair-vs-sorted.db").unwrap();
  let rt = Runtime::new().unwrap();
  let (a, b, _ba, _bb) = find_conflict_pair(&store, "xv");

  for round in 0..ROUNDS {
    {
      let mut c = consumer_on(&store);
      roundtrip(&rt, &mut c, &[b"DEL", &a, &b]);
      assert_eq!(roundtrip(&rt, &mut c, &[b"RPUSH", &a, b"e1"]), b":1\r\n");
    }
    let gate = Arc::new(Barrier::new(2));
    let handles: Vec<_> = [false, true]
      .into_iter()
      .map(|is_rename| {
        let store = Arc::clone(&store);
        let gate = Arc::clone(&gate);
        let a = a.clone();
        let b = b.clone();
        thread::spawn(move || {
          let rt = Runtime::new().unwrap();
          let mut c = consumer_on(&store);
          gate.wait();
          if is_rename {
            roundtrip(&rt, &mut c, &[b"RENAME", &a, &b])
          } else {
            roundtrip(&rt, &mut c, &[b"RPOPLPUSH", &a, &b])
          }
        })
      })
      .collect();
    let (move_resp, rename_resp) = {
      let mut it = handles.into_iter().map(|h| h.join().unwrap());
      (it.next().unwrap(), it.next().unwrap())
    };
    let (move_busy, rename_busy) = (is_busy(&move_resp), is_busy(&rename_resp));
    assert!(
      !(move_busy && rename_busy),
      "第 {round} 轮：双键臂与 sorted 臂互逐同轮双忙（异序交叉残留即此形态）"
    );
    // 忙拒方补放：无对手机械后同命令必一次性成窗执行（有界重放即决）
    if move_busy || rename_busy {
      let mut c = consumer_on(&store);
      if move_busy {
        assert!(
          !is_busy(&roundtrip(&rt, &mut c, &[b"RPOPLPUSH", &a, &b])),
          "第 {round} 轮：无竞争补放 RPOPLPUSH 仍忙拒（预算收口破坏）"
        );
      }
      if rename_busy {
        assert!(
          !is_busy(&roundtrip(&rt, &mut c, &[b"RENAME", &a, &b])),
          "第 {round} 轮：无竞争补放 RENAME 仍忙拒（预算收口破坏）"
        );
      }
    }
    // 串行可达终态：无论何种序，e1 恒落 b 键且 a 消失
    let mut c = consumer_on(&store);
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"LLEN", &b]),
      b":1\r\n",
      "第 {round} 轮：交叠终态 b 必恰含 e1（部分写/覆写丢失即此炸出）"
    );
    assert_eq!(roundtrip(&rt, &mut c, &[b"LLEN", &a]), b":0\r\n");
  }
}

/// 案一判据三（同桶折叠）：同桶异名键对经 pair 臂取窗，由计划折叠为单闩
/// 直达执行（旧臂 `first == second` 仅判同键，异名同桶双取非重入闩必空转
/// 满预算后以半保护窗续行；新臂折叠去重后首槽即成）
#[test]
fn pair_arm_same_bucket_pair_folds_single_latch() {
  let (_dir, store) = open_test_store("pair-fold.db").unwrap();
  let rt = Runtime::new().unwrap();
  let index = store.active_index();
  let mut by_bucket = HashMap::<usize, Vec<u8>>::new();
  let mut pair = None;
  for i in 0..200_000usize {
    let x = format!("fd{i}").into_bytes();
    let bx = index.bucket_index_for_hash(scoped_hash(SessionPrefixBuf::ROOT.as_slice(), &x));
    match by_bucket.entry(bx) {
      Entry::Occupied(e) => {
        let y = e.get().clone();
        if y != x {
          pair = Some((x, y));
          break;
        }
      }
      Entry::Vacant(e) => {
        e.insert(x);
      }
    }
  }
  let (x, y) = pair.expect("夹具须筛出同桶异名键对");
  let mut c = consumer_on(&store);
  assert_eq!(roundtrip(&rt, &mut c, &[b"RPUSH", &x, b"e1"]), b":1\r\n");
  // 同桶双键：折叠单闩即成窗执行，无半保护/忙拒面
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"RPOPLPUSH", &x, &y]),
    b"$2\r\ne1\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"LLEN", &y]), b":1\r\n");
}
