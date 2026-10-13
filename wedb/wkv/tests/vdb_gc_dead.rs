#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 死亡号账本与偏序 GC 屏障集成测试
//!
//! 覆盖：死亡账本小根堆到期前缀与截断线屏障、注销后宽限期与 prune 清理、
//! 单轮弹出配额上限（cap）、宽限期秒数下限守卫（MIN_GRACE_DELAY_SECS 钳制）及跟随配置调大。

use wkv::{
  DEFAULT_DB_GC_RECLAIM_DELAY_SECS,
  vdb::{GcDeadEntry, GcDeadLog, reclaim_expired_at},
};

/// 死亡账本小根堆：sweep 只弹到期前缀，未到期与截断线未越界条目保留
#[test]
fn test_gc_dead_expiry_heap_prefix() {
  let log = GcDeadLog::new();
  let entry = |expired_at: i64, tail: u64| GcDeadEntry {
    expired_at,
    tail_address: tail,
    vns: None,
  };
  log.insert(1, entry(100, 0));
  log.insert(2, entry(200, 0));
  log.insert(3, entry(300, 0));
  log.insert(4, entry(250, 999)); // 到期但截断线未越界

  // 弹到 200 为止：250/300 未到期保留；100/200 已回收摘除
  let got = log.pop_reclaimable(200, 0, 256);
  assert_eq!(got, vec![(1, entry(100, 0)), (2, entry(200, 0))]);
  assert!(log.get(&1).is_none() && log.get(&2).is_none());
  assert_eq!(log.get(&3).map(|e| e.expired_at), Some(300));

  // 同一时刻重弹：无重复回收，账本不动
  assert!(log.pop_reclaimable(200, 0, 256).is_empty());

  // 截断线越界后 250 先回收，300 到期随后回收（堆序）
  let got = log.pop_reclaimable(300, 999, 256);
  assert_eq!(got, vec![(4, entry(250, 999)), (3, entry(300, 0))]);
  assert_eq!(log.len(), 0);
}

/// 注销后宽限期：离册条目在宽限期内依然判死，超期后自动清除
#[test]
fn test_gc_dead_grace_period() {
  let log = GcDeadLog::new();
  log.set_grace_delay_ticks(50); // 宽限 50 ticks
  let entry = GcDeadEntry {
    expired_at: 100,
    tail_address: 500,
    vns: Some(10), // 库级退役，归属 vns=10
  };
  log.insert(2, entry);

  // 在册期间：未被 pop_reclaimable 弹出
  assert!(!log.is_in_grace(10, 2, 100));

  // begin_address 越过 500，在 now=100 时弹出注销
  let got = log.pop_reclaimable(100, 500, 256);
  assert_eq!(got.len(), 1);
  assert!(log.get(&2).is_none(), "条目已从账本弹出");

  // 宽限期内（now=100..=150）：依然判死
  assert!(log.is_in_grace(10, 2, 100));
  assert!(log.is_in_grace(10, 2, 150));
  assert!(!log.is_in_grace(99, 2, 120), "归属 vns 不符不判死");
  assert!(!log.is_in_grace(10, 2, 151), "超出宽限期不再判死");

  // 下一轮 sweep 触发 prune 清理
  log.pop_reclaimable(160, 500, 256);
  assert!(!log.is_in_grace(10, 2, 160));

  // 回滚补偿 cancel：彻底清除宽限期
  log.insert(3, entry);
  log.remove(&3);
  assert!(log.is_in_grace(10, 3, 200));
  log.cancel(&3);
  assert!(!log.is_in_grace(10, 3, 200), "cancel 必须清除宽限期");
}

/// 单轮投递大于 cap 的到期项，单轮弹出注销数恰为 cap，余量留在堆中下轮续收
#[test]
fn test_gc_dead_pop_reclaimable_cap() {
  let log = GcDeadLog::new();
  let entry = |expired_at: i64| GcDeadEntry {
    expired_at,
    tail_address: 0,
    vns: None,
  };
  const CAP: usize = 3;
  // 投递 7 个已到期条目（> cap）
  for id in 1..=7 {
    log.insert(id, entry(100 + id as i64));
  }
  assert_eq!(log.len(), 7);

  // 第一轮弹出：由于 cap 限制，恰好弹出前 3 条（ID: 1, 2, 3）
  let first = log.pop_reclaimable(200, 0, CAP);
  assert_eq!(first.len(), CAP, "单轮弹出数恰为 cap");
  assert_eq!(
    first.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
    vec![1, 2, 3]
  );
  assert_eq!(log.len(), 4, "余量留在堆与账本中待下轮续收");
  for id in 1..=3 {
    assert!(log.get(&id).is_none());
  }
  for id in 4..=7 {
    assert!(log.get(&id).is_some());
  }

  // 第二轮续收：再弹出 cap=3 条（ID: 4, 5, 6）
  let second = log.pop_reclaimable(200, 0, CAP);
  assert_eq!(second.len(), CAP, "第二轮弹出数恰为 cap");
  assert_eq!(
    second.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
    vec![4, 5, 6]
  );
  assert_eq!(log.len(), 1, "余量 1 条仍留在堆中");

  // 第三轮续收：弹出剩余的最后 1 条（ID: 7）
  let third = log.pop_reclaimable(200, 0, CAP);
  assert_eq!(third.len(), 1);
  assert_eq!(third[0].0, 7);
  assert_eq!(log.len(), 0, "账本最终清零");
  assert!(log.is_empty());
}

/// 宽限期秒数下限守卫（max(60) 钳制与默认值单源验证）
#[test]
fn test_gc_dead_grace_delay_guard_and_defaults() {
  let log = GcDeadLog::new();
  assert_eq!(
    log.grace_delay_ticks(),
    reclaim_expired_at(0, DEFAULT_DB_GC_RECLAIM_DELAY_SECS),
    "初值应单点引自 DEFAULT_DB_GC_RECLAIM_DELAY_SECS"
  );

  // 传 secs = 0 时按 60 秒下限钳制
  log.set_grace_delay_secs(0);
  assert_eq!(
    log.grace_delay_ticks(),
    reclaim_expired_at(0, 60),
    "0 秒应被 max(60) 守卫钳制为 60 秒"
  );

  // 传 secs = 1 时同样按 60 秒下限钳制
  log.set_grace_delay_secs(1);
  assert_eq!(
    log.grace_delay_ticks(),
    reclaim_expired_at(0, 60),
    "1 秒应被 max(60) 守卫钳制为 60 秒"
  );

  // 传 secs = 120 时正常设置为 120 秒
  log.set_grace_delay_secs(120);
  assert_eq!(log.grace_delay_ticks(), reclaim_expired_at(0, 120));
}

/// 宽限期跟随配置调大（远大于 24h 时的存活时长自证）
#[test]
fn test_gc_dead_grace_delay_follow_config_greater_than_24h() {
  let log = GcDeadLog::new();
  const THIRTY_DAYS: u64 = 30 * DEFAULT_DB_GC_RECLAIM_DELAY_SECS;
  log.set_grace_delay_secs(THIRTY_DAYS);
  let entry = GcDeadEntry {
    expired_at: 100,
    tail_address: 0,
    vns: Some(10),
  };
  log.insert(2, entry);
  log.pop_reclaimable(100, 0, 256);

  let delay_ticks = reclaim_expired_at(0, THIRTY_DAYS);
  let day_ticks = reclaim_expired_at(0, DEFAULT_DB_GC_RECLAIM_DELAY_SECS);

  // 越过 24h 但在 30 天内：仍处于宽限期维持判死
  let past_24h = 100 + day_ticks + 10;
  assert!(
    log.is_in_grace(10, 2, past_24h),
    "越过 24h 但在 30 天内应维持判死，证明宽限期跟随配置而非写死 24h"
  );

  // 越过 30 天：超出宽限期不再判死，且 prune 清理
  let past_30d = 100 + delay_ticks + 1;
  assert!(!log.is_in_grace(10, 2, past_30d));
  log.pop_reclaimable(past_30d, 0, 256);
  assert!(!log.is_in_grace(10, 2, past_30d));
}
