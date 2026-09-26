//! 自研依据: doc/zh/db.md 死亡号账本与偏序 GC 屏障
use std::{
  cmp::Reverse,
  collections::BinaryHeap,
  sync::{
    Arc,
    atomic::{
      AtomicBool, AtomicI64, AtomicU64,
      Ordering::{Acquire, Release},
    },
  },
};

use parking_lot::Mutex;
use wbase::{
  convert::expire_after_to_ticks,
  map::{ConcurrentMap, new_concurrent_map},
  time::now_ticks,
};

use super::routing::DbRoutingTable;
use crate::config::DEFAULT_DB_GC_RECLAIM_DELAY_SECS;

/// 换号旧域/旧空间的回收截止 ticks：`now_ticks + db_gc_reclaim_delay_secs` 秒
///
/// 秒→tick 一律走 [`expire_after_to_ticks`] 单点（时长换算与饱和加法均在其中），
/// 严禁在存储层裸乘刻度字面量或在 u64 域内乘完再 `as i64` 静默收窄（debug 构建
/// 溢出 panic、release 构建环绕成过去时刻）。配置秒数超出 i64 值域时钳到
/// i64::MAX = 截止永不到期：宁可旧域滞留磁盘，也不透支「严防幽灵读取」的
/// 安全纪元窗（提前回收才是数据可见性事故）
#[inline]
pub(crate) fn reclaim_expired_at(now_ticks: i64, delay_secs: u64) -> i64 {
  expire_after_to_ticks(now_ticks, i64::try_from(delay_secs).unwrap_or(i64::MAX))
}

/// 待回收死亡虚拟 ID 项
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcDeadEntry {
  pub expired_at: i64,
  pub tail_address: u64,
  /// 库级换号时记录所属虚拟命名空间 ID (vns)；命名空间换号时为 None
  pub vns: Option<u64>,
}

/// 处于注销后宽限期的死亡虚拟 ID 项（紧缩兜底判死）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcDeadGraceEntry {
  pub grace_until: i64,
  pub vns: Option<u64>,
}

/// 租户路由快照（槽位级单元格路由表 + 会话引用计数）
///
/// `refs` 为绑定到本租户的活跃会话数：绑定协议（[`VirtualDbManager::bind_route`]）
/// 保证持引用期间快照不被空闲析构摘除，同步读路径因此永远命中在册快照、
/// 绝不盲分配换号；引用归零后快照进入空闲析构候选，由 GC 轮次摘除释放，
/// 后续访问经磁盘 DbMeta 点查装载回建（doc/zh/db.md 冷租户条款）
pub struct TenantRouting {
  pub table: DbRoutingTable,
  /// 绑定会话数（0 = 空闲可析构）
  pub(super) refs: AtomicU64,
  /// 本运行期权威全量标志：新建租户（ns 首次映射）置位——其路由表由本运行
  /// 期逐库持久化维护，表内缺库即真新库，可同步分配；磁盘装载回建的租户
  /// 恒为否（表仅部分装载，缺库须经点查甄别冷库与真新库）
  pub(super) authoritative: AtomicBool,
}

impl TenantRouting {
  #[inline]
  pub fn new(table: DbRoutingTable) -> Arc<Self> {
    Arc::new(Self {
      table,
      refs: AtomicU64::new(0),
      authoritative: AtomicBool::new(false),
    })
  }

  #[inline]
  pub(super) fn refs(&self) -> u64 {
    self.refs.load(Acquire)
  }
}

/// 死亡虚拟 ID 待回收账本（O(1) 点查判死 + 按到期时间排序的小根堆索引 + 注销后宽限期兜底）
///
/// 全量历史墓碑留存底层磁盘（KeyTag::DbMeta），内存账本仅持有未回收项；
/// `map` 承载点查判死（Compaction 顺带丢弃与 keyspace 统计过滤），
/// `expiry_heap` 为按 `expired_at` 升序的惰性索引——sweep 只弹到期前缀，
/// 彻底消除按租户数放大的每轮全表遍历（doc/zh/db.md「内存仅加载近期
/// 即将到期的小根堆」）。堆条目不随删除同步移除，弹出时以 `map` 最新态
/// 校验失效；同 vid 无重用号空间，账本条目唯一。
/// 离册条目转入 `grace_map` 维持注销后宽限期，防在途孤儿高位滞留后判活回拷。
pub struct GcDeadLog {
  map: ConcurrentMap<u64, GcDeadEntry>,
  expiry_heap: Mutex<BinaryHeap<Reverse<(i64, u64)>>>,
  grace_map: ConcurrentMap<u64, GcDeadGraceEntry>,
  grace_heap: Mutex<BinaryHeap<Reverse<(i64, u64)>>>,
  grace_delay_ticks: AtomicI64,
}

impl GcDeadLog {
  #[inline]
  pub(super) fn new() -> Self {
    Self {
      map: new_concurrent_map(),
      expiry_heap: Mutex::new(BinaryHeap::new()),
      grace_map: new_concurrent_map(),
      grace_heap: Mutex::new(BinaryHeap::new()),
      grace_delay_ticks: AtomicI64::new(reclaim_expired_at(0, DEFAULT_DB_GC_RECLAIM_DELAY_SECS)),
    }
  }

  /// 设置注销后宽限期时长（秒）
  #[inline]
  pub fn set_grace_delay_secs(&self, secs: u64) {
    let ticks = reclaim_expired_at(0, secs.max(60));
    self.grace_delay_ticks.store(ticks, Release);
  }

  /// 设置注销后宽限期精确 ticks（供单测使用）
  #[cfg(test)]
  #[inline]
  pub(crate) fn set_grace_delay_ticks(&self, ticks: i64) {
    self.grace_delay_ticks.store(ticks, Release);
  }

  /// 登记死亡项（map + 到期堆索引）
  #[inline]
  pub fn insert(&self, vid: u64, entry: GcDeadEntry) {
    self.map.pin().insert(vid, entry);
    self
      .expiry_heap
      .lock()
      .push(Reverse((entry.expired_at, vid)));
  }

  /// 摘除死亡项（注销进入宽限期；堆索引惰性失效，弹出时校验）
  #[inline]
  pub fn remove(&self, vid: &u64) {
    if let Some(entry) = self.map.pin().remove(vid) {
      let delay = self.grace_delay_ticks.load(Acquire);
      let now = now_ticks();
      let grace_until = now.saturating_add(delay);
      self.grace_map.pin().insert(
        *vid,
        GcDeadGraceEntry {
          grace_until,
          vns: entry.vns,
        },
      );
      self.grace_heap.lock().push(Reverse((grace_until, *vid)));
    }
  }

  /// 撤销死亡项（回滚补偿专属：彻底清除账本与宽限期，恢复为活域）
  #[inline]
  pub fn cancel(&self, vid: &u64) {
    self.map.pin().remove(vid);
    self.grace_map.pin().remove(vid);
  }

  /// 点查死亡项
  #[inline]
  pub fn get(&self, vid: &u64) -> Option<GcDeadEntry> {
    self.map.pin().get(vid).copied()
  }

  /// 检查指定虚拟 ID 是否处于注销后宽限期内（紧缩兜底判死）
  #[inline]
  pub fn is_in_grace(&self, vns: u64, vdb: u64, now: i64) -> bool {
    let pin = self.grace_map.pin();
    if let Some(entry) = pin.get(&vdb)
      && entry.vns.is_some_and(|owner| owner == vns)
      && now <= entry.grace_until
    {
      return true;
    }
    if let Some(entry) = pin.get(&vns)
      && entry.vns.is_none()
      && now <= entry.grace_until
    {
      return true;
    }
    false
  }

  /// 清理已超宽限期的陈旧项（O(log K) 堆扫描）
  fn prune_expired_grace(&self, now: i64) {
    let mut expired = Vec::new();
    {
      let mut heap = self.grace_heap.lock();
      while let Some(Reverse((grace_until, vid))) = heap.peek().copied() {
        if grace_until > now {
          break;
        }
        heap.pop();
        expired.push(vid);
      }
    }
    if !expired.is_empty() {
      let pin = self.grace_map.pin();
      for vid in expired {
        if let Some(entry) = pin.get(&vid)
          && entry.grace_until <= now
        {
          pin.remove(&vid);
        }
      }
    }
  }

  /// 积压长度（高低水位熔断口径）
  #[inline]
  pub fn len(&self) -> usize {
    self.map.pin().len()
  }

  /// 积压是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }

  /// 清空（超管物理截断重置）
  #[inline]
  pub fn clear(&self) {
    self.map.pin().clear();
    self.expiry_heap.lock().clear();
    self.grace_map.pin().clear();
    self.grace_heap.lock().clear();
  }

  /// 弹出「已到期且日志截断线已越过屏障排尾安全水位」的回收前缀（至多弹出 `cap` 条）
  ///
  /// 只沿小根堆弹出到期条目（`expired_at <= now`），达到配额上限 `cap` 或未到期即停——单轮
  /// 扫描成本受 `cap` 有界约束，未弹出条目原样留堆待下轮续收；到期但 `begin_address`
  /// 尚未越过安全尾水位 `tail_address` 的条目暂不入回收集，待紧缩推进后下轮重弹
  /// （幂等）；堆中已被回收/撤销的陈旧索引条目直接丢弃。
  /// 弹出注销项自动转入宽限期映射（[`Self::is_in_grace`]），兜底紧缩判死。
  ///
  /// 锁面短临界批量解耦（doc/zh/db.md 换号零锁口径的 GC 侧对表）：锁内只
  /// 摘到期前缀入局部栈，账本双检、物理摘除与暂扣回插全在锁外执行——后台
  /// sweep 长循环不再阻塞换号路径的墓碑登记（[`Self::insert`]），杜绝并发
  /// FLUSHDB 在 sweep 窗口的等锁尖刺
  pub fn pop_reclaimable(&self, now: i64, begin_addr: u64, cap: usize) -> Vec<(u64, GcDeadEntry)> {
    // 锁内短临界：仅按堆序摘出到期前缀（纯堆操作，无账本访问）
    let mut due: Vec<(i64, u64)> = {
      let mut heap = self.expiry_heap.lock();
      let mut due = Vec::with_capacity(cap.min(heap.len()));
      while let Some(Reverse((expired_at, vid))) = heap.peek().copied() {
        if due.len() >= cap || expired_at > now {
          break;
        }
        heap.pop();
        due.push((expired_at, vid));
      }
      due
    };
    // 锁外甄别回收：账本双检、物理摘除与暂扣分类；暂扣项统一回插等紧缩推进
    let pin = self.map.pin();
    let mut reclaimed = Vec::with_capacity(due.len());
    let mut deferred = Vec::new();
    let delay = self.grace_delay_ticks.load(Acquire);
    let grace_until = now.saturating_add(delay);
    for (expired_at, vid) in due.drain(..) {
      match pin.get(&vid).copied() {
        // 陈旧索引（已回收 / 重建期墓碑撤销）：丢弃
        None => {}
        Some(entry) if entry.expired_at == expired_at => {
          if entry.tail_address <= begin_addr {
            pin.remove(&vid);
            self.grace_map.pin().insert(
              vid,
              GcDeadGraceEntry {
                grace_until,
                vns: entry.vns,
              },
            );
            self.grace_heap.lock().push(Reverse((grace_until, vid)));
            reclaimed.push((vid, entry));
          } else {
            // 截断线未越界：暂扣，收集完后统一回插（等紧缩推进，下轮重弹）
            deferred.push(Reverse((expired_at, vid)));
          }
        }
        // 键存活但到期时间与索引不符（防御性丢弃陈旧索引）
        Some(_) => {}
      }
    }
    if !deferred.is_empty() {
      self.expiry_heap.lock().extend(deferred);
    }
    self.prune_expired_grace(now);
    reclaimed
  }
}

#[cfg(test)]
mod tests {
  use super::*;

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
      log.grace_delay_ticks.load(Acquire),
      reclaim_expired_at(0, DEFAULT_DB_GC_RECLAIM_DELAY_SECS),
      "初值应单点引自 DEFAULT_DB_GC_RECLAIM_DELAY_SECS"
    );

    // 传 secs = 0 时按 60 秒下限钳制
    log.set_grace_delay_secs(0);
    assert_eq!(
      log.grace_delay_ticks.load(Acquire),
      reclaim_expired_at(0, 60),
      "0 秒应被 max(60) 守卫钳制为 60 秒"
    );

    // 传 secs = 1 时同样按 60 秒下限钳制
    log.set_grace_delay_secs(1);
    assert_eq!(
      log.grace_delay_ticks.load(Acquire),
      reclaim_expired_at(0, 60),
      "1 秒应被 max(60) 守卫钳制为 60 秒"
    );

    // 传 secs = 120 时正常设置为 120 秒
    log.set_grace_delay_secs(120);
    assert_eq!(
      log.grace_delay_ticks.load(Acquire),
      reclaim_expired_at(0, 120)
    );
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
}
