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

/// 宽限期秒数下限守卫（60 秒）
pub const MIN_GRACE_DELAY_SECS: u64 = 60;

/// 换号旧域/旧空间的回收截止 ticks：`now_ticks + db_gc_reclaim_delay_secs` 秒
///
/// 秒→tick 一律走 [`expire_after_to_ticks`] 单点（时长换算与饱和加法均在其中），
/// 严禁在存储层裸乘刻度字面量或在 u64 域内乘完再 `as i64` 静默收窄（debug 构建
/// 溢出 panic、release 构建环绕成过去时刻）。配置秒数超出 i64 值域时钳到
/// i64::MAX = 截止永不到期：宁可旧域滞留磁盘，也不透支「严防幽灵读取」的
/// 安全纪元窗（提前回收才是数据可见性事故）
#[doc(hidden)]
#[inline]
pub fn reclaim_expired_at(now_ticks: i64, delay_secs: u64) -> i64 {
  expire_after_to_ticks(now_ticks, i64::try_from(delay_secs).unwrap_or(i64::MAX))
}

/// 待回收死亡虚拟 ID 项
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GcDeadEntry {
  pub expired_at: i64,
  pub tail_address: u64,
  /// 库级换号时记录所属虚拟命名空间 ID (vns)；命名空间换号时为 None
  pub vns: Option<u64>,
}

/// 处于注销后宽限期的死亡虚拟 ID 项（紧缩兜底判死）
#[derive(Clone)]
pub(crate) struct GcDeadGraceEntry {
  pub grace_until: i64,
  pub vns: Option<u64>,
}

/// 租户路由快照（槽位级单元格路由表 + 会话引用计数）
///
/// `refs` 为绑定到本租户的活跃会话数：绑定协议（[`crate::vdb::VirtualDbManager::bind_route`]）
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

impl Default for GcDeadLog {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl GcDeadLog {
  #[inline]
  pub fn new() -> Self {
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
    let ticks = reclaim_expired_at(0, secs.max(MIN_GRACE_DELAY_SECS));
    self.grace_delay_ticks.store(ticks, Release);
  }

  /// 设置注销后宽限期精确 ticks（供集成测试使用）
  #[doc(hidden)]
  #[inline]
  pub fn set_grace_delay_ticks(&self, ticks: i64) {
    self.grace_delay_ticks.store(ticks, Release);
  }

  /// 获取当前注销后宽限期精确 ticks（供集成测试使用）
  #[doc(hidden)]
  #[inline]
  pub fn grace_delay_ticks(&self) -> i64 {
    self.grace_delay_ticks.load(Acquire)
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
