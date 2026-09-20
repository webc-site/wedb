use std::{
  cmp::Reverse,
  collections::BinaryHeap,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering::Acquire},
  },
};

use parking_lot::Mutex;
use wbase::{
  convert::expire_after_to_ticks,
  map::{ConcurrentMap, new_concurrent_map},
};

use super::routing::DbRoutingTable;

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

/// 死亡虚拟 ID 待回收账本（O(1) 点查判死 + 按到期时间排序的小根堆索引）
///
/// 全量历史墓碑留存底层磁盘（KeyTag::DbMeta），内存账本仅持有未回收项；
/// `map` 承载点查判死（Compaction 顺带丢弃与 keyspace 统计过滤），
/// `expiry_heap` 为按 `expired_at` 升序的惰性索引——sweep 只弹到期前缀，
/// 彻底消除按租户数放大的每轮全表遍历（doc/zh/db.md「内存仅加载近期
/// 即将到期的小根堆」）。堆条目不随删除同步移除，弹出时以 `map` 最新态
/// 校验失效；同 vid 无重用号空间，账本条目唯一
pub struct GcDeadLog {
  map: ConcurrentMap<u64, GcDeadEntry>,
  expiry_heap: Mutex<BinaryHeap<Reverse<(i64, u64)>>>,
}

impl GcDeadLog {
  #[inline]
  pub(super) fn new() -> Self {
    Self {
      map: new_concurrent_map(),
      expiry_heap: Mutex::new(BinaryHeap::new()),
    }
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

  /// 摘除死亡项（重建期墓碑回放撤销；堆索引惰性失效，弹出时校验）
  #[inline]
  pub fn remove(&self, vid: &u64) {
    self.map.pin().remove(vid);
  }

  /// 点查死亡项
  #[inline]
  pub fn get(&self, vid: &u64) -> Option<GcDeadEntry> {
    self.map.pin().get(vid).copied()
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
  }

  /// 弹出「已到期且日志截断线已越过生前写入点」的回收前缀
  ///
  /// 只沿小根堆弹出到期条目（`expired_at <= now`），未到期即停——单轮
  /// 扫描成本与到期前缀成正比，与账本总量无关。到期但 `begin_address`
  /// 尚未越过 `tail_address` 的条目暂不入回收集，待紧缩推进后下轮重弹
  /// （幂等）；堆中已被回收/撤销的陈旧索引条目直接丢弃。
  ///
  /// 锁面短临界批量解耦（doc/zh/db.md 换号零锁口径的 GC 侧对表）：锁内只
  /// 摘到期前缀入局部栈，账本双检、物理摘除与暂扣回插全在锁外执行——后台
  /// sweep 长循环不再阻塞换号路径的墓碑登记（[`Self::insert`]），杜绝并发
  /// FLUSHDB 在 sweep 窗口的等锁尖刺
  pub fn pop_reclaimable(&self, now: i64, begin_addr: u64) -> Vec<(u64, GcDeadEntry)> {
    // 锁内短临界：仅按堆序摘出到期前缀（纯堆操作，无账本访问）
    let mut due: Vec<(i64, u64)> = {
      let mut heap = self.expiry_heap.lock();
      let mut due = Vec::new();
      while let Some(Reverse((expired_at, vid))) = heap.peek().copied() {
        if expired_at > now {
          break;
        }
        heap.pop();
        due.push((expired_at, vid));
      }
      due
    };
    // 锁外甄别回收：账本双检、物理摘除与暂扣分类；暂扣项统一回插等紧缩推进
    let pin = self.map.pin();
    let mut reclaimed = Vec::new();
    let mut deferred = Vec::new();
    for (expired_at, vid) in due.drain(..) {
      match pin.get(&vid).copied() {
        // 陈旧索引（已回收 / 重建期墓碑撤销）：丢弃
        None => {}
        Some(entry) if entry.expired_at == expired_at => {
          if entry.tail_address <= begin_addr {
            pin.remove(&vid);
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
    let got = log.pop_reclaimable(200, 0);
    assert_eq!(got, vec![(1, entry(100, 0)), (2, entry(200, 0))]);
    assert!(log.get(&1).is_none() && log.get(&2).is_none());
    assert_eq!(log.get(&3).map(|e| e.expired_at), Some(300));

    // 同一时刻重弹：无重复回收，账本不动
    assert!(log.pop_reclaimable(200, 0).is_empty());

    // 截断线越界后 250 先回收，300 到期随后回收（堆序）
    let got = log.pop_reclaimable(300, 999);
    assert_eq!(got, vec![(4, entry(250, 999)), (3, entry(300, 0))]);
    assert_eq!(log.len(), 0);
  }
}
