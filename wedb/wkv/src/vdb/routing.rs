use std::sync::Arc;

use arc_swap::ArcSwap;
use wbase::map::{ConcurrentMap, new_concurrent_map};

/// 逻辑数据库到虚拟数据库 ID 的槽位级路由表（每格 `Arc<ArcSwap<u64>>` 单元格）
///
/// papaya 并发字典承载 logic_db → 单元格指针，FLUSHDB / SWAPDB 换号只单格
/// 原子换指虚拟库 ID：成本恒定 O(1)，与租户在册库数无关，杜绝整表写时克隆
/// 的线性放大（doc/zh/db.md 1.3/1.4「单次 O(1) 原子替换、耗时小于 1 微秒」
/// 的内存换号段口径）。读面一律经本表单点访问器（[`Self::get`] 等），禁散点
/// 直取 cells，杜绝旁路绕过单元格语义。
#[derive(Debug)]
pub struct DbRoutingTable {
  cells: ConcurrentMap<u64, Arc<ArcSwap<u64>>>,
}

impl DbRoutingTable {
  pub fn new() -> Self {
    Self {
      cells: new_concurrent_map(),
    }
  }

  /// 单点读访问器：逻辑库当前指向的虚拟库 ID（未映射为 None）
  #[inline]
  pub fn get(&self, logic_db: u64) -> Option<u64> {
    self.cells.pin().get(&logic_db).map(|cell| **cell.load())
  }

  /// 单点在册判定：逻辑库是否已有映射单元格（冷库甄别只读原语）
  #[inline]
  pub fn contains(&self, logic_db: u64) -> bool {
    self.cells.pin().contains_key(&logic_db)
  }

  /// 取用逻辑库单元格（缺席则原子建格预置 `initial`，恒返回在册格）——
  /// 换号写面（flush / swap / 回灌装载）共用的单点结构入口，并发首访经
  /// papaya get_or_insert 无锁抢占仅一者胜出插入
  #[inline]
  pub fn cell_or_insert(&self, logic_db: u64, initial: u64) -> Arc<ArcSwap<u64>> {
    let pin = self.cells.pin();
    Arc::clone(pin.get_or_insert(logic_db, Arc::new(ArcSwap::from_pointee(initial))))
  }

  /// 幂等覆盖写映射（重建装载与点查回建单点：后写覆盖，无旧值语义）
  #[inline]
  pub fn set(&self, logic_db: u64, vdb: u64) {
    self.cell_or_insert(logic_db, vdb).store(Arc::new(vdb));
  }

  /// FLUSHDB 单格换号：原子换指新虚拟库号并返回换号前旧指向（None = 本
  /// 运行期首映射，库从无退役旧域）。新建号全局唯一，故 swap 返回值等于
  /// `new_vdb` 即本格为刚建格，无需 CAS 重试
  #[inline]
  pub fn swap_out(&self, logic_db: u64, new_vdb: u64) -> Option<u64> {
    let old = self
      .cell_or_insert(logic_db, new_vdb)
      .swap(Arc::new(new_vdb));
    (*old != new_vdb).then_some(*old)
  }

  /// 移除指定逻辑库单元格（回滚补偿单点：未曾持久化的首映射失败时清除新格）
  #[inline]
  pub fn remove(&self, logic_db: u64) {
    self.cells.pin().remove(&logic_db);
  }

  /// 只读枚举全部在册映射 `(logic_db, vdb)`（库枚举 / INFO 统计面专用，
  /// O(在册库数)，不在换号与读写热路径上）
  pub fn snapshot(&self) -> Vec<(u64, u64)> {
    let pin = self.cells.pin();
    pin
      .iter()
      .map(|(logic_db, cell)| (*logic_db, **cell.load()))
      .collect()
  }
}

impl Default for DbRoutingTable {
  fn default() -> Self {
    Self::new()
  }
}
