use std::{
  cmp::Reverse,
  collections::BinaryHeap,
  sync::{
    Arc,
    atomic::{
      AtomicU64,
      Ordering::{Acquire, Relaxed},
    },
  },
};

use parking_lot::Mutex;
use wbase::{
  map::{ConcurrentMap, new_concurrent_map},
  time::now_ms,
};

use super::{
  gc_dead::{GcDeadLog, TenantRouting},
  routing::DbRoutingTable,
};

/// 根域虚拟号（ns 0 / db 0 共用的零号）：常驻 immortal，永不参与空闲析构与引用计数
pub const ROOT_VIRTUAL_ID: u64 = 0;

/// 虚拟数据库管理器（papaya 无锁并发字典 + 槽位级 ArcSwap 单元格路由表）
///
/// 双层映射的内存形态按「冷租户与冷库 0 内存常驻」收敛为三类：
/// - `ns_map` / `active_vns`：逻辑命名空间映射标量（16 字节级，装载后常驻）；
/// - `db_routing`：租户路由快照（[`DbRoutingTable`] 槽位级单元格表），按需
///   装载、引用归零空闲析构，析构后访问经磁盘 DbMeta 点查装载回建；
/// - [`GcDeadLog`]：死亡号账本 + 近期到期小根堆，sweep 只弹到期前缀。
///
/// 启动期重建不再全量灌入映射（仅根域、死亡账本与分配水位），冷数据全留磁盘；
/// 同步路径（[`Self::get_or_create_ns`] / [`Self::get_or_create_db`]）退化为
/// 纯内存原语，冷装载由异步解析面单点承担
pub struct VirtualDbManager {
  /// 租户空间映射: logic_ns -> virtual_ns_id
  pub ns_map: ConcurrentMap<u64, u64>,

  /// 活跃虚空间映射 (反向索引 O(1) 判活): virtual_ns_id -> logic_ns
  pub active_vns: ConcurrentMap<u64, u64>,

  /// 库级路由快照表: virtual_ns_id -> Arc<TenantRouting>
  pub db_routing: ConcurrentMap<u64, Arc<TenantRouting>>,

  /// 全局下一分配 ID
  pub next_virtual_id: AtomicU64,

  /// 全局换号纪元版本（FLUSHDB / SWAPDB / FLUSHALL 自增，驱动 StoreSession 刷新本地缓存）
  pub generation: AtomicU64,

  /// 死亡待回收账本（点查判死 + 到期前缀弹出）
  pub gc_dead: GcDeadLog,

  /// 空闲析构期限堆：(期限毫秒, vns) 升序；引用归零时登记，GC 轮次弹出
  /// 到期前缀逐个摘除（惰性失效：弹出时复核引用与在册状态）
  idle_heap: Mutex<BinaryHeap<Reverse<(u64, u64)>>>,
}

impl Default for VirtualDbManager {
  fn default() -> Self {
    Self::new()
  }
}

impl VirtualDbManager {
  pub fn new() -> Self {
    let ns_map = new_concurrent_map();
    let active_vns = new_concurrent_map();
    let db_routing = new_concurrent_map();
    {
      let pin_ns = ns_map.pin();
      pin_ns.insert(0, 0);
      let pin_active = active_vns.pin();
      pin_active.insert(0, 0);
      let pin_db = db_routing.pin();
      let root_table = DbRoutingTable::new();
      root_table.set(0, ROOT_VIRTUAL_ID);
      pin_db.insert(0, TenantRouting::new(root_table));
    }
    let vdb = Self {
      ns_map,
      active_vns,
      db_routing,
      next_virtual_id: AtomicU64::new(1),
      generation: AtomicU64::new(1),
      gc_dead: GcDeadLog::new(),
      idle_heap: Mutex::new(BinaryHeap::new()),
    };
    // 根域快照 immortal（重建全量装载 + 永不空闲析构），权威全量
    vdb.mark_route_authoritative(ROOT_VIRTUAL_ID);
    vdb
  }

  /// 重置虚拟数据库映射（超管物理截断时重置为初始零号状态）
  pub fn reset(&self) {
    let pin_ns = self.ns_map.pin();
    pin_ns.clear();
    pin_ns.insert(0, 0);

    let pin_active = self.active_vns.pin();
    pin_active.clear();
    pin_active.insert(0, 0);

    let pin_db = self.db_routing.pin();
    pin_db.clear();
    let root_table = DbRoutingTable::new();
    root_table.set(0, ROOT_VIRTUAL_ID);
    pin_db.insert(0, TenantRouting::new(root_table));
    self.mark_route_authoritative(ROOT_VIRTUAL_ID);

    self.gc_dead.clear();
    self.idle_heap.lock().clear();

    self.next_virtual_id.store(1, Relaxed);
    self.bump_generation();
  }

  /// 插入或更新逻辑命名空间到虚拟命名空间的映射，同步维护活跃反向索引
  pub fn insert_ns_mapping(&self, logic_ns: u64, vns: u64) {
    let pin_ns = self.ns_map.pin();
    if let Some(&old_vns) = pin_ns.insert(logic_ns, vns)
      && old_vns != vns
    {
      self.active_vns.pin().remove(&old_vns);
    }
    self.active_vns.pin().insert(vns, logic_ns);
  }

  /// 推进换号版本代数
  #[inline]
  pub fn bump_generation(&self) {
    self.generation.fetch_add(1, Relaxed);
  }

  /// 获取新虚拟 ID
  #[inline]
  pub fn alloc_next_virtual_id(&self) -> u64 {
    self.next_virtual_id.fetch_add(1, Relaxed)
  }

  /// 单调抬升分配水位（DbMeta 镜像应用面：主库已用的号在本节点绝不再分配）
  ///
  /// 从库应用镜像映射/墓碑记录时折叠值侧新号与键侧死亡旧号（与重建收尾
  /// [`WedbStore::finish_vdb_rebuild`] 的 `fetch_max(max_vid + 1)` 同口径），
  /// 保证后续本地取号不与主库未来取号撞号
  #[inline]
  pub fn bump_watermark(&self, min_next: u64) {
    self.next_virtual_id.fetch_max(min_next, Relaxed);
  }

  /// 枚举本节点全部活跃逻辑库 `(namespace, db)`（集群库级分片面的唯一
  /// 库枚举取用：CLUSTER COUNTKEYSINSLOT / GETKEYSINSLOT 按库定槽聚合、
  /// 扩缩容整库搬迁枚举本节点持有库，均经此单点，不新增 db 元数据结构）
  pub fn list_logic_dbs(&self) -> Vec<(u64, u64)> {
    let ns_pin = self.ns_map.pin();
    let routing_pin = self.db_routing.pin();
    let mut out = Vec::new();
    for (logic_ns, vns) in ns_pin.iter() {
      let Some(routing) = routing_pin.get(vns) else {
        continue;
      };
      out.extend(
        routing
          .table
          .snapshot()
          .into_iter()
          .map(|(db, _)| (*logic_ns, db)),
      );
    }
    out
  }

  /// 纯内存原语：获取虚命名空间 ID，不存在则原子分配新 ID
  ///
  /// 仅限启动引导、内部非严格会话与解析面的「磁盘点查未命中后分配」语义；
  /// 用户会话的冷装载走 [`crate::store::WedbStore::resolve_context`] 先点查
  /// 磁盘 DbMeta，禁止在未点查前盲分配（冷租户一经盲分配即换新号，
  /// 旧域数据被判死丢失——正是本模块要消灭的缺陷）
  pub fn get_or_create_ns(&self, logic_ns: u64) -> (u64, bool) {
    let pin = self.ns_map.pin();
    if let Some(&vns) = pin.get(&logic_ns) {
      return (vns, false);
    }
    let new_vns = self.alloc_next_virtual_id();
    let actual = pin.get_or_insert(logic_ns, new_vns);
    let created = *actual == new_vns;
    if created {
      self.active_vns.pin().insert(new_vns, logic_ns);
    }
    (*actual, created)
  }

  /// 取租户路由快照（在册直返，缺席建空表回插）
  pub fn routing_for(&self, vns: u64) -> Arc<TenantRouting> {
    let pin_routing = self.db_routing.pin();
    if let Some(r) = pin_routing.get(&vns) {
      return Arc::clone(r);
    }
    pin_routing
      .get_or_insert(vns, TenantRouting::new(DbRoutingTable::new()))
      .clone()
  }

  /// 纯内存原语：获取虚数据库 ID，不存在则写入（语义约束同 [`Self::get_or_create_ns`]）
  pub fn get_or_create_db(&self, logic_ns: u64, logic_db: u64) -> (u64, bool) {
    let (vns, _) = self.get_or_create_ns(logic_ns);
    let routing = self.routing_for(vns);
    if let Some(vdb) = routing.table.get(logic_db) {
      return (vdb, false);
    }

    // 慢路径：并发首访经单格 get_or_insert 无锁抢占，仅一者建格胜出；
    // 新建号全局唯一，格内值等于本号即建格者，否则复用并发胜出者
    let new_vdb = self.alloc_next_virtual_id();
    let current = **routing.table.cell_or_insert(logic_db, new_vdb).load();
    (current, current == new_vdb)
  }

  /// 获取当前逻辑库对应的虚拟空间与虚拟库 ID（纯内存原语，语义约束同
  /// [`Self::get_or_create_ns`]）
  #[inline]
  pub fn get_virtual_ids(&self, logic_ns: u64, logic_db: u64) -> (u64, u64) {
    let (vns, _) = self.get_or_create_ns(logic_ns);
    let (vdb, _) = self.get_or_create_db(logic_ns, logic_db);
    (vns, vdb)
  }

  /// 同步只读判定 (ns, db) 是否为冷库（严格上下文免盲分配门）
  ///
  /// 冷库 = ns 标量在册（重建装载全部 ns 标量，磁盘有记录必在册，未在册即
  /// 全新租户）且库映射未装载且路由表非本运行期权威全量——冷库与真新库
  /// 无法同步甄别（磁盘权威须点查方知），严格会话遇之拒绝盲分配，交异步
  /// 解析点查磁盘：命中装载既有映射（绝不换号），未命中创建并持久化
  #[inline]
  pub fn is_cold_db(&self, ns: u64, db: u64) -> bool {
    let Some(&vns) = self.ns_map.pin().get(&ns) else {
      return false; // ns 未在册 = 全新租户，同步创建
    };
    match self.db_routing.pin().get(&vns) {
      None => true, // 冷租户：路由快照未装载
      Some(r) => !r.authoritative.load(Relaxed) && !r.table.contains(db),
    }
  }

  /// 标记租户路由表为本运行期权威全量（ns 首次映射时调用）：后续新库映射
  /// 可直接同步分配，无需点查甄别
  #[inline]
  pub fn mark_route_authoritative(&self, vns: u64) {
    self.routing_for(vns).authoritative.store(true, Relaxed);
  }

  /// 只读判定租户路由快照是否已为本运行期权威全量（冷装载幂等门）
  ///
  /// 真 = 该租户库级映射已完整在册（根域启动即权威、新租户首映射即权威、
  /// 冷租户经点查回建后权威），调用方据此零 I/O 直返；假 = 表缺席或仅存量格
  /// 的部分快照（严格会话冷库判据 [`Self::is_cold_db`] 的同源读端）
  #[inline]
  pub fn is_route_authoritative(&self, vns: u64) -> bool {
    self
      .db_routing
      .pin()
      .get(&vns)
      .is_some_and(|r| r.authoritative.load(Relaxed))
  }

  /// 点查租户路由表中的逻辑库映射（严格上下文与统计过滤的同步只读原语）
  #[inline]
  pub fn route_vdb_of(&self, vns: u64, logic_db: u64) -> Option<u64> {
    self
      .db_routing
      .pin()
      .get(&vns)
      .and_then(|r| r.table.get(logic_db))
  }

  /// 点查逻辑命名空间的虚拟空间 ID（未在册即 None；语义约束同
  /// [`Self::get_or_create_ns`]——只读甄别，绝不盲分配）
  #[inline]
  pub fn vns_of_ns(&self, logic_ns: u64) -> Option<u64> {
    self.ns_map.pin().get(&logic_ns).copied()
  }

  /// 点查虚拟空间的逻辑命名空间（[`Self::insert_ns_mapping`] 维护的
  /// `active_vns` 逆向索引只读读端：重建期 0x01 记录装载全部在册租户，
  /// 未在册即本节点从无该空间的逻辑入口；根域 (0, 0) 恒在册）
  #[inline]
  pub fn logic_ns_of(&self, vns: u64) -> Option<u64> {
    self.active_vns.pin().get(&vns).copied()
  }

  /// 物理域 → 逻辑域反查单点（[`Self::get_virtual_ids`] 的逆运算，只读）
  ///
  /// 回放面唯一的域反查入口：AOF keyed 条目只带物理前缀 `[vns][vdb]`（入账侧
  /// 键一律是引擎物理键），而库级定槽按逻辑域现算（`doc/zh/db.md` 4.1
  /// 「同一个 DB 对应同一个槽位」），故重放向量/索引族须经本反查取回逻辑
  /// `(ns, db)` 再交 `slot_of`，与在线面逐值同值。
  ///
  /// 两腿取用口径：`vns → logic_ns` 走 [`Self::active_vns`] 逆表（重建全量装载，
  /// 恒在册）；`vdb → logic_db` 走该租户路由表内指向本号的库格（O(在册库数)
  /// 只读枚举）。**任一反查未在册即返回 `None`，绝不以物理号冒充逻辑号**——
  /// 物理号定槽与在线面分叉，错值一经 [`Self::insert_db_mapping`] 之外的路径
  /// 落定即不可自愈（向量登记 context 建时盖章、按槽枚举从此取不到该集）。
  /// 冷租户快照未装载（重启 / 检查点基线后面非根域按 0 内存常驻条款不装载）
  /// 是本返回值的唯一成因，调用方须先经
  /// [`crate::store::WedbStore::load_routes_of_vns`] 点查磁盘回建再反查，
  /// 回建后仍未命中即本节点对该域确无逻辑入口（退役域条目），由调用方显式裁决。
  /// 全程零分配、零落盘、绝不改动任何映射。
  pub fn logic_domain_of(&self, vns: u64, vdb: u64) -> Option<(u64, u64)> {
    let logic_ns = self.logic_ns_of(vns)?;
    let logic_db = self.db_routing.pin().get(&vns).and_then(|routing| {
      routing
        .table
        .snapshot()
        .into_iter()
        .find(|&(_, mapped)| mapped == vdb)
        .map(|(logic_db, _)| logic_db)
    })?;
    Some((logic_ns, logic_db))
  }

  /// 只读枚举在册租户库快照 `(虚拟库号, 逻辑库号)`（[`Self::list_logic_dbs`]
  /// 的单租户投影，C# `GetDatabasesSnapshot` 的在册库口径）
  ///
  /// 租户未在册或路由快照未装载（冷库）即空集：本入口不建快照（区别于
  /// [`Self::routing_for`] 的缺席建表回插）、不分配虚库号、不落 DbMeta，
  /// 只读命令（INFO KEYSPACE 统计面）因此绝不改写存储状态
  pub fn registered_dbs(&self, vns: u64) -> Vec<(u64, u64)> {
    let routing_pin = self.db_routing.pin();
    let Some(routing) = routing_pin.get(&vns) else {
      return Vec::new();
    };
    routing
      .table
      .snapshot()
      .into_iter()
      .map(|(logic_db, vdb)| (vdb, logic_db))
      .collect()
  }

  /// 向租户路由表写入映射（后写覆盖，单格幂等）
  ///
  /// 磁盘 DbMeta 为映射权威：重建期根域装载与点查装载回建共用本入口，
  /// 覆盖引导期占位（如根域 (0,0)→0 初值）与陈旧在册值；并发写路径
  /// （flush/swap）持久化先于本入口可见时以磁盘记录为准
  pub fn insert_db_mapping(&self, vns: u64, logic_db: u64, vdb: u64) {
    self.routing_for(vns).table.set(logic_db, vdb);
  }

  /// 绑定会话到租户路由快照（refs+1），返回是否实际持到在册快照引用
  ///
  /// 摘除-回插竞态协议：计数递增后复核快照仍在映射内（被摘除则回插或
  /// 换绑新者），返回真即调用方持有一个在册快照的引用——空闲析构的
  /// 摘除判定以引用为准，绑定期间快照绝不被析构，同步读路径因此永远
  /// 命中在册表。根域常驻 immortal 免计数，恒真；返回假 = 调用瞬间快照
  /// 缺席（恰被空闲析构摘除且无他方回插），未持任何引用——调用方（严格
  /// 上下文解析前的钉引用）据此回退异步点查装载，绝不盲分配覆写既有映射
  pub fn bind_route(&self, vns: u64) -> bool {
    if vns == ROOT_VIRTUAL_ID {
      return true;
    }
    let pin = self.db_routing.pin();
    let mut route = match pin.get(&vns) {
      Some(r) => Arc::clone(r),
      None => return false,
    };
    loop {
      route.refs.fetch_add(1, Acquire);
      match pin.get(&vns) {
        Some(r) if Arc::ptr_eq(r, &route) => return true,
        Some(r) => {
          // 摘除后他方回插了新快照：解绑旧者改绑新者
          route.refs.fetch_sub(1, Acquire);
          route = Arc::clone(r);
        }
        None => match pin.get_or_insert(vns, Arc::clone(&route)) {
          // 被摘除且回插窗口内无他方快照：回插本快照（与磁盘重载内容一致）
          r if Arc::ptr_eq(r, &route) => return true,
          r => {
            route.refs.fetch_sub(1, Acquire);
            route = Arc::clone(r);
          }
        },
      }
    }
  }

  /// 解绑租户路由快照（refs-1；归零时按 `idle_ms` 登记空闲析构期限）
  pub fn unbind_route(&self, vns: u64, idle_ms: u64) {
    if vns == ROOT_VIRTUAL_ID {
      return;
    }
    let Some(route) = self.db_routing.pin().get(&vns).cloned() else {
      return;
    };
    if route.refs.fetch_sub(1, Acquire) == 1 {
      let deadline = now_ms().saturating_add(idle_ms);
      self.idle_heap.lock().push(Reverse((deadline, vns)));
    }
  }

  /// 弹出空闲析构期限已到期的候选 vns 前缀
  ///
  /// 惰性失效：引用已复归（重绑定）、快照已不在册或已入死亡账本的条目
  /// 直接丢弃；未到期前缀保留在堆内
  pub fn pop_idle_candidates(&self, now_ms: u64) -> Vec<u64> {
    let mut heap = self.idle_heap.lock();
    let mut due = Vec::new();
    while let Some(Reverse((deadline, vns))) = heap.peek().copied() {
      if deadline > now_ms {
        break;
      }
      heap.pop();
      let idle = self
        .db_routing
        .pin()
        .get(&vns)
        .is_some_and(|r| r.refs() == 0)
        && !self.is_dead_ns(vns);
      if idle {
        due.push(vns);
      }
    }
    due
  }

  /// 空闲析构单个租户路由快照：摘除后引用仍归零才真正释放
  ///
  /// 摘除与绑定并发时（绑定方在摘除后递增计数）按摘除-回插协议回插同一
  /// 快照，绑定方经复核换绑，绝无「绑定生效而快照缺席」状态；根域拒释
  pub fn evict_idle_route(&self, vns: u64) -> bool {
    if vns == ROOT_VIRTUAL_ID {
      return false;
    }
    let pin = self.db_routing.pin();
    let Some(route) = pin.remove(&vns) else {
      return false;
    };
    if route.refs() != 0 {
      pin.get_or_insert(vns, Arc::clone(route));
      return false;
    }
    true
  }

  /// 零锁无等待极速判断物理域 (vns, vdb) 是否已过期死亡（供 Compaction
  /// 顺带丢弃）
  ///
  /// 与 [`Self::is_dead_domain`] 同口径按退役角色精确比对（vns 与 vdb 共用
  /// 全局号空间，裸 id 命中判定在根库退役窗口会把一切同号活域整批误判死亡），
  /// 叠加 `expired_at` 到期判定：紧缩只丢已越过回收延迟的死域记录
  #[inline]
  pub fn is_virtual_id_dead_and_expired(&self, vns: u64, vdb: u64, now: i64) -> bool {
    if self
      .gc_dead
      .get(&vdb)
      .is_some_and(|e| e.vns.is_some() && e.expired_at <= now)
    {
      return true;
    }
    self
      .gc_dead
      .get(&vns)
      .is_some_and(|e| e.vns.is_none() && e.expired_at <= now)
  }

  /// 按退役角色精确判定物理域 (vns, vdb) 是否已死亡废弃
  ///
  /// vns 与 vdb 共用同一全局号空间（[`Self::get_or_create_ns`]、
  /// [`Self::get_or_create_db`]、[`Self::flush_db`]、[`Self::flush_ns`] 同走
  /// [`Self::alloc_next_virtual_id`]），故裸 id 命中判定在根域边界失准：
  /// FLUSHDB(0, 0) 退役 vdb 0 后 gc_dead 含键 0，任何 vns=0 的活域都被误判死亡。
  /// 本判定经 [`GcDeadEntry::vns`] 区分退役角色——vdb 键须为库级退役
  /// （[`Self::flush_db`] 落 vns = Some）、vns 键须为命名空间级退役
  /// （[`Self::flush_ns`] 落 vns = None），角色不符的裸 id 碰撞不判死
  #[inline]
  pub fn is_dead_domain(&self, vns: u64, vdb: u64) -> bool {
    if self.gc_dead.get(&vdb).is_some_and(|e| e.vns.is_some()) {
      return true;
    }
    self.is_dead_ns(vns)
  }

  /// 命名空间级退役判定（点查装载防复活：换号退役的旧 vns 不再装回）
  #[inline]
  pub fn is_dead_ns(&self, vns: u64) -> bool {
    self.gc_dead.get(&vns).is_some_and(|e| e.vns.is_none())
  }

  /// 检查虚拟命名空间是否仍为活跃映射（O(1) 判定）
  #[inline]
  pub fn is_active_vns(&self, vns: u64) -> bool {
    self.active_vns.pin().contains_key(&vns)
  }

  /// 检查物理 (vns, vdb) 是否匹配当前活跃的指定逻辑库
  #[inline]
  pub fn matches_logic_db(&self, vns: u64, vdb: u64, target_logic_db: u64) -> bool {
    if self.is_dead_domain(vns, vdb) || !self.is_active_vns(vns) {
      return false;
    }
    self.route_vdb_of(vns, target_logic_db) == Some(vdb)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_vdb_active_vns() {
    let vdb = VirtualDbManager::new();
    assert!(vdb.is_active_vns(0));
    assert!(!vdb.is_active_vns(1));

    let (vns1, created) = vdb.get_or_create_ns(100);
    assert!(created);
    assert!(vdb.is_active_vns(vns1));

    // FLUSHALL
    let (vns2, old_vns) = vdb.flush_ns(100, 1000, 0);
    assert_eq!(old_vns, Some(vns1));
    assert!(!vdb.is_active_vns(vns1));
    assert!(vdb.is_active_vns(vns2));

    // insert_ns_mapping
    vdb.insert_ns_mapping(200, 999);
    assert!(vdb.is_active_vns(999));

    // Reset
    vdb.reset();
    assert!(vdb.is_active_vns(0));
    assert!(!vdb.is_active_vns(vns2));
    assert!(!vdb.is_active_vns(999));
  }

  /// 退役角色精确判定：FLUSHDB(0, 0) 退役 vdb 0 后 gc_dead 含键 0，
  /// 同 vns=0 的活库不得判死（仅旧 vdb 判死）；FLUSHNS 退役 vns 后该空间全域才判死
  #[test]
  fn test_vdb_dead_domain_role() {
    let vdb = VirtualDbManager::new();
    // 根命名空间：db0 -> vdb0（初始零号）、db1 -> vdb1（新分配）
    assert_eq!(vdb.get_virtual_ids(0, 1), (0, 1));
    assert!(!vdb.is_dead_domain(0, 0));
    assert!(!vdb.is_dead_domain(0, 1));

    // 库级换号：退役 vdb 0，键 0 与在用 vns 0 同号碰撞
    let (new_vdb, old_vdb) = vdb.flush_db(0, 0, 1000, 0);
    assert_eq!((new_vdb, old_vdb), (2, Some(0)));
    assert!(vdb.is_dead_domain(0, 0), "退役旧 vdb 判死");
    assert!(!vdb.is_dead_domain(0, 1), "同空间活库不得因裸 id 碰撞判死");
    assert!(vdb.matches_logic_db(0, 1, 1), "活库路由仍可匹配");
    assert!(vdb.matches_logic_db(0, new_vdb, 0), "换号后新 vdb 承接 db0");
    assert!(!vdb.matches_logic_db(0, 0, 0), "旧 vdb 不再匹配");

    // 命名空间级换号：退役 vns 0，该空间全域判死
    let (new_vns, old_vns) = vdb.flush_ns(0, 2000, 0);
    assert_eq!((new_vns, old_vns), (3, Some(0)));
    assert!(!vdb.is_active_vns(0));
    assert!(vdb.is_dead_domain(0, 1), "退役空间的活域判死");
  }

  /// 紧缩过期判定与角色比对同口径：库级退役键（vns=Some）只死配 vdb 槽，
  /// 空间级退役键（vns=None）只死配 vns 槽；未到期不判死（裸 id 同号碰撞
  /// 与到期前缀两维同时封死）
  #[test]
  fn test_vdb_dead_expired_role_and_time() {
    let vdb = VirtualDbManager::new();
    assert_eq!(vdb.get_virtual_ids(0, 1), (0, 1));
    // FLUSHDB(0,0) 以 expired_at=1000 退役 vdb 0：键 0 与在用 vns 0 同号
    let (_, Some(old_vdb)) = vdb.flush_db(0, 0, 1000, 0) else {
      unreachable!();
    };
    assert_eq!(old_vdb, 0);
    assert!(
      !vdb.is_virtual_id_dead_and_expired(0, 0, 999),
      "未到期不得判死"
    );
    assert!(
      vdb.is_virtual_id_dead_and_expired(0, 0, 1000),
      "到期判死退役 vdb 0"
    );
    assert!(
      !vdb.is_virtual_id_dead_and_expired(0, 1, 1000),
      "同空间活 vdb 1 不得因 vns 槽裸 id 命中判死（键 0 为库级退役）"
    );
    // FLUSHNS(0) 以 expired_at=2000 退役 vns 0：新 vns 3 号域判死，
    // 活 vdb 1 不因空间级键 0 的 vdb 槽比对被牵连
    let (new_vns, _) = vdb.flush_ns(0, 2000, 0);
    assert!(new_vns > 0);
    assert!(
      !vdb.is_virtual_id_dead_and_expired(0, 1, 1999),
      "空间级退役未到期不判死"
    );
    assert!(
      vdb.is_virtual_id_dead_and_expired(0, 1, 2000),
      "vns 0 空间级退役到期，该空间全域判死"
    );
  }

  /// 绑定返回值契约：在册持引用返回真、缺席未持引用返回假（严格上下文
  /// 解析前钉引用的回退判据——假即快照恰被空闲析构摘除，须回退异步点查）
  #[test]
  fn test_vdb_bind_route_bool_contract() {
    let vdb = VirtualDbManager::new();
    let (vns, _) = vdb.get_or_create_ns(9);
    assert!(!vdb.bind_route(vns), "快照缺席不得虚报持引用");
    vdb.get_or_create_db(9, 1);
    assert!(vdb.bind_route(vns), "在册快照绑定持引用");
    assert!(vdb.bind_route(ROOT_VIRTUAL_ID), "根域免计数恒真");
    vdb.unbind_route(vns, 0);
  }

  /// 绑定协议：绑定期间空闲析构被回插拦截且映射不丢，解绑登记期限后可析构
  #[test]
  fn test_vdb_route_ref_eviction() {
    let vdb = VirtualDbManager::new();
    let (vns, _) = vdb.get_or_create_ns(7);
    vdb.get_or_create_db(7, 1);
    assert!(vdb.db_routing.pin().get(&vns).is_some());

    // 未绑定：即可析构（此形态对应未装载校验的防御面）
    assert!(vdb.evict_idle_route(vns), "空闲快照应被析构");
    assert!(vdb.db_routing.pin().get(&vns).is_none());

    // 析构后点查装载回建并绑定：绑定期间摘除被回插拦截
    vdb.insert_db_mapping(vns, 1, 1);
    vdb.bind_route(vns);
    assert!(!vdb.evict_idle_route(vns), "绑定期间不得析构");
    assert!(vdb.db_routing.pin().get(&vns).is_some());
    assert_eq!(vdb.route_vdb_of(vns, 1), Some(1), "绑定期间映射不丢");

    // 解绑归零登记期限，弹出候选后可析构
    vdb.unbind_route(vns, 0);
    assert!(vdb.pop_idle_candidates(0).is_empty(), "未到期不弹");
    assert_eq!(vdb.pop_idle_candidates(u64::MAX), vec![vns]);
    assert!(vdb.evict_idle_route(vns));

    // 重绑定使候选失效（惰性丢弃）
    vdb.bind_route(vns);
    vdb.unbind_route(vns, 0);
    vdb.bind_route(vns);
    assert!(
      vdb.pop_idle_candidates(u64::MAX).is_empty(),
      "引用复归候选失效"
    );
    vdb.unbind_route(vns, 0);

    // 根域永不参与计数与析构
    vdb.bind_route(ROOT_VIRTUAL_ID);
    vdb.unbind_route(ROOT_VIRTUAL_ID, 0);
    assert!(!vdb.pop_idle_candidates(u64::MAX).contains(&0));
    assert!(!vdb.evict_idle_route(ROOT_VIRTUAL_ID), "根域拒释");
    assert!(vdb.db_routing.pin().get(&0).is_some(), "根域快照常驻");
  }
}
