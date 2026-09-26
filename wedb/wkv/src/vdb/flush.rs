use super::{gc_dead::GcDeadEntry, manager::VirtualDbManager};

impl VirtualDbManager {
  /// FLUSHDB：清空库并换号，返回 (vns, new_vdb, old_vdb_opt)
  ///
  /// 内存换号段真 O(1)：分配新虚拟库号后对目标逻辑库单元格单次
  /// [`DbRoutingTable::swap_out`] 原子换指，零整表克隆、零 CAS 重试循环，
  /// 成本与租户在册库数无关（doc/zh/db.md 1.3「单次 O(1) 原子替换」口径）；
  /// 换出的旧库号登记死亡账本交延时 GC 回收。并发 flush 链式退役：后换出者
  /// 拿到的旧指向即前次换入的新号，逐代判死，杜绝旧号复用撞号。
  pub fn flush_db(
    &self,
    logic_ns: u64,
    logic_db: u64,
    expired_at: i64,
    tail_address: u64,
  ) -> (u64, u64, Option<u64>) {
    let (vns, new_vdb, old_vdb_opt) = self.swap_db(logic_ns, logic_db);
    if let Some(old_vdb) = old_vdb_opt {
      self.record_dead_db(vns, old_vdb, expired_at, tail_address);
    }
    (vns, new_vdb, old_vdb_opt)
  }

  /// 库级换号第 1 阶段：原子换指逻辑库路由格并推进代数
  pub fn swap_db(&self, logic_ns: u64, logic_db: u64) -> (u64, u64, Option<u64>) {
    let (vns, _) = self.get_or_create_ns(logic_ns);
    let routing = self.routing_for(vns);

    let new_vdb = self.alloc_next_virtual_id();
    let old_vdb_opt = routing.table.swap_out(logic_db, new_vdb);
    self.bump_generation();
    (vns, new_vdb, old_vdb_opt)
  }

  /// 库级换号第 2 阶段：登记死亡账本（入账尾水位为屏障排空后的安全水位）
  pub fn record_dead_db(&self, vns: u64, old_vdb: u64, expired_at: i64, tail_address: u64) {
    self.gc_dead.insert(
      old_vdb,
      GcDeadEntry {
        expired_at,
        tail_address,
        vns: Some(vns),
      },
    );
  }

  /// FLUSHALL：清空命名空间下所有库，返回 (new_vns, old_vns_opt)
  pub fn flush_ns(&self, logic_ns: u64, expired_at: i64, tail_address: u64) -> (u64, Option<u64>) {
    let (new_vns, old_vns_opt) = self.swap_ns(logic_ns);
    if let Some(old_vns) = old_vns_opt {
      self.record_dead_ns(old_vns, expired_at, tail_address);
    }
    (new_vns, old_vns_opt)
  }

  /// 空间级换号第 1 阶段：原子换指命名空间映射并推进代数
  pub fn swap_ns(&self, logic_ns: u64) -> (u64, Option<u64>) {
    let new_vns = self.alloc_next_virtual_id();
    let pin_ns = self.ns_map.pin();
    let old_vns = pin_ns.insert(logic_ns, new_vns).copied().unwrap_or(new_vns);

    let pin_active = self.active_vns.pin();
    if old_vns != new_vns {
      pin_active.remove(&old_vns);
    }
    pin_active.insert(new_vns, logic_ns);

    // 换号后新空间为本运行期权威全量（新库映射逐个持久化，表内缺库即真新库）
    self.mark_route_authoritative(new_vns);
    self.bump_generation();
    (new_vns, (old_vns != new_vns).then_some(old_vns))
  }

  /// 空间级换号第 2 阶段：登记死亡账本（入账尾水位为屏障排空后的安全水位）
  pub fn record_dead_ns(&self, old_vns: u64, expired_at: i64, tail_address: u64) {
    self.gc_dead.insert(
      old_vns,
      GcDeadEntry {
        expired_at,
        tail_address,
        vns: None,
      },
    );
  }

  /// FLUSHDB 换号回滚补偿（commit_swap 失败时调用）：
  /// 换回旧指向或清除新格、彻底撤销 gc_dead 死亡登记与宽限期、重推进代数，使内存态与磁盘态重新对齐
  pub fn rollback_flush_db(&self, vns: u64, logic_db: u64, old_vdb_opt: Option<u64>) {
    let routing = self.routing_for(vns);
    match old_vdb_opt {
      Some(old_vdb) => {
        routing.table.swap_out(logic_db, old_vdb);
        self.gc_dead.cancel(&old_vdb);
      }
      None => {
        routing.table.remove(logic_db);
      }
    }
    self.bump_generation();
  }

  /// FLUSHALL / FLUSHNS 换号回滚补偿（commit_swap 失败时调用）：
  /// 恢复逻辑命名空间映射与活跃反向索引、清除新命名空间快照、彻底撤销 gc_dead 死亡登记与宽限期、重推进代数
  pub fn rollback_flush_ns(&self, logic_ns: u64, new_vns: u64, old_vns_opt: Option<u64>) {
    let pin_active = self.active_vns.pin();
    pin_active.remove(&new_vns);
    self.db_routing.pin().remove(&new_vns);
    let pin_ns = self.ns_map.pin();
    match old_vns_opt {
      Some(old_vns) => {
        pin_ns.insert(logic_ns, old_vns);
        pin_active.insert(old_vns, logic_ns);
        self.gc_dead.cancel(&old_vns);
      }
      None => {
        pin_ns.remove(&logic_ns);
      }
    }
    self.bump_generation();
  }

  /// SWAPDB 换号回滚补偿（persist 失败时调用）：两格换回原位并重推进代数
  ///
  /// swap 不取新号、无死亡登记，补偿面严格零 gc——禁顺带触碰 gc_dead；
  /// 批内已落残余前缀按 flush 族「最坏旧域泄漏」既有裁决对齐，不立第二
  /// 墓碑机制（与 [`Self::rollback_flush_db`] / [`Self::rollback_flush_ns`]
  /// 同族单机制，garnet 无对位——C# 每库独立实例的 map 换指无落盘补偿面）
  pub fn rollback_swap(
    &self,
    vns: u64,
    logic_db1: u64,
    logic_db2: u64,
    final_vdb1: u64,
    final_vdb2: u64,
  ) {
    let routing = self.routing_for(vns);
    routing.table.set(logic_db1, final_vdb1);
    routing.table.set(logic_db2, final_vdb2);
    self.bump_generation();
  }
}
