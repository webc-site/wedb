use super::{gc_dead::GcDeadEntry, manager::VirtualDbManager};

impl VirtualDbManager {
  /// FLUSHDB：清空库并换号，返回 (new_vdb, old_vdb_opt)
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
  ) -> (u64, Option<u64>) {
    let (vns, _) = self.get_or_create_ns(logic_ns);
    let routing = self.routing_for(vns);

    let new_vdb = self.alloc_next_virtual_id();
    let old_vdb_opt = routing.table.swap_out(logic_db, new_vdb);

    if let Some(old_vdb) = old_vdb_opt {
      self.gc_dead.insert(
        old_vdb,
        GcDeadEntry {
          expired_at,
          tail_address,
          vns: Some(vns),
        },
      );
    }

    self.bump_generation();
    (new_vdb, old_vdb_opt)
  }

  /// FLUSHALL：清空命名空间下所有库，返回 (new_vns, old_vns_opt)
  pub fn flush_ns(&self, logic_ns: u64, expired_at: i64, tail_address: u64) -> (u64, Option<u64>) {
    let new_vns = self.alloc_next_virtual_id();
    let pin_ns = self.ns_map.pin();
    let old_vns = pin_ns.insert(logic_ns, new_vns).copied().unwrap_or(new_vns);

    let pin_active = self.active_vns.pin();
    if old_vns != new_vns {
      pin_active.remove(&old_vns);
    }
    pin_active.insert(new_vns, logic_ns);

    let old_vns_opt = if old_vns != new_vns {
      self.gc_dead.insert(
        old_vns,
        GcDeadEntry {
          expired_at,
          tail_address,
          vns: None,
        },
      );
      Some(old_vns)
    } else {
      None
    };

    // 换号后新空间为本运行期权威全量（新库映射逐个持久化，表内缺库即真新库）
    self.mark_route_authoritative(new_vns);

    self.bump_generation();
    (new_vns, old_vns_opt)
  }
}
