//! 虚拟库管理器（VirtualDbManager）单元语义测试（自 src/vdb/manager.rs 内嵌模块外移）
//!
//! 覆盖五件事：
//! - 活跃 vns 判定与 FLUSHNS 换号 / insert_ns_mapping / reset 的判活闭环；
//! - 退役角色精确判定：FLUSHDB(0, 0) 退役 vdb 0 后 gc_dead 含键 0，同 vns=0
//!   的活库不得判死（仅旧 vdb 判死）；FLUSHNS 退役 vns 后该空间全域才判死；
//! - 紧缩过期判定与角色比对同口径：库级退役键（vns=Some）只死配 vdb 槽，
//!   空间级退役键（vns=None）只死配 vns 槽，未到期不判死；
//! - 路由绑定返回值契约与绑定协议（绑定期间空闲析构被回插拦截、解绑登记
//!   期限后可析构、重绑定使候选失效、根域常驻 immortal 拒释）；
//! - list_active_virtual_dbs 换号后退役旧号不出现在活跃列表。

use wkv::vdb::{ROOT_VIRTUAL_ID, VirtualDbManager};

#[test]
fn test_vdb_active_vns() {
  let vdb = VirtualDbManager::new();
  assert!(vdb.logic_ns_of(0).is_some());
  assert!(!vdb.logic_ns_of(1).is_some());

  let (vns1, created) = vdb.get_or_create_ns(100);
  assert!(created);
  assert!(vdb.logic_ns_of(vns1).is_some());

  // FLUSHALL
  let (vns2, old_vns) = vdb.flush_ns(100, 1000, 0);
  assert_eq!(old_vns, Some(vns1));
  assert!(!vdb.logic_ns_of(vns1).is_some());
  assert!(vdb.logic_ns_of(vns2).is_some());

  // insert_ns_mapping
  vdb.insert_ns_mapping(200, 999);
  assert!(vdb.logic_ns_of(999).is_some());

  // Reset
  vdb.reset();
  assert!(vdb.logic_ns_of(0).is_some());
  assert!(!vdb.logic_ns_of(vns2).is_some());
  assert!(!vdb.logic_ns_of(999).is_some());
}

/// 退役角色精确判定：FLUSHDB(0, 0) 退役 vdb 0 后 gc_dead 含键 0，
/// 同 vns=0 的活库不得判死（仅旧 vdb 判死）；FLUSHNS 退役 vns 后该空间全域才判死
#[test]
fn test_vdb_dead_domain_role() {
  let vdb = VirtualDbManager::new();
  // 根命名空间：db0 -> vdb0（初始零号）、db1 -> vdb1（新分配）
  let (ids_vns, ids_vdb, ..) = vdb.get_virtual_ids_with_created(0, 1);
  assert_eq!((ids_vns, ids_vdb), (0, 1));
  assert!(!vdb.is_dead_domain(0, 0));
  assert!(!vdb.is_dead_domain(0, 1));

  // 库级换号：退役 vdb 0，键 0 与在用 vns 0 同号碰撞
  let (vns, new_vdb, old_vdb) = vdb.flush_db(0, 0, 1000, 0);
  assert_eq!((vns, new_vdb, old_vdb), (0, 2, Some(0)));
  assert!(vdb.is_dead_domain(0, 0), "退役旧 vdb 判死");
  assert!(!vdb.is_dead_domain(0, 1), "同空间活库不得因裸 id 碰撞判死");
  assert!(vdb.matches_logic_db(0, 1, 1), "活库路由仍可匹配");
  assert!(vdb.matches_logic_db(0, new_vdb, 0), "换号后新 vdb 承接 db0");
  assert!(!vdb.matches_logic_db(0, 0, 0), "旧 vdb 不再匹配");

  // 命名空间级换号：退役 vns 0，该空间全域判死
  let (new_vns, old_vns) = vdb.flush_ns(0, 2000, 0);
  assert_eq!((new_vns, old_vns), (3, Some(0)));
  assert!(!vdb.logic_ns_of(0).is_some());
  assert!(vdb.is_dead_domain(0, 1), "退役空间的活域判死");
}

/// 紧缩过期判定与角色比对同口径：库级退役键（vns=Some）只死配 vdb 槽，
/// 空间级退役键（vns=None）只死配 vns 槽；未到期不判死（裸 id 同号碰撞
/// 与到期前缀两维同时封死）
#[test]
fn test_vdb_dead_expired_role_and_time() {
  let vdb = VirtualDbManager::new();
  let (ids_vns, ids_vdb, ..) = vdb.get_virtual_ids_with_created(0, 1);
  assert_eq!((ids_vns, ids_vdb), (0, 1));
  // FLUSHDB(0,0) 以 expired_at=1000 退役 vdb 0：键 0 与在用 vns 0 同号
  let (_, _, Some(old_vdb)) = vdb.flush_db(0, 0, 1000, 0) else {
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

#[test]
fn test_vdb_list_active_virtual_dbs() {
  let vdb = VirtualDbManager::new();
  assert_eq!(vdb.list_active_virtual_dbs(), vec![(0, 0)]);

  // 访问 ns=10, db=1
  let (vns10, vdb10_1, ..) = vdb.get_virtual_ids_with_created(10, 1);
  let mut list = vdb.list_active_virtual_dbs();
  assert!(list.contains(&(0, 0)));
  assert!(list.contains(&(vns10, vdb10_1)));

  // flush db 换号，退役旧号不出现在活跃列表中
  let (_, new_vdb, _) = vdb.flush_db(10, 1, 1000, 0);
  list = vdb.list_active_virtual_dbs();
  assert!(!list.contains(&(vns10, vdb10_1)));
  assert!(list.contains(&(vns10, new_vdb)));
}
