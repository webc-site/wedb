use gxhash::HashSet;
use wbase::hash_slot::{CLUSTER_SLOT_COUNT, slot_of};

/// 不变量一：同库恒同槽（纯函数确定性，库内键 100% 收敛同节点）
#[test]
fn same_db_never_changes_slot() {
  for db in 0..64u64 {
    for ns in [0u64, 1, 7, 4096, u64::MAX] {
      let s = slot_of(ns, db);
      assert_eq!(s, slot_of(ns, db), "同 (ns, db) 必须恒同槽");
      assert!((s as usize) < CLUSTER_SLOT_COUNT, "槽位越界: {s}");
    }
  }
}

/// 不变量二：同 namespace 不同 db 高概率不同槽（多库分布式并发前提）
#[test]
fn sibling_dbs_land_on_distinct_slots() {
  // 16384 槽 256 库：仅按生日界要求碰撞数远小于线性期望
  let distinct: HashSet<u16> = (0..256u64).map(|db| slot_of(1, db)).collect();
  assert!(
    distinct.len() >= 240,
    "同 ns 连续 256 库应高概率离散，实得 {} 个不同槽位",
    distinct.len()
  );
}

/// 不变量三：连续自增 db 在 16384 槽中均匀离散（doc/zh/db.md:283 完美雪崩）
#[test]
fn consecutive_dbs_distribute_uniformly_over_slots() {
  // 16 个等宽槽位桶，4096 个连续 db 落桶数与期望值 256 的偏斜须有界
  let mut buckets = [0usize; 16];
  for db in 0..4096u64 {
    buckets[slot_of(0, db) as usize / (CLUSTER_SLOT_COUNT / 16)] += 1;
  }
  for (i, n) in buckets.iter().enumerate() {
    assert!(
      (160..=400).contains(n),
      "槽位桶 {i} 计数 {n} 偏离均匀期望 256 过远: {buckets:?}"
    );
  }
  // 低位雪崩：相邻 db 的槽位差不得呈短周期（连续 db 落相邻槽位即混合失效）
  let adjacency = (1..4096u64)
    .filter(|&db| slot_of(0, db).wrapping_sub(slot_of(0, db - 1)).abs_diff(1) == 0)
    .count();
  assert!(
    adjacency <= 8,
    "相邻 db 槽位相邻（差 1）出现 {adjacency} 次，雪崩效应不足"
  );
}

/// 交叉不变量：不同 namespace 同 db 亦离散（域分离，无键内容参与）
#[test]
fn namespaces_are_domain_separated() {
  let distinct: HashSet<u16> = (0..256u64).map(|ns| slot_of(ns, 3)).collect();
  assert!(
    distinct.len() >= 240,
    "同 db 连续 256 ns 应高概率离散，实得 {} 个不同槽位",
    distinct.len()
  );
}
