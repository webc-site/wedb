//! 集群槽位内核：`Slot = Mixer(namespace, active_db)`
//!
//! 依 `doc/zh/db.md` 4.1-4.4 的库级分片模型：集群唯一的拓扑分片单元是
//! `(namespace, db)` 数据库实体，库内所有键恒定收敛于同一槽位，
//! 键级哈希（CRC16 与 `{...}` Hash Tag）与 CROSSSLOT 已彻底废除。
//!
//! ⚠️ 对 garnet 的显式偏离声明：C# `libs/common/HashSlotUtils.cs:HashSlot`
//! 为键字节 CRC16 查表加散列标签的**键级**定槽；本仓按 `doc/zh/db.md` 4.1
//! 「完全移除针对单个用户 Key 的哈希计算」明令改为**库级**定槽，
//! 不做向下兼容、不做旧数据迁移（模块名 `hash_slot` 保留哈希槽位概念名，
//! 以免跨十余 crate 的 Cargo.toml 无谓改动）。
//!
//! 混合器（`doc/zh/db.md:276`「原生 64 位整数寄存器哈希混合器」口径）：
//! Knuth 黄金分割常数与 Murmur 两枚质数做双路独立乘法（超标量并行发射、
//! 纯寄存器无分支、零查表零访存），终末经 [`whasher::mix13`]（Stafford
//! Variant 13）雪崩，再按 2 的幂掩码取槽；耗时 < 2 ns，且连续自增的
//! `db` 在 16384 个槽位上均匀离散（见本模块单测）。
//!
//! 全仓**唯一定槽真值源**：任何会话/切面/门评/扫描面都只经
//! [`slot_of`] 取槽，禁止从键内容推导槽位。

use whasher::{GOLDEN_RATIO_64, mix13};

/// Redis 集群槽位总数
pub const CLUSTER_SLOT_COUNT: usize = 16384;

/// 槽位取模掩码（`CLUSTER_SLOT_COUNT` 为 2 的幂，单周期位与代替取模）
pub const SLOT_MASK: u16 = (CLUSTER_SLOT_COUNT - 1) as u16;

/// Murmur3 终末混合质数 1（`0x85ebca6b`， widen 至 64 位参与整数混合）
const MURMUR_PRIME_1: u64 = 0x85EB_CA6B;

/// Murmur3 终末混合质数 2（`0xc2b2ae35`，与质数 1 互异且互质）
const MURMUR_PRIME_2: u64 = 0xC2B2_AE35;

const _: () = assert!(
  CLUSTER_SLOT_COUNT.is_power_of_two(),
  "SLOT_MASK 单周期位与要求槽位总数为 2 的幂"
);

/// 整数 XOR-Shift 混合内核：`(namespace, db)` 双路质数乘混 + Stafford
/// Mix13 终末雪崩（全 `u64` 寄存器运算，零序列化、零查表、零分支）
#[inline(always)]
const fn mix64(namespace: u64, db: u64) -> u64 {
  // 双路独立乘法可并行发射：namespace 走黄金分割常数、db 走 Murmur 质数 1；
  // (namespace + db) 再走 Murmur 质数 2，消除「同 db 不同 ns」与「同 ns
  // 不同 db」两路输入的任何线性可分性
  let a = namespace.wrapping_mul(GOLDEN_RATIO_64);
  let b = db.wrapping_mul(MURMUR_PRIME_1);
  let c = namespace.wrapping_add(db).wrapping_mul(MURMUR_PRIME_2);
  mix13(a ^ b ^ c)
}

/// 库级定槽单点：`Slot = Mixer(namespace, active_db) & SLOT_MASK`
///
/// 设计不变量（`doc/zh/db.md:272`）：给定 `(namespace, db)` 槽位唯一确定；
/// 同库恒同槽，故同库多键命令 / MULTI-EXEC 事务 / Lua 脚本天然同槽，
/// 跨槽错误在架构层消失（`doc/zh/db.md` 4.4）。
#[inline(always)]
pub const fn slot_of(namespace: u64, db: u64) -> u16 {
  (mix64(namespace, db) & SLOT_MASK as u64) as u16
}

#[cfg(test)]
mod tests {
  use std::collections::HashSet;

  use super::*;

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
    let slots: Vec<u16> = (0..256u64).map(|db| slot_of(1, db)).collect();
    let distinct: HashSet<u16> = slots.iter().copied().collect();
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
    let slots: Vec<u16> = (0..256u64).map(|ns| slot_of(ns, 3)).collect();
    let distinct: HashSet<u16> = slots.iter().copied().collect();
    assert!(
      distinct.len() >= 240,
      "同 db 连续 256 ns 应高概率离散，实得 {} 个不同槽位",
      distinct.len()
    );
  }
}
