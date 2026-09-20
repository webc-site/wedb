//! 随机工具（对标 libs/common/RandomUtils.cs）

use fastrand::Rng;
use std::collections::HashSet;
use wbase::map::GxBuildHasher;

/// 从 n 个元素中随机取 k 个下标（HRANDFIELD/SRANDMEMBER/ZRANDMEMBER 共用）
///
/// libs/common/RandomUtils.cs:PickKRandomIndexes
///
/// 刻意差异（对照 C#）：.NET `Random(seed)` 的洗牌/迭代抽取序列与 fastrand 不同，
/// 仅保语义等价。分支结构 1:1 对齐：
/// - `distinct=false` 或 `k/n < K_OVER_N_THRESHOLD` 走迭代抽取（distinct 用
///   拒绝采样，O(k) 空间，C# PickKRandomIndexesIteratively）；
/// - 否则全量洗牌取前 k（C# PickKRandomDistinctIndexesWithShuffle）。
///
/// 空集直接返回空（C# `Random.Next(0)` 抛 ArgumentOutOfRangeException，
/// 按无结果处理）
pub fn pick_k_random_indexes(n: usize, k: usize, seed: i32, distinct: bool) -> Vec<usize> {
  /// k/n 低于该阈值走迭代抽取（C# RandomUtils.KOverNThreshold）
  const K_OVER_N_THRESHOLD: f64 = 0.1;

  let mut rng = Rng::with_seed(u64::from(seed as u32));
  if n == 0 || k == 0 {
    return Vec::new();
  }

  if !distinct || (k as f64) / (n as f64) < K_OVER_N_THRESHOLD {
    let mut indexes = Vec::with_capacity(k);
    if !distinct {
      indexes.extend((0..k).map(|_| rng.usize(..n)));
    } else {
      // 拒绝采样：k <= n 保证可终止
      let mut picked = HashSet::with_capacity_and_hasher(k, GxBuildHasher::default());
      while indexes.len() < k {
        let idx = rng.usize(..n);
        if picked.insert(idx) {
          indexes.push(idx);
        }
      }
    }
    indexes
  } else {
    // 部分洗牌取前 k（k == n 时即全量洗牌）
    let mut perm: Vec<usize> = (0..n).collect();
    for i in 0..k.min(n) {
      let j = rng.usize(i..perm.len());
      perm.swap(i, j);
    }
    perm.truncate(k);
    perm
  }
}

/// 单下标随机取（HRANDFIELD/SRANDMEMBER 无 count 形态）
///
/// libs/common/RandomUtils.cs:PickRandomIndex（.NET rand 为非负随机数，% 取模）
#[inline]
pub fn pick_random_index(n: usize, rand: i32) -> usize {
  (rand as u32 as usize) % n
}

#[cfg(test)]
mod tests {
  use super::pick_k_random_indexes;

  /// k/n 阈值分派（C# RandomUtils.KOverNThreshold）：小比例走迭代/拒绝采样，
  /// 达阈值才建全量洗牌。返回向量的容量即分配规模的结构侧证——旧 zset 内联
  /// 洗牌在千级基数抽 3 个时也先分配 n×8 字节置换，共用单源只按 k 预留
  #[test]
  fn pick_k_random_indexes_dispatches_by_ratio() {
    let mut small = pick_k_random_indexes(1000, 3, 7, true);
    assert_eq!(
      (small.len(), small.capacity()),
      (3, 3),
      "k/n 低于阈值应只按 k 预留，不按集合规模分配"
    );
    assert!(small.iter().all(|&index| index < 1000));
    small.sort();
    small.dedup();
    assert_eq!(small.len(), 3, "不放回采样互异");

    let mut large = pick_k_random_indexes(1000, 500, 7, true);
    assert_eq!(large.len(), 500);
    assert!(
      large.capacity() >= 1000,
      "k/n 达阈值才建全量置换（C# new int[n]）"
    );
    assert!(large.iter().all(|&index| index < 1000));
    large.sort();
    large.dedup();
    assert_eq!(large.len(), 500, "不放回采样互异");

    // 放回臂：长度恒为 k，可重复
    let repeated = pick_k_random_indexes(4, 10, 7, false);
    assert_eq!((repeated.len(), repeated.capacity()), (10, 10));
    assert!(repeated.iter().all(|&index| index < 4));

    // 空集与零取样：无结果（C# Random.Next(0) 抛异常，按无结果处理）
    assert!(pick_k_random_indexes(0, 3, 7, true).is_empty());
    assert!(pick_k_random_indexes(10, 0, 7, true).is_empty());
  }
}
