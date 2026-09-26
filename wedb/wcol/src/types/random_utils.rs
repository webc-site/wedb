//! 随机工具（对标 libs/common/RandomUtils.cs）
//!
//! 自研偏差锁: doc/zh/deviations.md SRANDMEMBER 采样域（fastrand 面）

use fastrand::Rng;
use wbase::map::{HashSet, HashSetExt};

/// 从 n 个元素中随机取 k 个下标逐个交 `sink`（HRANDFIELD/SRANDMEMBER/ZRANDMEMBER 共用）
///
/// libs/common/RandomUtils.cs:PickKRandomIndexes
///
/// 刻意差异（对照 C#）：.NET `Random(seed)` 的洗牌/迭代抽取序列与 fastrand 不同，
/// 仅保语义等价。分支结构 1:1 对齐：
/// - `distinct=false` 或 `k/n < K_OVER_N_THRESHOLD` 走迭代抽取（distinct 用
///   拒绝采样，O(k) 空间，C# PickKRandomIndexesIteratively）；
/// - 否则全量洗牌取前 k（C# PickKRandomDistinctIndexesWithShuffle）。
///
/// 下标流式 sink 产出、放回臂零存储：负 count 的 |k| 由客户端参数直控、与集合
/// 基数脱钩，C# 侧 `new int[countParameter]`（SetObjectImpl.cs:219 等）在该臂
/// 是连接级 OOM 面，rust 侧预分配更会放大为 GB 级单命令分配乃至分配失败 abort
/// 全进程——故禁止任何按 k 的预分配，消费点逐下标直写 RESP，空间 O(1)。
/// 空集/零取样不调 `sink`（C# `Random.Next(0)` 抛 ArgumentOutOfRangeException，
/// 按无结果处理）
pub fn pick_k_random_indexes(
  n: usize,
  k: usize,
  seed: i32,
  distinct: bool,
  mut sink: impl FnMut(usize),
) {
  /// k/n 低于该阈值走迭代抽取（C# RandomUtils.KOverNThreshold）
  const K_OVER_N_THRESHOLD: f64 = 0.1;

  let mut rng = Rng::with_seed(u64::from(seed as u32));
  if n == 0 || k == 0 {
    return;
  }

  if !distinct {
    // 放回臂：k 可为 |count| 极值，逐个产出零存储
    for _ in 0..k {
      sink(rng.usize(..n));
    }
    return;
  }

  if (k as f64) / (n as f64) < K_OVER_N_THRESHOLD {
    // 拒绝采样：k < n 才入此臂，k 受集合基数约束，O(k) 空间有界
    let mut picked = HashSet::with_capacity(k);
    let mut emitted = 0;
    while emitted < k {
      let idx = rng.usize(..n);
      if picked.insert(idx) {
        sink(idx);
        emitted += 1;
      }
    }
  } else {
    // 部分洗牌取前 k（k == n 时即全量洗牌）；置换域 n 即集合基数，
    // 分配与对象本体同阶
    let mut perm: Vec<usize> = (0..n).collect();
    for i in 0..k.min(n) {
      let j = rng.usize(i..perm.len());
      perm.swap(i, j);
    }
    perm.truncate(k);
    perm.into_iter().for_each(sink);
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
  /// 达阈值才建全量洗牌。以 sink 逐个收集断言序列形态
  #[test]
  fn pick_k_random_indexes_dispatches_by_ratio() {
    let collect = |n, k, seed, distinct| {
      let mut out = Vec::new();
      pick_k_random_indexes(n, k, seed, distinct, |i| out.push(i));
      out
    };

    let mut small = collect(1000, 3, 7, true);
    assert_eq!(small.len(), 3, "k/n 低于阈值走拒绝采样，只产出 k 个");
    assert!(small.iter().all(|&index| index < 1000));
    small.sort();
    small.dedup();
    assert_eq!(small.len(), 3, "不放回采样互异");

    let mut large = collect(1000, 500, 7, true);
    assert_eq!(large.len(), 500);
    assert!(large.iter().all(|&index| index < 1000));
    large.sort();
    large.dedup();
    assert_eq!(large.len(), 500, "不放回采样互异");

    // 放回臂：sink 次数恒为 k，可重复、值域受 n 约束；零预分配为结构性保证
    //（本函数无任何按 k 的容量预留，负 count 极值不再有 GB 级分配面）
    let repeated = collect(4, 10, 7, false);
    assert_eq!(repeated.len(), 10);
    assert!(repeated.iter().all(|&index| index < 4));

    // 空集与零取样：无结果（C# Random.Next(0) 抛异常，按无结果处理）
    assert!(collect(0, 3, 7, true).is_empty());
    assert!(collect(10, 0, 7, true).is_empty());
  }
}
