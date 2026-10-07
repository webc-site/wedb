#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 随机下标采样分派回归（pick_k_random_indexes 三臂：拒绝采样 / 洗牌 / 放回）。
//!
//! k/n 阈值分派（C# RandomUtils.KOverNThreshold）：小比例走迭代/拒绝采样，
//! 达阈值才建全量洗牌。迁自 src/types/random_utils.rs 内联测试
//! （原依赖全 pub 才可直测）。

use wcol::types::random_utils::pick_k_random_indexes;

/// 以 sink 逐个收集断言序列形态
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
