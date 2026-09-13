//! 有序集合条目比较器（对标 libs/server/Objects/SortedSetComparer.cs）
//!
//! 排序键为 (score, member)：先按分值，分值相同再按成员字节序。
//! 排序视图（BTreeSet/BTreeMap range 查询）的正确性依赖本比较器。

use std::cmp::Ordering;

pub struct SortedSetComparer;

impl SortedSetComparer {
  /// 比较 (score, member) 二元组
  ///
  /// libs/server/Objects/SortedSetComparer.cs:Compare
  ///
  /// 刻意差异（对照 C#）：C# `double.CompareTo` 把 NaN 排在所有数值之前；
  /// Rust `f64::total_cmp` 把 NaN 排在所有数值之后（且 -NaN < +NaN）。
  /// Garnet 命令层禁止 NaN 分值入库（ZADD/ZINCRBY 的 NaN 直接报错），
  /// 故该差异不可观测。
  #[inline]
  pub fn compare(
    (x_score, x_member): (&f64, &[u8]),
    (y_score, y_member): (&f64, &[u8]),
  ) -> Ordering {
    x_score
      .total_cmp(y_score)
      .then_with(|| x_member.cmp(y_member))
  }
}
