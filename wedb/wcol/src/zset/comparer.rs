//! 有序集合条目比较器（对标 libs/server/Objects/SortedSetComparer.cs）
//!
//! 在 garnet 中的相对路径: libs/server/Storage/Session/ObjectStore/SortedSetObject.cs（成员比较器 ByteArrayWrapperComparer）

use std::cmp::Ordering;

pub struct SortedSetComparer;

impl SortedSetComparer {
  /// 分值 + 成员字典序（C#: x.Item1.CompareTo(y.Item1)，同分回退
  /// ReadOnlySpan<byte>.SequenceCompareTo）
  ///
  /// 全序口径逐字对位 .NET `Double.CompareTo`（非 total_cmp）：
  /// - `+0.0` 与 `-0.0` 相等（同分回退 member 字节序，不分先后）；
  /// - `NaN` 与任何值比较：双方皆 NaN 则相等，否则 NaN 小于一切非 NaN；
  /// - 其余按数值序。
  ///
  /// C# 侧 ZADD/INCRBYFLOAT 在命令层拒绝 NaN 分值，NaN 臂为比较器语义完备性
  /// 保留（对位 CompareTo 行为，而非 IEEE 全序）。
  ///
  /// libs/server/Objects/SortedSetComparer.cs:Compare
  #[inline]
  pub fn compare(
    (x_score, x_member): (&f64, &[u8]),
    (y_score, y_member): (&f64, &[u8]),
  ) -> Ordering {
    compare_to(*x_score, *y_score).then_with(|| x_member.cmp(y_member))
  }
}

/// C#: System.Double.CompareTo（< / > / == 三分支 + NaN 特判）
#[inline]
fn compare_to(x: f64, y: f64) -> Ordering {
  if x < y {
    Ordering::Less
  } else if x > y {
    Ordering::Greater
  } else if x == y {
    // ±0.0 走此臂（IEEE == 对 +0.0/-0.0 为真）
    Ordering::Equal
  } else if x.is_nan() {
    // NaN vs NaN 相等；NaN 小于任何非 NaN
    if y.is_nan() {
      Ordering::Equal
    } else {
      Ordering::Less
    }
  } else {
    Ordering::Greater
  }
}
