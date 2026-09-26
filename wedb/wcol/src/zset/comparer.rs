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

#[cfg(test)]
mod tests {
  use super::*;

  const NAN: f64 = f64::NAN;

  #[test]
  fn normal_ordering() {
    assert_eq!(
      SortedSetComparer::compare((&1.0, b"a"), (&2.0, b"a")),
      Ordering::Less
    );
    assert_eq!(
      SortedSetComparer::compare((&2.0, b"a"), (&1.0, b"a")),
      Ordering::Greater
    );
    // 同分回退 member 字节序（C#: SequenceCompareTo）
    assert_eq!(
      SortedSetComparer::compare((&1.0, b"b"), (&1.0, b"a")),
      Ordering::Greater
    );
    assert_eq!(
      SortedSetComparer::compare((&1.0, b"a"), (&1.0, b"a")),
      Ordering::Equal
    );
  }

  /// C# Double.CompareTo：+0.0 与 -0.0 相等，同分回退 member（total_cmp 会判
  /// -0.0 < +0.0 不分 member，属需排除的口径）
  #[test]
  fn signed_zero_equal_falls_back_to_member() {
    assert_eq!(
      SortedSetComparer::compare((&0.0, b"a"), (&-0.0, b"a")),
      Ordering::Equal
    );
    // member 主导：(-0.0, "b") 排在 (0.0, "a") 之后
    assert_eq!(
      SortedSetComparer::compare((&-0.0, b"b"), (&0.0, b"a")),
      Ordering::Greater
    );
    assert_eq!(
      SortedSetComparer::compare((&0.0, b"a"), (&-0.0, b"b")),
      Ordering::Less
    );
  }

  /// C# Double.CompareTo：NaN vs NaN 相等；NaN 小于任何非 NaN（含 -inf）；
  /// 符号位不参与判序（total_cmp 会区分 -NaN/+NaN 位序，属需排除的口径）
  #[test]
  fn nan_ordering_matches_dotnet_compareto() {
    assert_eq!(
      SortedSetComparer::compare((&NAN, b"a"), (&NAN, b"a")),
      Ordering::Equal
    );
    assert_eq!(
      SortedSetComparer::compare((&NAN, b"b"), (&-NAN, b"a")),
      Ordering::Greater
    ); // NaN==NaN 同分后回退 member b > a
    assert_eq!(
      SortedSetComparer::compare((&NAN, b"a"), (&f64::NEG_INFINITY, b"a")),
      Ordering::Less
    );
    assert_eq!(
      // .NET: +inf.CompareTo(NaN) == 1（NaN 小于一切非 NaN，非 NaN 大于 NaN）
      SortedSetComparer::compare((&f64::INFINITY, b"a"), (&NAN, b"a")),
      Ordering::Greater
    );
    assert_eq!(
      SortedSetComparer::compare((&-NAN, b"a"), (&NAN, b"a")),
      Ordering::Equal
    );
  }
}
