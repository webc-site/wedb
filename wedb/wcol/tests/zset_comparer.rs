use std::cmp::Ordering;

use wcol::zset::comparer::SortedSetComparer;

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
