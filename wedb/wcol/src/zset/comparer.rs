//! 有序集合条目比较器（对标 libs/server/Objects/SortedSetComparer.cs）

use std::cmp::Ordering;

pub struct SortedSetComparer;

impl SortedSetComparer {
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
