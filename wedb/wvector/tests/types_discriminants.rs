//! 向量共享枚举判别值与标志位语义测试（自 src/types.rs 内联测试外迁，零私有依赖）

use wvector::{
  Context, Term, VectorDistanceMetricType, VectorIdFormat, VectorQuantType, VectorSetFlags,
  VectorValueType, store::TERM_BITMASK,
};

#[test]
fn enum_discriminants_match_csharp() {
  assert_eq!(VectorQuantType::NoQuant as i32, 1);
  assert_eq!(VectorQuantType::Bin as i32, 2);
  assert_eq!(VectorQuantType::Q8 as i32, 3);
  assert_eq!(VectorQuantType::XnoQuantU8 as i32, 4);
  assert_eq!(VectorQuantType::XnoQuantI8 as i32, 5);
  assert_eq!(VectorQuantType::XbinI8 as i32, 6);
  assert_eq!(VectorQuantType::XbinU8 as i32, 7);

  assert_eq!(VectorValueType::FP32 as i32, 1);
  assert_eq!(VectorValueType::XU8 as i32, 2);
  assert_eq!(VectorValueType::XI8 as i32, 3);

  assert_eq!(VectorDistanceMetricType::Cosine as i32, 0);
  assert_eq!(VectorDistanceMetricType::InnerProduct as i32, 1);
  assert_eq!(VectorDistanceMetricType::L2 as i32, 2);
  assert_eq!(VectorDistanceMetricType::XCosineNormalized as i32, 3);

  assert_eq!(VectorIdFormat::I32LengthPrefixed as i32, 1);
}

#[test]
fn flags_semantics() {
  assert_eq!(VectorSetFlags::NONE.bits(), 0);
  assert!(!VectorSetFlags::NONE.contains(VectorSetFlags::SUPPRESS_CLEANUP));
  let f = VectorSetFlags::from_bits(1);
  assert!(f.contains(VectorSetFlags::SUPPRESS_CLEANUP));
  assert_eq!(f.union(VectorSetFlags::NONE).bits(), 1);
}

#[test]
fn term_or_masks_into_context() {
  let ctx = Context::new(8);
  assert_eq!(ctx.term(Term::Vector).inner(), 8);
  assert_eq!(ctx.term(Term::ExtMap).inner(), 8 | 6);
  assert_eq!(TERM_BITMASK, 7);
}
