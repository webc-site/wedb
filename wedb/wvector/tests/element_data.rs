//! 向量元素格式规约回归（原 src/element_data.rs 内联 mod tests 迁出，
//! 对照 libs/server/Resp/Vector/VectorManager.ElementData.cs 各 Convert 臂
//! 与 C# 错误文案）。

use wvector::{
  VectorQuantType, VectorValueType, element_data::PrepareError, native_format, prepare_vector_data,
};
use wvector_test::f32_bytes;

/// libs/server/Resp/Vector/VectorManager.ElementData.cs:ConvertF32ForAlignment
///（f32 字节切片直读）
fn f32_values(data: &[u8]) -> Vec<f32> {
  data
    .as_chunks::<4>()
    .0
    .iter()
    .map(|c| f32::from_le_bytes(*c))
    .collect()
}

#[test]
fn redis_quantizers_expect_f32() {
  // 三种输入格式 → F32
  for quant in [
    VectorQuantType::NoQuant,
    VectorQuantType::Q8,
    VectorQuantType::Bin,
  ] {
    assert_eq!(native_format(quant), VectorValueType::FP32);
  }
  assert_eq!(
    native_format(VectorQuantType::XnoQuantU8),
    VectorValueType::XU8
  );
  assert_eq!(native_format(VectorQuantType::XbinI8), VectorValueType::XI8);
  assert_eq!(
    native_format(VectorQuantType::Invalid),
    VectorValueType::Invalid
  );

  // FP32 直通
  let p = prepare_vector_data(
    VectorQuantType::NoQuant,
    VectorValueType::FP32,
    &f32_bytes(&[1.0, 2.0]),
  )
  .unwrap();
  assert_eq!(p.element_count, 2);

  // I8 扩展
  let p = prepare_vector_data(VectorQuantType::NoQuant, VectorValueType::XI8, &[250u8, 3]).unwrap();
  assert_eq!(p.element_count, 2);
  assert_eq!(f32_values(&p.bytes), vec![-6.0, 3.0]);

  // U8 扩展
  let p = prepare_vector_data(VectorQuantType::Q8, VectorValueType::XU8, &[0, 255]).unwrap();
  assert_eq!(f32_values(&p.bytes), vec![0.0, 255.0]);
}

#[test]
fn extended_u8_targets() {
  // FP32 → U8
  let p = prepare_vector_data(
    VectorQuantType::XnoQuantU8,
    VectorValueType::FP32,
    &f32_bytes(&[0.0, 255.0]),
  )
  .unwrap();
  assert_eq!(p.bytes, vec![0, 255]);
  // 越界
  assert_eq!(
    prepare_vector_data(
      VectorQuantType::XnoQuantU8,
      VectorValueType::FP32,
      &f32_bytes(&[-1.0])
    )
    .unwrap_err(),
    PrepareError::NotU8Range
  );
  assert_eq!(
    prepare_vector_data(
      VectorQuantType::XnoQuantU8,
      VectorValueType::FP32,
      &f32_bytes(&[256.0])
    )
    .unwrap_err(),
    PrepareError::NotU8Range
  );

  // I8 → U8
  let p = prepare_vector_data(VectorQuantType::XbinU8, VectorValueType::XI8, &[3, 127]).unwrap();
  assert_eq!(p.bytes, vec![3, 127]);
  assert_eq!(
    prepare_vector_data(VectorQuantType::XnoQuantU8, VectorValueType::XI8, &[255]).unwrap_err(),
    PrepareError::NegativeForU8
  );

  // U8 直通
  let p = prepare_vector_data(VectorQuantType::XbinU8, VectorValueType::XU8, &[9, 8]).unwrap();
  assert_eq!(p.bytes, vec![9, 8]);
  assert_eq!(p.element_count, 2);
}

#[test]
fn extended_i8_targets() {
  // FP32 → I8
  let p = prepare_vector_data(
    VectorQuantType::XnoQuantI8,
    VectorValueType::FP32,
    &f32_bytes(&[-128.0, 127.0]),
  )
  .unwrap();
  assert_eq!(p.bytes, vec![128, 127]);
  assert_eq!(
    prepare_vector_data(
      VectorQuantType::XbinI8,
      VectorValueType::FP32,
      &f32_bytes(&[127.5])
    )
    .unwrap_err(),
    PrepareError::NotI8Range
  );

  // U8 → I8
  assert_eq!(
    prepare_vector_data(VectorQuantType::XnoQuantI8, VectorValueType::XU8, &[128]).unwrap_err(),
    PrepareError::OverI8Max
  );
  let p =
    prepare_vector_data(VectorQuantType::XnoQuantI8, VectorValueType::XU8, &[0, 127]).unwrap();
  assert_eq!(p.bytes, vec![0, 127]);

  // XI8 直通
  let p = prepare_vector_data(VectorQuantType::XbinI8, VectorValueType::XI8, &[254, 2]).unwrap();
  assert_eq!(p.bytes, vec![254, 2]);
  assert_eq!(p.element_count, 2);
}

#[test]
fn error_messages_match_csharp() {
  assert!(
    PrepareError::NotU8Range
      .message()
      .starts_with(b"ERR Vector contains element that is < 0 or > 255")
  );
  assert!(
    PrepareError::NegativeForU8
      .message()
      .starts_with(b"ERR Vector contains element that is < 0,")
  );
  assert!(
    PrepareError::NotI8Range
      .message()
      .starts_with(b"ERR Vector contains element that is < -128 or > 127")
  );
  assert!(
    PrepareError::OverI8Max
      .message()
      .starts_with(b"ERR Vector contains element that is > 127")
  );
}
