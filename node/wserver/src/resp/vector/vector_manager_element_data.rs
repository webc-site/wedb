//! 向量数据格式规约（对标 libs/server/Resp/Vector/VectorManager.ElementData.cs）
//!
//! 量化器对输入有"原生"格式期望（Redis 兼容量化器 → F32；
//! X 系量化器 → XU8/XI8），即使格式匹配也要求按元素自然对齐。
//! C# 侧以 ArrayPool + GCHandle 固定对齐；Rust 侧字节切片天然对齐，
//! 故本模块聚焦格式转换与精度损失校验。

use super::vector_types::{VectorQuantType, VectorValueType};

/// 规约后的向量数据：目标格式字节 + 元素个数。
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedVectorData {
  /// 目标格式字节。
  pub bytes: Vec<u8>,
  /// 元素个数（非字节数）。
  pub element_count: usize,
}

/// 格式转换错误（对齐 C# out error 的 "ERR Vector contains element ..." 文案）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrepareError {
  /// 元素 < 0 或 > 255，转 u8 将损失精度。
  NotU8Range,
  /// 元素 < 0，转 u8 将损失精度。
  NegativeForU8,
  /// 元素 < -128 或 > 127，转 i8 将损失精度。
  NotI8Range,
  /// 元素 > 127，转 i8 将损失精度。
  OverI8Max,
}

impl PrepareError {
  /// 对齐 C# 错误文案。
  pub fn message(self) -> &'static [u8] {
    match self {
      PrepareError::NotU8Range => {
        b"ERR Vector contains element that is < 0 or > 255, operation will lose precision"
      }
      PrepareError::NegativeForU8 => {
        b"ERR Vector contains element that is < 0, operation will lose precision"
      }
      PrepareError::NotI8Range => {
        b"ERR Vector contains element that is < -128 or > 127, operation will lose precision"
      }
      PrepareError::OverI8Max => {
        b"ERR Vector contains element that is > 127, operation will lose precision"
      }
    }
  }
}

/// 量化器期望的原生格式。
pub fn native_format(quant: VectorQuantType) -> VectorValueType {
  match quant {
    // 所有 Redis 兼容量化器期望 F32 向量
    VectorQuantType::NoQuant | VectorQuantType::Q8 | VectorQuantType::Bin => VectorValueType::FP32,
    VectorQuantType::XNoQuant_U8 | VectorQuantType::XBin_U8 => VectorValueType::XU8,
    VectorQuantType::XNoQuant_I8 | VectorQuantType::XBin_I8 => VectorValueType::XI8,
    VectorQuantType::Invalid => VectorValueType::Invalid,
  }
}

/// libs/server/Resp/Vector/VectorManager.ElementData.cs:PrepareVectorData
///
/// 按量化器原生格式把 `value_type` 编码的输入规约为目标格式字节。
pub fn prepare_vector_data(
  quant: VectorQuantType,
  value_type: VectorValueType,
  provided: &[u8],
) -> Result<PreparedVectorData, PrepareError> {
  match quant {
    // 所有 Redis 兼容量化器期望 F32 向量
    VectorQuantType::NoQuant | VectorQuantType::Q8 | VectorQuantType::Bin => match value_type {
      VectorValueType::FP32 => Ok(convert_f32_for_alignment(provided)),
      VectorValueType::XI8 => Ok(convert_i8_to_f32(provided)),
      VectorValueType::XU8 => Ok(convert_u8_to_f32(provided)),
      VectorValueType::Invalid => Err(PrepareError::NotI8Range),
    },
    // XNoQuant_U8 / XBin_U8 期望 U8 向量
    VectorQuantType::XNoQuant_U8 | VectorQuantType::XBin_U8 => match value_type {
      VectorValueType::FP32 => convert_f32_to_u8(provided),
      VectorValueType::XI8 => convert_i8_to_u8(provided),
      VectorValueType::XU8 => Ok(pass_through(provided)),
      VectorValueType::Invalid => Err(PrepareError::NotI8Range),
    },
    // XNoQuant_I8 / XBin_I8 期望 I8 向量
    VectorQuantType::XNoQuant_I8 | VectorQuantType::XBin_I8 => match value_type {
      VectorValueType::FP32 => convert_f32_to_i8(provided),
      VectorValueType::XI8 => Ok(pass_through(provided)),
      VectorValueType::XU8 => convert_u8_to_i8(provided),
      VectorValueType::Invalid => Err(PrepareError::NotI8Range),
    },
    VectorQuantType::Invalid => Err(PrepareError::NotI8Range),
  }
}

/// 原样通过（格式已匹配且天然对齐）。
fn pass_through(data: &[u8]) -> PreparedVectorData {
  PreparedVectorData {
    bytes: data.to_vec(),
    element_count: data.len(),
  }
}

/// f32 → f32（对齐拷贝；Rust 切片天然 4 字节对齐可分配）。
fn convert_f32_for_alignment(data: &[u8]) -> PreparedVectorData {
  PreparedVectorData {
    bytes: data.to_vec(),
    element_count: data.len() / 4,
  }
}

/// libs/server/Resp/Vector/VectorManager.ElementData.cs:ConvertI8ToF32
/// i8 → f32 扩展。
fn convert_i8_to_f32(data: &[u8]) -> PreparedVectorData {
  let values: Vec<f32> = data
    .iter()
    .map(|b| f32::from(i8::from_le_bytes([*b])))
    .collect();
  PreparedVectorData {
    bytes: values.iter().flat_map(|v| v.to_le_bytes()).collect(),
    element_count: data.len(),
  }
}

/// libs/server/Resp/Vector/VectorManager.ElementData.cs:ConvertU8ToF32
/// u8 → f32 扩展。
fn convert_u8_to_f32(data: &[u8]) -> PreparedVectorData {
  let values: Vec<f32> = data.iter().map(|b| f32::from(*b)).collect();
  PreparedVectorData {
    bytes: values.iter().flat_map(|v| v.to_le_bytes()).collect(),
    element_count: data.len(),
  }
}

/// libs/server/Resp/Vector/VectorManager.ElementData.cs:ConvertF32ToU8
/// f32 → u8 截断；负数或 > 255 时报精度损失。
fn convert_f32_to_u8(data: &[u8]) -> Result<PreparedVectorData, PrepareError> {
  let values = f32_values(data);
  if values.iter().any(|v| !(0.0..=255.0).contains(v)) {
    return Err(PrepareError::NotU8Range);
  }
  Ok(PreparedVectorData {
    bytes: values.iter().map(|v| *v as u8).collect(),
    element_count: values.len(),
  })
}

/// libs/server/Resp/Vector/VectorManager.ElementData.cs:ConvertI8ToU8
/// i8 → u8 截断；负数时报精度损失。
fn convert_i8_to_u8(data: &[u8]) -> Result<PreparedVectorData, PrepareError> {
  if data.iter().any(|b| i8::from_le_bytes([*b]) < 0) {
    return Err(PrepareError::NegativeForU8);
  }
  Ok(PreparedVectorData {
    bytes: data.to_vec(),
    element_count: data.len(),
  })
}

/// libs/server/Resp/Vector/VectorManager.ElementData.cs:ConvertF32ToI8
/// f32 → i8 截断；超出 [-128, 127] 时报精度损失。
fn convert_f32_to_i8(data: &[u8]) -> Result<PreparedVectorData, PrepareError> {
  let values = f32_values(data);
  if values.iter().any(|v| !(-128.0..=127.0).contains(v)) {
    return Err(PrepareError::NotI8Range);
  }
  Ok(PreparedVectorData {
    bytes: values.iter().map(|v| *v as i8 as u8).collect(),
    element_count: values.len(),
  })
}

/// libs/server/Resp/Vector/VectorManager.ElementData.cs:ConvertU8ToI8
/// u8 → i8 截断；> 127 时报精度损失。
fn convert_u8_to_i8(data: &[u8]) -> Result<PreparedVectorData, PrepareError> {
  if data.iter().any(|b| *b > 127) {
    return Err(PrepareError::OverI8Max);
  }
  Ok(PreparedVectorData {
    bytes: data.to_vec(),
    element_count: data.len(),
  })
}

/// libs/server/Resp/Vector/VectorManager.ElementData.cs:ConvertF32ForAlignment
///（f32 字节切片直读）
fn f32_values(data: &[u8]) -> Vec<f32> {
  data
    .as_chunks::<4>()
    .0
    .iter()
    .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
    .collect()
}

#[cfg(test)]
mod tests {
  use super::*;

  fn f32_bytes(vals: &[f32]) -> Vec<u8> {
    vals.iter().flat_map(|v| v.to_le_bytes()).collect()
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
      native_format(VectorQuantType::XNoQuant_U8),
      VectorValueType::XU8
    );
    assert_eq!(
      native_format(VectorQuantType::XBin_I8),
      VectorValueType::XI8
    );
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
    let p =
      prepare_vector_data(VectorQuantType::NoQuant, VectorValueType::XI8, &[250u8, 3]).unwrap();
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
      VectorQuantType::XNoQuant_U8,
      VectorValueType::FP32,
      &f32_bytes(&[0.0, 255.0]),
    )
    .unwrap();
    assert_eq!(p.bytes, vec![0, 255]);
    // 越界
    assert_eq!(
      prepare_vector_data(
        VectorQuantType::XNoQuant_U8,
        VectorValueType::FP32,
        &f32_bytes(&[-1.0])
      )
      .unwrap_err(),
      PrepareError::NotU8Range
    );
    assert_eq!(
      prepare_vector_data(
        VectorQuantType::XNoQuant_U8,
        VectorValueType::FP32,
        &f32_bytes(&[256.0])
      )
      .unwrap_err(),
      PrepareError::NotU8Range
    );

    // I8 → U8
    let p = prepare_vector_data(VectorQuantType::XBin_U8, VectorValueType::XI8, &[3, 127]).unwrap();
    assert_eq!(p.bytes, vec![3, 127]);
    assert_eq!(
      prepare_vector_data(VectorQuantType::XNoQuant_U8, VectorValueType::XI8, &[255]).unwrap_err(),
      PrepareError::NegativeForU8
    );

    // U8 直通
    let p = prepare_vector_data(VectorQuantType::XBin_U8, VectorValueType::XU8, &[9, 8]).unwrap();
    assert_eq!(p.bytes, vec![9, 8]);
    assert_eq!(p.element_count, 2);
  }

  #[test]
  fn extended_i8_targets() {
    // FP32 → I8
    let p = prepare_vector_data(
      VectorQuantType::XNoQuant_I8,
      VectorValueType::FP32,
      &f32_bytes(&[-128.0, 127.0]),
    )
    .unwrap();
    assert_eq!(p.bytes, vec![128, 127]);
    assert_eq!(
      prepare_vector_data(
        VectorQuantType::XBin_I8,
        VectorValueType::FP32,
        &f32_bytes(&[127.5])
      )
      .unwrap_err(),
      PrepareError::NotI8Range
    );

    // U8 → I8
    assert_eq!(
      prepare_vector_data(VectorQuantType::XNoQuant_I8, VectorValueType::XU8, &[128]).unwrap_err(),
      PrepareError::OverI8Max
    );
    let p = prepare_vector_data(
      VectorQuantType::XNoQuant_I8,
      VectorValueType::XU8,
      &[0, 127],
    )
    .unwrap();
    assert_eq!(p.bytes, vec![0, 127]);

    // XI8 直通
    let p = prepare_vector_data(VectorQuantType::XBin_I8, VectorValueType::XI8, &[254, 2]).unwrap();
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
}
