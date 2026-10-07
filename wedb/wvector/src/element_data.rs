//! 向量数据格式规约（对标 diskann-garnet/VectorManager.ElementData.cs）
//!
//! 量化器对输入有"原生"格式期望（Redis 兼容量化器 → F32；
//! X 系量化器 → XU8/XI8），即使格式匹配也要求按元素自然对齐。
//! C# 侧以 ArrayPool + GCHandle 固定对齐；Rust 侧字节切片天然对齐，
//! 故本模块聚焦格式转换与精度损失校验。

use super::types::{VectorQuantType, VectorValueType};

/// 规约后的向量数据：目标格式字节 + 元素个数。
///
/// libs/server/Resp/Vector/VectorManager.ElementData.cs:Dispose 的承接：
/// C# PreparedVectorData 为 pinning 缓冲的 ref struct（Dispose 解除
/// GCHandle 固定）；rust 对位为自持 `Vec<u8>` 的值结构，随作用域自然
/// 释放，无固定句柄即无 Dispose 面。
#[derive(Debug, PartialEq)]
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
  pub const fn message(self) -> &'static [u8] {
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
pub const fn native_format(quant: VectorQuantType) -> VectorValueType {
  match quant {
    // 所有 Redis 兼容量化器期望 F32 向量
    VectorQuantType::NoQuant | VectorQuantType::Q8 | VectorQuantType::Bin => VectorValueType::FP32,
    VectorQuantType::XnoQuantU8 | VectorQuantType::XbinU8 => VectorValueType::XU8,
    VectorQuantType::XnoQuantI8 | VectorQuantType::XbinI8 => VectorValueType::XI8,
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
      VectorValueType::XI8 => Ok(convert_int_to_f32::<i8>(provided)),
      VectorValueType::XU8 => Ok(convert_int_to_f32::<u8>(provided)),
      VectorValueType::Invalid => Err(PrepareError::NotI8Range),
    },
    // XnoQuantU8 / XbinU8 期望 U8 向量
    VectorQuantType::XnoQuantU8 | VectorQuantType::XbinU8 => match value_type {
      VectorValueType::FP32 => convert_f32_to_u8(provided),
      VectorValueType::XI8 => convert_i8_to_u8(provided),
      VectorValueType::XU8 => Ok(pass_through(provided)),
      VectorValueType::Invalid => Err(PrepareError::NotI8Range),
    },
    // XnoQuantI8 / XbinI8 期望 I8 向量
    VectorQuantType::XnoQuantI8 | VectorQuantType::XbinI8 => match value_type {
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
/// libs/server/Resp/Vector/VectorManager.ElementData.cs:ConvertU8ToF32
///
/// i8/u8 → f32 扩展：字节按元素类型符号重解释后统一扩宽（单实现，消除双份循环）。
fn convert_int_to_f32<T: bytemuck::Pod>(data: &[u8]) -> PreparedVectorData
where
  f32: From<T>,
{
  let mut bytes = Vec::with_capacity(data.len() * 4);
  for &v in bytemuck::cast_slice::<u8, T>(data) {
    bytes.extend_from_slice(&f32::from(v).to_le_bytes());
  }
  PreparedVectorData {
    bytes,
    element_count: data.len(),
  }
}

/// libs/server/Resp/Vector/VectorManager.ElementData.cs:ConvertF32ToU8
/// f32 → u8 截断；负数或 > 255 时报精度损失。
fn convert_f32_to_u8(data: &[u8]) -> Result<PreparedVectorData, PrepareError> {
  let chunks = data.as_chunks::<4>().0;
  let mut bytes = Vec::with_capacity(chunks.len());
  for c in chunks {
    let v = f32::from_le_bytes(*c);
    if !(0.0..=255.0).contains(&v) {
      return Err(PrepareError::NotU8Range);
    }
    bytes.push(v as u8);
  }
  Ok(PreparedVectorData {
    bytes,
    element_count: chunks.len(),
  })
}

/// libs/server/Resp/Vector/VectorManager.ElementData.cs:ConvertI8ToU8
/// i8 → u8 截断；负数时报精度损失。
fn convert_i8_to_u8(data: &[u8]) -> Result<PreparedVectorData, PrepareError> {
  if data.iter().any(|b| (*b as i8) < 0) {
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
  let chunks = data.as_chunks::<4>().0;
  let mut bytes = Vec::with_capacity(chunks.len());
  for c in chunks {
    let v = f32::from_le_bytes(*c);
    if !(-128.0..=127.0).contains(&v) {
      return Err(PrepareError::NotI8Range);
    }
    bytes.push(v as i8 as u8);
  }
  Ok(PreparedVectorData {
    bytes,
    element_count: chunks.len(),
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
