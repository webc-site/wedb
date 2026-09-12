//! 保序变长整数编解码原语 (Order-Preserving Prefix Varint, OPPV)
//!
//! 专为存储引擎的有序复合键设计：
//! - 编码产物的字节字典序与原始 64 位整数的大小关系完全一致（`a < b <=> encode(a) < encode(b)`）；
//! - 自定界单调递增前缀：首字节即可判定总长度，杜绝反向回溯；
//! - 长度查找表 `VARINT_LEN_LUT` 编译期常量展开，单指令提取长度；
//! - 100% 支持 `const fn`，零堆分配。

use core::fmt;
use std::error::Error;

/// OPPV 单字节编码上限 (128)
pub const VARINT_1B_MAX: u64 = 128;
/// OPPV 双字节编码上限 (16,512)
pub const VARINT_2B_MAX: u64 = 16_512;
/// OPPV 三字节编码上限 (2,113,664)
pub const VARINT_3B_MAX: u64 = 2_113_664;
/// OPPV 四字节编码上限 (270,549,120)
pub const VARINT_4B_MAX: u64 = 270_549_120;

/// OPPV 九字节满位前缀标志位 (0xFF)
pub const VARINT_9B_MARKER: u8 = 0xFF;

/// OPPV 单字节首字节最大值 (0x7F)
pub const VARINT_1B_FIRST_BYTE_MAX: u8 = 0x7F;
/// OPPV 单字节首字节门限值 (0x80)，小于此门限必定为单字节 OPPV
pub const VARINT_1B_FIRST_BYTE_LIMIT: u8 = 0x80;

/// OPPV 双字节前缀标志位 (0x80) 与首字节最大值 (0xBF)
pub const VARINT_2B_MARKER: u8 = 0x80;
pub const VARINT_2B_FIRST_BYTE_MAX: u8 = 0xBF;
/// OPPV 双字节首字节有效载荷掩码 (6 位载荷, 0x3F)
pub const VARINT_2B_PAYLOAD_MASK: u8 = 0x3F;

/// OPPV 三字节前缀标志位 (0xC0) 与首字节最大值 (0xDF)
pub const VARINT_3B_MARKER: u8 = 0xC0;
pub const VARINT_3B_FIRST_BYTE_MAX: u8 = 0xDF;
/// OPPV 三字节首字节有效载荷掩码 (5 位载荷, 0x1F)
pub const VARINT_3B_PAYLOAD_MASK: u8 = 0x1F;

/// OPPV 四字节前缀标志位 (0xE0) 与首字节最大值 (0xEF)
pub const VARINT_4B_MARKER: u8 = 0xE0;
pub const VARINT_4B_FIRST_BYTE_MAX: u8 = 0xEF;
/// OPPV 四字节首字节有效载荷掩码 (4 位载荷, 0x0F)
pub const VARINT_4B_PAYLOAD_MASK: u8 = 0x0F;

/// 单个变长数值最大编码字节数 (9 字节)
pub const MAX_VARINT_LEN: usize = 9;

/// OPPV 单字节变长长度 256 字节只读查找表（编译期构造，常驻 L1 数据缓存）
///
/// 消除首字节分支判断与预测失败，0 分支 1 条指令完成长度提取（0 表示非法或非规范字节）
pub const VARINT_LEN_LUT: [u8; 256] = {
  let mut lut = [0u8; 256];
  let mut i = 0;
  while i < 256 {
    lut[i] = match i as u8 {
      0..=VARINT_1B_FIRST_BYTE_MAX => 1,
      VARINT_2B_MARKER..=VARINT_2B_FIRST_BYTE_MAX => 2,
      VARINT_3B_MARKER..=VARINT_3B_FIRST_BYTE_MAX => 3,
      VARINT_4B_MARKER..=VARINT_4B_FIRST_BYTE_MAX => 4,
      VARINT_9B_MARKER => MAX_VARINT_LEN as u8,
      _ => 0,
    };
    i += 1;
  }
  lut
};

// 编译期静态断言：验证变长整型查找表关键区间与边界值映射准确无误
const _: () = {
  assert!(VARINT_LEN_LUT[0] == 1);
  assert!(VARINT_LEN_LUT[VARINT_1B_FIRST_BYTE_MAX as usize] == 1);
  assert!(VARINT_LEN_LUT[VARINT_2B_MARKER as usize] == 2);
  assert!(VARINT_LEN_LUT[VARINT_2B_FIRST_BYTE_MAX as usize] == 2);
  assert!(VARINT_LEN_LUT[VARINT_3B_MARKER as usize] == 3);
  assert!(VARINT_LEN_LUT[VARINT_3B_FIRST_BYTE_MAX as usize] == 3);
  assert!(VARINT_LEN_LUT[VARINT_4B_MARKER as usize] == 4);
  assert!(VARINT_LEN_LUT[VARINT_4B_FIRST_BYTE_MAX as usize] == 4);
  assert!(VARINT_LEN_LUT[VARINT_9B_MARKER as usize] == 9);
};

/// 变长整数编解码错误
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VarintError {
  /// 缓冲区长度不足
  BufferTooShort { expected: usize, actual: usize },
  /// 变长整型编码非规范或存在冗余/非法前缀
  NonCanonical,
}

impl fmt::Display for VarintError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::BufferTooShort { expected, actual } => {
        write!(
          f,
          "变长整数缓冲区不足: 期望至少 {expected} 字节，实际仅 {actual} 字节"
        )
      }
      Self::NonCanonical => write!(f, "变长整数编码非规范或存在非法前缀"),
    }
  }
}

impl Error for VarintError {}

/// 计算单个 u64 数值经 OPPV 编码后占用的字节数 (1..=9, const fn)
#[inline(always)]
pub const fn varint_len(val: u64) -> usize {
  if val < VARINT_1B_MAX {
    1
  } else if val < VARINT_2B_MAX {
    2
  } else if val < VARINT_3B_MAX {
    3
  } else if val < VARINT_4B_MAX {
    4
  } else {
    MAX_VARINT_LEN
  }
}

/// 编码单一 u64 为 OPPV 变长字节数组 (const fn, 零堆分配)
///
/// 编码逻辑复用 [`encode_u64`] 单一真源；9 字节定长缓冲恒充足，不可能失败
#[inline]
pub const fn encode_u64_to_array(val: u64) -> ([u8; MAX_VARINT_LEN], usize) {
  let mut buf = [0u8; MAX_VARINT_LEN];
  match encode_u64(val, &mut buf) {
    Ok(len) => (buf, len),
    Err(_) => unreachable!(),
  }
}

/// 将单一 u64 编码直接写入目标切片 (const fn, 单次分支判定, 零中间数组拷贝)
#[inline]
pub const fn encode_u64(val: u64, dst: &mut [u8]) -> Result<usize, VarintError> {
  if val < VARINT_1B_MAX {
    if dst.is_empty() {
      return Err(VarintError::BufferTooShort {
        expected: 1,
        actual: 0,
      });
    }
    dst[0] = val as u8;
    Ok(1)
  } else if val < VARINT_2B_MAX {
    if dst.len() < 2 {
      return Err(VarintError::BufferTooShort {
        expected: 2,
        actual: dst.len(),
      });
    }
    let offset = (val - VARINT_1B_MAX) as u16;
    let bytes = offset.to_be_bytes();
    dst[0] = VARINT_2B_MARKER | bytes[0];
    dst[1] = bytes[1];
    Ok(2)
  } else if val < VARINT_3B_MAX {
    if dst.len() < 3 {
      return Err(VarintError::BufferTooShort {
        expected: 3,
        actual: dst.len(),
      });
    }
    let offset = (val - VARINT_2B_MAX) as u32;
    let bytes = offset.to_be_bytes();
    dst[0] = VARINT_3B_MARKER | bytes[1];
    dst[1] = bytes[2];
    dst[2] = bytes[3];
    Ok(3)
  } else if val < VARINT_4B_MAX {
    if dst.len() < 4 {
      return Err(VarintError::BufferTooShort {
        expected: 4,
        actual: dst.len(),
      });
    }
    let offset = (val - VARINT_3B_MAX) as u32;
    let bytes = offset.to_be_bytes();
    dst[0] = VARINT_4B_MARKER | bytes[0];
    dst[1] = bytes[1];
    dst[2] = bytes[2];
    dst[3] = bytes[3];
    Ok(4)
  } else {
    if dst.len() < MAX_VARINT_LEN {
      return Err(VarintError::BufferTooShort {
        expected: MAX_VARINT_LEN,
        actual: dst.len(),
      });
    }
    dst[0] = VARINT_9B_MARKER;
    let bytes = val.to_be_bytes();
    dst[1] = bytes[0];
    dst[2] = bytes[1];
    dst[3] = bytes[2];
    dst[4] = bytes[3];
    dst[5] = bytes[4];
    dst[6] = bytes[5];
    dst[7] = bytes[6];
    dst[8] = bytes[7];
    Ok(MAX_VARINT_LEN)
  }
}

/// 从只读切片零拷贝安全解码单一 OPPV 变长整型 (const fn)
///
/// 成功返回 `(数值, 消耗字节数)`。若切片不足或编码非规范则返回错误。
#[inline]
pub const fn decode_u64(slice: &[u8]) -> Result<(u64, usize), VarintError> {
  match slice {
    [b0, ..] if *b0 <= VARINT_1B_FIRST_BYTE_MAX => Ok((*b0 as u64, 1)),
    [b0, b1, ..] if *b0 >= VARINT_2B_MARKER && *b0 <= VARINT_2B_FIRST_BYTE_MAX => {
      let offset = u16::from_be_bytes([*b0 & VARINT_2B_PAYLOAD_MASK, *b1]);
      Ok((VARINT_1B_MAX + offset as u64, 2))
    }
    [b0, b1, b2, ..] if *b0 >= VARINT_3B_MARKER && *b0 <= VARINT_3B_FIRST_BYTE_MAX => {
      let offset = u32::from_be_bytes([0, *b0 & VARINT_3B_PAYLOAD_MASK, *b1, *b2]);
      Ok((VARINT_2B_MAX + offset as u64, 3))
    }
    [b0, b1, b2, b3, ..] if *b0 >= VARINT_4B_MARKER && *b0 <= VARINT_4B_FIRST_BYTE_MAX => {
      let offset = u32::from_be_bytes([*b0 & VARINT_4B_PAYLOAD_MASK, *b1, *b2, *b3]);
      Ok((VARINT_3B_MAX + offset as u64, 4))
    }
    [VARINT_9B_MARKER, b0, b1, b2, b3, b4, b5, b6, b7, ..] => {
      let val = u64::from_be_bytes([*b0, *b1, *b2, *b3, *b4, *b5, *b6, *b7]);
      if val < VARINT_4B_MAX {
        return Err(VarintError::NonCanonical);
      }
      Ok((val, MAX_VARINT_LEN))
    }
    [] => Err(VarintError::BufferTooShort {
      expected: 1,
      actual: 0,
    }),
    [b0, ..] => {
      let expected = VARINT_LEN_LUT[*b0 as usize] as usize;
      if expected == 0 {
        Err(VarintError::NonCanonical)
      } else {
        Err(VarintError::BufferTooShort {
          expected,
          actual: slice.len(),
        })
      }
    }
  }
}

/// 尝试从只读切片快速解码 OPPV 变长整型（const fn，失败返回 None）
#[inline(always)]
pub const fn decode_u64_opt(slice: &[u8]) -> Option<(u64, usize)> {
  match decode_u64(slice) {
    Ok(v) => Some(v),
    Err(_) => None,
  }
}
