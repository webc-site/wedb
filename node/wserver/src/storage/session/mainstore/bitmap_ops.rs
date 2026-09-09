//! 位图操作（对标 libs/server/Storage/Session/MainStore/BitmapOps.cs，C# 为 StorageSession partial）
//!
//! 位图按字节序布局在字符串值上（Redis 语义：第 offset 位 = 第 offset/8 字节的
//! MSB 起第 offset%8 位），读-改-写经 [`StorageSession`] 字符串路径闭环。

use wdev::Device;

use super::super::storage_session::StorageSession;
use crate::api::garnet_status::GarnetStatus;

/// 位运算种类（libs/server/Objects/Bitmap/BitmapOperation.cs 语义）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitmapOp {
  /// 按位与
  And,
  /// 按位或
  Or,
  /// 按位异或
  Xor,
  /// 按位取反（单键）
  Not,
}

/// BITFIELD 子操作
#[derive(Debug, Clone, Copy)]
pub enum BitFieldOp {
  /// 读取指定类型位域
  Get {
    is_signed: bool,
    bits: u8,
    offset: u64,
  },
  /// 写入指定类型位域，回吐旧值
  Set {
    is_signed: bool,
    bits: u8,
    offset: u64,
    value: i64,
    wrap: bool,
    sat: bool,
  },
  /// 位域自增并回吐新值
  IncrBy {
    is_signed: bool,
    bits: u8,
    offset: u64,
    increment: i64,
    wrap: bool,
    sat: bool,
  },
}

impl<'a, D: Device> StorageSession<'a, D> {
  /// SETBIT：置位并回吐旧位值
  ///
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:StringSetBit
  pub async fn string_set_bit(
    &self,
    key: &[u8],
    offset: u64,
    bit: u8,
  ) -> wkv::Result<(GarnetStatus, u8)> {
    let byte_idx = (offset / 8) as usize;
    let mut buf = self.read_string(key).await?.unwrap_or_default();
    if buf.len() <= byte_idx {
      buf.resize(byte_idx + 1, 0);
    }
    let mask = 0x80u8 >> (offset % 8);
    let old = u8::from(buf[byte_idx] & mask != 0);
    if bit != 0 {
      buf[byte_idx] |= mask;
    } else {
      buf[byte_idx] &= !mask;
    }
    self.upsert_string(key, &buf).await?;
    Ok((GarnetStatus::Ok, old))
  }

  /// GETBIT：读位
  ///
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:StringGetBit
  pub async fn string_get_bit(&self, key: &[u8], offset: u64) -> wkv::Result<(GarnetStatus, u8)> {
    let Some(buf) = self.read_string(key).await? else {
      return Ok((GarnetStatus::NotFound, 0));
    };
    let byte_idx = (offset / 8) as usize;
    if byte_idx >= buf.len() {
      return Ok((GarnetStatus::Ok, 0));
    }
    let bit = u8::from(buf[byte_idx] & (0x80u8 >> (offset % 8)) != 0);
    Ok((GarnetStatus::Ok, bit))
  }

  /// BITOP：多键位运算写入目标键，返回目标串字节长度
  ///
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:StringBitOperation
  pub async fn string_bit_operation(
    &self,
    op: BitmapOp,
    dest: &[u8],
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    // NOT 仅支持单键取反
    if op == BitmapOp::Not {
      let Some(key) = keys.first() else {
        return Ok((GarnetStatus::NotFound, 0));
      };
      let flipped: Vec<u8> = self
        .read_string(key)
        .await?
        .unwrap_or_default()
        .iter()
        .map(|b| !b)
        .collect();
      let len = flipped.len();
      self.upsert_string(dest, &flipped).await?;
      return Ok((GarnetStatus::Ok, len));
    }
    // AND/OR/XOR：短串缺位按 0 补齐，逐字节折叠
    let mut acc: Vec<u8> = Vec::new();
    for key in keys {
      let buf = self.read_string(key).await?.unwrap_or_default();
      let long = acc.len().max(buf.len());
      let mut merged = vec![0u8; long];
      for (i, slot) in merged.iter_mut().enumerate() {
        let x = acc.get(i).copied().unwrap_or(0);
        let y = buf.get(i).copied().unwrap_or(0);
        *slot = match op {
          BitmapOp::And => x & y,
          BitmapOp::Or => x | y,
          BitmapOp::Xor => x ^ y,
          BitmapOp::Not => unreachable!("NOT 分支已提前返回"),
        };
      }
      acc = merged;
    }
    let len = acc.len();
    self.upsert_string(dest, &acc).await?;
    Ok((GarnetStatus::Ok, len))
  }

  /// 释放位运算溢出缓冲
  ///
  /// 缺口说明：C# 侧归还 BitmapOps 内部的 SpanAndMemory 溢出缓冲；Rust 侧
  /// 中间结果由所有权与 Drop 自动回收，无显式缓冲可还，退化为空操作。
  ///
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:ReleaseOverflowBuffers
  pub fn release_overflow_buffers(&self) {}

  /// BITCOUNT：区间位计数（`mode` 为 false 按字节区间，为 true 按位区间）
  ///
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:StringBitCount
  pub async fn string_bit_count(
    &self,
    key: &[u8],
    start: i64,
    end: i64,
    bit_mode: bool,
  ) -> wkv::Result<(GarnetStatus, u64)> {
    let Some(buf) = self.read_string(key).await? else {
      return Ok((GarnetStatus::NotFound, 0));
    };
    let total_bits = buf.len() as i64 * 8;
    let (lo, hi) = normalize_range(
      start,
      end,
      if bit_mode {
        total_bits
      } else {
        buf.len() as i64
      },
    );
    // 空区间（含空值键）：计数恒 0，避免空缓冲切片越界
    if lo > hi {
      return Ok((GarnetStatus::Ok, 0));
    }
    if bit_mode {
      let mut n = 0u64;
      for bit in lo..=hi {
        let byte = buf[(bit / 8) as usize];
        n += u64::from(byte & (0x80u8 >> (bit % 8)) != 0);
      }
      Ok((GarnetStatus::Ok, n))
    } else {
      let mut n = 0u64;
      for &b in &buf[lo as usize..=hi as usize] {
        n += u64::from(b.count_ones());
      }
      Ok((GarnetStatus::Ok, n))
    }
  }

  /// BITPOS：查找首个指定位（区间限定），无命中返回 -1
  ///
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:StringBitPosition
  pub async fn string_bit_position(
    &self,
    key: &[u8],
    bit: u8,
    start: i64,
    end: i64,
    bit_mode: bool,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    let Some(buf) = self.read_string(key).await? else {
      return Ok((GarnetStatus::NotFound, -1));
    };
    let total_bits = buf.len() as i64 * 8;
    let (lo, hi) = normalize_range(
      start,
      end,
      if bit_mode {
        total_bits
      } else {
        buf.len() as i64
      },
    );
    let want = bit != 0;
    for bit_idx in lo..=hi {
      let byte = buf[(bit_idx / 8) as usize];
      if (byte & (0x80u8 >> (bit_idx % 8)) != 0) == want {
        return Ok((GarnetStatus::Ok, bit_idx));
      }
    }
    Ok((GarnetStatus::Ok, -1))
  }

  /// BITFIELD 写路径（SET/INCRBY，支持 WRAP/SAT 溢出策略），回吐每个子操作结果
  ///
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:StringBitField
  pub async fn string_bit_field(
    &self,
    key: &[u8],
    ops: &[BitFieldOp],
  ) -> wkv::Result<(GarnetStatus, Vec<Option<i64>>)> {
    let mut buf = self.read_string(key).await?.unwrap_or_default();
    let mut results = Vec::with_capacity(ops.len());
    for op in ops {
      match *op {
        BitFieldOp::Get { .. } => {
          let r = bit_field_apply(&mut buf, *op);
          results.push(r);
        }
        _ => {
          let r = bit_field_apply(&mut buf, *op);
          results.push(r);
          self.upsert_string(key, &buf).await?;
        }
      }
    }
    Ok((GarnetStatus::Ok, results))
  }

  /// BITFIELD 只读路径（仅 GET 子操作）
  ///
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:StringBitFieldReadOnly
  pub async fn string_bit_field_read_only(
    &self,
    key: &[u8],
    gets: &[(bool, u8, u64)],
  ) -> wkv::Result<(GarnetStatus, Vec<Option<i64>>)> {
    let Some(buf) = self.read_string(key).await? else {
      return Ok((GarnetStatus::NotFound, gets.iter().map(|_| None).collect()));
    };
    let mut out = Vec::with_capacity(gets.len());
    let mut scratch = buf;
    for &(is_signed, bits, offset) in gets {
      out.push(bit_field_apply(
        &mut scratch,
        BitFieldOp::Get {
          is_signed,
          bits,
          offset,
        },
      ));
    }
    Ok((GarnetStatus::Ok, out))
  }
}

/// 将 Redis 区间参数归一化为非负闭区间 [lo, hi]（负数自尾部计数）
pub(crate) fn normalize_range(start: i64, end: i64, total: i64) -> (i64, i64) {
  let s = if start < 0 { total + start } else { start }.max(0);
  let e = if end < 0 { total + end } else { end }.min(total - 1);
  if s > e || total == 0 {
    (1, 0) // 空区间哨兵：lo > hi
  } else {
    (s, e)
  }
}

/// 对缓冲执行单个 BITFIELD 子操作，返回结果值（溢出 FAIL 时为 None）
fn bit_field_apply(buf: &mut Vec<u8>, op: BitFieldOp) -> Option<i64> {
  let (is_signed, bits, offset, is_write, _value, wrap, sat) = match op {
    BitFieldOp::Get {
      is_signed,
      bits,
      offset,
    } => (is_signed, bits, offset, false, 0i64, false, false),
    BitFieldOp::Set {
      is_signed,
      bits,
      offset,
      value,
      wrap,
      sat,
    } => (is_signed, bits, offset, true, value, wrap, sat),
    BitFieldOp::IncrBy {
      is_signed,
      bits,
      offset,
      increment,
      wrap,
      sat,
    } => (is_signed, bits, offset, true, increment, wrap, sat),
  };
  let byte_idx = (offset / 8) as usize;
  // 位域末端字节：写路径须把缓冲增长到覆盖整个位域（对齐 C# RESP 层
  // NewBlockAllocLengthFromType 的写前增长），只长到首字节会把高位截断丢失
  if is_write {
    let end_byte = ((offset + u64::from(bits) - 1) / 8) as usize;
    if end_byte >= buf.len() {
      buf.resize(end_byte + 1, 0);
    }
  } else if byte_idx >= buf.len() {
    // 读路径：位域起点整体越界恒 0（对齐 C# GetBitfield）
    return Some(0);
  }
  // 逐位读出当前位域：越界位按 0 续位（对齐 C# GetValue 的 vend 截断语义，
  // 缺失位必须保持原位参与移位，而非整体截断）
  let mut current: i64 = 0;
  for k in 0..bits {
    let pos = offset + u64::from(k);
    let idx = (pos / 8) as usize;
    let bit = idx < buf.len() && buf[idx] & (0x80u8 >> (pos % 8)) != 0;
    current = (current << 1) | i64::from(bit);
  }
  // 符号扩展
  let old = if is_signed && bits < 64 {
    let shift = 64 - bits;
    (current << shift) >> shift
  } else {
    current
  };
  if !is_write {
    return Some(old);
  }
  let new_val = match op {
    BitFieldOp::Set { value, .. } => value,
    BitFieldOp::IncrBy { increment, .. } => old.wrapping_add(increment),
    _ => return Some(old),
  };
  // 位域可表示范围（bits==64 时全 i64 恒不溢出）
  let (min, max) = if bits >= 64 {
    (i64::MIN, i64::MAX)
  } else if is_signed {
    (-(1i64 << (bits - 1)), (1i64 << (bits - 1)) - 1)
  } else {
    (0, (1i64 << bits) - 1)
  };
  let (stored, result) = if (min..=max).contains(&new_val) {
    (new_val, new_val)
  } else if wrap {
    // WRAP：截断到位宽后有符号域做符号扩展
    let mask = if bits >= 64 {
      -1i64
    } else {
      (1i64 << bits) - 1
    };
    let masked = new_val & mask;
    let sign_ext = if is_signed && bits < 64 && (masked >> (bits - 1)) & 1 == 1 {
      masked | !mask
    } else {
      masked
    };
    (sign_ext, sign_ext)
  } else if sat {
    let clamped = new_val.clamp(min, max);
    (clamped, clamped)
  } else {
    return None; // FAIL：放弃写入，本子操作结果为 nil
  };
  // 逐位写回
  for k in 0..bits {
    let pos = offset + u64::from(k);
    let idx = (pos / 8) as usize;
    if idx >= buf.len() {
      break;
    }
    let bit_set = (stored >> (bits - 1 - k)) & 1 == 1;
    if bit_set {
      buf[idx] |= 0x80u8 >> (pos % 8);
    } else {
      buf[idx] &= !(0x80u8 >> (pos % 8));
    }
  }
  Some(result)
}

#[cfg(test)]
mod tests {
  use super::{BitFieldOp, bit_field_apply};

  /// SET 跨越缓冲末端：整域必须落盘（修复只增长首字节的高位截断）
  #[test]
  fn set_extends_buffer_to_full_field_extent() {
    let mut buf = Vec::new(); // 键缺失等价空缓冲
    let r = bit_field_apply(
      &mut buf,
      BitFieldOp::Set {
        is_signed: false,
        bits: 16,
        offset: 0,
        value: 300,
        wrap: false,
        sat: false,
      },
    );
    assert_eq!(r, Some(300));
    assert_eq!(buf, vec![0x01, 0x2C]); // 300 = 0x012C
    // 写后读回须全域一致
    let r = bit_field_apply(
      &mut buf,
      BitFieldOp::Get {
        is_signed: false,
        bits: 16,
        offset: 0,
      },
    );
    assert_eq!(r, Some(300));
  }

  /// GET 部分跨越值末端：可得位按实读取、缺失位补 0（修复整体返 0 的误判）
  #[test]
  fn get_partial_overlap_reads_available_bits() {
    let mut buf = vec![0xFFu8];
    let r = bit_field_apply(
      &mut buf,
      BitFieldOp::Get {
        is_signed: false,
        bits: 8,
        offset: 4,
      },
    );
    assert_eq!(r, Some(0b1111_0000));
    let r = bit_field_apply(
      &mut buf,
      BitFieldOp::Get {
        is_signed: true,
        bits: 8,
        offset: 4,
      },
    );
    assert_eq!(r, Some(-16)); // 0b11110000 作 i8 符号扩展
    // 起点整体越界仍恒 0
    let r = bit_field_apply(
      &mut buf,
      BitFieldOp::Get {
        is_signed: false,
        bits: 16,
        offset: 8,
      },
    );
    assert_eq!(r, Some(0));
  }

  /// 非对齐 SET 写回：邻位不受扰动，域后缓冲按需增长
  #[test]
  fn set_unaligned_neighbors_untouched() {
    let mut buf = vec![0x00, 0xFF];
    let r = bit_field_apply(
      &mut buf,
      BitFieldOp::Set {
        is_signed: false,
        bits: 8,
        offset: 4,
        value: 0xAB,
        wrap: false,
        sat: false,
      },
    );
    assert_eq!(r, Some(0xAB));
    // 低半字节保留 0x0，位 4..12 = 0xAB，位 12..16 保留 0xF
    assert_eq!(buf, vec![0x0A, 0xBF]);
  }

  /// FAIL 溢出：返回 None 且不落任何位
  #[test]
  fn incr_overflow_fail_writes_nothing() {
    let mut buf = vec![0x00];
    let r = bit_field_apply(
      &mut buf,
      BitFieldOp::IncrBy {
        is_signed: false,
        bits: 8,
        offset: 0,
        increment: 300,
        wrap: false,
        sat: false,
      },
    );
    assert_eq!(r, None);
    assert_eq!(buf, vec![0x00]);
  }
}
