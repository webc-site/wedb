//! 位图操作（对标 libs/server/Storage/Session/MainStore/BitmapOps.cs，C# 为 StorageSession partial）
//!
//! 位图按字节序布局在字符串值上（Redis 语义：第 offset 位 = 第 offset/8 字节的
//! MSB 起第 offset%8 位），读-改-写经 [`StorageSession`] 字符串路径闭环。

use wdev::Device;

use super::super::storage_session::StorageSession;
use crate::{
  bitmap::{
    bitmap_manager_bit_op::{BitmapOperation, invoke_bit_operation_unsafe},
    bitmap_manager_bitfield::{
      BIT_FIELD_SIGN_SIGNED, BitFieldCmdArgs, BitFieldOverflow, BitFieldSecondaryCommand,
      bit_field_execute, new_block_alloc_length_from_type,
    },
  },
  types::GarnetStatus,
};

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
  /// 差集（首源对其余源按位清除）
  Diff,
}

impl From<BitmapOp> for BitmapOperation {
  fn from(op: BitmapOp) -> Self {
    match op {
      BitmapOp::And => BitmapOperation::And,
      BitmapOp::Or => BitmapOperation::Or,
      BitmapOp::Xor => BitmapOperation::Xor,
      BitmapOp::Not => BitmapOperation::Not,
      BitmapOp::Diff => BitmapOperation::Diff,
    }
  }
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

impl<'a, D: Device, CR: wkv::ConsistentReadFunctions> StorageSession<'a, D, CR> {
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
    let byte_idx = (offset / 8) as usize;
    let mask = 0x80u8 >> (offset % 8);
    let res = self
      .read_string_with(key, |buf| {
        if byte_idx >= buf.len() {
          0
        } else {
          u8::from(buf[byte_idx] & mask != 0)
        }
      })
      .await?;
    match res {
      Some(bit) => Ok((GarnetStatus::Ok, bit)),
      None => Ok((GarnetStatus::NotFound, 0)),
    }
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
    if op == BitmapOp::Not {
      let Some(key) = keys.first() else {
        return Ok((GarnetStatus::NotFound, 0));
      };
      if keys.len() != 1 {
        return Ok((GarnetStatus::NotFound, 0));
      }
      let Some(src) = self.read_string(key).await? else {
        return Ok((GarnetStatus::Ok, 0));
      };
      let mut dst = vec![0u8; src.len()];
      let _ = invoke_bit_operation_unsafe(BitmapOperation::Not, &[&src], &mut dst, src.len());
      let len = dst.len();
      self.upsert_string(dest, &dst).await?;
      return Ok((GarnetStatus::Ok, len));
    }

    if op == BitmapOp::Diff && keys.len() < 2 {
      return Ok((GarnetStatus::NotFound, 0));
    }

    // 读源键，缺失键跳过（C# StringBitOperation 中 NOTFOUND continue 语义）
    let mut srcs: Vec<Vec<u8>> = Vec::with_capacity(keys.len());
    let mut min_len = usize::MAX;
    let mut max_len = 0;
    for key in keys {
      if let Some(buf) = self.read_string(key).await? {
        min_len = min_len.min(buf.len());
        max_len = max_len.max(buf.len());
        srcs.push(buf);
      }
    }

    // 若全部源键缺失，对齐 C#：回 OK，长度 0
    if srcs.is_empty() {
      return Ok((GarnetStatus::Ok, 0));
    }

    if op == BitmapOp::Diff && srcs.len() < 2 {
      return Ok((GarnetStatus::Ok, 0));
    }

    let slices: Vec<&[u8]> = srcs.iter().map(|s| s.as_slice()).collect();
    let mut dst = vec![0u8; max_len];
    if invoke_bit_operation_unsafe(op.into(), &slices, &mut dst, min_len).is_err() {
      return Ok((GarnetStatus::Ok, 0));
    }

    if max_len > 0 {
      self.upsert_string(dest, &dst).await?;
    }
    Ok((GarnetStatus::Ok, max_len))
  }

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
    let res = self
      .read_string_with(key, |buf| {
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
          return 0u64;
        }
        if bit_mode {
          let mut n = 0u64;
          for bit in lo..=hi {
            let byte = buf[(bit / 8) as usize];
            n += u64::from(byte & (0x80u8 >> (bit % 8)) != 0);
          }
          n
        } else {
          count_ones_slice(&buf[lo as usize..=hi as usize])
        }
      })
      .await?;
    match res {
      Some(n) => Ok((GarnetStatus::Ok, n)),
      None => Ok((GarnetStatus::NotFound, 0)),
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
    let res = self
      .read_string_with(key, |buf| {
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
            return bit_idx;
          }
        }
        -1
      })
      .await?;
    match res {
      Some(pos) => Ok((GarnetStatus::Ok, pos)),
      None => Ok((GarnetStatus::NotFound, -1)),
    }
  }

  /// BITFIELD 写路径（SET/INCRBY，支持 WRAP/SAT/FAIL 溢出策略），回吐每个子操作结果
  ///
  /// 写子操作共享同一缓冲就地变更，循环结束后统一落盘一次（对标 C# RMW
  /// 单次写回，避免逐子操作全量重写）。
  ///
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:StringBitField
  pub async fn string_bit_field(
    &self,
    key: &[u8],
    ops: &[BitFieldOp],
  ) -> wkv::Result<(GarnetStatus, Vec<Option<i64>>)> {
    let mut buf = self.read_string(key).await?.unwrap_or_default();
    let mut results = Vec::with_capacity(ops.len());
    let mut dirty = false;
    for op in ops {
      let is_write = !matches!(op, BitFieldOp::Get { .. });
      let r = bit_field_apply(&mut buf, *op);
      results.push(r);
      dirty |= is_write;
    }
    if dirty {
      self.upsert_string(key, &buf).await?;
    }
    Ok((GarnetStatus::Ok, results))
  }

  /// BITFIELD 只读路径（仅 GET 子操作，零拷贝读出）
  ///
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:StringBitFieldReadOnly
  pub async fn string_bit_field_read_only(
    &self,
    key: &[u8],
    gets: &[(bool, u8, u64)],
  ) -> wkv::Result<(GarnetStatus, Vec<Option<i64>>)> {
    let res = self
      .read_string_with(key, |buf| {
        let mut out = Vec::with_capacity(gets.len());
        for &(is_signed, bits, offset) in gets {
          out.push(Some(bit_field_get(buf, is_signed, bits, offset)));
        }
        out
      })
      .await?;
    match res {
      Some(out) => Ok((GarnetStatus::Ok, out)),
      None => Ok((GarnetStatus::NotFound, gets.iter().map(|_| None).collect())),
    }
  }
}

/// 计算切片中的置位总数（u64 8 字节字批处理 + POPCNT 硬件加速）
#[inline]
fn count_ones_slice(slice: &[u8]) -> u64 {
  let mut count = 0u64;
  let (chunks, remainder) = slice.as_chunks::<8>();
  for chunk in chunks {
    let val = u64::from_ne_bytes(*chunk);
    count += val.count_ones() as u64;
  }
  for &b in remainder {
    count += b.count_ones() as u64;
  }
  count
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

/// 读取单个 BITFIELD 值（只读，零拷贝）
pub(crate) fn bit_field_get(buf: &[u8], is_signed: bool, bits: u8, offset: u64) -> i64 {
  let byte_idx = (offset / 8) as usize;
  if byte_idx >= buf.len() {
    return 0;
  }
  let mut current: i64 = 0;
  for k in 0..bits {
    let pos = offset + u64::from(k);
    let idx = (pos / 8) as usize;
    let bit = idx < buf.len() && buf[idx] & (0x80u8 >> (pos % 8)) != 0;
    current = (current << 1) | i64::from(bit);
  }
  if is_signed && bits < 64 {
    let shift = 64 - bits;
    (current << shift) >> shift
  } else {
    current
  }
}

/// 对缓冲执行单个 BITFIELD 子操作，返回结果值（FAIL 溢出时为 None）
///
/// 溢出判定与写入全量复用 RESP 层 [`bit_field_execute`]（与 C#
/// BitmapManager.BitFieldExecute 同源，BitmapManagerBitfield.cs）：
/// - SET 恒截断到位宽并回旧值——C# SetBitfield 的 FAIL 复核对域内旧值恒为
///   假，绝不失败（RMWMethods.cs:180 InitialUpdater 经 SetValue 掩码写入）；
/// - INCRBY 溢出时 C# IncrementBitfield 照旧把回绕/饱和/零值写入记录，仅
///   应答层以 nil 替代——故缓冲变更保留、结果回 None（RMWMethods.cs:658）。
fn bit_field_apply(buf: &mut Vec<u8>, op: BitFieldOp) -> Option<i64> {
  let (cmd, is_signed, bits, offset, value) = match op {
    BitFieldOp::Get {
      is_signed,
      bits,
      offset,
    } => (BitFieldSecondaryCommand::Get, is_signed, bits, offset, 0),
    BitFieldOp::Set {
      is_signed,
      bits,
      offset,
      value,
      ..
    } => (
      BitFieldSecondaryCommand::Set,
      is_signed,
      bits,
      offset,
      value,
    ),
    BitFieldOp::IncrBy {
      is_signed,
      bits,
      offset,
      increment,
      ..
    } => (
      BitFieldSecondaryCommand::IncrBy,
      is_signed,
      bits,
      offset,
      increment,
    ),
  };
  if cmd == BitFieldSecondaryCommand::Get {
    return Some(bit_field_get(buf, is_signed, bits, offset));
  }

  // 溢出策略：wrap→WRAP / sat→SAT / 皆否→FAIL（C# BitFieldOverflow 枚举序）
  let overflow_type = match op {
    BitFieldOp::Get { .. } => BitFieldOverflow::Wrap,
    BitFieldOp::Set { wrap, sat, .. } | BitFieldOp::IncrBy { wrap, sat, .. } => {
      if wrap {
        BitFieldOverflow::Wrap
      } else if sat {
        BitFieldOverflow::Sat
      } else {
        BitFieldOverflow::Fail
      }
    }
  };

  // API 层 offset 为已解析的无符号位偏移（`#` 倍乘形态仅存在于 RESP 解析期；
  // 合法性由 bit_field_execute 内 TryValidateBitfieldOffset 同源校验把关）
  let offset = i64::try_from(offset).ok()?;
  let type_info = if is_signed {
    BIT_FIELD_SIGN_SIGNED | bits
  } else {
    bits
  };
  let args = BitFieldCmdArgs::new(cmd, type_info, offset, value, overflow_type as u8);

  // 写前增长到位域覆盖（C# InitialUpdater/CopyUpdater 的 LengthFromType 增长，
  // 只长到首字节会截断域高位）
  let need = new_block_alloc_length_from_type(&args, buf.len() as i32) as usize;
  if buf.len() < need {
    buf.resize(need, 0);
  }

  match bit_field_execute(&args, buf) {
    Some((v, false)) => Some(v),
    // FAIL 溢出：缓冲已按 C# 语义写入零值，仅应答 nil
    _ => None,
  }
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
    assert_eq!(r, Some(0)); // SET 回旧值（C# SetBitfield 返回 oldValue）
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
    assert_eq!(r, Some(15)); // SET 回旧值：位 4..12 原值 = 0b0000_1111
    // 低半字节保留 0x0，位 4..12 = 0xAB，位 12..16 保留 0xF
    assert_eq!(buf, vec![0x0A, 0xBF]);
  }

  /// SET 越界值恒截断到位宽、绝不 FAIL（C# SetBitfield 的 FAIL 复核恒假）
  #[test]
  fn set_out_of_range_value_truncates_never_fails() {
    let mut buf = vec![0x00];
    let r = bit_field_apply(
      &mut buf,
      BitFieldOp::Set {
        is_signed: false,
        bits: 4,
        offset: 0,
        value: 300, // 300 & 0xF = 0xC
        wrap: false,
        sat: false,
      },
    );
    assert_eq!(r, Some(0)); // 回旧值，不失败
    assert_eq!(buf, vec![0xC0]); // 截断位宽落盘
  }

  /// FAIL 溢出：应答 None，域照 C# IncrementBitfield 写入零值（BitmapManagerBitfield.cs）
  #[test]
  fn incr_overflow_fail_zeroes_field() {
    let mut buf = vec![0xFA]; // 250
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
    assert_eq!(buf, vec![0x00], "C# FAIL 溢出仍写入 newValue=0");
  }

  /// INCRBY SAT：i8 域 i64 回绕值不得漏判溢出（C# 以进位判溢出后饱和）
  #[test]
  fn incr_sat_saturates_on_signed_wrap() {
    let mut buf = vec![120u8]; // i8 @0 = 120
    let r = bit_field_apply(
      &mut buf,
      BitFieldOp::IncrBy {
        is_signed: true,
        bits: 8,
        offset: 0,
        increment: 10,
        wrap: false,
        sat: true,
      },
    );
    assert_eq!(r, Some(127)); // 饱和到 i8 上界，而非回绕值 -126
    assert_eq!(buf, vec![0x7F]);
  }
}
