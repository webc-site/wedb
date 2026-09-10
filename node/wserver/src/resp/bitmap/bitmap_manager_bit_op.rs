//! BITOP 位运算执行器（对标 libs/server/Resp/Bitmap/BitmapManagerBitOp.cs，
//! C# 为 BitmapManager partial）
//!
//! C# 按 Vector512/256/128 硬件支持三选一向量化并以泛型二元算子折叠；
//! Rust 侧无稳定跨平台向量抽象，以 u64 字批处理承接（同形展开），语义逐位
//! 一致：批内字节经最宽档消化，剩余走标量字批与逐字节尾部。

/// 位运算种类（libs/server/Resp/Bitmap/BitmapCommands.cs:BitmapOperation）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitmapOperation {
  /// 无
  None,
  /// 按位与
  And,
  /// 按位或
  Or,
  /// 按位异或
  Xor,
  /// 按位取反
  Not,
  /// 差集（首源对其余源按位清除）
  Diff,
}

/// C# Garnet.common.Numerics.IBinaryOperator 的泛型二元算子承接
trait BinaryOperator {
  fn invoke_u64(a: u64, b: u64) -> u64;
  fn invoke_u8(a: u8, b: u8) -> u8;
  /// 源耗尽时该算子是否把结果清零（C# `typeof(T)==typeof(BitwiseAndOperator)` 特判）
  fn zeroes_when_exhausted() -> bool;
}

struct BitwiseAndOperator;
impl BinaryOperator for BitwiseAndOperator {
  #[inline]
  fn invoke_u64(a: u64, b: u64) -> u64 {
    a & b
  }
  #[inline]
  fn invoke_u8(a: u8, b: u8) -> u8 {
    a & b
  }
  #[inline]
  fn zeroes_when_exhausted() -> bool {
    true
  }
}

struct BitwiseOrOperator;
impl BinaryOperator for BitwiseOrOperator {
  #[inline]
  fn invoke_u64(a: u64, b: u64) -> u64 {
    a | b
  }
  #[inline]
  fn invoke_u8(a: u8, b: u8) -> u8 {
    a | b
  }
  #[inline]
  fn zeroes_when_exhausted() -> bool {
    false
  }
}

struct BitwiseXorOperator;
impl BinaryOperator for BitwiseXorOperator {
  #[inline]
  fn invoke_u64(a: u64, b: u64) -> u64 {
    a ^ b
  }
  #[inline]
  fn invoke_u8(a: u8, b: u8) -> u8 {
    a ^ b
  }
  #[inline]
  fn zeroes_when_exhausted() -> bool {
    false
  }
}

struct BitwiseAndNotOperator;
impl BinaryOperator for BitwiseAndNotOperator {
  #[inline]
  fn invoke_u64(a: u64, b: u64) -> u64 {
    a & !b
  }
  #[inline]
  fn invoke_u8(a: u8, b: u8) -> u8 {
    a & !b
  }
  #[inline]
  fn zeroes_when_exhausted() -> bool {
    false
  }
}

/// 对多源位图执行位运算并把结果写入目标缓冲
///
/// `srcs` 为源位图切片序列（C# srcPtrs/srcEndPtrs），`dst` 长度即
/// dstLength（= 最长源长度），`shortest_src_length` 为最短源长度。
/// 源被键缺失吞并后仅剩单源时 DIFF 非法（C# GarnetException → Err）。
///
/// libs/server/Resp/Bitmap/BitmapManagerBitOp.cs:InvokeBitOperationUnsafe
pub fn invoke_bit_operation_unsafe(
  op: BitmapOperation,
  srcs: &[&[u8]],
  dst: &mut [u8],
  shortest_src_length: usize,
) -> Result<(), &'static str> {
  debug_assert!(matches!(
    op,
    BitmapOperation::Not
      | BitmapOperation::And
      | BitmapOperation::Or
      | BitmapOperation::Xor
      | BitmapOperation::Diff
  ));
  debug_assert!(!srcs.is_empty());
  debug_assert!(dst.len() >= shortest_src_length);

  if srcs.len() == 1 {
    if op == BitmapOperation::Diff {
      return Err("BITOP DIFF operation requires at least two source bitmaps");
    }

    let src_bitmap = srcs[0];

    if op == BitmapOperation::Not {
      // TensorPrimitives.OnesComplement
      for (d, s) in dst.iter_mut().zip(src_bitmap) {
        *d = !s;
      }
    } else {
      dst[..src_bitmap.len()].copy_from_slice(src_bitmap);
    }
  } else if op == BitmapOperation::And {
    invoke_nary_bitwise_operation::<BitwiseAndOperator>(srcs, dst, shortest_src_length);
  } else if op == BitmapOperation::Or {
    invoke_nary_bitwise_operation::<BitwiseOrOperator>(srcs, dst, shortest_src_length);
  } else if op == BitmapOperation::Xor {
    invoke_nary_bitwise_operation::<BitwiseXorOperator>(srcs, dst, shortest_src_length);
  } else if op == BitmapOperation::Diff {
    invoke_nary_bitwise_operation::<BitwiseAndNotOperator>(srcs, dst, shortest_src_length);
  }
  Ok(())
}

/// n 元位图二元折叠：宽批次级联 + 标量字批 + 尾部
///
/// C# 按硬件探测三选一（Vectorized512/256/128）后走标量字批与尾部；
/// Rust 侧三档依次消化，逐字节结果一致。
///
/// libs/server/Resp/Bitmap/BitmapManagerBitOp.cs:InvokeNaryBitwiseOperation
fn invoke_nary_bitwise_operation<O: BinaryOperator>(
  srcs: &[&[u8]],
  dst: &mut [u8],
  shortest_src_length: usize,
) {
  // 各源独立游标（C# 复制 tmpSrcPtrs，不回写调用方）
  let mut cursors = vec![0usize; srcs.len()];
  let mut dst_pos = 0usize;
  let mut remaining = shortest_src_length;

  // 512 → 256 → 128：每档消化 remaining 的宽度对齐前缀
  let batch = remaining & !(64 * 8 - 1);
  if batch > 0 {
    vectorized512::<O>(srcs, &mut cursors, dst, dst_pos, batch);
    dst_pos += batch;
    remaining -= batch;
  }
  let batch = remaining & !(32 * 8 - 1);
  if batch > 0 {
    vectorized256::<O>(srcs, &mut cursors, dst, dst_pos, batch);
    dst_pos += batch;
    remaining -= batch;
  }
  let batch = remaining & !(16 * 8 - 1);
  if batch > 0 {
    vectorized128::<O>(srcs, &mut cursors, dst, dst_pos, batch);
    dst_pos += batch;
    remaining -= batch;
  }

  // 标量：8 字节 × 4
  let words_end = dst_pos + remaining - (remaining & (size_of::<u64>() * 4 - 1));
  while dst_pos < words_end {
    for lane in 0..4 {
      let off = dst_pos + lane * 8;
      let mut v = u64::from_le_bytes(srcs[0][off..off + 8].try_into().unwrap());
      for src in srcs.iter().skip(1) {
        let b = u64::from_le_bytes(src[off..off + 8].try_into().unwrap());
        v = O::invoke_u64(v, b);
      }
      dst[off..off + 8].copy_from_slice(&v.to_le_bytes());
    }
    let chunk = size_of::<u64>() * 4;
    for cur in cursors.iter_mut() {
      *cur += chunk;
    }
    dst_pos += chunk;
  }

  // 尾部：越过源末尾的字节按算子语义处理（AND 清零，其余恒等）
  while dst_pos < dst.len() {
    let mut d00 = 0u8;

    let first = srcs[0];
    let cur0 = cursors[0];
    if cur0 < first.len() {
      d00 = first[cur0];
      cursors[0] += 1;
    }

    for (i, cur) in cursors.iter_mut().enumerate().skip(1) {
      let src = srcs[i];
      if *cur < src.len() {
        d00 = O::invoke_u8(d00, src[*cur]);
        *cur += 1;
      } else if O::zeroes_when_exhausted() {
        d00 = 0;
      }
    }

    dst[dst_pos] = d00;
    dst_pos += 1;
  }
}

/// 64 字节 × 8 路展开批处理
///
/// libs/server/Resp/Bitmap/BitmapManagerBitOp.cs:Vectorized512
fn vectorized512<O: BinaryOperator>(
  srcs: &[&[u8]],
  cursors: &mut [usize],
  dst: &mut [u8],
  dst_start: usize,
  batch: usize,
) {
  vectorized_n::<O>(srcs, cursors, dst, dst_start, batch, 64);
}

/// 32 字节 × 8 路展开批处理
///
/// libs/server/Resp/Bitmap/BitmapManagerBitOp.cs:Vectorized256
fn vectorized256<O: BinaryOperator>(
  srcs: &[&[u8]],
  cursors: &mut [usize],
  dst: &mut [u8],
  dst_start: usize,
  batch: usize,
) {
  vectorized_n::<O>(srcs, cursors, dst, dst_start, batch, 32);
}

/// 16 字节 × 8 路展开批处理
///
/// libs/server/Resp/Bitmap/BitmapManagerBitOp.cs:Vectorized128
fn vectorized128<O: BinaryOperator>(
  srcs: &[&[u8]],
  cursors: &mut [usize],
  dst: &mut [u8],
  dst_start: usize,
  batch: usize,
) {
  vectorized_n::<O>(srcs, cursors, dst, dst_start, batch, 16);
}

/// `width` 字节 × 8 路展开批处理核心
fn vectorized_n<O: BinaryOperator>(
  srcs: &[&[u8]],
  cursors: &mut [usize],
  dst: &mut [u8],
  dst_start: usize,
  batch: usize,
  width: usize,
) {
  let mut dst_ptr = dst_start;
  let dst_batch_end = dst_start + batch;
  while dst_ptr < dst_batch_end {
    // width 字节内按 u64 分片折叠（8 路展开由外层批与编译器承接）
    for chunk in 0..width / 8 {
      let off = dst_ptr + chunk * 8;
      let mut v = u64::from_le_bytes(srcs[0][off..off + 8].try_into().unwrap());
      for src in srcs.iter().skip(1) {
        let b = u64::from_le_bytes(src[off..off + 8].try_into().unwrap());
        v = O::invoke_u64(v, b);
      }
      dst[off..off + 8].copy_from_slice(&v.to_le_bytes());
    }
    for cur in cursors.iter_mut() {
      *cur += width;
    }
    dst_ptr += width;
  }
}

#[cfg(test)]
mod tests {
  use super::{BitmapOperation, invoke_bit_operation_unsafe};

  fn naive(op: BitmapOperation, srcs: &[&[u8]], dst_len: usize) -> Vec<u8> {
    let mut dst = vec![0u8; dst_len];
    for i in 0..dst_len {
      let mut b = if srcs[0].len() > i { srcs[0][i] } else { 0 };
      for s in &srcs[1..] {
        b = if s.len() > i {
          match op {
            BitmapOperation::And => b & s[i],
            BitmapOperation::Or => b | s[i],
            BitmapOperation::Xor => b ^ s[i],
            BitmapOperation::Diff => b & !s[i],
            _ => b,
          }
        } else if op == BitmapOperation::And {
          0
        } else {
          b
        };
      }
      dst[i] = b;
    }
    dst
  }

  fn run(op: BitmapOperation, srcs: &[&[u8]]) -> Vec<u8> {
    let shortest = srcs.iter().map(|s| s.len()).min().unwrap();
    let longest = srcs.iter().map(|s| s.len()).max().unwrap();
    let mut dst = vec![0u8; longest];
    invoke_bit_operation_unsafe(op, srcs, &mut dst, shortest).unwrap();
    dst
  }

  #[test]
  fn nary_matches_naive() {
    let a: &[u8] = &[0b1100_0011, 0xff, 0x00, 0x0f, 0xf0, 0x55, 0xaa];
    let b: &[u8] = &[0b1010_1010, 0x01, 0x81];
    let c: &[u8] = &[0xff, 0x00, 0xff, 0x00, 0xff, 0x00, 0xff, 0x00, 0xff];
    for (op, srcs) in [
      (BitmapOperation::And, vec![a, b]),
      (BitmapOperation::Or, vec![a, b]),
      (BitmapOperation::Xor, vec![a, b]),
      (BitmapOperation::Diff, vec![a, b]),
      (BitmapOperation::And, vec![c, a, b]),
      (BitmapOperation::Or, vec![c, a, b]),
      (BitmapOperation::Xor, vec![c, a, b]),
      (BitmapOperation::Diff, vec![c, a, b]),
      (BitmapOperation::And, vec![a, b, c, a]),
    ] {
      assert_eq!(
        run(op, &srcs),
        naive(op, &srcs, srcs.iter().map(|s| s.len()).max().unwrap()),
        "{op:?}"
      );
    }
  }

  #[test]
  fn not_and_single_source() {
    let a: &[u8] = &[0x0f, 0xf0, 0xff];
    // NOT 取反
    let mut dst = vec![0u8; 3];
    invoke_bit_operation_unsafe(BitmapOperation::Not, &[a], &mut dst, 3).unwrap();
    assert_eq!(dst, vec![0xf0, 0x0f, 0x00]);
    // 单源非 NOT：拷贝
    let mut dst = vec![0u8; 5];
    invoke_bit_operation_unsafe(BitmapOperation::And, &[a], &mut dst, 3).unwrap();
    assert_eq!(dst[..3], a[..3]);
    // 单源 DIFF：非法
    let mut dst = vec![0u8; 3];
    assert!(invoke_bit_operation_unsafe(BitmapOperation::Diff, &[a], &mut dst, 3).is_err());
  }
}
