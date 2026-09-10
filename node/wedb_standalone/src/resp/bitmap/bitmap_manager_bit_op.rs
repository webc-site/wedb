//! 位图逻辑操作（BITOP）底层算法
//! 对标 Garnet `libs/server/Resp/Bitmap/BitmapManagerBitOp.cs`

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitmapOperation {
  And,
  Or,
  Xor,
  Not,
  Diff,
}

pub struct BitmapManagerBitOp;

impl BitmapManagerBitOp {
  /// libs/server/Resp/Bitmap/BitmapManagerBitOp.cs:InvokeBitOperationUnsafe
  pub fn invoke_bit_operation(op: BitmapOperation, sources: &[&[u8]]) -> Result<Vec<u8>, &'static str> {
    if sources.is_empty() {
      return Err("BITOP requires at least one source bitmap");
    }

    if op == BitmapOperation::Not {
      if sources.len() != 1 {
        return Err("BITOP NOT operation takes only one source key");
      }
      let src = sources[0];
      let mut dst = vec![0u8; src.len()];
      for i in 0..src.len() {
        dst[i] = !src[i];
      }
      return Ok(dst);
    }

    if sources.len() == 1 {
      if op == BitmapOperation::Diff {
        return Err("BITOP DIFF operation requires at least two source bitmaps");
      }
      return Ok(sources[0].to_vec());
    }

    let max_len = sources.iter().map(|s| s.len()).max().unwrap_or(0);
    let mut dst = vec![0u8; max_len];

    match op {
      BitmapOperation::And => {
        let min_len = sources.iter().map(|s| s.len()).min().unwrap_or(0);
        for i in 0..min_len {
          let mut val = sources[0][i];
          for src in &sources[1..] {
            val &= src[i];
          }
          dst[i] = val;
        }
        // 超过 min_len 的部分与 0 做 AND 结果为 0，已由 vec![0; max_len] 保证
      }
      BitmapOperation::Or => {
        for src in sources {
          for (i, &b) in src.iter().enumerate() {
            dst[i] |= b;
          }
        }
      }
      BitmapOperation::Xor => {
        for src in sources {
          for (i, &b) in src.iter().enumerate() {
            dst[i] ^= b;
          }
        }
      }
      BitmapOperation::Diff => {
        dst.copy_from_slice(sources[0]);
        for src in &sources[1..] {
          let common = src.len().min(dst.len());
          for i in 0..common {
            dst[i] &= !src[i];
          }
        }
      }
      BitmapOperation::Not => unreachable!(),
    }

    Ok(dst)
  }

  /// libs/server/Resp/Bitmap/BitmapManagerBitOp.cs:InvokeNaryBitwiseOperation
  #[inline]
  pub fn invoke_nary_bitwise_operation(op: BitmapOperation, sources: &[&[u8]]) -> Result<Vec<u8>, &'static str> {
    Self::invoke_bit_operation(op, sources)
  }
}
