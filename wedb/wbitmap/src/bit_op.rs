//! BITOP 位运算执行器（对标 libs/server/Resp/Bitmap/BitmapManagerBitOp.cs，
//! C# 为 BitmapManager partial）
//!
//! C# 按 Vector512/256/128 硬件支持三选一向量化并以泛型二元算子折叠；本仓
//! 位折叠刻意走 u64 字批处理承接，三档批宽形态保留（最宽档依次消化对齐
//! 前缀），剩余走标量字批与逐字节尾部，逐位结果一致。fearless_simd 选型面
//! （本仓 SIMD 单点 `wbase::simd`）覆盖变长键比对（`fast_key_eq`），此处不引
//! 向量臂：纯位运算为等宽数据并行，实际向量宽度由编译期 target-cpu 档位经
//! 自动向量化承接，无需复刻 C# 的运行时三档探测。

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

/// BITOP 源增量折叠器（[`invoke_bit_operation_unsafe`] 的回调流式对位）
///
/// 会话层逐源回调读把切片借用进闭包即弃（C# StringBitOperation 的
/// PinnedSpanByte 源指针收集 + InvokeBitOperationUnsafe 单次运算组合无法
/// 借用逃逸），改为逐源折叠：首命中源拷入目标缓冲（NOT 逐字节取反，
/// 对位单源路径），后续源在公共前缀原位折叠；更长源按算子耗尽语义补段
/// （OR/XOR 零恒等承接剩余、AND/DIFF 补零）。逐位结果与全量版一致，
/// 仅持有一份最长源长度的目标缓冲。
pub struct BitOpAccumulator {
  op: BitmapOperation,
  /// 目标缓冲，长度恒等于已见最长源长度
  dst: Vec<u8>,
  /// 已见最短源长度（无命中源时无意义，finish 按 keys_found 判定）
  shortest_src_length: usize,
  /// 命中源数（C# keysFound）
  keys_found: usize,
}

impl BitOpAccumulator {
  /// 按位运算种类建空折叠器
  pub fn new(op: BitmapOperation) -> Self {
    Self {
      op,
      dst: Vec::new(),
      shortest_src_length: usize::MAX,
      keys_found: 0,
    }
  }

  /// 折叠一个命中源切片（借用即弃，不持有）
  pub fn fold(&mut self, src: &[u8]) {
    self.keys_found += 1;
    self.shortest_src_length = self.shortest_src_length.min(src.len());
    debug_assert!(self.op != BitmapOperation::Not || self.keys_found == 1);

    // 首命中源定初值：NOT 取反（TensorPrimitives.OnesComplement 对位），
    // 其余原样拷入（C# 单源路径 copy_from_slice）
    if self.keys_found == 1 {
      self.dst = match self.op {
        BitmapOperation::Not => src.iter().map(|b| !b).collect(),
        _ => src.to_vec(),
      };
      return;
    }

    // 公共前缀原位折叠（C# 逐源二元算子）
    let common = self.dst.len().min(src.len());
    match self.op {
      BitmapOperation::And => {
        for (d, s) in self.dst[..common].iter_mut().zip(&src[..common]) {
          *d &= s;
        }
        // 更长源补段：耗尽源清零语义（zeroes_when_exhausted），尾段由
        // finish 统一清零，此处补零维持"长度 = 已见最长源"不变式
        if src.len() > self.dst.len() {
          self.dst.resize(src.len(), 0);
        }
      }
      BitmapOperation::Or => {
        for (d, s) in self.dst[..common].iter_mut().zip(&src[..common]) {
          *d |= s;
        }
        // 耗尽源零恒等：补段承接剩余字节
        self.dst.extend_from_slice(&src[common..]);
      }
      BitmapOperation::Xor => {
        for (d, s) in self.dst[..common].iter_mut().zip(&src[..common]) {
          *d ^= s;
        }
        self.dst.extend_from_slice(&src[common..]);
      }
      BitmapOperation::Diff => {
        for (d, s) in self.dst[..common].iter_mut().zip(&src[..common]) {
          *d &= !s;
        }
        // 耗尽源恒等（不清零）：首源已耗尽区补零，后续 andnot 零仍零，
        // 与 C# 尾部"跳过耗尽源"逐位一致
        if src.len() > self.dst.len() {
          self.dst.resize(src.len(), 0);
        }
      }
      // NOT 一元，多源不可达（会话层参数校验拦截）；None 非法位运算种类
      BitmapOperation::Not | BitmapOperation::None => {}
    }
  }

  /// 收尾取出结果：AND 清零耗尽尾段（C# zeroes_when_exhausted）；DIFF 仅
  /// 单命中源非法（C# GarnetException 对位）；无命中源回空缓冲（会话层
  /// 据此回 0 且不写目的键）
  pub fn finish(mut self) -> Result<Vec<u8>, &'static str> {
    if self.op == BitmapOperation::Diff && self.keys_found == 1 {
      return Err("BITOP DIFF operation requires at least two source bitmaps");
    }
    // AND 清尾（无命中源时最短长度未定义，dst 为空本就无需清）
    if self.op == BitmapOperation::And && self.keys_found > 0 {
      self.dst[self.shortest_src_length..].fill(0);
    }
    Ok(self.dst)
  }
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
  use super::{BitOpAccumulator, BitmapOperation, invoke_bit_operation_unsafe};

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

  /// 增量折叠器全量对拍（缺失键已被会话层过滤，srcs 即命中源序列）
  fn run_accumulator(op: BitmapOperation, srcs: &[&[u8]]) -> Result<Vec<u8>, &'static str> {
    let mut acc = BitOpAccumulator::new(op);
    for s in srcs {
      acc.fold(s);
    }
    acc.finish()
  }

  #[test]
  fn accumulator_matches_full_invoke() {
    // 长短序列覆盖：递增/递减/乱序/等长/零长源（会话层真实顺序任意）
    let lens: &[&[usize]] = &[
      &[7, 3, 9],
      &[9, 3, 7],
      &[3, 9, 7],
      &[5, 5],
      &[1, 2, 3, 4, 5, 6, 7, 8],
      &[0, 5],
      &[5, 0],
      &[0, 0],
      &[9, 1, 9, 1, 9],
    ];
    let mut bytes = [0u8; 9];
    for (i, b) in bytes.iter_mut().enumerate() {
      *b = (i as u8).wrapping_mul(37).wrapping_mul(191) | 1;
    }
    for op in [
      BitmapOperation::And,
      BitmapOperation::Or,
      BitmapOperation::Xor,
      BitmapOperation::Diff,
    ] {
      for seq in lens {
        let srcs: Vec<&[u8]> = seq.iter().map(|&l| &bytes[..l]).collect();
        let got = run_accumulator(op, &srcs).unwrap();
        let mut want = vec![0u8; seq.iter().copied().max().unwrap_or(0)];
        let shortest = seq.iter().copied().min().unwrap_or(0);
        invoke_bit_operation_unsafe(op, &srcs, &mut want, shortest).unwrap();
        assert_eq!(got, want, "{op:?} lens {seq:?}");
      }
    }
  }

  #[test]
  fn accumulator_edges() {
    let a: &[u8] = &[0x0f, 0xf0, 0xff];
    // 无命中源：空缓冲（会话层据此回 0 不写）
    for op in [
      BitmapOperation::And,
      BitmapOperation::Or,
      BitmapOperation::Xor,
      BitmapOperation::Not,
    ] {
      assert_eq!(run_accumulator(op, &[]).unwrap(), Vec::<u8>::new());
    }
    // DIFF 无命中源合法（源全被吞并仅在单命中源时非法）
    assert_eq!(
      run_accumulator(BitmapOperation::Diff, &[]).unwrap(),
      Vec::<u8>::new()
    );
    // 单源 NOT：取反
    assert_eq!(
      run_accumulator(BitmapOperation::Not, &[a]).unwrap(),
      vec![0xf0, 0x0f, 0x00]
    );
    // 单源非 NOT：拷贝
    assert_eq!(
      run_accumulator(BitmapOperation::Or, &[a]).unwrap(),
      a.to_vec()
    );
    // DIFF 单命中源：非法（C# GarnetException 对位）
    assert!(run_accumulator(BitmapOperation::Diff, &[a]).is_err());
  }
}
