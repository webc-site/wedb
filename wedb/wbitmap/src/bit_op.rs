//! BITOP 位运算执行器（对标 libs/server/Resp/Bitmap/BitmapManagerBitOp.cs）
//!
//! 会话层 BITOP 以 [`BitOpAccumulator`] 增量折叠承接：
//! 逐源流式对位，首命中源定初值（NOT 逐字节取反），后续源在公共前缀原位折叠；
//! 更长源按算子耗尽语义补段（OR/XOR 零恒等承接剩余、AND/DIFF 补零）。
//! 逐位结果与 Redis 语义逐字节一致，仅持有一份最长源长度的目标缓冲。
//!
//! 自研依据: BITOP 算子（C# 对应 GarnetBitmapTests.cs BITOP 面）

/// 位运算种类（libs/server/Resp/Bitmap/BitmapCommands.cs:BitmapOperation）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitmapOperation {
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

/// BITOP 源增量折叠器（libs/server/Resp/Bitmap/BitmapManagerBitOp.cs 回调流式对位）
///
/// 会话层逐源回调读把切片借用进闭包即弃，逐源折叠：首命中源拷入目标缓冲
/// （NOT 逐字节取反，对位单源路径），后续源在公共前缀原位折叠；更长源按算子耗尽语义补段
/// （OR/XOR 零恒等承接剩余、AND/DIFF 补零）。逐位结果一致，
/// 仅持有一份最长源长度的目标缓冲。
pub struct BitOpAccumulator {
  op: BitmapOperation,
  /// 目标缓冲，长度恒等于已见最长源长度
  dst: Vec<u8>,
  /// 已见最短源长度（无折叠源时无意义，finish 按 keys_found 判定）
  shortest_src_length: usize,
  /// 已折叠源数（命中 + 缺失零长切片；Redis bitops.c:1294-1301 缺失=零长串并入折叠序）
  keys_found: usize,
}

impl BitOpAccumulator {
  /// 按位运算种类建空折叠器
  #[inline]
  pub const fn new(op: BitmapOperation) -> Self {
    Self {
      op,
      dst: Vec::new(),
      shortest_src_length: usize::MAX,
      keys_found: 0,
    }
  }

  /// libs/server/Resp/Bitmap/BitmapManagerBitOp.cs:InvokeBitOperationUnsafe
  ///
  /// 折叠一个源切片（借用即弃，不持有）；缺失源传零长切片（Redis
  /// bitops.c:1294-1301 缺失=零长串就地参与运算，会话层 Missing 臂同径）
  pub fn fold(&mut self, src: &[u8]) {
    self.keys_found += 1;
    self.shortest_src_length = self.shortest_src_length.min(src.len());
    debug_assert!(self.op != BitmapOperation::Not || self.keys_found == 1);

    // 首命中源定初值：NOT 取反，其余原样拷入
    if self.keys_found == 1 {
      self.dst = match self.op {
        BitmapOperation::Not => src.iter().map(|b| !b).collect(),
        _ => src.to_vec(),
      };
      return;
    }

    // 公共前缀原位折叠（LLVM 自动向量化）
    let common = self.dst.len().min(src.len());
    match self.op {
      BitmapOperation::And => {
        for (d, s) in self.dst[..common].iter_mut().zip(&src[..common]) {
          *d &= s;
        }
        // 更长源补段：耗尽源清零语义，尾段由 finish 统一清零，此处补零维持长度
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
        // 耗尽源恒等（不清零）：首源已耗尽区补零
        if src.len() > self.dst.len() {
          self.dst.resize(src.len(), 0);
        }
      }
      // NOT 一元，多源不可达（会话层参数校验拦截）
      BitmapOperation::Not => {}
    }
  }

  /// 收尾取出结果：AND 清零耗尽尾段；无命中源回空缓冲
  pub fn finish(mut self) -> Result<Vec<u8>, &'static str> {
    // DIFF 单命中源 Err 防御位：会话层 arity 恒 >= 2 源
    if self.op == BitmapOperation::Diff && self.keys_found == 1 {
      return Err("BITOP DIFF operation requires at least two source bitmaps");
    }
    // AND 清尾（无命中源时最短长度未定义，dst 为空无需清）
    if self.op == BitmapOperation::And && self.keys_found > 0 {
      self.dst[self.shortest_src_length..].fill(0);
    }
    Ok(self.dst)
  }
}
