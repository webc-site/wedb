//! 帧头、有效性判读与元信息提取（对标 HyperLogLog.cs 头部与校验逻辑）。

use crate::{HYLL_MAGIC, HllDtype, HyperLogLog, SPARSE_SIZE_MAX_CAP};

impl HyperLogLog {
  /// 头部校验：类型合法且魔数为 HYLL
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:IsValidHYLL(byte*)
  #[inline]
  pub fn is_valid_hyll(&self, ptr: &[u8]) -> bool {
    Self::is_hyll(ptr) && (self.is_sparse(ptr) || self.is_dense(ptr))
  }

  /// 魔数 + 长度校验（重载 IsValidHYLL(byte*, int)）
  #[inline]
  pub fn is_valid_hyll_len(&self, ptr: &[u8], length: usize) -> bool {
    Self::is_hyll(ptr) && self.is_valid_hll_length(ptr, length)
  }

  /// 魔数检查（offset 4 起 4 字节 == 0x48594C4C，即 "HYLL" 的 LE int）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:IsHYLL
  #[inline]
  pub fn is_hyll(ptr: &[u8]) -> bool {
    ptr.len() >= 8 && u32::from_le_bytes(ptr[4..8].try_into().unwrap()) == HYLL_MAGIC
  }

  /// 长度校验：稠密须恰好 DenseBytes；稀疏须在合法区间且 RLE 流完整
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:IsValidHLLLength
  pub fn is_valid_hll_length(&self, ptr: &[u8], length: usize) -> bool {
    if self.is_dense(ptr) {
      return length == self.dense_bytes;
    }

    if !self.is_sparse(ptr) {
      return false;
    }

    if !(self.sparse_initial_length(1)..=SPARSE_SIZE_MAX_CAP).contains(&length) {
      return false;
    }

    if length < self.sparse_header_size {
      return false;
    }
    let sparse_payload_bytes = length - self.sparse_header_size;
    self.get_sparse_rle_size(ptr) as usize <= sparse_payload_bytes
      && self.is_valid_sparse_stream(ptr)
  }

  /// 写 HYLL 魔数前缀
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:SetPrefix
  #[inline]
  pub fn set_prefix(&self, ptr: &mut [u8]) {
    ptr[..8].copy_from_slice(&0x4859_4C4C_0000_0000_u64.to_le_bytes());
  }

  /// 读数据结构类型（offset 3）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:GetType
  #[inline]
  pub fn get_type(ptr: &[u8]) -> u8 {
    ptr.get(3).copied().unwrap_or(u8::MAX)
  }

  /// 写数据结构类型
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:SetType
  #[inline]
  pub fn set_type(ptr: &mut [u8], dtype: HllDtype) {
    ptr[3] = dtype as u8;
  }

  /// 是否稀疏
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:IsSparse
  #[inline]
  pub fn is_sparse(&self, ptr: &[u8]) -> bool {
    Self::get_type(ptr) == HllDtype::Sparse as u8
  }

  /// 是否稠密
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:IsDense
  #[inline]
  pub fn is_dense(&self, ptr: &[u8]) -> bool {
    Self::get_type(ptr) == HllDtype::Dense as u8
  }

  /// 写缓存的基数估计（负数表示已失效）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:SetCard
  #[inline]
  pub fn set_card(&self, ptr: &mut [u8], card: i64) {
    ptr[8..16].copy_from_slice(&card.to_le_bytes());
  }

  /// 读缓存的基数估计
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:GetCard
  #[inline]
  pub fn get_card(ptr: &[u8]) -> i64 {
    if ptr.len() >= 16 {
      i64::from_le_bytes(ptr[8..16].try_into().unwrap())
    } else {
      i64::MIN
    }
  }

  /// 缓存基数是否有效
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:IsValidCard
  #[inline]
  pub fn is_valid_card(ptr: &[u8]) -> bool {
    Self::get_card(ptr) >= 0
  }
}
