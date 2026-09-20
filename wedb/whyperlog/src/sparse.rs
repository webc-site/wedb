//! 稀疏编码域：RLE 操作码流的初始化、校验、增长、更新、转换与导出（对标 HyperLogLog.cs 稀疏分支）。

#[cfg(debug_assertions)]
use std::fmt::Write;

use super::{
  HLL_HEADER_BYTES, HllDtype, HyperLogLog, SPARSE_MAX_BYTES_PER_INSERT, SPARSE_MEMORY_SECTOR_SIZE,
  SPARSE_SIZE_MAX_CAP,
};

impl HyperLogLog {
  /// 稀疏 RLE 流语义校验：寄存器值在 [1, qbit+1] 且恰好覆盖全部寄存器
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:IsValidSparseStream
  pub fn is_valid_sparse_stream(&self, ptr: &[u8]) -> bool {
    let rle_size = self.get_sparse_rle_size(ptr) as usize;
    let start = self.sparse_header_size;
    let end = start + rle_size;
    if end > ptr.len() {
      return false;
    }

    let mut covered_registers = 0_usize;
    for &op in &ptr[start..end] {
      if Self::is_zero_range(op) {
        covered_registers += Self::zero_range_len(op);
      } else {
        let non_zero = Self::get_non_zero(op);
        // 非零操作码编码的前导零计数须在 [1, qbit + 1]
        if non_zero > self.qbit + 1 {
          return false;
        }
        covered_registers += 1;
      }

      // 超出寄存器空间的流非法
      if covered_registers > self.mcnt {
        return false;
      }
    }

    covered_registers == self.mcnt
  }

  /// 初始化稀疏载荷（初始零段 RLE 编码）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:InitSparse
  pub fn init_sparse(&self, ptr: &mut [u8]) {
    self.set_prefix(ptr);
    Self::set_type(ptr, HllDtype::Sparse);
    self.set_card(ptr, i64::MIN);

    let ranges = self.sparse_zero_ranges;
    self.set_sparse_rle_size(ptr, ranges as u16);
    let regs_start = HLL_HEADER_BYTES + 2;

    // 全零初始编码：0xFF = 1vvv... → 单字节零段覆盖 128 寄存器
    ptr[regs_start..regs_start + ranges].fill(0xFF);
  }

  /// 按待插入元素数推算稀疏初始长度（超上限返回稠密长度）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:SparseInitialLength
  pub fn sparse_initial_length(&self, count: usize) -> usize {
    let required_bytes = self.sparse_required_bytes(count);
    if self.sparse_header_size + required_bytes > SPARSE_SIZE_MAX_CAP {
      self.dense_bytes
    } else if required_bytes < self.sparse_zero_ranges + SPARSE_MEMORY_SECTOR_SIZE {
      self.sparse_bytes()
    } else {
      self.sparse_header_size + required_bytes
    }
  }

  /// count 个元素的稀疏空间需求（按 128B 扇区对齐）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:SparseRequiredBytes
  #[inline]
  pub fn sparse_required_bytes(&self, cnt: usize) -> usize {
    let used_bytes = cnt * SPARSE_MAX_BYTES_PER_INSERT;
    // 对齐 C# ((u-1)/s)+1 截断除法：u=0 时仍占一个扇区，且不发生下溢
    let page_count = used_bytes.saturating_sub(1) / SPARSE_MEMORY_SECTOR_SIZE + 1;
    page_count * SPARSE_MEMORY_SECTOR_SIZE
  }

  /// 稀疏当前实际占用（头 + RLE 流）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:SparseCurrentSizeInBytes
  #[inline]
  pub fn sparse_current_size_in_bytes(&self, ptr: &[u8]) -> usize {
    self.sparse_header_size + self.get_sparse_rle_size(ptr) as usize
  }

  /// 现有分配能否原位容纳 count 个新元素
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:CanGrowInPlace
  #[inline]
  pub fn can_grow_in_place(&self, value: &[u8], value_len: usize, count: usize) -> bool {
    self.sparse_current_size_in_bytes(value) + (SPARSE_MAX_BYTES_PER_INSERT * count) < value_len
  }

  /// 增长后的新载荷长度：稀疏按需扩张（超上限稠密化），稠密不变
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:UpdateGrow
  pub fn update_grow(&self, count: usize, value: &[u8]) -> usize {
    if self.is_sparse(value) {
      let sparse_blob_bytes =
        self.sparse_current_size_in_bytes(value) + self.sparse_required_bytes(count);
      return if sparse_blob_bytes < SPARSE_SIZE_MAX_CAP {
        sparse_blob_bytes
      } else {
        self.dense_bytes
      };
    }

    self.dense_bytes
  }

  /// 合并触发的增长计算（稀疏→稀疏按源非零数扩张，其余稠密化）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:MergeGrow
  pub fn merge_grow(&self, src_hll: &[u8], dst_hll: &[u8]) -> usize {
    let dst_type = Self::get_type(dst_hll);
    let src_type = Self::get_type(src_hll);
    if dst_type == HllDtype::Sparse as u8 && src_type == HllDtype::Sparse as u8 {
      let src_non_zero_bytes = self.sparse_count_non_zero(src_hll) * SPARSE_MAX_BYTES_PER_INSERT;
      // 对齐 C# ((x-1)/s)+1：空源（非零数为 0，如 PFMERGE 全缺失源产生的空 HLL）
      // 时仍预留一个扇区，避免 usize 下溢（debug 构建 panic / release 回绕致载荷破坏）
      let page_count = src_non_zero_bytes.saturating_sub(1) / SPARSE_MEMORY_SECTOR_SIZE + 1;
      let sparse_blob_bytes =
        self.sparse_current_size_in_bytes(dst_hll) + page_count * SPARSE_MEMORY_SECTOR_SIZE;
      return if sparse_blob_bytes < SPARSE_SIZE_MAX_CAP {
        sparse_blob_bytes
      } else {
        self.dense_bytes
      };
    }
    self.dense_bytes
  }

  /// 合并触发的迁移：旧载荷变换/拷贝到新载荷后并入源
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:CopyUpdateMerge
  pub fn copy_update_merge(
    &self,
    src_hll: &[u8],
    old_dst: &[u8],
    new_dst: &mut [u8],
    old_value_len: usize,
    new_value_len: usize,
  ) {
    if old_value_len == new_value_len {
      new_dst[..old_value_len].copy_from_slice(&old_dst[..old_value_len]);
    } else {
      if new_value_len == self.dense_bytes {
        self.init_dense(new_dst);
      } else {
        self.init_sparse(new_dst);
      }
      self.merge(old_dst, new_dst);
    }
    self.merge(src_hll, new_dst);
    self.set_card(new_dst, i64::MIN);
  }

  /// 主拷贝更新：旧稀疏载荷升级（稠密化或原地扩容）并插入新元素
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:CopyUpdate
  pub fn copy_update(&self, elements: &[&[u8]], old_value: &[u8], new_value: &mut [u8]) -> bool {
    let mut f_updated = false;

    // 仅当旧载荷为稀疏时进入
    if self.is_sparse(old_value) {
      if new_value.len() == self.dense_bytes {
        // 升级为稠密
        self.init_dense(new_value);
        f_updated |= self.sparse_to_dense(old_value, new_value);
        f_updated |= self.iterate_update(elements, new_value, true);
        return f_updated;
      }

      // 扩容为更大稀疏
      self.init_sparse(new_value);
      let sparse_blob_bytes = self.sparse_current_size_in_bytes(old_value);
      new_value[..sparse_blob_bytes].copy_from_slice(&old_value[..sparse_blob_bytes]);
      f_updated = self.iterate_update(elements, new_value, false);
      return f_updated;
    }

    debug_assert!(false, "HyperLogLog Update invalid data structure type");
    false
  }

  /// 拷贝旧稀疏到新稠密并插入单哈希
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:SparseToDenseCopy
  pub fn sparse_to_dense_copy(&self, hv: u64, old_value: &[u8], new_value: &mut [u8]) -> bool {
    let mut f_updated = false;
    self.init_dense(new_value);
    f_updated |= self.sparse_to_dense(old_value, new_value);
    f_updated |= self.update_dense(new_value, hv);
    f_updated
  }

  /// 拷贝旧稀疏到新稀疏并插入单哈希
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:SparseToSparseCopy
  pub fn sparse_to_sparse_copy(&self, hv: u64, old_value: &[u8], new_value: &mut [u8]) -> bool {
    let sparse_blob_bytes = self.sparse_current_size_in_bytes(old_value);
    new_value[..sparse_blob_bytes].copy_from_slice(&old_value[..sparse_blob_bytes]);
    self.update_sparse(new_value, hv)
  }

  /// 写稀疏 RLE 流长度
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:SetSparseRLESize
  #[inline]
  pub fn set_sparse_rle_size(&self, ptr: &mut [u8], size: u16) {
    ptr[HLL_HEADER_BYTES..HLL_HEADER_BYTES + 2].copy_from_slice(&size.to_le_bytes());
  }

  /// 读稀疏 RLE 流长度
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:GetSparseRLESize
  #[inline]
  pub fn get_sparse_rle_size(&self, ptr: &[u8]) -> u16 {
    match ptr.get(HLL_HEADER_BYTES..HLL_HEADER_BYTES + 2) {
      Some(&[b0, b1]) => u16::from_le_bytes([b0, b1]),
      _ => 0,
    }
  }

  /// 零段操作码判定（1xxx xxxx）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:IsZeroRange
  #[inline]
  pub fn is_zero_range(p: u8) -> bool {
    (p & 0x80) != 0
  }

  /// 零段长度（(b & 0x7F) + 1）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:ZeroRangeLen
  #[inline]
  pub fn zero_range_len(p: u8) -> usize {
    ((p & 0x7F) as usize) + 1
  }

  /// 构造零段操作码
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:ZeroRangeSet
  #[inline]
  pub fn zero_range_set(len: usize) -> u8 {
    ((len - 1) | 0x80) as u8
  }

  /// 非零操作码取值（前导零计数）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:GetNonZero
  #[inline]
  pub fn get_non_zero(p: u8) -> u8 {
    (p & 0x7F) + 1
  }

  /// 构造非零操作码
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:SetNonZero
  #[inline]
  pub fn set_non_zero(cnt: u8) -> u8 {
    cnt - 1
  }

  /// 更新稀疏表示（单哈希入口，变更时失效基数缓存）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:UpdateSparse
  #[inline]
  pub fn update_sparse(&self, ptr: &mut [u8], hv: u64) -> bool {
    let idx = self.reg_idx(hv);
    let cntlz = self.clz(hv);
    let f_updated = self.update_sparse_reg(ptr, idx, cntlz);
    if f_updated {
      // 失效先前计算的基数
      self.set_card(ptr, i64::MIN);
    }
    f_updated
  }

  /// 稀疏寄存器择大更新：命中非零操作码原位改写，命中零段按需拆分
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:UpdateSparseReg
  pub fn update_sparse_reg(&self, ptr: &mut [u8], idx: u16, cntlz: u8) -> bool {
    let rle_size = self.get_sparse_rle_size(ptr) as usize;
    let header = self.sparse_header_size;

    let mut curr = header; // 稀疏流起点
    let end = curr + rle_size; // 稀疏流终点

    let mut offset = 0_usize; // 已覆盖寄存器偏移

    // 1. 定位覆盖 idx 的操作码（流恰好覆盖全部寄存器，必有命中）
    let mut clen;
    loop {
      let op = ptr[curr];
      clen = if Self::is_zero_range(op) {
        Self::zero_range_len(op)
      } else {
        1
      };
      if (idx as usize) < offset + clen {
        break;
      }
      curr += 1;
      offset += clen;
    }

    let next = curr + 1;
    let is_val = !Self::is_zero_range(ptr[curr]);

    // 2. 命中非零操作码：原位改写或无变更
    if is_val {
      let lz = Self::get_non_zero(ptr[curr]);
      if cntlz <= lz {
        return false;
      }
      ptr[curr] = Self::set_non_zero(cntlz);
      self.set_card(ptr, i64::MIN);
      return true;
    }

    // 3. 拆分零段：[左零段][非零][右零段]
    let mut buf = [0_u8; 3];
    let mut blen = 0_usize;
    let roffset = offset + clen - 1;

    if offset != idx as usize {
      // 左零段
      let zrange = idx as usize - offset;
      buf[blen] = Self::zero_range_set(zrange);
      blen += 1;
    }

    buf[blen] = Self::set_non_zero(cntlz);
    blen += 1;

    if roffset != idx as usize {
      // 右零段
      let zrange = roffset - idx as usize;
      buf[blen] = Self::zero_range_set(zrange);
      blen += 1;
    }

    // 4. 后缀右移（blen - 1）后写入新操作码
    let suffixlen = end - next + 1; // 对齐 C#：多搬一个尾字节
    ptr.copy_within(next..next + suffixlen, next + blen - 1);
    ptr[curr..curr + blen].copy_from_slice(&buf[..blen]);

    self.set_sparse_rle_size(ptr, (rle_size + blen - 1) as u16);
    self.set_card(ptr, i64::MIN);
    true
  }

  /// 稀疏 → 稠密 逐寄存器展开
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:SparseToDense
  pub fn sparse_to_dense(&self, src: &[u8], dst: &mut [u8]) -> bool {
    let mut f_updated = false;

    let rle_size = self.get_sparse_rle_size(src) as usize;
    let start = self.sparse_header_size;
    let end = start + rle_size;
    let mut offset = 0_usize;

    for &op in &src[start..end] {
      let iszero = Self::is_zero_range(op);
      if !iszero {
        let lz = Self::get_non_zero(op);
        f_updated |= self.update_dense_register(dst, offset as u16, lz);
      }
      offset += if iszero { Self::zero_range_len(op) } else { 1 };
    }
    f_updated
  }

  /// 稀疏 → 稀疏 逐寄存器择大合并
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:SparseToSparse
  pub fn sparse_to_sparse(&self, src: &[u8], dst: &mut [u8]) -> bool {
    let mut f_updated = false;
    let start = self.sparse_header_size;
    let end = start + self.get_sparse_rle_size(src) as usize;
    let mut offset = 0_usize;

    for &op in &src[start..end] {
      let iszero = Self::is_zero_range(op);
      let clen = if iszero { Self::zero_range_len(op) } else { 1 };

      if !iszero {
        let lz = Self::get_non_zero(op);
        f_updated |= self.update_sparse_reg(dst, offset as u16, lz);
      }

      offset += clen;
    }

    f_updated
  }

  /// 稀疏非零寄存器计数
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:SparseCountNonZero
  pub fn sparse_count_non_zero(&self, ptr: &[u8]) -> usize {
    let rle_size = self.get_sparse_rle_size(ptr) as usize;
    let start = self.sparse_header_size;
    let end = (start + rle_size).min(ptr.len());
    ptr[start..end]
      .iter()
      .filter(|&&op| !Self::is_zero_range(op))
      .count()
  }

  // ---- 调试导出（C# DEBUG 分支；返回字符串替代 Console.WriteLine） ----
  //
  // C# 该组件整体位于 HyperLogLog.cs:1100-1298 的 #if DEBUG 段内，
  // 生产零读者，故以 debug_assertions 门控，release 面不导出。

  /// 稀疏流原始字节导出
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:DumpSparseRawBytes
  #[cfg(debug_assertions)]
  pub fn dump_sparse_raw_bytes(&self, ptr: &[u8]) -> String {
    let used = self.get_sparse_rle_size(ptr) as usize + self.sparse_header_size;
    format!("{:02x?}\n", &ptr[..used.min(ptr.len())])
  }

  /// 稀疏非零寄存器导出
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:DumpSparseRegs
  #[cfg(debug_assertions)]
  pub fn dump_sparse_regs(&self, sparse: &[u8]) -> String {
    let rle_size = self.get_sparse_rle_size(sparse) as usize;
    let start = self.sparse_header_size;
    let end = start + rle_size;
    let mut offset = 0_usize;
    let mut out = String::new();

    for &op in &sparse[start..end] {
      let iszero = Self::is_zero_range(op);
      let clen = if iszero { Self::zero_range_len(op) } else { 1 };

      if !iszero {
        let _ = writeln!(out, "{offset} = {}", Self::get_non_zero(op));
      }
      offset += clen;
    }
    out
  }
}
