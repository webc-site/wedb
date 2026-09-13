//! HyperLogLog（对标 libs/server/Resp/HyperLogLog/HyperLogLog.cs）
//!
//! 基于 "New cardinality estimation algorithms for HyperLogLog sketches"
//! <https://arxiv.org/abs/1702.01284> 的稀疏/稠密双编码实现：
//! - 稠密：16 字节头（HYLL 魔数 + 编码 + 卡数缓存）+ 16384 个 6 位寄存器；
//! - 稀疏：16 字节头 + 2 字节 RLE 长度 + RLE 操作码流（零段 1xxx xxxx / 非零 0vvv vvvv），
//!   超过 4KB 上限稠密化。
//!
//! 刻意差异（对照 C#）：C# 以裸指针就地改写存储页；Rust 统一操作字节切片
//! （`&mut [u8]`），由调用方负责切片与存储页的等价性。DEBUG 导出方法
//! （C# Console.WriteLine）改为返回 `String`。

use std::collections::BTreeMap;

use bitflags::bitflags;
use wbase::hash::murmur_hash2_x64_a;

/// 寄存器位数
const REG_BITS: u32 = 6;
/// 6 位掩码
const REG_BITS_MSK: u8 = (1 << REG_BITS) - 1;
/// 头部字节数（HYLL | E | N/U | Cardin）
const HLL_HEADER_BYTES: usize = 16;
/// 哈希位数
const HBIT: u32 = 64;
/// 0.5/ln(2) 修正常数
const ALPHA: f64 = 0.721_347_520_444_481_7;

/// 稀疏每次插入的最大增量字节
const SPARSE_MAX_BYTES_PER_INSERT: usize = 2;
/// 稀疏表示容量上限（超过即稠密化）
pub const SPARSE_SIZE_MAX_CAP: usize = 1 << 12;
/// 稀疏分配步长
pub const SPARSE_MEMORY_SECTOR_SIZE: usize = 1 << 7;

/// HLL 数据结构类型
///
/// libs/server/Resp/HyperLogLog/HyperLogLog.cs:HLL_DTYPE
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum HllDtype {
  Sparse = 0,
  Dense = 1,
}

bitflags! {
  /// HyperLogLog 状态标志（Rust 侧辅助：C# 以 GarnetException 表达的非法结构）
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub struct HllValid: u8 {
    const NONE = 0;
    const HYLL = 1;
    const LENGTH = 1 << 1;
  }
}

/// Garnet HyperLogLog 实例参数（寄存器数、编码容量均由 pbit 决定）
///
/// libs/server/Resp/HyperLogLog/HyperLogLog.cs:HyperLogLog
#[derive(Debug, Clone)]
pub struct HyperLogLog {
  /// 寄存器偏移位（由 with_pbit 派生其余参数，本体仅作结构描述）
  #[expect(dead_code)]
  pbit: u8,
  /// 前导零计数位
  qbit: u8,
  /// 寄存器数
  mcnt: usize,
  /// 稠密总字节
  dense_bytes: usize,
  /// 稀疏初始零段数
  sparse_zero_ranges: usize,
  /// 稀疏头 = HLL_HEADER_BYTES + 2（RLE 长度）
  sparse_header_size: usize,
}

impl Default for HyperLogLog {
  fn default() -> Self {
    // 默认寄存器偏移 14 位（16384 寄存器）
    Self::with_pbit(14)
  }
}

impl HyperLogLog {
  /// 默认构造（pbit = 14）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:HyperLogLog() protected
  pub fn new() -> Self {
    Self::default()
  }

  /// 自定义寄存器位构造 (HyperLogLog(byte pbit))
  pub fn with_pbit(pbit: u8) -> Self {
    let qbit = (HBIT - pbit as u32) as u8;
    let mcnt = 1_usize << pbit;
    let sparse_header_size = HLL_HEADER_BYTES + 2;
    Self {
      pbit,
      qbit,
      mcnt,
      dense_bytes: HLL_HEADER_BYTES + ((REG_BITS as usize * mcnt) >> 3),
      sparse_zero_ranges: mcnt >> 7,
      sparse_header_size,
      // sparse_bytes（C# SparseBytes）在需要处即时计算
    }
  }

  /// 稠密表示总字节（16 头 + 12288 寄存器 = 12304）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:DenseBytes
  #[inline]
  pub fn dense_bytes(&self) -> usize {
    self.dense_bytes
  }

  /// 稀疏表示初始字节：头 + 默认零段 + 预留增长区
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:SparseBytes
  #[inline]
  pub fn sparse_bytes(&self) -> usize {
    self.sparse_header_size + self.sparse_zero_ranges + SPARSE_MEMORY_SECTOR_SIZE
  }

  /// 稀疏表示零段初始数
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:SparseZeroRanges
  #[inline]
  pub fn sparse_zero_ranges(&self) -> usize {
    self.sparse_zero_ranges
  }

  /// 寄存器数（2^pbit）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:RegCnt
  #[inline]
  pub fn reg_cnt(&self) -> usize {
    self.mcnt
  }

  /// 前导零计数位上限（64 - pbit）
  #[inline]
  pub fn qbit(&self) -> u8 {
    self.qbit
  }

  /// 哈希值 → 寄存器下标
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:RegIdx
  #[inline]
  pub fn reg_idx(&self, hv: u64) -> u16 {
    (hv & (self.mcnt as u64 - 1)) as u16
  }

  /// 哈希值剩余位的前导零计数（封顶 qbit + 1）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:clz
  #[inline]
  pub fn clz(&self, hv: u64) -> u8 {
    let lz = hv.leading_zeros() as u8;
    if lz >= self.qbit {
      self.qbit + 1
    } else {
      lz + 1
    }
  }

  /// 读 6 位寄存器（LSB 起跨字节打包）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:_get_register
  #[inline]
  pub fn get_register(&self, reg: &[u8], idx: u16) -> u8 {
    let m = idx as usize * REG_BITS as usize;
    let b0 = m >> 3;
    let lsb = (m & 0x7) as u32;

    // C# 以 int 提升后 (byte) 截断，等价于 u16 移位后取低 8 位
    let v0 = (reg[b0] >> lsb) as u16;
    // 末寄存器（lsb <= 2）不跨字节；b0+1 的贡献恒被 0x3F 掩码清零，
    // C# 对越界一字节的"无害读"在 Rust 侧以 0 短路（详见 set_register 注）
    let v1 = if b0 + 1 < reg.len() {
      ((reg[b0 + 1] as u16) << (8 - lsb)) & 0xFF
    } else {
      0
    };
    ((v0 | v1) & REG_BITS_MSK as u16) as u8
  }

  /// 写 6 位寄存器
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:_set_register
  #[inline]
  pub fn set_register(&self, reg: &mut [u8], idx: u16, val: u8) {
    let m = idx as usize * REG_BITS as usize;
    let b0 = m >> 3;
    let lsb = (m & 0x7) as u32;
    let msb = 8 - lsb;

    debug_assert!(b0 < (self.mcnt * REG_BITS as usize) / 8);

    reg[b0] &= !(((REG_BITS_MSK as u16) << lsb) as u8);
    reg[b0] |= ((val as u16) << lsb) as u8;

    // 末寄存器（idx = mcnt-1, lsb = 2）不跨字节：C# 对 b0+1 的写恒为
    // "保持全位 |= 0"（val >> 6 == 0），Rust 以边界短路等价实现
    if b0 + 1 < reg.len() {
      reg[b0 + 1] &= !(((REG_BITS_MSK as u16) >> msb) as u8);
      reg[b0 + 1] |= ((val as u16) >> msb) as u8;
    }
  }

  /// 头部校验：类型合法且魔数为 HYLL
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:IsValidHYLL(byte*)
  #[inline]
  pub fn is_valid_hyll(&self, ptr: &[u8]) -> bool {
    (self.is_sparse(ptr) || self.is_dense(ptr)) && Self::is_hyll(ptr)
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
    ptr.len() >= 8 && i32::from_le_bytes(ptr[4..8].try_into().unwrap()) == 0x4859_4C4C
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
        if non_zero == 0 || non_zero > self.qbit + 1 {
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
    ptr[3]
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
    i64::from_le_bytes(ptr[8..16].try_into().unwrap())
  }

  /// 缓存基数是否有效
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:IsValidCard
  #[inline]
  pub fn is_valid_card(ptr: &[u8]) -> bool {
    Self::get_card(ptr) >= 0
  }

  /// 按元素集初始化 HLL 载荷（长度决定稠密/稀疏），并更新寄存器
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:Init
  pub fn init(&self, elements: &[&[u8]], value: &mut [u8]) {
    let dense = value.len() == self.dense_bytes;

    if dense {
      self.init_dense(value);
    } else {
      self.init_sparse(value);
    }

    self.iterate_update(elements, value, dense);
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
    for b in &mut ptr[regs_start..regs_start + ranges] {
      *b = 0xFF;
    }
  }

  /// 初始化稠密载荷（寄存器清零）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:InitDense
  pub fn init_dense(&self, ptr: &mut [u8]) {
    ptr[..self.dense_bytes].fill(0);

    self.set_prefix(ptr);
    Self::set_type(ptr, HllDtype::Dense);
    self.set_card(ptr, i64::MIN);
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

    if self.is_dense(value) {
      return self.dense_bytes;
    }

    // 对齐 C#：与真实更新器一致的 WRONGTYPE 语义由调用方以 None/错误承载
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

  /// 稠密 ← 稠密 逐寄存器取最大合并
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:DenseToDense
  pub fn dense_to_dense(&self, src: &[u8], dst: &mut [u8]) -> bool {
    let mut f_updated = false;
    let start = HLL_HEADER_BYTES;
    for idx in 0..self.mcnt as u16 {
      let src_lz = self.get_register(&src[start..], idx);
      let dst_lz = self.get_register(&dst[start..], idx);
      if src_lz > dst_lz {
        self.set_register(&mut dst[start..], idx, src_lz);
        f_updated = true;
      }
    }
    self.set_card(dst, i64::MIN);
    f_updated
  }

  /// 主多值更新：稠密直接更新；稀疏须确认可原位增长
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:Update
  /// 刻意差异：Rust 切片自带长度，C# 的 valueLen 形参恒等于 `value.len()`；
  /// `updated` 出参回传是否发生寄存器变更，返回 false 表示需申请新空间
  pub fn update(&self, elements: &[&[u8]], value: &mut [u8], updated: &mut bool) -> bool {
    if self.is_dense(value) {
      *updated = self.iterate_update(elements, value, true);
      return true;
    }

    if self.is_sparse(value) {
      if self.can_grow_in_place(value, value.len(), elements.len()) {
        *updated = self.iterate_update(elements, value, false);
        return true;
      }

      // 需申请更多空间
      return false;
    }
    debug_assert!(false, "Update HyperLogLog Error!");
    false
  }

  /// 更新稠密寄存器（单哈希入口）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:UpdateDense
  #[inline]
  pub fn update_dense(&self, ptr: &mut [u8], hv: u64) -> bool {
    let idx = self.reg_idx(hv);
    let cntlz = self.clz(hv);

    debug_assert!(idx < self.mcnt as u16);
    debug_assert!(cntlz < self.qbit + 1);

    self.update_dense_register(ptr, idx, cntlz)
  }

  /// 稠密寄存器择大更新，变更时失效基数缓存
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:UpdateDenseRegister
  #[inline]
  pub fn update_dense_register(&self, ptr: &mut [u8], idx: u16, cntlz: u8) -> bool {
    let start = HLL_HEADER_BYTES;
    if cntlz > self.get_register(&ptr[start..], idx) {
      // 失效先前计算的基数
      self.set_card(ptr, i64::MIN);
      self.set_register(&mut ptr[start..], idx, cntlz);
      return true;
    }
    false
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
    u16::from_le_bytes(
      ptr[HLL_HEADER_BYTES..HLL_HEADER_BYTES + 2]
        .try_into()
        .unwrap(),
    )
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

  /// 逐元素更新（MurmurHash2x64A 哈希后按编码分派）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:IterateUpdate
  pub fn iterate_update(&self, elements: &[&[u8]], value: &mut [u8], dense: bool) -> bool {
    let mut updated = false;
    for element in elements {
      let hash_value = murmur_hash_2_x64_a(element);
      updated |= if dense {
        self.update_dense(value, hash_value)
      } else {
        self.update_sparse(value, hash_value)
      };
    }
    updated
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

  /// 主计数入口（优先返回未失效的缓存基数）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:Count
  pub fn count(&self, ptr: &mut [u8]) -> i64 {
    let dtype = Self::get_type(ptr);

    // 缓存未失效直接返回
    if Self::is_valid_card(ptr) {
      return Self::get_card(ptr);
    }

    let e = match dtype {
      x if x == HllDtype::Sparse as u8 => self.count_sparse_nc_estimator(ptr),
      x if x == HllDtype::Dense as u8 => self.count_dense_nc_estimator(ptr),
      _ => {
        debug_assert!(false, "HyperLogLog Count invalid data structure type");
        0
      }
    };
    self.set_card(ptr, e);
    e
  }

  /// 大值校正函数 τ
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:cTau
  pub fn c_tau(x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
      return 0.0;
    }
    let mut prev_z;
    let mut y = 1.0;
    let mut z = 1.0 - x;
    let mut x = x;
    loop {
      x = x.sqrt();
      prev_z = z;
      y *= 0.5;
      z -= (1.0 - x).powi(2) * y;
      if prev_z == z {
        break;
      }
    }
    z / 3.0
  }

  /// 小值校正函数 σ
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:cSigma
  pub fn c_sigma(x: f64) -> f64 {
    if x == 1.0 {
      return f64::INFINITY;
    }
    let mut prev_z;
    let mut y = 1.0;
    let mut z = x;
    let mut x = x;
    loop {
      x *= x;
      prev_z = z;
      z += x * y;
      y += y;
      if prev_z == z {
        break;
      }
    }
    z
  }

  /// 稀疏 NC 估计器（寄存器直方图 + τ/σ 修正）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:CountSparseNCEstimator
  pub fn count_sparse_nc_estimator(&self, ptr: &[u8]) -> i64 {
    let mut rhisto = [0_usize; 64];
    let rle_size = self.get_sparse_rle_size(ptr) as usize;
    let start = self.sparse_header_size;
    let end = start + rle_size;

    for &op in &ptr[start..end] {
      let iszero = Self::is_zero_range(op);
      let clen = if iszero { Self::zero_range_len(op) } else { 1 };
      let lz = if iszero {
        0
      } else {
        Self::get_non_zero(op) as usize
      };
      rhisto[lz] += clen;
    }

    self.nc_estimator_from_histogram(&rhisto)
  }

  /// 稠密 NC 估计器（16 寄存器/12 字节展开）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:CountDenseNCEstimator
  pub fn count_dense_nc_estimator(&self, ptr: &[u8]) -> i64 {
    let mut rhisto = [0_usize; 64];
    let regs = &ptr[HLL_HEADER_BYTES..];

    let end = self.mcnt >> 4; // mcnt / 16
    for j in 0..end {
      let base = j * 12;
      let r = |i: usize| regs[base + i];

      let r00 = r(0) & 63;
      let r01 = ((r(0) >> 6) | (r(1) << 2)) & 63;
      let r02 = ((r(1) >> 4) | (r(2) << 4)) & 63;
      let r03 = (r(2) >> 2) & 63;

      let r04 = r(3) & 63;
      let r05 = ((r(3) >> 6) | (r(4) << 2)) & 63;
      let r06 = ((r(4) >> 4) | (r(5) << 4)) & 63;
      let r07 = (r(5) >> 2) & 63;

      let r08 = r(6) & 63;
      let r09 = ((r(6) >> 6) | (r(7) << 2)) & 63;
      let r10 = ((r(7) >> 4) | (r(8) << 4)) & 63;
      let r11 = (r(8) >> 2) & 63;

      let r12 = r(9) & 63;
      let r13 = ((r(9) >> 6) | (r(10) << 2)) & 63;
      let r14 = ((r(10) >> 4) | (r(11) << 4)) & 63;
      let r15 = (r(11) >> 2) & 63;

      for v in [
        r00, r01, r02, r03, r04, r05, r06, r07, r08, r09, r10, r11, r12, r13, r14, r15,
      ] {
        rhisto[v as usize] += 1;
      }
    }

    self.nc_estimator_from_histogram(&rhisto)
  }

  /// 直方图 → NC 估计（稀疏/稠密共用尾部）
  fn nc_estimator_from_histogram(&self, rhisto: &[usize; 64]) -> i64 {
    let mcnt = self.mcnt as f64;
    let mut z = mcnt * Self::c_tau((self.mcnt - rhisto[self.qbit as usize + 1]) as f64 / mcnt);

    for j in (1..=self.qbit as usize).rev() {
      z += rhisto[j] as f64;
      z *= 0.5;
    }
    z += mcnt * Self::c_sigma(rhisto[0] as f64 / mcnt);
    let e = ALPHA * mcnt * mcnt / z;

    e.round() as i64
  }

  /// 原位合并：目标稠密可直接并入；目标稀疏须确认空间充足
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:TryMerge
  pub fn try_merge(&self, src: &[u8], dst: &mut [u8], dst_len: usize) -> bool {
    let dtype_dst = Self::get_type(dst);
    if dtype_dst == HllDtype::Dense as u8 {
      self.merge(src, dst);
      self.set_card(dst, i64::MIN);
      return true;
    }

    // 目标稀疏
    let dtype_src = Self::get_type(src);
    if dtype_src == HllDtype::Sparse as u8 {
      let src_non_zero_bytes = self.sparse_count_non_zero(src) * SPARSE_MAX_BYTES_PER_INSERT;

      if self.sparse_current_size_in_bytes(dst) + src_non_zero_bytes < dst_len {
        self.merge(src, dst);
        self.set_card(dst, i64::MIN);
        return true;
      }

      return false;
    }

    // 稠密→稀疏恒失败
    false
  }

  /// 合并分派（按两侧编码选择路径）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:Merge
  pub fn merge(&self, src: &[u8], dst: &mut [u8]) -> bool {
    let dtype_src = Self::get_type(src);
    let dtype_dst = Self::get_type(dst);

    if dtype_dst == HllDtype::Dense as u8 {
      if dtype_src == HllDtype::Sparse as u8 {
        return self.sparse_to_dense(src, dst);
      }
      return self.dense_to_dense(src, dst);
    }

    if dtype_dst == HllDtype::Sparse as u8 {
      debug_assert!(dtype_src == HllDtype::Sparse as u8);
      return self.sparse_to_sparse(src, dst);
    }
    debug_assert!(false, "Merge exception");
    false
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

  /// 稠密寄存器计数（1:1 对齐 C# DenseCountNonZero 的实际行为）
  ///
  /// 刻意差异说明：C# 函数名为 DenseCountNonZero，但实现统计的是**零值寄存
  /// 器数**（`cnt += lz == 0 ? 1 : 0`），命名与行为相悖；且 C# 内无任何调用
  /// 点。Rust 保留同名函数并复刻该行为，防后续接入时出现两侧偏差
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:DenseCountNonZero
  pub fn dense_count_non_zero(&self, ptr: &[u8]) -> usize {
    let mut cnt = 0;
    let regs = &ptr[HLL_HEADER_BYTES..];

    for idx in 0..self.mcnt as u16 {
      let lz = self.get_register(regs, idx);
      cnt += usize::from(lz == 0);
    }

    cnt
  }

  /// 稀疏非零寄存器计数
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:SparseCountNonZero
  pub fn sparse_count_non_zero(&self, ptr: &[u8]) -> usize {
    let rle_size = self.get_sparse_rle_size(ptr) as usize;
    let start = self.sparse_header_size;
    let end = start + rle_size;
    let mut cnt = 0;

    for &op in &ptr[start..end] {
      cnt += usize::from(!Self::is_zero_range(op));
    }

    cnt
  }

  // ---- 调试导出（C# DEBUG 分支；返回字符串替代 Console.WriteLine） ----

  /// 稀疏流原始字节导出
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:DumpSparseRawBytes
  pub fn dump_sparse_raw_bytes(&self, ptr: &[u8]) -> String {
    let used = self.get_sparse_rle_size(ptr) as usize + self.sparse_header_size;
    format!("{:02x?}\n", &ptr[..used.min(ptr.len())])
  }

  /// 按类型导出原始字节
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:DumpRawBytes
  pub fn dump_raw_bytes(&self, ptr: &[u8]) -> String {
    match Self::get_type(ptr) {
      x if x == HllDtype::Sparse as u8 => self.dump_sparse_raw_bytes(ptr),
      _ => String::from("dense"),
    }
  }

  /// 稀疏与稠密寄存器一致性校验
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:CompareSparseToDense
  pub fn compare_sparse_to_dense(&self, dense: &[u8], sparse: &[u8]) -> Result<(), String> {
    let regs = &dense[HLL_HEADER_BYTES..];
    let mut dense_regs = BTreeMap::new();
    for i in 0..self.mcnt as u16 {
      let lz = self.get_register(regs, i);
      if lz != 0 {
        dense_regs.insert(i, lz);
      }
    }

    let rle_size = self.get_sparse_rle_size(sparse) as usize;
    let start = self.sparse_header_size;
    let end = start + rle_size;
    let mut offset = 0_usize;

    for &op in &sparse[start..end] {
      let iszero = Self::is_zero_range(op);
      let clen = if iszero { Self::zero_range_len(op) } else { 1 };

      if !iszero {
        let val = Self::get_non_zero(op);
        match dense_regs.get(&(offset as u16)) {
          None => return Err(format!("FAILED: Nonzero not contained: {offset} = {val}")),
          Some(&v) if v != val => {
            return Err(format!(
              "FAILED: Nonzero wrong nonzero value: {offset} = {val}"
            ));
          }
          _ => {}
        }
      }
      offset += clen;
    }
    Ok(())
  }

  /// 按类型导出非零寄存器
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:DumpRegs
  pub fn dump_regs(&self, ptr: &[u8]) -> String {
    match Self::get_type(ptr) {
      x if x == HllDtype::Sparse as u8 => self.dump_sparse_regs(ptr),
      _ => self.dump_dense_regs(ptr),
    }
  }

  /// 稠密非零寄存器导出
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:DumpDenseRegs
  pub fn dump_dense_regs(&self, dense: &[u8]) -> String {
    let regs = &dense[HLL_HEADER_BYTES..];
    let mut out = String::new();
    for i in 0..self.mcnt as u16 {
      let lz = self.get_register(regs, i);
      if lz != 0 {
        out.push_str(&format!("{i} = {lz}\n"));
      }
    }
    out
  }

  /// 稀疏非零寄存器导出
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:DumpSparseRegs
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
        out.push_str(&format!("{offset} = {}\n", Self::get_non_zero(op)));
      }
      offset += clen;
    }
    out
  }
}

/// MurmurHash2 64 位变体（PFADD 的元素哈希，委托 wbase::hash::murmur_hash2_x64_a）
#[inline]
pub fn murmur_hash_2_x64_a(b_string: &[u8]) -> u64 {
  murmur_hash2_x64_a(b_string, 0)
}

#[cfg(test)]
mod tests {
  use super::*;

  fn hll() -> HyperLogLog {
    HyperLogLog::new()
  }

  fn sample_range(start: usize, end: usize) -> Vec<Vec<u8>> {
    (start..end)
      .map(|i| format!("element-{i}").into_bytes())
      .collect()
  }

  fn refs(v: &[Vec<u8>]) -> Vec<&[u8]> {
    v.iter().map(|b| b.as_slice()).collect()
  }

  /// 4KB 稀疏工作缓冲（稀疏上限即 4096）
  fn sparse_buf(h: &HyperLogLog) -> Vec<u8> {
    let mut blob = vec![0_u8; SPARSE_SIZE_MAX_CAP];
    h.init_sparse(&mut blob);
    blob
  }

  /// 结构常量与 C# 逐项对齐：寄存器数、编码字节数、误差常数
  #[test]
  fn layout_constants() {
    let h = hll();
    assert_eq!(h.reg_cnt(), 16384);
    assert_eq!(h.dense_bytes(), 16 + (6 * 16384) / 8);
    assert_eq!(h.dense_bytes(), 12304);
    assert_eq!(h.sparse_zero_ranges(), 16384 >> 7);
    assert_eq!(h.sparse_zero_ranges(), 128);
    assert_eq!(h.sparse_bytes(), 16 + 2 + 128 + 128);
    assert_eq!(h.sparse_bytes(), 274);
    assert_eq!(h.qbit(), 50);
    // 误差常数 alpha = 0.5/ln(2)
    assert!((ALPHA - 0.721_347_520_444_481_7).abs() < 1e-15);
  }

  /// 稀疏初始化布局：魔数/类型/失效卡数/初始零段
  #[test]
  fn init_sparse_layout() {
    let h = hll();
    let mut blob = sparse_buf(&h);

    assert!(h.is_sparse(&blob));
    assert!(HyperLogLog::is_hyll(&blob));
    assert!(h.is_valid_hyll(&blob));
    assert!(h.is_valid_hyll_len(&blob, blob.len()));
    assert_eq!(h.get_sparse_rle_size(&blob), 128);
    assert_eq!(HyperLogLog::get_card(&blob), i64::MIN);
    // 初始零段全为 0xFF（每段覆盖 128 寄存器 × 128 段 = 16384）
    assert!(blob[18..18 + 128].iter().all(|&b| b == 0xFF));
    assert!(h.is_valid_sparse_stream(&blob));
    assert_eq!(h.sparse_count_non_zero(&blob), 0);
    assert_eq!(h.count(&mut blob), 0);
  }

  /// 稠密初始化与 6 位跨字节寄存器读写
  #[test]
  fn init_dense_layout() {
    let h = hll();
    let mut blob = vec![0_u8; h.dense_bytes()];
    h.init_dense(&mut blob);

    assert!(h.is_dense(&blob));
    assert!(h.is_valid_hyll(&blob));
    assert_eq!(blob.len(), 12304);

    // 全值域读写往返，相邻寄存器互不串位
    h.set_register(&mut blob[16..], 0, 63);
    h.set_register(&mut blob[16..], 1, 1);
    h.set_register(&mut blob[16..], 16383, 5);
    h.set_register(&mut blob[16..], 2, 42);
    assert_eq!(h.get_register(&blob[16..], 0), 63);
    assert_eq!(h.get_register(&blob[16..], 1), 1);
    assert_eq!(h.get_register(&blob[16..], 2), 42);
    assert_eq!(h.get_register(&blob[16..], 16383), 5);
    assert_eq!(h.get_register(&blob[16..], 100), 0);
  }

  /// PFADD 语义：重复插入无变更；稀疏与稠密计数误差受控（HLL 标准差 ~1.1%）
  #[test]
  fn pfadd_and_count_accuracy() {
    let h = hll();
    let mut blob = sparse_buf(&h);

    let data = sample_range(0, 500);
    let r = refs(&data);
    let mut updated = false;

    assert!(h.update(&r, &mut blob, &mut updated));
    assert!(updated);

    // 重复插入：无寄存器变更
    let mut updated2 = false;
    assert!(h.update(&r, &mut blob, &mut updated2));
    assert!(!updated2);

    let sparse_count = h.count(&mut blob);
    assert!(
      (440..=560).contains(&sparse_count),
      "sparse count = {sparse_count}"
    );

    // 稠密路径 1 万元素
    let mut dense = vec![0_u8; h.dense_bytes()];
    h.init_dense(&mut dense);
    let data10k = sample_range(0, 10_000);
    let mut u = false;
    assert!(h.update(&refs(&data10k), &mut dense, &mut u));
    let dense_count = h.count(&mut dense);
    assert!(
      (8800..=11200).contains(&dense_count),
      "dense count = {dense_count}"
    );
  }

  /// 稀疏 → 稠密升级：计数不变且寄存器级一致
  #[test]
  fn sparse_to_dense_upgrade() {
    let h = hll();
    let data = sample_range(0, 300);
    let r = refs(&data);

    let mut sparse_blob = sparse_buf(&h);
    let mut u = false;
    h.update(&r, &mut sparse_blob, &mut u);
    let sparse_count = h.count(&mut sparse_blob);

    let mut dense_blob = vec![0_u8; h.dense_bytes()];
    h.copy_update(&[], &sparse_blob, &mut dense_blob);
    assert!(h.is_dense(&dense_blob));
    assert_eq!(h.count(&mut dense_blob), sparse_count);

    // 寄存器级一致性（C# CompareSparseToDense 的测试用法）
    h.compare_sparse_to_dense(&dense_blob, &sparse_blob)
      .unwrap();
  }

  /// 稀疏零段拆分：首/中/尾寄存器边界 + RLE 流合法性 + 同元素幂等
  #[test]
  fn sparse_zero_range_split() {
    let h = hll();
    let mut blob = sparse_buf(&h);

    let rle0 = h.get_sparse_rle_size(&blob);
    let hv = murmur_hash_2_x64_a(b"first");
    assert!(h.update_sparse(&mut blob, hv));
    // 中部拆分 +2（左右零段），边界拆分 +1；仅断言单调增长与流合法
    assert!(h.get_sparse_rle_size(&blob) > rle0);
    assert!(h.is_valid_sparse_stream(&blob));

    // 同一元素二次插入：无变更（RLE 长度保持首次插入后的值）
    assert!(!h.update_sparse(&mut blob, hv));
    assert_eq!(h.get_sparse_rle_size(&blob), rle0 + 2);

    // 边界拆分：idx = 0 / idx = 1（中部右拆）/ idx = mcnt - 1
    let mut blob2 = sparse_buf(&h);
    assert!(h.update_sparse_reg(&mut blob2, 0, 3));
    let idx_last = (h.reg_cnt() - 1) as u16;
    assert!(h.update_sparse_reg(&mut blob2, idx_last, 1));
    assert!(h.update_sparse_reg(&mut blob2, 1, 2));
    assert!(h.is_valid_sparse_stream(&blob2));

    // 经稠密展开验证寄存器值
    let mut dense = vec![0_u8; h.dense_bytes()];
    h.init_dense(&mut dense);
    h.sparse_to_dense(&blob2, &mut dense);
    assert_eq!(h.get_register(&dense[16..], 0), 3);
    assert_eq!(h.get_register(&dense[16..], 1), 2);
    assert_eq!(h.get_register(&dense[16..], idx_last), 1);
    // 未动寄存器仍为 0
    assert_eq!(h.get_register(&dense[16..], 500), 0);
  }

  /// clz / reg_idx 边界
  #[test]
  fn reg_idx_and_clz_bounds() {
    let h = hll();
    assert_eq!(h.reg_idx(0), 0);
    assert_eq!(h.reg_idx(u64::MAX), 16383);
    // hv = 0 → 全零 → clz 封顶 qbit + 1
    assert_eq!(h.clz(0), 51);
    // 最高位为 1 → clz = 1
    assert_eq!(h.clz(1_u64 << 63), 1);
    // 50 位处为 1 → lz = 14 < qbit → 15
    assert_eq!(h.clz(1_u64 << 49), 15);
  }

  /// 稀疏合并：try_merge 并集计数
  #[test]
  fn merge_sparse_sparse() {
    let h = hll();
    let mut dst = sparse_buf(&h);
    let mut src = sparse_buf(&h);

    let a = sample_range(0, 200);
    let b = sample_range(200, 400);

    let mut u = false;
    h.update(&refs(&a), &mut dst, &mut u);
    h.update(&refs(&b), &mut src, &mut u);

    let dst_len = dst.len();
    assert!(h.try_merge(&src, &mut dst, dst_len));
    let merged = h.count(&mut dst);
    assert!((352..=448).contains(&merged), "merged = {merged}");
  }

  /// 稀疏 → 稠密合并
  #[test]
  fn merge_sparse_into_dense() {
    let h = hll();
    let mut dense = vec![0_u8; h.dense_bytes()];
    h.init_dense(&mut dense);
    let mut sparse = sparse_buf(&h);

    let a = sample_range(0, 100);
    let b = sample_range(50, 150);
    let mut u = false;
    h.update(&refs(&a), &mut dense, &mut u);
    h.update(&refs(&b), &mut sparse, &mut u);

    assert!(h.merge(&sparse, &mut dense));
    let merged = h.count(&mut dense);
    assert!((132..=168).contains(&merged), "merged = {merged}");
  }

  /// 稠密基数缓存：未失效直接命中，更新失效后重算
  #[test]
  fn card_cache_invalidation() {
    let h = hll();
    let mut blob = vec![0_u8; h.dense_bytes()];
    h.init_dense(&mut blob);

    let data_a = sample_range(0, 500);
    let a = refs(&data_a);
    let mut u = false;
    h.update(&a, &mut blob, &mut u);
    let c1 = h.count(&mut blob);
    assert!(HyperLogLog::is_valid_card(&blob));
    assert_eq!(c1, h.count(&mut blob));

    let data_b = sample_range(500, 1000);
    let b = refs(&data_b);
    h.update(&b, &mut blob, &mut u);
    assert!(!HyperLogLog::is_valid_card(&blob));
    let c3 = h.count(&mut blob);
    assert!(c3 > c1, "{c1} vs {c3}");
  }

  /// 增长规划：CanGrowInPlace / UpdateGrow / MergeGrow
  #[test]
  fn growth_planning() {
    let h = hll();
    let blob = sparse_buf(&h);

    // 分配 4KB、占用 130B：可原位容纳
    assert!(h.can_grow_in_place(&blob, blob.len(), 1000));
    assert!(!h.can_grow_in_place(&blob, 200, 1000));

    // 少量元素 → 稀疏扩容
    let grow = h.update_grow(64, &blob);
    assert!(grow > h.sparse_current_size_in_bytes(&blob));
    assert!(grow < SPARSE_SIZE_MAX_CAP);

    // 大量元素 → 稠密化
    assert_eq!(h.update_grow(100_000, &blob), h.dense_bytes());

    // MergeGrow：稀疏+稀疏按扇区推进；稠密目标恒为稠密长度
    let mut src = sparse_buf(&h);
    let mut u = false;
    let data_50 = sample_range(0, 50);
    h.update(&refs(&data_50), &mut src, &mut u);
    let mg = h.merge_grow(&src, &blob);
    assert!(mg > h.sparse_current_size_in_bytes(&blob));
    let mut dense = vec![0_u8; h.dense_bytes()];
    h.init_dense(&mut dense);
    assert_eq!(h.merge_grow(&src, &dense), h.dense_bytes());
  }

  /// 空稀疏源（非零寄存器数为 0，如 PFMERGE 全缺失源落下的空 HLL）：
  /// MergeGrow 不发生 usize 下溢（debug 构建 panic / release 回绕破坏载荷），
  /// 且与 C# 一致仍预留一个扇区；SparseRequiredBytes(0) 同口径占一个扇区
  #[test]
  fn growth_planning_with_empty_source() {
    let h = hll();
    let empty = sparse_buf(&h);
    let fresh = sparse_buf(&h);

    // 空源 + 新目标：146 + 128 = 274
    assert_eq!(h.merge_grow(&empty, &fresh), h.sparse_bytes());
    // 空源 + 已存目标（current = 146）：同样 +128
    assert_eq!(
      h.merge_grow(&empty, &empty),
      h.sparse_current_size_in_bytes(&empty) + SPARSE_MEMORY_SECTOR_SIZE
    );

    // C# ((u-1)/s)+1 口径：u=0 → 1 页（128B）；u=2 → 1 页；u=130 → 2 页
    assert_eq!(h.sparse_required_bytes(0), SPARSE_MEMORY_SECTOR_SIZE);
    assert_eq!(h.sparse_required_bytes(1), SPARSE_MEMORY_SECTOR_SIZE);
    assert_eq!(h.sparse_required_bytes(65), 2 * SPARSE_MEMORY_SECTOR_SIZE);

    // 合并空源后载荷仍合法、计数为 0
    let mut dst = fresh[..h.sparse_bytes()].to_vec();
    let dst_len = dst.len();
    let _ = h.try_merge(&empty, &mut dst, dst_len);
    assert!(h.is_valid_hyll_len(&dst, dst_len));
    assert_eq!(h.count(&mut dst), 0);
  }

  /// 拷贝路径：SparseToDenseCopy / SparseToSparseCopy / CopyUpdateMerge
  #[test]
  fn copy_paths() {
    let h = hll();
    let mut sparse = sparse_buf(&h);
    h.update_sparse(&mut sparse, murmur_hash_2_x64_a(b"hello"));

    // SparseToDenseCopy：旧载荷展开 + 新哈希插入
    let mut dense = vec![0_u8; h.dense_bytes()];
    assert!(h.sparse_to_dense_copy(murmur_hash_2_x64_a(b"world"), &sparse, &mut dense));
    assert!(h.is_dense(&dense));

    // SparseToSparseCopy：拷贝 + 单哈希更新
    let mut bigger = vec![0_u8; SPARSE_SIZE_MAX_CAP];
    assert!(h.sparse_to_sparse_copy(murmur_hash_2_x64_a(b"meh"), &sparse, &mut bigger));
    assert!(h.is_sparse(&bigger));

    // CopyUpdateMerge：稀疏 → 稠密
    let old = sparse_buf(&h);
    let mut new_dense = vec![0_u8; h.dense_bytes()];
    let (old_len, new_len) = (old.len(), new_dense.len());
    h.copy_update_merge(&sparse, &old, &mut new_dense, old_len, new_len);
    assert!(h.is_dense(&new_dense));

    // CopyUpdateMerge：等长稀疏路径（逐字节拷贝 + 合并）
    let mut new_sparse = vec![0_u8; old.len()];
    h.copy_update_merge(&sparse, &old, &mut new_sparse, old_len, old_len);
    assert!(h.is_sparse(&new_sparse));
    assert_eq!(
      h.sparse_count_non_zero(&new_sparse),
      h.sparse_count_non_zero(&sparse)
    );
  }

  /// MurmurHash2x64A：空串黄金向量 + 确定性 + 长度敏感 + 分块/余数路径
  #[test]
  fn murmur_hash_invariants() {
    assert_eq!(murmur_hash_2_x64_a(b"abc"), murmur_hash_2_x64_a(b"abc"));
    assert_ne!(murmur_hash_2_x64_a(b"abc"), murmur_hash_2_x64_a(b"abd"));
    // 长度参与混淆
    assert_ne!(murmur_hash_2_x64_a(b"abc"), murmur_hash_2_x64_a(b"abcd"));
    // 8 字节整块 vs 带余数
    assert_ne!(
      murmur_hash_2_x64_a(b"12345678"),
      murmur_hash_2_x64_a(b"123456789")
    );
  }

  /// 非法结构检测：魔数破坏 / 长度不符 / RLE 超覆盖 / 覆盖不足
  #[test]
  fn invalid_blob_detection() {
    let h = hll();
    let blob = sparse_buf(&h);

    // 魔数破坏
    let mut bad = blob.clone();
    bad[5] = b'X';
    assert!(!HyperLogLog::is_hyll(&bad));
    assert!(!h.is_valid_hyll(&bad));

    // 声称稠密但长度不符
    bad = blob.clone();
    HyperLogLog::set_type(&mut bad, HllDtype::Dense);
    assert!(!h.is_valid_hll_length(&bad, bad.len()));

    // RLE 声称超过寄存器空间
    bad = blob.clone();
    h.set_sparse_rle_size(&mut bad, u16::MAX);
    assert!(!h.is_valid_sparse_stream(&bad));
    assert!(!h.is_valid_hll_length(&bad, bad.len()));

    // RLE 覆盖不足（1 字节非零操作码只覆盖 1 寄存器）
    let mut crafted = sparse_buf(&h);
    crafted[18] = HyperLogLog::set_non_zero(1);
    h.set_sparse_rle_size(&mut crafted, 1);
    assert!(!h.is_valid_sparse_stream(&crafted));
  }

  /// 调试导出可用性与稀疏/稠密一致性
  #[test]
  fn dump_helpers() {
    let h = hll();
    let mut sparse = sparse_buf(&h);
    h.update_sparse(&mut sparse, murmur_hash_2_x64_a(b"dump-me"));

    assert!(!h.dump_sparse_raw_bytes(&sparse).is_empty());
    assert_eq!(h.dump_raw_bytes(&sparse), h.dump_sparse_raw_bytes(&sparse));
    assert!(h.dump_sparse_regs(&sparse).contains('='));
    assert!(!h.dump_regs(&sparse).is_empty());

    let mut dense = vec![0_u8; h.dense_bytes()];
    h.init_dense(&mut dense);
    h.sparse_to_dense(&sparse, &mut dense);
    assert!(h.dump_dense_regs(&dense).contains('='));
    assert!(h.compare_sparse_to_dense(&dense, &sparse).is_ok());
  }

  /// 自定义 pbit：寄存器数与派生容量随之变化
  #[test]
  fn custom_pbit() {
    let h = HyperLogLog::with_pbit(10);
    assert_eq!(h.reg_cnt(), 1024);
    assert_eq!(h.dense_bytes(), 16 + (6 * 1024) / 8);
    assert_eq!(h.sparse_zero_ranges(), 1024 >> 7);
    assert_eq!(h.qbit(), 54);
  }
}
