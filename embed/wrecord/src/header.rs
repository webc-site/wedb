use core::mem::{align_of, size_of};

use crate::{
  codec::checked_record_size,
  error::{Error, Result},
};

/// 记录头字节大小（16 字节）
///
/// 对标 C# Tsavorite `Constants.FixedHeaderSize = RecordInfo.Size + RecordDataHeader.Size = 8 + 8`：
/// Rust 将 C# 的 RecordInfo（8B 复合状态字）与 RecordDataHeader（8B 长度字）合并为
/// 单一 16 字节定长头，布局为 `[prev_address: u64][key_len: u32][val_len: u32]`（小端）。
///
/// 与 C# 的位布局差异（Rust 为自定义磁盘格式，不与 C# 二进制兼容）：
/// - C# RecordInfo：bits 0..47 地址、bit 47 ReadCache（地址位内最高位）、bit 48 Tombstone、
///   bit 49 Valid、bit 50 InNewVersion、bit 51 Modified、bit 52 Sealed、bits 53..63 保留；
/// - Rust 复合字：bits 0..47 地址、bits 48..58 松弛填充（8 位词 + 3 位余数，替代 C# 的
///   Valid 位与全部保留位，填充由 C# RecordDataHeader 的 8 位 FillerWords 扩展为单字节精度）、
///   bits 59..63 为 Modified/Sealed/InNewVersion/ReadCache/Tombstone 五个标志位；
/// - 无 Valid 位：C# 的 Valid/Sealed 并发状态机由上层（whlog/wreviv）以原子 CAS 承担，
///   纯格式层不感知；
/// - 注意 [HEADER_READ_CACHE_BIT] 为本 头复合字 内的 bit 62，与 wbase::addr::HEADER_READ_CACHE_BIT
///   （哈希指针地址空间内的 bit 47）同名不同值，分属两个不同的 64 位字。
pub const HEADER_SIZE: usize = 16;

/// 换页填充标记中的特殊魔数（key_len 为 u32::MAX 表示 Pad 填充）
pub const PAD_KEY_LEN: u32 = u32::MAX;

/// 48 位逻辑地址掩码与位宽（统一由 wbase 提供）
pub use wbase::addr::{ADDRESS_BITS, ADDRESS_MASK};

/// 8 位填充词偏移量（bits 48..55，每词 8 字节，最多 255 * 8 = 2040 字节松弛空间，对标 libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs:FillerWords）
pub(crate) const FILLER_WORDS_SHIFT: u32 = ADDRESS_BITS;
pub(crate) const FILLER_WORDS_MASK: u64 = 0xFFu64 << FILLER_WORDS_SHIFT;

/// 3 位单字节填充余数偏移量（bits 56..58，支持 0..7 字节精细填充，消除非 8 字节对齐差值的物理长度漂移）
pub(crate) const FILLER_REM_SHIFT: u32 = 56;
pub(crate) const FILLER_REM_MASK: u64 = 0x07u64 << FILLER_REM_SHIFT;

/// 复合松弛空间掩码（bits 48..58，覆盖 8 字节块与单字节余数）
pub(crate) const FILLER_TOTAL_MASK: u64 = FILLER_WORDS_MASK | FILLER_REM_MASK;

/// 最大可表示的单记录松弛填充字节数（255 * 8 + 7 = 2047 字节）
pub const MAX_FILLER_BYTES: usize = 2047;

/// 修改位掩码（第 59 位，对标 C# Tsavorite RecordInfo.Modified）
pub const MODIFIED_BIT: u64 = 1u64 << 59;

/// 密封位掩码（第 60 位，对标 C# Tsavorite RecordInfo.IsSealed / TrySeal，复活槽位锁定与冻结）
pub const SEALED_BIT: u64 = 1u64 << 60;

/// Checkpoint 检查点新版本标记位掩码（第 61 位，对标 C# Tsavorite RecordInfo.IsInNewVersion）
pub const IN_NEW_VERSION_BIT: u64 = 1u64 << 61;

/// 读缓存标记位掩码（第 62 位，对标 C# Tsavorite RecordInfo.IsReadCache / LogAddress.kIsReadCacheBitMask）
pub const HEADER_READ_CACHE_BIT: u64 = 1u64 << 62;

/// 墓碑标记位掩码（第 63 位: 0x8000_0000_0000_0000）
pub const TOMBSTONE_BIT: u64 = 1u64 << 63;

// 编译期静态断言 1：位域封闭性 —— 48 位地址 + 11 位松弛填充（8+3）+ 5 个标志位
// 恰好无缝铺满 64 位字，任何位既不重叠也不遗漏，宽度分配在编译期即可证明正确。
const _: () = assert!(
  (ADDRESS_MASK
    | FILLER_TOTAL_MASK
    | MODIFIED_BIT
    | SEALED_BIT
    | IN_NEW_VERSION_BIT
    | HEADER_READ_CACHE_BIT
    | TOMBSTONE_BIT)
    == u64::MAX
);

// 编译期静态断言 2：内存布局与磁盘序列化布局严格一致（16 字节、8 字节对齐，
// 对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Constants.cs:FixedHeaderSize = RecordInfo.Size + RecordDataHeader.Size 与 kRecordAlignment）
const _: () = assert!(size_of::<RecordHeader>() == HEADER_SIZE);
const _: () = assert!(align_of::<RecordHeader>() == 8);

/// 16 字节紧凑记录头结构体（C 对齐）
///
/// 内存排布（小端 16 字节）：
/// - `[0..8)`: `prev_address: u64`（低 48 位为前驱版本逻辑地址形成反向链表，bits 48..55 为 FillerWords 动态松弛填充词，bits 56..58 为 FillerRem 单字节余数，bit 59 为 MODIFIED 修改位，bit 60 为 SEALED 密封位，bit 61 为 IN_NEW_VERSION 纪元位，bit 62 为 READ_CACHE 读缓存位，最高位 1<<63 为 TOMBSTONE 墓碑标记）
/// - `[8..12)`: `key_len: u32`（键长度）
/// - `[12..16)`: `val_len: u32`（值长度）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(C)]
pub struct RecordHeader {
  /// 前驱版本逻辑地址与墓碑标记复合字段
  pub prev_address: u64,
  /// 键长度（字节数）
  pub key_len: u32,
  /// 值长度（字节数）
  pub val_len: u32,
}

/// 按开关置位/清零复合字中的指定位段（const fn，供各标志位 setter 共享）
#[inline(always)]
const fn with_bit(word: u64, mask: u64, on: bool) -> u64 {
  if on { word | mask } else { word & !mask }
}

impl RecordHeader {
  /// 构造新的记录头并校验 48 位地址有效性（const fn）
  #[inline]
  pub const fn new(prev_addr: u64, key_len: u32, val_len: u32, is_tombstone: bool) -> Result<Self> {
    if prev_addr & !ADDRESS_MASK != 0 {
      return Err(Error::AddressOverflow(prev_addr));
    }
    Ok(Self {
      prev_address: with_bit(prev_addr, TOMBSTONE_BIT, is_tombstone),
      key_len,
      val_len,
    })
  }

  /// 直接从原始数据快速构造（无地址检查，适用于内部或高性能路径）
  #[inline]
  pub const fn from_raw(prev_address: u64, key_len: u32, val_len: u32) -> Self {
    Self {
      prev_address,
      key_len,
      val_len,
    }
  }

  /// 提取 48 位前驱版本逻辑地址
  #[inline]
  pub const fn address(&self) -> u64 {
    self.prev_address & ADDRESS_MASK
  }

  /// 是否带有墓碑删除标记
  #[inline(always)]
  pub const fn is_tombstone(&self) -> bool {
    (self.prev_address & TOMBSTONE_BIT) != 0
  }

  /// 是否带有修改标记（对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:Modified）
  #[inline(always)]
  pub const fn is_modified(&self) -> bool {
    (self.prev_address & MODIFIED_BIT) != 0
  }

  /// 设置或清除修改标记（const fn）
  #[inline(always)]
  pub const fn set_modified(&mut self, modified: bool) {
    self.prev_address = with_bit(self.prev_address, MODIFIED_BIT, modified);
  }

  /// 是否带有密封标记（对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:IsSealed / TrySeal）
  #[inline(always)]
  pub const fn is_sealed(&self) -> bool {
    (self.prev_address & SEALED_BIT) != 0
  }

  /// 设置或清除密封标记（const fn）
  #[inline(always)]
  pub const fn set_sealed(&mut self, sealed: bool) {
    self.prev_address = with_bit(self.prev_address, SEALED_BIT, sealed);
  }

  /// 是否属于 Checkpoint 新版本纪元（对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:IsInNewVersion）
  #[inline(always)]
  pub const fn is_in_new_version(&self) -> bool {
    (self.prev_address & IN_NEW_VERSION_BIT) != 0
  }

  /// 设置或清除 Checkpoint 新版本纪元标记（const fn）
  #[inline(always)]
  pub const fn set_in_new_version(&mut self, in_new_version: bool) {
    self.prev_address = with_bit(self.prev_address, IN_NEW_VERSION_BIT, in_new_version);
  }

  /// 是否标记为读缓存记录（对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:IsReadCache / LogAddress.kIsReadCacheBitMask）
  #[inline(always)]
  pub const fn is_read_cache(&self) -> bool {
    (self.prev_address & HEADER_READ_CACHE_BIT) != 0
  }

  /// 设置或清除读缓存标记（const fn）
  #[inline(always)]
  pub const fn set_read_cache(&mut self, is_read_cache: bool) {
    self.prev_address = with_bit(self.prev_address, HEADER_READ_CACHE_BIT, is_read_cache);
  }

  /// 提取 8 位松弛填充词数量（每词代表 8 字节填充，对标 libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs:FillerWords）
  #[inline(always)]
  pub const fn filler_words(&self) -> u8 {
    ((self.prev_address & FILLER_WORDS_MASK) >> FILLER_WORDS_SHIFT) as u8
  }

  /// 提取 3 位单字节填充余数（0..7 字节）
  #[inline(always)]
  pub const fn filler_rem(&self) -> u8 {
    ((self.prev_address & FILLER_REM_MASK) >> FILLER_REM_SHIFT) as u8
  }

  /// 获取松弛填充字节总数（FillerWords * 8 + FillerRem，单字节级高精度）
  #[inline(always)]
  pub const fn filler_bytes(&self) -> usize {
    ((self.filler_words() as usize) << 3) | (self.filler_rem() as usize)
  }

  /// 一步设置完整的松弛填充字节数（自动分解为 8 字节词与单字节余数，超出 [MAX_FILLER_BYTES] 时静默钳位）
  #[inline(always)]
  pub const fn set_filler_bytes(&mut self, total_bytes: usize) {
    let clamped = if total_bytes > MAX_FILLER_BYTES {
      MAX_FILLER_BYTES
    } else {
      total_bytes
    };
    let words = ((clamped >> 3) as u64) << FILLER_WORDS_SHIFT;
    let rem = ((clamped & 7) as u64) << FILLER_REM_SHIFT;
    self.prev_address = (self.prev_address & !FILLER_TOTAL_MASK) | words | rem;
  }

  /// 设置 8 位松弛填充词数量（保留余数与其他高位标记）
  #[inline(always)]
  pub const fn set_filler_words(&mut self, words: u8) {
    self.prev_address =
      (self.prev_address & !FILLER_WORDS_MASK) | ((words as u64) << FILLER_WORDS_SHIFT);
  }

  /// 获取当前记录槽位物理容纳值的最大字节容量（val_len + filler_bytes）
  #[inline(always)]
  pub const fn val_capacity(&self) -> usize {
    (self.val_len as usize).saturating_add(self.filler_bytes())
  }

  /// 获取键长度
  #[inline]
  pub const fn key_len(&self) -> u32 {
    self.key_len
  }

  /// 获取值长度
  #[inline]
  pub const fn val_len(&self) -> u32 {
    self.val_len
  }

  /// 获取整条记录（头 + 键 + 值）的理论逻辑字节长度
  #[inline]
  pub const fn record_size(&self) -> usize {
    HEADER_SIZE
      .saturating_add(self.key_len as usize)
      .saturating_add(self.val_len as usize)
  }

  /// 获取整条记录在物理上占据的总字节大小（头 + 键 + 值 + 松弛填充）
  #[inline(always)]
  pub const fn physical_size(&self) -> usize {
    self.record_size().saturating_add(self.filler_bytes())
  }

  /// 安全计算整条记录理论字节长度（包含 16B 记录头），若计算溢出 usize 则返回 None
  #[inline]
  pub const fn checked_record_size(&self) -> Option<usize> {
    checked_record_size(self.key_len as usize, self.val_len as usize)
  }

  /// 安全计算整条记录物理字节大小（头 + 键 + 值 + 松弛填充），若溢出返回 None
  #[inline(always)]
  pub const fn checked_physical_size(&self) -> Option<usize> {
    if let Some(s) = self.checked_record_size() {
      s.checked_add(self.filler_bytes())
    } else {
      None
    }
  }

  /// 更新前驱版本逻辑地址（保留高位所有元数据，const fn）
  #[inline]
  pub const fn set_address(&mut self, prev_addr: u64) -> Result<()> {
    if prev_addr & !ADDRESS_MASK != 0 {
      return Err(Error::AddressOverflow(prev_addr));
    }
    self.prev_address = (prev_addr & ADDRESS_MASK) | (self.prev_address & !ADDRESS_MASK);
    Ok(())
  }

  /// 设置或清除墓碑标记（保留原有前驱地址与松弛填充词，const fn）
  #[inline(always)]
  pub const fn set_tombstone(&mut self, is_tombstone: bool) {
    self.prev_address = with_bit(self.prev_address, TOMBSTONE_BIT, is_tombstone);
  }

  /// 翻转墓碑标记位，并返回翻转后的墓碑状态（const fn）
  #[inline(always)]
  pub const fn flip_tombstone(&mut self) -> bool {
    self.prev_address ^= TOMBSTONE_BIT;
    self.is_tombstone()
  }

  /// 判断是否可以在原位等长更新指定长度的值（要求非墓碑且新值长度严格一致，const fn）
  #[inline(always)]
  pub const fn can_update_in_place(&self, new_val_len: usize) -> bool {
    !self.is_tombstone() && self.val_len as usize == new_val_len
  }

  /// 判断是否可以利用动态松弛原位更新指定长度的值（要求非墓碑且新值长度不超过槽位最大物理容量）
  #[inline(always)]
  pub const fn can_update_with_slack(&self, new_val_len: usize) -> bool {
    !self.is_tombstone()
      && new_val_len <= self.val_capacity()
      && (self.val_capacity() - new_val_len) <= MAX_FILLER_BYTES
  }

  /// 编码为 16 字节定长数组（小端编码，const fn，双 64 位整型融合打包）
  #[inline(always)]
  pub const fn to_bytes(&self) -> [u8; HEADER_SIZE] {
    let p = self.prev_address.to_le_bytes();
    let lens = ((self.key_len as u64) | ((self.val_len as u64) << 32)).to_le_bytes();
    [
      p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], lens[0], lens[1], lens[2], lens[3], lens[4],
      lens[5], lens[6], lens[7],
    ]
  }

  /// 从 16 字节定长数组直接解码记录头（const fn，双 64 位整型快速解包）
  #[inline(always)]
  pub const fn from_bytes(bytes: [u8; HEADER_SIZE]) -> Self {
    let prev_address = u64::from_le_bytes([
      bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]);
    let lens = u64::from_le_bytes([
      bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    ]);
    Self {
      prev_address,
      key_len: lens as u32,
      val_len: (lens >> 32) as u32,
    }
  }

  /// 将记录头编码写入目标切片（零堆分配）
  #[inline]
  pub fn write_to_slice(&self, dst: &mut [u8]) -> Result<()> {
    if let Some(chunk) = dst.first_chunk_mut::<HEADER_SIZE>() {
      *chunk = self.to_bytes();
      Ok(())
    } else {
      Err(Error::BufferTooShort {
        expected: HEADER_SIZE,
        actual: dst.len(),
      })
    }
  }

  /// 从切片前 16 字节解码记录头（const fn，复用 [Self::from_bytes] 零中间拷贝）
  #[inline]
  pub const fn from_slice(src: &[u8]) -> Result<Self> {
    match src.first_chunk::<HEADER_SIZE>() {
      Some(chunk) => Ok(Self::from_bytes(*chunk)),
      None => Err(Error::BufferTooShort {
        expected: HEADER_SIZE,
        actual: src.len(),
      }),
    }
  }

  /// 从切片前 16 字节尝试安全解码记录头（const fn，不足 16 字节返回 None）
  ///
  /// 供扫描器链式短路（`and_then`/`filter`）使用的 Option 风格探针（whlog/wkv 在用）。
  #[inline(always)]
  pub const fn decode_opt(src: &[u8]) -> Option<Self> {
    if let Some(chunk) = src.first_chunk::<HEADER_SIZE>() {
      Some(Self::from_bytes(*chunk))
    } else {
      None
    }
  }

  /// 头部是否为全零空记录（对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:IsNull 与 RecordDataHeader.GetRecordLength 零头守卫）
  ///
  /// Rust 将 C# 的 RecordInfo（8B）与长度字段（RDH）合并为 16 字节头，故空记录判定覆盖
  /// 前驱地址、键长、值长三者同时为零（前驱地址为 0 但键值非零属合法创世记录，不算空头）。
  /// 扫描器遇到空头应按最小 16 字节记录推进，严格对标 C# 零 RDH 守卫语义。
  #[inline(always)]
  pub const fn is_null(&self) -> bool {
    self.prev_address == 0 && self.key_len == 0 && self.val_len == 0
  }

  /// 构造换页填充 Pad 头（key_len 设为 [PAD_KEY_LEN]，val_len 设为剩余容纳字节数）
  #[inline(always)]
  pub const fn pad(remaining_bytes: usize) -> Self {
    let val_len = if remaining_bytes >= HEADER_SIZE {
      (remaining_bytes - HEADER_SIZE) as u32
    } else {
      0
    };
    Self {
      prev_address: 0,
      key_len: PAD_KEY_LEN,
      val_len,
    }
  }

  /// 是否为换页填充 Pad 记录头
  #[inline(always)]
  pub const fn is_pad(&self) -> bool {
    self.key_len == PAD_KEY_LEN
  }

  /// 快速判定切片前 16 字节是否全为零（const fn，不足 16 字节严格检查已有字节全零）
  ///
  /// 基于两个 64 位小端整数融合比对 `(w0 | w1) == 0`，消除单字节逐一比较与结构体构造开销，
  /// 为扫描器自旋等待在途空头提供最高性能的热路径探测。
  #[inline(always)]
  pub const fn is_zero_slice(src: &[u8]) -> bool {
    if let Some(chunk) = src.first_chunk::<HEADER_SIZE>() {
      let w0 = u64::from_le_bytes([
        chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
      ]);
      let w1 = u64::from_le_bytes([
        chunk[8], chunk[9], chunk[10], chunk[11], chunk[12], chunk[13], chunk[14], chunk[15],
      ]);
      (w0 | w1) == 0
    } else {
      let mut i = 0;
      while i < src.len() {
        if src[i] != 0 {
          return false;
        }
        i += 1;
      }
      true
    }
  }

  /// 快速只读探针：提取前驱逻辑地址（48 位）
  #[inline(always)]
  pub const fn read_address(src: &[u8]) -> Option<u64> {
    if let Some(chunk) = src.first_chunk::<8>() {
      Some(u64::from_le_bytes(*chunk) & ADDRESS_MASK)
    } else {
      None
    }
  }

  /// 快速只读探针：提取墓碑标记
  #[inline(always)]
  pub const fn read_is_tombstone(src: &[u8]) -> Option<bool> {
    if let Some(chunk) = src.first_chunk::<8>() {
      Some((u64::from_le_bytes(*chunk) & TOMBSTONE_BIT) != 0)
    } else {
      None
    }
  }

  /// 快速只读探针：判断是否为 Pad 填充头（复用 [Self::read_key_len] 消除重复字节解析）
  #[inline(always)]
  pub const fn read_is_pad(src: &[u8]) -> Option<bool> {
    match Self::read_key_len(src) {
      Some(key_len) => Some(key_len == PAD_KEY_LEN),
      None => None,
    }
  }

  /// 快速只读探针：提取键长
  #[inline(always)]
  pub const fn read_key_len(src: &[u8]) -> Option<u32> {
    if let Some(chunk) = src.first_chunk::<12>() {
      Some(u32::from_le_bytes([
        chunk[8], chunk[9], chunk[10], chunk[11],
      ]))
    } else {
      None
    }
  }

  /// 快速只读探针：提取值长
  #[inline(always)]
  pub const fn read_val_len(src: &[u8]) -> Option<u32> {
    if let Some(chunk) = src.first_chunk::<HEADER_SIZE>() {
      Some(u32::from_le_bytes([
        chunk[12], chunk[13], chunk[14], chunk[15],
      ]))
    } else {
      None
    }
  }
}
