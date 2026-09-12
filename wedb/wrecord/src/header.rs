use core::{
  fmt,
  hint::spin_loop,
  mem::{align_of, size_of},
  sync::atomic::{AtomicU64, Ordering},
};
use std::thread::yield_now;

use wbase::addr::TEMP_INVALID_ADDRESS;

use crate::{
  codec::checked_record_size,
  error::{Error, Result},
};

/// 记录头字节大小（16 字节）
///
/// 对标 C# Tsavorite `Constants.FixedHeaderSize = RecordInfo.Size + RecordDataHeader.Size = 8 + 8`：
/// Rust 将 C# 的 RecordInfo（8B 复合状态字）与 RecordDataHeader（8B 长度原子字）合并为
/// 单一 16 字节定长头，布局为 `[info_word: u64][rdh_word: u64]`（小端）。
///
/// **第一字（RecordInfo 字，offset 0）**，对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs：
/// - bits 0..47 地址、bits 48..58 保留（原松弛填充位段已回收，恒零）、
///   bits 59..63 为 Modified/Sealed/InNewVersion/ReadCache/Tombstone 五个标志位；
/// - 无 Valid 位：C# 的 Valid/Sealed 并发状态机由上层（whlog/wreviv）以原子 CAS 承担，
///   纯格式层不感知；
///
/// **第二字（RecordDataHeader 原子字，offset 8）**，对标
/// libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs（位段相对次序 filler→key→val
/// 与 C# 一致，位宽因无 overflow key/value 机制而放宽）：
/// - bits 0..7 `FillerWords`（8 位，每词 8 字节松弛填充，对标 C# kFillerWordsShift/kFillerWordsBits）；
/// - bits 8..31 `KeyLength`（24 位，对标 C# kKeyLengthBits=10——C# 超限键走 OverflowByteArray，
///   Rust 全内联故扩展位宽）；
/// - bits 32..63 `ValueLength`（32 位，对标 C# kValueLengthBits=24——C# 超限值走 overflow，
///   Rust 全内联故扩展位宽）；
/// - C# RDH byte 6 RecordType 判别字节与 byte 7 Namespace 字节不在本层承载：记录类型判别由
///   上层 wval 以键前缀 KeyTag（enum u8）与 `MetaValue.collection_type`（值载荷）承载，
///   命名空间由 wval 会话前缀（ns+db varint）编入物理键——与 C#「RecordInfo/RDH 属 Tsavorite
///   核心、RecordType 语义由 Garnet 调用方解释」的分层约束等价；
///
/// **单字原子发布协议（1:1 对标 RecordDataHeader.cs）**：key_len + val_len + filler 全部收进
/// 第二字，`recordLength`（[RecordHeader::record_size] + filler）由该字单独推导——原位更新以
/// 单次 8 字节对齐原子写（[crate::RecordMut] 的 RDH 单 store）发布完整一致的新记录布局，
/// 并发读者（原子 Acquire 载入，见 [RecordHeader::from_ptr_atomic]）只会观察到前态或后态，
/// 绝无「新 val_len + 旧 filler」混合态。这依赖记录 8 字节对齐不变式
/// [RECORD_ALIGNMENT]（对标 C# `Constants.kRecordAlignment = 8`，隐式对齐填充由
/// [RecordHeader::record_size] 吸收、显式松弛由 FillerWords 承载）。
///
/// - 注意 [HEADER_READ_CACHE_BIT] 为本 RecordInfo 字内的 bit 62，与 wbase::addr::HEADER_READ_CACHE_BIT
///   （哈希指针地址空间内的 bit 47）同名不同值，分属两个不同的 64 位字。
pub const HEADER_SIZE: usize = 16;

/// 记录对齐字节数（对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Constants.cs:kRecordAlignment = 8）
///
/// 所有记录的逻辑尺寸（头 + 键 + 值）向上对齐到 8 字节边界，差值吸收为隐式填充：
/// - 保证页内每条记录的 16 字节头恒 8 字节对齐，第一字/第二字均可单次原子读写
///   （未对齐的 u64 原子访问跨缓存行时不具备原子性，属未定义行为）；
/// - 对齐差值天然为 8 的整数倍，显式松弛填充（FillerWords）只需词粒度。
pub const RECORD_ALIGNMENT: usize = 8;

/// 换页填充标记中的特殊魔数（key_len 为 24 位全 1 表示 Pad 填充）
pub const PAD_KEY_LEN: u32 = (1 << 24) - 1;

/// 48 位逻辑地址掩码与位宽（统一由 wbase 提供）
pub use wbase::addr::{ADDRESS_BITS, ADDRESS_MASK};

/// RecordInfo 字保留位段掩码（bits 48..58，原松弛填充位段回收，恒零）
const RESERVED_INFO_MASK: u64 = 0x7FFu64 << ADDRESS_BITS;

/// RecordInfo 字标志位统一掩码（bits 59..63）
const INFO_FLAG_MASK: u64 =
  MODIFIED_BIT | SEALED_BIT | IN_NEW_VERSION_BIT | HEADER_READ_CACHE_BIT | TOMBSTONE_BIT;

/// 8 位填充词位段（RDH bits 0..7，每词 8 字节，最多 255 * 8 = 2040 字节松弛空间，
/// 对标 libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs:FillerWords）
pub(crate) const FILLER_WORDS_SHIFT: u32 = 0;
pub(crate) const FILLER_WORDS_BITS: u32 = 8;
pub(crate) const FILLER_WORDS_VALUE_MASK: u64 = (1u64 << FILLER_WORDS_BITS) - 1;
pub(crate) const FILLER_WORDS_MASK: u64 = FILLER_WORDS_VALUE_MASK << FILLER_WORDS_SHIFT;

/// 24 位键长度位段（RDH bits 8..31，对标 libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs:KeyLength）
const KEY_LEN_SHIFT: u32 = FILLER_WORDS_SHIFT + FILLER_WORDS_BITS;
pub(crate) const KEY_LEN_BITS: u32 = 24;
const KEY_LEN_VALUE_MASK: u64 = (1u64 << KEY_LEN_BITS) - 1;
const KEY_LEN_MASK: u64 = KEY_LEN_VALUE_MASK << KEY_LEN_SHIFT;

/// 32 位值长度位段（RDH bits 32..63，对标 libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs:ValueLength）
const VAL_LEN_SHIFT: u32 = KEY_LEN_SHIFT + KEY_LEN_BITS;
const VAL_LEN_BITS: u32 = 32;
const VAL_LEN_VALUE_MASK: u64 = (1u64 << VAL_LEN_BITS) - 1;
const VAL_LEN_MASK: u64 = VAL_LEN_VALUE_MASK << VAL_LEN_SHIFT;

/// 最大可表示的单记录显式松弛填充字节数（255 词 * 8 字节 = 2040 字节，
/// 对标 libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs:MaxFillerWords * kRecordAlignment）
pub const MAX_FILLER_BYTES: usize = (FILLER_WORDS_VALUE_MASK as usize) << 3;

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

// 编译期静态断言 1：RecordInfo 字位域封闭性 —— 48 位地址 + 11 位保留 + 5 个标志位
// 恰好无缝铺满 64 位字，任何位既不重叠也不遗漏，宽度分配在编译期即可证明正确。
const _: () = assert!((ADDRESS_MASK | RESERVED_INFO_MASK | INFO_FLAG_MASK) == u64::MAX);

// 编译期静态断言 2：RDH 原子字位域封闭性 —— 8 位填充词 + 24 位键长 + 32 位值长
// 恰好铺满 64 位字（对标 RecordDataHeader 单字容纳完整记录布局的推导能力）。
const _: () = assert!((FILLER_WORDS_MASK | KEY_LEN_MASK | VAL_LEN_MASK) == u64::MAX);

// 编译期静态断言 3：内存布局与磁盘序列化布局严格一致（16 字节、8 字节对齐，
// 对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Constants.cs:FixedHeaderSize = RecordInfo.Size + RecordDataHeader.Size 与 kRecordAlignment）
const _: () = assert!(size_of::<RecordHeader>() == HEADER_SIZE);
const _: () = assert!(align_of::<RecordHeader>() == RECORD_ALIGNMENT);

/// 向上对齐到 [RECORD_ALIGNMENT] 边界（对标 C# Utility.RoundUp(sum, kRecordAlignment)）
#[inline(always)]
pub(crate) const fn align_record_size(size: usize) -> usize {
  (size + (RECORD_ALIGNMENT - 1)) & !(RECORD_ALIGNMENT - 1)
}

/// 16 字节紧凑记录头结构体（C 对齐，双 8 字节原子字）
///
/// 内存排布（小端 16 字节）：
/// - `[0..8)`: RecordInfo 字（低 48 位为前驱版本逻辑地址形成反向链表，bits 48..58 保留恒零，
///   bit 59 为 MODIFIED 修改位，bit 60 为 SEALED 密封位，bit 61 为 IN_NEW_VERSION 纪元位，
///   bit 62 为 READ_CACHE 读缓存位，最高位 1<<63 为 TOMBSTONE 墓碑标记）
/// - `[8..16)`: RecordDataHeader 原子字（bits 0..7 为 FillerWords 松弛填充词，bits 8..31 为
///   key_len 键长度，bits 32..63 为 val_len 值长度；单次原子写发布完整一致的记录布局）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(C)]
pub struct RecordHeader {
  /// RecordInfo 复合字：前驱版本逻辑地址 + 标志位
  pub prev_address: u64,
  /// RecordDataHeader 原子字：filler | key_len | val_len 位段
  ///
  /// 对标 libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs:word——所有访问
  /// 必须经由本字的位段访问器，保证单次 8 字节原子读写发布完整记录布局。
  pub rdh_word: u64,
}

/// 按开关置位/清零复合字中的指定位段（const fn，供各标志位 setter 共享）
#[inline(always)]
const fn with_bit(word: u64, mask: u64, on: bool) -> u64 {
  if on { word | mask } else { word & !mask }
}

impl RecordHeader {
  /// 由两个 8 字节原始字直接构造记录头（const fn）
  #[inline(always)]
  pub const fn from_words(prev_address: u64, rdh_word: u64) -> Self {
    Self {
      prev_address,
      rdh_word,
    }
  }

  /// 构造新的记录头并校验 48 位地址有效性（const fn）
  #[inline]
  pub const fn new(prev_addr: u64, key_len: u32, val_len: u32, is_tombstone: bool) -> Result<Self> {
    if prev_addr & !ADDRESS_MASK != 0 {
      return Err(Error::AddressOverflow(prev_addr));
    }
    Ok(Self::from_words(
      with_bit(prev_addr, TOMBSTONE_BIT, is_tombstone),
      pack_rdh_word(0, key_len, val_len),
    ))
  }

  /// 直接从原始数据快速构造（无地址检查，适用于内部或高性能路径）
  #[inline]
  pub const fn from_raw(prev_address: u64, key_len: u32, val_len: u32) -> Self {
    Self::from_words(prev_address, pack_rdh_word(0, key_len, val_len))
  }

  /// 初始有效记录头常量（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:InitialValid）
  pub const INITIAL_VALID: Self = Self {
    prev_address: TEMP_INVALID_ADDRESS,
    rdh_word: 0,
  };

  /// 构造初始有效记录头（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:InitialValid）
  #[inline(always)]
  pub const fn initial_valid() -> Self {
    Self::INITIAL_VALID
  }

  /// 提取 48 位前驱版本逻辑地址
  #[inline]
  pub const fn address(&self) -> u64 {
    self.prev_address & ADDRESS_MASK
  }

  /// 提取 48 位前驱版本逻辑地址（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:PreviousAddress getter）
  #[inline(always)]
  pub const fn previous_address(&self) -> u64 {
    self.address()
  }

  /// 设置 48 位前驱版本逻辑地址
  #[inline]
  pub const fn set_previous_address(&mut self, prev_addr: u64) -> Result<()> {
    self.set_address(prev_addr)
  }

  /// 记录是否有效（非密封且非失效状态，对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:Valid）
  #[inline(always)]
  pub const fn is_valid(&self) -> bool {
    !self.is_closed()
  }

  /// 是否处于失效状态（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:Invalid）
  #[inline(always)]
  pub const fn is_invalid(&self) -> bool {
    self.is_closed()
  }

  /// 扫描跳过判定（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:SkipOnScan）
  #[inline(always)]
  pub const fn skip_on_scan(&self) -> bool {
    self.is_closed()
  }

  /// 是否带有墓碑删除标记
  #[inline(always)]
  pub const fn is_tombstone(&self) -> bool {
    (self.prev_address & TOMBSTONE_BIT) != 0
  }

  /// 设置或清除墓碑标记（const fn）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:SetTombstone
  #[inline(always)]
  pub const fn set_tombstone(&mut self, tombstone: bool) {
    self.prev_address = with_bit(self.prev_address, TOMBSTONE_BIT, tombstone);
  }

  /// 清除墓碑标记（const fn）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:ClearTombstone
  #[inline(always)]
  pub const fn clear_tombstone(&mut self) {
    self.set_tombstone(false);
  }

  /// 是否带有修改标记（对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:Modified）
  #[inline(always)]
  pub const fn is_modified(&self) -> bool {
    (self.prev_address & MODIFIED_BIT) != 0
  }

  /// 设置或清除修改标记（const fn）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:SetModified
  #[inline(always)]
  pub const fn set_modified(&mut self, modified: bool) {
    self.prev_address = with_bit(self.prev_address, MODIFIED_BIT, modified);
  }

  /// 是否带有原位更新标记（C# Tsavorite Status.InPlaceUpdated / RecordInfo.Modified 别名）
  #[inline(always)]
  pub const fn is_in_place_updated(&self) -> bool {
    self.is_modified()
  }

  /// 设置或清除原位更新标记（C# Tsavorite Status.InPlaceUpdated / RecordInfo.SetModified 别名）
  #[inline(always)]
  pub const fn set_in_place_updated(&mut self, in_place_updated: bool) {
    self.set_modified(in_place_updated);
  }

  /// 初始化新记录状态字（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:InitializeForNewRecord）
  ///
  /// 初始置为 SEALED 状态，防止并发读者在未完成写入前观察到。
  #[inline(always)]
  pub const fn initialize_for_new_record() -> Self {
    Self {
      prev_address: SEALED_BIT,
      rdh_word: 0,
    }
  }

  /// 写入新记录元数据（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:WriteInfo）
  #[inline(always)]
  pub const fn write_info(&mut self, in_new_version: bool, previous_address: u64) {
    let mut word = (previous_address & ADDRESS_MASK) | SEALED_BIT;
    if in_new_version {
      word |= IN_NEW_VERSION_BIT;
    }
    self.prev_address = word;
  }

  /// 判定状态字是否处于关闭/密封状态（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:IsClosedWord）
  #[inline(always)]
  pub const fn is_closed_word(word: u64) -> bool {
    (word & SEALED_BIT) != 0
  }

  /// 当前记录是否处于关闭/密封状态（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:IsClosed）
  #[inline(always)]
  pub const fn is_closed(&self) -> bool {
    Self::is_closed_word(self.prev_address)
  }

  /// 是否关闭或带有墓碑（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:IsClosedOrTombstoned）
  #[inline(always)]
  pub const fn is_closed_or_tombstoned(&self) -> bool {
    self.is_closed() || self.is_tombstone()
  }

  /// 解除密封状态并验证（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:UnsealAndValidate）
  #[inline(always)]
  pub const fn unseal_and_validate(&mut self) {
    self.set_sealed(false);
  }

  /// 密封并废弃槽位（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:SealAndInvalidate）
  #[inline(always)]
  pub const fn seal_and_invalidate(&mut self) {
    self.set_sealed(true);
  }

  /// 置为失效状态（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:SetInvalid）
  #[inline(always)]
  pub const fn set_invalid(&mut self) {
    self.set_sealed(true);
  }

  /// 尝试对记录头原子置位 SEALED 密封位（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:TrySeal）
  ///
  /// - `_invalidate`: 对标 C# RecordInfo.cs:TrySeal(bool invalidate) 签名。
  ///   Rust RecordHeader 无独立 Valid 位（由状态字原子 CAS 承担），保留该形参以匹配固定接口规范。
  ///   若记录已封闭（已被密封），返回 false；CAS 成功置位 SEALED_BIT 返回 true。
  #[inline]
  pub fn try_seal(atomic_word: &AtomicU64, _invalidate: bool) -> bool {
    let expected = atomic_word.load(Ordering::Acquire);
    if Self::is_closed_word(expected) {
      return false;
    }
    let new_word = expected | SEALED_BIT;
    atomic_word
      .compare_exchange(expected, new_word, Ordering::AcqRel, Ordering::Acquire)
      .is_ok()
  }

  /// 自旋重试上限常数（对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Constants.cs:kMaxLockSpins，
  /// C# 值 10——放大至 100 以压降高争用下误判锁失败的概率，配合退避路径兜底）
  pub const MAX_LOCK_SPINS: usize = 100;

  /// 尝试原子清除 MODIFIED 修改标记（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:TryResetModifiedAtomic）
  pub fn try_reset_modified_atomic(atomic_word: &AtomicU64) -> bool {
    let mut spin_count = Self::MAX_LOCK_SPINS;
    loop {
      let expected = atomic_word.load(Ordering::Acquire);
      if Self::is_closed_word(expected) {
        return false;
      }
      if (expected & MODIFIED_BIT) == 0 {
        return true;
      }
      if atomic_word
        .compare_exchange_weak(
          expected,
          expected & !MODIFIED_BIT,
          Ordering::AcqRel,
          Ordering::Acquire,
        )
        .is_ok()
      {
        return true;
      }
      if spin_count == 0 {
        return false;
      }
      spin_count -= 1;
      yield_now();
    }
  }

  /// 尝试原子更新前驱版本逻辑地址（保留高位所有元数据，严格对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:TryUpdateAddress）
  pub fn try_update_address(
    atomic_word: &AtomicU64,
    expected_prev_addr: u64,
    new_prev_addr: u64,
  ) -> bool {
    let expected = atomic_word.load(Ordering::Acquire);
    if (expected & ADDRESS_MASK) != (expected_prev_addr & ADDRESS_MASK) {
      return false;
    }
    let new_word = (expected & !ADDRESS_MASK) | (new_prev_addr & ADDRESS_MASK);
    atomic_word
      .compare_exchange(expected, new_word, Ordering::AcqRel, Ordering::Acquire)
      .is_ok()
  }

  /// 原子设置失效状态（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:SetInvalidAtomic）
  pub fn set_invalid_atomic(atomic_word: &AtomicU64) {
    loop {
      let expected = atomic_word.load(Ordering::Acquire);
      let new_word = expected | SEALED_BIT;
      if atomic_word
        .compare_exchange_weak(expected, new_word, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
      {
        return;
      }
      spin_loop();
    }
  }

  /// 是否带有密封标记（对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:IsSealed）
  #[inline(always)]
  pub const fn is_sealed(&self) -> bool {
    (self.prev_address & SEALED_BIT) != 0
  }

  /// 设置或清除密封标记（const fn）
  #[inline(always)]
  pub const fn set_sealed(&mut self, sealed: bool) {
    self.prev_address = with_bit(self.prev_address, SEALED_BIT, sealed);
  }

  /// 密封记录（const fn）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:Seal
  #[inline(always)]
  pub const fn seal(&mut self) {
    self.set_sealed(true);
  }

  /// 清除磁盘映像中的临时易失标志位（如 SEALED 位，严格对标 C# Tsavorite RecordInfo.ClearBitsForDiskImages）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:ClearBitsForDiskImages
  #[inline(always)]
  pub const fn clear_bits_for_disk_images(&mut self) {
    self.set_sealed(false);
  }

  /// 是否属于 Checkpoint 新版本纪元（对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:IsInNewVersion）
  #[inline(always)]
  pub const fn is_in_new_version(&self) -> bool {
    (self.prev_address & IN_NEW_VERSION_BIT) != 0
  }

  /// 设置或清除 Checkpoint 新版本纪元标记（const fn）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:SetIsInNewVersion
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
    ((self.rdh_word & FILLER_WORDS_MASK) >> FILLER_WORDS_SHIFT) as u8
  }

  /// 获取显式松弛填充字节总数（FillerWords * 8，词粒度；
  /// 记录对齐不变式保证隐式对齐填充与显式填充均为 8 字节整数倍）
  #[inline(always)]
  pub const fn filler_bytes(&self) -> usize {
    (self.filler_words() as usize) << 3
  }

  /// 一步设置完整的松弛填充字节数（按 8 字节词折算，超出 [MAX_FILLER_BYTES] 时静默钳位）
  ///
  /// 钳位仅为防御兜底：钳位后 [RecordHeader::physical_size] 将低报真实占用，破坏物理尺寸恒定
  /// 不变量。调用方必须先保证 `total_bytes <= MAX_FILLER_BYTES` 且为 [RECORD_ALIGNMENT] 整数倍
  /// （记录 8 字节对齐不变式下松弛差值恒为词整数倍，在位更新路径由
  /// [RecordHeader::can_update_with_slack] 与 [crate::RecordMut] 前置校验，
  /// 槽位复用路径的富余恒为词整数倍）。C# 同场景（RecordDataHeader.SetFiller）超限时
  /// 走记录分裂，Rust 以「拒绝更新 + Pad 填充头」在上层等价承接。
  #[inline(always)]
  pub const fn set_filler_bytes(&mut self, total_bytes: usize) {
    let words = total_bytes >> 3;
    self.set_filler_words(if words > FILLER_WORDS_VALUE_MASK as usize {
      FILLER_WORDS_VALUE_MASK as u8
    } else {
      words as u8
    });
  }

  /// 设置 8 位松弛填充词数量（保留键长与值长位段）
  #[inline(always)]
  pub const fn set_filler_words(&mut self, words: u8) {
    self.rdh_word = (self.rdh_word & !FILLER_WORDS_MASK)
      | ((words as u64 & FILLER_WORDS_VALUE_MASK) << FILLER_WORDS_SHIFT);
  }

  /// 设置键长度位段（const fn；调用方须保证不超出 24 位上限）
  #[inline(always)]
  pub const fn set_key_len(&mut self, key_len: u32) {
    self.rdh_word =
      (self.rdh_word & !KEY_LEN_MASK) | (((key_len as u64) & KEY_LEN_VALUE_MASK) << KEY_LEN_SHIFT);
  }

  /// 设置值长度位段（const fn）
  #[inline(always)]
  pub const fn set_val_len(&mut self, val_len: u32) {
    self.rdh_word =
      (self.rdh_word & !VAL_LEN_MASK) | (((val_len as u64) & VAL_LEN_VALUE_MASK) << VAL_LEN_SHIFT);
  }

  /// 获取当前记录槽位物理容纳值的最大字节容量
  ///
  /// 对标 C# TrySetPinnedValueSpan 容量口径：`physical_size - HEADER_SIZE - key_len`，
  /// 含可复用的隐式对齐填充（新值对齐后尺寸不超过槽位物理占用即可容纳，
  /// 与 [Self::can_update_with_slack] 精确口径等价）。
  #[inline(always)]
  pub const fn val_capacity(&self) -> usize {
    self
      .physical_size()
      .saturating_sub(HEADER_SIZE + self.key_len() as usize)
  }

  /// 获取键长度
  #[inline]
  pub const fn key_len(&self) -> u32 {
    ((self.rdh_word & KEY_LEN_MASK) >> KEY_LEN_SHIFT) as u32
  }

  /// 获取值长度
  #[inline]
  pub const fn val_len(&self) -> u32 {
    ((self.rdh_word & VAL_LEN_MASK) >> VAL_LEN_SHIFT) as u32
  }

  /// 获取整条记录（头 + 键 + 值 + 隐式对齐填充）的理论逻辑字节长度
  ///
  /// 对标 C# RecordDataHeader.GetAlignedComponentSum：记录长度不再落盘存储，由 RDH 原子字
  /// 单独推导（头 + 键长 + 值长向上对齐到 [RECORD_ALIGNMENT]，差值为隐式填充）。
  #[inline(always)]
  pub const fn record_size(&self) -> usize {
    align_record_size(self.kv_size())
  }

  /// 获取头 + 键 + 值的未对齐区段字节数（值数据结束边界）
  ///
  /// 对标 C# RecordDataHeader.GetUnalignedComponentSum（无可选字段与扩展命名空间的口径）。
  #[inline(always)]
  pub const fn kv_size(&self) -> usize {
    HEADER_SIZE
      .saturating_add(self.key_len() as usize)
      .saturating_add(self.val_len() as usize)
  }

  /// 获取整条记录在物理上占据的总字节大小（对齐逻辑尺寸 + 显式松弛填充）
  ///
  /// 对标 C# RecordDataHeader.GetRecordLength：`alignedSum + (FillerWords << 3)`，
  /// 全部由 RDH 原子字推导。
  #[inline(always)]
  pub const fn physical_size(&self) -> usize {
    self.record_size().saturating_add(self.filler_bytes())
  }

  /// 安全计算头 + 键 + 值未对齐区段字节长度，若计算溢出 usize 则返回 None
  #[inline(always)]
  pub const fn checked_record_size(&self) -> Option<usize> {
    checked_record_size(self.key_len() as usize, self.val_len() as usize)
  }

  /// 安全计算整条记录物理字节大小（对齐逻辑尺寸 + 显式松弛填充），若溢出返回 None
  #[inline(always)]
  pub const fn checked_physical_size(&self) -> Option<usize> {
    self.record_size().checked_add(self.filler_bytes())
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

  /// 翻转墓碑标记位，并返回翻转后的墓碑状态（const fn）
  #[inline(always)]
  pub const fn flip_tombstone(&mut self) -> bool {
    self.prev_address ^= TOMBSTONE_BIT;
    self.is_tombstone()
  }

  /// 判断是否可以在原位等长更新指定长度的值（要求非墓碑且新值长度严格一致，const fn）
  #[inline(always)]
  pub const fn can_update_in_place(&self, new_val_len: usize) -> bool {
    !self.is_tombstone() && self.val_len() as usize == new_val_len
  }

  /// 判断是否可以利用动态松弛原位更新指定长度的值（要求非墓碑且新值长度不超过槽位最大物理容量）
  ///
  /// 精确口径：新逻辑尺寸向上对齐后不得超过槽位物理占用，且腾出的显式松弛
  /// （物理占用 - 新对齐尺寸，恒为 8 字节整数倍）不超过 [MAX_FILLER_BYTES]。
  #[inline(always)]
  pub const fn can_update_with_slack(&self, new_val_len: usize) -> bool {
    let physical = self.physical_size();
    let new_aligned = align_record_size(
      HEADER_SIZE
        .saturating_add(self.key_len() as usize)
        .saturating_add(new_val_len),
    );
    !self.is_tombstone() && new_aligned <= physical && (physical - new_aligned) <= MAX_FILLER_BYTES
  }

  /// 编码为 16 字节定长数组（小端编码，const fn，双 64 位整型融合打包）
  #[inline(always)]
  pub const fn to_bytes(&self) -> [u8; HEADER_SIZE] {
    let p = self.prev_address.to_le_bytes();
    let r = self.rdh_word.to_le_bytes();
    [
      p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], r[0], r[1], r[2], r[3], r[4], r[5], r[6],
      r[7],
    ]
  }

  /// 从 16 字节定长数组直接解码记录头（const fn，双 64 位整型快速解包）
  #[inline(always)]
  pub const fn from_bytes(bytes: [u8; HEADER_SIZE]) -> Self {
    let prev_address = u64::from_le_bytes([
      bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]);
    let rdh_word = u64::from_le_bytes([
      bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    ]);
    Self::from_words(prev_address, rdh_word)
  }

  /// 原子解析 16 字节记录头（两个 8 字节对齐字 Acquire 载入，供无锁直读路径）
  ///
  /// 对标 C# RecordDataHeader 单 8 字节原子字读协议：原位更新者以单次原子写发布完整
  /// 记录布局（filler + key_len + val_len 同字），并发读者经本方法 Acquire 载入只会
  /// 观察到前态或后态，绝无半字混合态；RecordInfo 字的标志位（墓碑/密封）同样以
  /// 原子写维护，Acquire 载入与之配对。
  ///
  /// # Safety
  /// `ptr` 必须 8 字节对齐且指向完整 16 字节可读记录头——由记录 [RECORD_ALIGNMENT]
  /// 对齐不变式保证（页内记录字节紧排但逐条 8 字节对齐）。
  #[inline]
  pub unsafe fn from_ptr_atomic(ptr: *const u8) -> Self {
    debug_assert_eq!(
      ptr as usize % RECORD_ALIGNMENT,
      0,
      "记录头必须 8 字节对齐（RECORD_ALIGNMENT 不变式）"
    );
    // SAFETY: 调用方契约保证 ptr 8 字节对齐且 [ptr, ptr + 16) 可读已初始化；
    // AtomicU64 与 u64 布局一致，对齐字载入在所有支持平台均为原子指令
    let info = unsafe { &*(ptr as *const AtomicU64) }.load(Ordering::Acquire);
    // SAFETY: ptr.add(8) 同样 8 字节对齐且可读
    let rdh = unsafe { &*(ptr.add(8) as *const AtomicU64) }.load(Ordering::Acquire);
    Self::from_words(info, rdh)
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
  /// Rust 将 C# 的 RecordInfo（8B）与 RDH（8B）合并为 16 字节头，故空记录判定覆盖
  /// 前驱地址与 RDH 原子字同时为零（前驱地址为 0 但键值非零属合法创世记录，不算空头）。
  /// 扫描器遇到空头应按最小 16 字节记录推进，严格对标 C# 零 RDH 守卫语义。
  #[inline(always)]
  pub const fn is_null(&self) -> bool {
    self.prev_address == 0 && (self.rdh_word & !FILLER_WORDS_MASK) == 0
  }

  /// 构造换页填充 Pad 头（key_len 设为 [PAD_KEY_LEN]，val_len 设为剩余容纳字节数）
  ///
  /// 仅当 `remaining_bytes >= HEADER_SIZE` 时产物有意义（扫描器按 `HEADER_SIZE + val_len`
  /// 精确越过填充区）；`remaining_bytes < HEADER_SIZE` 的分支仅为避免 const fn 整数下溢的
  /// 防御值，调用方（槽位复用路径）须吸纳为松弛填充而非落 Pad 头。
  #[inline(always)]
  pub const fn pad(remaining_bytes: usize) -> Self {
    let val_len = if remaining_bytes >= HEADER_SIZE {
      (remaining_bytes - HEADER_SIZE) as u32
    } else {
      0
    };
    Self::from_words(0, pack_rdh_word(0, PAD_KEY_LEN, val_len))
  }

  /// 是否为换页填充 Pad 记录头
  #[inline(always)]
  pub const fn is_pad(&self) -> bool {
    self.key_len() == PAD_KEY_LEN
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
    if let Some(key_len) = Self::read_key_len(src) {
      Some(key_len == PAD_KEY_LEN)
    } else {
      None
    }
  }

  /// 快速只读探针：提取键长（RDH 原子字 bits 8..31，跨字节 9..12）
  #[inline(always)]
  pub const fn read_key_len(src: &[u8]) -> Option<u32> {
    if let Some(chunk) = src.first_chunk::<12>() {
      let rdh_low = u32::from_le_bytes([chunk[8], chunk[9], chunk[10], chunk[11]]);
      Some(rdh_low >> KEY_LEN_SHIFT)
    } else {
      None
    }
  }

  /// 快速只读探针：提取值长（RDH 原子字高 32 位，字节 12..16）
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

/// RDH 原子字三段位段融合打包（const fn：filler | key_len | val_len）
#[inline(always)]
pub(crate) const fn pack_rdh_word(filler_words: u8, key_len: u32, val_len: u32) -> u64 {
  ((filler_words as u64) & FILLER_WORDS_VALUE_MASK)
    | (((key_len as u64) & KEY_LEN_VALUE_MASK) << KEY_LEN_SHIFT)
    | (((val_len as u64) & VAL_LEN_VALUE_MASK) << VAL_LEN_SHIFT)
}

/// libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:ToString
impl fmt::Display for RecordHeader {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "RecordHeader(prev={:#x}, key_len={}, val_len={}, tomb={}, sealed={}, mod={}, new_ver={}, rc={}, filler={})",
      self.address(),
      self.key_len(),
      self.val_len(),
      self.is_tombstone(),
      self.is_sealed(),
      self.is_modified(),
      self.is_in_new_version(),
      self.is_read_cache(),
      self.filler_bytes()
    )
  }
}
