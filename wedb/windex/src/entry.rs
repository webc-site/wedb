use std::fmt;

use wbase::addr;

/// 哈希桶条目（64位紧凑无锁结构）
///
/// 内存排布（小端 64 位整型）：
/// - `[0..48)`：48 位逻辑地址（address，最大寻址 256TB）
/// - `[48..63)`：15 位哈希指纹（tag，用于常数级快速过滤）
/// - `[63..64)`：1 位试探性标记（tentative，两阶段并发插入保护）
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct HashBucketEntry(pub u64);

impl HashBucketEntry {
  /// 地址位长度（48位，支持最大 256TB 寻址空间）
  pub const ADDRESS_BITS: u32 = addr::ADDRESS_BITS;
  /// 地址掩码（低 48 位全部置 1: 0x0000_FFFF_FFFF_FFFF）
  pub const ADDRESS_MASK: u64 = addr::ADDRESS_MASK;

  /// 指纹位长度（15位）
  pub const TAG_BITS: u32 = 15;
  /// 指纹偏移量（48位）
  pub const TAG_SHIFT: u32 = Self::ADDRESS_BITS;
  /// 指纹有效值掩码（0x7FFF）
  pub const TAG_MASK: u64 = (1u64 << Self::TAG_BITS) - 1;
  /// 指纹在 64 位整型中的位置掩码（0x7FFF_0000_0000_0000）
  pub const TAG_POS_MASK: u64 = Self::TAG_MASK << Self::TAG_SHIFT;

  /// 试探性标记位偏移量（第 63 位）
  pub const TENTATIVE_SHIFT: u32 = 63;
  /// 试探性标记位掩码（0x8000_0000_0000_0000）
  pub const TENTATIVE_MASK: u64 = 1u64 << Self::TENTATIVE_SHIFT;

  /// 哈希计算提取 Tag 的右移位数（64 - 15 = 49）
  pub const HASH_TAG_SHIFT: u32 = 64 - Self::TAG_BITS;

  /// 无效逻辑地址常数（0L，对齐 C# Tsavorite LogAddress.kInvalidAddress）
  pub const INVALID_ADDRESS: u64 = addr::INVALID_ADDRESS;

  /// 构造新的条目
  #[inline]
  pub const fn new(address: u64, tag: u16, tentative: bool) -> Self {
    let word = (address & Self::ADDRESS_MASK)
      | (((tag as u64) & Self::TAG_MASK) << Self::TAG_SHIFT)
      | if tentative { Self::TENTATIVE_MASK } else { 0 };
    Self(word)
  }

  /// 从原生 u64 整型恢复条目
  #[inline]
  pub const fn from_raw(raw: u64) -> Self {
    Self(raw)
  }

  /// 获取底层的 u64 原生数值
  #[inline]
  pub const fn as_raw(&self) -> u64 {
    self.0
  }

  /// 解码获取 48 位逻辑地址
  #[inline]
  pub const fn address(&self) -> u64 {
    self.0 & Self::ADDRESS_MASK
  }

  /// 解码获取 15 位哈希指纹 Tag
  #[inline]
  pub const fn tag(&self) -> u16 {
    ((self.0 & Self::TAG_POS_MASK) >> Self::TAG_SHIFT) as u16
  }

  /// 判定是否处于试探性插入状态（tentative）
  #[inline]
  pub const fn is_tentative(&self) -> bool {
    (self.0 & Self::TENTATIVE_MASK) != 0
  }

  /// 判定条目是否为空（未初始化或已置零释放）
  #[inline]
  pub const fn is_empty(&self) -> bool {
    self.0 == 0
  }

  /// 判定条目是否有效（非空且已提交非试探态）
  #[inline]
  pub const fn is_valid(&self) -> bool {
    self.0 != 0 && !self.is_tentative()
  }

  /// ReadCache 逻辑地址指示位掩码（第 47 位，严格对齐 libs/storage/Tsavorite/cs/src/core/Index/Common/LogAddress.cs:kIsReadCacheBitMask）
  pub const READ_CACHE_BIT: u64 = addr::READ_CACHE_BIT;

  /// 47 位绝对物理地址掩码（低 48 位中去除最高位 ReadCache 标记位）
  pub const ABSOLUTE_ADDRESS_MASK: u64 = addr::ABSOLUTE_ADDRESS_MASK;

  /// 判定条目是否属于 ReadCache 独立只读内存缓存（严格对齐 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucketEntry.cs:IsReadCache）
  #[inline]
  pub const fn is_read_cache(&self) -> bool {
    (self.0 & Self::READ_CACHE_BIT) != 0
  }

  /// 解码获取纯净的 47 位物理/绝对逻辑地址（去除 ReadCache 虚拟指示位）
  #[inline]
  pub const fn absolute_address(&self) -> u64 {
    self.0 & Self::ABSOLUTE_ADDRESS_MASK
  }

  /// 判定条目是否匹配指定指纹且为已提交的非零有效地址（单次位运算比较）
  ///
  /// 利用 64 位内存排布特性：高 16 位为 `[tentative(1位), tag(15位)]`。
  /// 当条目已正式提交（tentative == 0）且 tag 匹配时，`raw >> 48` 严格等于 `tag as u64`，
  /// 配合低 48 位地址非零判定，单次位运算与掩码即可完成原先三次条件判断。
  #[inline]
  pub const fn matches_tag(&self, tag: u16) -> bool {
    (self.0 >> Self::TAG_SHIFT) == ((tag as u64) & Self::TAG_MASK)
      && (self.0 & Self::ADDRESS_MASK) != 0
  }

  /// 替换试探状态并返回新条目
  #[inline]
  #[must_use]
  pub const fn with_tentative(self, tentative: bool) -> Self {
    let word = if tentative {
      self.0 | Self::TENTATIVE_MASK
    } else {
      self.0 & !Self::TENTATIVE_MASK
    };
    Self(word)
  }

  /// 替换指纹 Tag 并返回新条目
  #[inline]
  #[must_use]
  pub const fn with_tag(self, tag: u16) -> Self {
    let word =
      (self.0 & !Self::TAG_POS_MASK) | (((tag as u64) & Self::TAG_MASK) << Self::TAG_SHIFT);
    Self(word)
  }

  /// 从 64 位哈希值的高位提取 15 位 Tag 指纹
  #[inline]
  pub const fn tag_from_hash(hash: u64) -> u16 {
    ((hash >> Self::HASH_TAG_SHIFT) & Self::TAG_MASK) as u16
  }
}

impl fmt::Debug for HashBucketEntry {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("HashBucketEntry")
      .field("address", &format_args!("{:#x}", self.address()))
      .field("tag", &self.tag())
      .field("tentative", &self.is_tentative())
      .field("raw", &format_args!("{:#018x}", self.0))
      .finish()
  }
}

impl fmt::Display for HashBucketEntry {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "Entry(addr: {:#x}, tag: {}, tentative: {})",
      self.address(),
      self.tag(),
      self.is_tentative()
    )
  }
}
