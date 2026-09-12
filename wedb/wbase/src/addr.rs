//! 48 位逻辑日志地址原语与常量定义
//!
//! 对照 C# Tsavorite `LogAddress.cs`：
//! - 常量与位运算语义逐项对齐（kAddressBits / kAddressBitMask / kAbsoluteAddressBitMask /
//!   kInvalidAddress / kTempInvalidAddress；MaxValidAddress 即 [`ABSOLUTE_ADDRESS_MASK]，
//!   FirstValidAddress = PageHeader.Size 依赖页头布局，归页层 crate 落地时定义）；
//! - ReadCache 指示位为 48 位地址空间内的最高位（第 47 位），对齐 `RecordInfo.kIsReadCacheBitMask`；
//! - 差异：C# `AddressString` 十进制 pretty-print 在此由 `LogAddress` 的 `Display` 承担，格式一致。

use core::{fmt, ops::Deref};

/// 地址位长度（48 位，支持最大 256TB 寻址空间）
///
/// 对齐 C# `LogAddress.kAddressBits`
pub const ADDRESS_BITS: u32 = 48;

/// 48 位物理/逻辑地址掩码（低 48 位全部置 1: 0x0000_FFFF_FFFF_FFFF）
///
/// 对齐 C# `LogAddress.kAddressBitMask`
pub const ADDRESS_MASK: u64 = (1u64 << ADDRESS_BITS) - 1;

/// ReadCache 逻辑地址指示位掩码（第 47 位）
///
/// 对齐 C# `RecordInfo.kIsReadCacheBitMask`（kIsReadCacheBitOffset = kAddressBits - 1）
pub const READ_CACHE_BIT: u64 = 1u64 << (ADDRESS_BITS - 1);

/// 47 位绝对物理地址掩码（低 48 位中去除最高位 ReadCache 标记位）
///
/// 对齐 C# `LogAddress.kAbsoluteAddressBitMask`
pub const ABSOLUTE_ADDRESS_MASK: u64 = ADDRESS_MASK & !READ_CACHE_BIT;

/// 无效逻辑地址常数（0）
///
/// 对齐 C# `LogAddress.kInvalidAddress`；零值隐含 `RecordInfo.IsNull` 时记录无效
pub const INVALID_ADDRESS: u64 = 0;

/// 特定初始化场景使用的临时无效逻辑地址（1）
///
/// 对齐 C# `LogAddress.kTempInvalidAddress`
pub const TEMP_INVALID_ADDRESS: u64 = 1;

/// 截断为低 48 位干净逻辑地址（对应 C# 以 kAddressBitMask 掩码取址）
#[inline(always)]
pub const fn clean_address(addr: u64) -> u64 {
  addr & ADDRESS_MASK
}

/// 判定地址是否有效（非 0；C# 无对应函数，为 Rust 侧补充的便捷判定）
#[inline(always)]
pub const fn is_valid(addr: u64) -> bool {
  clean_address(addr) != INVALID_ADDRESS
}

/// 判定地址是否属于 ReadCache 独立内存缓存
///
/// 对齐 C# `LogAddress.IsReadCache`
#[inline(always)]
pub const fn is_read_cache(addr: u64) -> bool {
  (addr & READ_CACHE_BIT) == READ_CACHE_BIT
}

/// 转换为纯净的 47 位物理/绝对逻辑地址（去除 ReadCache 虚拟指示位）
///
/// 对齐 C# `LogAddress.AbsoluteAddress`
#[inline(always)]
pub const fn to_absolute(addr: u64) -> u64 {
  addr & ABSOLUTE_ADDRESS_MASK
}

/// 附加 ReadCache 标志位（C# 由 RecordInfo 设置，此处提供地址侧等价操作）
#[inline(always)]
pub const fn with_read_cache(addr: u64) -> u64 {
  (addr & ADDRESS_MASK) | READ_CACHE_BIT
}

/// 计算逻辑地址所在页号（AllocatorBase / ScanIteratorBase 共用工具）
///
/// 对齐 C# `LogAddress.GetPageOfAddress`：先取绝对地址再右移页位宽
#[inline(always)]
pub const fn page_of_address(addr: u64, page_bits: u32) -> u64 {
  to_absolute(addr) >> page_bits
}

/// 计算页起始逻辑地址（AllocatorBase / ScanIteratorBase 共用工具）
///
/// 对齐 C# `LogAddress.GetLogicalAddressOfStartOfPage`：页号左移页位宽
#[inline(always)]
pub const fn address_of_page_start(page: u64, page_bits: u32) -> u64 {
  page << page_bits
}

/// 48 位紧凑日志地址类型包裹
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct LogAddress(pub u64);

impl LogAddress {
  /// 掩码构造（截断高 16 位脏位，保留 ReadCache 标志位）
  #[inline(always)]
  pub const fn new(addr: u64) -> Self {
    Self(clean_address(addr))
  }

  /// 原样构造（不做掩码清洗，信任调用方）
  #[inline(always)]
  pub const fn from_raw(raw: u64) -> Self {
    Self(raw)
  }

  /// 读原始 64 位值
  #[inline(always)]
  pub const fn as_raw(&self) -> u64 {
    self.0
  }

  #[inline(always)]
  pub const fn is_valid(&self) -> bool {
    is_valid(self.0)
  }

  #[inline(always)]
  pub const fn is_read_cache(&self) -> bool {
    is_read_cache(self.0)
  }

  #[inline(always)]
  pub const fn absolute(&self) -> Self {
    Self(to_absolute(self.0))
  }

  #[inline(always)]
  pub const fn with_read_cache(&self) -> Self {
    Self(with_read_cache(self.0))
  }

  /// 计算所在页号（对齐 C# `LogAddress.GetPageOfAddress`）
  #[inline(always)]
  pub const fn page(&self, page_bits: u32) -> u64 {
    page_of_address(self.0, page_bits)
  }

  /// 取页起始地址（对齐 C# `LogAddress.GetLogicalAddressOfStartOfPage`）
  #[inline(always)]
  pub const fn page_start(page: u64, page_bits: u32) -> Self {
    Self(address_of_page_start(page, page_bits))
  }
}

impl Deref for LogAddress {
  type Target = u64;

  #[inline(always)]
  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl From<u64> for LogAddress {
  #[inline(always)]
  fn from(addr: u64) -> Self {
    Self::new(addr)
  }
}

impl From<LogAddress> for u64 {
  #[inline(always)]
  fn from(addr: LogAddress) -> Self {
    addr.0
  }
}

impl fmt::Debug for LogAddress {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("LogAddress")
      .field("addr", &format_args!("{:#x}", self.0))
      .field("read_cache", &self.is_read_cache())
      .field("valid", &self.is_valid())
      .finish()
  }
}

/// 对齐 C# `LogAddress.AddressString`：`rc:N` / `kInvalid` / `kTempInvalid` / `log:N`（十进制）
impl fmt::Display for LogAddress {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    if self.is_read_cache() {
      write!(f, "rc:{}", self.absolute().0)
    } else if self.0 == INVALID_ADDRESS {
      f.write_str("kInvalid")
    } else if self.0 == TEMP_INVALID_ADDRESS {
      f.write_str("kTempInvalid")
    } else {
      write!(f, "log:{}", self.0)
    }
  }
}
