//! 48 位逻辑日志地址原语与常量定义（严格对齐 C# Garnet / Tsavorite LogAddress 规范）

use core::{fmt, ops::Deref};

/// 地址位长度（48位，支持最大 256TB 寻址空间）
pub const ADDRESS_BITS: u32 = 48;

/// 48 位物理/逻辑地址掩码（低 48 位全部置 1: 0x0000_FFFF_FFFF_FFFF）
pub const ADDRESS_MASK: u64 = (1u64 << ADDRESS_BITS) - 1;

/// ReadCache 逻辑地址指示位掩码（第 47 位，严格对齐 C# Garnet LogAddress.kIsReadCacheBitMask）
pub const READ_CACHE_BIT: u64 = 1u64 << (ADDRESS_BITS - 1);

/// 47 位绝对物理地址掩码（低 48 位中去除最高位 ReadCache 标记位）
pub const ABSOLUTE_ADDRESS_MASK: u64 = ADDRESS_MASK & !READ_CACHE_BIT;

/// 无效逻辑地址常数（0，严格对齐 C# Tsavorite LogAddress.kInvalidAddress）
pub const INVALID_ADDRESS: u64 = 0;

/// 第一个合法逻辑地址（保留前 64 字节规避空指针与极小值混淆）
pub const FIRST_VALID_ADDRESS: u64 = 64;

/// 截断为低 48 位干净逻辑地址
#[inline(always)]
pub const fn clean_address(addr: u64) -> u64 {
  addr & ADDRESS_MASK
}

/// 判定地址是否有效（非 0）
#[inline(always)]
pub const fn is_valid(addr: u64) -> bool {
  clean_address(addr) != INVALID_ADDRESS
}

/// 判定地址是否属于 ReadCache 独立内存缓存
#[inline(always)]
pub const fn is_read_cache(addr: u64) -> bool {
  (addr & READ_CACHE_BIT) != 0
}

/// 转换为纯净的 47 位物理/绝对逻辑地址（去除 ReadCache 虚拟指示位）
#[inline(always)]
pub const fn to_absolute(addr: u64) -> u64 {
  addr & ABSOLUTE_ADDRESS_MASK
}

/// 附加 ReadCache 标志位
#[inline(always)]
pub const fn with_read_cache(addr: u64) -> u64 {
  (addr & ADDRESS_MASK) | READ_CACHE_BIT
}

/// 48 位紧凑日志地址类型包裹
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct LogAddress(pub u64);

impl LogAddress {
  pub const INVALID: Self = Self(INVALID_ADDRESS);

  #[inline(always)]
  pub const fn new(addr: u64) -> Self {
    Self(clean_address(addr))
  }

  #[inline(always)]
  pub const fn from_raw(raw: u64) -> Self {
    Self(raw)
  }

  #[inline(always)]
  pub const fn as_raw(&self) -> u64 {
    self.0
  }

  #[inline(always)]
  pub const fn as_u64(&self) -> u64 {
    self.0
  }

  #[inline(always)]
  pub const fn into_raw(self) -> u64 {
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

impl fmt::Display for LogAddress {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    if self.is_read_cache() {
      write!(f, "RC:{:#x}", self.absolute().0)
    } else {
      write!(f, "{:#x}", self.0)
    }
  }
}
