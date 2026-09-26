//! 48 位逻辑日志地址原语与常量定义
//!
//! 对照 C# Tsavorite `LogAddress.cs`：
//! - 常量与位运算语义逐项对齐（kAddressBits / kAddressBitMask / kAbsoluteAddressBitMask /
//!   kInvalidAddress / kTempInvalidAddress；MaxValidAddress 即 [`ABSOLUTE_ADDRESS_MASK]，
//!   FirstValidAddress = PageHeader.Size 依赖页头布局，归页层 crate 落地时定义）；
//! - ReadCache 指示位为 48 位地址空间内的最高位（第 47 位），对齐 `RecordInfo.kIsReadCacheBitMask`；
//! - 差异：C# `AddressString` 十进制 pretty-print 与实例包装类型无生产消费面，未落地。

/// 地址位长度（48 位，支持最大 256TB 寻址空间）
///
/// 对齐 C# `LogAddress.kAddressBits`
pub const ADDRESS_BITS: u32 = 48;

/// 48 位物理/逻辑地址掩码（低 48 位全部置 1: 0x0000_FFFF_FFFF_FFFF）
///
/// 对齐 C# `LogAddress.kAddressBitMask`
pub const ADDRESS_MASK: u64 = (1u64 << ADDRESS_BITS) - 1;

/// ReadCache 逻辑地址指示位掩码（第 47 位，仍落在 48 位地址空间内）
///
/// 对齐 C# `LogAddress.IsReadCache` 判定的 `RecordInfo.kIsReadCacheBitMask`
/// （kIsReadCacheBitOffset = kAddressBits - 1）：C# 该位声明在 RecordInfo 内、由 LogAddress
/// 跨类引用，rust 侧统一归地址域，只表示「本地址指向 ReadCache 日志」。
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
/// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Common/LogAddress.cs:IsReadCache
#[inline(always)]
pub const fn is_read_cache(addr: u64) -> bool {
  (addr & READ_CACHE_BIT) == READ_CACHE_BIT
}

/// 转换为纯净的 47 位物理/绝对逻辑地址（去除 ReadCache 虚拟指示位）
///
/// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Common/LogAddress.cs:AbsoluteAddress
#[inline(always)]
pub const fn to_absolute(addr: u64) -> u64 {
  addr & ABSOLUTE_ADDRESS_MASK
}

/// 附加 ReadCache 标志位（C# 由 RecordInfo 设置，此处提供地址侧等价操作）
#[inline(always)]
pub const fn with_read_cache(addr: u64) -> u64 {
  (addr & ADDRESS_MASK) | READ_CACHE_BIT
}
