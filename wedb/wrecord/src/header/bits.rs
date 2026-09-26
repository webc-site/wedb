//! 记录头位段常量群：16 字节双字（RecordInfo 字 + RecordDataHeader 字）的掩码、位移、
//! 编译期封闭性断言与 `pack_rdh_word` 类纯位运算。
//!
//! 位段编解码定义全仓唯此一处，视图侧（record_ref / record_mut）经 `Deref` 复用记录头访问器，
//! 不另立第二套位段。新增位段一律进本件掩码区 + 记录头字段访问器，禁在视图文件散落裸位运算。

use wbase::addr::{ADDRESS_BITS, ADDRESS_MASK};

use super::RECORD_ALIGNMENT;

/// 换页填充标记中的特殊魔数（key_len 为 24 位全 1 表示 Pad 填充）
///
/// 哨兵独占本位段顶值：合法键长上限 `codec::MAX_KEY_LEN = PAD_KEY_LEN - 1`，
/// 两值域严格互斥，真实记录绝无与 Pad 头混淆的编码形态。
pub const PAD_KEY_LEN: u32 = (1 << 24) - 1;

// RecordInfo 字（第一字 bits 48..63）的保留与标志位段——位段声明在记录头自身类型内，
// 与 C# libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs 的同名位段同拓扑；
// 低 48 位地址与地址级 ReadCache 指示位属地址域，仍由 wbase::addr 供。

/// RecordInfo 字保留位段掩码（bits 48..58，共 11 位，恒零）
const RECORD_INFO_RESERVED_MASK: u64 = 0x7FFu64 << ADDRESS_BITS;

/// 修改位掩码（第 59 位，对标 C# Tsavorite RecordInfo.Modified）
pub const MODIFIED_BIT: u64 = 1u64 << 59;

/// 密封位掩码（第 60 位，对标 C# Tsavorite RecordInfo.IsSealed / TrySeal，复活槽位锁定与冻结）
pub const SEALED_BIT: u64 = 1u64 << 60;

/// Checkpoint 检查点新版本标记位掩码（第 61 位，对标 C# Tsavorite RecordInfo.IsInNewVersion）
pub const IN_NEW_VERSION_BIT: u64 = 1u64 << 61;

/// 读缓存记录标记位掩码（第 62 位，对标 C# Tsavorite RecordInfo.IsReadCache）
///
/// 注意：本位是 RecordInfo 字内的**记录级**驻留标志，与 wbase::addr::READ_CACHE_BIT
/// （48 位地址空间内的 bit 47 **地址级** ReadCache 指示）名近值异，分属两个不同的
/// 64 位字，维护时严禁互相混用。
pub const HEADER_READ_CACHE_BIT: u64 = 1u64 << 62;

/// 墓碑标记位掩码（第 63 位: 0x8000_0000_0000_0000）
pub const TOMBSTONE_BIT: u64 = 1u64 << 63;

/// RecordInfo 字标志位统一掩码（bits 59..63）
const RECORD_INFO_FLAG_MASK: u64 =
  MODIFIED_BIT | SEALED_BIT | IN_NEW_VERSION_BIT | HEADER_READ_CACHE_BIT | TOMBSTONE_BIT;

/// 8 位填充词位段（RDH bits 0..7，每词 8 字节，最多 255 * 8 = 2040 字节松弛空间，
/// 对标 libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs:FillerWords）
pub(crate) const FILLER_WORDS_SHIFT: u32 = 0;
pub(crate) const FILLER_WORDS_BITS: u32 = 8;
pub(crate) const FILLER_WORDS_VALUE_MASK: u64 = (1u64 << FILLER_WORDS_BITS) - 1;
pub(crate) const FILLER_WORDS_MASK: u64 = FILLER_WORDS_VALUE_MASK << FILLER_WORDS_SHIFT;

/// 24 位键长度位段（RDH bits 8..31，对标 libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs:KeyLength）
pub(super) const KEY_LEN_SHIFT: u32 = FILLER_WORDS_SHIFT + FILLER_WORDS_BITS;
pub(crate) const KEY_LEN_BITS: u32 = 24;
const KEY_LEN_VALUE_MASK: u64 = (1u64 << KEY_LEN_BITS) - 1;
pub(super) const KEY_LEN_MASK: u64 = KEY_LEN_VALUE_MASK << KEY_LEN_SHIFT;

/// 32 位值长度位段（RDH bits 32..63，对标 libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs:ValueLength）
pub(super) const VAL_LEN_SHIFT: u32 = KEY_LEN_SHIFT + KEY_LEN_BITS;
const VAL_LEN_BITS: u32 = 32;
pub(super) const VAL_LEN_VALUE_MASK: u64 = (1u64 << VAL_LEN_BITS) - 1;
pub(super) const VAL_LEN_MASK: u64 = VAL_LEN_VALUE_MASK << VAL_LEN_SHIFT;

/// 最大可表示的单记录显式松弛填充字节数（255 词 * 8 字节 = 2040 字节，
/// 对标 libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs:MaxFillerWords * kRecordAlignment）
pub const MAX_FILLER_BYTES: usize = (FILLER_WORDS_VALUE_MASK as usize) << 3;

// 编译期静态断言 1：RecordInfo 字位域封闭性 —— 48 位地址 + 11 位保留 + 5 个标志位
// 恰好无缝铺满 64 位字，任何位既不重叠也不遗漏，宽度分配在编译期即可证明正确。
const _: () =
  assert!((ADDRESS_MASK | RECORD_INFO_RESERVED_MASK | RECORD_INFO_FLAG_MASK) == u64::MAX);

// 编译期静态断言 2：RDH 原子字位域封闭性 —— 8 位填充词 + 24 位键长 + 32 位值长
// 恰好铺满 64 位字（对标 RecordDataHeader 单字容纳完整记录布局的推导能力）。
const _: () = assert!((FILLER_WORDS_MASK | KEY_LEN_MASK | VAL_LEN_MASK) == u64::MAX);

/// 向上对齐到 [RECORD_ALIGNMENT] 边界（对标 C# Utility.RoundUp(sum, kRecordAlignment)）
#[inline(always)]
pub(crate) const fn align_record_size(size: usize) -> usize {
  (size + (RECORD_ALIGNMENT - 1)) & !(RECORD_ALIGNMENT - 1)
}

/// 按开关置位/清零复合字中的指定位段（const fn，供各标志位 setter 共享）
#[inline(always)]
pub(super) const fn with_bit(word: u64, mask: u64, on: bool) -> u64 {
  if on { word | mask } else { word & !mask }
}

/// RDH 原子字三段位段融合打包（const fn：filler | key_len | val_len）
#[inline(always)]
pub(crate) const fn pack_rdh_word(filler_words: u8, key_len: u32, val_len: u32) -> u64 {
  ((filler_words as u64) & FILLER_WORDS_VALUE_MASK)
    | (((key_len as u64) & KEY_LEN_VALUE_MASK) << KEY_LEN_SHIFT)
    | (((val_len as u64) & VAL_LEN_VALUE_MASK) << VAL_LEN_SHIFT)
}
