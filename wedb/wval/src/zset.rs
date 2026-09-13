//! 有序集合保序分值编解码（复用 [`wbase::float`] 的 IEEE 754 全序映射）
//!
//! 与 C# Garnet 有序全序的等价性（garnet/libs/server/Objects/SortedSet/
//! SortedSetObject.cs）：C# 内存态为 `SortedSet<(double Score, byte[] Element)>`
//! （红黑树，无跳表），其全序由 SortedSetComparer 定义
//! （garnet/libs/server/Objects/SortedSetComparer.cs:18-24）：先
//! `double.CompareTo`，同分再按成员字节 `SequenceCompareTo`（无符号字典序）。
//!
//! 保序映射复用 [`wbase::float`] 的 IEEE 754 全序（符号位翻转编码）：全部有限
//! 数值与 ±∞ 的字节序与 `double.CompareTo` 一致；位级差异仅两处——
//! .NET CompareTo 将 NaN 视为小于任何数且彼此相等，编码将 ±NaN 排于 ±∞ 外侧；
//! .NET CompareTo 视 -0.0 == +0.0，编码区分 -0.0 < +0.0（两值可区分存储）。
//! Redis ZADD 语义本就拒绝 NaN 分值，命令层先行拦截，不影响扫描等价性。
//!
//! 打平大集合的有序扫描由 wcol 的 ZSet 单树双前缀算子（TreePrefix::ZSetScore /
//! ZSetMember）承担，本模块仅提供紧凑编码 CompactZSet 所需的分值保序位运算。

use wbase::float;

/// 将 f64 浮点数转换为大端保序 8 字节数组（底层直接复用 wbase::float::encode_f64）
#[inline(always)]
pub const fn encode_order_preserving_f64(val: f64) -> [u8; 8] {
  float::encode_f64(val)
}

/// 从保序大端 8 字节数组还原 f64 浮点数（底层直接复用 wbase::float::decode_f64）
#[inline(always)]
pub const fn decode_order_preserving_f64(bytes: [u8; 8]) -> f64 {
  float::decode_f64(bytes)
}
