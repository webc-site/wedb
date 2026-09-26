//! BITFIELD 位域操作（对标 libs/server/Resp/Bitmap/BitmapManagerBitfield.cs，
//! C# 为 BitmapManager partial）
//!
//! 算法前提与 C# 一致：位域按 MSB 在前的字节序列存放，`offset` 为域首位的
//! 位偏移，`encoding` 为位宽。C# 以 `stackalloc byte[8]` 草稿 + 裸指针
//! （curr/cend/vend）读写；Rust 侧以游标下标承接，`vend`（位图末端）截断
//! 语义保持不变。执行核收敛双签名：写命令走 [`execute::bit_field_execute`]
//!（GET/SET/INCRBY），BITFIELD_RO 走只读 [`execute::bit_field_execute_ro`]
//!（GET-only，无写面，对标 C# BitFieldExecute_RO）。

mod execute;
mod parse;

pub use execute::{bit_field_execute, bit_field_execute_ro};
pub use parse::{
  BitFieldCmdArgs, BitFieldOverflow, BitFieldSecondaryCommand, new_block_alloc_length_from_type,
  parse_bitfield_encoding, parse_bitfield_overflow_slice, parse_bitfield_type_offset,
  parse_bitmap_offset_type, try_get_bitfield_secondary_command,
};
