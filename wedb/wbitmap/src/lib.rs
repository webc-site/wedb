//! 高性能位图与位域核心算法库（对标 Garnet BitmapManager 系列）
//!
//! 提供纯位运算、位计数、位查找、位域（BITFIELD）语法解析与执行逻辑，
//! 基于零拷贝切片设计，不绑定网络和存储。

pub mod bit_count;
pub mod bit_op;
pub mod bit_pos;
pub mod bitfield;
pub mod manager;

pub use bit_count::bit_count_driver;
pub use bit_op::{BitOpAccumulator, BitmapOperation};
pub use bit_pos::bit_pos_driver;
pub use bitfield::{
  BitFieldCmdArgs, BitFieldOverflow, BitFieldSecondaryCommand, bit_field_execute,
  bit_field_execute_ro, new_block_alloc_length_from_type, parse_bitfield_encoding,
  parse_bitfield_overflow_slice, parse_bitfield_type_offset, parse_bitmap_offset_type,
  try_get_bitfield_secondary_command,
};
pub use manager::{
  MAX_BITMAP_PAYLOAD_BYTES, get_bit, is_valid_bit_offset, length_in_bytes,
  try_validate_bit_pos_offsets, update_bitmap,
};
