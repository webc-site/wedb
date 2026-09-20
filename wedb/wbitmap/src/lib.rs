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
pub use bit_op::{BitOpAccumulator, BitmapOperation, invoke_bit_operation_unsafe};
pub use bit_pos::bit_pos_driver;
pub use bitfield::{
  BIT_FIELD_SIGN_SIGNED, BitFieldCmdArgs, BitFieldOverflow, BitFieldSecondaryCommand,
  bit_field_execute, bit_field_execute_ro, check_bitfield_overflow, is_large_enough_for_type,
  length_from_type, new_block_alloc_length_from_type, parse_bitfield_encoding,
  parse_bitfield_offset, parse_bitfield_overflow_slice, parse_bitfield_type_offset,
};
pub use manager::{
  MAX_BITMAP_PAYLOAD_BYTES, MAX_OFFSET_FOR_BITMAP_LENGTH, get_bit, index, is_large_enough,
  is_valid_bit_offset, length, length_in_bytes, new_block_alloc_length,
  try_validate_bit_pos_offsets, try_validate_bitfield_offset, try_validate_length_in_bytes,
  update_bitmap,
};
