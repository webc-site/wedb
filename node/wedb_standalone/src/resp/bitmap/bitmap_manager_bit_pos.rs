//! 位图查找命令（BITPOS）底层算法
//! 对标 Garnet `libs/server/Resp/Bitmap/BitmapManagerBitPos.cs`

use super::bitmap_manager::BitmapManager;

pub struct BitmapManagerBitPos;

impl BitmapManagerBitPos {
  /// libs/server/Resp/Bitmap/BitmapManagerBitPos.cs:BitPosDriver
  pub fn bit_pos_driver(
    input: &[u8],
    mut start_offset: i64,
    mut end_offset: i64,
    search_for: u8,
    offset_type: u8,
  ) -> i64 {
    let input_len = input.len() as i64;
    if offset_type == 0x0 {
      // 字节模式 (BYTE)
      if start_offset < 0 {
        start_offset = BitmapManager::process_negative_offset(start_offset, input_len);
      }
      if end_offset < 0 {
        end_offset = BitmapManager::process_negative_offset(end_offset, input_len);
      }

      if start_offset >= input_len || start_offset > end_offset {
        return -1;
      }

      if end_offset >= input_len {
        end_offset = input_len - 1;
      }

      Self::bit_pos_byte_search(input, start_offset, end_offset, search_for)
    } else {
      // 位模式 (BIT)
      let bit_len = input_len * 8;
      if start_offset < 0 {
        start_offset = BitmapManager::process_negative_offset(start_offset, bit_len);
      }
      if end_offset < 0 {
        end_offset = BitmapManager::process_negative_offset(end_offset, bit_len);
      }

      let start_byte_index = start_offset >> 3;
      let end_byte_index = end_offset >> 3;

      if start_byte_index >= input_len || start_byte_index > end_byte_index {
        return -1;
      }

      if end_offset >= bit_len {
        end_offset = bit_len - 1;
      }

      Self::bit_pos_bit_search(input, start_offset, end_offset, search_for)
    }
  }

  /// libs/server/Resp/Bitmap/BitmapManagerBitPos.cs:BitPosBitSearch
  pub fn bit_pos_bit_search(
    input: &[u8],
    start_bit_offset: i64,
    end_bit_offset: i64,
    search_for: u8,
  ) -> i64 {
    let mut current_bit_offset = start_bit_offset;
    while current_bit_offset <= end_bit_offset {
      let byte_index = (current_bit_offset >> 3) as usize;
      let left_bit_offset = (current_bit_offset & 7) as u32;
      let boundary = 8 - left_bit_offset;
      let right_bit_offset = if current_bit_offset + (boundary as i64) <= end_bit_offset {
        left_bit_offset + boundary
      } else {
        (end_bit_offset & 7) as u32 + 1
      };

      // 构造起止位区间掩码
      let mask = ((0xffu8 >> left_bit_offset) ^ (0xffu8 >> right_bit_offset)) as u64;
      let b = input[byte_index] as u64;
      let payload = b & mask;
      let invalid_mask = if search_for == 1 { 0 } else { mask };

      if payload != invalid_mask {
        let mut p = payload << (56 + left_bit_offset);
        if search_for == 0 {
          p = !p;
        }
        let lzcnt = p.leading_zeros() as i64;
        return current_bit_offset + lzcnt;
      }

      current_bit_offset += boundary as i64;
    }
    -1
  }

  /// libs/server/Resp/Bitmap/BitmapManagerBitPos.cs:BitPosByteSearch
  pub fn bit_pos_byte_search(
    input: &[u8],
    start_offset: i64,
    end_offset: i64,
    search_for: u8,
  ) -> i64 {
    let invalid_mask8 = if search_for == 1 { 0x00u8 } else { 0xffu8 };
    let mut curr = start_offset as usize;
    let end = end_offset as usize;

    while curr <= end {
      let rem = end - curr + 1;
      if rem >= 8 {
        let val = u64::from_be_bytes(input[curr..curr + 8].try_into().unwrap());
        let invalid = if search_for == 1 { 0u64 } else { !0u64 };
        if val != invalid {
          let p = if search_for == 1 { val } else { !val };
          return (curr as i64 * 8) + p.leading_zeros() as i64;
        }
        curr += 8;
      } else if rem >= 4 {
        let val = u32::from_be_bytes(input[curr..curr + 4].try_into().unwrap());
        let invalid = if search_for == 1 { 0u32 } else { !0u32 };
        if val != invalid {
          let p = if search_for == 1 { val } else { !val };
          return (curr as i64 * 8) + p.leading_zeros() as i64;
        }
        curr += 4;
      } else {
        let b = input[curr];
        if b != invalid_mask8 {
          let p = if search_for == 1 { b } else { !b };
          return (curr as i64 * 8) + p.leading_zeros() as i64;
        }
        curr += 1;
      }
    }
    -1
  }
}
