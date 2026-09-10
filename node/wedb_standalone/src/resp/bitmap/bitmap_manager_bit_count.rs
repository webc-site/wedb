pub struct BitmapManagerBitCount;

impl BitmapManagerBitCount {
  /// libs/server/Resp/Bitmap/BitmapManagerBitCount.cs:BitIndexCount
  pub fn bit_index_count(data: &[u8], start_bit: usize, end_bit: usize) -> u64 {
    if start_bit > end_bit || start_bit >= data.len() * 8 {
      return 0;
    }
    let mut count = 0u64;
    for bit_pos in start_bit..=end_bit.min(data.len() * 8 - 1) {
      let byte_idx = bit_pos >> 3;
      let bit_idx = 7 - (bit_pos & 7);
      if (data[byte_idx] >> bit_idx) & 1 == 1 {
        count += 1;
      }
    }
    count
  }

  /// libs/server/Resp/Bitmap/BitmapManagerBitCount.cs:BitCountDriver
  pub fn bit_count_driver(data: &[u8]) -> u64 {
    Self::__scalar_popc(data)
  }

  /// libs/server/Resp/Bitmap/BitmapManagerBitCount.cs:__scalar_popc
  pub fn __scalar_popc(data: &[u8]) -> u64 {
    let mut total = 0u64;
    let (chunks, rem) = data.as_chunks::<8>();
    for chunk in chunks {
      let val = u64::from_le_bytes(*chunk);
      total += val.count_ones() as u64;
    }
    for &b in rem {
      total += b.count_ones() as u64;
    }
    total
  }

  /// libs/server/Resp/Bitmap/BitmapManagerBitCount.cs:__simd_popcX128
  pub fn __simd_popc_x128(data: &[u8]) -> u64 {
    Self::__scalar_popc(data)
  }

  /// libs/server/Resp/Bitmap/BitmapManagerBitCount.cs:__simd_popcX256
  pub fn __simd_popc_x256(data: &[u8]) -> u64 {
    Self::__scalar_popc(data)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_popcount() {
    let data = [0xFF, 0x0F, 0x00, 0xAA];
    assert_eq!(BitmapManagerBitCount::__scalar_popc(&data), 8 + 4 + 4);
    assert_eq!(BitmapManagerBitCount::bit_index_count(&data, 0, 7), 8);
    assert_eq!(BitmapManagerBitCount::bit_index_count(&data, 8, 11), 0);
    assert_eq!(BitmapManagerBitCount::bit_index_count(&data, 12, 15), 4);
  }
}
