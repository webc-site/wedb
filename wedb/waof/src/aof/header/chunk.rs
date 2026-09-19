//! 分块大值帧头（对标 libs/server/AOF/AofChunkHeader.cs 的 AofChunkHeader）

use std::mem::size_of;

use super::write_at;

/// libs/server/AOF/AofChunkHeader.cs:AofChunkHeader
///
/// 分块帧头（28B = 3×u32 + u64 + i64）：长度三元组 + objectId + keyHash。
///（对齐 C# AofChunkHeader.cs:AofChunkHeader.TotalSize）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AofChunkHeader {
  /// 溢出 key 长度。
  pub overflow_key_length: u32,
  /// 溢出 value 长度。
  pub overflow_value_length: u32,
  /// input 长度。
  pub input_length: u32,
  /// 分块对象 id。
  pub object_id: u64,
  /// key 哈希。
  pub key_hash: i64,
}

impl AofChunkHeader {
  /// 头尺寸。
  pub const TOTAL_SIZE: usize = 3 * size_of::<u32>() + size_of::<u64>() + size_of::<i64>();

  /// 序列化为 28B（LE 布局，字段偏移对标 C# AofChunkHeader FieldOffset 0/4/8/12/20）。
  #[inline]
  pub const fn to_bytes(&self) -> [u8; Self::TOTAL_SIZE] {
    let mut out = [0u8; Self::TOTAL_SIZE];
    write_at(&mut out, 0, self.overflow_key_length.to_le_bytes());
    write_at(&mut out, 4, self.overflow_value_length.to_le_bytes());
    write_at(&mut out, 8, self.input_length.to_le_bytes());
    write_at(&mut out, 12, self.object_id.to_le_bytes());
    write_at(&mut out, 20, self.key_hash.to_le_bytes());
    out
  }

  /// 解析。
  #[inline]
  pub const fn parse(entry: &[u8]) -> Option<Self> {
    let Some(chunk) = entry.first_chunk::<{ Self::TOTAL_SIZE }>() else {
      return None;
    };
    Some(Self {
      overflow_key_length: u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]),
      overflow_value_length: u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]),
      input_length: u32::from_le_bytes([chunk[8], chunk[9], chunk[10], chunk[11]]),
      object_id: u64::from_le_bytes([
        chunk[12], chunk[13], chunk[14], chunk[15], chunk[16], chunk[17], chunk[18], chunk[19],
      ]),
      key_hash: i64::from_le_bytes([
        chunk[20], chunk[21], chunk[22], chunk[23], chunk[24], chunk[25], chunk[26], chunk[27],
      ]),
    })
  }
}

#[cfg(test)]
mod tests {
  use super::AofChunkHeader;

  #[test]
  fn test_aof_chunk_header_roundtrip() {
    let chunk = AofChunkHeader {
      overflow_key_length: 12,
      overflow_value_length: 4096,
      input_length: 64,
      object_id: 12345678901234,
      key_hash: -987654321,
    };
    let bytes = chunk.to_bytes();
    assert_eq!(bytes.len(), AofChunkHeader::TOTAL_SIZE);
    let parsed = AofChunkHeader::parse(&bytes).unwrap();
    assert_eq!(parsed, chunk);
  }
}
