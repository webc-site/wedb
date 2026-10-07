//! 分块大值帧头（对标 libs/server/AOF/AofChunkHeader.cs 的 AofChunkHeader）
//!
//! 自研依据: AOF 分块头

use std::mem::size_of;

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
    let k = self.overflow_key_length.to_le_bytes();
    let v = self.overflow_value_length.to_le_bytes();
    let i = self.input_length.to_le_bytes();
    let o = self.object_id.to_le_bytes();
    let h = self.key_hash.to_le_bytes();
    [
      k[0], k[1], k[2], k[3], v[0], v[1], v[2], v[3], i[0], i[1], i[2], i[3], o[0], o[1], o[2],
      o[3], o[4], o[5], o[6], o[7], h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7],
    ]
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
