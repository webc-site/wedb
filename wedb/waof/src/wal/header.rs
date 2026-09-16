//! WAL 物理帧头（8B 定长记录头，TsavoriteLog 物理层对标）

use wbase::crc::{Crc32Hasher, crc32};

use crate::error::{Error, Result};

/// WAL 记录头定长字节大小（8 字节：4 字节 entry_len + 4 字节 crc32）
pub const RECORD_HEADER_LEN: usize = 8;

/// 空负载记录的 CRC32 哨兵值（刻意取非 0）
///
/// 使全零 8 字节头唯一对应扇区填充（padding）/崩溃残缺尾部，而已提交的空记录
/// 携带非零哨兵可在崩溃恢复中被识别，兑现 commit 的持久性承诺
/// （对照 C# TsavoriteLog：其记录头含非零 AllocatedSize 字段天然可区分，无此歧义）
const EMPTY_PAYLOAD_CRC: u32 = 0xFFFF_FFFF;

/// 计算记录负载的 CRC32 校验码（空负载返回哨兵值，保证头不全零）
#[inline]
fn payload_crc(payload: &[u8]) -> u32 {
  if payload.is_empty() {
    EMPTY_PAYLOAD_CRC
  } else {
    crc32(payload)
  }
}

/// WAL 记录头元数据
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct RecordHeader {
  /// 记录负载的字节长度
  pub entry_len: u32,
  /// 记录负载的 CRC32 校验码
  pub crc32: u32,
}

impl RecordHeader {
  /// 创建新的记录头
  #[inline]
  pub const fn new(entry_len: u32, crc32: u32) -> Self {
    Self { entry_len, crc32 }
  }

  /// 检查记录头是否为全零（仅扇区末尾 padding 或残缺尾部；合法记录头绝不全零）
  #[inline]
  pub const fn is_zero(&self) -> bool {
    self.entry_len == 0 && self.crc32 == 0
  }

  /// 获取负载长度（usize 格式）
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:GetLength
  ///
  /// C# GetLength 按日志 checksum 类型取偏移（None=0 / PerEntry=8）后读 4B 记录长度，
  /// 供扫描迭代器推进；waof 记录头固定 8B（4B entry_len + 4B 内嵌 CRC32），无 checksum
  /// 变体偏移，entry_len 恒在偏移 0，与 C# 两种取偏移情形固定等价（帧总长 =
  /// payload_len() + RECORD_HEADER_LEN）。迭代器推进见 waof/src/wal/iterator.rs
  #[inline]
  pub const fn payload_len(&self) -> usize {
    self.entry_len as usize
  }

  /// 为给定的负载数据计算 CRC32 并创建记录头
  #[inline]
  pub fn for_payload(payload: &[u8]) -> Self {
    Self {
      entry_len: payload.len() as u32,
      crc32: payload_crc(payload),
    }
  }

  /// 为分部件负载增量计算 CRC32 并创建记录头（scatter-write 入口）
  ///
  /// CRC32 线性可分段：分段累加与整包单遍结果逐位一致，调用方无须预拼
  /// 整包即可得到与 [`Self::for_payload`] 完全相同的记录头
  #[inline]
  pub fn for_payload_parts(parts: &[&[u8]]) -> Self {
    let total_len: usize = parts.iter().map(|part| part.len()).sum();
    if total_len == 0 {
      return Self::new(0, EMPTY_PAYLOAD_CRC);
    }
    let mut hasher = Crc32Hasher::new();
    for part in parts {
      hasher.update(part);
    }
    Self {
      entry_len: total_len as u32,
      crc32: hasher.finalize(),
    }
  }

  /// 校验负载数据长度与 CRC32 校验码
  #[inline]
  pub fn verify(&self, payload: &[u8]) -> Result<()> {
    if payload.len() != self.entry_len as usize {
      return Err(Error::InvalidRecordHeader);
    }
    let actual = payload_crc(payload);
    if actual != self.crc32 {
      return Err(Error::ChecksumMismatch {
        expected: self.crc32,
        actual,
      });
    }
    Ok(())
  }

  /// 将记录头转为 8 字节定长数组（单次 64 位位移与编码）
  #[inline]
  pub const fn to_bytes(&self) -> [u8; RECORD_HEADER_LEN] {
    let packed = (self.entry_len as u64) | ((self.crc32 as u64) << 32);
    packed.to_le_bytes()
  }

  /// 从 8 字节定长数组无失败解码记录头（内存路径专用，单次 64 位无分支解码）
  #[inline]
  pub const fn from_bytes(src: &[u8; RECORD_HEADER_LEN]) -> Self {
    let packed = u64::from_le_bytes(*src);
    Self {
      entry_len: packed as u32,
      crc32: (packed >> 32) as u32,
    }
  }

  /// 尝试从字节切片中快速解码记录头（const fn）
  #[inline(always)]
  pub const fn decode_opt(src: &[u8]) -> Option<Self> {
    if let Some((chunk, _)) = src.split_first_chunk::<RECORD_HEADER_LEN>() {
      Some(Self::from_bytes(chunk))
    } else {
      None
    }
  }

  /// 从切片中解码记录头（磁盘路径专用，切片长度不足 8 字节时报错）
  #[inline]
  pub fn decode(src: &[u8]) -> Result<Self> {
    Self::decode_opt(src).ok_or(Error::InvalidRecordHeader)
  }
}

#[cfg(test)]
mod tests {
  use super::{EMPTY_PAYLOAD_CRC, RECORD_HEADER_LEN, RecordHeader};

  #[test]
  fn test_record_header_roundtrip() {
    let header = RecordHeader::new(128, 0x1234_5678);
    let bytes = header.to_bytes();
    let decoded = RecordHeader::decode(&bytes).unwrap();
    assert_eq!(decoded, header);
    assert_eq!(decoded.payload_len(), 128);
    assert!(!decoded.is_zero());

    let zero = RecordHeader::new(0, 0);
    assert!(zero.is_zero());

    // 空负载头必须带非零 CRC 哨兵，决不能与全零 padding 混淆
    let empty_payload_header = RecordHeader::for_payload(&[]);
    assert_eq!(empty_payload_header.payload_len(), 0);
    assert!(
      !empty_payload_header.is_zero(),
      "空有效记录头必须携带非零哨兵 CRC"
    );
    assert_eq!(empty_payload_header.crc32, EMPTY_PAYLOAD_CRC);

    // 校验全零定长数组为 padding / 损坏
    let all_zeros = [0u8; RECORD_HEADER_LEN];
    let zero_decoded = RecordHeader::decode(&all_zeros).unwrap();
    assert!(zero_decoded.is_zero(), "全零头唯一标识 padding 或残缺尾部");
  }

  #[test]
  fn test_record_header_for_payload_and_verify() {
    let payload = b"hello aof payload";
    let header = RecordHeader::for_payload(payload);
    assert_eq!(header.payload_len(), payload.len());
    assert!(header.verify(payload).is_ok());

    let corrupted = b"hello aof payloae";
    assert!(header.verify(corrupted).is_err());
    assert!(header.verify(&payload[..payload.len() - 1]).is_err());
  }

  #[test]
  fn test_record_header_decode_boundary() {
    let short = [0u8; 7];
    assert!(RecordHeader::decode_opt(&short).is_none());
    assert!(RecordHeader::decode(&short).is_err());
  }
}
