use wbase::crc::crc32;

use super::error::{Error, Result};

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

  /// 将记录头编码到 8 字节目标切片中（小端字节序）
  #[inline]
  pub fn encode(&self, dest: &mut [u8]) {
    debug_assert!(dest.len() >= RECORD_HEADER_LEN);
    if let Some(chunk) = dest.first_chunk_mut::<RECORD_HEADER_LEN>() {
      *chunk = self.to_bytes();
    }
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

  /// 检查切片开头是否为全零记录头（单次 64 位无符号整数比对）
  #[inline(always)]
  pub const fn is_zero_slice(src: &[u8]) -> bool {
    if let Some((chunk, _)) = src.split_first_chunk::<RECORD_HEADER_LEN>() {
      u64::from_le_bytes(*chunk) == 0
    } else {
      false
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
