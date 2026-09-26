use std::{borrow::Borrow, ops::Deref};

use super::header::WalFrameHeader;

/// WAL 迭代扫描返回的记录
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
  /// 记录起始逻辑地址（包含记录头）
  pub address: u64,
  /// 下一条记录的起始逻辑地址
  pub next_address: u64,
  /// 记录头元数据
  pub header: WalFrameHeader,
  /// 记录负载数据
  pub payload: Vec<u8>,
}

impl Deref for WalRecord {
  type Target = [u8];

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.payload
  }
}

impl AsRef<[u8]> for WalRecord {
  #[inline]
  fn as_ref(&self) -> &[u8] {
    &self.payload
  }
}

impl Borrow<[u8]> for WalRecord {
  #[inline]
  fn borrow(&self) -> &[u8] {
    &self.payload
  }
}

impl WalRecord {
  /// 获取负载字节切片
  #[inline]
  pub fn as_slice(&self) -> &[u8] {
    &self.payload
  }

  /// 重建完整记录帧（8B 记录头 + 负载）
  ///
  /// 复制转发用：与复制推流拉取的帧口径一致（8B 记录头 + 负载），
  /// 从侧/补扫侧拿到的是同一字节序列（保真落盘经 [`crate::log::WalLog::enqueue_raw`]）
  pub fn reconstruct_frame(&self) -> Vec<u8> {
    let mut frame = Vec::with_capacity(super::header::RECORD_HEADER_LEN + self.payload.len());
    frame.extend_from_slice(&self.header.to_bytes());
    frame.extend_from_slice(&self.payload);
    frame
  }

  /// 获取负载字节长度
  #[inline]
  pub fn len(&self) -> usize {
    self.payload.len()
  }

  /// 检查负载是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.payload.is_empty()
  }
}

/// WAL 迭代扫描返回的直接组装完整帧（8B 记录头 + 负载，一次分配零二次重拷）
///
/// 复制推流与网络直传用：与推流端口帧口径严格一致（8B 记录头 + 负载），
/// decode 校验通过后一次分配完整帧缓冲，消除 reconstruct_frame 的二次分配重拷
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFrame {
  /// 记录起始逻辑地址（包含记录头）
  pub address: u64,
  /// 下一条记录的起始逻辑地址
  pub next_address: u64,
  /// 完整记录帧数据（8B 记录头 + 负载）
  pub frame: Vec<u8>,
}

impl WalFrame {
  /// 获取完整帧切片（8B 记录头 + 负载）
  #[inline]
  pub fn as_slice(&self) -> &[u8] {
    &self.frame
  }

  /// 获取负载切片（跳过 8B 记录头）
  #[inline]
  pub fn payload(&self) -> &[u8] {
    &self.frame[super::header::RECORD_HEADER_LEN..]
  }

  /// 获取记录头元数据
  #[inline]
  pub fn header(&self) -> WalFrameHeader {
    WalFrameHeader::from_bytes(
      self.frame[..super::header::RECORD_HEADER_LEN]
        .try_into()
        .expect("frame header len matches"),
    )
  }

  /// 获取完整帧字节长度
  #[inline]
  pub fn len(&self) -> usize {
    self.frame.len()
  }

  /// 检查完整帧是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.frame.is_empty()
  }
}

impl Deref for WalFrame {
  type Target = [u8];

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.frame
  }
}

impl AsRef<[u8]> for WalFrame {
  #[inline]
  fn as_ref(&self) -> &[u8] {
    &self.frame
  }
}

impl Borrow<[u8]> for WalFrame {
  #[inline]
  fn borrow(&self) -> &[u8] {
    &self.frame
  }
}
