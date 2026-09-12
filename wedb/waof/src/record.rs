use std::{borrow::Borrow, ops::Deref};

use super::header::RecordHeader;

/// WAL 迭代扫描返回的记录
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
  /// 记录起始逻辑地址（包含记录头）
  pub address: u64,
  /// 下一条记录的起始逻辑地址
  pub next_address: u64,
  /// 记录头元数据
  pub header: RecordHeader,
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
  /// 复制转发用：与 `crate::log::ReplicationSinkFn` 推流端口的帧口径一致，
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
