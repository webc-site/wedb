use std::ops::Deref;

use wbase::pool::AlignedBuf;
use wrecord::RecordRef;

use crate::error::Result;

/// 记录读取输出结果
///
/// 统一封装来自内存驻留区或磁盘落盘区的记录数据，支持零拷贝只读解构。
#[derive(Debug)]
pub enum RecordOutput {
  /// 来自内存页缓冲
  Memory(Vec<u8>),
  /// 来自磁盘扇区对齐 I/O 缓冲
  Disk(AlignedBuf),
}

impl Deref for RecordOutput {
  type Target = [u8];

  #[inline]
  fn deref(&self) -> &Self::Target {
    self.as_slice()
  }
}

impl AsRef<[u8]> for RecordOutput {
  #[inline]
  fn as_ref(&self) -> &[u8] {
    self.as_slice()
  }
}

impl RecordOutput {
  /// 获取底层字节切片
  #[inline]
  pub fn as_slice(&self) -> &[u8] {
    match self {
      Self::Memory(v) => v.as_slice(),
      Self::Disk(b) => b.as_slice(),
    }
  }

  /// 获取字节长度
  #[inline]
  pub fn len(&self) -> usize {
    self.as_slice().len()
  }

  /// 字节切片是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.as_slice().is_empty()
  }

  /// 解析为只读零拷贝 RecordRef 视图
  #[inline]
  pub fn as_record_ref(&self) -> Result<RecordRef<'_>> {
    RecordRef::from_slice(self.as_slice()).map_err(Into::into)
  }

  /// 获取键切片借用
  #[inline]
  pub fn key(&self) -> Result<&[u8]> {
    self.as_record_ref().map(|r| r.key())
  }

  /// 获取值切片借用
  #[inline]
  pub fn value(&self) -> Result<&[u8]> {
    self.as_record_ref().map(|r| r.value())
  }

  /// 是否为墓碑记录
  #[inline]
  pub fn is_tombstone(&self) -> Result<bool> {
    self.as_record_ref().map(|r| r.is_tombstone())
  }
}
