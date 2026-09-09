use std::{io, result};

use thiserror::Error;

/// wdev 错误类型
#[derive(Error, Debug)]
pub enum Error {
  /// 底层 I/O 错误
  #[error(transparent)]
  Io(#[from] io::Error),

  /// 内存/对齐错误（来自 wram）
  #[error(transparent)]
  Mem(#[from] wram::Error),

  /// 偏移量未按扇区对齐
  #[error("偏移量未对齐: 偏移量 {offset} 不是扇区大小 {align} 的整数倍")]
  UnalignedOffset { offset: u64, align: usize },

  /// 长度未按扇区对齐
  #[error("长度未对齐: 长度 {len} 不是扇区大小 {align} 的整数倍")]
  UnalignedLen { len: usize, align: usize },

  /// 缓冲区内存地址未按扇区对齐
  #[error("缓冲区内存未对齐: 指针地址 {ptr:#x} 不是扇区大小 {align} 的整数倍")]
  UnalignedBuffer { ptr: usize, align: usize },

  /// 段文件未找到
  #[error("段不存在: 段编号 {0}")]
  SegmentNotFound(u32),

  /// 访问越界
  #[error("访问越界: 偏移量 {offset}, 长度 {len}")]
  OutOfBounds { offset: u64, len: usize },

  /// 无效扇区大小（非 2 的幂或小于最小值）
  #[error("无效扇区大小: 大小 {size}, 最小为 {min}")]
  InvalidSectorSize { size: usize, min: usize },

  /// 无效段大小（未按扇区对齐、非 2 的幂或小于扇区大小）
  #[error("无效段大小: 大小 {0} 必须大于 0、为 2 的幂且至少为扇区大小")]
  InvalidSegmentSize(u64),

  /// 无效容量上限（须为段大小的整数倍，对标 C# Initialize 容量校验）
  #[error("无效容量: {capacity} 必须为段大小的正整数倍")]
  InvalidCapacity { capacity: u64 },

  /// 段编号超出 u32 范围
  #[error("段编号超出范围: {0}")]
  SegmentExceeded(u64),

  /// 读取提前遇到 EOF
  #[error("读取提前结束 (EOF): 预期至少读取 {expected} 字节，实际仅读取 {actual} 字节")]
  UnexpectedEof { expected: usize, actual: usize },

  /// 恢复时已存在的段文件超过配置段大小（对标 C# ValidateRecoveredSegments）
  #[error("恢复校验失败: 段 {segment} 文件大小 {file_size} 超过配置段大小 {segment_size}")]
  SegmentSizeMismatch {
    segment: u32,
    file_size: u64,
    segment_size: u64,
  },

  /// 设备处于只读模式，拒绝写操作（对标 C# readOnly 保护）
  #[error("设备处于只读模式，拒绝写操作: 偏移量 {offset}, 长度 {len}")]
  ReadOnly { offset: u64, len: usize },
}

/// wdev 结果类型
pub type Result<T> = result::Result<T, Error>;

impl From<wram::SectorRangeError> for Error {
  #[inline]
  fn from(err: wram::SectorRangeError) -> Self {
    Self::Mem(wram::Error::from(err))
  }
}
