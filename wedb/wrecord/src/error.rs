use std::result;

use thiserror::Error;

/// 记录编解码与内存视图错误枚举
#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
  /// 缓冲区长度不足
  #[error("缓冲区长度不足: 期望至少 {expected} 字节，实际仅 {actual} 字节")]
  BufferTooShort { expected: usize, actual: usize },

  /// 逻辑地址超出 48 位上限（最大 256TB）
  #[error("逻辑地址超出 48 位限制 (最大 256TB): {0:#x}")]
  AddressOverflow(u64),

  /// 键长度超出 u32 上限
  #[error("键长度超出 u32 上限: {0}")]
  KeyLengthOverflow(usize),

  /// 值长度超出 u32 上限
  #[error("值长度超出 u32 上限: {0}")]
  ValueLengthOverflow(usize),

  /// 记录总长度溢出 usize
  #[error("记录总大小溢出 usize 寻址空间")]
  RecordSizeOverflow,

  /// 原位更新时新值长度与原记录值长度不一致
  #[error("原位更新值长度不匹配: 期望 {expected} 字节, 实际 {actual} 字节")]
  ValueLengthMismatch { expected: usize, actual: usize },

  /// 墓碑记录禁止普通原位更新（复活须走显式复活路径 revivify_with_slack）
  #[error("墓碑记录禁止原位更新, 须走复活路径")]
  TombstoneUpdate,
}

/// 记录模块结果类型
pub type Result<T> = result::Result<T, Error>;
