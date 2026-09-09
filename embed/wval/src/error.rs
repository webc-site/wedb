use std::result;

use thiserror::Error;

/// 值层编解码错误枚举（子键命名空间 / 集合元数据 / 紧凑容器）
#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
  /// 缓冲区长度不足
  #[error("缓冲区长度不足: 期望至少 {expected} 字节，实际仅 {actual} 字节")]
  BufferTooShort { expected: usize, actual: usize },

  /// 键长度超出 u32 上限
  #[error("键长度超出 u32 上限: {0}")]
  KeyLengthOverflow(usize),

  /// 值长度超出 u32 上限
  #[error("值长度超出 u32 上限: {0}")]
  ValueLengthOverflow(usize),

  /// 编码总长度溢出 usize 寻址空间
  #[error("编码总大小溢出 usize 寻址空间")]
  RecordSizeOverflow,

  /// 非法或未知的键命名空间标签
  #[error("非法或未知的键标签字节: {0:#x}")]
  InvalidKeyTag(u8),

  /// 非法或未知的集合类型
  #[error("非法或未知的集合类型字节: {0:#x}")]
  InvalidCollectionType(u8),

  /// 紧凑集合容量溢出（超出 u16 上限 65535）
  #[error("紧凑集合元素数量超出 u16 上限: {0}")]
  CompactCountOverflow(usize),

  /// 紧凑编码数据损坏或格式非法
  #[error("紧凑编码数据损坏: {0}")]
  CorruptedCompactData(&'static str),

  /// 变长整型编码非规范或存在冗余/非法前缀
  #[error("变长整型编码非规范或存在冗余/非法前缀")]
  NonCanonicalEncoding,

  /// bitcode 编解码数据损坏或格式非法
  #[error("bitcode 编解码失败: {0}")]
  BitcodeDecode(&'static str),

  /// 记录层错误透明转发（值层扩展视图触发的原位更新长度不匹配等）
  #[error(transparent)]
  Record(#[from] wrecord::Error),
}

impl From<bitcode::Error> for Error {
  #[inline]
  fn from(_: bitcode::Error) -> Self {
    Self::BitcodeDecode("bitcode 数据反序列化失败")
  }
}

/// 值层模块结果类型
pub type Result<T> = result::Result<T, Error>;
