use std::result;

use thiserror::Error;

/// 值层编解码错误枚举（子键命名空间 / 集合元数据 / 紧凑容器）
///
/// 全变体仅承载纯数据（零 drop 胶水、可 Copy），以支撑本 crate 编解码函数
/// 全量 const 化——编译期求值契约由 tests/meta_and_subkey.rs 的 `const` 断言
/// 显式锁定。
#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
  /// 缓冲区长度不足
  #[error("缓冲区长度不足: 期望至少 {expected} 字节，实际仅 {actual} 字节")]
  BufferTooShort { expected: usize, actual: usize },

  /// 值长度超出 u32 上限
  #[error("值长度超出 u32 上限: {0}")]
  ValueLengthOverflow(usize),

  /// 编码总长度溢出 usize 寻址空间
  #[error("编码总大小溢出 usize 寻址空间")]
  RecordSizeOverflow,

  /// 非法或未知的键命名空间标签
  #[error("非法或未知的键标签字节: {0:#x}")]
  InvalidKeyTag(u8),

  /// 非法或未知的 Garnet 对象类型
  #[error("非法或未知的 Garnet 对象类型字节: {0:#x}")]
  InvalidGarnetObjectType(u8),

  /// 非法或未知的自定义扩展对象类型
  #[error("非法或未知的自定义扩展对象类型字节: {0:#x}")]
  InvalidCustomObjectType(u8),

  /// 非法或未知的存储编码（持久化解码前向兼容：未知编码字节显式拒绝，
  /// 绝不静默折叠为 Compact——缺数据优于错数据）
  #[error("非法或未知的存储编码字节: {0:#x}")]
  InvalidStorageEncoding(u8),

  /// 紧凑集合容量溢出（超出 u16 上限 65535）
  #[error("紧凑集合元素数量超出 u16 上限: {0}")]
  CompactCountOverflow(usize),

  /// 紧凑编码数据损坏或格式非法
  #[error("紧凑编码数据损坏: {0}")]
  CorruptedCompactData(&'static str),

  /// 参数非法
  #[error("参数非法: {0}")]
  InvalidArgument(&'static str),

  /// 变长整型编码非规范或存在冗余/非法前缀
  #[error("变长整型编码非规范或存在冗余/非法前缀")]
  NonCanonicalEncoding,
}

/// 值层模块结果类型
pub type Result<T> = result::Result<T, Error>;
