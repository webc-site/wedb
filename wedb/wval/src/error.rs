use std::result;

use thiserror::Error;

/// 值层编解码错误枚举（子键命名空间 / 集合元数据 / 紧凑容器）
///
/// 全变体仅承载纯数据（零 drop 胶水、可 Copy），以支撑本 crate 编解码函数
/// 全量 const 化——编译期求值契约由 tests/meta_and_subkey.rs 的 `const` 断言
/// 显式锁定。bitcode::Error 含堆载荷（debug 构建为 `Cow<'static, str>`，
/// 带 drop 胶水），并入本枚举将使全体 const fn 陷入 E0493（常量求值禁止
/// drop），故独立为 [`BitcodeError`] 承载，见其文档。
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

/// bitcode 编解码错误（保留原始错误链）
///
/// 对齐 wcpr 的保留策略：Display 透出底层错误信息，`source()` 亦指向原始
/// `bitcode::Error`，不做字符串化有损降级，调用方可继续向下解构错误链。
/// 仅由非 const 的 bitcode 路径（[`crate::meta::MetaValue::decode_bitcode`]
/// 等）返回；不提供 `From<BitcodeError> for Error` 的降级转换，杜绝错误链
/// 在传播途中静默丢失。
#[derive(Error, Debug)]
#[error("bitcode 编解码失败: {0}")]
pub struct BitcodeError(#[from] bitcode::Error);

/// 值层模块结果类型
pub type Result<T> = result::Result<T, Error>;

/// bitcode 路径结果类型（错误为保留原始错误链的 [`BitcodeError`]）
pub type BitcodeResult<T> = result::Result<T, BitcodeError>;
