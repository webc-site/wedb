use bitcode::{Decode, Encode};
use strum::{AsRefStr, Display, FromRepr, IntoStaticStr};

use crate::error::{Error, Result};

/// 底层物理键命名空间标签枚举（1 字节紧凑前缀码）
///
/// 保证数学上前缀完全封闭，彻底杜绝跨类型命名空间污染与边界滑动冲突。
#[derive(
  Debug,
  Clone,
  Copy,
  PartialEq,
  Eq,
  Hash,
  PartialOrd,
  Ord,
  FromRepr,
  Display,
  AsRefStr,
  IntoStaticStr,
  Encode,
  Decode,
)]
#[repr(u8)]
pub enum KeyTag {
  /// 普通字符串键 (0x00)
  String = 0x00,
  /// 集合元数据记录 (0x01)
  Meta = 0x01,
  /// 哈希字段子键 (0x02，工业级别名 HashField)
  Hash = 0x02,
  /// 无序集合成员子键 (0x03，工业级别名 SetMember)
  Set = 0x03,
  /// 有序集合分块数据/成员子键 (0x04，工业级别名 ZMember)
  ZSetChunk = 0x04,
  /// 有序集合成员反查分值映射/分值索引子键 (0x05，工业级别名 ZScore)
  ZSetM2s = 0x05,
  /// 列表分块数据 (0x06)
  ListChunk = 0x06,
  /// 哈希分块字段索引 (0x07)
  HashChunk = 0x07,
  /// 无序集合分块成员索引 (0x08)
  SetChunk = 0x08,
  /// key 级 TTL 记录 (0x09)：key = 会话前缀 + 本标签 + 用户键，
  /// value = 8 字节大端 u64 绝对毫秒过期时间戳（独立旁路记录，避免双真值来源）
  Ttl = 0x09,
}

impl KeyTag {
  /// KeyTag 物理键标签定长 1 字节
  pub const TAG_LEN: usize = 1;

  /// 从 1 字节整数解析标签 (const fn)
  #[inline(always)]
  pub const fn from_u8(val: u8) -> Option<Self> {
    Self::from_repr(val)
  }

  /// 转换为 1 字节原始数值 (const fn)
  #[inline(always)]
  pub const fn as_u8(self) -> u8 {
    self as u8
  }

  /// 转换为静态字符串切片 (const fn)
  #[inline(always)]
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::String => "String",
      Self::Meta => "Meta",
      Self::Hash => "Hash",
      Self::Set => "Set",
      Self::ZSetChunk => "ZSetChunk",
      Self::ZSetM2s => "ZSetM2s",
      Self::ListChunk => "ListChunk",
      Self::HashChunk => "HashChunk",
      Self::SetChunk => "SetChunk",
      Self::Ttl => "Ttl",
    }
  }

  /// 生成 1 字节定长前缀数组 (const fn, 零堆分配)
  #[inline(always)]
  pub const fn prefix(self) -> [u8; Self::TAG_LEN] {
    [self as u8]
  }

  /// 从完整物理键中剥离单字节标签，提取子标识切片 (const fn)
  #[inline(always)]
  pub const fn strip_prefix(self, key: &[u8]) -> Option<&[u8]> {
    match key {
      [first, rest @ ..] if *first == self as u8 => Some(rest),
      _ => None,
    }
  }

  /// 判断是否为内部打平子键 (0x02..=0x08, const fn)
  ///
  /// 上界必须封闭到 SetChunk：Ttl (0x09) 是旁路记录（载荷为用户键原文），
  /// 绝不能被子键解码器按 key_id/version 头误解析
  #[inline(always)]
  pub const fn is_subkey(self) -> bool {
    let v = self as u8;
    v >= Self::Hash as u8 && v <= Self::SetChunk as u8
  }

  /// 判断是否为用户可见逻辑键 (String 或 Meta, const fn)
  #[inline(always)]
  pub const fn is_user_visible(self) -> bool {
    matches!(self, Self::String | Self::Meta)
  }
}

impl TryFrom<u8> for KeyTag {
  type Error = Error;

  #[inline]
  fn try_from(val: u8) -> Result<Self> {
    Self::from_repr(val).ok_or(Error::InvalidKeyTag(val))
  }
}

impl From<KeyTag> for u8 {
  #[inline(always)]
  fn from(tag: KeyTag) -> Self {
    tag as Self
  }
}

/// 集合逻辑数据结构类型（定长 1 字节，存储于 MetaRecord）
#[derive(
  Debug,
  Clone,
  Copy,
  PartialEq,
  Eq,
  Hash,
  PartialOrd,
  Ord,
  FromRepr,
  Default,
  Display,
  AsRefStr,
  IntoStaticStr,
  Encode,
  Decode,
)]
#[strum(serialize_all = "lowercase")]
#[repr(u8)]
pub enum CollectionType {
  /// 哈希对象 (1)
  #[default]
  Hash = 1,
  /// 无序集合 (2)
  Set = 2,
  /// 有序集合 (3)
  ZSet = 3,
  /// 列表 (4)
  List = 4,
  /// 范围索引 (5)
  RangeIndex = 5,
}

impl CollectionType {
  /// 从 1 字节整数解析集合类型 (const fn)
  #[inline(always)]
  pub const fn from_u8(val: u8) -> Option<Self> {
    Self::from_repr(val)
  }

  /// 转换为 1 字节原始数值
  #[inline(always)]
  pub const fn as_u8(self) -> u8 {
    self as u8
  }

  /// 转换为 Redis 规范小写类型字符串（如 "hash", "set", "zset", "list", "rangeindex"，const fn）
  #[inline(always)]
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::Hash => "hash",
      Self::Set => "set",
      Self::ZSet => "zset",
      Self::List => "list",
      Self::RangeIndex => "rangeindex",
    }
  }
}

impl TryFrom<u8> for CollectionType {
  type Error = Error;

  #[inline]
  fn try_from(val: u8) -> Result<Self> {
    Self::from_repr(val).ok_or(Error::InvalidCollectionType(val))
  }
}

impl From<CollectionType> for u8 {
  #[inline(always)]
  fn from(t: CollectionType) -> Self {
    t as Self
  }
}
