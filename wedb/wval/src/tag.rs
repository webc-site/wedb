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
)]
#[repr(u8)]
pub enum KeyTag {
  /// 普通字符串键 (0x00)
  String = 0x00,
  /// 集合元数据记录 (0x01)
  Meta = 0x01,
  /// key 级 TTL 记录 (0x09)：key = 会话前缀 + 本标签 + 用户键，
  /// value = 8 字节大端 i64 绝对 .NET Ticks 过期时间戳
  /// （[`crate::codec::I64Codec`] 编解码；独立旁路记录，避免双真值来源）
  Ttl = 0x09,
  /// 向量索引与图拓扑物理子键 (0x0A)
  Vector = 0x0A,
  /// key 级 ETag 记录 (0x0B)：key = 会话前缀 + 本标签 + 用户键，
  /// value = 8 字节大端 i64 etag（[`crate::codec::I64Codec`] 编解码）。
  /// 对标 C# Tsavorite LogRecord 记录尾可选 ETag 字段（LogRecord.cs:ETagSize /
  /// NoETag = 0）：Rust 记录头无可选字段位，仿 KeyTag::Ttl 旁路记录实现
  /// 等价生命周期（键删除即随删，普通 SET 覆写保留），键生命周期内唯一真值源
  Etag = 0x0B,
  /// 集合对象信封记录 (0x0C)：key = 会话前缀 + 本标签 + 用户键，
  /// value = `[1B GarnetObjectType 标签][bitcode 载荷]`（内层标签区分子类型）。
  /// 对标 C# UnifiedStore LogRecord.DataHeader.ValueIsObject 带外位
  ///（libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:HandleObjectEncoding）：
  /// 对象与用户字符串共用用户键空间，二者以物理键标签带外区分，
  /// 值内容任意（用户 SET "\x01..." 不再被误判为对象）
  ObjectEnvelope = 0x0C,
  /// ACL 用户规则记录 (0x0D)：key = 会话前缀 + 本标签 + 用户名，
  /// 权威持久化存储 ACL 规则描述
  Acl = 0x0D,
  /// 虚拟数据库元数据记录 (0x0E)
  DbMeta = 0x0E,
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
      Self::Ttl => "Ttl",
      Self::Vector => "Vector",
      Self::Etag => "Etag",
      Self::ObjectEnvelope => "ObjectEnvelope",
      Self::Acl => "Acl",
      Self::DbMeta => "DbMeta",
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

  /// 判断是否为用户可见逻辑键 (String / Meta / ObjectEnvelope, const fn)
  #[inline(always)]
  pub const fn is_user_visible(self) -> bool {
    matches!(self, Self::String | Self::Meta | Self::ObjectEnvelope)
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

/// 原生保留类型最大编号（Garnet 内置对象类型保留段上限 [0, 0x3F]）
///
/// 对标 C# `GarnetObjectTypeExtensions.LastReservedBuiltinType = (GarnetObjectType)0x3F`
/// （libs/server/Objects/Types/GarnetObjectType.cs:66）。
pub const LAST_RESERVED_BUILTIN_TYPE: u8 = 0x3F;

/// 自定义扩展对象起始编号（0x40）
///
/// 对标 C# `CustomCommandManager.CustomObjectTypeMinId = (byte)LastReservedBuiltinType + 1`
/// （libs/server/Custom/CustomCommandManager.cs:29）。
pub const CUSTOM_OBJECT_TYPE_BASE: u8 = LAST_RESERVED_BUILTIN_TYPE + 1;

/// Garnet 全局统一对象与集合类型枚举
///
/// 对标 C# libs/server/Objects/Types/GarnetObjectType.cs（该枚举只有 Null=0、SortedSet=1、
/// List=2、Hash=3、Set=4、All=0xfb 六个成员），其上叠加本仓扩展 RangeIndex=5：
/// 该成员系 transpile SKILL「类型枚举」条款明文规定的自定义项，不是对 C# 的复刻，
/// 因此本枚举与 C# 并非逐成员 1:1——去 C# 枚举里找 RangeIndex 会一无所获。
/// C# 侧 RangeIndex 不是对象类型成员而是独立存储形态（RangeIndexManager 的 RangeIndexRecordType），
/// 其 TYPE 回显由统一存储读侧对该记录类型的特判给出（ReadMethods.cs 的 HandleType，
/// 回显 CmdStrings.rangeindext），与 rust 的 rangeindex 回显口径一致，无行为分叉；
/// SCAN TYPE 过滤值仍只认 C# 同源的 zset/list/set/hash/string 五值，不含 rangeindex。
///
/// 权威强类型定义：Null=0, SortedSet=1, List=2, Hash=3, Set=4, RangeIndex=5, All=0xfb。
/// 全链路信封编解码、存储元记录类型判断与 WRONGTYPE 报错统一使用该枚举。
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
)]
#[strum(serialize_all = "lowercase")]
#[repr(u8)]
pub enum GarnetObjectType {
  /// 空对象 (0)
  #[default]
  Null = 0,
  /// 有序集合 (1, 兼容 Redis "zset")
  #[strum(serialize = "zset")]
  SortedSet = 1,
  /// 列表 (2, 兼容 Redis "list")
  #[strum(serialize = "list")]
  List = 2,
  /// 哈希对象 (3, 兼容 Redis "hash")
  #[strum(serialize = "hash")]
  Hash = 3,
  /// 无序集合 (4, 兼容 Redis "set")
  #[strum(serialize = "set")]
  Set = 4,
  /// 范围索引 (5, 树化有序索引 "rangeindex")
  #[strum(serialize = "rangeindex")]
  RangeIndex = 5,
  /// C# 原生保留的泛型扫描哨兵（COSCAN 通配任意对象类型，0xfb）
  #[strum(serialize = "all")]
  All = 0xfb,
}

impl GarnetObjectType {
  /// 从 1 字节整数解析集合与对象类型 (const fn)
  #[inline(always)]
  pub const fn from_u8(val: u8) -> Option<Self> {
    Self::from_repr(val)
  }

  /// 转换为 1 字节原始数值
  #[inline(always)]
  pub const fn as_u8(self) -> u8 {
    self as u8
  }

  /// 转换为 Redis 规范小写类型字符串（如 "zset", "list", "hash", "set", "rangeindex", "all"，const fn）
  #[inline(always)]
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::Null => "null",
      Self::SortedSet => "zset",
      Self::List => "list",
      Self::Hash => "hash",
      Self::Set => "set",
      Self::RangeIndex => "rangeindex",
      Self::All => "all",
    }
  }
}

impl TryFrom<u8> for GarnetObjectType {
  type Error = Error;

  #[inline]
  fn try_from(val: u8) -> Result<Self> {
    Self::from_repr(val).ok_or(Error::InvalidGarnetObjectType(val))
  }
}

impl From<GarnetObjectType> for u8 {
  #[inline(always)]
  fn from(t: GarnetObjectType) -> Self {
    t as Self
  }
}

/// 自定义扩展对象类型枚举（信封标签分配单点，起自 [`CUSTOM_OBJECT_TYPE_BASE`]）
///
/// 对标 C# CustomCommandManager.cs:406 `(GarnetObjectType)(CustomObjectTypeMinId + id)`
/// 的集中分配形态：扩展对象标签由本枚举一处分配，扩展 crate 经
/// [`CustomObjectType::as_u8`] 取值，严禁 `CUSTOM_OBJECT_TYPE_BASE + n`
/// 裸偏移散落（C# 模块只注册不自拣数字，转写后由枚举变体序承接）。
/// TYPE 注册名与执行体等描述面见 wcustom 静态描述清单项。
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
)]
#[repr(u8)]
pub enum CustomObjectType {
  /// Roaring 位图 (0x40，首个扩展槽位；对标 RoaringBitmapModule 注册序)
  Roaring = CUSTOM_OBJECT_TYPE_BASE,
  /// JSON 对象 (0x41，次一扩展槽位；对标 GarnetJSON 注册序)
  Json = CUSTOM_OBJECT_TYPE_BASE + 1,
}

impl CustomObjectType {
  /// 从 1 字节整数解析扩展对象类型 (const fn)
  #[inline(always)]
  pub const fn from_u8(val: u8) -> Option<Self> {
    Self::from_repr(val)
  }

  /// 转换为 1 字节信封标签 (const fn)
  #[inline(always)]
  pub const fn as_u8(self) -> u8 {
    self as u8
  }
}

impl TryFrom<u8> for CustomObjectType {
  type Error = Error;

  #[inline]
  fn try_from(val: u8) -> Result<Self> {
    Self::from_repr(val).ok_or(Error::InvalidCustomObjectType(val))
  }
}

impl From<CustomObjectType> for u8 {
  #[inline(always)]
  fn from(t: CustomObjectType) -> Self {
    t as Self
  }
}
