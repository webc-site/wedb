use bitcode::{Decode, Encode};
use strum::{AsRefStr, Display, FromRepr, IntoStaticStr};

use crate::error::{Error, Result};

/// 基于 BfTree 磁盘块级有序存储引擎的专属物理键前缀标签枚举（定长 1 字节紧凑前缀）
///
/// 物理键排布格式：`[BfTag: 1B] + [payload]`
///
/// 键空间划分为：
/// - 业务有序数据区：0..=31（预留 32 个槽位，当前分配 ZMember=0, ZScore=1）
/// - 系统元数据区：32..=63（预留 32 个槽位，从 32 起分配系统内部元数据）
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
pub enum BfTag {
  // --- 业务有序数据区 (预留 32 个槽位: 0..=31) ---
  /// 有序集合成员索引 (0，物理键: `[0x00, key_id: 8B, version: 8B, member]`, Val: 8B be f64)
  ZMember = 0,
  /// 有序集合分值索引 (1，物理键: `[0x01, key_id: 8B, version: 8B, score: 8B, member]`, Val: empty)
  ZScore = 1,

  // --- 系统内部元数据区 (预留 32 个槽位: 32..=63) ---
  /// 命名空间自增持久化水位 (32，物理键: `[0x20]`, Val: 8B be u64)
  NextNamespace = 32,
  /// ACL 用户实体数据 (33，物理键: `[0x21, username bytes]`, Val: bitcode)
  AclUser = 33,
  /// ACL 用户计数元数据 (34，物理键: `[0x22]`, Val: 8B be u64)
  AclMeta = 34,
  /// 集群拓扑元数据 (35，物理键: `[0x23]`, Val: bitcode)
  ClusterMeta = 35,
  /// 复制位点元数据 (36，物理键: `[0x24]`, Val: 8B be u64)
  ReplMeta = 36,
}

impl BfTag {
  /// BfTree 物理键标签定长 1 字节
  pub const TAG_LEN: usize = 1;
  /// 业务有序数据标签上限 (0..=31 共 32 个槽位)
  pub const BUSINESS_TAG_MAX: u8 = 31;
  /// 系统元数据起始边界 (32..=63 共 32 个槽位)
  pub const SYSTEM_TAG_BASE: u8 = 32;  /// 栈分配键最大容量 (64 字节，对齐 L1 缓存行)
  pub const STACK_KEY_CAP: usize = 64;

  /// 从 1 字节整数解析 BfTree 标签 (const fn)
  #[inline(always)]
  pub const fn from_u8(val: u8) -> Option<Self> {
    Self::from_repr(val)
  }

  /// 转换为 1 字节数值 (const fn)
  #[inline(always)]
  pub const fn as_u8(self) -> u8 {
    self as u8
  }

  /// 转换为静态名称切片 (const fn)
  #[inline(always)]
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::ZMember => "ZMember",
      Self::ZScore => "ZScore",
      Self::NextNamespace => "NextNamespace",
      Self::AclUser => "AclUser",
      Self::AclMeta => "AclMeta",
      Self::ClusterMeta => "ClusterMeta",
      Self::ReplMeta => "ReplMeta",
    }
  }

  /// 生成 1 字节定长前缀数组 (const fn, 零堆分配)
  #[inline(always)]
  pub const fn prefix(self) -> [u8; Self::TAG_LEN] {
    [self as u8]
  }

  /// 判断是否属于业务有序数据标签 (0..=31 单指令高速判定)
  #[inline(always)]
  pub const fn is_business(self) -> bool {
    (self as u8) <= Self::BUSINESS_TAG_MAX
  }

  /// 判断是否属于系统内部元数据标签 (32..=63 单指令高速判定)
  #[inline(always)]
  pub const fn is_system(self) -> bool {
    (self as u8) >= Self::SYSTEM_TAG_BASE
  }

  /// 判断是否属于有序集合业务子键标签 (0, 1)
  #[inline(always)]
  pub const fn is_zset(self) -> bool {
    matches!(self, Self::ZMember | Self::ZScore)
  }

  /// 从完整物理键中剥离单字节标签，提取子标识切片 (const fn)
  #[inline(always)]
  pub const fn strip_prefix(self, key: &[u8]) -> Option<&[u8]> {
    match key {
      [first, rest @ ..] if *first == self as u8 => Some(rest),
      _ => None,
    }
  }
}

impl TryFrom<u8> for BfTag {
  type Error = Error;

  #[inline]
  fn try_from(val: u8) -> Result<Self> {
    Self::from_repr(val).ok_or(Error::InvalidKeyTag(val))
  }
}

impl From<BfTag> for u8 {
  #[inline(always)]
  fn from(tag: BfTag) -> Self {
    tag as Self
  }
}
