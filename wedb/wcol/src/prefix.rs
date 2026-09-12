//! 集合与范围索引统一树前缀 (TreePrefix)
//!
//! 物理阻断与数学级防穿透：
//! 所有在 BfTree 存储的子键统一前缀，字节序物理隔离，杜绝跨数据结构交叉穿透。

use std::{mem::MaybeUninit, ptr, slice};

/// BfTree 集合与索引统一前缀枚举
///
/// 严格采用单字节 0x01..=0x06，杜绝裸常量硬编码。
/// 值域与 `.agents/skills/transpile/SKILL.md` 单树多前缀物理隔离公理一致：
/// ZSetScore=0x01, ZSetMember=0x02, SetMember=0x03, ListIndex=0x04,
/// RangeIndexKey=0x05, HashField=0x06。
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TreePrefix {
  /// 有序集合分值排序索引 (0x01)
  ZSetScore = 0x01,
  /// 有序集合成员反查分值索引 (0x02)
  ZSetMember = 0x02,
  /// 无序集合成员索引 (0x03)
  SetMember = 0x03,
  /// 列表双端序号索引 (0x04)
  ListIndex = 0x04,
  /// 范围索引裸键 (0x05)
  RangeIndexKey = 0x05,
  /// 哈希字段索引 (0x06)
  HashField = 0x06,
}

/// 栈缓冲区阈值（1024 字节，对齐 L1 缓存，消除绝大多数键编码堆分配）
pub const STACK_KEY_BUF_SIZE: usize = 1024;

impl TreePrefix {
  /// 转换为 1 字节原始数值 (const fn)
  #[inline(always)]
  pub const fn as_u8(self) -> u8 {
    self as u8
  }

  /// 从 1 字节数值解析为统一前缀 (const fn)
  #[inline(always)]
  pub const fn from_u8(val: u8) -> Option<Self> {
    match val {
      0x01 => Some(Self::ZSetScore),
      0x02 => Some(Self::ZSetMember),
      0x03 => Some(Self::SetMember),
      0x04 => Some(Self::ListIndex),
      0x05 => Some(Self::RangeIndexKey),
      0x06 => Some(Self::HashField),
      _ => None,
    }
  }
}

impl From<TreePrefix> for u8 {
  #[inline(always)]
  fn from(prefix: TreePrefix) -> Self {
    prefix as Self
  }
}

impl TryFrom<u8> for TreePrefix {
  type Error = crate::CollectionError;

  #[inline]
  fn try_from(val: u8) -> Result<Self, Self::Error> {
    Self::from_u8(val).ok_or(crate::CollectionError::InvalidArgument("无效的树前缀"))
  }
}

/// 栈优先构造前缀单段组合键 `[prefix: 1B][sub_key]`
#[inline]
pub fn with_prefixed_key<R>(prefix: u8, sub_key: &[u8], f: impl FnOnce(&[u8]) -> R) -> R {
  let total_len = 1 + sub_key.len();
  if total_len <= STACK_KEY_BUF_SIZE {
    let mut buf = [MaybeUninit::<u8>::uninit(); STACK_KEY_BUF_SIZE];
    unsafe {
      let ptr = buf.as_mut_ptr() as *mut u8;
      *ptr = prefix;
      ptr::copy_nonoverlapping(sub_key.as_ptr(), ptr.add(1), sub_key.len());
      f(slice::from_raw_parts(ptr, total_len))
    }
  } else {
    let mut buf = Vec::with_capacity(total_len);
    buf.push(prefix);
    buf.extend_from_slice(sub_key);
    f(&buf)
  }
}

/// 栈优先构造前缀双段组合键 `[prefix: 1B][part1][part2]`
#[inline]
pub fn with_prefixed_key2<R>(
  prefix: u8,
  part1: &[u8],
  part2: &[u8],
  f: impl FnOnce(&[u8]) -> R,
) -> R {
  let total_len = 1 + part1.len() + part2.len();
  if total_len <= STACK_KEY_BUF_SIZE {
    let mut buf = [MaybeUninit::<u8>::uninit(); STACK_KEY_BUF_SIZE];
    unsafe {
      let ptr = buf.as_mut_ptr() as *mut u8;
      *ptr = prefix;
      ptr::copy_nonoverlapping(part1.as_ptr(), ptr.add(1), part1.len());
      ptr::copy_nonoverlapping(part2.as_ptr(), ptr.add(1 + part1.len()), part2.len());
      f(slice::from_raw_parts(ptr, total_len))
    }
  } else {
    let mut buf = Vec::with_capacity(total_len);
    buf.push(prefix);
    buf.extend_from_slice(part1);
    buf.extend_from_slice(part2);
    f(&buf)
  }
}
