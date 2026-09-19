//! pub/sub 通道 namespace 隔离键编解码（wedb 自有架构，C# 无对位）
//!
//! C# Garnet 无 namespace 概念，[`crate::subscribe_broker`] 与 [`crate::session_commands`]
//! 本就裸 channel 收发；wedb 多租户架构（SKILL：认证 `<ns>#用户名`、物理键 `[NsVarint]`
//! 刚性隔离）要求消息域同口径隔离。本模块在会话侧把 ns 折叠进通道键，broker 表结构不改，
//! 隔离纯粹经前缀键达成。
//!
//! 为何采用「ASCII 十进制数字 + 定界符」而非存储域同款二进制 `[NsVarint]`：模式订阅广播走
//! [`wbase::glob::glob_match`] 匹配，前缀字节即模式串首段，二进制 ns 字节可能落入 glob 元字符
//! （`*`/`?`/`[`/`\`）而污染匹配、造成跨 ns 误命中或本 ns 漏命中。十进制数字与定界符 `:` 均为
//! glob 字面安全字节，且数字规范无歧义、单个 `:` 唯一定界前缀与裸通道的边界，前缀判定退化为
//! 纯字节 [`slice::strip_prefix`]，无需反解 ns。

use itoa::Buffer;

/// ns 隔离前缀定界符：非 glob 元字符、非数字字节，规范十进制数字后恰此一字节界定前缀边界
const NS_DELIM: u8 = b':';

/// `u64` 十进制最大位数（`18446744073709551615` 为 20 位）
const MAX_NS_DIGITS: usize = 20;

/// 会话 ns 对应的通道隔离前缀（定长栈缓冲，覆盖 ns 0..=u64::MAX）
///
/// 由会话 ns 单次构造、随命令复用，避免订阅/发布循环内逐通道重算编码。
#[derive(Clone, Copy, Debug)]
pub struct ChannelNsPrefix {
  buf: [u8; MAX_NS_DIGITS + 1],
  len: u8,
}

impl ChannelNsPrefix {
  /// 编码命名空间为隔离前缀（十进制数字 + 定界符）
  #[inline]
  pub fn new(ns: u64) -> Self {
    let mut digits_buf = Buffer::new();
    let digits = digits_buf.format(ns);
    let mut buf = [0u8; MAX_NS_DIGITS + 1];
    let len = digits.len();
    buf[..len].copy_from_slice(digits.as_bytes());
    buf[len] = NS_DELIM;
    Self {
      buf,
      len: (len + 1) as u8,
    }
  }

  /// 隔离前缀只读切片
  #[inline(always)]
  pub fn as_slice(&self) -> &[u8] {
    &self.buf[..self.len as usize]
  }

  /// 折叠裸通道为 broker 隔离键：`[数字前缀 + 定界符] + [裸通道]`
  #[inline]
  pub fn isolate(&self, raw: &[u8]) -> Vec<u8> {
    let prefix = self.as_slice();
    let mut key = Vec::with_capacity(prefix.len() + raw.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(raw);
    key
  }

  /// 从隔离键剥离本会话前缀，还原用户视角裸通道名；非本 ns 键返回 `None`
  ///
  /// 规范十进制前缀 + 单定界符保证：不同 ns 的隔离键必不以前缀互为前导，故 [`strip_prefix`]
  /// 的命中即归属判定唯一无歧义。
  #[inline(always)]
  pub fn strip<'a>(&self, isolated: &'a [u8]) -> Option<&'a [u8]> {
    isolated.strip_prefix(self.as_slice())
  }
}

#[cfg(test)]
mod tests {
  use wbase::glob::glob_match;

  use super::*;

  #[test]
  fn prefix_is_stripped_symmetrically() {
    let p = ChannelNsPrefix::new(7);
    let iso = p.isolate(b"news");
    assert_eq!(iso, b"7:news");
    assert_eq!(p.strip(&iso), Some(&b"news"[..]));
    // 他 ns 键不匹配本前缀
    let other = ChannelNsPrefix::new(42).isolate(b"news");
    assert_eq!(p.strip(&other), None);
  }

  #[test]
  fn prefix_is_glob_safe_and_unambiguous() {
    // 数字与定界符均非 glob 元字符：前缀段按字面匹配
    let p42 = ChannelNsPrefix::new(42);
    let pat = p42.isolate(b"news.*");
    // 同 ns 命中
    assert!(glob_match(&pat, &p42.isolate(b"news.tech")));
    // 跨 ns 不命中（含数字前导歧义：ns 4 的 "2x" 与 ns 42 的 "x"）
    assert!(!glob_match(
      &pat,
      &ChannelNsPrefix::new(4).isolate(b"2news.tech")
    ));
    assert!(!glob_match(&pat, &ChannelNsPrefix::new(4).isolate(b"2x")));
    assert!(!glob_match(
      &ChannelNsPrefix::new(4).isolate(b"2x"),
      &p42.isolate(b"x")
    ));
  }
}
