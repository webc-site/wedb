//! 会话域隔离前缀编解码（wedb 自有架构，C# 无对位）
//!
//! C# Garnet 无 namespace 概念，跨租户域无从谈起；wedb 多租户架构（SKILL：
//! 认证 `<ns>#用户名`、物理键 `[NsVarint]` 刚性隔离）要求进程级共享索引
//! （pub/sub 订阅表、阻塞族经纪观察表）同口径隔离。本模块把域折叠进索引键，
//! 表结构不改，隔离纯粹经前缀键达成。
//!
//! 编码为「ASCII 十进制数字 + 定界符」段序列（每段 `数字 + ':'`）：
//! - 为何不用存储域同款二进制 `[NsVarint]`：pub/sub 模式订阅广播走
//!   [`crate::glob::glob_match`] 匹配，前缀字节即模式串首段，二进制 ns 字节
//!   可能落入 glob 元字符（`*`/`?`/`[`/`\`）而污染匹配、造成跨 ns 误命中或
//!   本 ns 漏命中。十进制数字与定界符 `:` 均为 glob 字面安全字节。
//! - 为何无歧义：itoa 十进制规范无前导零，单 `:` 定界段边界，段序列自解释；
//!   不同域的折叠键互不前导碰撞，前缀判定退化为纯字节 [`slice::strip_prefix`]，
//!   无需反解域。
//!
//! 自研依据: doc/zh/db.md 会话存储前缀（ns#用户名 认证绑定）

use itoa::Buffer;

/// 域段定界符：非 glob 元字符、非数字字节，规范十进制数字后恰此一字节界定段边界
const NS_DELIM: u8 = b':';

/// 单段 `u64` 十进制最大位数（`18446744073709551615` 为 20 位）
const MAX_DIGITS: usize = 20;

/// 两段域（ns + db）的缓冲上界（每段数字 + 定界符）
const BUF_LEN: usize = MAX_DIGITS * 2 + 2;

/// 会话域隔离前缀（定长栈缓冲，覆盖两段 `u64` 域，`Copy` 零堆分配）
///
/// 由会话域单次构造、随命令复用，避免订阅/发布/注册循环内逐键重算编码。
/// 单段形态（[`Self::new`]）承接 pub/sub 通道域；阻塞族经纪观察域为
/// `(ns, db)` 两段，经 [`Self::join`] 追加库段（同库同名队列在共享观察表
/// 中同样不可串扰）。
#[derive(Clone, Copy, Debug)]
pub struct NsPrefix {
  buf: [u8; BUF_LEN],
  len: u8,
}

impl NsPrefix {
  /// 零长起点（内部构造基元）
  #[inline]
  const fn empty() -> Self {
    Self {
      buf: [0; BUF_LEN],
      len: 0,
    }
  }

  /// 追加一段域（数字 + 定界符）
  #[inline]
  fn push(mut self, n: u64) -> Self {
    let mut digits_buf = Buffer::new();
    let digits = digits_buf.format(n);
    let start = self.len as usize;
    let end = start + digits.len() + 1;
    self.buf[start..end - 1].copy_from_slice(digits.as_bytes());
    self.buf[end - 1] = NS_DELIM;
    self.len = end as u8;
    self
  }

  /// 单段域前缀（pub/sub 通道域形态：`[ns ':']`）
  #[inline]
  pub fn new(ns: u64) -> Self {
    Self::empty().push(ns)
  }

  /// 追加一段域（阻塞族经纪观察域形态：`[ns ':'][db ':']`）
  #[inline]
  pub fn join(&self, n: u64) -> Self {
    self.push(n)
  }

  /// 隔离前缀只读切片
  #[inline(always)]
  pub fn as_slice(&self) -> &[u8] {
    &self.buf[..self.len as usize]
  }

  /// 折叠裸键为隔离键：`[前缀] + [裸键]`
  #[inline]
  pub fn isolate(&self, raw: &[u8]) -> Vec<u8> {
    let prefix = self.as_slice();
    let mut key = Vec::with_capacity(prefix.len() + raw.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(raw);
    key
  }

  /// 从隔离键剥离本域前缀，还原用户视角裸键名；非本域键返回 `None`
  ///
  /// 规范十进制段 + 单定界符保证：不同域的折叠键互不为前导，[`strip_prefix`]
  /// 的命中即归属判定唯一无歧义。
  #[inline(always)]
  pub fn strip<'a>(&self, isolated: &'a [u8]) -> Option<&'a [u8]> {
    isolated.strip_prefix(self.as_slice())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn single_segment_roundtrip() {
    let p = NsPrefix::new(7);
    assert_eq!(p.as_slice(), b"7:");
    let iso = p.isolate(b"news");
    assert_eq!(iso, b"7:news");
    assert_eq!(p.strip(&iso), Some(&b"news"[..]));
    // 他域键不匹配本前缀
    let other = NsPrefix::new(42).isolate(b"news");
    assert_eq!(p.strip(&other), None);
  }

  #[test]
  fn joined_segments_roundtrip() {
    let p = NsPrefix::new(7).join(0);
    assert_eq!(p.as_slice(), b"7:0:");
    let iso = p.isolate(b"q");
    assert_eq!(iso, b"7:0:q");
    assert_eq!(p.strip(&iso), Some(&b"q"[..]));
    // 同 ns 不同 db 的折叠键互不命中
    let pdb = NsPrefix::new(7).join(1).isolate(b"q");
    assert_eq!(p.strip(&pdb), None);
    assert_eq!(NsPrefix::new(7).join(1).strip(&iso), None);
  }

  #[test]
  fn extreme_domains_are_unambiguous() {
    let (ns, db) = (u64::MAX, u64::MAX);
    let p = NsPrefix::new(ns).join(db);
    let iso = p.isolate(b"k");
    assert_eq!(iso.len(), 20 + 1 + 20 + 1 + 1);
    assert_eq!(p.strip(&iso), Some(&b"k"[..]));
    // 无前导零：跨段拼接不产生前导碰撞
    let p2 = NsPrefix::new(1).join(2);
    assert_eq!(p2.isolate(b"3"), b"1:2:3");
    assert_eq!(NsPrefix::new(12).strip(&p2.isolate(b"3")), None);
  }
}
