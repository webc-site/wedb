//! pub/sub 通道 namespace 隔离键（wedb 自有架构，C# 无对位）
//!
//! C# Garnet 无 namespace 概念，[`crate::subscribe_broker`] 与 [`crate::session_commands`]
//! 本就裸 channel 收发；wedb 多租户架构（SKILL：认证 `<ns>#用户名`、物理键 `[NsVarint]`
//! 刚性隔离）要求消息域同口径隔离。本模块在会话侧把 ns 折叠进通道键，broker 表结构不改，
//! 隔离纯粹经前缀键达成。
//!
//! 编解码单源已收敛 [`wbase::ns_prefix`]（阻塞族经纪观察域同源复用，二进制
//! `[NsVarint]` 弃用缘由见彼处模块文档——glob 字面安全与段无歧义）；本模块仅保留
//! 通道域的转发别名与 glob 交互语义测试。

pub use wbase::ns_prefix::NsPrefix as ChannelNsPrefix;

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
