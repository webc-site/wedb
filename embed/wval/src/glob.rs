pub use wbase::glob::{glob_match, glob_match_nocase, glob_match_opt};

#[cfg(test)]
mod tests {
  use super::{glob_match, glob_match_nocase, glob_match_opt};

  #[test]
  fn test_glob_basic() {
    // C# GlobUtils 入口条件：目标为空仅空模式命中
    assert!(!glob_match(b"*", b""));
    assert!(glob_match(b"*", b"hello"));
    assert!(glob_match(b"", b""));
    assert!(!glob_match(b"", b"a"));
    assert!(glob_match(b"h?llo", b"hello"));
    assert!(!glob_match(b"h?llo", b"hllo"));
  }

  #[test]
  fn test_glob_brackets_and_escapes() {
    assert!(glob_match(b"[a-z]ello", b"hello"));
    assert!(!glob_match(b"[0-9]ello", b"hello"));
    // '!' 非取反符，属字面集合成员：集合为 {'!', '0'-'9'}
    assert!(!glob_match(b"[!0-9]ello", b"hello"));
    assert!(glob_match(b"[!0-9]ello", b"!ello"));
    assert!(glob_match(b"[^0-9]ello", b"hello"));
    assert!(glob_match(b"[\\]]", b"]"));
    assert!(glob_match(b"[\\\\\\\\]", b"\\"));
    assert!(glob_match(b"\\*hello", b"*hello"));
    assert!(!glob_match(b"\\*hello", b"foo_hello"));
    // C# 区间分支：`a-]` 的 ']' 为区间上界且不终止集合，集合为 {']'..'a'}
    assert!(glob_match(b"[a-]", b"]"));
    assert!(glob_match(b"[a-]", b"a"));
    assert!(!glob_match(b"[a-]", b"b"));
    // 区间后仍有成员：继续扫描真实 ']'，集合 {']'..'a'} ∪ {'x'} 一次消费整个模式串
    assert!(glob_match(b"[a-]x]", b"x"));
    assert!(glob_match(b"[a-]x]", b"]"));
    assert!(!glob_match(b"[a-]x]", b"]x"));
    assert!(glob_match(b"[z-a]", b"m"));
    assert!(glob_match(b"[abc", b"a"));
    assert!(!glob_match(b"[abc", b"d"));
    assert!(!glob_match(b"[", b"["));
  }

  #[test]
  fn test_glob_nocase() {
    assert!(!glob_match(b"hello", b"HELLO"));
    assert!(glob_match_nocase(b"hello", b"HELLO"));
    assert!(glob_match_opt(b"h[a-z]llo", b"hEllo", true));
    // C# ignoreCase 语义：先按原始字节交换端点再小写化、不回退排序，
    // [k-M] 小写化后为逆序区间 m..k，恒不匹配（大小写敏感路径 [z-a] 则正常匹配）
    assert!(!glob_match_nocase(b"[k-M]ello", b"lello"));
    assert!(!glob_match_nocase(b"[Z-a]ello", b"mello"));
    assert!(!glob_match_nocase(b"[a-Z]ello", b"mello"));
  }
}
