//! 密钥材料共享工具（对标 libs/server/ACL/SecretsUtility.cs）

/// 常量时间字节比较（O(n)，不因提前退出泄露前缀信息）
///
/// libs/server/ACL/SecretsUtility.cs:ConstantEquals
#[inline]
pub fn constant_equals(a: &[u8], b: &[u8]) -> bool {
  // 长度不等直接 false（与 C# FixedTimeEquals 一致：长度本身不属于秘密）
  if a.len() != b.len() {
    return false;
  }
  let mut diff = 0u8;
  for (x, y) in a.iter().zip(b.iter()) {
    diff |= x ^ y;
  }
  diff == 0
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn matches_only_on_full_equality() {
    assert!(constant_equals(b"abc", b"abc"));
    assert!(!constant_equals(b"abc", b"abd"));
    assert!(!constant_equals(b"abc", b"ab"));
    assert!(constant_equals(b"", b""));
  }
}
