/// 配置参数名（原始字节）的 ASCII 大小写不敏感比较与哈希
/// （对标 libs/server/Config/ConfigNameComparer.cs:ConfigNameComparer）。
///
/// 无状态：比较、哈希均为纯函数，C# 的 `Instance` 单例在此退化为关联函数。
pub struct ConfigNameComparer;

impl ConfigNameComparer {
  /// libs/server/Config/ConfigNameComparer.cs:Equals
  ///
  /// 按字节逐位比较两个参数名，忽略 ASCII 大小写。
  pub fn equals(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
      && left
        .iter()
        .zip(right.iter())
        .all(|(&l, &r)| Self::to_upper_ascii(l) == Self::to_upper_ascii(r))
  }

  /// libs/server/Config/ConfigNameComparer.cs:GetHashCode
  ///
  /// 大小写不敏感哈希：`hash = hash * 31 + upper(b)`（含 C# 的 unchecked 溢出回绕）。
  pub fn hash_code(key: &[u8]) -> i32 {
    let mut hash: i32 = 17;
    for &b in key {
      hash = hash
        .wrapping_mul(31)
        .wrapping_add(i32::from(Self::to_upper_ascii(b)));
    }
    hash
  }

  /// libs/server/Config/ConfigNameComparer.cs:ToUpperAscii
  ///
  /// 小写 ASCII 字母转大写，其余字节原样返回。
  #[inline]
  pub fn to_upper_ascii(value: u8) -> u8 {
    if value.is_ascii_lowercase() {
      value - b'a' + b'A'
    } else {
      value
    }
  }
}
