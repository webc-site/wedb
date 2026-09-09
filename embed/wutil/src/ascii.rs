/// garnet/libs/common/AsciiUtils.cs:IsBetween
#[inline(always)]
pub fn is_between(c: u8, min_inclusive: u8, max_inclusive: u8) -> bool {
  (c.wrapping_sub(min_inclusive) as u32) <= ((max_inclusive.wrapping_sub(min_inclusive)) as u32)
}

/// garnet/libs/common/AsciiUtils.cs:ToLower
#[inline]
pub fn to_lower(mut c: u8) -> u8 {
  if is_between(c, b'A', b'Z') {
    c |= 0x20;
  }
  c
}

/// garnet/libs/common/AsciiUtils.cs:ToUpper
#[inline]
pub fn to_upper(mut c: u8) -> u8 {
  if is_between(c, b'a', b'z') {
    c &= !0x20;
  }
  c
}

/// garnet/libs/common/AsciiUtils.cs:ToUpperInPlace
#[inline]
pub fn to_upper_in_place(command: &mut [u8]) {
  command.make_ascii_uppercase();
}

/// garnet/libs/common/AsciiUtils.cs:ToLowerInPlace
#[inline]
pub fn to_lower_in_place(command: &mut [u8]) {
  command.make_ascii_lowercase();
}

/// garnet/libs/common/AsciiUtils.cs:EqualsUpperCaseSpanIgnoringCase
///
/// 语义注意：调用方约定 `left` 已为大写形式；对任意 `b2`（含非字母）均允许
/// `b1.wrapping_sub(32) == b2` 的小写映射，与 C# 逐字节对齐，勿与下方内核混同
pub fn equals_upper_case_span_ignoring_case(left: &[u8], right: &[u8]) -> bool {
  if left == right {
    return true;
  }
  if left.len() != right.len() {
    return false;
  }
  // 双串已判等长，zip 消除右串边界检查
  left
    .iter()
    .zip(right.iter())
    .all(|(&b1, &b2)| b1 == b2 || b1.wrapping_sub(32) == b2)
}

/// 大小写折叠比较共享内核
///
/// `base`：目标字母区间首字节（'A' 或 'a'），`delta`：`b1` 折叠至 `b2` 的有符号步长
/// （上区间版本 -32 表示 left 小写视同大写，下区间版本 +32 表示 left 大写视同小写）。
/// 与 C# 语义逐字节对齐：非字母目标仅在 `allow_non_alphabetic_chars = true` 时要求严格相等
#[inline(always)]
fn equals_case_span_kernel(
  left: &[u8],
  right: &[u8],
  allow_non_alphabetic_chars: bool,
  base: u8,
  delta: u8,
) -> bool {
  if left == right {
    return true;
  }
  if left.len() != right.len() {
    return false;
  }
  // 双串已判等长，zip 消除右串边界检查；wrapping_add 使 ±32 统一为同一加法路径
  for (&b1, &b2) in left.iter().zip(right.iter()) {
    if is_between(b2, base, base + 25) {
      if b1 != b2 && b1.wrapping_add(delta) != b2 {
        return false;
      }
    } else if !allow_non_alphabetic_chars || b1 != b2 {
      return false;
    }
  }
  true
}

/// garnet/libs/common/AsciiUtils.cs:EqualsUpperCaseSpanIgnoringCase
pub fn equals_upper_case_span_ignoring_case_with_non_alphabetic(
  left: &[u8],
  right: &[u8],
  allow_non_alphabetic_chars: bool,
) -> bool {
  // delta = -32 (wrapping) => left 小写视同大写
  equals_case_span_kernel(left, right, allow_non_alphabetic_chars, b'A', u8::MAX - 31)
}

/// garnet/libs/common/AsciiUtils.cs:EqualsLowerCaseSpanIgnoringCase
pub fn equals_lower_case_span_ignoring_case(
  left: &[u8],
  right: &[u8],
  allow_non_alphabetic_chars: bool,
) -> bool {
  // delta = +32 => left 大写视同小写
  equals_case_span_kernel(left, right, allow_non_alphabetic_chars, b'a', 32)
}
