/// garnet/libs/common/AsciiUtils.cs:IsBetween
pub fn is_between(c: u8, min_inclusive: u8, max_inclusive: u8) -> bool {
  (c.wrapping_sub(min_inclusive) as u32) <= ((max_inclusive.wrapping_sub(min_inclusive)) as u32)
}

/// garnet/libs/common/AsciiUtils.cs:ToLower
pub fn to_lower(mut c: u8) -> u8 {
  if is_between(c, b'A', b'Z') {
    c |= 0x20;
  }
  c
}

/// garnet/libs/common/AsciiUtils.cs:ToUpper
pub fn to_upper(mut c: u8) -> u8 {
  if is_between(c, b'a', b'z') {
    c &= !0x20;
  }
  c
}

/// garnet/libs/common/AsciiUtils.cs:ToUpperInPlace
pub fn to_upper_in_place(command: &mut [u8]) {
  command.make_ascii_uppercase();
}

/// garnet/libs/common/AsciiUtils.cs:ToLowerInPlace
pub fn to_lower_in_place(command: &mut [u8]) {
  command.make_ascii_lowercase();
}

/// garnet/libs/common/AsciiUtils.cs:EqualsUpperCaseSpanIgnoringCase
pub fn equals_upper_case_span_ignoring_case(left: &[u8], right: &[u8]) -> bool {
  if left == right {
    return true;
  }
  if left.len() != right.len() {
    return false;
  }
  for (i, &b1) in left.iter().enumerate() {
    let b2 = right[i];
    if b1 == b2 || b1.wrapping_sub(32) == b2 {
      continue;
    }
    return false;
  }
  true
}

/// garnet/libs/common/AsciiUtils.cs:EqualsUpperCaseSpanIgnoringCase
pub fn equals_upper_case_span_ignoring_case_with_non_alphabetic(
  left: &[u8],
  right: &[u8],
  allow_non_alphabetic_chars: bool,
) -> bool {
  if left == right {
    return true;
  }
  if left.len() != right.len() {
    return false;
  }
  for (i, &b1) in left.iter().enumerate() {
    let b2 = right[i];
    if (65..=90).contains(&b2) {
      if b1 != b2 && b1.wrapping_sub(32) != b2 {
        return false;
      }
    } else if !allow_non_alphabetic_chars || b1 != b2 {
      return false;
    }
  }
  true
}

/// garnet/libs/common/AsciiUtils.cs:EqualsLowerCaseSpanIgnoringCase
pub fn equals_lower_case_span_ignoring_case(
  left: &[u8],
  right: &[u8],
  allow_non_alphabetic_chars: bool,
) -> bool {
  if left == right {
    return true;
  }
  if left.len() != right.len() {
    return false;
  }
  for (i, &b1) in left.iter().enumerate() {
    let b2 = right[i];
    if (97..=122).contains(&b2) {
      if b1 != b2 && b1.wrapping_add(32) != b2 {
        return false;
      }
    } else if !allow_non_alphabetic_chars || b1 != b2 {
      return false;
    }
  }
  true
}
