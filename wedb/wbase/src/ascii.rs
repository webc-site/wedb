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
