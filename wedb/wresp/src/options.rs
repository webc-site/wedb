//! RESP 协议通用命令选项与词法解析（对标 Garnet.common 与 Garnet.server:RespEnums）
//!
//! 集中统一定义 Redis 命令通用选项枚举，提供零分配、无分支/最少分支的快速词法解析。

use bitflags::bitflags;

/// 比较字节切片是否相等（忽略 ASCII 大小写）
///
/// 对标 Garnet CmdStrings.EqualsUpperCaseSpanIgnoringCase
#[inline]
pub fn equals_ignore_case(a: &[u8], b: &[u8]) -> bool {
  a.eq_ignore_ascii_case(b)
}

bitflags! {
  /// ZADD 修饰选项（对标 Garnet.common/Objects/SortedSet:SortedSetAddOption）
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
  pub struct SortedSetAddOption: u8 {
    /// 无选项
    const NONE = 0;
    const None = 0;
    /// 仅更新已存在元素
    const XX = 1;
    const Xx = 1;
    /// 仅新增不存在元素
    const NX = 1 << 1;
    const Nx = 1 << 1;
    /// 新分值小于当前分值才更新
    const LT = 1 << 2;
    const Lt = 1 << 2;
    /// 新分值大于当前分值才更新
    const GT = 1 << 3;
    const Gt = 1 << 3;
    /// 返回值改为"新增+变更"总数
    const CH = 1 << 4;
    const Ch = 1 << 4;
    /// ZADD 退化为 ZINCRBY，仅允许单对 score-element
    const INCR = 1 << 5;
    const Incr = 1 << 5;
  }
}

/// 解析 ZADD 选项词元（XX/NX/LT/GT/CH/INCR）
#[inline]
pub fn try_get_sorted_set_add_option(v: &[u8]) -> Option<SortedSetAddOption> {
  match v.len() {
    2 => {
      let b0 = v[0].to_ascii_uppercase();
      let b1 = v[1].to_ascii_uppercase();
      match (b0, b1) {
        (b'X', b'X') => Some(SortedSetAddOption::XX),
        (b'N', b'X') => Some(SortedSetAddOption::NX),
        (b'L', b'T') => Some(SortedSetAddOption::LT),
        (b'G', b'T') => Some(SortedSetAddOption::GT),
        (b'C', b'H') => Some(SortedSetAddOption::CH),
        _ => None,
      }
    }
    4 => {
      let b0 = v[0].to_ascii_uppercase();
      let b1 = v[1].to_ascii_uppercase();
      let b2 = v[2].to_ascii_uppercase();
      let b3 = v[3].to_ascii_uppercase();
      if (b0, b1, b2, b3) == (b'I', b'N', b'C', b'R') {
        Some(SortedSetAddOption::INCR)
      } else {
        None
      }
    }
    _ => None,
  }
}

bitflags! {
  /// 过期条件选项（NX/XX/GT/LT；对标 Garnet.server:ExpireOption）
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
  pub struct ExpireOption: u8 {
    /// 无条件
    const NONE = 0;
    const None = 0;
    /// 仅当无既有过期时设置
    const NX = 1 << 0;
    const Nx = 1 << 0;
    /// 仅当有既有过期时设置
    const XX = 1 << 1;
    const Xx = 1 << 1;
    /// 仅当新过期晚于当前时设置
    const GT = 1 << 2;
    const Gt = 1 << 2;
    /// 仅当新过期早于当前时设置
    const LT = 1 << 3;
    const Lt = 1 << 3;
    /// 既有且更大
    const XXGT = Self::XX.bits() | Self::GT.bits();
    /// 既有且更小
    const XXLT = Self::XX.bits() | Self::LT.bits();
  }
}

/// 解析过期选项词元（NX/XX/GT/LT）
#[inline]
pub fn try_get_expire_option(v: &[u8]) -> Option<ExpireOption> {
  if v.len() != 2 {
    return None;
  }
  let b0 = v[0].to_ascii_uppercase();
  let b1 = v[1].to_ascii_uppercase();
  match (b0, b1) {
    (b'N', b'X') => Some(ExpireOption::NX),
    (b'X', b'X') => Some(ExpireOption::XX),
    (b'G', b'T') => Some(ExpireOption::GT),
    (b'L', b'T') => Some(ExpireOption::LT),
    _ => None,
  }
}

/// 从单个 token 解析过期选项（等价于 [`try_get_expire_option`]）
#[inline]
pub fn expire_option_from_token(arg: &[u8]) -> Option<ExpireOption> {
  try_get_expire_option(arg)
}

/// SET/EXPIRE 过期形式（对标 Garnet.server:ExpirationOption）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ExpirationOption {
  /// 无
  #[default]
  None,
  /// 秒相对
  Ex,
  /// 毫秒相对
  Px,
  /// 秒绝对
  Exat,
  /// 毫秒绝对
  Pxat,
  /// 保留 TTL
  Keepttl,
}

/// 解析过期形式（EX/PX/EXAT/PXAT/KEEPTTL）
#[inline]
pub fn try_get_expiration_option(token: &[u8]) -> Option<ExpirationOption> {
  match token.len() {
    2 => {
      let b0 = token[0].to_ascii_uppercase();
      let b1 = token[1].to_ascii_uppercase();
      match (b0, b1) {
        (b'E', b'X') => Some(ExpirationOption::Ex),
        (b'P', b'X') => Some(ExpirationOption::Px),
        _ => None,
      }
    }
    4 => {
      let b0 = token[0].to_ascii_uppercase();
      let b1 = token[1].to_ascii_uppercase();
      let b2 = token[2].to_ascii_uppercase();
      let b3 = token[3].to_ascii_uppercase();
      match (b0, b1, b2, b3) {
        (b'E', b'X', b'A', b'T') => Some(ExpirationOption::Exat),
        (b'P', b'X', b'A', b'T') => Some(ExpirationOption::Pxat),
        _ => None,
      }
    }
    7 => {
      if token.eq_ignore_ascii_case(b"KEEPTTL") {
        Some(ExpirationOption::Keepttl)
      } else {
        None
      }
    }
    _ => None,
  }
}

/// 从 token 解析过期形式（等价于 [`try_get_expiration_option`]）
#[inline]
pub fn expiration_option_from_token(token: &[u8]) -> Option<ExpirationOption> {
  try_get_expiration_option(token)
}

/// 存在性约束选项（对标 Garnet.server:ExistOptions）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ExistOptions {
  #[default]
  None,
  Nx,
  Xx,
}

/// 解析存在性约束（NX/XX）
#[inline]
pub fn try_get_exist_options(token: &[u8]) -> Option<ExistOptions> {
  if token.len() != 2 {
    return None;
  }
  let b0 = token[0].to_ascii_uppercase();
  let b1 = token[1].to_ascii_uppercase();
  match (b0, b1) {
    (b'N', b'X') => Some(ExistOptions::Nx),
    (b'X', b'X') => Some(ExistOptions::Xx),
    _ => None,
  }
}

/// 有序集合聚合类型（对标 Garnet.server:SortedSetAggregateType）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SortedSetAggregateType {
  #[default]
  Sum,
  Min,
  Max,
}

/// 解析有序集合聚合方式（SUM/MIN/MAX）
#[inline]
pub fn try_get_sorted_set_aggregate_type(token: &[u8]) -> Option<SortedSetAggregateType> {
  if token.len() != 3 {
    return None;
  }
  let b0 = token[0].to_ascii_uppercase();
  let b1 = token[1].to_ascii_uppercase();
  let b2 = token[2].to_ascii_uppercase();
  match (b0, b1, b2) {
    (b'S', b'U', b'M') => Some(SortedSetAggregateType::Sum),
    (b'M', b'I', b'N') => Some(SortedSetAggregateType::Min),
    (b'M', b'A', b'X') => Some(SortedSetAggregateType::Max),
    _ => None,
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_equals_ignore_case() {
    assert!(equals_ignore_case(b"hello", b"HELLO"));
    assert!(equals_ignore_case(b"ZADD", b"zadd"));
    assert!(!equals_ignore_case(b"ZADD", b"zadd1"));
    assert!(!equals_ignore_case(b"", b"a"));
  }

  #[test]
  fn test_sorted_set_add_options_parse() {
    for (raw, expected) in [
      (b"xx" as &[u8], SortedSetAddOption::XX),
      (b"XX", SortedSetAddOption::XX),
      (b"nx", SortedSetAddOption::NX),
      (b"NX", SortedSetAddOption::NX),
      (b"lt", SortedSetAddOption::LT),
      (b"LT", SortedSetAddOption::LT),
      (b"gt", SortedSetAddOption::GT),
      (b"GT", SortedSetAddOption::GT),
      (b"ch", SortedSetAddOption::CH),
      (b"CH", SortedSetAddOption::CH),
      (b"incr", SortedSetAddOption::INCR),
      (b"INCR", SortedSetAddOption::INCR),
    ] {
      assert_eq!(try_get_sorted_set_add_option(raw), Some(expected));
    }
    for bad in [b"" as &[u8], b"x", b"xxx", b"inc", b"incr1", b"zz"] {
      assert_eq!(try_get_sorted_set_add_option(bad), None);
    }
  }

  #[test]
  fn test_expire_options_parse() {
    for (raw, expected) in [
      (b"nx" as &[u8], ExpireOption::NX),
      (b"NX", ExpireOption::NX),
      (b"xx", ExpireOption::XX),
      (b"XX", ExpireOption::XX),
      (b"gt", ExpireOption::GT),
      (b"GT", ExpireOption::GT),
      (b"lt", ExpireOption::LT),
      (b"LT", ExpireOption::LT),
    ] {
      assert_eq!(try_get_expire_option(raw), Some(expected));
      assert_eq!(expire_option_from_token(raw), Some(expected));
    }
    for bad in [b"" as &[u8], b"n", b"nxx", b"zz", b"none"] {
      assert_eq!(try_get_expire_option(bad), None);
      assert_eq!(expire_option_from_token(bad), None);
    }
  }

  #[test]
  fn test_expiration_options_parse() {
    for (raw, expected) in [
      (b"ex" as &[u8], ExpirationOption::Ex),
      (b"EX", ExpirationOption::Ex),
      (b"px", ExpirationOption::Px),
      (b"PX", ExpirationOption::Px),
      (b"exat", ExpirationOption::Exat),
      (b"EXAT", ExpirationOption::Exat),
      (b"pxat", ExpirationOption::Pxat),
      (b"PXAT", ExpirationOption::Pxat),
      (b"keepttl", ExpirationOption::Keepttl),
      (b"KEEPTTL", ExpirationOption::Keepttl),
    ] {
      assert_eq!(try_get_expiration_option(raw), Some(expected));
      assert_eq!(expiration_option_from_token(raw), Some(expected));
    }
    for bad in [b"" as &[u8], b"e", b"exx", b"pxa", b"keeptt", b"keepttll"] {
      assert_eq!(try_get_expiration_option(bad), None);
      assert_eq!(expiration_option_from_token(bad), None);
    }
  }

  #[test]
  fn test_exist_options_parse() {
    assert_eq!(try_get_exist_options(b"nx"), Some(ExistOptions::Nx));
    assert_eq!(try_get_exist_options(b"NX"), Some(ExistOptions::Nx));
    assert_eq!(try_get_exist_options(b"xx"), Some(ExistOptions::Xx));
    assert_eq!(try_get_exist_options(b"XX"), Some(ExistOptions::Xx));
    assert_eq!(try_get_exist_options(b"none"), None);
    assert_eq!(try_get_exist_options(b""), None);
  }

  #[test]
  fn test_sorted_set_aggregate_type_parse() {
    assert_eq!(
      try_get_sorted_set_aggregate_type(b"sum"),
      Some(SortedSetAggregateType::Sum)
    );
    assert_eq!(
      try_get_sorted_set_aggregate_type(b"SUM"),
      Some(SortedSetAggregateType::Sum)
    );
    assert_eq!(
      try_get_sorted_set_aggregate_type(b"min"),
      Some(SortedSetAggregateType::Min)
    );
    assert_eq!(
      try_get_sorted_set_aggregate_type(b"MAX"),
      Some(SortedSetAggregateType::Max)
    );
    assert_eq!(try_get_sorted_set_aggregate_type(b"avg"), None);
    assert_eq!(try_get_sorted_set_aggregate_type(b"summ"), None);
    assert_eq!(try_get_sorted_set_aggregate_type(b""), None);
  }
}
