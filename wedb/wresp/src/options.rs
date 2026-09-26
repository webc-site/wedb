//! RESP 协议通用命令选项与词法解析（对标 Garnet.common 与 Garnet.server:RespEnums）
//!
//! 集中统一定义 Redis 命令通用选项枚举，提供零分配、无分支/最少分支的快速词法解析。
//!
//! 在 garnet 中的相对路径: libs/server/Resp/Option? 对标 C# 命令选项（NX/XX/GT/LT 等）

use bitflags::bitflags;

/// 比较字节切片是否相等（忽略 ASCII 大小写）
///
/// 对标 Garnet CmdStrings.EqualsUpperCaseSpanIgnoringCase
#[inline]
pub const fn equals_ignore_case(a: &[u8], b: &[u8]) -> bool {
  wbase::eq_ascii_case(a, b)
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
///
/// libs/server/SessionParseStateExtensions.cs:TryGetSortedSetAddOption
#[inline]
pub const fn try_get_sorted_set_add_option(v: &[u8]) -> Option<SortedSetAddOption> {
  match v.len() {
    2 => {
      if equals_ignore_case(v, b"XX") {
        Some(SortedSetAddOption::XX)
      } else if equals_ignore_case(v, b"NX") {
        Some(SortedSetAddOption::NX)
      } else if equals_ignore_case(v, b"LT") {
        Some(SortedSetAddOption::LT)
      } else if equals_ignore_case(v, b"GT") {
        Some(SortedSetAddOption::GT)
      } else if equals_ignore_case(v, b"CH") {
        Some(SortedSetAddOption::CH)
      } else {
        None
      }
    }
    4 if equals_ignore_case(v, b"INCR") => Some(SortedSetAddOption::INCR),
    _ => None,
  }
}

bitflags! {
  /// 过期条件选项（NX/XX/GT/LT；对标 Garnet.server:ExpireOption）
  ///
  /// 键级与字段级 TTL 的选项组合口径（rust 与 C# 两边一致，重构勿改）：
  /// - 键级 TTL（EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT）可给两个选项，但只放行 XX+GT、XX+LT
  ///   两种兼容对（即下文的 XXGT/XXLT 复合常量），其余组合回 not compatible，
  ///   对应 C# KeyAdminCommands 的 NetworkEXPIRE 两参合并分支；
  /// - 字段级 TTL（HEXPIRE 族、ZEXPIRE 族）只解析单个选项词元，不支持复合，
  ///   对应 C# HashCommands 的 HashExpire 与 SortedSetCommands 的 SortedSetExpire
  ///   （两边都是「试读一个选项、读不到就当没有」的形态）。
  ///
  /// 禁止把键级的双选口子放开到字段级（那将与 C# 分叉），也禁止反过来收掉键级复合。
  /// 判据现状：键级见 wnode 的 network_expire，字段级见 parse_hash_expire_args 与 sorted_set_expire。
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
    /// 既有且更大（XX+GT：键级 TTL 双选项唯一两种合法组合之一，字段级不使用复合常量）
    const XXGT = Self::XX.bits() | Self::GT.bits();
    /// 既有且更小（XX+LT：键级 TTL 双选项唯一两种合法组合之一，字段级不使用复合常量）
    const XXLT = Self::XX.bits() | Self::LT.bits();
  }
}

/// 解析过期选项词元（NX/XX/GT/LT）
///
/// libs/server/SessionParseStateExtensions.cs:TryGetExpireOption
#[inline]
pub const fn try_get_expire_option(v: &[u8]) -> Option<ExpireOption> {
  if v.len() != 2 {
    return None;
  }
  if equals_ignore_case(v, b"NX") {
    Some(ExpireOption::NX)
  } else if equals_ignore_case(v, b"XX") {
    Some(ExpireOption::XX)
  } else if equals_ignore_case(v, b"GT") {
    Some(ExpireOption::GT)
  } else if equals_ignore_case(v, b"LT") {
    Some(ExpireOption::LT)
  } else {
    None
  }
}

/// 复合过期时间戳与条件选项（对标 Garnet libs/server/ExpirationWithOption.cs）
///
/// 低 4 位存储 [`ExpireOption`]，高 60 位存储粗粒度 .NET Ticks（1600ns 分辨率）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ExpirationWithOption {
  word: i64,
}

impl ExpirationWithOption {
  /// libs/server/ExpirationWithOption.cs:ExpirationWithOption(long, ExpireOption)
  #[inline]
  pub fn new(expiration_time_in_ticks: i64, expire_option: ExpireOption) -> Self {
    Self {
      word: ((expiration_time_in_ticks >> 4) << 4) | (expire_option.bits() as i64 & 0xF),
    }
  }

  /// 由 (word_head, word_tail) 两个 i32 拼装（C# RespServerSession 传参形态）
  #[inline]
  pub fn from_word_head_tail(word_head: i32, word_tail: i32) -> Self {
    Self {
      word: ((((word_head as u32) as u64) << 32) | (word_tail as u32 as u64)) as i64,
    }
  }

  /// libs/server/ExpirationWithOption.cs:ExpirationTimeInTicks
  #[inline]
  pub fn expiration_time_in_ticks(&self) -> i64 {
    (self.word >> 4) << 4
  }

  /// libs/server/ExpirationWithOption.cs:ExpireOption
  #[inline]
  pub fn expire_option(&self) -> ExpireOption {
    ExpireOption::from_bits_truncate((self.word & 0xF) as u8)
  }

  /// libs/server/ExpirationWithOption.cs:Word
  #[inline]
  pub fn word(&self) -> i64 {
    self.word
  }

  /// libs/server/ExpirationWithOption.cs:WordHead
  #[inline]
  pub fn word_head(&self) -> i32 {
    ((self.word >> 32) & 0xFFFF_FFFF) as i32
  }
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
///
/// libs/server/SessionParseStateExtensions.cs:TryGetExpirationOption
#[inline]
pub const fn try_get_expiration_option(token: &[u8]) -> Option<ExpirationOption> {
  match token.len() {
    2 => {
      if equals_ignore_case(token, b"EX") {
        Some(ExpirationOption::Ex)
      } else if equals_ignore_case(token, b"PX") {
        Some(ExpirationOption::Px)
      } else {
        None
      }
    }
    4 => {
      if equals_ignore_case(token, b"EXAT") {
        Some(ExpirationOption::Exat)
      } else if equals_ignore_case(token, b"PXAT") {
        Some(ExpirationOption::Pxat)
      } else {
        None
      }
    }
    7 if equals_ignore_case(token, b"KEEPTTL") => Some(ExpirationOption::Keepttl),
    _ => None,
  }
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
pub const fn try_get_exist_options(token: &[u8]) -> Option<ExistOptions> {
  if token.len() != 2 {
    return None;
  }
  if equals_ignore_case(token, b"NX") {
    Some(ExistOptions::Nx)
  } else if equals_ignore_case(token, b"XX") {
    Some(ExistOptions::Xx)
  } else {
    None
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

impl SortedSetAggregateType {
  /// 对两分值执行聚合运算（SUM / MIN / MAX）
  #[inline(always)]
  pub fn apply(self, a: f64, b: f64) -> f64 {
    match self {
      Self::Sum => a + b,
      Self::Min => a.min(b),
      Self::Max => a.max(b),
    }
  }
}

/// 解析有序集合聚合方式（SUM/MIN/MAX）
///
/// libs/server/SessionParseStateExtensions.cs:TryGetSortedSetAggregateType
#[inline]
pub const fn try_get_sorted_set_aggregate_type(token: &[u8]) -> Option<SortedSetAggregateType> {
  if token.len() != 3 {
    return None;
  }
  if equals_ignore_case(token, b"SUM") {
    Some(SortedSetAggregateType::Sum)
  } else if equals_ignore_case(token, b"MIN") {
    Some(SortedSetAggregateType::Min)
  } else if equals_ignore_case(token, b"MAX") {
    Some(SortedSetAggregateType::Max)
  } else {
    None
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
    }
    for bad in [b"" as &[u8], b"n", b"nxx", b"zz", b"none"] {
      assert_eq!(try_get_expire_option(bad), None);
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
    }
    for bad in [b"" as &[u8], b"e", b"exx", b"pxa", b"keeptt", b"keepttll"] {
      assert_eq!(try_get_expiration_option(bad), None);
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

  #[test]
  fn test_sorted_set_aggregate_apply() {
    assert_eq!(SortedSetAggregateType::Sum.apply(2.5, 3.5), 6.0);
    assert_eq!(SortedSetAggregateType::Min.apply(2.5, 3.5), 2.5);
    assert_eq!(SortedSetAggregateType::Max.apply(2.5, 3.5), 3.5);
  }

  #[test]
  fn test_expiration_with_option() {
    let ticks = 1_000_000_000i64;
    let opt = ExpireOption::GT;
    let e = ExpirationWithOption::new(ticks, opt);
    assert_eq!(e.expire_option(), ExpireOption::GT);
    assert_eq!(e.expiration_time_in_ticks(), (ticks >> 4) << 4);

    let reconstructed =
      ExpirationWithOption::from_word_head_tail(e.word_head(), (e.word() & 0xFFFF_FFFF) as i32);
    assert_eq!(e, reconstructed);
    assert_eq!(e.word(), reconstructed.word());
  }
}
