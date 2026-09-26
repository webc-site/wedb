//! 内存与存储容量单位解析工具
//!
//! 支持解析带单位尺寸字符串（如 "1k", "64mb", "4gb", "1t", "2p"），
//! 以及 2 的幂对齐与人类可读格式化。
//!
//! 页尺寸（主存日志 / read cache / AOF）的「下取 2 的幂 + 下限校验 + 位宽换算」
//! 一律收口在 [`validated_page_size_bits`] 一处，[`MIN_PAGE_SIZE_BYTES`] 为其唯一
//! 真源，调用点不得各自改写判定。
//!
//! 自研依据: 尺寸解析（C# 对应 libs/common/ServerOptionsParser? 尺寸字面量解析面）

/// 常见单位乘数，编译期常量
const MUL_K: i64 = 1024;
const MUL_M: i64 = 1024 * MUL_K;
const MUL_G: i64 = 1024 * MUL_M;
const MUL_T: i64 = 1024 * MUL_G;
const MUL_P: i64 = 1024 * MUL_T;

/// 最小页容量字节（对标 ServerOptions.cs:MinPageSizeBytes）
///
/// 最坏情况的单条内联记录（默认口径下约 490B，含 64B 页头）必须落入单页，
/// 512 是容得下它的最小 2 的幂页容量；由 [`validated_page_size_bits`] 单点读取。
pub const MIN_PAGE_SIZE_BYTES: i64 = 512;

/// 页尺寸校验核的拒绝面（C# ServerOptions.cs 中 ValidatedPageSizeBits 的
/// `throw new Exception` 形态，文案点名配置属性名）
///
/// 取幂后的生效页容量仍低于 [`MIN_PAGE_SIZE_BYTES`]。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
  "{prop_name} 生效 {effective} 字节（尺寸已下取 2 的幂）低于页容量下限 \
   {MIN_PAGE_SIZE_BYTES} 字节，最坏情况单条记录无法落入单页"
)]
pub struct PageSizeError {
  /// 点名的配置属性名（对标 C# `propName` 形参）
  pub prop_name: &'static str,
  /// 下取 2 的幂后的生效字节（判定跌破用的即此值）
  pub effective: i64,
}

/// libs/server/Servers/ServerOptions.cs:ValidatedPageSizeBits
///
/// 页容量字节 → 页位宽：下取 2 的幂 → 折损告警（点名属性名，C# 同文案）→ 对
/// 取幂后的生效值强制 [`MIN_PAGE_SIZE_BYTES`] 下限 → log2。
///
/// 全仓页尺寸投影的**唯一**校验核：主存日志 / read cache（rust 侧共用
/// [`crate::node_options::HlogOptions`] 的页容量投影，对标 C# `PageSizeBits` 与
/// `ReadCachePageSizeBits` 同调本入口的复用形态）与 AOF 页尺寸皆经此处，
/// 调用点禁另写判定或另起配置校验通道。
pub fn validated_page_size_bits(size: i64, prop_name: &'static str) -> Result<u32, PageSizeError> {
  let adjusted = previous_power_of_2(size);
  if size != adjusted {
    log::info!("Warning: using lower {prop_name} than specified (power of 2)");
  }
  if adjusted < MIN_PAGE_SIZE_BYTES {
    return Err(PageSizeError {
      prop_name,
      effective: adjusted,
    });
  }
  Ok(log2_exact(adjusted) as u32)
}

/// libs/client/Utility.cs:ParseSize
/// libs/server/Servers/ServerOptions.cs:ParseSize
///（C# 两处同体实现（服务端参数解析与客户端工具各一份），rust 收敛单内核，
/// 服务端 `TryParseSize` / 启动参数页尺寸解析同走此口）
///
/// 解析内存尺寸字符串（如 "1gb"、"64mb"、"512kb"）
///
/// 返回 `(解析出的字节数, 消费的字符数)`。
#[inline]
pub fn parse_size(value: &str) -> (i64, usize) {
  parse_size_bytes(value.as_bytes())
}

/// [`parse_size`] 的字节切片形态，零拷贝解析
pub fn parse_size_bytes(value: &[u8]) -> (i64, usize) {
  let mut result: i64 = 0;
  let mut bytes_read = 0usize;

  for (i, &c) in value.iter().enumerate() {
    if c.is_ascii_digit() {
      result = result.wrapping_mul(10).wrapping_add(i64::from(c - b'0'));
      bytes_read += 1;
    } else {
      let mul = match c.to_ascii_lowercase() {
        b'k' => MUL_K,
        b'm' => MUL_M,
        b'g' => MUL_G,
        b't' => MUL_T,
        b'p' => MUL_P,
        _ => 0,
      };
      if mul != 0 {
        result = result.wrapping_mul(mul);
        bytes_read += 1;
        if i + 1 < value.len() && value[i + 1].eq_ignore_ascii_case(&b'b') {
          bytes_read += 1;
        }
        return (result, bytes_read);
      }
    }
  }
  (result, bytes_read)
}

/// libs/server/Servers/ServerOptions.cs:TryParseSize
///
/// 尝试全量解析内存尺寸字符串
///
/// 仅当全量输入被有效消费时返回 `Some(字节数)`，否则返回 `None`。
#[inline]
pub fn try_parse_size(value: &str) -> Option<i64> {
  try_parse_size_bytes(value.as_bytes())
}

/// [`try_parse_size`] 的字节切片形态
#[inline]
pub fn try_parse_size_bytes(value: &[u8]) -> Option<i64> {
  let (size, chars_read) = parse_size_bytes(value);
  (chars_read == value.len()).then_some(size)
}

/// 下取 2 的幂
#[inline]
#[must_use]
pub const fn previous_power_of_2(mut v: i64) -> i64 {
  v |= v >> 1;
  v |= v >> 2;
  v |= v >> 4;
  v |= v >> 8;
  v |= v >> 16;
  v |= v >> 32;
  v - (v >> 1)
}

/// 上取 2 的幂
#[inline]
#[must_use]
pub const fn next_power_of_2(mut v: i64) -> i64 {
  v = v.wrapping_sub(1);
  v |= v >> 1;
  v |= v >> 2;
  v |= v >> 4;
  v |= v >> 8;
  v |= v >> 16;
  v |= v >> 32;
  v.wrapping_add(1)
}

/// 精确计算 2 的幂的 log2（v <= 0 时安全返回 0）
#[inline]
#[must_use]
pub const fn log2_exact(v: i64) -> i32 {
  if v <= 0 { 0 } else { v.ilog2() as i32 }
}

/// libs/client/Utility.cs:PrettySize
/// libs/server/Servers/ServerOptions.cs:PrettySize
///（C# 两处同体实现（服务端 CONFIG GET 尺寸回显与客户端工具各一份），
/// rust 收敛单内核）
///
/// 尺寸字节的人类可读形式（自动选择 k/m/g/t/p 单位）
#[must_use]
pub fn pretty_size(value: i64) -> String {
  const SUFFIX: [char; 5] = ['k', 'm', 'g', 't', 'p'];
  fn round12(v: f64) -> f64 {
    let scaled = v * 1e12;
    let bumped = if scaled >= 0.0 {
      scaled + 0.5
    } else {
      scaled - 0.5
    };
    bumped.floor() / 1e12
  }

  let mut v = value as f64;
  let mut exp: i32 = 0;
  while v - v.floor() > 0.0 {
    if exp >= 18 {
      break;
    }
    exp += 3;
    v *= 1024.0;
    v = round12(v);
  }
  while v.floor().abs() >= 1000.0 {
    if exp <= -18 {
      break;
    }
    exp -= 3;
    v /= 1024.0;
    v = round12(v);
  }
  if exp > 0 {
    let c = SUFFIX[(exp / 3 - 1) as usize];
    format!("{v}{c}")
  } else if exp < 0 {
    let idx = (-exp / 3 - 1) as usize;
    if idx < SUFFIX.len() {
      let c = SUFFIX[idx];
      format!("{v}{c}")
    } else {
      format!("{v}")
    }
  } else {
    format!("{v}")
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_parse_size() {
    assert_eq!(parse_size("1k"), (1024, 2));
    assert_eq!(parse_size("64kb"), (64 * 1024, 4));
    assert_eq!(parse_size("16m"), (16 * 1024 * 1024, 3));
    assert_eq!(parse_size("1gb"), (1024 * 1024 * 1024, 3));
    assert_eq!(try_parse_size("1gb"), Some(1024 * 1024 * 1024));
    assert_eq!(try_parse_size(""), Some(0));
    assert_eq!(try_parse_size("invalid"), None);
  }

  #[test]
  fn test_powers_of_2() {
    assert_eq!(previous_power_of_2(1000), 512);
    assert_eq!(previous_power_of_2(1024), 1024);
    assert_eq!(next_power_of_2(1000), 1024);
    assert_eq!(next_power_of_2(1024), 1024);
    assert_eq!(log2_exact(1024), 10);
  }

  /// 页尺寸校验核（对标 ServerOptions.cs:ValidatedPageSizeBits）：下限判定落在
  /// 取幂后的生效值上，恰界合法、跌破即点名属性名拒
  #[test]
  fn test_validated_page_size_bits() {
    // 恰在下限：512 合法，位宽 9；默认主存页 16m 位宽 24
    assert_eq!(validated_page_size_bits(512, "page"), Ok(9));
    assert_eq!(validated_page_size_bits(16 * 1024 * 1024, "page"), Ok(24));
    // 取幂折损但仍在下界：600 → 生效 512
    assert_eq!(validated_page_size_bits(600, "page"), Ok(9));
    // 256 页尺寸不得被静默接受
    assert_eq!(
      validated_page_size_bits(256, "page"),
      Err(PageSizeError {
        prop_name: "page",
        effective: 256
      })
    );
    // 向下取幂后跌破下限：511 本身高于生效档，取幂归 256 才判负（报错给生效值）
    assert_eq!(
      validated_page_size_bits(511, "page"),
      Err(PageSizeError {
        prop_name: "page",
        effective: 256
      })
    );
    // 文案点名属性名与下限字节
    let msg = validated_page_size_bits(0, "aof-page-size")
      .expect_err("零页容量必拒")
      .to_string();
    assert!(
      msg.contains("aof-page-size") && msg.contains(&MIN_PAGE_SIZE_BYTES.to_string()),
      "文案须点名属性名与下限字节: {msg}"
    );
  }
}
