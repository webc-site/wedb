//! 服务器选项换算面（对标 libs/server/Servers/ServerOptions.cs:ServerOptions）
//!
//! C# ServerOptions 为旧版配置面（字符串规格 → 2 的幂位宽）；Rust 侧
//! 承接其换算族，字符串解析与 2 的幂工具复用
//! [`super::garnet_server_options`] 的实现（同域单一来源）。

use super::garnet_server_options::OptionsError;

/// 默认 RESP 协议版本（libs/server/Servers/ServerOptions.cs:DEFAULT_RESP_VERSION）
pub const DEFAULT_RESP_VERSION: u8 = 2;

/// 最小主存日志页大小字节（libs/server/Servers/ServerOptions.cs:MinPageSizeBytes
/// —— 最坏情况内联记录 + 页头须完整落入单页）
pub const MIN_PAGE_SIZE_BYTES: i64 = 512;

/// 服务器选项（换算面所需字段子集）
#[derive(Debug, Clone)]
pub struct ServerOptions {
  /// 主存日志内存规格串（C# LogMemorySize，默认 16g）
  pub log_memory_size: String,
  /// 页大小规格串（C# PageSize，默认 16m）
  pub page_size: String,
  /// pub/sub 专日志页大小规格串（C# PubSubPageSize，默认 4k）
  pub pub_sub_page_size: String,
  /// 主存日志段大小规格串（C# SegmentSize，默认 1g）
  pub segment_size: String,
  /// 对象日志段大小规格串（C# ObjectLogSegmentSize，默认 1g）
  pub object_log_segment_size: String,
  /// 哈希索引内存规格串（C# IndexMemorySize，默认 128m）
  pub index_memory_size: String,
  /// 页大小校验下限（测试面可放宽；C# 为常量）
  pub min_page_size_bytes: i64,
}

impl Default for ServerOptions {
  fn default() -> Self {
    Self {
      log_memory_size: "16g".into(),
      page_size: "16m".into(),
      pub_sub_page_size: "4k".into(),
      segment_size: "1g".into(),
      object_log_segment_size: "1g".into(),
      index_memory_size: "128m".into(),
      min_page_size_bytes: MIN_PAGE_SIZE_BYTES,
    }
  }
}

impl ServerOptions {
  /// 构造默认选项
  ///
  /// libs/server/Servers/ServerOptions.cs:ServerOptions（构造）
  pub fn new() -> Self {
    Self::default()
  }

  /// 主存日志内存位宽（向下取 2 的幂）
  ///
  /// libs/server/Servers/ServerOptions.cs:MemorySizeBits
  pub fn memory_size_bits(&self) -> i32 {
    let size = parse_size(&self.log_memory_size).0;
    // 非整幂时告警并下取（C# LogInformation 同款；log 面为尽力而为）
    log2_exact(previous_power_of_2(size.max(1)))
  }

  /// 页大小规格校验换算位宽（强制 ≥ MinPageSizeBytes）
  ///
  /// libs/server/Servers/ServerOptions.cs:ValidatedPageSizeBits
  pub fn validated_page_size_bits(
    &self,
    value: &str,
    _prop_name: &str,
  ) -> Result<i32, OptionsError> {
    let size = parse_size(value).0;
    let adjusted = previous_power_of_2(size);
    if adjusted < self.min_page_size_bytes {
      // C# 抛 Exception（有效页必须容纳最坏情况记录）
      return Err(OptionsError::PageSizeTooSmall(
        value.to_string(),
        adjusted,
        self.min_page_size_bytes,
      ));
    }
    Ok(log2_exact(adjusted))
  }

  /// 页大小位宽
  ///
  /// libs/server/Servers/ServerOptions.cs:PageSizeBits
  pub fn page_size_bits(&self) -> Result<i32, OptionsError> {
    self.validated_page_size_bits(&self.page_size, "PageSize")
  }

  /// pub/sub 专日志页大小字节（向下取 2 的幂）
  ///
  /// libs/server/Servers/ServerOptions.cs:PubSubPageSizeBytes
  pub fn pub_sub_page_size_bytes(&self) -> i64 {
    let size = parse_size(&self.pub_sub_page_size).0;
    previous_power_of_2(size)
  }

  /// 段大小位宽（`is_obj` 选对象日志段规格）
  ///
  /// libs/server/Servers/ServerOptions.cs:SegmentSizeBits
  pub fn segment_size_bits(&self, is_obj: bool) -> i32 {
    let value = if is_obj {
      &self.object_log_segment_size
    } else {
      &self.segment_size
    };
    let size = parse_size(value).0;
    log2_exact(previous_power_of_2(size.max(1)))
  }

  /// 哈希索引 cacheline（64B）数，区间 [64, 2^37] 越界报错
  ///
  /// libs/server/Servers/ServerOptions.cs:IndexSizeCachelines
  pub fn index_size_cachelines(&self, name: &str, index_size: &str) -> Result<i32, OptionsError> {
    let size = parse_size(index_size).0;
    let adjusted = previous_power_of_2(size);
    if !(64..=(1i64 << 37)).contains(&adjusted) {
      return Err(OptionsError::OutOfRange(name.to_string(), adjusted));
    }
    Ok((adjusted / 64) as i32)
  }

  /// 解析大小规格串（返回 (字节, 消费字符数)；k/m/g/t/p 后缀）
  pub fn parse_size(value: &str) -> (i64, usize) {
    parse_size(value)
  }

  /// 尝试解析大小规格串（整串消费才成功）
  pub fn try_parse_size(value: &str) -> Option<i64> {
    try_parse_size(value)
  }

  /// 规格串友好展示（浮点收敛至 3 位整数内换档 k/m/g/t/p）
  pub fn pretty_size(value: i64) -> String {
    pretty_size(value)
  }

  /// 前一个 2 的幂
  pub fn previous_power_of_2(v: i64) -> i64 {
    previous_power_of_2(v)
  }

  /// 下一个 2 的幂
  pub fn next_power_of_2(v: i64) -> i64 {
    next_power_of_2(v)
  }

  /// pub/sub 页大小位宽（宿主构建发布订阅中枢用）
  pub fn pub_sub_page_size_bits(&self) -> u32 {
    self.pub_sub_page_size_bytes().max(2).ilog2()
  }
}

/// libs/server/Servers/ServerOptions.cs:ParseSize
///
/// 解析内存尺寸串（`[0-9]+[kmgtp][b]?`，大小写不敏感）；
/// 返回字节数与消费的字符数。
pub fn parse_size(value: &str) -> (i64, usize) {
  parse_size_bytes(value.as_bytes())
}

/// [`parse_size`] 的字节切片形态
pub fn parse_size_bytes(value: &[u8]) -> (i64, usize) {
  const SUFFIX_EXP: [(u8, u32); 5] = [(b'k', 1), (b'm', 2), (b'g', 3), (b't', 4), (b'p', 5)];
  let mut result: i64 = 0;
  let mut bytes_read = 0usize;

  for (i, &c) in value.iter().enumerate() {
    if c.is_ascii_digit() {
      result = result.wrapping_mul(10).wrapping_add(i64::from(c - b'0'));
      bytes_read += 1;
    } else if let Some((_, exp)) = SUFFIX_EXP.iter().find(|(s, _)| s.eq_ignore_ascii_case(&c)) {
      result = result.wrapping_mul(1024_i64.pow(*exp));
      bytes_read += 1;
      if i + 1 < value.len() && value[i + 1].eq_ignore_ascii_case(&b'b') {
        bytes_read += 1;
      }
      return (result, bytes_read);
    }
  }
  (result, bytes_read)
}

/// libs/server/Servers/ServerOptions.cs:TryParseSize
///
/// 全量消费才算解析成功。
pub fn try_parse_size(value: &str) -> Option<i64> {
  try_parse_size_bytes(value.as_bytes())
}

/// [`try_parse_size`] 的字节切片形态
pub fn try_parse_size_bytes(value: &[u8]) -> Option<i64> {
  let (size, chars_read) = parse_size_bytes(value);
  (chars_read == value.len()).then_some(size)
}

/// libs/server/Servers/ServerOptions.cs:PreviousPowerOf2
///
/// 下取 2 的幂。
#[must_use]
pub fn previous_power_of_2(v: i64) -> i64 {
  let mut v = v;
  v |= v >> 1;
  v |= v >> 2;
  v |= v >> 4;
  v |= v >> 8;
  v |= v >> 16;
  v |= v >> 32;
  v - (v >> 1)
}

/// libs/server/Servers/ServerOptions.cs:NextPowerOf2
///
/// 上取 2 的幂。
#[must_use]
pub fn next_power_of_2(v: i64) -> i64 {
  let mut v = v;
  v = v.wrapping_sub(1);
  v |= v >> 1;
  v |= v >> 2;
  v |= v >> 4;
  v |= v >> 8;
  v |= v >> 16;
  v |= v >> 32;
  v.wrapping_add(1)
}

/// libs/server/Servers/ServerOptions.cs:PrettySize
///
/// 尺寸字节的人类可读形式（自动选 k/m/g/t/p 单位）。
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
  while v.floor().to_string().len() > 3 {
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

/// 位数的 log2（输入保证为 2 的幂且 > 0；与 garnet_server_options 同式）
fn log2_exact(v: i64) -> i32 {
  63 - v.leading_zeros() as i32
}


#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn parses_size_specs_with_suffix_and_b() {
    assert_eq!(ServerOptions::parse_size("1024"), (1024, 4));
    assert_eq!(ServerOptions::parse_size("4k"), (4 * 1024, 2));
    assert_eq!(ServerOptions::parse_size("4kb"), (4 * 1024, 3));
    assert_eq!(ServerOptions::parse_size("16m"), (16 * 1024 * 1024, 3));
    assert_eq!(
      ServerOptions::parse_size("2g"),
      (2i64 * 1024 * 1024 * 1024, 2)
    );
    assert_eq!(ServerOptions::parse_size("1t"), (1i64 << 40, 2));
    assert_eq!(ServerOptions::parse_size(""), (0, 0));
    assert_eq!(ServerOptions::try_parse_size("16m"), Some(16 * 1024 * 1024));
    assert_eq!(
      ServerOptions::try_parse_size("16mb"),
      Some(16 * 1024 * 1024)
    );
    assert_eq!(ServerOptions::try_parse_size("16mx"), None);
  }

  #[test]
  fn powers_of_two() {
    assert_eq!(ServerOptions::previous_power_of_2(1000), 512);
    assert_eq!(ServerOptions::previous_power_of_2(1024), 1024);
    assert_eq!(ServerOptions::next_power_of_2(1000), 1024);
    assert_eq!(ServerOptions::next_power_of_2(1024), 1024);
  }

  #[test]
  fn memory_and_page_size_bits() {
    let mut options = ServerOptions::new();
    assert_eq!(options.memory_size_bits(), 34); // 16g = 2^34
    assert_eq!(options.page_size_bits().expect("默认页合法"), 24); // 16m = 2^24
    options.log_memory_size = "10g".into();
    // 非整幂下取 2^33
    assert_eq!(options.memory_size_bits(), 33);
  }

  #[test]
  fn validated_page_size_enforces_minimum() {
    let options = ServerOptions::new();
    assert!(options.validated_page_size_bits("4k", "PageSize").is_ok());
    let too_small = options.validated_page_size_bits("64", "PageSize");
    assert!(too_small.is_err());
    // 下限放宽后通过
    let mut relaxed = ServerOptions::new();
    relaxed.min_page_size_bytes = 16;
    assert!(relaxed.validated_page_size_bits("64", "PageSize").is_ok());
  }

  #[test]
  fn pub_sub_page_size_bytes() {
    let mut options = ServerOptions::new();
    assert_eq!(options.pub_sub_page_size_bytes(), 4096);
    options.pub_sub_page_size = "3k".into();
    assert_eq!(options.pub_sub_page_size_bytes(), 2048); // 下取 2 的幂
  }

  #[test]
  fn segment_size_bits_selects_log() {
    let mut options = ServerOptions::new();
    assert_eq!(options.segment_size_bits(false), 30); // 1g
    assert_eq!(options.segment_size_bits(true), 30); // 默认同为 1g
    options.object_log_segment_size = "512m".into();
    assert_eq!(options.segment_size_bits(true), 29);
  }

  #[test]
  fn index_size_cachelines_bounds() {
    let options = ServerOptions::new();
    assert_eq!(
      options
        .index_size_cachelines("IndexMemorySize", "128m")
        .expect("合法索引"),
      128 * 1024 * 1024 / 64
    );
    assert!(options.index_size_cachelines("idx", "32").is_err());
    assert!(options.index_size_cachelines("idx", "64g").is_ok());
  }

  #[test]
  fn pretty_size_switches_suffix() {
    assert_eq!(ServerOptions::pretty_size(1024), "1k");
    assert_eq!(ServerOptions::pretty_size(1536), "1.5k");
    assert_eq!(ServerOptions::pretty_size(500), "500");
    assert_eq!(ServerOptions::pretty_size(4 * 1024 * 1024 * 1024), "4g");
    // exp == -18 档：C# suffix[5] 越界上游缺陷，安全回落无后缀不 panic
    assert_eq!(ServerOptions::pretty_size(i64::MAX), "8");
  }
}
