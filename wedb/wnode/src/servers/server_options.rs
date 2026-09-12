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
  #[inline]
  pub fn parse_size(value: &str) -> (i64, usize) {
    parse_size(value)
  }

  /// 尝试解析大小规格串（整串消费才成功）
  #[inline]
  pub fn try_parse_size(value: &str) -> Option<i64> {
    try_parse_size(value)
  }

  /// 规格串友好展示（浮点收敛至 3 位整数内换档 k/m/g/t/p）
  #[inline]
  pub fn pretty_size(value: i64) -> String {
    pretty_size(value)
  }

  /// 前一个 2 的幂
  #[inline]
  pub const fn previous_power_of_2(v: i64) -> i64 {
    previous_power_of_2(v)
  }

  /// 下一个 2 的幂
  #[inline]
  pub const fn next_power_of_2(v: i64) -> i64 {
    next_power_of_2(v)
  }

  /// pub/sub 页大小位宽（宿主构建发布订阅中枢用）
  #[inline]
  pub fn pub_sub_page_size_bits(&self) -> u32 {
    self.pub_sub_page_size_bytes().max(2).ilog2()
  }
}

// 统一复用 wnode 底层通用单位与位运算实现，消除重复代码
pub use crate::{
  log2_exact, next_power_of_2, parse_size, parse_size_bytes, pretty_size, previous_power_of_2,
  try_parse_size, try_parse_size_bytes,
};
