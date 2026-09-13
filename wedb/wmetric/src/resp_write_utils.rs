use std::str::from_utf8;

/// RESP 写入的最小子集，仅覆盖指标类命令的响应编码。
///
/// 对标 Garnet.common/RespWriteUtils 的 TryWriteArrayLength /
/// TryWriteInt32 / TryWriteSimpleString / TryWriteAsciiBulkString；
/// 会话域落地后由 wresp 统一承接，此处的非容量检查版本仅用于
/// 直写 `Vec<u8>`/`String` 缓冲的指标路径（无 SendAndReset 循环）。
pub struct RespWriteUtils;

impl RespWriteUtils {
  /// 追加数组长度：`*<n>\r\n`。
  #[inline]
  pub fn push_array_length(out: &mut String, n: usize) {
    out.push('*');
    out.push_str(itoa::Buffer::new().format(n));
    out.push_str("\r\n");
  }

  /// 追加整数：`:<n>\r\n`。
  #[inline]
  pub fn push_integer(out: &mut String, n: i64) {
    out.push(':');
    out.push_str(itoa::Buffer::new().format(n));
    out.push_str("\r\n");
  }

  /// 追加简单字符串：`+<s>\r\n`。
  #[inline]
  pub fn push_simple_string(out: &mut String, s: &str) {
    out.push('+');
    out.push_str(s);
    out.push_str("\r\n");
  }

  /// 追加 ASCII 批量串：`$<len>\r\n<s>\r\n`。
  #[inline]
  pub fn push_bulk_string(out: &mut String, s: &str) {
    out.push('$');
    out.push_str(itoa::Buffer::new().format(s.len()));
    out.push_str("\r\n");
    out.push_str(s);
    out.push_str("\r\n");
  }

  /// 追加二进制安全批量串（参数 token 场景）。
  #[inline]
  pub fn push_bulk_string_bytes(out: &mut String, s: &[u8]) {
    out.push('$');
    out.push_str(itoa::Buffer::new().format(s.len()));
    out.push_str("\r\n");
    if let Ok(valid) = from_utf8(s) {
      out.push_str(valid);
    } else {
      out.push_str(&String::from_utf8_lossy(s));
    }
    out.push_str("\r\n");
  }

  /// 写入数组长度：`*<n>\r\n`。
  pub fn array_length(n: usize) -> String {
    let mut out = String::with_capacity(16);
    Self::push_array_length(&mut out, n);
    out
  }

  /// 写入整数：`:<n>\r\n`。
  pub fn integer(n: i64) -> String {
    let mut out = String::with_capacity(24);
    Self::push_integer(&mut out, n);
    out
  }

  /// 写入简单字符串：`+<s>\r\n`。
  pub fn simple_string(s: &str) -> String {
    let mut out = String::with_capacity(3 + s.len());
    Self::push_simple_string(&mut out, s);
    out
  }

  /// 写入 ASCII 批量串：`$<len>\r\n<s>\r\n`。
  pub fn bulk_string(s: &str) -> String {
    let mut out = String::with_capacity(8 + s.len());
    Self::push_bulk_string(&mut out, s);
    out
  }
}
