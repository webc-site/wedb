/// RESP 写入的最小子集，仅覆盖指标类命令的响应编码。
///
/// 对标 Garnet.common/RespWriteUtils 的 TryWriteArrayLength /
/// TryWriteInt32 / TryWriteSimpleString / TryWriteAsciiBulkString；
/// 会话域落地后由 wresp 统一承接，此处的非容量检查版本仅用于
/// 直写 `Vec<u8>`/`String` 缓冲的指标路径（无 SendAndReset 循环）。
pub struct RespWriteUtils;

impl RespWriteUtils {
  /// 写入数组长度：`*<n>\r\n`。
  pub fn array_length(n: usize) -> String {
    format!("*{n}\r\n")
  }

  /// 写入整数：`:<n>\r\n`。
  pub fn integer(n: i64) -> String {
    format!(":{n}\r\n")
  }

  /// 写入简单字符串：`+<s>\r\n`。
  pub fn simple_string(s: &str) -> String {
    format!("+{s}\r\n")
  }

  /// 写入 ASCII 批量串：`$<len>\r\n<s>\r\n`。
  pub fn bulk_string(s: &str) -> String {
    format!("${}\r\n{s}\r\n", s.len())
  }

  /// 写入二进制安全批量串（参数 token 场景）。
  pub fn bulk_string_bytes(s: &[u8]) -> String {
    format!("${}\r\n", s.len()) + &String::from_utf8_lossy(s) + "\r\n"
  }
}
