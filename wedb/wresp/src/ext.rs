//! RESP 写出扩展面（Vec<u8> / &[u8] 会话缓冲入口）。
//!
//! null 一族在本 crate 只有 [`RespVecExt::write_resp_null_ver`] 与
//! [`RespVecExt::write_resp_null_array_ver`] 两个入口，其余 crate 与
//! wnode/wcol/wpubsub/wmetric/wext_* 一律转调，不得再就地展开版本
//! if/else，也不得自存第二份协议版本状态。
//!
//! 对位 C# 的两层版本裁决：会话层 RespServerSession.cs:WriteNull /
//! WriteNullArray（经 RespServerSessionOutput.cs 的 respProtocolVersion 裁决）
//! 与 writer 层 RespMemoryWriter.cs 的 resp3 字段分支
//! （WriteNull/WriteNullArray/WriteDoubleNumeric）。rust 因 RespWriter 以
//! Resp2/Resp3 类型参数静态分派，两层在此合并为一处运行时二选一。
//! 协议恒定的 `$-1\r\n` 不再作为命令面应答存在，仅集群配置线格式
//! （wedb/src/server/cluster_config/serializer.rs）按 C# ClusterConfig.cs
//! 的序列化口径持有。

use core::str;

use wbase::num::strict_i64;

use crate::resp_memory_writer::{Resp2, Resp3, RespProtocol, RespWriter};

/// Redis 规范错误前缀最小长度（如 "ERR" 长度为 3）
pub const MIN_ERROR_PREFIX_LEN: usize = 3;

/// 最大单行错误文案长度（防恶意巨幅文案攻击）
pub const MAX_ERROR_MSG_LEN: usize = 512;

/// 错误帧净化单点（字节域）：以 `\r` 或 `\n` 截断防止 RESP 协议帧注入，并施加长度帽
/// （按字节边界切，不保证切在 UTF-8 字符边界）
///
/// 回显客户可控字节（命令名/段名/事件名/脚本错误文案）的错误帧必须经本函数或
/// 其消费方 [`RespWriter::write_error`]（含 `cmd_strings::abort_with_*` 门面）
/// 出口，不得在业务面手拼 `-ERR ...` 前缀裸写字节、也不得在调用侧另立清洗 ——
/// 后者即为本机制的旁路。
///
/// C# 侧无对位实现：`RespMemoryWriter` 的 `WriteError` 直落 `RespWriteUtils` 的
/// `TryWriteError` 原样拷贝字节，只在 XML 注释里声明
/// “The string mustn't contain a CR (\r) or LF (\n) bytes” 的前置约定，全仓不设错误
/// 文本清洗层。本层是 wedb 输出门面自立的纵深防御（我方机制收口，非对标差异），
/// 故按字节截断而不走 `from_utf8_lossy`：错误应答是行分隔帧，不要求客户端可解码为
/// 合法 UTF-8，lossy 会静默改写脚本原始错误字节（如 `error("\u{fffd}")` 类文案）。
#[inline]
pub fn sanitize_error_bytes(s: &[u8], max_len: usize) -> &[u8] {
  let cut = match s.iter().position(|&b| b == b'\r' || b == b'\n') {
    Some(idx) => idx,
    None => s.len(),
  };
  &s[..cut.min(max_len)]
}

/// [`sanitize_error_bytes`] 的 `&str` 门面：同一套清洗逻辑，额外把长度帽回退到
/// UTF-8 字符边界，使返回值仍可直接当 `&str` 消费（既有调用点语义不变）。
/// `\r`/`\n` 为单字节，不可能是多字节字符的后缀，故 CRLF 切断点恒在字符边界上。
#[inline]
pub fn sanitize_error_str(s: &str, max_len: usize) -> &str {
  let cleaned = sanitize_error_bytes(s.as_bytes(), max_len);
  &s[..s.floor_char_boundary(cleaned.len())]
}

pub trait RespSliceExt {
  fn as_str_safe(&self) -> &str;
  /// 严格解析：参数整体须为合法整数（对应 C# parseState.TryGetInt / TryGetLong，
  /// allowLeadingZeros: false —— 前导零、空白、尾随垃圾一律失败），失败返回 None
  fn try_parse_i64(&self) -> Option<i64>;
}

impl RespSliceExt for [u8] {
  #[inline]
  fn as_str_safe(&self) -> &str {
    str::from_utf8(self).unwrap_or("")
  }
  #[inline]
  fn try_parse_i64(&self) -> Option<i64> {
    strict_i64(self)
  }
}

pub trait RespVecExt {
  fn write_resp_int(&mut self, val: i64);
  fn write_resp_bulk_string(&mut self, val: &[u8]);
  fn write_resp_array_len(&mut self, len: usize);
  fn write_resp_error(&mut self, msg: &str);
  fn write_resp_simple_string(&mut self, msg: &str);
  /// 按会话 RESP 版本写 null 应答（RESP3 `_\r\n`、RESP2 `$-1\r\n`）
  ///
  /// libs/server/Resp/RespServerSessionOutput.cs:WriteNull 的会话版本分派
  /// 在分离写缓冲域的形态（会话自持缓冲经 RespServerSession::write_null 共用本单源）
  fn write_resp_null_ver(&mut self, resp_version: u8);
  /// 按会话 RESP 版本写 null 数组应答（RESP3 `_\r\n`、RESP2 `*-1\r\n`）
  ///
  /// libs/server/Resp/RespServerSessionOutput.cs:WriteNullArray 的会话版本
  /// 分派在分离写缓冲域的形态
  fn write_resp_null_array_ver(&mut self, resp_version: u8);
  fn resp_writer<P: RespProtocol>(&mut self) -> RespWriter<&mut Vec<u8>, P>;
  fn resp_writer2(&mut self) -> RespWriter<&mut Vec<u8>, Resp2>;
  fn resp_writer3(&mut self) -> RespWriter<&mut Vec<u8>, Resp3>;
}

impl RespVecExt for Vec<u8> {
  #[inline]
  fn write_resp_int(&mut self, val: i64) {
    RespWriter::new_ref(self).write_int64(val);
  }
  #[inline]
  fn write_resp_bulk_string(&mut self, val: &[u8]) {
    RespWriter::new_ref(self).write_bulk_string(val);
  }
  #[inline]
  fn write_resp_array_len(&mut self, len: usize) {
    RespWriter::new_ref(self).write_array_length(len);
  }
  #[inline]
  fn write_resp_error(&mut self, msg: &str) {
    let mut writer = RespWriter::new_ref(self);
    if let Some((prefix, _)) = msg.split_once(' ')
      && prefix.len() >= MIN_ERROR_PREFIX_LEN
      && prefix.bytes().all(|b| b.is_ascii_uppercase())
    {
      writer.write_error(msg);
    } else if msg == "ERR" {
      writer.write_error("ERR");
    } else {
      writer.write_error_with_prefix("ERR", msg);
    }
  }
  #[inline]
  fn write_resp_simple_string(&mut self, msg: &str) {
    RespWriter::new_ref(self).write_simple_string(msg);
  }
  #[inline]
  fn write_resp_null_ver(&mut self, resp_version: u8) {
    if resp_version >= 3 {
      Resp3::write_null(self);
    } else {
      Resp2::write_null(self);
    }
  }
  #[inline]
  fn write_resp_null_array_ver(&mut self, resp_version: u8) {
    if resp_version >= 3 {
      Resp3::write_null_array(self);
    } else {
      Resp2::write_null_array(self);
    }
  }
  #[inline]
  fn resp_writer<P: RespProtocol>(&mut self) -> RespWriter<&mut Vec<u8>, P> {
    RespWriter::new_ref_p(self)
  }
  #[inline]
  fn resp_writer2(&mut self) -> RespWriter<&mut Vec<u8>, Resp2> {
    RespWriter::new_ref(self)
  }
  #[inline]
  fn resp_writer3(&mut self) -> RespWriter<&mut Vec<u8>, Resp3> {
    RespWriter::new_ref_p(self)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::cmd_strings::{RESP_ERR_WRONG_TYPE, write_error_raw};

  #[test]
  fn try_parse_i64_strict() {
    assert_eq!(b"42".try_parse_i64(), Some(42));
    assert_eq!(b"-7".try_parse_i64(), Some(-7));
    assert_eq!(b"+3".try_parse_i64(), Some(3));
    assert_eq!(b"0".try_parse_i64(), Some(0));
    assert_eq!(b"-0".try_parse_i64(), Some(0));
    assert_eq!(b"9223372036854775807".try_parse_i64(), Some(i64::MAX));
    assert_eq!(b"-9223372036854775808".try_parse_i64(), Some(i64::MIN));
    assert_eq!(b"007".try_parse_i64(), None);
    assert_eq!(b"-007".try_parse_i64(), None);
    assert_eq!(b"abc".try_parse_i64(), None);
    assert_eq!(b"".try_parse_i64(), None);
    assert_eq!(b"1 2".try_parse_i64(), None);
    assert_eq!(b" 1".try_parse_i64(), None);
    assert_eq!(b"5 ".try_parse_i64(), None);
    assert_eq!(b"1x".try_parse_i64(), None);
    assert_eq!(b"9223372036854775808".try_parse_i64(), None);
    assert_eq!(b"-9223372036854775809".try_parse_i64(), None);
  }

  #[test]
  fn vec_ext_formatting() {
    let mut buf = Vec::new();
    buf.write_resp_int(100);
    buf.write_resp_simple_string("OK");
    buf.write_resp_bulk_string(b"foo");
    buf.write_resp_array_len(2);
    buf.write_resp_null_ver(2);
    buf.write_resp_error("something failed");
    assert_eq!(
      buf,
      b":100\r\n+OK\r\n$3\r\nfoo\r\n*2\r\n$-1\r\n-ERR something failed\r\n"
    );

    let mut buf2 = Vec::new();
    buf2.write_resp_error("ERR already has prefix");
    assert_eq!(buf2, b"-ERR already has prefix\r\n");

    let mut buf3 = Vec::new();
    buf3.write_resp_error(RESP_ERR_WRONG_TYPE);
    let mut want = Vec::new();
    write_error_raw(&mut want, RESP_ERR_WRONG_TYPE);
    assert_eq!(buf3, want);
  }

  #[test]
  fn vec_ext_versioned_null_writers() {
    let mut buf = Vec::new();
    buf.write_resp_null_ver(2);
    buf.write_resp_null_ver(3);
    buf.write_resp_null_array_ver(2);
    buf.write_resp_null_array_ver(3);
    assert_eq!(buf, b"$-1\r\n_\r\n*-1\r\n_\r\n");
  }

  /// 错误帧净化单点：字节域核心与 `&str` 门面同源（CRLF 切断 + 长度帽），
  /// 门面只是把长度帽回退到 UTF-8 字符边界，不是第二套清洗。
  #[test]
  fn sanitize_error_bytes_and_str_share_one_mechanism() {
    assert_eq!(
      sanitize_error_bytes(b"boom\r\n:4242", 512),
      b"boom".as_slice()
    );
    assert_eq!(
      sanitize_error_bytes(b"boom\n:4242", 512),
      b"boom".as_slice()
    );
    assert_eq!(sanitize_error_bytes(b"plain", 512), b"plain".as_slice());
    assert_eq!(sanitize_error_bytes(b"abcdef", 3), b"abc".as_slice());
    // 字节域长度帽按字节切，允许落在多字节字符中间（成帧点不要求可解码）
    assert_eq!(
      sanitize_error_bytes("é中".as_bytes(), 3),
      [0xC3u8, 0xA9, 0xE4].as_slice()
    );

    assert_eq!(sanitize_error_str("boom\r\n:4242", 512), "boom");
    assert_eq!(sanitize_error_str("abcdef", 3), "abc");
    // 门面的长度帽回退到字符边界：5 字节落在第二个 é 之后
    assert_eq!(sanitize_error_str("ééé", 5), "éé");
  }
}
