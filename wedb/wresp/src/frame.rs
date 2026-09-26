//! RESP 协议帧切分与参数提取
//!
//! 统一提供对 RESP 数组命令（*count\r\n...）与内联文本命令（PING\r\n）的切帧能力
//!
//! 在 garnet 中的相对路径: libs/common/RespReadUtils.cs + test/standalone/Garnet.test/Resp/RespReadUtilsTests.cs（RESP 帧读取）

use crate::{
  Error, Result,
  read::{try_read_ptr_with_signed_length_header, try_read_unsigned_array_length},
};

/// 请求帧解析产物（消费字节数 + 参数切片列表）
pub type ParsedFrame<'a> = (usize, Vec<&'a [u8]>);

/// 解析单条 RESP 请求帧
///
/// 三态语义（对齐 C# 解析异常上抛通道）：
/// - `Ok(Some((consumed, args)))`：完整帧
/// - `Ok(None)`：半包未到齐，等待更多字节
/// - `Err(e)`：协议违规（`*-1\r\n` 负数组长度 / 坏 sigil / 整数溢出 /
///   `$-1\r\n` NULL 元素形态），消费端须断流（C# RespParsingException
///   抛出即断连，不得按半包等待）
pub fn parse_resp_frame(buffer: &[u8]) -> Result<Option<ParsedFrame<'_>>> {
  if buffer.is_empty() {
    return Ok(None);
  }
  let mut ptr = buffer;
  if buffer[0] == b'*' {
    let mut count = 0;
    if !try_read_unsigned_array_length(&mut count, &mut ptr)? {
      return Ok(None);
    }
    let count = count as usize;
    let mut args = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
      let mut span = None;
      if !try_read_ptr_with_signed_length_header(&mut span, &mut ptr)? {
        return Ok(None);
      }
      // 请求帧元素禁 NULL 形态（$-1\r\n）：协议违规（C# 请求向解析
      // 无 NULL 元素，同口径抛异常断流）
      let Some(span) = span else {
        return Err(Error::InvalidStringLength(-1));
      };
      args.push(span);
    }
    let consumed = buffer.len() - ptr.len();
    Ok(Some((consumed, args)))
  } else {
    let Some(newline) = buffer.iter().position(|&b| b == b'\n') else {
      return Ok(None);
    };
    let line = &buffer[..newline];
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let args: Vec<&[u8]> = line
      .split(|&b| b == b' ' || b == b'\t')
      .filter(|s| !s.is_empty())
      .collect();
    Ok(Some((newline + 1, args)))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_parse_resp_array() {
    let raw = b"*2\r\n$4\r\nECHO\r\n$5\r\nHELLO\r\n";
    let (consumed, args) = parse_resp_frame(raw)
      .expect("应当解析成功")
      .expect("完整帧");
    assert_eq!(consumed, raw.len());
    assert_eq!(args, vec![&b"ECHO"[..], &b"HELLO"[..]]);
  }

  #[test]
  fn test_parse_resp_inline() {
    let raw = b"PING\r\n";
    let (consumed, args) = parse_resp_frame(raw)
      .expect("应当解析成功")
      .expect("完整帧");
    assert_eq!(consumed, raw.len());
    assert_eq!(args, vec![&b"PING"[..]]);
  }

  #[test]
  fn test_parse_resp_inline_whitespace() {
    let raw = b"SET   foo   bar\r\n";
    let (consumed, args) = parse_resp_frame(raw)
      .expect("应当解析成功")
      .expect("完整帧");
    assert_eq!(consumed, raw.len());
    assert_eq!(args, vec![&b"SET"[..], &b"foo"[..], &b"bar"[..]]);
  }

  #[test]
  fn test_parse_resp_incomplete() {
    let raw = b"*2\r\n$4\r\nECH";
    assert!(matches!(parse_resp_frame(raw), Ok(None)));
  }

  /// 负数组长度（*-1\r\n）：协议违规 Err 透传，不按半包等待
  #[test]
  fn test_parse_resp_negative_array_length_is_error() {
    let res = parse_resp_frame(b"*-1\r\n");
    assert_eq!(res, Err(Error::InvalidStringLength(-1)));
  }

  /// 坏 sigil 元素（非 $ 前导）：协议违规 Err 透传
  #[test]
  fn test_parse_resp_bad_element_sigil_is_error() {
    let res = parse_resp_frame(b"*1\r\n:5\r\n");
    assert_eq!(res, Err(Error::UnexpectedToken(b':')));
  }

  /// NULL 元素形态（$-1\r\n）：请求帧禁 NULL 元素，协议违规 Err 透传
  #[test]
  fn test_parse_resp_null_element_is_error() {
    let res = parse_resp_frame(b"*2\r\n$4\r\nECHO\r\n$-1\r\n");
    assert_eq!(res, Err(Error::InvalidStringLength(-1)));
  }

  /// 长度头整数溢出：协议违规 Err 透传
  #[test]
  fn test_parse_resp_length_overflow_is_error() {
    let res = parse_resp_frame(b"*99999999999\r\n");
    assert!(matches!(res, Err(Error::IntegerOverflow { .. })));
  }
}
