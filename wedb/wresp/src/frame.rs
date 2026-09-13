//! RESP 协议帧切分与参数提取
//!
//! 统一提供对 RESP 数组命令（*count\r\n...）与内联文本命令（PING\r\n）的切帧能力

use crate::read::{try_read_ptr_with_signed_length_header, try_read_unsigned_array_length};

/// 解析单条 RESP 请求帧，返回 (消费字节数, 参数切片列表)
pub fn parse_resp_frame(buffer: &[u8]) -> Option<(usize, Vec<&[u8]>)> {
  if buffer.is_empty() {
    return None;
  }
  let mut ptr = buffer;
  if buffer[0] == b'*' {
    let mut count = 0;
    if !try_read_unsigned_array_length(&mut count, &mut ptr).ok()? || count < 0 {
      return None;
    }
    let count = count as usize;
    let mut args = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
      let mut span = None;
      if !try_read_ptr_with_signed_length_header(&mut span, &mut ptr).ok()? {
        return None;
      }
      args.push(span.unwrap_or_default());
    }
    let consumed = buffer.len() - ptr.len();
    Some((consumed, args))
  } else {
    let newline = buffer.iter().position(|&b| b == b'\n')?;
    let line = &buffer[..newline];
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let args: Vec<&[u8]> = line
      .split(|&b| b == b' ' || b == b'\t')
      .filter(|s| !s.is_empty())
      .collect();
    Some((newline + 1, args))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_parse_resp_array() {
    let raw = b"*2\r\n$4\r\nECHO\r\n$5\r\nHELLO\r\n";
    let (consumed, args) = parse_resp_frame(raw).expect("应当解析成功");
    assert_eq!(consumed, raw.len());
    assert_eq!(args, vec![&b"ECHO"[..], &b"HELLO"[..]]);
  }

  #[test]
  fn test_parse_resp_inline() {
    let raw = b"PING\r\n";
    let (consumed, args) = parse_resp_frame(raw).expect("应当解析成功");
    assert_eq!(consumed, raw.len());
    assert_eq!(args, vec![&b"PING"[..]]);
  }

  #[test]
  fn test_parse_resp_inline_whitespace() {
    let raw = b"SET   foo   bar\r\n";
    let (consumed, args) = parse_resp_frame(raw).expect("应当解析成功");
    assert_eq!(consumed, raw.len());
    assert_eq!(args, vec![&b"SET"[..], &b"foo"[..], &b"bar"[..]]);
  }

  #[test]
  fn test_parse_resp_incomplete() {
    let raw = b"*2\r\n$4\r\nECH";
    assert!(parse_resp_frame(raw).is_none());
  }
}
