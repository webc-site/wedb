use core::str;

use crate::{Error, Result};

/// libs/client/RespReadResponseUtils.cs:RespReadResponseUtils
pub struct RespReadResponseUtils;

impl RespReadResponseUtils {
  pub fn try_read_simple_string(ptr: &mut &[u8]) -> Result<Option<String>> {
    Ok(read_token_line(ptr, b'+')?.map(str::to_string))
  }

  pub fn try_read_integer_as_string(ptr: &mut &[u8]) -> Result<Option<String>> {
    Ok(read_token_line(ptr, b':')?.map(str::to_string))
  }

  pub fn try_read_integer(ptr: &mut &[u8]) -> Result<Option<i64>> {
    match read_token_line(ptr, b':')? {
      Some(s) => s.parse().map(Some).map_err(|_| Error::Other("Invalid integer".into())),
      None => Ok(None),
    }
  }

  pub fn try_read_error_as_string(ptr: &mut &[u8]) -> Result<Option<String>> {
    Ok(read_token_line(ptr, b'-')?.map(str::to_string))
  }

  pub fn try_read_string_with_length_header(ptr: &mut &[u8]) -> Result<Option<Option<String>>> {
    // 长度头已消费，ptr 此刻指向正文起点
    let Some(len) = read_length_header(ptr, b'$')? else {
      return Ok(None);
    };
    if len < 0 {
      return Ok(Some(None)); // null bulk string
    }
    let len = len as usize;
    if ptr.len() < len + 2 {
      return Ok(None);
    }
    if &ptr[len..len + 2] != b"\r\n" {
      return Err(Error::Other("Missing CRLF after bulk string".into()));
    }
    let s = str::from_utf8(&ptr[..len]).map_err(|e| Error::Other(e.to_string()))?;
    *ptr = &ptr[len + 2..];
    Ok(Some(Some(s.to_string())))
  }

  pub fn try_read_string_array_with_length_header(
    ptr: &mut &[u8],
  ) -> Result<Option<Option<Vec<String>>>> {
    let Some(len) = read_length_header(ptr, b'*')? else {
      return Ok(None);
    };
    if len < 0 {
      return Ok(Some(None)); // null array
    }
    let mut res = Vec::with_capacity(len as usize);
    for _ in 0..len {
      if ptr.is_empty() {
        return Ok(None);
      }
      match ptr[0] {
        // 内层返回 None 即应答未到齐：整体按不完整处理
        b'$' => {
          let Some(s) = Self::try_read_string_with_length_header(ptr)? else {
            return Ok(None);
          };
          res.push(s.unwrap_or_default());
        }
        // 简单串/整数/错误行共用同一行读取路径
        b'+' | b':' | b'-' => {
          let Some(s) = read_token_line(ptr, ptr[0])? else {
            return Ok(None);
          };
          res.push(s.to_string());
        }
        b'*' => {
          let Some(arr) = Self::try_read_string_array_with_length_header(ptr)? else {
            return Ok(None);
          };
          res.push(arr.map(|a| a.join(", ")).unwrap_or_default());
        }
        b => return Err(unexpected_token(b)),
      }
    }
    Ok(Some(Some(res)))
  }
}

/// 读取 `<token><正文>\r\n` 一行并返回正文借用（零拷贝）；应答未到齐返回 None
#[inline]
fn read_token_line<'a>(ptr: &mut &'a [u8], token: u8) -> Result<Option<&'a str>> {
  let Some((&first, rest)) = ptr.split_first() else {
    return Ok(None);
  };
  if first != token {
    return Err(unexpected_token(first));
  }
  match find_crlf(rest) {
    Some(r_idx) => {
      let s = str::from_utf8(&rest[..r_idx]).map_err(|e| Error::Other(e.to_string()))?;
      *ptr = &rest[r_idx + 2..];
      Ok(Some(s))
    }
    None => Ok(None),
  }
}

/// 解析 `$<len>\r\n` / `*<len>\r\n` 长度头：定位行尾、解析十进制长度并前移指针
/// （成功后 `ptr` 指向正文起点）；应答未到齐返回 None
#[inline]
fn read_length_header(ptr: &mut &[u8], token: u8) -> Result<Option<isize>> {
  let Some((&first, rest)) = ptr.split_first() else {
    return Ok(None);
  };
  if first != token {
    return Err(unexpected_token(first));
  }
  let Some(r_idx) = find_crlf(rest) else {
    return Ok(None);
  };
  let len = str::from_utf8(&rest[..r_idx])
    .map_err(|e| Error::Other(e.to_string()))?
    .parse()
    .map_err(|_| Error::Other("Invalid length".into()))?;
  *ptr = &rest[r_idx + 2..];
  Ok(Some(len))
}

/// 查找首个 CRLF，返回 `\r` 下标；无完整 CRLF 时返回 None（应答未到齐）
///
/// memchr 加速扫描 `\n`，回看一字节校验 `\r`；孤立的 `\n` 跳过继续找
/// （与 C# RespReadUtils 的逐字节扫描语义一致，但单次遍历为 SIMD 加速）
fn find_crlf(data: &[u8]) -> Option<usize> {
  let mut from = 0;
  while let Some(i) = memchr::memchr(b'\n', &data[from..]) {
    let lf = from + i;
    if lf > 0 && data[lf - 1] == b'\r' {
      return Some(lf - 1);
    }
    from = lf + 1;
  }
  None
}

/// 非法协议标记错误（统一错误文案）
#[inline]
pub(crate) fn unexpected_token(b: u8) -> Error {
  Error::Other(format!("Unexpected token {}", b as char))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_resp_parser() {
    let mut data = &b"+OK\r\n"[..];
    let res = RespReadResponseUtils::try_read_simple_string(&mut data).unwrap();
    assert_eq!(res, Some("OK".to_string()));
    assert_eq!(data, b"");

    let mut data = &b"$3\r\nfoo\r\n"[..];
    let res = RespReadResponseUtils::try_read_string_with_length_header(&mut data).unwrap();
    assert_eq!(res, Some(Some("foo".to_string())));
    assert_eq!(data, b"");

    let mut data = &b"*-1\r\n"[..];
    let res = RespReadResponseUtils::try_read_string_array_with_length_header(&mut data).unwrap();
    assert_eq!(res, Some(None));
    assert_eq!(data, b"");
  }

  #[test]
  fn integer_parses_without_intermediate_string() {
    let mut data = &b":12345\r\n"[..];
    assert_eq!(RespReadResponseUtils::try_read_integer(&mut data).unwrap(), Some(12345));
    assert_eq!(data, b"");

    let mut data = &b":nope\r\n"[..];
    assert!(RespReadResponseUtils::try_read_integer(&mut data).is_err());
  }
}
