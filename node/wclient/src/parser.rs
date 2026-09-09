use std::str;

use crate::{Error, Result};

/// libs/client/RespReadResponseUtils.cs:RespReadResponseUtils
pub struct RespReadResponseUtils;

impl RespReadResponseUtils {
  pub fn try_read_simple_string(ptr: &mut &[u8]) -> Result<Option<String>> {
    if ptr.is_empty() {
      return Ok(None);
    }
    if ptr[0] != b'+' {
      return Err(Error::Other(format!("Unexpected token {}", ptr[0] as char)));
    }
    if let Some(r_idx) = Self::find_crlf(&ptr[1..]) {
      let res = str::from_utf8(&ptr[1..1 + r_idx])
        .map_err(|e| Error::Other(e.to_string()))?
        .to_string();
      *ptr = &ptr[1 + r_idx + 2..];
      Ok(Some(res))
    } else {
      Ok(None)
    }
  }

  pub fn try_read_integer_as_string(ptr: &mut &[u8]) -> Result<Option<String>> {
    if ptr.is_empty() {
      return Ok(None);
    }
    if ptr[0] != b':' {
      return Err(Error::Other(format!("Unexpected token {}", ptr[0] as char)));
    }
    if let Some(r_idx) = Self::find_crlf(&ptr[1..]) {
      let res = str::from_utf8(&ptr[1..1 + r_idx])
        .map_err(|e| Error::Other(e.to_string()))?
        .to_string();
      *ptr = &ptr[1 + r_idx + 2..];
      Ok(Some(res))
    } else {
      Ok(None)
    }
  }

  pub fn try_read_integer(ptr: &mut &[u8]) -> Result<Option<i64>> {
    let s = match Self::try_read_integer_as_string(ptr)? {
      Some(s) => s,
      None => return Ok(None),
    };
    let val: i64 = s
      .parse()
      .map_err(|_| Error::Other("Invalid integer".into()))?;
    Ok(Some(val))
  }

  pub fn try_read_error_as_string(ptr: &mut &[u8]) -> Result<Option<String>> {
    if ptr.is_empty() {
      return Ok(None);
    }
    if ptr[0] != b'-' {
      return Err(Error::Other(format!("Unexpected token {}", ptr[0] as char)));
    }
    if let Some(r_idx) = Self::find_crlf(&ptr[1..]) {
      let res = str::from_utf8(&ptr[1..1 + r_idx])
        .map_err(|e| Error::Other(e.to_string()))?
        .to_string();
      *ptr = &ptr[1 + r_idx + 2..];
      Ok(Some(res))
    } else {
      Ok(None)
    }
  }

  pub fn try_read_string_with_length_header(ptr: &mut &[u8]) -> Result<Option<Option<String>>> {
    if ptr.is_empty() {
      return Ok(None);
    }
    if ptr[0] != b'$' {
      return Err(Error::Other(format!("Unexpected token {}", ptr[0] as char)));
    }
    let r_idx = if let Some(i) = Self::find_crlf(&ptr[1..]) {
      i
    } else {
      return Ok(None);
    };

    let len_str = str::from_utf8(&ptr[1..1 + r_idx]).map_err(|e| Error::Other(e.to_string()))?;
    let len: isize = len_str
      .parse()
      .map_err(|_| Error::Other("Invalid length".into()))?;

    if len < 0 {
      *ptr = &ptr[1 + r_idx + 2..];
      return Ok(Some(None));
    }

    let len = len as usize;
    let content_start = 1 + r_idx + 2;
    if ptr.len() < content_start + len + 2 {
      return Ok(None);
    }

    if &ptr[content_start + len..content_start + len + 2] != b"\r\n" {
      return Err(Error::Other("Missing CRLF after bulk string".into()));
    }

    let res = str::from_utf8(&ptr[content_start..content_start + len])
      .map_err(|e| Error::Other(e.to_string()))?
      .to_string();
    *ptr = &ptr[content_start + len + 2..];
    Ok(Some(Some(res)))
  }

  pub fn try_read_string_array_with_length_header(
    ptr: &mut &[u8],
  ) -> Result<Option<Option<Vec<String>>>> {
    if ptr.is_empty() {
      return Ok(None);
    }
    if ptr[0] != b'*' {
      return Err(Error::Other(format!("Unexpected token {}", ptr[0] as char)));
    }
    let r_idx = if let Some(i) = Self::find_crlf(&ptr[1..]) {
      i
    } else {
      return Ok(None);
    };

    let len_str = str::from_utf8(&ptr[1..1 + r_idx]).map_err(|e| Error::Other(e.to_string()))?;
    let len: isize = len_str
      .parse()
      .map_err(|_| Error::Other("Invalid array length".into()))?;

    *ptr = &ptr[1 + r_idx + 2..];

    if len < 0 {
      return Ok(Some(None));
    }

    let mut res = Vec::with_capacity(len as usize);
    for _ in 0..len {
      if ptr.is_empty() {
        return Ok(None);
      }
      match ptr[0] {
        b'$' => {
          let s = match Self::try_read_string_with_length_header(ptr)? {
            Some(s) => s.unwrap_or_default(),
            None => return Ok(None),
          };
          res.push(s);
        }
        b'+' => {
          let s = match Self::try_read_simple_string(ptr)? {
            Some(s) => s,
            None => return Ok(None),
          };
          res.push(s);
        }
        b':' => {
          let s = match Self::try_read_integer_as_string(ptr)? {
            Some(s) => s,
            None => return Ok(None),
          };
          res.push(s);
        }
        b'*' => {
          let s_arr = match Self::try_read_string_array_with_length_header(ptr)? {
            Some(Some(arr)) => arr.join(", "),
            Some(None) => String::new(),
            None => return Ok(None),
          };
          res.push(s_arr);
        }
        b'-' => {
          let s = match Self::try_read_error_as_string(ptr)? {
            Some(s) => s,
            None => return Ok(None),
          };
          res.push(s);
        }
        _ => return Err(Error::Other(format!("Unexpected token {}", ptr[0] as char))),
      }
    }

    Ok(Some(Some(res)))
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
}
