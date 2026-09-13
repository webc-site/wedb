use core::str;

use wresp::read::{try_read_as_span, try_read_signed_length_header};

use crate::{Error, Result};

/// libs/common/RespReadUtils.cs:MaxArgumentLengthBytes（单 bulk string 长度上限，
/// 对齐 Redis 默认 512MB 上限）
pub const MAX_ARGUMENT_LENGTH_BYTES: isize = wresp::MAX_ARGUMENT_LENGTH_BYTES as isize;

/// libs/client/RespReadResponseUtils.cs:RespReadResponseUtils
pub struct RespReadResponseUtils;

impl RespReadResponseUtils {
  /// libs/client/RespReadResponseUtils.cs:TryReadSimpleString
  pub fn try_read_simple_string(ptr: &mut &[u8]) -> Result<Option<String>> {
    Ok(read_token_line(ptr, b'+')?.map(str::to_string))
  }

  /// libs/client/RespReadResponseUtils.cs:TryReadIntegerAsString
  pub fn try_read_integer_as_string(ptr: &mut &[u8]) -> Result<Option<String>> {
    Ok(read_token_line(ptr, b':')?.map(str::to_string))
  }

  pub fn try_read_integer(ptr: &mut &[u8]) -> Result<Option<i64>> {
    match read_token_line(ptr, b':')? {
      Some(s) => s
        .parse()
        .map(Some)
        .map_err(|_| Error::Other("Invalid integer".into())),
      None => Ok(None),
    }
  }

  /// libs/client/RespReadResponseUtils.cs:TryReadErrorAsString
  pub fn try_read_error_as_string(ptr: &mut &[u8]) -> Result<Option<String>> {
    Ok(read_token_line(ptr, b'-')?.map(str::to_string))
  }

  /// RESP3 null: `_\r\n`
  pub fn try_read_null(ptr: &mut &[u8]) -> Result<Option<()>> {
    Ok(read_token_line(ptr, b'_')?.map(|_| ()))
  }

  /// libs/client/RespReadResponseUtils.cs:TryReadIntWithLengthHeader
  pub fn try_read_int_with_length_header(ptr: &mut &[u8]) -> Result<Option<i32>> {
    let orig = *ptr;
    let Some(len) = read_length_header(ptr, b'$')? else {
      return Ok(None);
    };
    if len < 0 {
      return Err(Error::Other(
        "Negative length in integer length header".into(),
      ));
    }
    let len = len as usize;
    if len > MAX_ARGUMENT_LENGTH_BYTES as usize || ptr.len() < len + 2 {
      *ptr = orig;
      return Ok(None);
    }
    if &ptr[len..len + 2] != b"\r\n" {
      return Err(Error::Other("Missing CRLF after bulk string".into()));
    }
    let s = str::from_utf8(&ptr[..len]).map_err(|e| Error::Other(e.to_string()))?;
    let val: i32 = s
      .parse()
      .map_err(|_| Error::Other("Invalid integer".into()))?;
    *ptr = &ptr[len + 2..];
    Ok(Some(val))
  }

  /// libs/client/RespReadResponseUtils.cs:TryReadStringWithLengthHeader
  pub fn try_read_string_with_length_header(ptr: &mut &[u8]) -> Result<Option<Option<String>>> {
    let orig = *ptr;
    // 长度头已消费，ptr 此刻指向正文起点
    let Some(len) = read_length_header(ptr, b'$')? else {
      return Ok(None);
    };
    if len < 0 {
      return Ok(Some(None)); // null bulk string
    }
    let len = len as usize;
    // C#（RespReadResponseUtils.cs:123）超 512MB 上限按"应答未到齐"处理，
    // 客户端同样挂起等待；此处对齐该语义（对端合规时不可达）
    if len > MAX_ARGUMENT_LENGTH_BYTES as usize {
      *ptr = orig;
      return Ok(None);
    }
    if ptr.len() < len + 2 {
      *ptr = orig;
      return Ok(None);
    }
    if &ptr[len..len + 2] != b"\r\n" {
      return Err(Error::Other("Missing CRLF after bulk string".into()));
    }
    let s = str::from_utf8(&ptr[..len]).map_err(|e| Error::Other(e.to_string()))?;
    *ptr = &ptr[len + 2..];
    Ok(Some(Some(s.to_string())))
  }

  /// 零拷贝读取单个 bulk string 字节切片引用（基于借用视图消除 Vec 堆分配）
  pub fn try_read_byte_slice_with_length_header<'a>(
    ptr: &mut &'a [u8],
  ) -> Result<Option<Option<&'a [u8]>>> {
    let orig = *ptr;
    let Some(len) = read_length_header(ptr, b'$')? else {
      return Ok(None);
    };
    if len < 0 {
      return Ok(Some(None));
    }
    let len = len as usize;
    if len > MAX_ARGUMENT_LENGTH_BYTES as usize {
      *ptr = orig;
      return Ok(None);
    }
    if ptr.len() < len + 2 {
      *ptr = orig;
      return Ok(None);
    }
    if &ptr[len..len + 2] != b"\r\n" {
      return Err(Error::Other("Missing CRLF after bulk string".into()));
    }
    let slice = &ptr[..len];
    *ptr = &ptr[len + 2..];
    Ok(Some(Some(slice)))
  }

  /// libs/client/RespReadResponseUtils.cs:TryReadByteArrayWithLengthHeader
  pub fn try_read_byte_array_with_length_header(
    ptr: &mut &[u8],
  ) -> Result<Option<Option<Vec<u8>>>> {
    Ok(Self::try_read_byte_slice_with_length_header(ptr)?.map(|opt| opt.map(|s| s.to_vec())))
  }

  /// libs/client/RespReadResponseUtils.cs:TryReadStringArrayWithLengthHeader
  pub fn try_read_string_array_with_length_header(
    ptr: &mut &[u8],
  ) -> Result<Option<Option<Vec<String>>>> {
    let orig = *ptr;
    let Some((&token, _)) = ptr.split_first() else {
      return Ok(None);
    };
    if !matches!(token, b'*' | b'~' | b'>') {
      return Err(unexpected_token(token));
    }
    let Some(len) = read_length_header(ptr, token)? else {
      return Ok(None);
    };
    if len < 0 {
      return Ok(Some(None)); // null array
    }
    // 预分配按上限截断：对端声称超长数组头（如 `*2000000000\r\n`）时
    // 按元素数精确预分配会立即触发容量溢出/分配失败进程中止（C# 为可捕获
    // 的 OOM）；截断为增量扩容，合法大数组仅损失对数级扩容拷贝
    let mut res = Vec::with_capacity(len.min(64) as usize);
    for _ in 0..len {
      if ptr.is_empty() {
        *ptr = orig;
        return Ok(None);
      }
      match ptr[0] {
        // 内层返回 None 即应答未到齐：整体按不完整处理，回滚游标
        b'$' => {
          let Some(s) = Self::try_read_string_with_length_header(ptr)? else {
            *ptr = orig;
            return Ok(None);
          };
          res.push(s.unwrap_or_default());
        }
        // 简单串/整数/错误行/RESP3 浮点与布尔共用行读取路径
        b'+' | b':' | b'-' | b',' | b'#' => {
          let Some(s) = read_token_line(ptr, ptr[0])? else {
            *ptr = orig;
            return Ok(None);
          };
          res.push(s.to_string());
        }
        b'*' | b'~' | b'>' => {
          let Some(arr) = Self::try_read_string_array_with_length_header(ptr)? else {
            *ptr = orig;
            return Ok(None);
          };
          res.push(arr.map(|a| a.join(", ")).unwrap_or_default());
        }
        // RESP3 null: _\r\n
        b'_' => {
          let Some(_) = read_token_line(ptr, b'_')? else {
            *ptr = orig;
            return Ok(None);
          };
          res.push(String::new());
        }
        b => return Err(unexpected_token(b)),
      }
    }
    Ok(Some(Some(res)))
  }

  /// 二进制安全的 RESP 数组零拷贝解析：元素为 bulk string 字节切片引用
  ///
  /// 集群复制帧（CLUSTER APPENDLOG 载荷）为原始二进制，不能经 UTF-8 字符串
  /// 路径解码；仅接受 bulk string 元素（发送端 RespWriteUtils.TryWriteArrayItem /
  /// TryWriteBulkString 的数组元素形态），直接借用输入缓冲区切片，杜绝堆分配
  pub fn try_read_byte_slice_array_with_length_header<'a>(
    ptr: &mut &'a [u8],
  ) -> Result<Option<Option<Vec<&'a [u8]>>>> {
    let orig = *ptr;
    let Some((&token, _)) = ptr.split_first() else {
      return Ok(None);
    };
    if !matches!(token, b'*' | b'~' | b'>') {
      return Err(unexpected_token(token));
    }
    let Some(len) = read_length_header(ptr, token)? else {
      return Ok(None);
    };
    if len < 0 {
      return Ok(Some(None)); // null array
    }
    let mut res = Vec::with_capacity(len.min(64) as usize);
    for _ in 0..len {
      let Some(item) = Self::try_read_byte_slice_with_length_header(ptr)? else {
        *ptr = orig;
        return Ok(None);
      };
      res.push(item.unwrap_or_default());
    }
    Ok(Some(Some(res)))
  }

  /// 二进制安全的 RESP 数组解析：元素为 bulk string 字节序列
  pub fn try_read_byte_array_array_with_length_header(
    ptr: &mut &[u8],
  ) -> Result<Option<Option<Vec<Vec<u8>>>>> {
    Ok(
      Self::try_read_byte_slice_array_with_length_header(ptr)?
        .map(|opt| opt.map(|arr| arr.into_iter().map(Vec::from).collect())),
    )
  }
}

/// 读取 `<token><正文>\r\n` 一行并返回正文借用（零拷贝）；应答未到齐返回 None
#[inline]
pub(crate) fn read_token_line<'a>(ptr: &mut &'a [u8], token: u8) -> Result<Option<&'a str>> {
  let Some((&first, rest)) = ptr.split_first() else {
    return Ok(None);
  };
  if first != token {
    return Err(unexpected_token(first));
  }
  let mut span = &[][..];
  let mut temp = rest;
  match try_read_as_span(&mut span, &mut temp) {
    Ok(true) => {
      let s = str::from_utf8(span).map_err(|e| Error::Other(e.to_string()))?;
      *ptr = temp;
      Ok(Some(s))
    }
    Ok(false) => Ok(None),
    Err(e) => Err(Error::Other(e.to_string())),
  }
}

/// 解析 `$<len>\r\n` / `*<len>\r\n` 长度头，统一下沉复用 try_read_signed_length_header
#[inline]
fn read_length_header(ptr: &mut &[u8], token: u8) -> Result<Option<isize>> {
  let mut len = 0;
  let orig = *ptr;
  match try_read_signed_length_header(&mut len, ptr, token) {
    Ok(true) => Ok(Some(len as isize)),
    Ok(false) => {
      *ptr = orig;
      Ok(None)
    }
    Err(e) => Err(Error::Other(e.to_string())),
  }
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
    assert_eq!(
      RespReadResponseUtils::try_read_integer(&mut data).unwrap(),
      Some(12345)
    );
    assert_eq!(data, b"");

    let mut data = &b":nope\r\n"[..];
    assert!(RespReadResponseUtils::try_read_integer(&mut data).is_err());
  }

  /// 长度头词法对齐 C# TryReadUInt64：'+' 号与空数字头拒绝，负号仅允许单前导
  #[test]
  fn length_header_lexer_rejects_signed_and_empty() {
    for bad in [
      &b"$+3\r\nX"[..],
      b"$\r\nX",
      b"$-\r\nX",
      b"$1x\r\nX",
      b"$--1\r\nX",
    ] {
      let mut data = bad;
      assert!(
        RespReadResponseUtils::try_read_string_with_length_header(&mut data).is_err(),
        "{:?} 应判协议错误",
        bad
      );
    }
    // 前导零合法（TryReadUInt64 允许）
    let mut data = &b"$003\r\nabc\r\n"[..];
    assert_eq!(
      RespReadResponseUtils::try_read_string_with_length_header(&mut data).unwrap(),
      Some(Some("abc".to_string()))
    );
    // -0 折算为 0（C# -(int)0 同为 0）
    let mut data = &b"$-0\r\n\r\n"[..];
    assert_eq!(
      RespReadResponseUtils::try_read_string_with_length_header(&mut data).unwrap(),
      Some(Some(String::new()))
    );
  }

  /// 头部值域按 C# int 承载：i32 越界判协议错误，int.MinValue 边界放行为 null
  #[test]
  fn length_header_value_range_is_i32() {
    // 超过 i32::MAX（合法十进制但越 int 值域）
    let mut data = &b"$2147483648\r\n"[..];
    assert!(RespReadResponseUtils::try_read_string_with_length_header(&mut data).is_err());
    // 超过 u64 域的超长数字串
    let mut data = &b"$99999999999999999999\r\n"[..];
    assert!(RespReadResponseUtils::try_read_string_with_length_header(&mut data).is_err());
    // int.MinValue 边界：负向放行，按 null bulk 处理
    let mut data = &b"$-2147483648\r\n"[..];
    assert_eq!(
      RespReadResponseUtils::try_read_string_with_length_header(&mut data).unwrap(),
      Some(None)
    );
  }

  /// 超 512MB 上限的 bulk 头按"未到齐"挂起（对齐 RespReadResponseUtils.cs:123）；
  /// 超长数组头不再按元素数精确预分配（防分配中止），首元素缺失按未到齐处理
  #[test]
  fn oversized_headers_do_not_allocate_or_error() {
    let mut data = &b"$536870913\r\n"[..]; // 512MB + 1
    assert_eq!(
      RespReadResponseUtils::try_read_string_with_length_header(&mut data).unwrap(),
      None
    );
    // 2000 万元素：合法 i32 值域，预分配被截断后按未到齐挂起而非中止进程
    let mut data = &b"*20000000\r\n"[..];
    assert_eq!(
      RespReadResponseUtils::try_read_string_array_with_length_header(&mut data).unwrap(),
      None
    );
  }

  #[test]
  fn partial_frame_rollback_and_resp3_null() {
    // 部分到达的 bulk string：游标完整回滚
    let original = &b"$5\r\nhel"[..];
    let mut data = original;
    assert_eq!(
      RespReadResponseUtils::try_read_string_with_length_header(&mut data).unwrap(),
      None
    );
    assert_eq!(data, original, "游标必须完整回滚");

    // 部分到达的 array：游标完整回滚
    let original_arr = &b"*2\r\n$3\r\nfoo\r\n$3\r\nba"[..];
    let mut data = original_arr;
    assert_eq!(
      RespReadResponseUtils::try_read_string_array_with_length_header(&mut data).unwrap(),
      None
    );
    assert_eq!(data, original_arr, "数组未完整到达时游标必须完整回滚");

    // RESP3 null 测试
    let mut data = &b"_\r\n"[..];
    assert_eq!(
      RespReadResponseUtils::try_read_null(&mut data).unwrap(),
      Some(())
    );
    assert_eq!(data, b"");

    // 数组中包含 RESP3 null
    let mut data = &b"*2\r\n_\r\n$3\r\nbar\r\n"[..];
    let res = RespReadResponseUtils::try_read_string_array_with_length_header(&mut data).unwrap();
    assert_eq!(res, Some(Some(vec![String::new(), "bar".to_string()])));
    assert_eq!(data, b"");
  }

  #[test]
  fn int_with_length_header_and_resp3_collections() {
    let mut data = &b"$4\r\n1234\r\n"[..];
    let res = RespReadResponseUtils::try_read_int_with_length_header(&mut data).unwrap();
    assert_eq!(res, Some(1234));
    assert_eq!(data, b"");

    // 负长度应报错
    let mut data = &b"$-1\r\n"[..];
    assert!(RespReadResponseUtils::try_read_int_with_length_header(&mut data).is_err());

    // RESP3 集合类型 ~2
    let mut data = &b"~2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..];
    let res = RespReadResponseUtils::try_read_string_array_with_length_header(&mut data).unwrap();
    assert_eq!(res, Some(Some(vec!["foo".to_string(), "bar".to_string()])));
    assert_eq!(data, b"");

    // RESP3 推送类型 >1
    let mut data = &b">1\r\n+pubsub\r\n"[..];
    let res = RespReadResponseUtils::try_read_string_array_with_length_header(&mut data).unwrap();
    assert_eq!(res, Some(Some(vec!["pubsub".to_string()])));
    assert_eq!(data, b"");

    // 集合内含浮点与布尔
    let mut data = &b"*2\r\n,3.14\r\n#t\r\n"[..];
    let res = RespReadResponseUtils::try_read_string_array_with_length_header(&mut data).unwrap();
    assert_eq!(res, Some(Some(vec!["3.14".to_string(), "t".to_string()])));
    assert_eq!(data, b"");
  }

  #[test]
  fn byte_array_with_length_header() {
    let mut data = &b"$5\r\nhello\r\n"[..];
    let res = RespReadResponseUtils::try_read_byte_array_with_length_header(&mut data).unwrap();
    assert_eq!(res, Some(Some(b"hello".to_vec())));
    assert_eq!(data, b"");

    let mut data = &b"$-1\r\n"[..];
    let res = RespReadResponseUtils::try_read_byte_array_with_length_header(&mut data).unwrap();
    assert_eq!(res, Some(None));
    assert_eq!(data, b"");

    let mut data = &b"$5\r\nhel"[..];
    let res = RespReadResponseUtils::try_read_byte_array_with_length_header(&mut data).unwrap();
    assert_eq!(res, None);
    assert_eq!(data, &b"$5\r\nhel"[..]);
  }
}
