//! ZSCAN 族扫描输入解析（对标 libs/server/Objects/Types/GarnetObjectBase.cs
//! 的 ReadScanInput 段与 out 参数组；C# 承载于 GarnetObjectBase 抽象基类，
//! rust 侧解析不依赖对象实例，落为自由函数 + 借用结构）

use wbase::num::{strict_i32, strict_i64};
use wresp::cmd_strings::{
  COUNT, MATCH, NOVALUES, RESP_ERR_GENERIC_INVALIDCURSOR, RESP_ERR_GENERIC_SYNTAX_ERROR,
  RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
};

/// ZSCAN 族扫描输入参数（ReadScanInput 解析产物，pattern 零拷贝借用）
///
/// libs/server/Objects/Types/GarnetObjectBase.cs:ReadScanInput out 参数组
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanInput<'a> {
  pub cursor: i64,
  pub pattern: &'a [u8],
  pub count: i64,
  pub is_no_value: bool,
}

/// 解析 ZSCAN 族输入单点（HSCAN/SSCAN/ZSCAN 三对象共用）：光标 /
/// MATCH pattern / COUNT n / NOVALUES
///
/// libs/server/Objects/Types/GarnetObjectBase.cs:ReadScanInput
/// （COUNT 无条件钳制到 limit_count_in_output，对标 C# countInInput >
/// limitCountInOutput；解析失败返回错误文本，由调用方写 RESP 错误）
pub fn read_scan_input<'a>(
  args: &[&'a [u8]],
  limit_count_in_output: i32,
) -> Result<ScanInput<'a>, &'static [u8]> {
  let mut result = ScanInput {
    cursor: 0,
    pattern: &[],
    count: 10,
    is_no_value: false,
  };

  let Some(cursor) = (if !args.is_empty() {
    strict_i64(args[0])
  } else {
    None
  })
  .filter(|c| *c >= 0) else {
    return Err(RESP_ERR_GENERIC_INVALIDCURSOR.as_bytes());
  };
  result.cursor = cursor;

  let token_count = args.len();
  let mut curr_token_idx = 1;
  while curr_token_idx < token_count {
    let param = args[curr_token_idx];
    curr_token_idx += 1;

    if param.eq_ignore_ascii_case(MATCH) {
      if curr_token_idx >= token_count {
        return Err(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes());
      }
      result.pattern = args[curr_token_idx];
      curr_token_idx += 1;
    } else if param.eq_ignore_ascii_case(COUNT) {
      if curr_token_idx >= token_count {
        return Err(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes());
      }
      match strict_i32(args[curr_token_idx]) {
        Some(c) => {
          curr_token_idx += 1;
          // 无条件钳制单轮数量（对标 C# countInInput > limitCountInOutput）
          result.count = i64::from(c).min(i64::from(limit_count_in_output));
        }
        None => return Err(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes()),
      }
    } else if param.eq_ignore_ascii_case(NOVALUES) {
      result.is_no_value = true;
    }
    // C# 三支链无 else：未识别词元已在上文整词消费并自增下标，直接跳过继续
  }

  Ok(result)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_read_scan_input() {
    // 正常混合大小写解析
    let scan = read_scan_input(
      &[
        b"10".as_slice(),
        b"mAtCh",
        b"abc*",
        b"cOuNt",
        b"20",
        b"nOvAlUeS",
      ],
      100,
    )
    .unwrap();
    assert_eq!(scan.cursor, 10);
    assert_eq!(scan.pattern, b"abc*");
    assert_eq!(scan.count, 20);
    assert!(scan.is_no_value);

    // 未知项被整词跳过（C# ReadScanInput 三支链无 else）
    let scan = read_scan_input(&[b"0".as_slice(), b"UNKNOWN_OPTION"], 100).unwrap();
    assert_eq!(scan.cursor, 0);
    assert!(scan.pattern.is_empty());
    assert_eq!(scan.count, 10);

    // 跳过未知项不影响其后选项解析
    let scan = read_scan_input(
      &[b"0".as_slice(), b"FOO", b"MATCH", b"h*", b"COUNT", b"5"],
      100,
    )
    .unwrap();
    assert_eq!(scan.pattern, b"h*");
    assert_eq!(scan.count, 5);

    // MATCH 缺参
    assert_eq!(
      read_scan_input(&[b"0".as_slice(), b"match"], 100).unwrap_err(),
      RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes()
    );

    // 未知项后接缺参选项仍报语法错误
    assert_eq!(
      read_scan_input(&[b"0".as_slice(), b"FOO", b"COUNT"], 100).unwrap_err(),
      RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes()
    );

    // COUNT 缺参
    assert_eq!(
      read_scan_input(&[b"0".as_slice(), b"count"], 100).unwrap_err(),
      RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes()
    );

    // COUNT 非整数
    assert_eq!(
      read_scan_input(&[b"0".as_slice(), b"count", b"xyz"], 100).unwrap_err(),
      RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes()
    );

    // 负游标或非整数游标
    assert_eq!(
      read_scan_input(&[b"-5".as_slice()], 100).unwrap_err(),
      RESP_ERR_GENERIC_INVALIDCURSOR.as_bytes()
    );

    assert_eq!(
      read_scan_input(&[b"not_a_num".as_slice()], 100).unwrap_err(),
      RESP_ERR_GENERIC_INVALIDCURSOR.as_bytes()
    );
  }
}

use crate::resp::output::ObjectOutput;
use wresp::resp_memory_writer::RespWriter;

/// Scan 输入解析 + 输出回写：HSCAN/SSCAN 共用（对应 C# GarnetObjectBase 的
/// 基类角色，抽象 Scan 以闭包注入；sortedset 因分值可空项走独立实现）
///
/// libs/server/Objects/Types/GarnetObjectBase.cs:Scan
pub fn scan_operate_shared(
  args: &[&[u8]],
  limit_count_in_output: i32,
  output: &mut ObjectOutput<'_>,
  do_scan: impl FnOnce(i64, i64, &[u8], bool) -> (Vec<Vec<u8>>, i64),
) {
  // 参数解析走 GarnetObjectBase::ReadScanInput 单点（错误直接写 RESP 错误）
  let params = match read_scan_input(args, limit_count_in_output) {
    Ok(params) => params,
    Err(msg) => {
      RespWriter::new_ref(output.payload).write_error_bytes(msg);
      return;
    }
  };

  let (items, cursor_output) = do_scan(
    params.cursor,
    params.count,
    params.pattern,
    params.is_no_value,
  );
  let items_len = items.len();

  RespWriter::new_ref(output.payload).write_array_length(2);
  RespWriter::new_ref(output.payload).write_int64_as_bulk_string(cursor_output);

  if items.is_empty() {
    RespWriter::new_ref(output.payload).write_empty_array();
  } else {
    RespWriter::new_ref(output.payload).write_array_length(items.len());
    for item in items {
      RespWriter::new_ref(output.payload).write_bulk_string(&item);
    }
  }

  output.result1 = items_len as i64;
}
