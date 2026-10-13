#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! ZSCAN 族扫描输入解析回归（read_scan_input 全支路）。
//!
//! 对位 C# test/standalone/Garnet.test/RespScanCommandsTests.cs 的输入解析面：
//! 光标 / MATCH / COUNT / NOVALUES 混合大小写词元、未知项整词跳过（C#
//! ReadScanInput 三支链无 else）、缺参与非整数错误文本黄金值。
//! 迁自 src/types/scan_input.rs 内联测试（原依赖全 pub 才可直测）。

use wcol::types::read_scan_input;
use wresp::cmd_strings::{
  RESP_ERR_GENERIC_INVALIDCURSOR, RESP_ERR_GENERIC_SYNTAX_ERROR,
  RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
};

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

  // 未知项被整词跳过（C# ReadScanInput 三支链无 else）；缺省页大小黄金值 10
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
