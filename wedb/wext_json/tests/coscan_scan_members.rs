//! JSON 域 COSCAN 成员扫描执行体契约回归（票 wcol-coscan-object-domain-inversion
//! 测试点 c）
//!
//! 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:Scan
//!
//! C# 抛 NotImplementedException（会话级异常，无 RESP 帧对位）；rust 裁量错误
//! 帧收口（doc/zh/deviations.md §91），文案 `.NET NotImplementedException` 默认
//! 消息。钉住恒错误、恒同帧，杜绝回退旧假桩 `Ok(())` 空成功吞错形态。

use wext_json::{JsonCommand, RESP_ERR_NOT_IMPLEMENTED};

/// 用例形：任意载荷 / 起始游标 / count / 匹配模式 / 无值档
type Case = (&'static [u8], i64, i64, &'static [u8], bool);

/// 任意载荷与参数组合恒落 NotImplemented 裁量文案（错误帧由组帧内核
/// `wcol custom_scan_operate` 统一写出，此处钉执行体契约面）
#[test]
fn scan_members_always_errors_not_implemented() {
  let scan = JsonCommand::OBJECT_ENTRY.scan_members;

  let cases: &[Case] = &[
    (&[], 0, 10, b"", false),
    (b"{\"a\":1}", 0, 10, b"p*", false),
    (b"not-json", 99, 1, b"p*", true),
  ];

  for &(payload, start, count, pattern, is_no_value) in cases {
    assert_eq!(
      scan(payload, start, count, pattern, is_no_value),
      Err(RESP_ERR_NOT_IMPLEMENTED.as_bytes()),
      "JSON 域扫描应恒拒绝（start={start}, count={count}）"
    );
  }
}
