//! §206 JSON 深度安全门回归（登记全文见 doc/zh/deviations.md §206）
//!
//! sonic `Value` 快路（parse_dom → dispatch_value ↔ parse_array/parse_object）
//! 互递归无深度门，2MB 栈（compio worker 缺省）约 16000 层即栈溢出进程 abort；
//! check_depth 于解析入口单趟字节状态机预扫，MAX_JSON_DEPTH=255 与 sonic 通用
//! 路 serde 门值同值对齐。此处钉：入口门拒收形、JsonPath 过滤器数组字面量
//! 旁路同门形、引号域/转义闭包形（字符串字面量内结构字节不计深）。门先于
//! sonic 递归，拒臂零递归，缺省测试线程栈安全。

use wext_json::{GarnetJsonObject, JsonPath};

/// 超门拒收：300 层裸开括号（超 MAX_JSON_DEPTH=255），错误与解析失败同形
#[test]
fn rejects_300_open_brackets() {
  let payload = vec![b'['; 300];
  assert!(
    GarnetJsonObject::from_slice(&payload).is_err(),
    "300 层载荷须被 §206 深度门拒收"
  );
}

/// 超门拒收：20000 层（大深度同门；门超限即刻返回，恶意载荷不全遍历）
#[test]
fn rejects_20000_open_brackets() {
  let payload = vec![b'['; 20_000];
  assert!(
    GarnetJsonObject::from_slice(&payload).is_err(),
    "20000 层载荷须被 §206 深度门拒收"
  );
}

/// 引号域闭包：字符串字面量内的 300 个 `[` 不计深，载荷合法装载
#[test]
fn brackets_inside_string_do_not_count() {
  let mut payload = Vec::new();
  payload.extend_from_slice(b"[\"");
  payload.extend_from_slice(&[b'['; 300]);
  payload.extend_from_slice(b"\"]");
  assert!(
    GarnetJsonObject::from_slice(&payload).is_ok(),
    "字符串字面量内 `[` 不计深，载荷须合法装载"
  );
}

/// 转义闭包：`\"` 不提前出串，其后 300 个 `[` 仍在字符串域内不计深
#[test]
fn escaped_quote_keeps_string_state() {
  let mut payload = Vec::new();
  payload.extend_from_slice(b"[\"a\\\"");
  payload.extend_from_slice(&[b'['; 300]);
  payload.extend_from_slice(b"\"]");
  assert!(
    GarnetJsonObject::from_slice(&payload).is_ok(),
    "转义引号不得提前出串（InEscape 吞过后必回 InString）"
  );
}

/// 旁路同门（§206）：JsonPath 过滤器数组字面量与载荷同走 sonic `Value` 快路
///（parser.rs try_parse_array_literal），超门路径整体拒收，与本处解析失败同形
///（InvalidPath）
#[test]
fn filter_array_literal_over_gate_rejected() {
  let path = format!("$[?(@.a == {}{})]", "[".repeat(300), "]".repeat(300));
  assert!(
    JsonPath::parse(&path).is_err(),
    "过滤器数组字面量 300 层须被 §206 深度门拒收"
  );
}

/// 过滤器数组字面量界内形：浅层嵌套字面量路径解析照常（门不误伤）
#[test]
fn filter_array_literal_within_gate_ok() {
  assert!(JsonPath::parse("$[?(@.a == [1,[2]])]").is_ok());
}
