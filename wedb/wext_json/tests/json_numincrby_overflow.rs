#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use std::str::from_utf8;

use wext_json::JsonCommand;

const RESP_VER: u8 = 2;

fn resp_text(frame: &[u8]) -> String {
  let s = from_utf8(frame).unwrap();
  if let Some(rest) = s.strip_prefix('$') {
    let head_end = rest.find("\r\n").unwrap();
    let len: usize = rest[..head_end].parse().unwrap();
    let body_start = head_end + 2;
    return rest[body_start..body_start + len].to_string();
  }
  s.trim_end_matches("\r\n")
    .trim_start_matches('-')
    .to_string()
}

fn run_updater(cmd: JsonCommand, doc: &str, args: &[&str]) -> (bool, String, String) {
  let mut payload = doc.as_bytes().to_vec();
  let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
  let mut out = Vec::new();
  let ok = (cmd.fns().updater)(&mut payload, &args, &mut out, RESP_VER);
  (ok, resp_text(&out), String::from_utf8(payload).unwrap())
}

/// a) JSON.SET k $ 1e308 → NUMINCRBY k $ 1e308 断言错误帧且原值不变
#[test]
fn numincrby_overflow_returns_error() {
  let doc = r#"{"a":1e308}"#;
  let (ok, out, payload) = run_updater(JsonCommand::NumIncrBy, doc, &["$.a", "1e308"]);
  assert!(ok, "溢出同构回 true 且报错误帧");
  assert!(out.contains("ERR number value is not a valid float"));
  assert_eq!(payload, doc, "发生溢出时 payload 不应被改写");
}

/// b) inf 入参（"inf"）拒收
#[test]
fn numincrby_rejects_inf_arg() {
  let doc = r#"{"a":1}"#;
  for bad_arg in ["inf", "+inf", "-inf"] {
    let (ok, out, payload) = run_updater(JsonCommand::NumIncrBy, doc, &["$.a", bad_arg]);
    assert!(!ok, "非有限入参应被拦截: {bad_arg}");
    assert!(out.contains("ERR number value is not a valid float"));
    assert_eq!(payload, doc, "发生错误时 payload 不应被改写");
  }
}

/// c) NaN 入参（"NaN"/"nan"）拒收
#[test]
fn numincrby_rejects_nan_arg() {
  let doc = r#"{"a":1}"#;
  for bad_arg in ["NaN", "nan", "NAN"] {
    let (ok, out, payload) = run_updater(JsonCommand::NumIncrBy, doc, &["$.a", bad_arg]);
    assert!(!ok, "非有限入参应被拦截: {bad_arg}");
    assert!(out.contains("ERR number value is not a valid float"));
    assert_eq!(payload, doc, "发生错误时 payload 不应被改写");
  }
}

/// d) NUMMULTBY 同门同案（含 1e308 * 10 与 inf 入参）
#[test]
fn nummultby_overflow_and_bad_args() {
  // 溢出
  let doc1 = r#"{"a":1e308}"#;
  let (ok, out, payload) = run_updater(JsonCommand::NumMultBy, doc1, &["$.a", "10"]);
  assert!(ok, "溢出同构回 true 且报错误帧");
  assert!(out.contains("ERR number value is not a valid float"));
  assert_eq!(payload, doc1);

  // 非有限入参拒收
  let doc2 = r#"{"a":2}"#;
  for bad_arg in ["inf", "NaN"] {
    let (ok, out, payload) = run_updater(JsonCommand::NumMultBy, doc2, &["$.a", bad_arg]);
    assert!(!ok);
    assert!(out.contains("ERR number value is not a valid float"));
    assert_eq!(payload, doc2);
  }
}
