//! wext-json-empty-path-set-get-forks：JSON 空路径 `""` 三臂双侧分叉收口语义锁。
//!
//! 对位 C# 锚（逐臂）：
//! - 根替换仅认 `$`：garnet/modules/GarnetJSON/GarnetJsonObject.cs:Set :358
//!   （`pathStr.Length == 1 && pathStr[0] == '$'`），`""` 缺键落 :364 回
//!   RESP_NEW_OBJECT_AT_ROOT；
//! - GET Reader 快路单路径 `""` 经 TryGetToWriter → SelectNodes("") 空过滤器回
//!   [根]（JsonCommands.cs:165-178 / JsonPath.cs:Evaluate :122-127）产出 "[<根>]"；
//! - 多路径臂键值带包裹（GarnetJsonObject.cs:TryGet :147-173）。
//!
//! rust 侧收口：SET 缺键空路径回错误帧且不建根（守 §19 缺键不落空壳红线，对齐
//! C#/RedisJSON）；GET 单/多臂经 `path_is_root` 单源将 `""` 规范化为 `$` 同形，
//! 双臂统一回 "[<根>]"（取 C# 快路形；与 C# 通用臂带格式裸根之残余差登记
//! doc/zh/deviations.md 空路径 GET 归一条）。
//!
//! 严禁按 C# 自身两臂不一致形态回改（回改即破此处断言）。

use std::str::from_utf8;

use sonic_rs::{JsonContainerTrait, Value};
use wext_json::{GarnetJsonObject, JsonCommands, RESP_NEW_OBJECT_AT_ROOT, SetResult};
use wresp::options::ExistOptions;

/// 剥掉 RESP bulk string 帧头取回 JSON 文本，并解析为 Value（帧合法性证明）。
fn resp_json(resp: &[u8]) -> Value {
  let s = from_utf8(resp).unwrap();
  assert!(s.starts_with('$'), "期望 RESP bulk string，实得 {s}");
  let json_part = s.split("\r\n").nth(1).unwrap();
  sonic_rs::from_str(json_part).unwrap_or_else(|e| panic!("应答非法 JSON（{json_part:?}）：{e}"))
}

/// 剥掉 RESP bulk string 帧头取回载荷原始字节（供字节级同形断言）。
fn bulk_payload(bytes: &[u8]) -> Vec<u8> {
  let s = from_utf8(bytes).unwrap();
  assert!(s.starts_with('$'), "期望 RESP bulk string，实得 {s}");
  let first_crlf = s.find("\r\n").unwrap();
  let len: usize = s[1..first_crlf].parse().unwrap();
  let start = first_crlf + 2;
  bytes[start..start + len].to_vec()
}

fn json(src: &str) -> Value {
  sonic_rs::from_str(src).unwrap()
}

// ===================== a) SET 缺键空路径回错误帧且不建根 =====================

/// need_initial_update 门仅放行 "$"：缺键 + 空路径回 RESP_NEW_OBJECT_AT_ROOT
/// 错误帧并早退（false → 不落库，EXISTS 0），杜绝旧形「"" 同走根分支建根回 OK」。
#[test]
fn set_missing_key_empty_path_gate_rejects() {
  // 空路径：门拒，回错误帧，早退
  let mut out = Vec::new();
  let go = (JsonCommands::JSON_SET.need_initial_update)(&[b"", b"42"], &mut out, 2);
  assert!(!go, "缺键空路径须被 need_initial_update 门早退（false）");
  assert_eq!(
    out,
    format!("-{RESP_NEW_OBJECT_AT_ROOT}\r\n").as_bytes(),
    "须回 RESP_NEW_OBJECT_AT_ROOT 错误帧"
  );

  // 对照："$" 建根合法放行
  let mut out2 = Vec::new();
  assert!(
    (JsonCommands::JSON_SET.need_initial_update)(&[b"$", b"42"], &mut out2, 2),
    "根路径 $ 缺键建根须放行"
  );

  // 对照：非根非空路径同样被门拒（既有契约，勿因本票回改）
  let mut out3 = Vec::new();
  assert!(
    !(JsonCommands::JSON_SET.need_initial_update)(&[b"$.a", b"42"], &mut out3, 2),
    "既有非根路径缺键建根须被拒"
  );
}

/// 对象层：空文档（缺键根 None）set("") 不建根，回 RESP_NEW_OBJECT_AT_ROOT
/// 错误帧且 is_empty 恒真（EXISTS 0 之对象层投影，守 §19 缺键不落空壳红线）。
#[test]
fn set_empty_path_on_missing_root_errors_without_creating() {
  let mut obj = GarnetJsonObject::create();
  let res = obj.set(b"", b"42", ExistOptions::None).unwrap();
  assert_eq!(
    res,
    SetResult::Error(RESP_NEW_OBJECT_AT_ROOT.to_string()),
    "缺键空路径 set 须回错误帧"
  );
  assert!(
    obj.is_empty(),
    "错误帧臂绝不落根值（缺键不落空壳，EXISTS 0 的对象层投影）"
  );
}

/// 既有键 set("") 走通用臂经空过滤器命中根做替换，语义与 C#
/// `new JsonPath("")`.Evaluate 回 [根] 后 ReplaceMatches 等价（回成功、根被替换）。
#[test]
fn set_empty_path_on_existing_root_replaces_via_empty_filter() {
  let mut obj = GarnetJsonObject::create();
  obj.set(b"$", br#"{"a":1}"#, ExistOptions::None).unwrap();
  let res = obj.set(b"", br#"{"b":2}"#, ExistOptions::None).unwrap();
  assert_eq!(
    res,
    SetResult::Success,
    "既有键空路径经空过滤器命中根须替换成功"
  );
  let mut out = Vec::new();
  obj.try_get(&[b"$"], &mut out, None, None, None, 2).unwrap();
  assert_eq!(
    resp_json(&out),
    json(r#"[{"b":2}]"#),
    "根须被替换为 {{b:2}}"
  );
}

// ===================== b) GET 单路径双臂同形回 "[<根>]" =====================

/// GET 单路径 `""`：无格式臂与带格式臂经 path_is_root 单源收口，双臂同形回
/// "[<根>]"（取 C# Reader 快路 TryGetRoot 形）。带格式臂与 C# 通用臂裸根之差
/// 系登记级有意分叉。
#[test]
fn get_single_empty_path_wrapped_both_arms() {
  let mut obj = GarnetJsonObject::create();
  obj.set(b"$", br#"{"a":1}"#, ExistOptions::None).unwrap();

  // 无格式臂
  let mut out = Vec::new();
  obj.try_get(&[b""], &mut out, None, None, None, 2).unwrap();
  assert_eq!(
    bulk_payload(&out),
    br#"[{"a":1}]"#.as_slice(),
    "无格式 GET \"\" 须回带包裹 [根]"
  );

  // 对照：单路径 "$" 与 "" 逐字节同形（path_is_root 单源）
  let mut out_root = Vec::new();
  obj
    .try_get(&[b"$"], &mut out_root, None, None, None, 2)
    .unwrap();
  assert_eq!(
    bulk_payload(&out),
    bulk_payload(&out_root),
    "单路径 \"\" 与 \"$\" 双臂须同形"
  );

  // 带格式臂：仍回 "[<根>]"（外层包裹，内层 pretty），与 "$" 带格式同形
  let mut out_fmt = Vec::new();
  obj
    .try_get(&[b""], &mut out_fmt, Some("  "), None, None, 2)
    .unwrap();
  let mut out_fmt_root = Vec::new();
  obj
    .try_get(&[b"$"], &mut out_fmt_root, Some("  "), None, None, 2)
    .unwrap();
  assert!(
    bulk_payload(&out_fmt).starts_with(b"[{"),
    "带格式 GET \"\" 须以 [ 包裹根对象，实得 {:?}",
    String::from_utf8_lossy(&bulk_payload(&out_fmt))
  );
  assert_eq!(
    bulk_payload(&out_fmt),
    bulk_payload(&out_fmt_root),
    "带格式单路径 \"\" 与 \"$\" 须同形"
  );
}

// ===================== c) GET 多路径空路径臂键值带包裹 =====================

/// GET 多路径含 `""`：以路径原文为键，值走根路径臂回带包裹的 [根]
/// （`{"":[<根>],"$.a":[...]}`），与单臂经 path_is_root 统一。
#[test]
fn get_multi_empty_path_value_wrapped() {
  let mut obj = GarnetJsonObject::create();
  obj.set(b"$", br#"{"a":1}"#, ExistOptions::None).unwrap();

  let mut out = Vec::new();
  obj
    .try_get(&[b"", b"$.a"], &mut out, None, None, None, 2)
    .unwrap();
  let val = resp_json(&out);
  let map = val.as_object().expect("多路径应答须为 JSON 对象");

  // "" 键的值是带包裹的 [根]，非裸根
  let want_root = json(r#"[{"a":1}]"#);
  let want_a = json("[1]");
  assert_eq!(
    map.get(&String::new()),
    Some(&want_root),
    "多路径 \"\" 臂键值须带 [] 包裹回 [根]"
  );
  assert_eq!(
    map.get(&"$.a".to_string()),
    Some(&want_a),
    "对照：$.a 臂键值 [1]"
  );
}

/// 逐臂对拍：多路径 `""` 键值须与单路径 `""`/`"$"` 结果数组形一致（同 path_is_root 源）。
#[test]
fn get_multi_empty_matches_single_root_form() {
  let mut obj = GarnetJsonObject::create();
  obj
    .set(b"$", br#"{"a":1,"b":2}"#, ExistOptions::None)
    .unwrap();

  let mut single = Vec::new();
  obj
    .try_get(&[b""], &mut single, None, None, None, 2)
    .unwrap();
  // 单元素多路径（仅 ""）实际走单路径臂，取其根数组
  let single_arr = resp_json(&single);

  let mut two = Vec::new();
  obj
    .try_get(&[b"", b"$"], &mut two, None, None, None, 2)
    .unwrap();
  let two_val = resp_json(&two);
  let two_map = two_val.as_object().expect("多路径应答须为 JSON 对象");
  assert_eq!(
    two_map.get(&String::new()),
    Some(&single_arr),
    "多路径 \"\" 值须等于单路径根数组形"
  );
  assert_eq!(
    two_map.get(&"$".to_string()),
    Some(&single_arr),
    "多路径 \"$\" 值须等于单路径根数组形（同 path_is_root 源）"
  );
}
