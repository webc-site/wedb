//! wext-json-queryfilter-object-scalar-self-match-fork：JSON 过滤器对象/标量
//! 上下文臂语义锁（三引擎单语义收口）。
//!
//! 对位 C# 锚：
//! - garnet/modules/GarnetJSON/JSONPath/QueryFilter.cs:ExecuteFilter :34-92
//!   节点三臂——JsonArray 遍历元素、JsonObject 遍历属性值、标量无臂恒零产出；
//! - garnet/modules/GarnetJSON/GarnetJsonObject.cs:Set :374-383——Evaluate 回空
//!   且非静态路径 → RESP_WRONG_STATIC_PATH 错误帧（过滤器上下文零命中的唯一
//!   合法应答），杜绝 OK+零写谎报成功。
//!
//! 修复前 rust 求值臂（filter.rs Query）仅两臂、对象/标量对 current 自身求命中
//! 并产出自身，与 delete/mutate 值遍历臂互为两套语义：GET 可见命中而 DEL/SET
//! 零命中、SET 谎报成功。改后 evaluate/delete/mutate 三引擎同走值遍历单语义。

use std::str::from_utf8;

use sonic_rs::Value;
use wext_json::{GarnetJsonObject, JsonPath, RESP_WRONG_STATIC_PATH, SetResult};
use wresp::options::ExistOptions;

fn doc(s: &str) -> Value {
  sonic_rs::from_str(s).unwrap()
}

/// 剥掉 RESP bulk string 帧头取回载荷文本。
fn bulk_payload(resp: &[u8]) -> String {
  let s = from_utf8(resp).unwrap();
  assert!(s.starts_with('$'), "期望 RESP bulk string，实得 {s}");
  let first_crlf = s.find("\r\n").unwrap();
  let len: usize = s[1..first_crlf].parse().unwrap();
  let start = first_crlf + 2;
  s[start..start + len].to_string()
}

// ===================== $.o[?(@.a==1)] 于 {"o":{"a":1}} =====================

/// 求值面：对象上下文经属性值遍历，谓词 @.a==1 在标量值 1 上不命中 → 空集；
/// 修复前 rust 自匹配把 {"a":1} 整体产出，GET 谎报命中。
#[test]
fn object_context_filter_yields_empty_not_self() {
  let root = doc(r#"{"o":{"a":1}}"#);
  let path = JsonPath::parse("$.o[?(@.a==1)]").unwrap();
  assert_eq!(
    path.evaluate(&root).len(),
    0,
    "对象臂须遍历属性值而非自匹配"
  );
}

/// GET 面：JSON.GET $.o[?(@.a==1)] 回 "[]"（零命中的数组空形）。
#[test]
fn get_object_context_filter_returns_empty_array() {
  let mut obj = GarnetJsonObject::create();
  obj
    .set(b"$", br#"{"o":{"a":1}}"#, ExistOptions::None)
    .unwrap();
  let mut out = Vec::new();
  obj
    .try_get(&[b"$.o[?(@.a==1)]"], &mut out, None, None, None, 2)
    .unwrap();
  assert_eq!(bulk_payload(&out), "[]", "GET 须回空数组帧");
}

/// SET 面：JSON.SET k $.o[?(@.a==1)] 42 于 {"o":{"a":1}} 回 wrong static path
/// 错误帧且文档原样未改——修复前恒回 Success 零写入（数据面静默丢写谎报成功）。
#[test]
fn set_object_context_filter_errors_wrong_static_path() {
  let mut obj = GarnetJsonObject::create();
  obj
    .set(b"$", br#"{"o":{"a":1}}"#, ExistOptions::None)
    .unwrap();
  let res = obj
    .set(b"$.o[?(@.a==1)]", b"42", ExistOptions::None)
    .unwrap();
  assert_eq!(
    res,
    SetResult::Error(RESP_WRONG_STATIC_PATH.to_string()),
    "过滤器上下文零命中须回 RESP_WRONG_STATIC_PATH 同向帧"
  );
  let mut out = Vec::new();
  obj.try_get(&[b"$"], &mut out, None, None, None, 2).unwrap();
  assert_eq!(
    bulk_payload(&out),
    r#"[{"o":{"a":1}}]"#,
    "错误帧臂文档须原样未改"
  );
}

/// DEL 面：JSON.DEL $.o[?(@.a==1)] 于 {"o":{"a":1}} 回 0，文档不动
/// （与求值面同语义，修复前 GET 有而 DEL 无的分叉杜绝）。
#[test]
fn del_object_context_filter_returns_zero() {
  let mut root = doc(r#"{"o":{"a":1}}"#);
  let path = JsonPath::parse("$.o[?(@.a==1)]").unwrap();
  assert_eq!(path.delete_matches(&mut root), 0);
  assert_eq!(root, doc(r#"{"o":{"a":1}}"#), "零命中文档须原样");
}

// ===================== $.o[?(@>0)] 于 {"o":5}（标量上下文三面同空） ==========

/// 标量上下文：Field 收敛后 current 为标量 5，QueryFilter 无标量臂恒零产出；
/// GET（evaluate）/DEL（delete_matches）/变异（replace_matches）三面同空，
/// 修复前求值臂自匹配命中、变异臂值遍历零命中的双语义分叉杜绝。
#[test]
fn scalar_context_empty_across_three_engines() {
  let root = doc(r#"{"o":5}"#);
  let path = JsonPath::parse("$.o[?(@>0)]").unwrap();
  assert_eq!(path.evaluate(&root).len(), 0, "求值面：标量节点恒零产出");

  let mut root = doc(r#"{"o":5}"#);
  assert_eq!(path.delete_matches(&mut root), 0, "删除面：标量节点零命中");
  assert_eq!(root, doc(r#"{"o":5}"#));

  let mut root = doc(r#"{"o":5}"#);
  assert_eq!(
    path.replace_matches(&mut root, &doc("42")),
    0,
    "变异面：标量节点零命中"
  );
  assert_eq!(root, doc(r#"{"o":5}"#), "零命中变异文档须原样");
}

/// SET 命令面同轴：JSON.SET k $.o[?(@>0)] 42 于 {"o":5} 亦回 wrong static path。
#[test]
fn set_scalar_context_filter_errors_wrong_static_path() {
  let mut obj = GarnetJsonObject::create();
  obj.set(b"$", br#"{"o":5}"#, ExistOptions::None).unwrap();
  let res = obj.set(b"$.o[?(@>0)]", b"42", ExistOptions::None).unwrap();
  assert_eq!(res, SetResult::Error(RESP_WRONG_STATIC_PATH.to_string()));
}

// ===================== 对象臂正例：遍历属性值产出命中值 =====================

/// 对象臂正锁定语义为「遍历属性值逐个过谓词」而非自匹配：{"o":{"a":3,"b":1}}
/// 经 $.o[?(@>2)] 产出属性值 3（C# QueryFilter.cs:47-55 对位），非整个 {"a":3,"b":1}。
#[test]
fn object_context_filter_yields_matching_property_values() {
  let root = doc(r#"{"o":{"a":3,"b":1}}"#);
  let path = JsonPath::parse("$.o[?(@>2)]").unwrap();
  let hits = path.evaluate(&root);
  assert_eq!(hits.len(), 1, "仅属性值 3 命中: {hits:?}");
  assert_eq!(*hits[0], doc("3"), "产出物须为命中的属性值本身");
}

/// 对象臂变异面同轴：$.o[?(@>2)] 于 {"o":{"a":3,"b":1}} 替换命中属性值 3，
/// 属性键保留、b 不动（与求值面单语义）。
#[test]
fn object_context_filter_mutates_matching_property_values() {
  let mut root = doc(r#"{"o":{"a":3,"b":1}}"#);
  let path = JsonPath::parse("$.o[?(@>2)]").unwrap();
  assert_eq!(path.replace_matches(&mut root, &doc("9")), 1);
  assert_eq!(root, doc(r#"{"o":{"a":9,"b":1}}"#));
}
