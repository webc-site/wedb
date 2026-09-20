//! 对标 test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs

use std::str::from_utf8;

use sonic_rs::JsonValueTrait;
use wext_json::{
  ExistOptions, GarnetJsonObject, RESP_NEW_OBJECT_AT_ROOT, RESP_WRONG_STATIC_PATH, SetResult,
};

/// test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs:RegisterCustomCommand
pub fn register_custom_command() {
  assert!(wext_json::is_command_registered("JSON.SET"));
  assert!(wext_json::is_command_registered("JSON.GET"));
}

fn assert_resp_json_eq(resp: &[u8], expected_json: &str) {
  let s = from_utf8(resp).unwrap();
  assert!(s.starts_with('$'), "Expected RESP bulk string, got: {s}");
  let json_part = s.split("\r\n").nth(1).unwrap();
  let v1: sonic_rs::Value = sonic_rs::from_str(json_part).unwrap();
  let v2: sonic_rs::Value = sonic_rs::from_str(expected_json).unwrap();
  assert_eq!(v1, v2);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs:JsonSetGetTests
#[test]
fn json_set_get_tests() {
  register_custom_command();

  let mut obj = GarnetJsonObject::create();

  // Invalid JSON value
  assert!(obj.set(b"$", b"{\"a\": 1", ExistOptions::None).is_err());

  // Invalid JSON path on empty object
  let res = obj.set(b"a", b"{\"a\": 1}", ExistOptions::None).unwrap();
  assert_eq!(res, SetResult::Error(RESP_NEW_OBJECT_AT_ROOT.to_string()));

  // Set root object
  let res = obj
    .set(
      b"$",
      b"{\"f1\": {\"a\":1}, \"f2\":{\"a\":2}}",
      ExistOptions::None,
    )
    .unwrap();
  assert_eq!(res, SetResult::Success);

  // Get whole object
  let mut out = Vec::new();
  assert!(obj.try_get(&[], &mut out, None, None, None, 2).unwrap());
  assert_resp_json_eq(&out, "{\"f1\":{\"a\":1},\"f2\":{\"a\":2}}");

  // Get root path "$"
  out.clear();
  assert!(obj.try_get(&[b"$"], &mut out, None, None, None, 2).unwrap());
  assert_resp_json_eq(&out, "[{\"f1\":{\"a\":1},\"f2\":{\"a\":2}}]");

  // Set deep path $..a to 3
  let res = obj.set(b"$..a", b"3", ExistOptions::None).unwrap();
  assert_eq!(res, SetResult::Success);

  out.clear();
  assert!(obj.try_get(&[b"$"], &mut out, None, None, None, 2).unwrap());
  assert_resp_json_eq(&out, "[{\"f1\":{\"a\":3},\"f2\":{\"a\":3}}]");

  // Set new field $.f3 to 4
  let res = obj.set(b"$.f3", b"4", ExistOptions::None).unwrap();
  assert_eq!(res, SetResult::Success);

  out.clear();
  assert!(obj.try_get(&[b"$"], &mut out, None, None, None, 2).unwrap());
  assert_resp_json_eq(&out, "[{\"f1\":{\"a\":3},\"f2\":{\"a\":3},\"f3\":4}]");

  // Set new field $.f5 to object
  let res = obj.set(b"$.f5", b"{\"c\": 5}", ExistOptions::None).unwrap();
  assert_eq!(res, SetResult::Success);

  out.clear();
  assert!(obj.try_get(&[b"$"], &mut out, None, None, None, 2).unwrap());
  assert_resp_json_eq(
    &out,
    "[{\"f1\":{\"a\":3},\"f2\":{\"a\":3},\"f3\":4,\"f5\":{\"c\":5}}]",
  );

  // Get field f1
  out.clear();
  assert!(
    obj
      .try_get(&[b"f1"], &mut out, None, None, None, 2)
      .unwrap()
  );
  assert_resp_json_eq(&out, "[{\"a\":3}]");

  // Get multiple paths
  out.clear();
  assert!(
    obj
      .try_get(&[b"f1", b"$.f5"], &mut out, None, None, None, 2)
      .unwrap()
  );
  assert_resp_json_eq(&out, "{\"f1\":[{\"a\":3}],\"$.f5\":[{\"c\":5}]}");
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs:JsonSetGetBasicTypes
#[test]
fn json_set_get_basic_types() {
  for (input, expected) in [
    ("{\"a\": 1}", "{\"a\":1}"),
    ("{\"a\": [1,2,3]}", "{\"a\":[1,2,3]}"),
    ("{\"a\": null}", "{\"a\":null}"),
    ("{\"a\": \"\"}", "{\"a\":\"\"}"),
    ("{}", "{}"),
  ] {
    let mut obj = GarnetJsonObject::create();
    obj.set(b"$", input.as_bytes(), ExistOptions::None).unwrap();
    let mut out = Vec::new();
    obj.try_get(&[], &mut out, None, None, None, 2).unwrap();
    assert_resp_json_eq(&out, expected);
  }
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs:JsonSetPathOperations
#[test]
fn json_set_path_operations() {
  for (path, new_val, initial, expected) in [
    (
      "$..a",
      "42",
      "{\"x\":{\"a\":1},\"y\":{\"a\":2}}",
      "{\"x\":{\"a\":42},\"y\":{\"a\":42}}",
    ),
    ("$.x", "{\"b\":2}", "{\"x\":{\"a\":1}}", "{\"x\":{\"b\":2}}"),
    ("$.new", "123", "{\"x\":1}", "{\"x\":1,\"new\":123}"),
    ("$[0]", "42", "[1,2,3]", "[42,2,3]"),
  ] {
    let mut obj = GarnetJsonObject::create();
    obj
      .set(b"$", initial.as_bytes(), ExistOptions::None)
      .unwrap();
    obj
      .set(path.as_bytes(), new_val.as_bytes(), ExistOptions::None)
      .unwrap();
    let mut out = Vec::new();
    obj.try_get(&[], &mut out, None, None, None, 2).unwrap();
    assert_resp_json_eq(&out, expected);
  }
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs:JsonGetMultiplePathsTest
#[test]
fn json_get_multiple_paths_test() {
  let mut obj = GarnetJsonObject::create();
  let json = b"{\"a\":{\"x\":1},\"b\":{\"x\":2},\"c\":{\"x\":3}}";
  obj.set(b"$", json, ExistOptions::None).unwrap();

  let mut out = Vec::new();
  obj
    .try_get(&[b"$.a", b"$.b", b"$.c"], &mut out, None, None, None, 2)
    .unwrap();
  let expected = "{\"$.a\":[{\"x\":1}],\"$.b\":[{\"x\":2}],\"$.c\":[{\"x\":3}]}";
  let expected_resp = format!("${}\r\n{}\r\n", expected.len(), expected);
  assert_eq!(String::from_utf8(out).unwrap(), expected_resp);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs:JsonSetInvalidJsonTest
#[test]
fn json_set_invalid_json_test() {
  for invalid in ["{\"a\": ", "invalid", "{a:1}"] {
    let mut obj = GarnetJsonObject::create();
    assert!(
      obj
        .set(b"$", invalid.as_bytes(), ExistOptions::None)
        .is_err()
    );
  }
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs:JsonSetInvalidPathTest
#[test]
fn json_set_invalid_path_test() {
  for invalid_path in ["$[", "$..", "$[a]"] {
    let mut obj = GarnetJsonObject::create();
    obj.set(b"$", b"{\"a\":1}", ExistOptions::None).unwrap();
    assert!(
      obj
        .set(invalid_path.as_bytes(), b"42", ExistOptions::None)
        .is_err()
    );
  }
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs:JsonGetNonExistentTests
#[test]
fn json_get_non_existent_tests() {
  let obj = GarnetJsonObject::create();
  let mut out = Vec::new();
  obj.try_get(&[b"$"], &mut out, None, None, None, 2).unwrap();
  assert_eq!(String::from_utf8(out).unwrap(), "$-1\r\n");

  let mut obj2 = GarnetJsonObject::create();
  obj2.set(b"$", b"{\"a\":1}", ExistOptions::None).unwrap();
  let mut out2 = Vec::new();
  obj2
    .try_get(&[b"$..nonexistent"], &mut out2, None, None, None, 2)
    .unwrap();
  assert_eq!(String::from_utf8(out2).unwrap(), "$2\r\n[]\r\n");
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs:JsonGetComplexPathTests
#[test]
fn json_get_complex_path_tests() {
  let complex_json = r#"{
    "store": {
      "book": [
        { "category": "reference", "author": "Nigel Rees", "title": "Sayings of the Century", "price": 8.95 },
        { "category": "fiction", "author": "Evelyn Waugh", "title": "Sword of Honour", "price": 12.99 },
        { "category": "fiction", "author": "Herman Melville", "title": "Moby Dick", "price": 8.99 },
        { "category": "fiction", "author": "J. R. R. Tolkien", "title": "The Lord of the Rings", "price": 22.99 }
      ],
      "bicycle": { "color": "red", "price": 19.95 }
    }
  }"#;

  for (path, expected) in [
    (
      "$.store.book[*].author",
      "[\"Nigel Rees\",\"Evelyn Waugh\",\"Herman Melville\",\"J. R. R. Tolkien\"]",
    ),
    (
      "$..author",
      "[\"Nigel Rees\",\"Evelyn Waugh\",\"Herman Melville\",\"J. R. R. Tolkien\"]",
    ),
    (
      "$.store.book[2]",
      "[{\"category\":\"fiction\",\"author\":\"Herman Melville\",\"title\":\"Moby Dick\",\"price\":8.99}]",
    ),
    ("$.store.bicycle.color", "[\"red\"]"),
  ] {
    let mut obj = GarnetJsonObject::create();
    obj
      .set(b"$", complex_json.as_bytes(), ExistOptions::None)
      .unwrap();
    let mut out = Vec::new();
    obj
      .try_get(&[path.as_bytes()], &mut out, None, None, None, 2)
      .unwrap();
    let expected_resp = format!("${}\r\n{}\r\n", expected.len(), expected);
    assert_eq!(String::from_utf8(out).unwrap(), expected_resp);
  }
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs:JsonGetArrayOperationsTest
#[test]
fn json_get_array_operations_test() {
  for (path, expected, initial) in [
    ("$[0,1]", "[1,2]", "[1,2,3,4]"),
    ("$[1:3]", "[2,3]", "[1,2,3,4]"),
    ("$[-2:]", "[3,4]", "[1,2,3,4]"),
    ("$[::2]", "[1,3]", "[1,2,3,4]"),
  ] {
    let mut obj = GarnetJsonObject::create();
    obj
      .set(b"$", initial.as_bytes(), ExistOptions::None)
      .unwrap();
    let mut out = Vec::new();
    obj
      .try_get(&[path.as_bytes()], &mut out, None, None, None, 2)
      .unwrap();
    let expected_resp = format!("${}\r\n{}\r\n", expected.len(), expected);
    assert_eq!(String::from_utf8(out).unwrap(), expected_resp);
  }
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs:JsonGetAdvancedFiltersTest
#[test]
fn json_get_advanced_filters_test() {
  let complex_json = r#"{
    "store": {
      "book": [
        { "author": "Nigel Rees", "price": 8.95 },
        { "author": "J. R. R. Tolkien", "title": "The Lord of the Rings", "price": 22.99 }
      ]
    }
  }"#;

  let mut obj = GarnetJsonObject::create();
  obj
    .set(b"$", complex_json.as_bytes(), ExistOptions::None)
    .unwrap();

  let mut out = Vec::new();
  obj
    .try_get(
      &[b"$..book[?(@.author =~ /.*Tolkien/)]"],
      &mut out,
      None,
      None,
      None,
      2,
    )
    .unwrap();
  let s = String::from_utf8(out).unwrap();
  assert!(s.contains("J. R. R. Tolkien"));
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs:JsonSetComplexPathOperationsTest
#[test]
fn json_set_complex_path_operations_test() {
  let complex_json = r#"{
    "store": {
      "book": [
        { "price": 8.95 },
        { "price": 12.99 },
        { "price": 8.99 },
        { "price": 22.99 }
      ],
      "bicycle": { "price": 19.95 }
    }
  }"#;

  for (path, new_price) in [
    ("$.store.book[0].price", "15.99"),
    ("$.store.book[3].price", "25.99"),
    ("$.store.bicycle.price", "29.99"),
  ] {
    let mut obj = GarnetJsonObject::create();
    obj
      .set(b"$", complex_json.as_bytes(), ExistOptions::None)
      .unwrap();
    obj
      .set(path.as_bytes(), new_price.as_bytes(), ExistOptions::None)
      .unwrap();
    let mut out = Vec::new();
    obj
      .try_get(&[path.as_bytes()], &mut out, None, None, None, 2)
      .unwrap();
    let expected = format!("[{new_price}]");
    let expected_resp = format!("${}\r\n{}\r\n", expected.len(), expected);
    assert_eq!(String::from_utf8(out).unwrap(), expected_resp);
  }
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs:JsonSetNonStaticPathNewFieldTest
#[test]
fn json_set_non_static_path_new_field_test() {
  let complex_json = r#"{
    "store": {
      "book": [ { "price": 8.95 }, { "price": 12.99 } ],
      "bicycle": { "price": 19.95 }
    }
  }"#;

  for path in ["$..book[*].nonexistent", "$..[?(@.price)].discount"] {
    let mut obj = GarnetJsonObject::create();
    obj
      .set(b"$", complex_json.as_bytes(), ExistOptions::None)
      .unwrap();
    let res = obj
      .set(path.as_bytes(), b"42.99", ExistOptions::None)
      .unwrap();
    assert_eq!(res, SetResult::Error(RESP_WRONG_STATIC_PATH.to_string()));
  }
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs:JsonSetNonStaticPathExistingFieldTest
#[test]
fn json_set_non_static_path_existing_field_test() {
  let complex_json = r#"{
    "store": {
      "book": [ { "price": 8.95 }, { "price": 12.99 } ],
      "bicycle": { "price": 19.95 }
    }
  }"#;

  let mut obj = GarnetJsonObject::create();
  obj
    .set(b"$", complex_json.as_bytes(), ExistOptions::None)
    .unwrap();
  let res = obj.set(b"$..price", b"42.99", ExistOptions::None).unwrap();
  assert_eq!(res, SetResult::Success);

  let mut out = Vec::new();
  obj
    .try_get(&[b"$..price"], &mut out, None, None, None, 2)
    .unwrap();
  let s = String::from_utf8(out).unwrap();
  assert!(s.contains("42.99"));
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs:SaveRecoverTest
#[test]
fn save_recover_test() {
  let mut obj = GarnetJsonObject::create();
  obj.set(b"$", b"{\"a\": 1}", ExistOptions::None).unwrap();

  let mut buf = Vec::new();
  obj.serialize_object(&mut buf).unwrap();

  let back = GarnetJsonObject::deserialize(&mut &buf[..]).unwrap();
  let mut out = Vec::new();
  back.try_get(&[], &mut out, None, None, None, 2).unwrap();
  assert_eq!(String::from_utf8(out).unwrap(), "$7\r\n{\"a\":1}\r\n");
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs:AofUpsertRecoverTestAsync
#[test]
fn aof_upsert_recover_test_async() {
  let mut obj = GarnetJsonObject::create();
  obj.set(b"$", b"{\"a\": 1}", ExistOptions::None).unwrap();

  let mut buf = Vec::new();
  obj.serialize_object(&mut buf).unwrap();

  let back = GarnetJsonObject::deserialize(&mut &buf[..]).unwrap();
  assert_eq!(back.root_node.unwrap()["a"].as_i64(), Some(1));
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs:SerializationTest
#[test]
fn serialization_test() {
  let mut obj = GarnetJsonObject::create();
  obj.set(b"$", b"{\"a\": 1}", ExistOptions::None).unwrap();

  let mut buf = Vec::new();
  obj.serialize_object(&mut buf).unwrap();

  let back = GarnetJsonObject::deserialize(&mut &buf[..]).unwrap();
  let mut out = Vec::new();
  back
    .try_get(&[b"$"], &mut out, None, None, None, 2)
    .unwrap();
  assert_eq!(String::from_utf8(out).unwrap(), "$9\r\n[{\"a\":1}]\r\n");
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs:JsonModuleLoadTest
#[test]
fn json_module_load_test() {
  assert!(wext_json::is_command_registered("JSON.SET"));
  assert!(wext_json::is_command_registered("JSON.GET"));
  assert!(wext_json::is_command_registered("JSON.DEL"));
  assert!(wext_json::is_command_registered("JSON.TYPE"));
  assert!(wext_json::is_command_registered("JSON.MGET"));
}

/// RESP bulk string 载荷精确剥离（$len\r\n<payload>\r\n 按长度截取，
/// 供字节级断言使用）
fn bulk_payload(bytes: &[u8]) -> &[u8] {
  let s = from_utf8(bytes).unwrap();
  assert!(s.starts_with('$'), "Expected RESP bulk string, got: {s}");
  let first_crlf = s.find("\r\n").unwrap();
  let len: usize = s[1..first_crlf].parse().unwrap();
  let start = first_crlf + 2;
  &bytes[start..start + len]
}

#[test]
fn json_get_formatting_options_test() {
  // C# TryGet：任一 INDENT/NEWLINE/SPACE 选项出现即改用默认缩进序列化器
  //（IndentedJsonSerializerOptions，WriteIndented = true），选项取值忽略；
  // System.Text.Json WriteIndented 默认 2 空格缩进 + LF 换行 + ": " 键值
  // 分隔 + 空容器不换行，与 sonic_rs::to_vec_pretty 默认 PrettyFormatter
  //（b"  " 缩进、b"\n" 换行）逐字节同构，以下期望串即 C# 输出字节
  let mut obj = GarnetJsonObject::create();
  obj
    .set(
      b"$",
      b"{\"a\":1,\"b\":[1,2],\"c\":{\"x\":\"y\"}}",
      ExistOptions::None,
    )
    .unwrap();

  // 无选项：紧凑全量串（C# DefaultJsonSerializerOptions）
  let mut out = Vec::new();
  obj.try_get(&[], &mut out, None, None, None, 2).unwrap();
  assert_eq!(
    bulk_payload(&out),
    br#"{"a":1,"b":[1,2],"c":{"x":"y"}}"#.as_slice()
  );

  const PRETTY: &[u8] = br#"{
  "a": 1,
  "b": [
    1,
    2
  ],
  "c": {
    "x": "y"
  }
}"#;

  // INDENT：零路径直出全量美化串；取值忽略（\t 与 2 空格同输出）
  out.clear();
  obj
    .try_get(&[], &mut out, Some("  "), None, None, 2)
    .unwrap();
  assert_eq!(bulk_payload(&out), PRETTY);
  out.clear();
  obj
    .try_get(&[], &mut out, Some("\t"), None, None, 2)
    .unwrap();
  assert_eq!(bulk_payload(&out), PRETTY);

  // NEWLINE："$" 路径恒回 JSONPath 结果数组形态 [全量美化串]
  out.clear();
  obj
    .try_get(&[b"$"], &mut out, None, Some("\n"), None, 2)
    .unwrap();
  let mut wrapped = Vec::with_capacity(PRETTY.len() + 2);
  wrapped.push(b'[');
  wrapped.extend_from_slice(PRETTY);
  wrapped.push(b']');
  assert_eq!(bulk_payload(&out), wrapped.as_slice());

  // SPACE：非根路径逐匹配美化后以逗号拼接在结果数组内
  out.clear();
  obj
    .try_get(&[b"$.c"], &mut out, None, None, Some(" "), 2)
    .unwrap();
  assert_eq!(bulk_payload(&out), b"[{\n  \"x\": \"y\"\n}]".as_slice());

  // 多路径 + 选项：{"path": [匹配...]} 形态；$.b 单匹配为数组节点 [1,2]，
  // 美化后外层再包结果数组 → [[..]]
  out.clear();
  obj
    .try_get(
      &[b"$.b", b"$.c"],
      &mut out,
      Some("  "),
      Some("\n"),
      Some(" "),
      2,
    )
    .unwrap();
  assert_eq!(
    bulk_payload(&out),
    br#"{"$.b":[[
  1,
  2
]],"$.c":[{
  "x": "y"
}]}"#
      .as_slice()
  );
}

#[test]
fn json_set_get_error_fidelity_test() {
  let mut obj = GarnetJsonObject::create();
  let err = obj
    .set(b"$", b"{\"invalid\": ", ExistOptions::None)
    .unwrap_err();
  let msg = err.to_string();
  assert!(!msg.is_empty());
}

/// 命令目录 COMMAND_INFOS 与按名解析 JsonCommand::ALL 同一名单源（枚举
/// `JsonCommand::name`）：新增命令漏登记任一侧由本用例挡住，杜绝目录与
/// 解析判定漂移
#[test]
fn directory_and_match_share_one_name_source() {
  use wext_json::{COMMAND_INFOS, JsonCommand, is_command_registered};
  assert_eq!(
    COMMAND_INFOS.len(),
    JsonCommand::ALL.len(),
    "命令目录与枚举全集条目数漂移"
  );
  for cmd in JsonCommand::ALL.iter().copied() {
    let name = cmd.name();
    assert!(
      COMMAND_INFOS.iter().any(|info| info.name == name),
      "命令目录缺项 {name}"
    );
    assert_eq!(JsonCommand::match_command(name.as_bytes()), Some(cmd));
    assert!(is_command_registered(name));
  }
}
