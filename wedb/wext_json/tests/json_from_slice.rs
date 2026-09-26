//! GarnetJsonObject::from_slice 零拷贝入口用例
//!
//! 对标 test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs 的
//! SerializationTest / SaveRecoverTest 落盘-重载分层：rust 侧信封载荷即 JSON 文本
//! 字节，反序列化直借切片（不再经 `Read` 流 read_to_end 二次堆拷贝）。
//!
//! 自研依据: sonic_rs 切片装载（技术选型契约）

use sonic_rs::JsonValueTrait;
use wext_json::{GarnetJsonObject, SetResult};
use wresp::options::ExistOptions;

/// 空载荷 = 空对象（键缺失新建形态），与 create() 同构
#[test]
fn from_slice_empty_payload_yields_empty_object() {
  let obj = GarnetJsonObject::from_slice(&[]).unwrap();
  assert!(obj.root_node.is_none());
  assert!(obj.is_empty());
}

/// 载荷切片直读 DOM：零拷贝借用，无需临时缓冲
#[test]
fn from_slice_parses_payload_in_place() {
  let obj = GarnetJsonObject::from_slice(br#"{"a":1,"b":"x"}"#).unwrap();
  let root = obj.root_node.unwrap();
  assert_eq!(root["a"].as_i64(), Some(1));
  assert_eq!(root["b"].as_str(), Some("x"));
}

/// 非法 JSON 文本回错，由命令层裁决应答
#[test]
fn from_slice_rejects_malformed_payload() {
  assert!(GarnetJsonObject::from_slice(b"{\"a\": ").is_err());
}

/// 序列化-反序列化往返：与 C# SerializationTest 同一断言口径
#[test]
fn serialize_then_from_slice_roundtrip() {
  let mut obj = GarnetJsonObject::create();
  obj
    .set(b"$", br#"{"f":{"a":1}}"#, ExistOptions::None)
    .unwrap();
  assert_eq!(
    obj.set(b"$.b", b"2", ExistOptions::None).unwrap(),
    SetResult::Success
  );

  let mut buf = Vec::new();
  obj.serialize_object(&mut buf).unwrap();

  let back = GarnetJsonObject::from_slice(&buf).unwrap();
  let mut out = Vec::new();
  back
    .try_get(&[b"$.f"], &mut out, None, None, None, 2)
    .unwrap();
  assert!(String::from_utf8(out).unwrap().contains(r#""a":1"#));
}
