use std::io::{Read, Write};

use jsonpath_rust::JsonPath;
use serde_json::Value;

use crate::error::{Error, Result};

/// JSON 对象（对标 modules/GarnetJSON/GarnetJsonObject.cs:GarnetJsonObject）
///
/// DOM 统一采用 [`serde_json::Value`]：jsonpath_rust 的查询 API 只接受该类型，
/// 原实现以 sonic_rs::Value 存储导致每次查询需 2 次全文序列化往返、每命中节点
/// 再 2 次（4 次字符串转换）；改为 serde_json DOM 后查询零转换，解析/序列化
/// 边界仍统一走 sonic-rs（高性能 SIMD 实现，遵守"不用 serde_json 引擎"约束，
/// serde_json 仅作为 DOM 类型存在）
pub struct GarnetJsonObject {
  pub value: Value,
}

impl GarnetJsonObject {
  pub fn create() -> Self {
    Self {
      value: serde_json::json!({}),
    }
  }

  /// 从读取器解析 JSON 文本（sonic-rs 单次解析，无中间 DOM 转换）
  pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self> {
    let mut buf = String::new();
    reader
      .read_to_string(&mut buf)
      .map_err(|e| Error::Json(e.to_string()))?;
    let value: Value = sonic_rs::from_str(&buf).map_err(|e| Error::Json(e.to_string()))?;
    Ok(Self { value })
  }

  pub fn clone_object(&self) -> Self {
    Self {
      value: self.value.clone(),
    }
  }

  /// 序列化 JSON 文本（sonic-rs 单次序列化）
  pub fn serialize_object<W: Write>(&self, writer: &mut W) -> Result<()> {
    let s = sonic_rs::to_string(&self.value).map_err(|e| Error::Json(e.to_string()))?;
    writer
      .write_all(s.as_bytes())
      .map_err(|e| Error::Json(e.to_string()))?;
    Ok(())
  }

  /// 按路径查询节点（在 DOM 上直接执行，零序列化往返；命中 N 个节点为 O(N) 克隆）
  pub fn try_get(&self, path_str: &str) -> Result<Vec<Value>> {
    let found = self
      .value
      .query_with_path(path_str)
      .map_err(|e| Error::Json(e.to_string()))?;
    Ok(found.into_iter().map(|r| r.val().clone()).collect())
  }

  pub fn try_get_root(&self) -> Result<&Value> {
    Ok(&self.value)
  }
  pub fn try_get_to_writer<W: Write>(&self, _path_str: &str, _writer: &mut W) -> Result<()> {
    Err(Error::NotImplemented)
  }
  pub fn get_parent_path(&self, _path_str: &str) -> Result<String> {
    Err(Error::NotImplemented)
  }
  pub fn get_property_name(&self, _path_str: &str) -> Result<String> {
    Err(Error::NotImplemented)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn try_get_direct_query() {
    let obj = GarnetJsonObject {
      value: serde_json::json!({ "a": { "b": [1, 2, 3] } }),
    };
    let hits = obj.try_get("$.a.b[1]").unwrap();
    assert_eq!(hits, vec![serde_json::json!(2)]);
  }

  #[test]
  fn serialize_deserialize_round_trip() {
    let mut buf = Vec::new();
    let obj = GarnetJsonObject {
      value: serde_json::json!({ "k": "v", "n": 7 }),
    };
    obj.serialize_object(&mut buf).unwrap();
    let back = GarnetJsonObject::deserialize(&mut &buf[..]).unwrap();
    assert_eq!(back.value, obj.value);
  }
}
