use std::str::from_utf8;

use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};

use crate::RespValue;

/// JSON 值转换为 RESP3 数据结构
pub fn json_to_resp(val: &Value) -> RespValue {
  if val.is_null() {
    RespValue::Null
  } else if let Some(b) = val.as_bool() {
    RespValue::Bool(b)
  } else if let Some(i) = val.as_i64() {
    RespValue::Int(i)
  } else if let Some(u) = val.as_u64() {
    if u <= i64::MAX as u64 {
      RespValue::Int(u as i64)
    } else {
      RespValue::Float(u as f64)
    }
  } else if let Some(f) = val.as_f64() {
    RespValue::Float(f)
  } else if let Some(s) = val.as_str() {
    RespValue::Blob(s.as_bytes().to_vec())
  } else if let Some(arr) = val.as_array() {
    let elements = arr.iter().map(json_to_resp).collect();
    RespValue::Arr(elements)
  } else if let Some(obj) = val.as_object() {
    let mut map = Vec::with_capacity(obj.len());
    for (k, v) in obj.iter() {
      map.push((RespValue::Blob(k.as_bytes().to_vec()), json_to_resp(v)));
    }
    RespValue::Map(map)
  } else {
    RespValue::Null
  }
}

/// JSON 值转换为对标 Apache Kvrocks 的 RESP 扁平表达（TransformResp）
pub fn json_to_resp_flat(val: &Value) -> RespValue {
  if val.is_null() {
    RespValue::Null
  } else if let Some(b) = val.as_bool() {
    RespValue::Simple(if b {
      "true".to_string()
    } else {
      "false".to_string()
    })
  } else if let Some(i) = val.as_i64() {
    RespValue::Int(i)
  } else if let Some(u) = val.as_u64() {
    RespValue::Int(u as i64)
  } else if let Some(f) = val.as_f64() {
    let mut zmij_buf = zmij::Buffer::new();
    let formatted = zmij_buf.format(f);
    RespValue::Blob(formatted.as_bytes().to_vec())
  } else if let Some(s) = val.as_str() {
    RespValue::Blob(s.as_bytes().to_vec())
  } else if let Some(arr) = val.as_array() {
    let mut elements = Vec::with_capacity(arr.len() + 1);
    elements.push(RespValue::Simple("[".to_string()));
    for item in arr.iter() {
      elements.push(json_to_resp_flat(item));
    }
    RespValue::Arr(elements)
  } else if let Some(obj) = val.as_object() {
    let mut elements = Vec::with_capacity(obj.len() * 2 + 1);
    elements.push(RespValue::Simple("{".to_string()));
    for (k, v) in obj.iter() {
      elements.push(RespValue::Blob(k.as_bytes().to_vec()));
      elements.push(json_to_resp_flat(v));
    }
    RespValue::Arr(elements)
  } else {
    RespValue::Null
  }
}

/// 将 RESP3 值转换为 sonic_rs::Value
pub fn resp_to_json(resp: &RespValue) -> Value {
  match resp {
    RespValue::Null => sonic_rs::json!(null),
    RespValue::Bool(b) => sonic_rs::json!(*b),
    RespValue::Int(i) => sonic_rs::json!(*i),
    RespValue::Float(f) => sonic_rs::json!(*f),
    RespValue::Simple(s) | RespValue::Error(s) => sonic_rs::json!(s.as_str()),
    RespValue::Blob(b) => match from_utf8(b) {
      Ok(s) => sonic_rs::json!(s),
      Err(_) => sonic_rs::json!(b),
    },
    RespValue::Arr(elements) | RespValue::Set(elements) | RespValue::Push(elements) => {
      let list: Vec<Value> = elements.iter().map(resp_to_json).collect();
      sonic_rs::json!(list)
    }
    RespValue::Map(pairs) => {
      let mut obj = sonic_rs::Object::with_capacity(pairs.len());
      for (k, v) in pairs {
        if let Some(key_str) = k.as_str() {
          obj.insert(key_str, resp_to_json(v));
        } else {
          let key_str = format!("{k:?}");
          obj.insert(&key_str, resp_to_json(v));
        }
      }
      Value::from(obj)
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_json_to_resp_and_flat() {
    let json_val = sonic_rs::json!({
        "str": "hello",
        "int": 123,
        "float": 1.25,
        "bool": true,
        "null": null,
        "arr": [1, 2, "three"],
        "nested": { "k": "v" }
    });

    let resp3 = json_to_resp(&json_val);
    assert!(matches!(resp3, RespValue::Map(_)));

    let flat = json_to_resp_flat(&json_val);
    if let RespValue::Arr(elements) = flat {
      assert_eq!(elements[0], RespValue::Simple("{".to_string()));
    } else {
      panic!("Expected flat array for json object");
    }

    let back_json = resp_to_json(&resp3);
    assert_eq!(back_json["str"].as_str(), Some("hello"));
    assert_eq!(back_json["int"].as_i64(), Some(123));
    assert_eq!(back_json["bool"].as_bool(), Some(true));
    assert!(back_json["null"].is_null());
  }
}
