use std::io::{Read, Write};

use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};

use crate::error::{Error, Result};

/// JSON 对象（对标 modules/GarnetJSON/GarnetJsonObject.cs:GarnetJsonObject）
pub struct GarnetJsonObject {
  pub value: Value,
}

impl GarnetJsonObject {
  pub fn create() -> Self {
    Self {
      value: sonic_rs::json!({}),
    }
  }

  /// 从读取器解析 JSON 文本（sonic-rs 单遍读取解析，无 UTF-8 中转校验）
  pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self> {
    let value: Value = sonic_rs::from_reader(reader).map_err(|e| Error::Json(e.to_string()))?;
    Ok(Self { value })
  }

  pub fn clone_object(&self) -> Self {
    Self {
      value: self.value.clone(),
    }
  }

  /// 序列化 JSON 文本（sonic-rs 单次序列化为字节缓冲后直写）
  pub fn serialize_object<W: Write>(&self, writer: &mut W) -> Result<()> {
    let buf = sonic_rs::to_vec(&self.value).map_err(|e| Error::Json(e.to_string()))?;
    writer
      .write_all(&buf)
      .map_err(|e| Error::Json(e.to_string()))
  }

  /// 按路径查询节点（在 sonic_rs DOM 上直接执行，零序列化往返）
  pub fn try_get(&self, path_str: &str) -> Result<Vec<Value>> {
    let s = path_str.trim();
    if s == "$" || s.is_empty() {
      return Ok(vec![self.value.clone()]);
    }

    let segments = parse_json_path(s)?;
    let hits = evaluate_path(&self.value, &segments);
    Ok(hits.into_iter().cloned().collect())
  }

  pub fn try_get_root(&self) -> Result<&Value> {
    Ok(&self.value)
  }
}

#[derive(Debug, PartialEq, Eq)]
enum Segment {
  Field(String),
  Index(isize),
  Wildcard,
  RecursiveField(String),
  RecursiveWildcard,
  Slice {
    start: Option<isize>,
    end: Option<isize>,
    step: Option<isize>,
  },
}

fn parse_json_path(mut s: &str) -> Result<Vec<Segment>> {
  s = s.trim();
  if s.starts_with('$') {
    s = &s[1..];
  }
  let mut segments = Vec::new();
  while !s.is_empty() {
    if s.starts_with("..") {
      s = &s[2..];
      if s.starts_with('*') {
        s = &s[1..];
        segments.push(Segment::RecursiveWildcard);
      } else if !s.starts_with('[') {
        let end = s.find(['.', '[']).unwrap_or(s.len());
        let field = &s[..end];
        if !field.is_empty() {
          segments.push(Segment::RecursiveField(field.to_string()));
          s = &s[end..];
        }
      }
    } else if s.starts_with('.') {
      s = &s[1..];
      if s.starts_with('*') {
        s = &s[1..];
        segments.push(Segment::Wildcard);
      } else {
        let end = s.find(['.', '[']).unwrap_or(s.len());
        let field = &s[..end];
        if !field.is_empty() {
          segments.push(Segment::Field(field.to_string()));
          s = &s[end..];
        }
      }
    } else if s.starts_with('[') {
      let close = s
        .find(']')
        .ok_or_else(|| Error::Json("Unclosed bracket in JSONPath".into()))?;
      let inner = s[1..close].trim();
      s = &s[close + 1..];
      if inner == "*" {
        segments.push(Segment::Wildcard);
      } else if (inner.starts_with('\'') && inner.ends_with('\''))
        || (inner.starts_with('"') && inner.ends_with('"'))
      {
        let field = &inner[1..inner.len() - 1];
        segments.push(Segment::Field(field.to_string()));
      } else if inner.contains(':') {
        let parts: Vec<&str> = inner.split(':').collect();
        let start = parts.first().and_then(|p| p.trim().parse::<isize>().ok());
        let end = parts.get(1).and_then(|p| p.trim().parse::<isize>().ok());
        let step = parts.get(2).and_then(|p| p.trim().parse::<isize>().ok());
        segments.push(Segment::Slice { start, end, step });
      } else if let Ok(idx) = inner.parse::<isize>() {
        segments.push(Segment::Index(idx));
      } else {
        segments.push(Segment::Field(inner.to_string()));
      }
    } else {
      let end = s.find(['.', '[']).unwrap_or(s.len());
      let field = &s[..end];
      segments.push(Segment::Field(field.to_string()));
      s = &s[end..];
    }
  }
  Ok(segments)
}

fn evaluate_path<'a>(root: &'a Value, segments: &[Segment]) -> Vec<&'a Value> {
  let mut current = vec![root];
  for seg in segments {
    let mut next = Vec::new();
    for v in current {
      match seg {
        Segment::Field(field) => {
          if let Some(val) = v.get(field.as_str()) {
            next.push(val);
          }
        }
        Segment::Index(idx) => {
          if let Some(arr) = v.as_array() {
            let actual_idx = if *idx >= 0 {
              *idx as usize
            } else {
              arr.len().wrapping_add(*idx as usize)
            };
            if let Some(val) = arr.get(actual_idx) {
              next.push(val);
            }
          }
        }
        Segment::Wildcard => {
          if let Some(obj) = v.as_object() {
            for (_, val) in obj.iter() {
              next.push(val);
            }
          } else if let Some(arr) = v.as_array() {
            for val in arr.iter() {
              next.push(val);
            }
          }
        }
        Segment::RecursiveField(field) => {
          collect_recursive(v, &mut |node| {
            if let Some(sub) = node.get(field.as_str()) {
              next.push(sub);
            }
          });
        }
        Segment::RecursiveWildcard => {
          collect_recursive(v, &mut |node| {
            next.push(node);
          });
        }
        Segment::Slice { start, end, step } => {
          if let Some(arr) = v.as_array() {
            let len = arr.len() as isize;
            let step = step.unwrap_or(1);
            if step > 0 {
              let start = start
                .map(|s| if s < 0 { (len + s).max(0) } else { s.min(len) })
                .unwrap_or(0);
              let end = end
                .map(|e| if e < 0 { (len + e).max(0) } else { e.min(len) })
                .unwrap_or(len);
              let mut i = start;
              while i < end {
                if let Some(val) = arr.get(i as usize) {
                  next.push(val);
                }
                i += step;
              }
            }
          }
        }
      }
    }
    current = next;
  }
  current
}

fn collect_recursive<'a, F>(v: &'a Value, f: &mut F)
where
  F: FnMut(&'a Value),
{
  if let Some(obj) = v.as_object() {
    for (_, val) in obj.iter() {
      f(val);
      collect_recursive(val, f);
    }
  } else if let Some(arr) = v.as_array() {
    for val in arr.iter() {
      f(val);
      collect_recursive(val, f);
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn try_get_direct_query() {
    let obj = GarnetJsonObject {
      value: sonic_rs::json!({ "a": { "b": [1, 2, 3] } }),
    };
    let hits = obj.try_get("$.a.b[1]").unwrap();
    assert_eq!(hits, vec![sonic_rs::json!(2)]);
  }

  #[test]
  fn serialize_deserialize_round_trip() {
    let mut buf = Vec::new();
    let obj = GarnetJsonObject {
      value: sonic_rs::json!({ "k": "v", "n": 7 }),
    };
    obj.serialize_object(&mut buf).unwrap();
    let back = GarnetJsonObject::deserialize(&mut &buf[..]).unwrap();
    assert_eq!(back.value, obj.value);
  }
}
