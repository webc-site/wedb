use sonic_rs::{JsonContainerTrait, JsonValueMutTrait, JsonValueTrait, Object, Value};
use std::str::from_utf8;

use crate::error::Result;

/// JSONPath 路径分段枚举
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSegment {
  Root,
  Field(String),
  Index(i64),
  Wildcard,
  RecursiveField(String),
  RecursiveWildcard,
  Slice(Option<i64>, Option<i64>),
}

/// 解析 JSONPath 路径字符串为分段序列
pub fn parse_json_path(path: &str) -> Vec<PathSegment> {
  let mut segments = Vec::new();
  let path = path.trim();
  if path.is_empty() || path == "$" {
    return vec![PathSegment::Root];
  }

  let bytes = path.as_bytes();
  let mut i = 0;
  let len = bytes.len();

  if bytes[0] == b'$' {
    segments.push(PathSegment::Root);
    i += 1;
  }

  while i < len {
    if i + 1 < len && bytes[i] == b'.' && bytes[i + 1] == b'.' {
      i += 2;
      if i < len && bytes[i] == b'*' {
        segments.push(PathSegment::RecursiveWildcard);
        i += 1;
      } else {
        let start = i;
        while i < len && bytes[i] != b'.' && bytes[i] != b'[' {
          i += 1;
        }
        let field_str = from_utf8(&bytes[start..i]).unwrap_or_default();
        if !field_str.is_empty() {
          segments.push(PathSegment::RecursiveField(field_str.to_string()));
        }
      }
    } else if bytes[i] == b'.' {
      i += 1;
      if i < len && bytes[i] == b'*' {
        segments.push(PathSegment::Wildcard);
        i += 1;
      } else {
        let start = i;
        while i < len && bytes[i] != b'.' && bytes[i] != b'[' {
          i += 1;
        }
        let field_str = from_utf8(&bytes[start..i]).unwrap_or_default();
        if !field_str.is_empty() {
          segments.push(PathSegment::Field(field_str.to_string()));
        }
      }
    } else if bytes[i] == b'[' {
      i += 1;
      let start = i;
      while i < len && bytes[i] != b']' {
        i += 1;
      }
      let inner = from_utf8(&bytes[start..i]).unwrap_or_default().trim();
      if i < len && bytes[i] == b']' {
        i += 1;
      }
      if inner == "*" {
        segments.push(PathSegment::Wildcard);
      } else if (inner.starts_with('\'') && inner.ends_with('\''))
        || (inner.starts_with('"') && inner.ends_with('"'))
      {
        if inner.len() >= 2 {
          segments.push(PathSegment::Field(inner[1..inner.len() - 1].to_string()));
        }
      } else if let Some((start_str, end_str)) = inner.split_once(':') {
        let s_opt = start_str.trim().parse::<i64>().ok();
        let e_opt = end_str.trim().parse::<i64>().ok();
        segments.push(PathSegment::Slice(s_opt, e_opt));
      } else if let Ok(idx) = inner.parse::<i64>() {
        segments.push(PathSegment::Index(idx));
      } else if !inner.is_empty() {
        segments.push(PathSegment::Field(inner.to_string()));
      }
    } else {
      let start = i;
      while i < len && bytes[i] != b'.' && bytes[i] != b'[' {
        i += 1;
      }
      let field_str = from_utf8(&bytes[start..i]).unwrap_or_default();
      if !field_str.is_empty() {
        segments.push(PathSegment::Field(field_str.to_string()));
      }
    }
  }

  if segments.is_empty() {
    vec![PathSegment::Root]
  } else {
    segments
  }
}

/// 执行 JSONPath 查询，返回所有匹配节点的不可变引用
pub fn json_path_query<'a>(root: &'a Value, path: &str) -> Vec<&'a Value> {
  let segments = parse_json_path(path);
  let mut current = vec![root];

  for segment in segments {
    if segment == PathSegment::Root {
      continue;
    }
    let mut next = Vec::new();
    for node in current {
      match &segment {
        PathSegment::Root => next.push(node),
        PathSegment::Field(field) => {
          if let Some(obj) = node.as_object()
            && let Some(child) = obj.get(&field.as_str())
          {
            next.push(child);
          }
        }
        PathSegment::Index(idx) => {
          if let Some(arr) = node.as_array() {
            let len = arr.len() as i64;
            let norm = if *idx < 0 { len + *idx } else { *idx };
            if norm >= 0 && (norm as usize) < arr.len() {
              next.push(&arr[norm as usize]);
            }
          }
        }
        PathSegment::Wildcard => {
          if let Some(obj) = node.as_object() {
            for (_, v) in obj.iter() {
              next.push(v);
            }
          } else if let Some(arr) = node.as_array() {
            for v in arr.iter() {
              next.push(v);
            }
          }
        }
        PathSegment::Slice(start_opt, end_opt) => {
          if let Some(arr) = node.as_array() {
            let len = arr.len() as i64;
            let start = match start_opt {
              Some(s) if *s < 0 => (len + *s).max(0) as usize,
              Some(s) => (*s as usize).min(arr.len()),
              None => 0,
            };
            let end = match end_opt {
              Some(e) if *e < 0 => (len + *e).max(0) as usize,
              Some(e) => (*e as usize).min(arr.len()),
              None => arr.len(),
            };
            if start < end && start < arr.len() {
              for item in &arr[start..end.min(arr.len())] {
                next.push(item);
              }
            }
          }
        }
        PathSegment::RecursiveField(field) => {
          collect_recursive_field(node, field.as_str(), &mut next);
        }
        PathSegment::RecursiveWildcard => {
          collect_all_recursive(node, &mut next);
        }
      }
    }
    current = next;
    if current.is_empty() {
      break;
    }
  }
  current
}

fn collect_recursive_field<'a>(node: &'a Value, field: &str, out: &mut Vec<&'a Value>) {
  if let Some(obj) = node.as_object() {
    if let Some(v) = obj.get(&field) {
      out.push(v);
    }
    for (_, child) in obj.iter() {
      collect_recursive_field(child, field, out);
    }
  } else if let Some(arr) = node.as_array() {
    for child in arr.iter() {
      collect_recursive_field(child, field, out);
    }
  }
}

fn collect_all_recursive<'a>(node: &'a Value, out: &mut Vec<&'a Value>) {
  if let Some(obj) = node.as_object() {
    for (_, child) in obj.iter() {
      out.push(child);
      collect_all_recursive(child, out);
    }
  } else if let Some(arr) = node.as_array() {
    for child in arr.iter() {
      out.push(child);
      collect_all_recursive(child, out);
    }
  }
}

/// 执行 JSONPath 原地替换并应用回调函数
pub fn json_path_replace<F>(root: &mut Value, path: &str, mut f: F) -> usize
where
  F: FnMut(&mut Value),
{
  let segments = parse_json_path(path);
  let real_segments: Vec<PathSegment> = segments
    .into_iter()
    .filter(|s| *s != PathSegment::Root)
    .collect();

  if real_segments.is_empty() {
    f(root);
    return 1;
  }

  replace_step(root, &real_segments, &mut f)
}

fn replace_step<F>(node: &mut Value, segments: &[PathSegment], f: &mut F) -> usize
where
  F: FnMut(&mut Value),
{
  if segments.is_empty() {
    f(node);
    return 1;
  }

  let mut count = 0;
  match &segments[0] {
    PathSegment::Root => replace_step(node, &segments[1..], f),
    PathSegment::Field(field) => {
      if let Some(obj) = node.as_object_mut()
        && let Some(child) = obj.get_mut(&field.as_str())
      {
        count += replace_step(child, &segments[1..], f);
      }
      count
    }
    PathSegment::Index(idx) => {
      if let Some(arr) = node.as_array_mut() {
        let len = arr.len() as i64;
        let norm = if *idx < 0 { len + *idx } else { *idx };
        if norm >= 0 && (norm as usize) < arr.len() {
          let idx_usize = norm as usize;
          count += replace_step(&mut arr[idx_usize], &segments[1..], f);
        }
      }
      count
    }
    PathSegment::Wildcard => {
      if let Some(obj) = node.as_object_mut() {
        for (_, child) in obj.iter_mut() {
          count += replace_step(child, &segments[1..], f);
        }
      } else if let Some(arr) = node.as_array_mut() {
        for child in arr.iter_mut() {
          count += replace_step(child, &segments[1..], f);
        }
      }
      count
    }
    PathSegment::Slice(start_opt, end_opt) => {
      if let Some(arr) = node.as_array_mut() {
        let len = arr.len() as i64;
        let start = match start_opt {
          Some(s) if *s < 0 => (len + *s).max(0) as usize,
          Some(s) => (*s as usize).min(arr.len()),
          None => 0,
        };
        let end = match end_opt {
          Some(e) if *e < 0 => (len + *e).max(0) as usize,
          Some(e) => (*e as usize).min(arr.len()),
          None => arr.len(),
        };
        if start < end && start < arr.len() {
          let e = end.min(arr.len());
          for child in &mut arr[start..e] {
            count += replace_step(child, &segments[1..], f);
          }
        }
      }
      count
    }
    PathSegment::RecursiveField(field) => {
      replace_recursive_field(node, field.as_str(), &segments[1..], f)
    }
    PathSegment::RecursiveWildcard => replace_recursive_all(node, &segments[1..], f),
  }
}

fn replace_recursive_field<F>(
  node: &mut Value,
  field: &str,
  rest_segments: &[PathSegment],
  f: &mut F,
) -> usize
where
  F: FnMut(&mut Value),
{
  let mut count = 0;
  if let Some(obj) = node.as_object_mut() {
    for (k, child) in obj.iter_mut() {
      if k == field {
        count += replace_step(child, rest_segments, f);
      }
      count += replace_recursive_field(child, field, rest_segments, f);
    }
  } else if let Some(arr) = node.as_array_mut() {
    for child in arr.iter_mut() {
      count += replace_recursive_field(child, field, rest_segments, f);
    }
  }
  count
}

fn replace_recursive_all<F>(node: &mut Value, rest_segments: &[PathSegment], f: &mut F) -> usize
where
  F: FnMut(&mut Value),
{
  let mut count = 0;
  if let Some(obj) = node.as_object_mut() {
    for (_, child) in obj.iter_mut() {
      count += replace_step(child, rest_segments, f);
      count += replace_recursive_all(child, rest_segments, f);
    }
  } else if let Some(arr) = node.as_array_mut() {
    for child in arr.iter_mut() {
      count += replace_step(child, rest_segments, f);
      count += replace_recursive_all(child, rest_segments, f);
    }
  }
  count
}

/// 设置指定路径处的 JSON 节点（支持 NX / XX 选项）
pub fn json_path_set(root: &mut Value, path: &str, new_val: Value, nx: bool, xx: bool) -> bool {
  let is_root = path == "$" || path == "." || path.is_empty();
  if is_root {
    if xx && root.is_null() {
      return false;
    }
    *root = new_val;
    return true;
  }

  let existing = json_path_query(root, path);
  if !existing.is_empty() {
    if nx {
      return false;
    }
    let cnt = json_path_replace(root, path, |target| {
      *target = new_val.clone();
    });
    cnt > 0
  } else {
    if xx {
      return false;
    }
    set_value_by_path(root, path, new_val)
  }
}

/// 删除指定路径处的节点
pub fn json_path_del(root: &mut Value, path: &str) -> usize {
  if path == "$" || path == "." || path.is_empty() {
    *root = Value::from(());
    return 1;
  }

  del_value_by_path(root, path)
}

/// 获取单个路径的值（兼容包装）
pub fn get_value_by_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
  let list = json_path_query(root, path);
  list.first().copied()
}

/// 获取单个路径的可变引用（兼容包装）
pub fn get_value_by_path_mut<'a>(root: &'a mut Value, path: &str) -> Option<&'a mut Value> {
  if path == "." || path == "$" || path.is_empty() {
    return Some(root);
  }

  let clean_path = path.trim_start_matches('$').trim_start_matches('.');
  let mut current = root;

  for part in clean_path.split('.') {
    if part.is_empty() {
      continue;
    }
    if let Ok(idx) = part.parse::<usize>()
      && current.is_array()
    {
      let arr = current.as_array_mut()?;
      current = arr.get_mut(idx)?;
      continue;
    }
    let obj = current.as_object_mut()?;
    current = obj.get_mut(&part)?;
  }

  Some(current)
}

/// 修改指定路径处的节点（标准创建路径包装）
pub fn set_value_by_path(root: &mut Value, path: &str, new_val: Value) -> bool {
  if path == "." || path == "$" || path.is_empty() {
    *root = new_val;
    return true;
  }

  let clean_path = path.trim_start_matches('$').trim_start_matches('.');
  let parts: Vec<&str> = clean_path.split('.').filter(|s| !s.is_empty()).collect();
  if parts.is_empty() {
    *root = new_val;
    return true;
  }

  let mut current = root;
  for &part in &parts[..parts.len() - 1] {
    if !current.is_object() && !current.is_array() {
      *current = Value::from(Object::new());
    }
    if let Some(obj) = current.as_object_mut() {
      if !obj.contains_key(&part) {
        obj.insert(part, Value::from(Object::new()));
      }
      if let Some(next) = obj.get_mut(&part) {
        current = next;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  let last_key = parts[parts.len() - 1];
  if let Some(obj) = current.as_object_mut() {
    obj.insert(last_key, new_val);
    true
  } else {
    false
  }
}

/// 删除指定路径处的节点
pub fn del_value_by_path(root: &mut Value, path: &str) -> usize {
  if path == "." || path == "$" || path.is_empty() {
    *root = Value::from(());
    return 1;
  }

  let clean_path = path.trim_start_matches('$').trim_start_matches('.');
  let parts: Vec<&str> = clean_path.split('.').filter(|s| !s.is_empty()).collect();
  if parts.is_empty() {
    *root = Value::from(());
    return 1;
  }

  let mut current = root;
  for &part in &parts[..parts.len() - 1] {
    if let Some(obj) = current.as_object_mut() {
      if let Some(next) = obj.get_mut(&part) {
        current = next;
      } else {
        return 0;
      }
    } else {
      return 0;
    }
  }

  let last_key = parts[parts.len() - 1];
  if let Some(obj) = current.as_object_mut() {
    if obj.remove(&last_key).is_some() {
      1
    } else {
      0
    }
  } else {
    0
  }
}

/// 获取 JSON 节点类型名称
pub fn json_type_str(v: &Value) -> &'static str {
  if v.is_null() {
    "null"
  } else if v.is_boolean() {
    "boolean"
  } else if v.is_i64() || v.is_u64() {
    "integer"
  } else if v.is_number() {
    "number"
  } else if v.is_str() {
    "string"
  } else if v.is_array() {
    "array"
  } else if v.is_object() {
    "object"
  } else {
    "null"
  }
}

/// 数值运算（NUMINCRBY / NUMMULTBY）
pub fn json_num_op(
  root: &mut Value,
  path: &str,
  num: f64,
  is_incr: bool,
) -> Result<Vec<Option<Value>>> {
  let mut results = Vec::new();
  json_path_replace(root, path, |val| {
    if let Some(cur_num) = val.as_f64() {
      let res = if is_incr {
        cur_num + num
      } else {
        cur_num * num
      };
      if res.is_infinite() || res.is_nan() {
        results.push(None);
        return;
      }
      if res.fract() == 0.0 && res >= (i64::MIN as f64) && res <= (i64::MAX as f64) {
        let int_v = res as i64;
        *val = sonic_rs::json!(int_v);
        results.push(Some(sonic_rs::json!(int_v)));
      } else {
        *val = sonic_rs::json!(res);
        results.push(Some(sonic_rs::json!(res)));
      }
    } else {
      results.push(None);
    }
  });
  Ok(results)
}

/// 布尔取反操作（TOGGLE）
pub fn json_toggle(root: &mut Value, path: &str) -> Vec<Option<bool>> {
  let mut results = Vec::new();
  json_path_replace(root, path, |val| {
    if let Some(b) = val.as_bool() {
      let new_b = !b;
      *val = sonic_rs::json!(new_b);
      results.push(Some(new_b));
    } else {
      results.push(None);
    }
  });
  results
}

/// 清空容器或将数字置零（CLEAR）
pub fn json_clear(root: &mut Value, path: &str) -> usize {
  let mut count = 0;
  json_path_replace(root, path, |val| {
    if let Some(arr) = val.as_array_mut() {
      if !arr.is_empty() {
        arr.clear();
        count += 1;
      }
    } else if let Some(obj) = val.as_object_mut() {
      if !obj.is_empty() {
        obj.clear();
        count += 1;
      }
    } else if let Some(n) = val.as_f64()
      && n != 0.0
    {
      *val = sonic_rs::json!(0);
      count += 1;
    }
  });
  count
}

/// 字符串追加（STRAPPEND）
pub fn json_str_append(root: &mut Value, path: &str, append_str: &str) -> Vec<Option<usize>> {
  let mut results = Vec::new();
  json_path_replace(root, path, |val| {
    if let Some(s) = val.as_str() {
      let mut new_s = s.to_string();
      new_s.push_str(append_str);
      let len = new_s.len();
      *val = sonic_rs::json!(new_s);
      results.push(Some(len));
    } else {
      results.push(None);
    }
  });
  results
}

/// 获取字符串长度（STRLEN）
pub fn json_str_len(root: &Value, path: &str) -> Vec<Option<usize>> {
  let nodes = json_path_query(root, path);
  nodes
    .into_iter()
    .map(|v| v.as_str().map(|s| s.len()))
    .collect()
}

/// 数组末尾追加元素（ARRAPPEND）
pub fn json_arr_append(root: &mut Value, path: &str, values: Vec<Value>) -> Vec<Option<usize>> {
  let mut results = Vec::new();
  json_path_replace(root, path, |val| {
    if let Some(arr) = val.as_array_mut() {
      for item in &values {
        arr.push(item.clone());
      }
      results.push(Some(arr.len()));
    } else {
      results.push(None);
    }
  });
  results
}

/// 数组插入元素（ARRINSERT）
pub fn json_arr_insert(
  root: &mut Value,
  path: &str,
  index: i64,
  values: Vec<Value>,
) -> Vec<Option<usize>> {
  let mut results = Vec::new();
  json_path_replace(root, path, |val| {
    if let Some(arr) = val.as_array_mut() {
      let len = arr.len() as i64;
      if index > len || index < -len {
        results.push(None);
        return;
      }
      let idx = if index < 0 {
        (len + index).max(0) as usize
      } else {
        index as usize
      };
      for (offset, item) in values.iter().enumerate() {
        arr.insert(idx + offset, item.clone());
      }
      results.push(Some(arr.len()));
    } else {
      results.push(None);
    }
  });
  results
}

/// 数组弹出元素（ARRPOP）
pub fn json_arr_pop(root: &mut Value, path: &str, index: i64) -> Vec<Option<Value>> {
  let mut results: Vec<Option<Value>> = Vec::new();
  json_path_replace(root, path, |val| {
    if let Some(arr) = val.as_array_mut() {
      if arr.is_empty() {
        results.push(None);
        return;
      }
      let len = arr.len() as i64;
      let idx = if index < 0 {
        (len + index).max(0) as usize
      } else {
        (index as usize).min(arr.len() - 1)
      };
      if idx < arr.len() {
        let popped = arr.get(idx).cloned();
        arr.remove(idx);
        results.push(popped);
      } else {
        results.push(None);
      }
    } else {
      results.push(None);
    }
  });
  results
}

/// 数组截取（ARRTRIM）
pub fn json_arr_trim(root: &mut Value, path: &str, start: i64, stop: i64) -> Vec<Option<usize>> {
  let mut results = Vec::new();
  json_path_replace(root, path, |val| {
    if let Some(arr) = val.as_array_mut() {
      let len = arr.len() as i64;
      let s = if start < 0 {
        (len + start).max(0) as usize
      } else {
        start.min(len) as usize
      };
      let e = if stop < 0 {
        (len + stop).max(0) as usize
      } else {
        stop.min(len.saturating_sub(1)) as usize
      };
      if s <= e && s < arr.len() {
        let slice = arr[s..=e.min(arr.len() - 1)].to_vec();
        arr.clear();
        for item in slice {
          arr.push(item);
        }
      } else {
        arr.clear();
      }
      results.push(Some(arr.len()));
    } else {
      results.push(None);
    }
  });
  results
}

/// 数组查找元素索引（ARRINDEX）
pub fn json_arr_index(
  root: &Value,
  path: &str,
  needle: &Value,
  start: i64,
  stop: i64,
) -> Vec<Option<i64>> {
  let nodes = json_path_query(root, path);
  nodes
    .into_iter()
    .map(|v| {
      if let Some(arr) = v.as_array() {
        let len = arr.len() as i64;
        let s = if start < 0 {
          (len + start).max(0) as usize
        } else {
          start.min(len) as usize
        };
        let e = if stop == 0 {
          len as usize
        } else if stop < 0 {
          (len + stop).max(0) as usize
        } else {
          (stop as usize).min(arr.len())
        };
        for i in s..e.min(arr.len()) {
          if &arr[i] == needle {
            return Some(i as i64);
          }
        }
        Some(-1)
      } else {
        None
      }
    })
    .collect()
}

/// 获取对象字段键列表（OBJKEYS）
pub fn json_obj_keys(root: &Value, path: &str) -> Vec<Option<Vec<String>>> {
  let nodes = json_path_query(root, path);
  nodes
    .into_iter()
    .map(|v| {
      v.as_object()
        .map(|obj| obj.iter().map(|(k, _)| k.to_string()).collect())
    })
    .collect()
}

/// 获取对象字段个数（OBJLEN）
pub fn json_obj_len(root: &Value, path: &str) -> Vec<Option<usize>> {
  let nodes = json_path_query(root, path);
  nodes
    .into_iter()
    .map(|v| v.as_object().map(|obj| obj.len()))
    .collect()
}

/// RFC 7396 JSON Merge Patch 合并
pub fn json_merge_patch(target: &mut Value, patch: &Value) {
  if let Some(patch_obj) = patch.as_object() {
    if !target.is_object() {
      *target = sonic_rs::json!({});
    }
    if let Some(target_obj) = target.as_object_mut() {
      for (key, val) in patch_obj.iter() {
        if val.is_null() {
          target_obj.remove(&key);
        } else if let Some(child) = target_obj.get_mut(&key) {
          json_merge_patch(child, val);
        } else {
          let mut new_child = sonic_rs::json!(null);
          json_merge_patch(&mut new_child, val);
          target_obj.insert(key, new_child);
        }
      }
    }
  } else {
    *target = patch.clone();
  }
}

/// 转换 JSON 值为 RESP3 协议响应格式（对标 Apache Kvrocks JSON.RESP）
pub use wedb_resp::json_to_resp_flat as json_to_resp;

/// 格式化 JSON 字符串输出（支持 INDENT, NEWLINE, SPACE 选项）
pub fn format_json_value(
  val: &Value,
  indent: Option<&str>,
  newline: Option<&str>,
  space: Option<&str>,
) -> String {
  if indent.is_none() && newline.is_none() && space.is_none() {
    return sonic_rs::to_string(val).unwrap_or_default();
  }
  let mut out = String::new();
  format_value_recursive(val, &mut out, 0, indent, newline, space);
  out
}

fn format_value_recursive(
  val: &Value,
  out: &mut String,
  depth: usize,
  indent: Option<&str>,
  newline: Option<&str>,
  space: Option<&str>,
) {
  let ind_str = indent.unwrap_or("");
  let nl_str = newline.unwrap_or("");
  let sp_str = space.unwrap_or("");

  if let Some(obj) = val.as_object() {
    if obj.is_empty() {
      out.push_str("{}");
      return;
    }
    out.push('{');
    out.push_str(nl_str);
    let mut first = true;
    for (k, v) in obj.iter() {
      if !first {
        out.push(',');
        out.push_str(nl_str);
      }
      first = false;
      for _ in 0..depth + 1 {
        out.push_str(ind_str);
      }
      out.push('"');
      out.push_str(k);
      out.push('"');
      out.push(':');
      out.push_str(sp_str);
      format_value_recursive(v, out, depth + 1, indent, newline, space);
    }
    out.push_str(nl_str);
    for _ in 0..depth {
      out.push_str(ind_str);
    }
    out.push('}');
  } else if let Some(arr) = val.as_array() {
    if arr.is_empty() {
      out.push_str("[]");
      return;
    }
    out.push('[');
    out.push_str(nl_str);
    let mut first = true;
    for v in arr.iter() {
      if !first {
        out.push(',');
        out.push_str(nl_str);
      }
      first = false;
      for _ in 0..depth + 1 {
        out.push_str(ind_str);
      }
      format_value_recursive(v, out, depth + 1, indent, newline, space);
    }
    out.push_str(nl_str);
    for _ in 0..depth {
      out.push_str(ind_str);
    }
    out.push(']');
  } else {
    out.push_str(&sonic_rs::to_string(val).unwrap_or_default());
  }
}
