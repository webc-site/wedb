//! 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs
//!
//! 承载 JSON DOM 根节点，提供基于 JSONPath 的检索、更新、删除与类型判定。

use core::str;
use std::io::{Read, Write};

use sonic_rs::{JsonValueMutTrait, JsonValueTrait, Value};
use wresp::ext::RespVecExt;

use crate::{
  error::{Error, RESP_NEW_OBJECT_AT_ROOT, RESP_WRONG_STATIC_PATH, Result},
  json_path::{JsonPath, select_nodes},
};

/// 存在性约束选项（对齐 Redis / Garnet NX / XX 语义）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExistOptions {
  None,
  NX,
  XX,
}

/// SET 操作结果
#[derive(Debug, PartialEq, Eq)]
pub enum SetResult {
  Success,
  ConditionNotMet,
  Error(String),
}

/// JSON 顶级对象
///
/// 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:GarnetJsonObject
///
/// C# CloneObject 深拷贝语义（在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:CloneObject）
/// 经 derive(Clone) 承接。
#[derive(Debug, Clone, Default)]
pub struct GarnetJsonObject {
  pub root_node: Option<Value>,
}

impl GarnetJsonObject {
  /// 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:GarnetJsonObject
  pub fn new(root_node: Option<Value>) -> Self {
    Self { root_node }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:Create
  pub fn create() -> Self {
    Self { root_node: None }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:Deserialize
  pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf)?;
    if buf.is_empty() {
      return Ok(Self::create());
    }
    let val: Value = sonic_rs::from_slice(&buf)?;
    Ok(Self {
      root_node: Some(val),
    })
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:SerializeObject
  pub fn serialize_object<W: Write>(&self, writer: &mut W) -> Result<()> {
    if let Some(root) = &self.root_node {
      let bytes = sonic_rs::to_vec(root)?;
      writer.write_all(&bytes)?;
    }
    Ok(())
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:Dispose
  pub fn dispose(&mut self) {
    self.root_node = None;
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:Scan
  pub fn scan(&self) -> Result<()> {
    Ok(())
  }

  /// 判定对象是否为空（空 → wedb 回收整键）
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.root_node.is_none()
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:TryGetRoot
  pub fn try_get_root(&self, output: &mut Vec<u8>, resp_version: u8) -> bool {
    let Some(root) = &self.root_node else {
      output.write_resp_null_ver(resp_version);
      return true;
    };
    let Ok(bytes) = sonic_rs::to_vec(root) else {
      output.write_resp_null_ver(resp_version);
      return false;
    };
    let mut res = Vec::with_capacity(bytes.len() + 2);
    res.push(b'[');
    res.extend_from_slice(&bytes);
    res.push(b']');
    output.write_resp_bulk_string(&res);
    true
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:TryGetToWriter
  pub fn try_get_to_writer(
    &self,
    path: &[u8],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> Result<bool> {
    let Some(root) = &self.root_node else {
      output.write_resp_null_ver(resp_version);
      return Ok(true);
    };

    let path_str = str::from_utf8(path).map_err(|_| Error::SyntaxError)?;
    let json_path = JsonPath::parse(path_str)?;
    let matches = json_path.evaluate(root);

    let mut res = Vec::new();
    res.push(b'[');
    let mut first = true;
    for node in matches {
      if !first {
        res.push(b',');
      }
      first = false;
      let node_bytes = sonic_rs::to_vec(node)?;
      res.extend_from_slice(&node_bytes);
    }
    res.push(b']');

    output.write_resp_bulk_string(&res);
    Ok(true)
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:TryGet
  pub fn try_get(
    &self,
    paths: &[&[u8]],
    output: &mut Vec<u8>,
    indent: Option<&str>,
    new_line: Option<&str>,
    space: Option<&str>,
    resp_version: u8,
  ) -> Result<bool> {
    let Some(root) = &self.root_node else {
      output.write_resp_null_ver(resp_version);
      return Ok(true);
    };

    let is_indented = indent.is_some() || new_line.is_some() || space.is_some();

    if paths.is_empty() {
      // 零路径直接返回全量 JSON 串
      let bytes = if is_indented {
        sonic_rs::to_vec_pretty(root)?
      } else {
        sonic_rs::to_vec(root)?
      };
      output.write_resp_bulk_string(&bytes);
      return Ok(true);
    }

    if paths.len() == 1 {
      let p = paths[0];
      if p.is_empty() {
        let bytes = if is_indented {
          sonic_rs::to_vec_pretty(root)?
        } else {
          sonic_rs::to_vec(root)?
        };
        output.write_resp_bulk_string(&bytes);
        return Ok(true);
      }
      if !is_indented && p == b"$" {
        return Ok(self.try_get_root(output, resp_version));
      }
      if !is_indented {
        return self.try_get_to_writer(p, output, resp_version);
      }

      if p == b"$" {
        let bytes = sonic_rs::to_vec_pretty(root)?;
        let mut res = Vec::with_capacity(bytes.len() + 2);
        res.push(b'[');
        res.extend_from_slice(&bytes);
        res.push(b']');
        output.write_resp_bulk_string(&res);
        return Ok(true);
      }

      let path_str = str::from_utf8(p).map_err(|_| Error::SyntaxError)?;
      let json_path = JsonPath::parse(path_str)?;
      let matches = json_path.evaluate(root);

      let mut res = Vec::new();
      res.push(b'[');
      let mut first = true;
      for node in matches {
        if !first {
          res.push(b',');
        }
        first = false;
        let node_bytes = sonic_rs::to_vec_pretty(node)?;
        res.extend_from_slice(&node_bytes);
      }
      res.push(b']');

      output.write_resp_bulk_string(&res);
      return Ok(true);
    }

    // 多路径: {"path1": [...], "path2": [...]}
    let mut res = Vec::new();
    res.push(b'{');
    let mut first = true;
    for &p in paths {
      if !first {
        res.push(b',');
      }
      first = false;
      let path_str = str::from_utf8(p).map_err(|_| Error::SyntaxError)?;
      let path_json = sonic_rs::to_vec(&path_str)?;
      res.extend_from_slice(&path_json);
      res.push(b':');

      let json_path = JsonPath::parse(path_str)?;
      let matches = json_path.evaluate(root);

      res.push(b'[');
      let mut item_first = true;
      for node in matches {
        if !item_first {
          res.push(b',');
        }
        item_first = false;
        let node_bytes = if is_indented {
          sonic_rs::to_vec_pretty(node)?
        } else {
          sonic_rs::to_vec(node)?
        };
        res.extend_from_slice(&node_bytes);
      }
      res.push(b']');
    }
    res.push(b'}');

    output.write_resp_bulk_string(&res);
    Ok(true)
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:Set
  pub fn set(
    &mut self,
    path: &[u8],
    value: &[u8],
    exist_options: ExistOptions,
  ) -> Result<SetResult> {
    let path_str = str::from_utf8(path).map_err(|_| Error::SyntaxError)?;
    let parsed_value: Value = sonic_rs::from_slice(value)?;

    if path_str == "$" || path_str.is_empty() {
      if self.root_node.is_none() {
        if exist_options == ExistOptions::XX {
          return Ok(SetResult::ConditionNotMet);
        }
        self.root_node = Some(parsed_value);
        return Ok(SetResult::Success);
      }
      if exist_options == ExistOptions::NX {
        return Ok(SetResult::ConditionNotMet);
      }
      self.root_node = Some(parsed_value);
      return Ok(SetResult::Success);
    }

    let Some(root) = self.root_node.as_mut() else {
      return Ok(SetResult::Error(RESP_NEW_OBJECT_AT_ROOT.to_string()));
    };

    let json_path = JsonPath::parse(path_str)?;
    let current_matches = json_path.evaluate(root);

    if current_matches.is_empty() {
      if exist_options == ExistOptions::XX {
        return Ok(SetResult::ConditionNotMet);
      }

      if !json_path.is_static_path() {
        return Ok(SetResult::Error(RESP_WRONG_STATIC_PATH.to_string()));
      }

      let (parent_path, prop_offset) = Self::get_parent_path(path_str);
      let parent_nodes = select_nodes(root, parent_path)?;
      if parent_nodes.is_empty() {
        return Ok(SetResult::ConditionNotMet);
      }

      let prop_name = Self::get_property_name(path_str, prop_offset);

      // Mutate parent
      let mut inserted = false;
      let parent_path_parsed = JsonPath::parse(parent_path)?;
      parent_path_parsed.replace_matches(root, &{
        let mut parent_copy = parent_nodes[0].clone();
        if let Some(obj) = parent_copy.as_object_mut() {
          obj.insert(prop_name, parsed_value.clone());
          inserted = true;
        } else if let Some(arr) = parent_copy.as_array_mut()
          && let Ok(idx) = prop_name.parse::<usize>()
          && idx <= arr.len()
        {
          arr.insert(idx, parsed_value.clone());
          inserted = true;
        }
        parent_copy
      });

      if inserted {
        Ok(SetResult::Success)
      } else {
        Ok(SetResult::ConditionNotMet)
      }
    } else {
      if exist_options == ExistOptions::NX {
        return Ok(SetResult::ConditionNotMet);
      }

      json_path.replace_matches(root, &parsed_value);
      Ok(SetResult::Success)
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:GetParentPath
  pub fn get_parent_path(path: &str) -> (&str, usize) {
    let bytes = path.as_bytes();
    if bytes.is_empty() {
      return ("$", 0);
    }
    let slice_to_search = if bytes.len() > 1 {
      &bytes[..bytes.len() - 1]
    } else {
      bytes
    };

    let last_sep = slice_to_search
      .iter()
      .rposition(|&b| b == b'.' || b == b']');
    match last_sep {
      None => ("$", 0),
      Some(mut offset) => {
        if bytes[offset] == b']' {
          offset += 1;
        }
        (&path[..offset], offset)
      }
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:GetPropertyName
  pub fn get_property_name(path: &str, mut offset: usize) -> &str {
    let bytes = path.as_bytes();
    if offset < bytes.len() && bytes[offset] == b'.' {
      offset += 1;
    }
    let mut s = &path[offset..];
    if s.starts_with('[') && s.ends_with(']') {
      s = &s[1..s.len() - 1];
    }
    if (s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')) {
      s = &s[1..s.len() - 1];
    }
    s
  }

  /// 删除指定路径对应的元素
  ///
  /// C# JSON 模块仅注册 JSON.SET / JSON.GET（见 modules/GarnetJSON/JsonModule.cs），
  /// JSON.DEL 为 wedb 侧删空自愈扩展，无 C# 同名对位。
  pub fn del(&mut self, path: Option<&[u8]>) -> usize {
    let Some(root) = self.root_node.as_mut() else {
      return 0;
    };

    let Some(p) = path else {
      self.root_node = None;
      return 1;
    };

    if p.is_empty() || p == b"$" {
      self.root_node = None;
      return 1;
    }

    let Ok(path_str) = str::from_utf8(p) else {
      return 0;
    };
    let Ok(json_path) = JsonPath::parse(path_str) else {
      return 0;
    };

    json_path.delete_matches(root)
  }

  /// 获取指定路径节点的 JSON 类型
  ///
  /// C# JSON 模块仅注册 JSON.SET / JSON.GET（见 modules/GarnetJSON/JsonModule.cs），
  /// JSON.TYPE 为 wedb 侧扩展，无 C# 同名对位。
  pub fn type_of(&self, path: Option<&[u8]>) -> Option<Vec<&'static str>> {
    let root = self.root_node.as_ref()?;
    let Some(p) = path else {
      return Some(vec![json_type_name(root)]);
    };
    if p.is_empty() || p == b"$" {
      return Some(vec![json_type_name(root)]);
    }

    let path_str = str::from_utf8(p).ok()?;
    let json_path = JsonPath::parse(path_str).ok()?;
    let matches = json_path.evaluate(root);
    if matches.is_empty() {
      return None;
    }
    Some(matches.into_iter().map(json_type_name).collect())
  }
}

/// sonic-rs AST 节点 → JSON 类型名（object/array/string/integer/number/boolean/null）；
/// 服务 wedb 侧 JSON.TYPE 扩展，C# 无同名对位。
pub fn json_type_name(v: &Value) -> &'static str {
  if v.is_object() {
    "object"
  } else if v.is_array() {
    "array"
  } else if v.is_str() {
    "string"
  } else if v.is_i64() || v.is_u64() {
    "integer"
  } else if v.is_f64() {
    "number"
  } else if v.is_boolean() {
    "boolean"
  } else if v.is_null() {
    "null"
  } else {
    "unknown"
  }
}
