//! 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs
//!
//! 承载 JSON DOM 根节点，提供基于 JSONPath 的检索、更新、删除与类型判定。

use core::{iter::once, result, str};
use std::io::Write;

use sonic_rs::{Deserializer, JsonValueMutTrait, JsonValueTrait, Value};
use wresp::ext::RespVecExt;
/// 统一使用 wresp 的 ExistOptions（对标 Garnet.server:ExistOptions）
pub use wresp::options::ExistOptions;

use crate::{
  error::{
    Error, RESP_ERR_NOT_IMPLEMENTED, RESP_NEW_OBJECT_AT_ROOT, RESP_WRONG_STATIC_PATH, Result,
  },
  json_path::{JsonPath, select_nodes},
};

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

/// JSON 对象堆内存估算（字节），入参为信封内层序列化载荷（不含 1B 类型标签）
///
/// 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:HeapMemorySize
///
/// C# 构造记账 `HeapMemorySize = MemoryUtils.DictionaryOverhead`（80，DOM 基座
/// 常数），Set 路径 TODO 不随文档更新（GarnetJsonObject.cs:350 备注），故
/// MEMORY USAGE 对 JSON 键恒回该常数——1:1 保留此语义：常数与载荷无关，
/// 不解析入参，与「信封记录物理尺寸已含序列化载荷本体」的口径互斥不冲突
///（该口径指不重复计载荷，此项计的是 DOM 对象基座开销）
pub const fn heap_estimate(_payload: &[u8]) -> i64 {
  /// C# Tsavorite MemoryUtils.DictionaryOverhead（DOM 字典基座常数）
  const DICTIONARY_OVERHEAD: i64 = 80;
  DICTIONARY_OVERHEAD
}

/// 载荷文本 → DOM 单点解析：数字节点以 sonic_rs RawNumber 存原文词形（唯一存储形态）
///
/// 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:95（Deserialize）/:360
/// （Set 根替换）/:391（Set 补插子节点）/:409（Set 覆写匹配节点）——C# 四处
/// `JsonNode.Parse` 全链不带 options，产出的 JsonValue 由 JsonElement 承载，写回时
/// 拷原始文档字节，故 SET 载荷里 "1.10"/"1e2"/"-0"/超 u64 大整数经落库后 GET 逐字节
/// 还原原文；rust 侧 `use_rawnumber()` 是同一形态的 sonic 原生承接（sonic-rs 0.5.10
/// 序列化面对 RawNumber 直拷原文，见 value/node.rs 的 RawNum 臂）。
///
/// 数值语义（JSON.TYPE 判型、过滤器比较、NUMINCRBY/NUMMULTBY/CLEAR 取值、JSON.RESP
/// 编码）零改动：sonic 的 `as_i64/as_u64/as_f64/is_*` 对 RawNumber 按需现解原文文本，
/// DOM 内不落第二数值表示。
///
/// `Deserializer` 手工路径缺 `sonic_rs::from_slice` 收尾的两步校验，此处补齐：整串
/// UTF-8 校验（否则串内非法字节被静默替换）与尾部垃圾拒收（`parse_trailing`）。
///
/// 解析深度唯一承载说明（勿在此另立深度门或散落裸数字）：wext_json 全链无独立
/// 深度裁决，载荷嵌套深度是否可解析完全由本漏斗的 sonic `Value` 装载承载。事实
/// 单源：`sonic_rs::Value` 的 `Deserialize`（registry sonic-rs 0.5.10
/// `src/value/de.rs:62` `deserialize_newtype_struct(TOKEN, ValueVisitor)`）走原生
/// DOM 快路，**绕过** `src/serde/de.rs:23` 的 `MAX_ALLOWED_DEPTH = u8::MAX = 255`
/// 门（该门仅挂通用 `Deserializer::deserialize_any` 的 visit_seq/visit_map 递归，
/// Value 快路不经由），实测本漏斗对 255/256 乃至数千层对象/数组载荷均正常返回。
/// 故切勿误信「rust 侧深度上限是 255」而据此写死数字或预扫；C# 侧
/// （`garnet/modules/GarnetJSON/GarnetJsonObject.cs` 四处 `JsonNode.Parse`，默认
/// MaxDepth=64，>64 层抛 JsonException 收错误帧）与 rust 宽接受之间的 >64 层分叉
/// 系登记内有意宽向裁量，详见 doc/zh/deviations.md §161；**严禁按 C# 64 在本 SET/
/// GET 热路径加 O(n) 深度预扫回改**（违零开销纪律，且无 255 门可依，属双错）。
pub(crate) fn parse_dom(payload: &[u8]) -> Result<Value> {
  str::from_utf8(payload).map_err(|_| Error::SyntaxError)?;
  let mut de = Deserializer::from_slice(payload).use_rawnumber();
  let root = de.deserialize::<Value>()?;
  de.end()?;
  Ok(root)
}

/// 根路径判定单源：空路径 `""` 规范化为与 `"$"` 同形。
///
/// 仅供 GET（`try_get` 单/多路径两臂）收口，消除 rust 自身「单路径臂裸根 vs
/// 多路径臂带包裹」双臂不一致（详见 doc/zh/deviations.md 空路径 GET 归一条）。
///
/// 注意：SET 根替换臂与 `json_set_need_initial_update` 缺键建根门不采此判定——
/// 二者仅认 `"$"`，`""` 缺键回 RESP_NEW_OBJECT_AT_ROOT 错误帧且不建键（对齐
/// C#/RedisJSON，守 §19 缺键不落空壳红线），故严禁在此并入 `""` 后回改 SET。
#[inline]
fn path_is_root(p: &[u8]) -> bool {
  p == b"$" || p.is_empty()
}

/// 节点序列 → `[n1,n2,...]` JSON 数组字节；pretty = true 时逐节点缩进序列化
///（try_get_to_writer / try_get 单·多路径四处同构拼接的收口单点）
fn nodes_json_array<'a>(
  nodes: impl Iterator<Item = &'a sonic_rs::Value>,
  pretty: bool,
) -> Result<Vec<u8>> {
  let mut res = Vec::new();
  res.push(b'[');
  let mut first = true;
  for node in nodes {
    if !first {
      res.push(b',');
    }
    first = false;
    let node_bytes = if pretty {
      sonic_rs::to_vec_pretty(node)?
    } else {
      sonic_rs::to_vec(node)?
    };
    res.extend_from_slice(&node_bytes);
  }
  res.push(b']');
  Ok(res)
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

  /// 由信封载荷字节直读 DOM，空载荷即空对象
  ///
  /// 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:Deserialize
  ///
  /// C# 走 `BinaryReader.ReadString()` 再 `JsonNode.Parse`（Tsavorite 给的是流），
  /// rust 侧信封载荷本身就是 JSON 文本字节，sonic_rs 原生收 `&[u8]`，故按
  /// wcol 惯例取名 `from_slice` 直借切片：经 `Read` 流反而要先 read_to_end
  /// 拷一份等长 Vec，击穿 wnode read_tag_with/probe_tag_sync 的零拷贝借用通道。
  /// 数字词形的原文保真由 `parse_dom` 承接（存即原文，重载不再二次归一）
  pub fn from_slice(payload: &[u8]) -> Result<Self> {
    if payload.is_empty() {
      return Ok(Self::create());
    }
    Ok(Self {
      root_node: Some(parse_dom(payload)?),
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

    let res = nodes_json_array(matches.into_iter(), false)?;

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
      // 根路径（"$" 或规范化同形的 ""）单源经 path_is_root 收口：无格式走
      // try_get_root、带格式走 pretty 包裹，两态皆回 "[<根>]"（取 C# Reader 快路
      // JsonCommands.cs:170-171 TryGetRoot 形；与 C# 通用臂带格式裸根之残余差
      // 登记 doc/zh/deviations.md 空路径 GET 归一条）。
      if path_is_root(p) {
        if !is_indented {
          return Ok(self.try_get_root(output, resp_version));
        }
        let res = nodes_json_array(once(root), true)?;
        output.write_resp_bulk_string(&res);
        return Ok(true);
      }
      if !is_indented {
        return self.try_get_to_writer(p, output, resp_version);
      }

      let path_str = str::from_utf8(p).map_err(|_| Error::SyntaxError)?;
      let json_path = JsonPath::parse(path_str)?;
      let matches = json_path.evaluate(root);

      let res = nodes_json_array(matches.into_iter(), true)?;

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

      // 根路径（"$"/""）与单路径臂同经 path_is_root 收口，命中集恒为 [根] 带
      // 包裹回 "[<根>]"（键值带包裹，与 C# 通用臂裸根之残余差登记 deviations）；
      // 非根路径照常求值。
      let inner = if path_is_root(p) {
        nodes_json_array(once(root), is_indented)?
      } else {
        let json_path = JsonPath::parse(path_str)?;
        nodes_json_array(json_path.evaluate(root).into_iter(), is_indented)?
      };
      res.extend_from_slice(&inner);
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
    let parsed_value = parse_dom(value)?;

    // 根替换臂仅认 "$"（对标 C# GarnetJsonObject.cs:Set :358 的
    // `pathStr.Length == 1 && pathStr[0] == '$'`）；空路径 "" 不再并入此臂——
    // 缺键时顺延至下方 rootNode.is_none() 守卫回 RESP_NEW_OBJECT_AT_ROOT（对齐
    // C#/RedisJSON 拒空路径建根）；既有键 "" 走通用臂经空过滤器命中根做替换，
    // 与 C# `new JsonPath("")`.Evaluate 回 [根] 后 ReplaceMatches 语义等价。
    if path_str == "$" {
      if self.root_node.is_none() {
        if exist_options == ExistOptions::Xx {
          return Ok(SetResult::ConditionNotMet);
        }
        self.root_node = Some(parsed_value);
        return Ok(SetResult::Success);
      }
      if exist_options == ExistOptions::Nx {
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
      if exist_options == ExistOptions::Xx {
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
      if exist_options == ExistOptions::Nx {
        return Ok(SetResult::ConditionNotMet);
      }

      let replaced = json_path.replace_matches(root, &parsed_value);
      // 防御锁：evaluate 非空但变异计数为 0 的组合在 C# 单语义引擎下不可达
      // （GarnetJsonObject.cs:Set 两阶段共用同一 Evaluate），一旦出现即为三引擎
      // 语义再度分叉的事故信号；C# 同形本走 :379-382 wrong static path 错误帧
      // （非合法写入形态），此处回同向帧杜绝 OK+零写谎报成功。
      if replaced == 0 {
        return Ok(SetResult::Error(RESP_WRONG_STATIC_PATH.to_string()));
      }
      Ok(SetResult::Success)
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:GetParentPath
  ///
  /// C# 只回看 '.' 与 ']' 两个分隔符，遇 `$[2]` 或 `$['foo']` 这类紧贴根容器的
  /// 路径直接命中 -1 兜底，让下游 GetPropertyName 以越界索引访问切片崩溃；rust
  /// 侧沿用 C# 的"末位裁剪 + 反向定位"骨架，把分隔符集合扩到 {'.'、'['、']'}，
  /// 命中 '[' 时让父路径止于 '[' 之前、prop_offset 指向 '['；完全未命中但路径以
  /// '$' 开头时把 prop_offset 归一化到 1，跳过根前缀，避免下游切片越界。
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
      .rposition(|&b| b == b'.' || b == b'[' || b == b']');
    match last_sep {
      None => {
        if bytes[0] == b'$' {
          ("$", 1)
        } else {
          ("$", 0)
        }
      }
      Some(mut offset) => {
        if bytes[offset] == b']' {
          offset += 1;
        }
        (&path[..offset], offset)
      }
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:GetPropertyName
  ///
  /// 承接 C# 的 '.' 前移 + '[' 剥壳 + 引号剥壳三步；额外剥离 `$` 前缀，兜住
  /// C# 依赖 GetParentPath 定位到根时路径偏移恰好为 0 的隐式假设。
  pub fn get_property_name(path: &str, mut offset: usize) -> &str {
    let bytes = path.as_bytes();
    if offset < bytes.len() && bytes[offset] == b'.' {
      offset += 1;
    }
    let mut s = &path[offset..];
    if let Some(stripped) = s.strip_prefix('$') {
      s = stripped;
    }
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

/// COSCAN 成员扫描执行体（JSON 域恒落错误，[`wcustom::CustomScanMembersFn`] 契约）
///
/// 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:Scan
///
/// C# 直接抛 NotImplementedException（JSON 域无 COSCAN 扫描语义）；rust 裁量
/// 以错误帧收口（会话级异常无 RESP 帧对位，文案取 .NET 异常默认消息并登记
/// doc/zh/deviations.md），绝不回空成功帧
pub fn scan_members(
  _payload: &[u8],
  _start: i64,
  _count: i64,
  _pattern: &[u8],
  _is_no_value: bool,
) -> result::Result<(Vec<Vec<u8>>, i64), &'static [u8]> {
  Err(RESP_ERR_NOT_IMPLEMENTED.as_bytes())
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
