//! 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonExtensions.cs
//!
//! JSONPath 解析与求值引擎(对标 modules/GarnetJSON/JSONPath/ 目录族)。
//! 采用 AST 零分配 / 最小分配状态机流式解析,
//! 基于 `sonic-rs` 的 AST 进行检索、过滤、修改和删除。

mod expression;
mod filter;
mod parser;
mod path;

pub use expression::QueryExpression;
pub(crate) use expression::val_from_f64;
pub use filter::PathFilter;
pub use path::JsonPath;
use sonic_rs::Value;

use crate::error::Result;

/// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonExtensions.cs:SelectNodes
pub fn select_nodes<'a>(root: &'a Value, path: &str) -> Result<Vec<&'a Value>> {
  let json_path = JsonPath::parse(path)?;
  Ok(json_path.evaluate(root))
}

/// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonExtensions.cs:TrySelectNode
pub fn try_select_node<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
  select_nodes(root, path)
    .ok()
    .and_then(|v| v.into_iter().next())
}
