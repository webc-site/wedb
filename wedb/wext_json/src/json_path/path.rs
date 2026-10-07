//! 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs
//!
//! JSONPath 路径对象与求值、变异(替换/删除)。
//! 删除状态机域见 path_delete.rs / path_scan.rs，变异状态机域见 path_mutate.rs。

use sonic_rs::Value;

use super::{
  filter::{PathFilter, evaluate_filters},
  parser::JsonPathParser,
};
use crate::error::Result;

/// JSONPath 主结构
///
/// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs
#[derive(Debug)]
pub struct JsonPath {
  pub filters: Vec<PathFilter>,
  /// 顶层过滤器是否含 Query / QueryScan 谓词：解析期一次判定，供变异期决定
  /// 是否需要文档快照根上下文（谓词内 `$` 是唯一读取文档根的路径）
  pub has_predicate: bool,
}

impl JsonPath {
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:JsonPath
  pub fn parse(expression: &str) -> Result<Self> {
    let mut parser = JsonPathParser::new(expression);
    let filters = parser.parse_main()?;
    let has_predicate = filters
      .iter()
      .any(|f| matches!(f, PathFilter::Query { .. } | PathFilter::QueryScan { .. }));
    Ok(Self {
      filters,
      has_predicate,
    })
  }

  /// 变异期的谓词根上下文：含谓词路径取变异前快照（对标 C# Set 的两阶段
  /// `Evaluate().ToArray()` 先于逐个 ReplaceWith，匹配上下文恒为变异前状态）；
  /// 无谓词路径的根上下文在 mutate_recursive / delete_recursive 中永不读取
  ///（root 只出现在 Query 与 QueryScan 两臂），故用 null 占位，免去整树深拷贝。
  fn predicate_root(&self, root: &Value) -> Value {
    if self.has_predicate {
      root.clone()
    } else {
      Value::from(())
    }
  }

  /// 就地遍历所有匹配节点并交回调改写：谓词根上下文由 [`Self::predicate_root`] 裁决，
  /// 调用方不再各自 `root.clone()`。
  pub fn mutate(&self, root: &mut Value, cb: &mut impl FnMut(&mut Value)) {
    let root_ctx = self.predicate_root(root);
    self.mutate_recursive(&root_ctx, root, 0, cb);
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:IsStaticPath
  pub(crate) fn is_static_path(&self) -> bool {
    self.filters.iter().all(|f| match f {
      PathFilter::Root => true,
      PathFilter::Field { name } => name.is_some(),
      PathFilter::ArrayIndex { index } => index.is_some(),
      _ => false,
    })
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:Evaluate
  pub fn evaluate<'a>(&self, root: &'a Value) -> Result<Vec<&'a Value>> {
    evaluate_filters(&self.filters, root, root)
  }

  /// 执行变异：就地替换所有匹配节点
  pub fn replace_matches(&self, root: &mut Value, new_val: &Value) -> usize {
    if self.filters.is_empty() || matches!(self.filters.as_slice(), [PathFilter::Root]) {
      *root = new_val.clone();
      return 1;
    }

    let mut count = 0;
    // 谓词求值的根上下文按 has_predicate 裁决（对标 C# Set 两阶段），无谓词零克隆
    self.mutate(root, &mut |target| {
      *target = new_val.clone();
      count += 1;
    });
    count
  }

  /// 执行删除：移除所有匹配节点
  pub fn delete_matches(&self, root: &mut Value) -> usize {
    if self.filters.is_empty() || matches!(self.filters.as_slice(), [PathFilter::Root]) {
      return 0; // 防御臂，已在 json_object.rs:del 根预检扩认中处理
    }

    let mut count = 0;
    // 谓词内 $ 的根上下文同样按 has_predicate 裁决，删除作用于活树
    let root_ctx = self.predicate_root(root);
    self.delete_recursive(&root_ctx, root, 0, &mut count);
    count
  }
}
