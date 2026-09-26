//! 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs
//!
//! JSONPath 路径对象与求值、变异(替换/删除)。

use sonic_rs::{JsonValueMutTrait, Value};

use super::{
  expression::QueryExpression,
  filter::{PathFilter, evaluate_filters, slice_indices},
  parser::JsonPathParser,
};
use crate::error::Result;

/// 负下标折算 + 越界判定的单源：JSONPath 的 i64 下标（负值自尾部起算）归一为
/// 数组实际下标；折算后仍为负或越界返回 None（与原各处
/// `actual >= 0 && (actual as usize) < len` 判定逐字节等价）。
#[inline]
fn resolve_index(idx: i64, len: usize) -> Option<usize> {
  let actual = if idx < 0 { len as i64 + idx } else { idx };
  (actual >= 0 && (actual as usize) < len).then_some(actual as usize)
}

/// 数组按命中下标集终结删除的共享骨架：统一升序排序去重后逆序 remove，
/// 避免前面的删除使后面的下标偏移（负步进切片/重复多下标共用）。
#[inline]
fn remove_indices_desc(arr: &mut sonic_rs::Array, mut idxs: Vec<usize>, count: &mut usize) {
  idxs.sort_unstable();
  idxs.dedup();
  for &i in idxs.iter().rev() {
    arr.remove(i);
    *count += 1;
  }
}

/// JSONPath 主结构
///
/// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs
#[derive(Debug, Clone)]
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
  /// （root 只出现在 Query 与 QueryScan 两臂），故用 null 占位，免去整树深拷贝。
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
  pub fn is_static_path(&self) -> bool {
    self.filters.iter().all(|f| match f {
      PathFilter::Root => true,
      PathFilter::Field { name } => name.is_some(),
      PathFilter::ArrayIndex { index } => index.is_some(),
      _ => false,
    })
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:Evaluate
  pub fn evaluate<'a>(&self, root: &'a Value) -> Vec<&'a Value> {
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
      return 1;
    }

    let mut count = 0;
    // 谓词内 $ 的根上下文同样按 has_predicate 裁决，删除作用于活树
    let root_ctx = self.predicate_root(root);
    self.delete_recursive(&root_ctx, root, 0, &mut count);
    count
  }

  fn delete_recursive(
    &self,
    // 顶层文档快照：谓词内 $ 引用一律按 C# JsonPath.Evaluate 的根上下文求值，
    // 杜绝以 current.clone() 造伪根；快照在进入递归前一次生成，遍历期零克隆。
    root: &Value,
    current: &mut Value,
    filter_idx: usize,
    count: &mut usize,
  ) {
    if filter_idx >= self.filters.len() {
      return;
    }

    let is_last = filter_idx + 1 == self.filters.len();
    let filter = &self.filters[filter_idx];

    if is_last {
      match filter {
        PathFilter::Field { name } => {
          if let Some(obj) = current.as_object_mut() {
            match name {
              Some(f) => {
                if obj.remove(f).is_some() {
                  *count += 1;
                }
              }
              None => {
                *count += obj.len();
                obj.clear();
              }
            }
          }
        }
        PathFilter::FieldMultiple { names } => {
          if let Some(obj) = current.as_object_mut() {
            for n in names {
              if obj.remove(n).is_some() {
                *count += 1;
              }
            }
          }
        }
        PathFilter::ArrayIndex { index } => {
          if let Some(arr) = current.as_array_mut() {
            match index {
              Some(idx) => {
                if let Some(i) = resolve_index(*idx, arr.len()) {
                  arr.remove(i);
                  *count += 1;
                }
              }
              None => {
                *count += arr.len();
                arr.clear();
              }
            }
          }
        }
        PathFilter::ArrayMultipleIndex { indices } => {
          if let Some(arr) = current.as_array_mut() {
            let hits: Vec<usize> = indices
              .iter()
              .filter_map(|&idx| resolve_index(idx, arr.len()))
              .collect();
            remove_indices_desc(arr, hits, count);
          }
        }
        PathFilter::Scan { name } => {
          self.delete_scan(current, name.as_deref(), count);
        }
        PathFilter::Query { expression } => {
          if let Some(arr) = current.as_array_mut() {
            let mut i = 0;
            while i < arr.len() {
              if expression.is_match(root, &arr[i]) {
                arr.remove(i);
                *count += 1;
              } else {
                i += 1;
              }
            }
          } else if let Some(obj) = current.as_object_mut() {
            let keys_to_del: Vec<String> = obj
              .iter()
              .filter_map(|(k, v)| {
                if expression.is_match(root, v) {
                  Some(k.to_string())
                } else {
                  None
                }
              })
              .collect();
            for k in keys_to_del {
              obj.remove(&k);
              *count += 1;
            }
          }
        }
        // WHY 占位臂：filters 仅含 Root 的路径已在 delete_matches 早退，
        // Root 不会作为终结过滤器到达此处；显式列出以保持 13 变体穷尽匹配。
        PathFilter::Root => {}
        // 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ArraySliceFilter.cs:ExecuteFilter
        PathFilter::ArraySlice { start, end, step } => {
          if let Some(arr) = current.as_array_mut() {
            let hits = slice_indices(arr.len(), *start, *end, *step);
            // 负步进返回降序，删除前统一升序排序后逆序 remove（骨架见 remove_indices_desc）。
            remove_indices_desc(arr, hits, count);
          }
        }
        // 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanMultipleFilter.cs:ExecuteFilter
        PathFilter::ScanMultiple { names } => {
          self.delete_scan_multiple(current, names, count);
        }
        // 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanArrayIndexFilter.cs:ExecuteFilter
        PathFilter::ScanArrayIndex { index } => {
          self.delete_scan_arrays(current, count, &|len| match index {
            Some(idx) => match resolve_index(*idx, len) {
              Some(i) => vec![i],
              None => Vec::new(),
            },
            None => (0..len).collect(),
          });
        }
        // 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanArrayMultipleIndexFilter.cs:ExecuteFilter
        PathFilter::ScanArrayMultipleIndex { indices } => {
          self.delete_scan_arrays(current, count, &|len| {
            indices
              .iter()
              .filter_map(|&idx| resolve_index(idx, len))
              .collect()
          });
        }
        // 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanArraySliceFilter.cs:ExecuteFilter
        PathFilter::ScanArraySlice { start, end, step } => {
          self.delete_scan_arrays(current, count, &|len| {
            slice_indices(len, *start, *end, *step)
          });
        }
        // 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryScanFilter.cs:ExecuteFilter
        PathFilter::QueryScan { expression } => {
          self.delete_query_scan(root, current, expression, count);
        }
      }
    } else {
      match filter {
        PathFilter::Root => {
          self.delete_recursive(root, current, filter_idx + 1, count);
        }
        PathFilter::Field { name } => {
          if let Some(obj) = current.as_object_mut() {
            match name {
              Some(f) => {
                if let Some(child) = obj.get_mut(f) {
                  self.delete_recursive(root, child, filter_idx + 1, count);
                }
              }
              None => {
                for (_, child) in obj.iter_mut() {
                  self.delete_recursive(root, child, filter_idx + 1, count);
                }
              }
            }
          }
        }
        PathFilter::ArrayIndex { index } => {
          if let Some(arr) = current.as_array_mut() {
            match index {
              Some(idx) => {
                if let Some(i) = resolve_index(*idx, arr.len())
                  && let Some(child) = arr.get_mut(i)
                {
                  self.delete_recursive(root, child, filter_idx + 1, count);
                }
              }
              None => {
                for child in arr.iter_mut() {
                  self.delete_recursive(root, child, filter_idx + 1, count);
                }
              }
            }
          }
        }
        PathFilter::Scan { name } => {
          self.delete_scan(current, name.as_deref(), count);
        }
        // 以下中间层路由臂与 mutate_recursive 对应臂同构：
        // 命中子节点下传 filter_idx + 1，扫描族再以自己的下标重入子节点续走遍历。
        PathFilter::FieldMultiple { names } => {
          if let Some(obj) = current.as_object_mut() {
            for n in names {
              if let Some(child) = obj.get_mut(n) {
                self.delete_recursive(root, child, filter_idx + 1, count);
              }
            }
          }
        }
        PathFilter::ArrayMultipleIndex { indices } => {
          if let Some(arr) = current.as_array_mut() {
            for &idx in indices {
              if let Some(i) = resolve_index(idx, arr.len())
                && let Some(child) = arr.get_mut(i)
              {
                self.delete_recursive(root, child, filter_idx + 1, count);
              }
            }
          }
        }
        // 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ArraySliceFilter.cs:ExecuteFilter
        PathFilter::ArraySlice { start, end, step } => {
          if let Some(arr) = current.as_array_mut() {
            for i in slice_indices(arr.len(), *start, *end, *step) {
              if let Some(child) = arr.get_mut(i) {
                self.delete_recursive(root, child, filter_idx + 1, count);
              }
            }
          }
        }
        // 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanMultipleFilter.cs:ExecuteFilter
        PathFilter::ScanMultiple { names } => {
          if let Some(obj) = current.as_object_mut() {
            let matched: Vec<String> = obj
              .iter()
              .filter_map(|(k, _)| {
                if names.iter().any(|n| n.as_str() == k) {
                  Some(k.to_string())
                } else {
                  None
                }
              })
              .collect();
            for key in &matched {
              if let Some(child) = obj.get_mut(key) {
                self.delete_recursive(root, child, filter_idx + 1, count);
              }
            }
            for (_, child) in obj.iter_mut() {
              self.delete_recursive(root, child, filter_idx, count);
            }
          } else if let Some(arr) = current.as_array_mut() {
            for child in arr.iter_mut() {
              self.delete_recursive(root, child, filter_idx, count);
            }
          }
        }
        // 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanArrayIndexFilter.cs:ExecuteFilter
        PathFilter::ScanArrayIndex { index } => {
          if let Some(arr) = current.as_array_mut() {
            match index {
              Some(idx) => {
                if let Some(i) = resolve_index(*idx, arr.len())
                  && let Some(child) = arr.get_mut(i)
                {
                  self.delete_recursive(root, child, filter_idx + 1, count);
                }
              }
              None => {
                for child in arr.iter_mut() {
                  self.delete_recursive(root, child, filter_idx + 1, count);
                }
              }
            }
            for child in arr.iter_mut() {
              self.delete_recursive(root, child, filter_idx, count);
            }
          } else if let Some(obj) = current.as_object_mut() {
            for (_, child) in obj.iter_mut() {
              self.delete_recursive(root, child, filter_idx, count);
            }
          }
        }
        // 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanArrayMultipleIndexFilter.cs:ExecuteFilter
        PathFilter::ScanArrayMultipleIndex { indices } => {
          if let Some(arr) = current.as_array_mut() {
            for &idx in indices {
              if let Some(i) = resolve_index(idx, arr.len())
                && let Some(child) = arr.get_mut(i)
              {
                self.delete_recursive(root, child, filter_idx + 1, count);
              }
            }
            for child in arr.iter_mut() {
              self.delete_recursive(root, child, filter_idx, count);
            }
          } else if let Some(obj) = current.as_object_mut() {
            for (_, child) in obj.iter_mut() {
              self.delete_recursive(root, child, filter_idx, count);
            }
          }
        }
        // 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanArraySliceFilter.cs:ExecuteFilter
        PathFilter::ScanArraySlice { start, end, step } => {
          if let Some(arr) = current.as_array_mut() {
            for i in slice_indices(arr.len(), *start, *end, *step) {
              if let Some(child) = arr.get_mut(i) {
                self.delete_recursive(root, child, filter_idx + 1, count);
              }
            }
            for child in arr.iter_mut() {
              self.delete_recursive(root, child, filter_idx, count);
            }
          } else if let Some(obj) = current.as_object_mut() {
            for (_, child) in obj.iter_mut() {
              self.delete_recursive(root, child, filter_idx, count);
            }
          }
        }
        PathFilter::Query { expression } => {
          if let Some(arr) = current.as_array_mut() {
            for item in arr.iter_mut() {
              if expression.is_match(root, item) {
                self.delete_recursive(root, item, filter_idx + 1, count);
              }
            }
          } else if let Some(obj) = current.as_object_mut() {
            for (_, item) in obj.iter_mut() {
              if expression.is_match(root, item) {
                self.delete_recursive(root, item, filter_idx + 1, count);
              }
            }
          }
          // 标量节点无臂恒零产出（对标 C# QueryFilter.ExecuteFilter 三臂，
          // 与求值臂 filter.rs 单语义收口；原标量自匹配兜底系分叉源，已删）。
        }
        // 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryScanFilter.cs:ExecuteFilter
        PathFilter::QueryScan { expression } => {
          // C# 枚举器先判 current 自身再栈式下钻，重入子节点同滤保持先序一致。
          if expression.is_match(root, current) {
            self.delete_recursive(root, current, filter_idx + 1, count);
          }
          if let Some(obj) = current.as_object_mut() {
            for (_, child) in obj.iter_mut() {
              self.delete_recursive(root, child, filter_idx, count);
            }
          } else if let Some(arr) = current.as_array_mut() {
            for child in arr.iter_mut() {
              self.delete_recursive(root, child, filter_idx, count);
            }
          }
        }
      }
    }
  }

  /// 终结 ScanMultiple 删除：逐层对象移除命中 names 的键再下钻剩余子节点，
  /// 命中点与遍历序 1:1 对应 C# ScanMultipleFilter.ExecuteFilter
  /// （仅对象成员可命中，数组仅下钻；锚点由 PathFilter::execute_filter
  /// 单点持有）；先收集后删除避免迭代中改动对象。
  fn delete_scan_multiple(&self, val: &mut Value, names: &[String], count: &mut usize) {
    if let Some(obj) = val.as_object_mut() {
      let matched: Vec<String> = obj
        .iter()
        .filter_map(|(k, _)| {
          if names.iter().any(|n| n.as_str() == k) {
            Some(k.to_string())
          } else {
            None
          }
        })
        .collect();
      for key in matched {
        if obj.remove(&key).is_some() {
          *count += 1;
        }
      }
      for (_, child) in obj.iter_mut() {
        self.delete_scan_multiple(child, names, count);
      }
    } else if let Some(arr) = val.as_array_mut() {
      for child in arr.iter_mut() {
        self.delete_scan_multiple(child, names, count);
      }
    }
  }

  /// 扫描族数组过滤器的终结删除共享骨架：子树中每个数组先按 hits 求解命中下标
  /// （负下标折算/切片步进由调用方闭包给出，与各自 C# ExecuteFilter 对位：
  /// ScanArrayIndexFilter、ScanArrayMultipleIndexFilter、ScanArraySliceFilter
  /// 三个 filter 类（锚点由 PathFilter::execute_filter 单点持有、求值对位见
  /// 本文件各 match 臂行注释）），
  /// 统一升序排序去重后逆序 remove，避免删除引起的下标偏移，再下钻删除后剩余子节点。
  fn delete_scan_arrays(
    &self,
    val: &mut Value,
    count: &mut usize,
    hits: &dyn Fn(usize) -> Vec<usize>,
  ) {
    if let Some(arr) = val.as_array_mut() {
      remove_indices_desc(arr, hits(arr.len()), count);
      for child in arr.iter_mut() {
        self.delete_scan_arrays(child, count, hits);
      }
    } else if let Some(obj) = val.as_object_mut() {
      for (_, child) in obj.iter_mut() {
        self.delete_scan_arrays(child, count, hits);
      }
    }
  }

  /// 终结 QueryScan 删除：对子树中每个容器执行与终结 Query 臂相同的
  /// "命中子值从本容器移除"操作并继续下钻未命中分支，承接 C#
  /// QueryScanFilter.ExecuteFilter（先序栈遍历；锚点由 PathFilter::execute_filter
  /// 单点持有）；current 自身命中由父层路由处理（与既有终结 Query 臂同口径）。
  fn delete_query_scan(
    &self,
    // 谓词求值统一走入口透传的文档快照根，不再子树自造伪根
    root: &Value,
    val: &mut Value,
    expression: &QueryExpression,
    count: &mut usize,
  ) {
    if let Some(arr) = val.as_array_mut() {
      let mut i = 0;
      while i < arr.len() {
        if expression.is_match(root, &arr[i]) {
          arr.remove(i);
          *count += 1;
        } else {
          self.delete_query_scan(root, &mut arr[i], expression, count);
          i += 1;
        }
      }
    } else if let Some(obj) = val.as_object_mut() {
      let keys_to_del: Vec<String> = obj
        .iter()
        .filter_map(|(k, v)| {
          if expression.is_match(root, v) {
            Some(k.to_string())
          } else {
            None
          }
        })
        .collect();
      for k in keys_to_del {
        obj.remove(&k);
        *count += 1;
      }
      for (_, child) in obj.iter_mut() {
        self.delete_query_scan(root, child, expression, count);
      }
    }
  }

  fn delete_scan(&self, val: &mut Value, name: Option<&str>, count: &mut usize) {
    if let Some(obj) = val.as_object_mut() {
      match name {
        Some(f) => {
          if obj.remove(&f).is_some() {
            *count += 1;
          }
        }
        None => {
          *count += obj.len();
          obj.clear();
        }
      }
      for (_, child) in obj.iter_mut() {
        self.delete_scan(child, name, count);
      }
    } else if let Some(arr) = val.as_array_mut() {
      if name.is_none() {
        *count += arr.len();
        arr.clear();
      }
      for child in arr.iter_mut() {
        self.delete_scan(child, name, count);
      }
    }
  }

  pub fn mutate_recursive(
    &self,
    // 顶层文档快照：谓词内 $ 引用按真实文档根求值（对标 C# RootInFilter），
    // 变异期匹配上下文恒为变异前状态，遍历中不再逐容器 clone。
    root: &Value,
    current: &mut Value,
    filter_idx: usize,
    cb: &mut impl FnMut(&mut Value),
  ) {
    if filter_idx >= self.filters.len() {
      cb(current);
      return;
    }

    // 终结过滤器判定（对标 delete_recursive 的 is_last）：终结扫描命中禁止重入
    let is_last = filter_idx + 1 == self.filters.len();
    let filter = &self.filters[filter_idx];
    match filter {
      PathFilter::Root => {
        self.mutate_recursive(root, current, filter_idx + 1, cb);
      }
      PathFilter::Field { name } => {
        if let Some(obj) = current.as_object_mut() {
          match name {
            Some(f) => {
              if let Some(child) = obj.get_mut(f) {
                self.mutate_recursive(root, child, filter_idx + 1, cb);
              }
            }
            None => {
              for (_, child) in obj.iter_mut() {
                self.mutate_recursive(root, child, filter_idx + 1, cb);
              }
            }
          }
        }
      }
      PathFilter::FieldMultiple { names } => {
        if let Some(obj) = current.as_object_mut() {
          for n in names {
            if let Some(child) = obj.get_mut(n) {
              self.mutate_recursive(root, child, filter_idx + 1, cb);
            }
          }
        }
      }
      PathFilter::ArrayIndex { index } => {
        if let Some(arr) = current.as_array_mut() {
          match index {
            Some(idx) => {
              if let Some(i) = resolve_index(*idx, arr.len())
                && let Some(child) = arr.get_mut(i)
              {
                self.mutate_recursive(root, child, filter_idx + 1, cb);
              }
            }
            None => {
              for child in arr.iter_mut() {
                self.mutate_recursive(root, child, filter_idx + 1, cb);
              }
            }
          }
        }
      }
      PathFilter::ArrayMultipleIndex { indices } => {
        if let Some(arr) = current.as_array_mut() {
          for &idx in indices {
            if let Some(i) = resolve_index(idx, arr.len())
              && let Some(child) = arr.get_mut(i)
            {
              self.mutate_recursive(root, child, filter_idx + 1, cb);
            }
          }
        }
      }
      // 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ArraySliceFilter.cs:ExecuteFilter
      PathFilter::ArraySlice { start, end, step } => {
        if let Some(arr) = current.as_array_mut() {
          for i in slice_indices(arr.len(), *start, *end, *step) {
            if let Some(child) = arr.get_mut(i) {
              self.mutate_recursive(root, child, filter_idx + 1, cb);
            }
          }
        }
      }
      PathFilter::Scan { name } => {
        if let Some(obj) = current.as_object_mut() {
          match name {
            Some(f) => {
              if let Some(child) = obj.get_mut(f) {
                self.mutate_recursive(root, child, filter_idx + 1, cb);
              }
            }
            None => {
              for (_, child) in obj.iter_mut() {
                self.mutate_recursive(root, child, filter_idx + 1, cb);
              }
            }
          }
          // 终结命中 child 已被 cb 原位写入新值，严禁以本层 filter 重入
          // （对标 C# Set 两阶段 Evaluate().ToArray()+ReplaceWith：新容器不被重扫）；
          // 非终结或未命中分支继续下钻续扫。
          for (k, child) in obj.iter_mut() {
            if is_last && name.as_deref().is_none_or(|f| k == f) {
              continue;
            }
            self.mutate_recursive(root, child, filter_idx, cb);
          }
        } else if let Some(arr) = current.as_array_mut() {
          if name.is_none() {
            for child in arr.iter_mut() {
              self.mutate_recursive(root, child, filter_idx + 1, cb);
            }
          }
          // 具名扫描不命中数组元素，终结时仍全量下钻续扫
          if name.is_some() || !is_last {
            for child in arr.iter_mut() {
              self.mutate_recursive(root, child, filter_idx, cb);
            }
          }
        }
      }
      // 以下扫描族臂与 filter.rs execute_filter 对应臂同构：命中本层下传
      // filter_idx + 1；非终结时再以本过滤器下钻子节点续走先序遍历，
      // 终结时命中 child 不重入。
      // 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanMultipleFilter.cs:ExecuteFilter
      PathFilter::ScanMultiple { names } => {
        if let Some(obj) = current.as_object_mut() {
          for (k, child) in obj.iter_mut() {
            let hit = names.iter().any(|n| n.as_str() == k);
            if hit {
              self.mutate_recursive(root, child, filter_idx + 1, cb);
            }
            if !hit || !is_last {
              self.mutate_recursive(root, child, filter_idx, cb);
            }
          }
        } else if let Some(arr) = current.as_array_mut() {
          for child in arr.iter_mut() {
            self.mutate_recursive(root, child, filter_idx, cb);
          }
        }
      }
      // 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanArrayIndexFilter.cs:ExecuteFilter
      PathFilter::ScanArrayIndex { index } => {
        if let Some(arr) = current.as_array_mut() {
          // 终结命中位一次求解两轮共用：单下标负位折算并范围过滤
          let hit: Option<usize> = index.and_then(|idx| resolve_index(idx, arr.len()));
          match index {
            Some(_) => {
              if let Some(i) = hit
                && let Some(child) = arr.get_mut(i)
              {
                self.mutate_recursive(root, child, filter_idx + 1, cb);
              }
            }
            None => {
              for child in arr.iter_mut() {
                self.mutate_recursive(root, child, filter_idx + 1, cb);
              }
            }
          }
          for (i, child) in arr.iter_mut().enumerate() {
            if is_last && (hit == Some(i) || index.is_none()) {
              continue;
            }
            self.mutate_recursive(root, child, filter_idx, cb);
          }
        } else if let Some(obj) = current.as_object_mut() {
          for (_, child) in obj.iter_mut() {
            self.mutate_recursive(root, child, filter_idx, cb);
          }
        }
      }
      // 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanArrayMultipleIndexFilter.cs:ExecuteFilter
      PathFilter::ScanArrayMultipleIndex { indices } => {
        if let Some(arr) = current.as_array_mut() {
          // 逐下标折算过滤，重复下标保留重复命中（同 C# 枚举器逐 index 产出）
          let hits: Vec<usize> = indices
            .iter()
            .filter_map(|&idx| resolve_index(idx, arr.len()))
            .collect();
          for &i in &hits {
            if let Some(child) = arr.get_mut(i) {
              self.mutate_recursive(root, child, filter_idx + 1, cb);
            }
          }
          for (i, child) in arr.iter_mut().enumerate() {
            if is_last && hits.contains(&i) {
              continue;
            }
            self.mutate_recursive(root, child, filter_idx, cb);
          }
        } else if let Some(obj) = current.as_object_mut() {
          for (_, child) in obj.iter_mut() {
            self.mutate_recursive(root, child, filter_idx, cb);
          }
        }
      }
      // 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanArraySliceFilter.cs:ExecuteFilter
      PathFilter::ScanArraySlice { start, end, step } => {
        if let Some(arr) = current.as_array_mut() {
          let hits = slice_indices(arr.len(), *start, *end, *step);
          for &i in &hits {
            if let Some(child) = arr.get_mut(i) {
              self.mutate_recursive(root, child, filter_idx + 1, cb);
            }
          }
          for (i, child) in arr.iter_mut().enumerate() {
            if is_last && hits.contains(&i) {
              continue;
            }
            self.mutate_recursive(root, child, filter_idx, cb);
          }
        } else if let Some(obj) = current.as_object_mut() {
          for (_, child) in obj.iter_mut() {
            self.mutate_recursive(root, child, filter_idx, cb);
          }
        }
      }
      PathFilter::Query { expression } => {
        if let Some(arr) = current.as_array_mut() {
          for item in arr.iter_mut() {
            if expression.is_match(root, item) {
              self.mutate_recursive(root, item, filter_idx + 1, cb);
            }
          }
        } else if let Some(obj) = current.as_object_mut() {
          for (_, item) in obj.iter_mut() {
            if expression.is_match(root, item) {
              self.mutate_recursive(root, item, filter_idx + 1, cb);
            }
          }
        }
        // 标量节点无臂恒零产出（对标 C# QueryFilter.ExecuteFilter 三臂，
        // 与求值臂 filter.rs 单语义收口；原标量自匹配兜底系分叉源，已删）。
      }
      // 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryScanFilter.cs:ExecuteFilter
      PathFilter::QueryScan { expression } => {
        // C# 枚举器先判 current 自身再栈式下钻；终结自命中已被 cb 原位写入新值，
        // 严禁下钻新值内部，仅未命中或非终结时下钻子节点续扫。
        let hit = expression.is_match(root, current);
        if hit {
          self.mutate_recursive(root, current, filter_idx + 1, cb);
        }
        if !hit || !is_last {
          if let Some(obj) = current.as_object_mut() {
            for (_, child) in obj.iter_mut() {
              self.mutate_recursive(root, child, filter_idx, cb);
            }
          } else if let Some(arr) = current.as_array_mut() {
            for child in arr.iter_mut() {
              self.mutate_recursive(root, child, filter_idx, cb);
            }
          }
        }
      }
    }
  }
}
