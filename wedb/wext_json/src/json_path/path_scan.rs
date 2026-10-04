//! 扫描族（`$..` 下钻）终结删除状态机域：delete_scan / delete_scan_multiple /
//! delete_scan_arrays / delete_query_scan，由 path_delete.rs 的扫描臂路由调用。

use sonic_rs::{JsonValueMutTrait, Value};

use super::{expression::QueryExpression, path::JsonPath, path_delete::remove_indices_desc};

impl JsonPath {
  /// 终结 ScanMultiple 删除：逐层对象移除命中 names 的键再下钻剩余子节点，
  /// 命中点与遍历序 1:1 对应 C# ScanMultipleFilter.ExecuteFilter
  ///（仅对象成员可命中，数组仅下钻；锚点由 PathFilter::execute_filter
  /// 单点持有）；先收集后删除避免迭代中改动对象。
  pub(super) fn delete_scan_multiple(&self, val: &mut Value, names: &[String], count: &mut usize) {
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
  ///（负下标折算/切片步进由调用方闭包给出，与各自 C# ExecuteFilter 对位：
  /// ScanArrayIndexFilter、ScanArrayMultipleIndexFilter、ScanArraySliceFilter
  /// 三个 filter 类（锚点由 PathFilter::execute_filter 单点持有、求值对位见
  /// path_delete.rs 各 match 臂行注释）），
  /// 统一升序排序去重后逆序 remove，避免删除引起的下标偏移，再下钻删除后剩余子节点。
  pub(super) fn delete_scan_arrays<F: Fn(usize) -> Vec<usize>>(
    &self,
    val: &mut Value,
    count: &mut usize,
    hits: &F,
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
  pub(super) fn delete_query_scan(
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

  pub(super) fn delete_scan(&self, val: &mut Value, name: Option<&str>, count: &mut usize) {
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
}
