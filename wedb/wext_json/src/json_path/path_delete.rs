//! delete_recursive 状态机域：按过滤器链逐层路由的就地删除。

use sonic_rs::{JsonValueMutTrait, Value};

use super::{
  filter::{PathFilter, resolve_index, slice_indices},
  path::JsonPath,
};

/// 数组按命中下标集终结删除的共享骨架：统一升序排序去重后逆序 remove，
/// 避免前面的删除使后面的下标偏移（负步进切片/重复多下标共用）。
#[inline]
pub(super) fn remove_indices_desc(
  arr: &mut sonic_rs::Array,
  mut idxs: Vec<usize>,
  count: &mut usize,
) {
  idxs.sort_unstable();
  idxs.dedup();
  for &i in idxs.iter().rev() {
    arr.remove(i);
    *count += 1;
  }
}

impl JsonPath {
  pub(super) fn delete_recursive(
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
          } else if name.is_none()
            && let Some(arr) = current.as_array_mut()
          {
            // 交错臂（终结）：`$.*` 打数组根清空全部元素，沿用 delete_scan
            // None 臂数组清元素的容器对偶形；Some(name) 仅认对象。
            *count += arr.len();
            arr.clear();
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
          } else if index.is_none()
            && let Some(obj) = current.as_object_mut()
          {
            // 交错臂（终结）：`$[*]` 打对象根删全部键值，与数组根清元素对偶
            //（对标 ArrayIndexFilter.cs:45-47 求值命中形；删除为 rust 自有引擎，
            // 按 delete_scan None 臂双容器口径收口）。
            *count += obj.len();
            obj.clear();
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
            let hits = slice_indices(arr.len(), *start, *end, *step).unwrap_or_default();
            // 负步进返回降序，删除前统一升序排序后逆序 remove（骨架见 remove_indices_desc）。
            remove_indices_desc(arr, hits, count);
          }
        }
        // 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanMultipleFilter.cs:ExecuteFilter
        PathFilter::ScanMultiple { names } => {
          self.delete_scan_multiple(current, names, count);
        }
        // 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanArrayIndexFilter.cs:ExecuteFilter
        PathFilter::ScanArrayIndex { index } => match index {
          Some(idx) => {
            self.delete_scan_arrays(current, count, &|len| match resolve_index(*idx, len) {
              Some(i) => vec![i],
              None => Vec::new(),
            })
          }
          // 交错臂（终结）：Index is null 的 ScanArrayIndexFilter 枚举器与
          // ScanFilter(Name null) 逐行同构，下钻期对象属性值同为命中，与终结
          // Scan{None} 共用 delete_scan 双容器清形（对象清键值、数组清元素）。
          None => self.delete_scan(current, None, count),
        },
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
            slice_indices(len, *start, *end, *step).unwrap_or_default()
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
          } else if name.is_none()
            && let Some(arr) = current.as_array_mut()
          {
            // 交错臂（中间层）：`$.*` 打数组根逐元素下传续走后续过滤器。
            for child in arr.iter_mut() {
              self.delete_recursive(root, child, filter_idx + 1, count);
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
          } else if index.is_none()
            && let Some(obj) = current.as_object_mut()
          {
            // 交错臂（中间层）：`$[*]` 打对象根逐属性值下传续走后续过滤器。
            for (_, child) in obj.iter_mut() {
              self.delete_recursive(root, child, filter_idx + 1, count);
            }
          }
        }
        PathFilter::Scan { name } => {
          // 中间层扫描多链路由（与 ScanMultiple 中间层臂同构，对标 mutate 侧
          // Scan 臂）：命中子节点下传 filter_idx + 1 续走余链，全部子节点再以
          // 本层重入续扫嵌套命中（删除不换值，命中子树保留续扫，与 mutate 的
          // 「终结命中严禁重入」相反）；直接调 delete_scan 即终结语义，会误删
          // 命中键整棵子树（$..a.b 连 a 一并删）。
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
            for (_, child) in obj.iter_mut() {
              self.delete_recursive(root, child, filter_idx, count);
            }
          } else if let Some(arr) = current.as_array_mut() {
            // 具名扫描不命中数组元素，仅 None 档（$..*）逐元素命中下传
            if name.is_none() {
              for child in arr.iter_mut() {
                self.delete_recursive(root, child, filter_idx + 1, count);
              }
            }
            for child in arr.iter_mut() {
              self.delete_recursive(root, child, filter_idx, count);
            }
          }
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
            for i in slice_indices(arr.len(), *start, *end, *step).unwrap_or_default() {
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
            // 交错臂（中间层）：`$..[*]` 下钻期对象属性值同为命中，命中下传
            // filter_idx + 1 并以本过滤器续扫（Index is null 枚举器与 ScanFilter
            // 同构）；Some(idx) 时对象仅下钻不产出。
            if index.is_none() {
              for (_, child) in obj.iter_mut() {
                self.delete_recursive(root, child, filter_idx + 1, count);
              }
            }
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
            for i in slice_indices(arr.len(), *start, *end, *step).unwrap_or_default() {
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
}
