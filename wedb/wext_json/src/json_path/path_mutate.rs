//! mutate_recursive 状态机域：按过滤器链逐层路由的就地变异（替换）。

use sonic_rs::{JsonValueMutTrait, Value};

use super::{
  filter::{PathFilter, resolve_index, slice_indices},
  path::JsonPath,
};

impl JsonPath {
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
        } else if name.is_none()
          && let Some(arr) = current.as_array_mut()
        {
          // 交错臂：`$.*` 打数组根逐元素下传（对标 FieldFilter.cs:59-61/:135-141，
          // 仅 Name is null 成立；Some(name) 仅认对象，与 C# TryGetPropertyValue 同形）。
          for child in arr.iter_mut() {
            self.mutate_recursive(root, child, filter_idx + 1, cb);
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
        } else if index.is_none()
          && let Some(obj) = current.as_object_mut()
        {
          // 交错臂：`$[*]` 打对象根逐属性值下传（对标 ArrayIndexFilter.cs:45-47/
          // :115-121）；Some(idx) 仅认数组，与 C# TryGetTokenIndex 同形，严禁扩对象。
          for (_, child) in obj.iter_mut() {
            self.mutate_recursive(root, child, filter_idx + 1, cb);
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
          for i in slice_indices(arr.len(), *start, *end, *step).unwrap_or_default() {
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
          //（对标 C# Set 两阶段 Evaluate().ToArray()+ReplaceWith：新容器不被重扫）；
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
          // 交错臂：`$..[*]` 下钻期对象属性值同为命中（对标 ScanArrayIndexFilter.cs
          // :71-74，yield 严格在 Index is null 门内）；Some(idx) 时对象仅下钻不产出。
          // 终结命中 child 已被 cb 原位写入，不重入（沿 arr 臂 continue 形）。
          if index.is_none() {
            for (_, child) in obj.iter_mut() {
              self.mutate_recursive(root, child, filter_idx + 1, cb);
            }
          }
          for (_, child) in obj.iter_mut() {
            if is_last && index.is_none() {
              continue;
            }
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
          let hits = slice_indices(arr.len(), *start, *end, *step).unwrap_or_default();
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
