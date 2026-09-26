//! 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/PathFilter.cs
//!
//! 路径过滤器:Rust 枚举合并承接 C# PathFilter.cs 与全部 *Filter.cs 类族,
//! 经 enum/match 静态分发,不还原 C# 继承体系。

use sonic_rs::{JsonContainerTrait, Value};

use super::expression::QueryExpression;

/// 路径过滤器（Rust 枚举合并承接 C# 多 filter 类，逐臂标注对应 C# 文件:类）
///
/// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/PathFilter.cs:PathFilter
#[derive(Debug, Clone)]
pub enum PathFilter {
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/RootFilter.cs:RootFilter
  Root,

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/FieldFilter.cs:FieldFilter
  ///
  /// 求值承接 C# ExecuteFilter / ExecuteFilterMultiple
  /// （在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/FieldFilter.cs:ExecuteFilterMultiple）。
  Field { name: Option<String> }, // None = Wildcard '*'

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/FieldMultipleFilter.cs:FieldMultipleFilter
  FieldMultiple { names: Vec<String> },

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ArrayIndexFilter.cs:ArrayIndexFilter
  ///
  /// 求值承接 C# ExecuteFilter / ExecuteFilterMultiple
  /// （在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ArrayIndexFilter.cs:ExecuteFilterMultiple），
  /// 负下标折算承接
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/PathFilter.cs:TryGetTokenIndex。
  ArrayIndex { index: Option<i64> }, // None = Wildcard '[*]'

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ArrayMultipleIndexFilter.cs:ArrayMultipleIndexFilter
  ArrayMultipleIndex { indices: Vec<i64> },

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ArraySliceFilter.cs:ArraySliceFilter
  ArraySlice {
    start: Option<i64>,
    end: Option<i64>,
    step: Option<i64>,
  },

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanFilter.cs:ScanFilter
  Scan { name: Option<String> }, // None = '..*'

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanMultipleFilter.cs:ScanMultipleFilter
  ScanMultiple { names: Vec<String> },

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanArrayIndexFilter.cs:ScanArrayIndexFilter
  ScanArrayIndex { index: Option<i64> },

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanArrayMultipleIndexFilter.cs:ScanArrayMultipleIndexFilter
  ScanArrayMultipleIndex { indices: Vec<i64> },

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanArraySliceFilter.cs:ScanArraySliceFilter
  ScanArraySlice {
    start: Option<i64>,
    end: Option<i64>,
    step: Option<i64>,
  },

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryFilter.cs:QueryFilter
  Query { expression: QueryExpression },

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryScanFilter.cs:QueryScanFilter
  QueryScan { expression: QueryExpression },
}

impl PathFilter {
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/PathFilter.cs:ExecuteFilter
  ///
  /// 单引擎合并承接各具体 filter 的 ExecuteFilter（单节点与 IEnumerable 两重载
  /// 同名同面，逐臂对位；C# 虚接口虚方法多态在 rust 收敛为 PathFilter 枚举
  /// 静态分派，本注释块即各具体 filter 类 ExecuteFilter 的唯一法定映射位，
  /// 求值辅助函数的对位说明一律用纯文本、不重复挂锚）：
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/RootFilter.cs:ExecuteFilter
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/FieldFilter.cs:ExecuteFilter
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/FieldMultipleFilter.cs:ExecuteFilter
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ArrayIndexFilter.cs:ExecuteFilter
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ArrayMultipleIndexFilter.cs:ExecuteFilter
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanFilter.cs:ExecuteFilter
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanMultipleFilter.cs:ExecuteFilter
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanArrayIndexFilter.cs:ExecuteFilter
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanArrayMultipleIndexFilter.cs:ExecuteFilter
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryFilter.cs:ExecuteFilter
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryScanFilter.cs:ExecuteFilter
  pub fn execute_filter<'a>(&self, root: &'a Value, current: &'a Value) -> Vec<&'a Value> {
    match self {
      Self::Root => vec![root],
      Self::Field { name } => match name {
        Some(field) => current
          .as_object()
          .and_then(|obj| obj.get(field))
          .map(|v| vec![v])
          .unwrap_or_default(),
        None => current
          .as_object()
          .map(|obj| obj.iter().map(|(_, v)| v).collect())
          .unwrap_or_default(),
      },
      Self::FieldMultiple { names } => {
        let mut res = Vec::new();
        if let Some(obj) = current.as_object() {
          for n in names {
            if let Some(v) = obj.get(n) {
              res.push(v);
            }
          }
        }
        res
      }
      Self::ArrayIndex { index } => {
        let Some(arr) = current.as_array() else {
          return Vec::new();
        };
        match index {
          Some(idx) => {
            let actual_idx = if *idx < 0 {
              arr.len() as i64 + *idx
            } else {
              *idx
            };
            if actual_idx >= 0 && (actual_idx as usize) < arr.len() {
              arr.get(actual_idx as usize).into_iter().collect()
            } else {
              Vec::new()
            }
          }
          None => arr.iter().collect(),
        }
      }
      Self::ArrayMultipleIndex { indices } => {
        let Some(arr) = current.as_array() else {
          return Vec::new();
        };
        let mut res = Vec::new();
        for &idx in indices {
          let actual_idx = if idx < 0 { arr.len() as i64 + idx } else { idx };
          if actual_idx >= 0
            && (actual_idx as usize) < arr.len()
            && let Some(v) = arr.get(actual_idx as usize)
          {
            res.push(v);
          }
        }
        res
      }
      Self::ArraySlice { start, end, step } => {
        let Some(arr) = current.as_array() else {
          return Vec::new();
        };
        slice_array(arr.iter(), arr.len(), *start, *end, *step)
      }
      Self::Scan { name } => {
        let mut res = Vec::new();
        scan_filter_walk(current, name.as_deref(), &mut res);
        res
      }
      Self::ScanMultiple { names } => {
        let mut res = Vec::new();
        scan_descendants(current, &mut |v| {
          if let Some(obj) = v.as_object() {
            for n in names {
              if let Some(child) = obj.get(n) {
                res.push(child);
              }
            }
          }
        });
        res
      }
      Self::ScanArrayIndex { index } => {
        let mut res = Vec::new();
        scan_descendants(current, &mut |v| {
          if let Some(arr) = v.as_array() {
            match index {
              Some(idx) => {
                let actual = if *idx < 0 {
                  arr.len() as i64 + *idx
                } else {
                  *idx
                };
                if actual >= 0
                  && (actual as usize) < arr.len()
                  && let Some(child) = arr.get(actual as usize)
                {
                  res.push(child);
                }
              }
              None => {
                for child in arr.iter() {
                  res.push(child);
                }
              }
            }
          }
        });
        res
      }
      Self::ScanArrayMultipleIndex { indices } => {
        let mut res = Vec::new();
        scan_descendants(current, &mut |v| {
          if let Some(arr) = v.as_array() {
            for &idx in indices {
              let actual = if idx < 0 { arr.len() as i64 + idx } else { idx };
              if actual >= 0
                && (actual as usize) < arr.len()
                && let Some(child) = arr.get(actual as usize)
              {
                res.push(child);
              }
            }
          }
        });
        res
      }
      Self::ScanArraySlice { start, end, step } => {
        let mut res = Vec::new();
        scan_descendants(current, &mut |v| {
          if let Some(arr) = v.as_array() {
            let sliced = slice_array(arr.iter(), arr.len(), *start, *end, *step);
            res.extend(sliced);
          }
        });
        res
      }
      Self::Query { expression } => {
        // 对标 C# QueryFilter.ExecuteFilter 节点三臂：JsonArray 遍历元素、
        // JsonObject 遍历属性值、标量无臂恒零产出——求值/删除/变异三引擎单语义。
        let mut res = Vec::new();
        if let Some(arr) = current.as_array() {
          for item in arr.iter() {
            if expression.is_match(root, item) {
              res.push(item);
            }
          }
        } else if let Some(obj) = current.as_object() {
          for (_, v) in obj.iter() {
            if expression.is_match(root, v) {
              res.push(v);
            }
          }
        }
        res
      }
      Self::QueryScan { expression } => {
        let mut res = Vec::new();
        scan_descendants(current, &mut |v| {
          if expression.is_match(root, v) {
            res.push(v);
          }
        });
        res
      }
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ArraySliceFilter.cs:IsValid
  ///
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/ScanArraySliceFilter.cs:IsValid
  pub fn is_valid(&self) -> bool {
    true
  }
}

fn scan_descendants<'a>(val: &'a Value, cb: &mut impl FnMut(&'a Value)) {
  cb(val);
  if let Some(obj) = val.as_object() {
    for (_, child) in obj.iter() {
      scan_descendants(child, cb);
    }
  } else if let Some(arr) = val.as_array() {
    for child in arr.iter() {
      scan_descendants(child, cb);
    }
  }
}

/// 递归通配扫描：先序深度优先，遇到数组元素与对象值时按 name 匹配后立刻下钻,
/// 1:1 承接 C# ScanFilter.ExecuteFilter（锚点由 PathFilter::execute_filter
/// 单点持有）的枚举器 + 栈回溯实现，避免"父节点的兄弟先于子节点"造成的排序偏差；
/// name 为 None 时数组元素也计入匹配，name 为 Some 时只对对象键做匹配。
fn scan_filter_walk<'a>(v: &'a Value, name: Option<&str>, res: &mut Vec<&'a Value>) {
  if let Some(arr) = v.as_array() {
    for child in arr.iter() {
      if name.is_none() {
        res.push(child);
      }
      scan_filter_walk(child, name, res);
    }
  } else if let Some(obj) = v.as_object() {
    for (k, child) in obj.iter() {
      if name.is_none() || name == Some(k) {
        res.push(child);
      }
      scan_filter_walk(child, name, res);
    }
  }
}

/// 切片求值核：负下标折算与正反步进遍历，承接 C# ArraySliceFilter.ExecuteFilter
/// 与 ScanArraySliceFilter.ExecuteFilter 的切片循环本体（两者步进语义同构，
/// 仅外层遍历容器不同；锚点由 PathFilter::execute_filter 调用面单点持有）。
/// 索引计算下沉到 slice_indices，本函数只负责把命中的下标映射回 Value 引用。
fn slice_array<'a, I>(
  iter: I,
  len: usize,
  start: Option<i64>,
  end: Option<i64>,
  step: Option<i64>,
) -> Vec<&'a Value>
where
  I: Iterator<Item = &'a Value> + Clone,
{
  let items: Vec<&'a Value> = iter.collect();
  slice_indices(len, start, end, step)
    .into_iter()
    .map(|i| items[i])
    .collect()
}

/// 数组切片下标求解：正向步进返回升序、反向步进返回降序，与
/// C# ArraySliceFilter.ExecuteFilter
/// 的 `for (int i = startIndex; IsValid(i, stopIndex, positiveStep); i += stepCount)`
/// 完全对位；负下标折算、边界钳制、step==0 空返回与 C# 逐条同构，
/// 供 mutate_recursive / delete_recursive 的新增切片臂共享。
pub(super) fn slice_indices(
  len: usize,
  start: Option<i64>,
  end: Option<i64>,
  step: Option<i64>,
) -> Vec<usize> {
  let step_val = step.unwrap_or(1);
  if step_val == 0 || len == 0 {
    return Vec::new();
  }

  let len_i = len as i64;
  let mut s = start.unwrap_or(if step_val > 0 { 0 } else { len_i - 1 });
  let mut e = end.unwrap_or(if step_val > 0 { len_i } else { -len_i - 1 });

  if s < 0 {
    s += len_i;
  }
  if e < 0 {
    e += len_i;
  }

  let mut res = Vec::new();
  if step_val > 0 {
    let mut curr = s.max(0);
    let stop = e.min(len_i);
    while curr < stop {
      res.push(curr as usize);
      curr += step_val;
    }
  } else {
    let mut curr = s.min(len_i - 1);
    let stop = e.max(-1);
    while curr > stop {
      if curr >= 0 {
        res.push(curr as usize);
      }
      curr += step_val;
    }
  }
  res
}

pub(super) fn evaluate_filters<'a>(
  filters: &[PathFilter],
  root: &'a Value,
  target: &'a Value,
) -> Vec<&'a Value> {
  if filters.is_empty() {
    return vec![target];
  }

  let mut current = vec![target];
  for filter in filters {
    let mut next = Vec::new();
    for item in current {
      let matched = filter.execute_filter(root, item);
      next.extend(matched);
    }
    current = next;
    if current.is_empty() {
      break;
    }
  }
  current
}
