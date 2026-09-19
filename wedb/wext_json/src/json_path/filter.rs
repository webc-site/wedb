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
        scan_descendants(current, &mut |v| {
          if let Some(obj) = v.as_object() {
            match name {
              Some(field) => {
                if let Some(child) = obj.get(field) {
                  res.push(child);
                }
              }
              None => {
                for (_, child) in obj.iter() {
                  res.push(child);
                }
              }
            }
          }
        });
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
        let mut res = Vec::new();
        if let Some(arr) = current.as_array() {
          for item in arr.iter() {
            if expression.is_match(root, item) {
              res.push(item);
            }
          }
        } else if expression.is_match(root, current) {
          res.push(current);
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
  let step_val = step.unwrap_or(1);
  if step_val == 0 {
    return Vec::new();
  }

  let items: Vec<&'a Value> = iter.collect();
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
      if (curr as usize) < items.len() {
        res.push(items[curr as usize]);
      }
      curr += step_val;
    }
  } else {
    let mut curr = s.min(len_i - 1);
    let stop = e.max(-1);
    while curr > stop {
      if curr >= 0 && (curr as usize) < items.len() {
        res.push(items[curr as usize]);
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
