//! 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/PathFilter.cs
//!
//! 路径过滤器:Rust 枚举合并承接 C# PathFilter.cs 与全部 *Filter.cs 类族,
//! 经 enum/match 静态分发,不还原 C# 继承体系。

use sonic_rs::{JsonContainerTrait, Value};

use super::expression::QueryExpression;
use crate::error::{Error, Result};

/// 负下标折算 + 越界判定的单源：JSONPath 的 i64 下标（负值自尾部起算）归一为
/// 数组实际下标；折算后仍为负或越界返回 None。求值面四处（ArrayIndex /
/// ArrayMultipleIndex / ScanArrayIndex / ScanArrayMultipleIndex）内联算式的
/// 收口；path.rs 变异面同语义消费本单点（use filter::{resolve_index,
/// slice_indices}），全仓无第二实现。
#[inline]
pub(super) fn resolve_index(idx: i64, len: usize) -> Option<usize> {
  let actual = if idx < 0 { len as i64 + idx } else { idx };
  (actual >= 0 && (actual as usize) < len).then_some(actual as usize)
}

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
  pub fn execute_filter<'a>(&self, root: &'a Value, current: &'a Value) -> Result<Vec<&'a Value>> {
    let res = match self {
      Self::Root => vec![root],
      Self::Field { name } => match name {
        Some(field) => current
          .as_object()
          .and_then(|obj| obj.get(field))
          .map(|v| vec![v])
          .unwrap_or_default(),
        None => {
          // 交错臂：对标 FieldFilter.cs:59-61/:135-141，Name is null 时对象产出
          // 属性值、数组产出全部元素（`$.*` 打数组根）；Some(name) 仅认对象。
          if let Some(obj) = current.as_object() {
            obj.iter().map(|(_, v)| v).collect()
          } else if let Some(arr) = current.as_array() {
            arr.iter().collect()
          } else {
            Vec::new()
          }
        }
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
      Self::ArrayIndex { index } => match index {
        // Some(idx) 走 TryGetTokenIndex，仅认数组，严禁扩对象。
        Some(idx) => {
          let Some(arr) = current.as_array() else {
            return Ok(Vec::new());
          };
          resolve_index(*idx, arr.len())
            .and_then(|i| arr.get(i))
            .into_iter()
            .collect()
        }
        // 交错臂：对标 ArrayIndexFilter.cs:45-47/:115-121，Index is null 时数组
        // 产出元素、对象产出全部属性值（`$[*]` 打对象根）。
        None => {
          if let Some(arr) = current.as_array() {
            arr.iter().collect()
          } else if let Some(obj) = current.as_object() {
            obj.iter().map(|(_, v)| v).collect()
          } else {
            Vec::new()
          }
        }
      },
      Self::ArrayMultipleIndex { indices } => {
        let Some(arr) = current.as_array() else {
          return Ok(Vec::new());
        };
        let mut res = Vec::new();
        for &idx in indices {
          if let Some(v) = resolve_index(idx, arr.len()).and_then(|i| arr.get(i)) {
            res.push(v);
          }
        }
        res
      }
      Self::ArraySlice { start, end, step } => {
        if *step == Some(0) {
          return Err(Error::StepCannotBeZero);
        }
        let Some(arr) = current.as_array() else {
          return Ok(Vec::new());
        };
        slice_array(arr.iter(), arr.len(), *start, *end, *step)?
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
        match index {
          // 交错臂：ScanArrayIndexFilter.cs Index is null 门对数组元素与对象属性值
          // 双向 yield（:59-74 枚举器形），与 ScanFilter(Name null) 同构，
          // 故与 Scan{None} 共用 scan_filter_walk 先序双容器遍历；
          // Some(idx) 仍经 scan_descendants 仅对数组命中（TryGetTokenIndex 仅认数组）。
          None => scan_filter_walk(current, None, &mut res),
          Some(idx) => {
            scan_descendants(current, &mut |v| {
              if let Some(arr) = v.as_array()
                && let Some(child) = resolve_index(*idx, arr.len()).and_then(|i| arr.get(i))
              {
                res.push(child);
              }
            });
          }
        }
        res
      }
      Self::ScanArrayMultipleIndex { indices } => {
        let mut res = Vec::new();
        scan_descendants(current, &mut |v| {
          if let Some(arr) = v.as_array() {
            for &idx in indices {
              if let Some(child) = resolve_index(idx, arr.len()).and_then(|i| arr.get(i)) {
                res.push(child);
              }
            }
          }
        });
        res
      }
      Self::ScanArraySlice { start, end, step } => {
        if *step == Some(0) {
          return Err(Error::StepCannotBeZero);
        }
        let mut res = Vec::new();
        let mut err = None;
        scan_descendants(current, &mut |v| {
          if err.is_some() {
            return;
          }
          if let Some(arr) = v.as_array() {
            match slice_array(arr.iter(), arr.len(), *start, *end, *step) {
              Ok(sliced) => res.extend(sliced),
              Err(e) => err = Some(e),
            }
          }
        });
        if let Some(e) = err {
          return Err(e);
        }
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
    };
    Ok(res)
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
/// ScanArrayIndexFilter 在 Index is null 时的枚举器与 ScanFilter(Name null)
/// 逐行同构（仅 Index 非 null 的 TryGetTokenIndex 分支不同），故其 None 臂
/// 共享本遍历。
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
) -> Result<Vec<&'a Value>>
where
  I: Iterator<Item = &'a Value> + Clone,
{
  let indices = slice_indices(len, start, end, step)?;
  if indices.is_empty() {
    return Ok(Vec::new());
  }
  let items: Vec<&'a Value> = iter.collect();
  Ok(indices.into_iter().map(|i| items[i]).collect())
}

/// 数组切片下标求解：正向步进返回升序、反向步进返回降序，与
/// C# ArraySliceFilter.ExecuteFilter
/// 的 `for (int i = startIndex; IsValid(i, stopIndex, positiveStep); i += stepCount)`
/// 完全对位；C# Step==0 抛 JsonException，rust 经错误通道等价收帧；
/// len==0 回空与 C# 空数组同形，
/// 供 mutate_recursive / delete_recursive 的新增切片臂共享。
///
/// modules/GarnetJSON/JSONPath/ArraySliceFilter.cs:IsValid
/// modules/GarnetJSON/JSONPath/ScanArraySliceFilter.cs:IsValid
///（C# 两筛选器的循环界谓词折入本函数 `while curr < stop` 边界形）
pub(super) fn slice_indices(
  len: usize,
  start: Option<i64>,
  end: Option<i64>,
  step: Option<i64>,
) -> Result<Vec<usize>> {
  let step_val = step.unwrap_or(1);
  if step_val == 0 {
    return Err(Error::StepCannotBeZero);
  }
  if len == 0 {
    return Ok(Vec::new());
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
      match curr.checked_add(step_val) {
        Some(next) => curr = next,
        None => break,
      }
    }
  } else {
    let mut curr = s.min(len_i - 1);
    let stop = e.max(-1);
    while curr > stop {
      if curr >= 0 {
        res.push(curr as usize);
      }
      match curr.checked_add(step_val) {
        Some(next) => curr = next,
        None => break,
      }
    }
  }
  Ok(res)
}

pub(super) fn evaluate_filters<'a>(
  filters: &[PathFilter],
  root: &'a Value,
  target: &'a Value,
) -> Result<Vec<&'a Value>> {
  if filters.is_empty() {
    return Ok(vec![target]);
  }

  let mut current = vec![target];
  for filter in filters {
    let mut next = Vec::new();
    for item in current {
      let matched = filter.execute_filter(root, item)?;
      next.extend(matched);
    }
    current = next;
    if current.is_empty() {
      break;
    }
  }
  Ok(current)
}
