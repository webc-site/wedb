//! 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs
//!
//! JSONPath 路径对象与求值、变异(替换/删除)。

use sonic_rs::{JsonValueMutTrait, Value};

use super::{
  filter::{PathFilter, evaluate_filters},
  parser::JsonPathParser,
};
use crate::error::Result;

/// JSONPath 主结构
///
/// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs
#[derive(Debug, Clone)]
pub struct JsonPath {
  pub filters: Vec<PathFilter>,
}

impl JsonPath {
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:JsonPath
  pub fn parse(expression: &str) -> Result<Self> {
    let mut parser = JsonPathParser::new(expression);
    let filters = parser.parse_main()?;
    Ok(Self { filters })
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
    self.mutate_recursive(root, 0, &mut |target| {
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
    self.delete_recursive(root, 0, &mut count);
    count
  }

  fn delete_recursive(&self, current: &mut Value, filter_idx: usize, count: &mut usize) {
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
                let actual = if *idx < 0 {
                  arr.len() as i64 + *idx
                } else {
                  *idx
                };
                if actual >= 0 && (actual as usize) < arr.len() {
                  arr.remove(actual as usize);
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
            let mut sorted: Vec<usize> = indices
              .iter()
              .filter_map(|&idx| {
                let actual = if idx < 0 { arr.len() as i64 + idx } else { idx };
                if actual >= 0 && (actual as usize) < arr.len() {
                  Some(actual as usize)
                } else {
                  None
                }
              })
              .collect();
            sorted.sort_unstable();
            sorted.dedup();
            for &idx in sorted.iter().rev() {
              arr.remove(idx);
              *count += 1;
            }
          }
        }
        PathFilter::Scan { name } => {
          self.delete_scan(current, name.as_deref(), count);
        }
        PathFilter::Query { expression } => {
          let dummy_root = current.clone();
          if let Some(arr) = current.as_array_mut() {
            let mut i = 0;
            while i < arr.len() {
              if expression.is_match(&dummy_root, &arr[i]) {
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
                if expression.is_match(&dummy_root, v) {
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
        _ => {}
      }
    } else {
      match filter {
        PathFilter::Root => {
          self.delete_recursive(current, filter_idx + 1, count);
        }
        PathFilter::Field { name } => {
          if let Some(obj) = current.as_object_mut() {
            match name {
              Some(f) => {
                if let Some(child) = obj.get_mut(f) {
                  self.delete_recursive(child, filter_idx + 1, count);
                }
              }
              None => {
                for (_, child) in obj.iter_mut() {
                  self.delete_recursive(child, filter_idx + 1, count);
                }
              }
            }
          }
        }
        PathFilter::ArrayIndex { index } => {
          if let Some(arr) = current.as_array_mut() {
            match index {
              Some(idx) => {
                let actual = if *idx < 0 {
                  arr.len() as i64 + *idx
                } else {
                  *idx
                };
                if actual >= 0
                  && (actual as usize) < arr.len()
                  && let Some(child) = arr.get_mut(actual as usize)
                {
                  self.delete_recursive(child, filter_idx + 1, count);
                }
              }
              None => {
                for child in arr.iter_mut() {
                  self.delete_recursive(child, filter_idx + 1, count);
                }
              }
            }
          }
        }
        PathFilter::Scan { name } => {
          self.delete_scan(current, name.as_deref(), count);
        }
        _ => {}
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
      for child in arr.iter_mut() {
        self.delete_scan(child, name, count);
      }
    }
  }

  pub fn mutate_recursive(
    &self,
    current: &mut Value,
    filter_idx: usize,
    cb: &mut impl FnMut(&mut Value),
  ) {
    if filter_idx >= self.filters.len() {
      cb(current);
      return;
    }

    let filter = &self.filters[filter_idx];
    match filter {
      PathFilter::Root => {
        self.mutate_recursive(current, filter_idx + 1, cb);
      }
      PathFilter::Field { name } => {
        if let Some(obj) = current.as_object_mut() {
          match name {
            Some(f) => {
              if let Some(child) = obj.get_mut(f) {
                self.mutate_recursive(child, filter_idx + 1, cb);
              }
            }
            None => {
              for (_, child) in obj.iter_mut() {
                self.mutate_recursive(child, filter_idx + 1, cb);
              }
            }
          }
        }
      }
      PathFilter::FieldMultiple { names } => {
        if let Some(obj) = current.as_object_mut() {
          for n in names {
            if let Some(child) = obj.get_mut(n) {
              self.mutate_recursive(child, filter_idx + 1, cb);
            }
          }
        }
      }
      PathFilter::ArrayIndex { index } => {
        if let Some(arr) = current.as_array_mut() {
          match index {
            Some(idx) => {
              let actual = if *idx < 0 {
                arr.len() as i64 + *idx
              } else {
                *idx
              };
              if actual >= 0
                && (actual as usize) < arr.len()
                && let Some(child) = arr.get_mut(actual as usize)
              {
                self.mutate_recursive(child, filter_idx + 1, cb);
              }
            }
            None => {
              for child in arr.iter_mut() {
                self.mutate_recursive(child, filter_idx + 1, cb);
              }
            }
          }
        }
      }
      PathFilter::ArrayMultipleIndex { indices } => {
        if let Some(arr) = current.as_array_mut() {
          for &idx in indices {
            let actual = if idx < 0 { arr.len() as i64 + idx } else { idx };
            if actual >= 0
              && (actual as usize) < arr.len()
              && let Some(child) = arr.get_mut(actual as usize)
            {
              self.mutate_recursive(child, filter_idx + 1, cb);
            }
          }
        }
      }
      PathFilter::Scan { name } => {
        if let Some(obj) = current.as_object_mut() {
          match name {
            Some(f) => {
              if let Some(child) = obj.get_mut(f) {
                self.mutate_recursive(child, filter_idx + 1, cb);
              }
            }
            None => {
              for (_, child) in obj.iter_mut() {
                self.mutate_recursive(child, filter_idx + 1, cb);
              }
            }
          }
          for (_, child) in obj.iter_mut() {
            self.mutate_recursive(child, filter_idx, cb);
          }
        } else if let Some(arr) = current.as_array_mut() {
          for child in arr.iter_mut() {
            self.mutate_recursive(child, filter_idx, cb);
          }
        }
      }
      PathFilter::Query { expression } => {
        let dummy = current.clone();
        if let Some(arr) = current.as_array_mut() {
          for item in arr.iter_mut() {
            if expression.is_match(&dummy, item) {
              self.mutate_recursive(item, filter_idx + 1, cb);
            }
          }
        } else if let Some(obj) = current.as_object_mut() {
          for (_, item) in obj.iter_mut() {
            if expression.is_match(&dummy, item) {
              self.mutate_recursive(item, filter_idx + 1, cb);
            }
          }
        } else if expression.is_match(&dummy, current) {
          self.mutate_recursive(current, filter_idx + 1, cb);
        }
      }
      _ => {}
    }
  }
}
