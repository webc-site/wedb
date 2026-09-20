//! 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryExpression.cs
//!
//! 查询表达式与值比较谓词:操作符枚举、操作数与表达式求值。

use regex::Regex;
use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};

use super::filter::{PathFilter, evaluate_filters};

pub fn val_from_f64(f: f64) -> Value {
  sonic_rs::to_value(&f).unwrap_or_else(|_| Value::from(()))
}

pub fn val_from_vec(v: Vec<Value>) -> Value {
  sonic_rs::to_value(&v).unwrap_or_else(|_| Value::from(()))
}

/// 查询操作符
///
/// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryExpression.cs:QueryOperator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryOperator {
  None = 0,
  Equals = 1,
  NotEquals = 2,
  Exists = 3,
  LessThan = 4,
  LessThanOrEquals = 5,
  GreaterThan = 6,
  GreaterThanOrEquals = 7,
  And = 8,
  Or = 9,
  RegexEquals = 10,
  StrictEquals = 11,
  StrictNotEquals = 12,
  Not = 13,
  In = 14,
}

/// 查询表达式操作数
#[derive(Debug, Clone)]
pub enum QueryOperand {
  Path(Vec<PathFilter>),
  Literal(Value),
  Regex(String, String), // pattern, flags
  Null,
}

/// 查询表达式（Rust 枚举合并承接 C# 多表达式类，逐臂标注对应 C# 文件:类）
///
/// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryExpression.cs:QueryExpression
#[derive(Debug, Clone)]
pub enum QueryExpression {
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryExpression.cs:BooleanQueryExpression
  Boolean {
    op: QueryOperator,
    left: Box<QueryOperand>,
    right: Box<QueryOperand>,
  },
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryExpression.cs:CompositeExpression
  Composite {
    op: QueryOperator, // And, Or, Not
    expressions: Vec<QueryExpression>,
  },
}

impl QueryExpression {
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryExpression.cs:IsMatch
  pub fn is_match(&self, root: &Value, current: &Value) -> bool {
    match self {
      Self::Composite { op, expressions } => match op {
        QueryOperator::And => expressions.iter().all(|e| e.is_match(root, current)),
        QueryOperator::Or => expressions.iter().any(|e| e.is_match(root, current)),
        QueryOperator::Not => expressions
          .first()
          .is_none_or(|e| !e.is_match(root, current)),
        _ => false,
      },
      Self::Boolean { op, left, right } => {
        if *op == QueryOperator::Exists {
          return match left.as_ref() {
            QueryOperand::Path(filters) => !evaluate_filters(filters, root, current).is_empty(),
            QueryOperand::Literal(_) => true,
            _ => false,
          };
        }

        let left_vals = self.eval_operand(left, root, current);
        if left_vals.is_empty() {
          return false;
        }

        for l in &left_vals {
          if self.evaluate_match(root, current, *op, *l, right) {
            return true;
          }
        }
        false
      }
    }
  }

  fn eval_operand<'a>(
    &self,
    operand: &'a QueryOperand,
    root: &'a Value,
    current: &'a Value,
  ) -> Vec<Option<&'a Value>> {
    match operand {
      QueryOperand::Path(filters) => evaluate_filters(filters, root, current)
        .into_iter()
        .map(Some)
        .collect(),
      QueryOperand::Literal(v) => vec![Some(v)],
      QueryOperand::Null => vec![None],
      QueryOperand::Regex(..) => vec![None],
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryExpression.cs:EvaluateMatch
  fn evaluate_match(
    &self,
    root: &Value,
    current: &Value,
    op: QueryOperator,
    left: Option<&Value>,
    right_operand: &QueryOperand,
  ) -> bool {
    match right_operand {
      QueryOperand::Path(filters) => {
        let rights = evaluate_filters(filters, root, current);
        for r in rights {
          if self.match_tokens(left, Some(r), op, None) {
            return true;
          }
        }
        false
      }
      QueryOperand::Literal(r) => self.match_tokens(left, Some(r), op, None),
      QueryOperand::Null => self.match_tokens(left, None, op, None),
      QueryOperand::Regex(pat, flags) => self.match_tokens(left, None, op, Some((pat, flags))),
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryExpression.cs:MatchTokens
  fn match_tokens(
    &self,
    left: Option<&Value>,
    right: Option<&Value>,
    op: QueryOperator,
    regex_info: Option<(&str, &str)>,
  ) -> bool {
    match op {
      QueryOperator::RegexEquals => {
        if let Some((pat, flags)) = regex_info {
          Self::regex_equals_str(left, pat, flags)
        } else if let Some(r) = right {
          if let Some(r_str) = r.as_str() {
            Self::regex_equals_str(left, r_str, "")
          } else {
            false
          }
        } else {
          false
        }
      }
      QueryOperator::Equals => Self::equals_with_string_coercion(left, right),
      QueryOperator::StrictEquals => Self::equals_with_strict_match(left, right),
      QueryOperator::NotEquals => !Self::equals_with_string_coercion(left, right),
      QueryOperator::StrictNotEquals => !Self::equals_with_strict_match(left, right),
      QueryOperator::GreaterThan => Self::compare_to(left, right) > 0,
      QueryOperator::GreaterThanOrEquals => Self::compare_to(left, right) >= 0,
      QueryOperator::LessThan => Self::compare_to(left, right) < 0,
      QueryOperator::LessThanOrEquals => Self::compare_to(left, right) <= 0,
      QueryOperator::Exists => left.is_some(),
      QueryOperator::In => Self::check_in(left, right),
      _ => false,
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryExpression.cs:CheckIn
  pub fn check_in(left: Option<&Value>, right: Option<&Value>) -> bool {
    let Some(r) = right else { return false };
    let Some(arr) = r.as_array() else {
      return false;
    };
    match left {
      None => arr.iter().any(|x| x.is_null()),
      Some(l) => arr.iter().any(|x| x == l),
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryExpression.cs:CompareTo
  pub fn compare_to(left: Option<&Value>, right: Option<&Value>) -> i32 {
    let (Some(l), Some(r)) = (left, right) else {
      return match (left, right) {
        (None, None) => 0,
        (None, Some(_)) => -1,
        (Some(_), None) => 1,
        (Some(_), Some(_)) => 0,
      };
    };

    if let (Some(ln), Some(rn)) = (Self::try_get_as_double(l), Self::try_get_as_double(r)) {
      if ln < rn {
        -1
      } else if ln > rn {
        1
      } else {
        0
      }
    } else if let (Some(ls), Some(rs)) = (l.as_str(), r.as_str()) {
      ls.cmp(rs) as i32
    } else if let (Some(lb), Some(rb)) = (l.as_bool(), r.as_bool()) {
      lb.cmp(&rb) as i32
    } else {
      0
    }
  }

  /// 正则匹配（在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryExpression.cs:RegexEquals）；
  /// flags 转换承接 C# 局部函数 GetRegexOptions
  /// （在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryExpression.cs:GetRegexOptions，
  /// .NET RegexOptions → (?i)/(?m)/(?s) 内联标志）。
  fn regex_equals_str(left: Option<&Value>, pattern: &str, flags: &str) -> bool {
    let Some(l) = left else { return false };
    let Some(text) = l.as_str() else { return false };

    let mut regex_builder = String::new();
    if flags.contains('i') {
      regex_builder.push_str("(?i)");
    }
    if flags.contains('m') {
      regex_builder.push_str("(?m)");
    }
    if flags.contains('s') {
      regex_builder.push_str("(?s)");
    }
    regex_builder.push_str(pattern);

    match Regex::new(&regex_builder) {
      Ok(re) => re.is_match(text),
      Err(_) => false,
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryExpression.cs:EqualsWithStringCoercion
  pub fn equals_with_string_coercion(left: Option<&Value>, right: Option<&Value>) -> bool {
    let (Some(l), Some(r)) = (left, right) else {
      return left.is_none() && right.is_none();
    };

    if l == r {
      return true;
    }

    if let (Some(ln), Some(rn)) = (Self::try_get_as_double(l), Self::try_get_as_double(r)) {
      return (ln - rn).abs() < f64::EPSILON;
    }

    if let (Some(ls), Some(rn)) = (l.as_str(), Self::try_get_as_double(r))
      && let Ok(parsed) = ls.parse::<f64>()
    {
      return (parsed - rn).abs() < f64::EPSILON;
    }
    if let (Some(ln), Some(rs)) = (Self::try_get_as_double(l), r.as_str())
      && let Ok(parsed) = rs.parse::<f64>()
    {
      return (ln - parsed).abs() < f64::EPSILON;
    }

    false
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryExpression.cs:EqualsWithStrictMatch
  pub fn equals_with_strict_match(left: Option<&Value>, right: Option<&Value>) -> bool {
    match (left, right) {
      (None, None) => true,
      (Some(l), Some(r)) => l == r,
      _ => false,
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryExpression.cs:IsBoolean
  pub fn is_boolean(val: &Value) -> bool {
    val.is_boolean()
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryExpression.cs:TryGetAsDouble
  pub fn try_get_as_double(val: &Value) -> Option<f64> {
    if let Some(i) = val.as_i64() {
      Some(i as f64)
    } else if let Some(u) = val.as_u64() {
      Some(u as f64)
    } else if let Some(f) = val.as_f64() {
      Some(f)
    } else if let Some(s) = val.as_str() {
      s.parse::<f64>().ok()
    } else {
      None
    }
  }
}
