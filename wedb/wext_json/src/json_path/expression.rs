//! 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryExpression.cs
//!
//! 查询表达式与值比较谓词:操作符枚举、操作数与表达式求值。

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use regex::Regex;
use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};

use super::filter::{PathFilter, evaluate_filters};

pub fn val_from_f64(f: f64) -> Value {
  sonic_rs::to_value(&f).unwrap_or_else(|_| Value::from(()))
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

/// 由 pattern 与 flags 编译 `Regex`：flags 承接 C# 局部函数 GetRegexOptions
/// （在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryExpression.cs:GetRegexOptions，
/// .NET RegexOptions → (?i)/(?m)/(?s) 内联标志）参与构造最终 pattern。
///
/// 字面正则谓词在路径解析期调用一次并预编译存入 AST，消除逐元素求值路径上的重复编译；
/// 求值期只做 `is_match`，匹配结果与旧实现逐字节一致。
///
/// 在 garnet 中的相对路径:C# 侧的 RegexEquals
pub(crate) fn compile_regex(pattern: &str, flags: &str) -> Result<Regex, regex::Error> {
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
  // 局部测试钩子：固化“同一字面正则谓词只编译一次”，非生产逻辑（见下方单测说明）。
  #[cfg(test)]
  REGEX_COMPILES.fetch_add(1, Ordering::Relaxed);
  Regex::new(&regex_builder)
}

/// 仅测试可见的字面正则编译计数（`cfg(test)` 局部钩子，非生产开关）。
#[cfg(test)]
pub(crate) static REGEX_COMPILES: AtomicUsize = AtomicUsize::new(0);

/// 查询表达式操作数
#[derive(Debug, Clone)]
pub enum QueryOperand {
  Path(Vec<PathFilter>),
  Literal(Value),
  /// 解析期预编译的正则（pattern+flags 已在构造点编译为 `Regex`）。
  Regex(Regex),
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
      QueryOperand::Regex(re) => self.match_tokens(left, None, op, Some(re)),
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryExpression.cs:MatchTokens
  ///
  /// C# 入口先过容器门（QueryExpression.cs:210）：`leftResult is JsonValue or null &&
  /// rightResult is JsonValue or null` 才进比较族 switch；任一侧为数组/对象即落 else 臂
  /// （:242-247），该臂只对 Exists 与 NotEquals 回 true，其余运算符（含 StrictNotEquals
  /// 与 In）一律 false —— C# 因此对容器值不做深比较，`in` 的右值恒为数组故在 C# 侧
  /// 永不命中（CheckIn 不可达）。两处以 C# 代码为准，与票面枚举的差异已登记
  /// doc/zh/deviations.md 与工单执行注记。
  fn match_tokens(
    &self,
    left: Option<&Value>,
    right: Option<&Value>,
    op: QueryOperator,
    regex_info: Option<&Regex>,
  ) -> bool {
    let is_container = |v: &Value| v.is_array() || v.is_object();
    let container_side = left.is_some_and(is_container) || right.is_some_and(is_container);
    if container_side {
      // C# else 臂：switch 仅列 Exists / NotEquals 两个真臂，末尾 return false
      return matches!(op, QueryOperator::Exists | QueryOperator::NotEquals);
    }

    match op {
      QueryOperator::RegexEquals => {
        if let Some(re) = regex_info {
          Self::regex_match(re, left)
        } else if let Some(r) = right {
          if let Some(r_str) = r.as_str() {
            // 右操作数为运行期才定的字符串（路径求值/字面串），无法解析期预编译，
            // 只能按需编译；非法 pattern 视为不匹配，与既有失败语义一致。
            match compile_regex(r_str, "") {
              Ok(re) => Self::regex_match(&re, left),
              Err(_) => false,
            }
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

    // 对标 C# QueryExpression.cs:CompareTo 的 GetValueKind 同型优先分支：
    // 双字符串严格字典序（Ordinal），"100" < "20"；先做数值提升会把
    // 可解析为数字的字符串对错误地按数值比较，颠倒字符串比较契约。
    if let (Some(ls), Some(rs)) = (l.as_str(), r.as_str()) {
      return ls.cmp(rs) as i32;
    }
    // 双布尔：同值回 0（C# 同型 True/False kind 直接回 0），异值 false < true
    if let (Some(lb), Some(rb)) = (l.as_bool(), r.as_bool()) {
      return lb.cmp(&rb) as i32;
    }
    // 双数值：整数对走精确整数比较，避免大整数经 f64 回转丢精度；
    // 含浮点时回落 f64 比较（对位 C# long/double 混比分支）
    if l.is_number() && r.is_number() {
      if let (Some(li), Some(ri)) = (l.as_i64(), r.as_i64()) {
        return li.cmp(&ri) as i32;
      }
      if let (Some(lu), Some(ru)) = (l.as_u64(), r.as_u64()) {
        return lu.cmp(&ru) as i32;
      }
      // 数值节点经 try_get_as_double 必得 Some（i64/u64/f64 全覆盖），
      // 混型整数对（负 i64 vs 大 u64）在此走 f64 比较
      let ln = Self::try_get_as_double(l).unwrap_or(0.0);
      let rn = Self::try_get_as_double(r).unwrap_or(0.0);
      return if ln < rn {
        -1
      } else if ln > rn {
        1
      } else {
        0
      };
    }
    // 异型方做数值提升（C# TryGetAsDouble 接受数字字符串）；仍不可比时
    // 回字符串字典序（非字符串侧取其 JSON 文本，对位 C# ToJsonString 兜底）
    if let (Some(ln), Some(rn)) = (Self::try_get_as_double(l), Self::try_get_as_double(r)) {
      return if ln < rn {
        -1
      } else if ln > rn {
        1
      } else {
        0
      };
    }
    let ls = l
      .as_str()
      .map(str::to_string)
      .unwrap_or_else(|| l.to_string());
    let rs = r
      .as_str()
      .map(str::to_string)
      .unwrap_or_else(|| r.to_string());
    ls.cmp(&rs) as i32
  }

  /// 对预编译正则执行匹配（承接 C# RegexEquals 中 `Regex.IsMatch` 求值面）：
  /// 左值为非字符串或缺值时不匹配。
  ///
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/QueryExpression.cs:RegexEquals
  fn regex_match(re: &Regex, left: Option<&Value>) -> bool {
    let Some(l) = left else { return false };
    let Some(text) = l.as_str() else { return false };
    re.is_match(text)
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
      // C# 该分支为 leftNum.Equals(rightNum)：f64 逐位精确比较，不容差
      // （旧实现的 EPSILON 容差会把 0.1+0.2 与 0.3 判等，与 C# 分叉）
      return ln == rn;
    }

    // 布尔↔字符串对等转换（C# EqualsWithStringCoercion 的 bool.TryParse 分支，
    // 如 active == 'true'）：一侧布尔、另一侧可解析为布尔文本时按布尔值比对
    if let (Some(lb), Some(rs)) = (l.as_bool(), r.as_str()) {
      return rs.parse::<bool>() == Ok(lb);
    }
    if let (Some(rb), Some(ls)) = (r.as_bool(), l.as_str()) {
      return ls.parse::<bool>() == Ok(rb);
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

#[cfg(test)]
mod tests {
  use std::sync::atomic::Ordering;

  use sonic_rs::Value;

  use super::REGEX_COMPILES;
  use crate::json_path::JsonPath;

  /// 固化：字面正则谓词扫描多元素数组时，正则只在解析期编译一次，求值期零编译。
  ///
  /// `REGEX_COMPILES` 是 `compile_regex` 内的 `cfg(test)` 局部钩子（非生产开关）；
  /// 采用前后差值断言，避免与 crate 内其它正则编译（若有）相互干扰。
  #[test]
  fn regex_predicate_compiles_once_across_elements() {
    let json = r#"{"items":[{"name":"A1"},{"name":"AB"},{"name":"B"},{"name":"AC"}]}"#;
    let val: Value = sonic_rs::from_str(json).unwrap();

    let before = REGEX_COMPILES.load(Ordering::Relaxed);
    let path = JsonPath::parse("$.items[?(@.name =~ /^A/)]").unwrap();
    let compiled_at_parse = REGEX_COMPILES.load(Ordering::Relaxed) - before;
    // 解析期恰好编译一次。
    assert_eq!(compiled_at_parse, 1);

    let matches = path.evaluate(&val);
    // 扫描 4 个元素求值后，编译计数不再增长（求值期零编译）。
    let after = REGEX_COMPILES.load(Ordering::Relaxed);
    assert_eq!(after - before, compiled_at_parse);
    // 结果与旧实现逐字节一致：匹配 A1、AB、AC 共 3 项。
    assert_eq!(matches.len(), 3);
  }
}
