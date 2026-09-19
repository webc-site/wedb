//! 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs
//!
//! JSONPath 表达式的递归下降解析器与 token 化。

use sonic_rs::Value;

use super::{
  expression::{QueryExpression, QueryOperand, QueryOperator, val_from_f64, val_from_vec},
  filter::PathFilter,
};
use crate::error::{Error, Result};

/// JSONPath 解析器状态机
///
/// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs
pub(super) struct JsonPathParser<'a> {
  expression: &'a str,
  bytes: &'a [u8],
  pos: usize,
}

impl<'a> JsonPathParser<'a> {
  pub(super) fn new(expression: &'a str) -> Self {
    Self {
      expression,
      bytes: expression.as_bytes(),
      pos: 0,
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:EatWhitespace
  fn eat_whitespace(&mut self) {
    while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_whitespace() {
      self.pos += 1;
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:EnsureLength
  fn ensure_length(&self, len: usize) -> Result<()> {
    if self.pos + len > self.bytes.len() {
      Err(Error::InvalidPath("Unexpected end of path".to_string()))
    } else {
      Ok(())
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:CreateUnexpectedCharacterException
  fn unexpected_char(&self) -> Error {
    let c = if self.pos < self.bytes.len() {
      self.bytes[self.pos] as char
    } else {
      '\0'
    };
    Error::InvalidPath(format!("Unexpected character: {c} at pos {}", self.pos))
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:ParseMain
  pub(super) fn parse_main(&mut self) -> Result<Vec<PathFilter>> {
    let mut filters = Vec::new();
    self.eat_whitespace();

    if self.pos >= self.bytes.len() {
      return Ok(filters);
    }

    if self.bytes[self.pos] == b'$' {
      filters.push(PathFilter::Root);
      self.pos += 1;
      if self.pos >= self.bytes.len() {
        return Ok(filters);
      }
      if self.bytes[self.pos] == b'.' || self.bytes[self.pos] == b'[' {
        // Continue
      } else {
        // Treat as field starting with $
        self.pos -= 1;
        filters.pop();
      }
    }

    self.parse_path(&mut filters)?;
    self.eat_whitespace();
    if self.pos < self.bytes.len() {
      return Err(self.unexpected_char());
    }

    Ok(filters)
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:ParsePath
  fn parse_path(&mut self, filters: &mut Vec<PathFilter>) -> Result<()> {
    while self.pos < self.bytes.len() {
      // 对标 C# ParsePath 的 case ' '：空白即终止路径段；尾随空白交由
      // ParseMain 消化，段间残留非空白字符则报 Unexpected character
      let b = self.bytes[self.pos];
      if b.is_ascii_whitespace() {
        break;
      }
      match b {
        b'.' => {
          self.pos += 1;
          if self.pos < self.bytes.len() && self.bytes[self.pos] == b'.' {
            // Recursive scan ..
            self.pos += 1;
            if self.pos < self.bytes.len() && self.bytes[self.pos] == b'[' {
              let idx_filter = self.parse_indexer(true)?;
              filters.push(idx_filter);
            } else if self.pos < self.bytes.len() && self.bytes[self.pos] == b'*' {
              self.pos += 1;
              filters.push(PathFilter::Scan { name: None });
            } else {
              let name = self.parse_member_name()?;
              filters.push(Self::create_path_filter(Some(name), true));
            }
          } else if self.pos < self.bytes.len() && self.bytes[self.pos] == b'*' {
            self.pos += 1;
            filters.push(Self::create_path_filter(None, false));
          } else if self.pos < self.bytes.len() && self.bytes[self.pos] == b'[' {
            // .[0]
            let idx_filter = self.parse_indexer(false)?;
            filters.push(idx_filter);
          } else {
            let name = self.parse_member_name()?;
            filters.push(Self::create_path_filter(Some(name), false));
          }
        }
        b'[' => {
          let idx_filter = self.parse_indexer(false)?;
          filters.push(idx_filter);
        }
        _ => {
          // Plain property name without leading dot, e.g. "f1"
          let name = self.parse_member_name()?;
          filters.push(Self::create_path_filter(Some(name), false));
        }
      }
    }
    Ok(())
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:CreatePathFilter
  pub fn create_path_filter(member: Option<String>, scan: bool) -> PathFilter {
    if scan {
      PathFilter::Scan { name: member }
    } else {
      PathFilter::Field { name: member }
    }
  }

  fn parse_member_name(&mut self) -> Result<String> {
    let start = self.pos;
    while self.pos < self.bytes.len() {
      let b = self.bytes[self.pos];
      if b == b'.'
        || b == b'['
        || b == b']'
        || b == b'('
        || b == b')'
        || b == b'='
        || b == b'!'
        || b == b'<'
        || b == b'>'
        || b == b'&'
        || b == b'|'
        || b.is_ascii_whitespace()
      {
        break;
      }
      self.pos += 1;
    }
    if self.pos == start {
      return Err(self.unexpected_char());
    }
    Ok(self.expression[start..self.pos].to_string())
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:ParseIndexer
  fn parse_indexer(&mut self, scan: bool) -> Result<PathFilter> {
    self.pos += 1; // skip '['
    self.eat_whitespace();

    if self.pos >= self.bytes.len() {
      return Err(self.unexpected_char());
    }

    if self.bytes[self.pos] == b'?' {
      let query = self.parse_query()?;
      self.eat_whitespace();
      if self.pos >= self.bytes.len() || self.bytes[self.pos] != b']' {
        return Err(self.unexpected_char());
      }
      self.pos += 1;
      return Ok(if scan {
        PathFilter::QueryScan { expression: query }
      } else {
        PathFilter::Query { expression: query }
      });
    }

    if self.bytes[self.pos] == b'*' {
      self.pos += 1;
      self.eat_whitespace();
      if self.pos < self.bytes.len() && self.bytes[self.pos] == b']' {
        self.pos += 1;
        return Ok(if scan {
          PathFilter::ScanArrayIndex { index: None }
        } else {
          PathFilter::ArrayIndex { index: None }
        });
      }
    }

    if self.bytes[self.pos] == b'\'' || self.bytes[self.pos] == b'"' {
      return self.parse_quoted_fields(scan);
    }

    // Array index, multiple index, or slice
    self.parse_array_indexer(scan)
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:ParseQuotedField
  fn parse_quoted_fields(&mut self, scan: bool) -> Result<PathFilter> {
    let mut names = Vec::new();
    while self.pos < self.bytes.len() {
      let quote = self.bytes[self.pos];
      if quote != b'\'' && quote != b'"' {
        return Err(self.unexpected_char());
      }
      let s = self.read_quoted_string()?;
      names.push(s);

      self.eat_whitespace();
      if self.pos < self.bytes.len() && self.bytes[self.pos] == b',' {
        self.pos += 1;
        self.eat_whitespace();
      } else {
        break;
      }
    }

    if self.pos >= self.bytes.len() || self.bytes[self.pos] != b']' {
      return Err(self.unexpected_char());
    }
    self.pos += 1;

    if names.len() == 1 {
      let name = names.pop();
      Ok(if scan {
        PathFilter::Scan { name }
      } else {
        PathFilter::Field { name }
      })
    } else {
      Ok(if scan {
        PathFilter::ScanMultiple { names }
      } else {
        PathFilter::FieldMultiple { names }
      })
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:ParseArrayIndexer
  fn parse_array_indexer(&mut self, scan: bool) -> Result<PathFilter> {
    let mut numbers = Vec::new();
    let mut is_slice = false;
    let mut slice_parts: [Option<i64>; 3] = [None, None, None];
    let mut slice_idx = 0;

    while self.pos < self.bytes.len() && self.bytes[self.pos] != b']' {
      self.eat_whitespace();
      if self.pos >= self.bytes.len() {
        break;
      }

      if self.bytes[self.pos] == b':' {
        is_slice = true;
        self.pos += 1;
        slice_idx += 1;
        continue;
      }

      let num = self.try_parse_index()?;
      if let Some(n) = num {
        if is_slice {
          if slice_idx < 3 {
            slice_parts[slice_idx] = Some(n);
          }
        } else {
          numbers.push(n);
        }
      } else if self.pos < self.bytes.len()
        && self.bytes[self.pos] != b':'
        && self.bytes[self.pos] != b']'
        && self.bytes[self.pos] != b','
      {
        return Err(self.unexpected_char());
      }

      self.eat_whitespace();
      if self.pos < self.bytes.len() && self.bytes[self.pos] == b',' {
        self.pos += 1;
      } else if self.pos < self.bytes.len() && self.bytes[self.pos] == b':' {
        is_slice = true;
        if !numbers.is_empty() && slice_idx == 0 {
          slice_parts[0] = numbers.pop();
        }
        self.pos += 1;
        slice_idx += 1;
      }
    }

    if self.pos >= self.bytes.len() || self.bytes[self.pos] != b']' {
      return Err(self.unexpected_char());
    }
    self.pos += 1;

    if is_slice {
      let (start, end, step) = (slice_parts[0], slice_parts[1], slice_parts[2]);
      Ok(if scan {
        PathFilter::ScanArraySlice { start, end, step }
      } else {
        PathFilter::ArraySlice { start, end, step }
      })
    } else if numbers.len() == 1 {
      let index = Some(numbers[0]);
      Ok(if scan {
        PathFilter::ScanArrayIndex { index }
      } else {
        PathFilter::ArrayIndex { index }
      })
    } else if !numbers.is_empty() {
      Ok(if scan {
        PathFilter::ScanArrayMultipleIndex { indices: numbers }
      } else {
        PathFilter::ArrayMultipleIndex { indices: numbers }
      })
    } else {
      Err(Error::InvalidPath("Array index expected.".to_string()))
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:TryParseIndex
  fn try_parse_index(&mut self) -> Result<Option<i64>> {
    self.eat_whitespace();
    let start = self.pos;
    if self.pos < self.bytes.len() && (self.bytes[self.pos] == b'-' || self.bytes[self.pos] == b'+')
    {
      self.pos += 1;
    }
    while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_digit() {
      self.pos += 1;
    }
    if self.pos == start
      || (self.pos == start + 1 && (self.bytes[start] == b'-' || self.bytes[start] == b'+'))
    {
      self.pos = start;
      return Ok(None);
    }
    let s = &self.expression[start..self.pos];
    s.parse::<i64>()
      .map(Some)
      .map_err(|_| Error::InvalidPath(format!("Invalid integer: {s}")))
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:ReadQuotedString
  fn read_quoted_string(&mut self) -> Result<String> {
    let quote = self.bytes[self.pos];
    self.pos += 1; // skip quote
    let mut res = String::new();

    while self.pos < self.bytes.len() {
      let b = self.bytes[self.pos];
      if b == quote {
        self.pos += 1;
        return Ok(res);
      } else if b == b'\\' {
        self.pos += 1;
        if self.pos >= self.bytes.len() {
          return Err(self.unexpected_char());
        }
        let escaped = self.bytes[self.pos];
        match escaped {
          b'n' => res.push('\n'),
          b'r' => res.push('\r'),
          b't' => res.push('\t'),
          b'\\' => res.push('\\'),
          b'\'' => res.push('\''),
          b'"' => res.push('"'),
          b'/' => res.push('/'),
          b'u' => {
            self.pos += 1;
            let hex = self.try_parse_escaped_codepoint()?;
            if let Some(c) = char::from_u32(hex) {
              res.push(c);
            }
            continue;
          }
          _ => res.push(escaped as char),
        }
        self.pos += 1;
      } else {
        res.push(b as char);
        self.pos += 1;
      }
    }
    Err(self.unexpected_char())
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:TryParseEscapedCodepoint
  fn try_parse_escaped_codepoint(&mut self) -> Result<u32> {
    self.ensure_length(4)?;
    let s = &self.expression[self.pos..self.pos + 4];
    for c in s.chars() {
      if !Self::is_valid_hex(c) {
        return Err(Error::InvalidPath(format!("Invalid unicode escape: {s}")));
      }
    }
    self.pos += 4;
    u32::from_str_radix(s, 16)
      .map_err(|_| Error::InvalidPath(format!("Invalid unicode escape: {s}")))
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:IsValidHex
  pub fn is_valid_hex(c: char) -> bool {
    c.is_ascii_hexdigit()
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:ReadRegexString
  fn read_regex_string(&mut self) -> Result<(String, String)> {
    self.pos += 1; // skip '/'
    let mut pat = String::new();
    while self.pos < self.bytes.len() {
      let b = self.bytes[self.pos];
      if b == b'/' {
        self.pos += 1;
        let mut flags = String::new();
        while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_alphabetic() {
          flags.push(self.bytes[self.pos] as char);
          self.pos += 1;
        }
        return Ok((pat, flags));
      } else if b == b'\\' {
        self.pos += 1;
        if self.pos < self.bytes.len() {
          pat.push('\\');
          pat.push(self.bytes[self.pos] as char);
          self.pos += 1;
        }
      } else {
        pat.push(b as char);
        self.pos += 1;
      }
    }
    Err(self.unexpected_char())
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:ParseQuery
  fn parse_query(&mut self) -> Result<QueryExpression> {
    self.pos += 1; // skip '?'
    self.eat_whitespace();
    if self.pos < self.bytes.len() && self.bytes[self.pos] == b'(' {
      self.pos += 1;
      let expr = self
        .try_parse_expression()?
        .ok_or_else(|| self.unexpected_char())?;
      self.eat_whitespace();
      if self.pos < self.bytes.len() && self.bytes[self.pos] == b')' {
        self.pos += 1;
      }
      Ok(expr)
    } else {
      self
        .try_parse_expression()?
        .ok_or_else(|| self.unexpected_char())
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:ParseExpression
  fn parse_expression(&mut self) -> Result<QueryExpression> {
    let mut left = self.parse_sub_expression()?;
    self.eat_whitespace();

    while self.pos < self.bytes.len() {
      let op = if self.match_token(b"&&") {
        QueryOperator::And
      } else if self.match_token(b"||") {
        QueryOperator::Or
      } else {
        break;
      };

      let right = self.parse_sub_expression()?;
      left = QueryExpression::Composite {
        op,
        expressions: vec![left, right],
      };
      self.eat_whitespace();
    }

    Ok(left)
  }

  fn parse_sub_expression(&mut self) -> Result<QueryExpression> {
    self.eat_whitespace();
    if self.pos < self.bytes.len() && self.bytes[self.pos] == b'!' {
      self.pos += 1;
      let expr = self.parse_sub_expression()?;
      return Ok(QueryExpression::Composite {
        op: QueryOperator::Not,
        expressions: vec![expr],
      });
    }

    if self.pos < self.bytes.len() && self.bytes[self.pos] == b'(' {
      self.pos += 1;
      let expr = self.parse_expression()?;
      self.eat_whitespace();
      if self.pos < self.bytes.len() && self.bytes[self.pos] == b')' {
        self.pos += 1;
      }
      return Ok(expr);
    }

    let left = self.parse_side()?;
    self.eat_whitespace();

    let op = self.parse_operator()?;
    if op == QueryOperator::None {
      // Exists query
      return Ok(QueryExpression::Boolean {
        op: QueryOperator::Exists,
        left: Box::new(left),
        right: Box::new(QueryOperand::Null),
      });
    }

    self.eat_whitespace();
    let right = self.parse_side()?;

    Ok(QueryExpression::Boolean {
      op,
      left: Box::new(left),
      right: Box::new(right),
    })
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:ParseSide
  fn parse_side(&mut self) -> Result<QueryOperand> {
    self.eat_whitespace();
    if self.pos >= self.bytes.len() {
      return Err(self.unexpected_char());
    }

    if self.bytes[self.pos] == b'@' || self.bytes[self.pos] == b'$' {
      let is_root = self.bytes[self.pos] == b'$';
      self.pos += 1;
      let mut filters = Vec::new();
      if is_root {
        filters.push(PathFilter::Root);
      }
      self.parse_path_until_op(&mut filters)?;
      return Ok(QueryOperand::Path(filters));
    }

    if self.bytes[self.pos] == b'/' {
      let (pat, flags) = self.read_regex_string()?;
      return Ok(QueryOperand::Regex(pat, flags));
    }

    if self.bytes[self.pos] == b'\'' || self.bytes[self.pos] == b'"' {
      let s = self.read_quoted_string()?;
      return Ok(QueryOperand::Literal(Value::from(s.as_str())));
    }

    if self.bytes[self.pos] == b'[' {
      return self.try_parse_array_literal();
    }

    if let Some(val) = self.try_parse_value()? {
      return Ok(QueryOperand::Literal(val));
    }

    Err(self.unexpected_char())
  }

  fn parse_path_until_op(&mut self, filters: &mut Vec<PathFilter>) -> Result<()> {
    while self.pos < self.bytes.len() {
      self.eat_whitespace();
      if self.pos >= self.bytes.len() {
        break;
      }
      let b = self.bytes[self.pos];
      if b == b'='
        || b == b'!'
        || b == b'<'
        || b == b'>'
        || b == b'&'
        || b == b'|'
        || b == b')'
        || b == b']'
      {
        break;
      }
      if self.match_token(b"in ") || self.match_token(b"nin ") {
        self.pos -= 3;
        break;
      }

      if b == b'.' {
        self.pos += 1;
        if self.pos < self.bytes.len() && self.bytes[self.pos] == b'.' {
          self.pos += 1;
          let name = self.parse_member_name()?;
          filters.push(PathFilter::Scan { name: Some(name) });
        } else if self.pos < self.bytes.len() && self.bytes[self.pos] == b'*' {
          self.pos += 1;
          filters.push(PathFilter::Field { name: None });
        } else if self.pos < self.bytes.len() && self.bytes[self.pos] == b'[' {
          let idx = self.parse_indexer(false)?;
          filters.push(idx);
        } else {
          let name = self.parse_member_name()?;
          filters.push(PathFilter::Field { name: Some(name) });
        }
      } else if b == b'[' {
        let idx = self.parse_indexer(false)?;
        filters.push(idx);
      } else {
        let name = self.parse_member_name()?;
        filters.push(PathFilter::Field { name: Some(name) });
      }
    }
    Ok(())
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:TryParseArrayLiteral
  fn try_parse_array_literal(&mut self) -> Result<QueryOperand> {
    self.pos += 1; // skip '['
    let mut elements = Vec::new();
    while self.pos < self.bytes.len() {
      self.eat_whitespace();
      if self.pos < self.bytes.len() && self.bytes[self.pos] == b']' {
        self.pos += 1;
        break;
      }
      if let Ok(QueryOperand::Literal(v)) = self.parse_side() {
        elements.push(v);
      }
      self.eat_whitespace();
      if self.pos < self.bytes.len() && self.bytes[self.pos] == b',' {
        self.pos += 1;
      }
    }
    Ok(QueryOperand::Literal(val_from_vec(elements)))
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:TryParseValue
  fn try_parse_value(&mut self) -> Result<Option<Value>> {
    self.eat_whitespace();
    if self.match_token(b"null") {
      return Ok(Some(Value::from(())));
    }
    if self.match_token(b"true") {
      return Ok(Some(Value::from(true)));
    }
    if self.match_token(b"false") {
      return Ok(Some(Value::from(false)));
    }

    let start = self.pos;
    if self.pos < self.bytes.len() && (self.bytes[self.pos] == b'-' || self.bytes[self.pos] == b'+')
    {
      self.pos += 1;
    }
    let mut has_dot = false;
    while self.pos < self.bytes.len() {
      let b = self.bytes[self.pos];
      if b.is_ascii_digit() {
        self.pos += 1;
      } else if b == b'.' && !has_dot {
        has_dot = true;
        self.pos += 1;
      } else if b == b'e' || b == b'E' {
        self.pos += 1;
        if self.pos < self.bytes.len()
          && (self.bytes[self.pos] == b'-' || self.bytes[self.pos] == b'+')
        {
          self.pos += 1;
        }
      } else {
        break;
      }
    }

    if self.pos == start {
      return Ok(None);
    }

    let s = &self.expression[start..self.pos];
    if has_dot || s.contains('e') || s.contains('E') {
      if let Ok(f) = s.parse::<f64>() {
        return Ok(Some(val_from_f64(f)));
      }
    } else if let Ok(i) = s.parse::<i64>() {
      return Ok(Some(Value::from(i)));
    }

    self.pos = start;
    Ok(None)
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:ParseOperator
  fn parse_operator(&mut self) -> Result<QueryOperator> {
    self.eat_whitespace();
    if self.match_token(b"===") {
      Ok(QueryOperator::StrictEquals)
    } else if self.match_token(b"==") {
      Ok(QueryOperator::Equals)
    } else if self.match_token(b"!==") {
      Ok(QueryOperator::StrictNotEquals)
    } else if self.match_token(b"!=") {
      Ok(QueryOperator::NotEquals)
    } else if self.match_token(b"=~") {
      Ok(QueryOperator::RegexEquals)
    } else if self.match_token(b"<=") {
      Ok(QueryOperator::LessThanOrEquals)
    } else if self.match_token(b">=") {
      Ok(QueryOperator::GreaterThanOrEquals)
    } else if self.match_token(b"<") {
      Ok(QueryOperator::LessThan)
    } else if self.match_token(b">") {
      Ok(QueryOperator::GreaterThan)
    } else if self.match_token(b"in ") {
      Ok(QueryOperator::In)
    } else {
      Ok(QueryOperator::None)
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:Match
  fn match_token(&mut self, token: &[u8]) -> bool {
    if self.pos + token.len() <= self.bytes.len()
      && &self.bytes[self.pos..self.pos + token.len()] == token
    {
      self.pos += token.len();
      true
    } else {
      false
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:TryParseExpression
  pub fn try_parse_expression(&mut self) -> Result<Option<QueryExpression>> {
    self.parse_expression().map(Some)
  }
}
