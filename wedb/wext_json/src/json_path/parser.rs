//! 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs
//!
//! JSONPath 表达式的递归下降解析器与 token 化。

use sonic_rs::{JsonValueTrait, Value};

use super::{
  expression::{QueryExpression, QueryOperand, QueryOperator, compile_regex, val_from_f64},
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

/// `\u` 转义十六进制段长度（C# TryParseEscapedCodepoint 的 `var length = 4`）
const HEX_DIGITS: usize = 4;

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
  ///
  /// C# 入口即 `Encoding.UTF8.GetString`（GarnetJsonObject.cs:TryGetToWriter）把路径
  /// 参数转成 char 流后逐 char 拼接，非 ASCII 字段名完整保留；rust 侧对位为：非转义段
  /// 按 expression 原文切片整体收集（切片边界恒落在 char 边界——引号与反斜杠均为
  /// ASCII 字节，不会出现在 UTF-8 多字节序列中间），杜绝逐字节 `as char` 的
  /// Latin-1 mojibake。转义族与 C# switch 逐臂对齐：b/t/n/f/r、\\ " ' / 原样、
  /// u/U 走码点解析（含代理对合并）、未知转义按 default 臂抛错上抛。
  fn read_quoted_string(&mut self) -> Result<String> {
    let quote = self.bytes[self.pos];
    self.pos += 1; // skip quote
    let mut res = String::new();
    let mut seg = self.pos; // 未落账的非转义原文段起点（char 边界）

    while self.pos < self.bytes.len() {
      let b = self.bytes[self.pos];
      if b == quote {
        res.push_str(&self.expression[seg..self.pos]);
        self.pos += 1;
        return Ok(res);
      } else if b == b'\\' {
        res.push_str(&self.expression[seg..self.pos]);
        if self.pos + 1 >= self.bytes.len() {
          return Err(Error::InvalidPath(
            "Path ended with an open string.".to_string(),
          ));
        }
        self.pos += 1;
        let escaped = self.bytes[self.pos];
        self.pos += 1;
        match escaped {
          b'b' => res.push('\u{8}'),
          b't' => res.push('\t'),
          b'n' => res.push('\n'),
          b'f' => res.push('\u{c}'),
          b'r' => res.push('\r'),
          b'\\' | b'"' | b'\'' | b'/' => res.push(escaped as char),
          b'u' | b'U' => self.try_parse_escaped_codepoint(&mut res)?,
          _ => {
            return Err(Error::InvalidPath(format!(
              "Unknown escape character: \\{}",
              escaped as char
            )));
          }
        }
        seg = self.pos;
      } else {
        self.pos += 1;
      }
    }
    Err(Error::InvalidPath(
      "Path ended with an open string.".to_string(),
    ))
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:TryParseEscapedCodepoint
  ///
  /// 1:1 对位 C#：`\u`（或 `\U`）后恰取 4 位十六进制；紧随另一 `\u`+4 位时
  /// 无条件按代理对公式合并（C# 该公式对非高 surrogate 首段同样执行并产错值，
  /// 由码点范围校验兜为抛错）；合并算式用 i32 wrapping 对位 C# unchecked 溢出回绕；
  /// 越界或落在 surrogate 区（0xD800..=0xDFFF）即抛 "Invalid UTF-32 code point"。
  /// 入参前置：self.pos 已越过 `u`/`U` 落在首位十六进制上。
  fn try_parse_escaped_codepoint(&mut self, res: &mut String) -> Result<()> {
    let base = self.pos;
    let hex = self.parse_hex4(base)? as i32;
    let mut length = HEX_DIGITS;
    let mut cp = hex;
    // C# 条件：_currentIndex + 4 + 2 < Length 且紧随的恰是小写 `\u` 二字节
    if base + HEX_DIGITS + 2 < self.bytes.len()
      && self.bytes[base + HEX_DIGITS] == b'\\'
      && self.bytes[base + HEX_DIGITS + 1] == b'u'
    {
      length += 6;
      let hex2 = self.parse_hex4(base + HEX_DIGITS + 2)? as i32;
      cp = (hex - 0xD800)
        .wrapping_mul(0x400)
        .wrapping_add((hex2 - 0xDC00) % 0x400)
        .wrapping_add(0x10000);
    }

    if !(0..=0x10FFFF).contains(&cp) || (0xD800..=0xDFFF).contains(&cp) {
      return Err(Error::InvalidPath(format!(
        "Invalid UTF-32 code point: '{cp}'"
      )));
    }
    // 上面范围校验已排除越界与 surrogate，码点必为合法 char
    res.push(char::from_u32(cp as u32).unwrap());
    self.pos = base + length;
    Ok(())
  }

  /// 读取 `at` 起 4 位十六进制并转 u32（C# IsValidHex + int.TryParse(HexNumber) 两步合一）
  fn parse_hex4(&self, at: usize) -> Result<u32> {
    // C# 边界口径：pos + 4 >= Length 即报非法转义（要求十六进制段后至少留有一字符）
    if at + HEX_DIGITS >= self.bytes.len() {
      return Err(Error::InvalidPath(format!(
        "Invalid escape sequence: '{}'",
        &self.expression[at..]
      )));
    }
    let s = &self.expression[at..at + HEX_DIGITS];
    for c in s.chars() {
      if !Self::is_valid_hex(c) {
        return Err(Error::InvalidPath(format!(
          "Invalid escape sequence: '\\u{s}'"
        )));
      }
    }
    u32::from_str_radix(s, 16)
      .map_err(|_| Error::InvalidPath(format!("Invalid escape sequence: '\\u{s}'")))
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
      let expr = self.parse_expression()?;
      self.eat_whitespace();
      if self.pos < self.bytes.len() && self.bytes[self.pos] == b')' {
        self.pos += 1;
      }
      Ok(expr)
    } else {
      self.parse_expression()
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:ParseExpression
  ///
  /// 逐节点复刻 C# 的 parentExpression 挂链（JsonPath.cs:763-803）：同一运算符的相邻侧
  /// 并入当前链节，运算符一旦切换就新开链节并使其成为前一链节的末位子节点。故
  /// `a && b || c` 与 C# 一致产出 And(a, Or(b, c))、`a || b && c` 产出 Or(a, And(b, c))
  /// —— C# 该链是序敏感挂链而非标准优先级归约，逐形态对拍结论已记入工单执行注记。
  fn parse_expression(&mut self) -> Result<QueryExpression> {
    // 链节序列：(该链节运算符, 归并入本链节的操作数侧)，至多一条、只向深处下挂
    let mut frames: Vec<(QueryOperator, Vec<QueryExpression>)> = Vec::new();
    // 尚未挂入任何链节的收尾侧：仅「单侧无布尔连接符」形态停留于此
    let mut loose: Option<QueryExpression> = None;

    loop {
      // C# 在循环顶部逐字符判 '!' 前缀，不前吞空白
      let is_not = self.bytes.get(self.pos) == Some(&b'!');
      if is_not {
        self.pos += 1;
      }
      let mut side = self.parse_one_side()?;
      if is_not {
        // C# isNot 分支：把本侧包进 Not 复合节点后再参与挂链
        side = QueryExpression::Composite {
          op: QueryOperator::Not,
          expressions: vec![side],
        };
      }

      // C# 侧后按当前字符分流：')' 收尾、&&/|| 续链，其余字符回到循环顶继续解析下一侧
      // （非法起始符由 parse_one_side 抛 Unexpected character）
      let sep = match self.bytes.get(self.pos) {
        Some(b')') => None,
        Some(b'&') => {
          if !self.match_token(b"&&") {
            return Err(self.unexpected_char());
          }
          Some(QueryOperator::And)
        }
        Some(b'|') => {
          if !self.match_token(b"||") {
            return Err(self.unexpected_char());
          }
          Some(QueryOperator::Or)
        }
        None => {
          return Err(Error::InvalidPath(
            "Path ended with open query.".to_string(),
          ));
        }
        _ => continue,
      };

      match sep {
        // ')' 收尾：末侧挂入当前链节（无链节时留作整棵表达式）后由下方装配出树
        None => {
          match frames.last_mut() {
            Some((_, kids)) => kids.push(side),
            None => loose = Some(side),
          }
          break;
        }
        // 与当前运算符相同：并入当前链节（C# parentExpression.Expressions.Add）
        Some(op) if matches!(frames.last(), Some((cop, _)) if *cop == op) => {
          if let Some((_, kids)) = frames.last_mut() {
            kids.push(side);
          }
        }
        // 运算符切换：新建链节（C# 新复合节点挂到父节点子项末尾，parent 下移）
        Some(op) => frames.push((op, vec![side])),
      }
    }

    // 由最内链节向外装配，链节作为父链节末位子节点，与 C# 挂链产出的树逐节点同形
    let mut built = loose;
    for (op, mut kids) in frames.into_iter().rev() {
      kids.extend(built);
      built = Some(QueryExpression::Composite {
        op,
        expressions: kids,
      });
    }
    Ok(built.expect("挂链收尾必有表达式：有链节则装配产出，无链节即单侧留存"))
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:ParseSide
  /// 与 ParseExpression 循环体的运算符判定段（JsonPath.cs:714-743）
  ///
  /// C# 口径：侧后当前字符为 ')' / '|' / '&' 时按 Exists（无右操作数），否则必须由
  /// ParseOperator 解析出比较运算符，一个都不命中即抛 "Could not read query operator."
  /// （`nin` 在 C# 无分支，故按此报错上抛）；分组 `( ... )` 在 Exists 位解包为内层
  /// 表达式（JsonPath.cs:734-739），非 Exists 位按 C# 以单元素 `[QueryFilter]` 路径侧参与比较。
  fn parse_one_side(&mut self) -> Result<QueryExpression> {
    let left = self.parse_operand()?;
    self.after_side()?;
    let op = match self.bytes[self.pos] {
      b')' | b'|' | b'&' => QueryOperator::Exists,
      _ => self.parse_operator()?,
    };

    if op == QueryOperator::Exists {
      if let QueryOperand::Path(filters) = &left
        && filters.len() == 1
        && let PathFilter::Query { expression } = &filters[0]
      {
        return Ok(expression.clone());
      }
      return Ok(QueryExpression::Boolean {
        op,
        left: Box::new(left),
        right: Box::new(QueryOperand::Null),
      });
    }

    let right = self.parse_operand()?;
    self.after_side()?;
    Ok(QueryExpression::Boolean {
      op,
      left: Box::new(left),
      right: Box::new(right),
    })
  }

  /// C# ParseSide 每个成功分支尾部的 `EatWhitespace` + `EnsureLength`：侧后吞掉空白并
  /// 要求表达式仍有字符，否则按 "Path ended with open query." 报错
  fn after_side(&mut self) -> Result<()> {
    self.eat_whitespace();
    if self.pos >= self.bytes.len() {
      return Err(Error::InvalidPath(
        "Path ended with open query.".to_string(),
      ));
    }
    Ok(())
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:ParseSide
  ///
  /// C# 的 TryParseExpression（'$' / '@' / '(' 三入口）→ TryParseValue →
  /// TryParseArrayLiteral 三段尝试顺序；rust 侧按首字节直分流，正则串承接
  /// C# TryParseValue 的 '/' 分支（TryParseValue 内联，不另设值侧函数）。
  fn parse_operand(&mut self) -> Result<QueryOperand> {
    self.eat_whitespace();
    if self.pos >= self.bytes.len() {
      return Err(self.unexpected_char());
    }

    // 分组 ( ... )：C# TryParseExpression 走 ParseQuery(')') 产出单元素
    // [QueryFilter]，故此处同样收敛为 Path(vec![Query]) 操作数，Exists 位由
    // parse_one_side 解包（对位 JsonPath.cs:631-635 与 :734-739）
    if self.bytes[self.pos] == b'(' {
      let expr = self.parse_group()?;
      return Ok(QueryOperand::Path(vec![PathFilter::Query {
        expression: expr,
      }]));
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
      // 解析期预编译：pattern 非法即在构造点作为路径解析错误上抛，不拖到求值期静默。
      let re = compile_regex(&pat, &flags)
        .map_err(|e| Error::InvalidPath(format!("Invalid regex in path: {e}")))?;
      return Ok(QueryOperand::Regex(re));
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

  /// 分组 `( ... )`：对位 C# TryParseExpression 的 '(' 分支（ParseQuery(indexerCloseChar
  /// = ')')）—— ParseExpression 停在 ')' 前，由本函数吞空白后核对并消费闭括号
  fn parse_group(&mut self) -> Result<QueryExpression> {
    self.pos += 1; // skip '('
    if self.pos >= self.bytes.len() {
      return Err(Error::InvalidPath(
        "Path ended with open indexer.".to_string(),
      ));
    }

    let expr = self.parse_expression()?;
    if self.pos >= self.bytes.len() {
      return Err(Error::InvalidPath(
        "Path ended with open indexer.".to_string(),
      ));
    }
    self.eat_whitespace();
    if self.bytes.get(self.pos) != Some(&b')') {
      return Err(self.unexpected_char());
    }
    self.pos += 1;
    Ok(expr)
  }

  fn parse_path_until_op(&mut self, filters: &mut Vec<PathFilter>) -> Result<()> {
    // 是否刚跨过空白：对位 C# ParsePath 遇 ' ' 即终止路径段、运算符仅在段后被
    // ParseOperator 试探；`@in[0]` 这类紧贴 '@' 的 "in" 属成员名，不得误判为运算符
    let mut crossed_ws = false;
    while self.pos < self.bytes.len() {
      let before = self.pos;
      self.eat_whitespace();
      if self.pos > before {
        crossed_ws = true;
      }
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
      // in/nin 词边界探测（不要求尾随空格、不消费，精确停在词首交 parse_operator；
      // C# 无空格要求亲核 JsonPath.cs:ParseOperator 的 Match("in")，nin 无分支必报错）
      if crossed_ws && (self.at_word(b"nin") || self.at_word(b"in")) {
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
      // 路径段消费后回到行首紧贴形态：段间空白由本循环顶部统一消化并置位
      crossed_ws = false;
    }
    Ok(())
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:TryParseArrayLiteral
  ///
  /// 1:1 对位 C#：括号深度扫描（双引号与转义态跟随）定位字面量边界后，整段交
  /// JSON 解析；元素非法（如 `[x]`）即抛错上抛，杜绝逐元素 parse_side 吞 Err
  /// 且 pos 不推进造成的死循环。
  fn try_parse_array_literal(&mut self) -> Result<QueryOperand> {
    let start = self.pos;
    let mut inner = self.pos;
    let mut array_depth = 0i32;
    let mut in_quotes = false;
    let mut is_escaped = false;
    let mut done = false;
    while inner < self.bytes.len() && !done {
      let c = self.bytes[inner];
      inner += 1;
      if in_quotes && c == b'\\' && !is_escaped {
        is_escaped = true;
        continue;
      }
      if c == b'[' && !in_quotes {
        array_depth += 1;
      } else if c == b']' && !in_quotes {
        array_depth -= 1;
        if array_depth == 0 {
          done = true;
          break;
        }
      } else if c == b'"' && !is_escaped {
        in_quotes = !in_quotes;
      }
      is_escaped = false;
    }
    if !done {
      return Err(Error::InvalidPath("Incomplete Array Literal".to_string()));
    }

    self.pos = inner;
    // C# 用 JsonNode.Parse 严格 JSON 解析，非法文本抛 JsonException；
    // rust 侧 sonic_rs 同源严格，错误映射为 InvalidPath 上抛。
    let v: Value = sonic_rs::from_str(&self.expression[start..inner])
      .map_err(|e| Error::InvalidPath(format!("Invalid array literal: {e}")))?;
    if !v.is_array() {
      // 扫描以 '[' 起、深度归零止，产出必为数组；防御性兜底对位 C# AsArray 失败面
      return Err(Error::InvalidPath("Invalid array literal".to_string()));
    }
    Ok(QueryOperand::Literal(v))
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:TryParseValue
  ///
  /// 1:1 对位 C#：字面量首字符门为精确小写（'t'/'f'/'n'，`True`/`NULL` 首字母大写
  /// 即非字面量），整词由 Match（OrdinalIgnoreCase）自该字符起大小写不敏感匹配；
  /// C# 数值形态不识别前导 '+'，rust 侧同步去除。
  fn try_parse_value(&mut self) -> Result<Option<Value>> {
    self.eat_whitespace();
    let Some(&first) = self.bytes.get(self.pos) else {
      return Ok(None);
    };
    match first {
      b'n' => return Ok(self.match_token_ci(b"null").then(|| Value::from(()))),
      b't' => return Ok(self.match_token_ci(b"true").then(|| Value::from(true))),
      b'f' => return Ok(self.match_token_ci(b"false").then(|| Value::from(false))),
      _ => {}
    }

    if !(first.is_ascii_digit() || first == b'-') {
      return Ok(None);
    }
    let start = self.pos;
    self.pos += 1;
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
  ///
  /// 1:1 对位 C#：逐臂 Match（OrdinalIgnoreCase、不要求尾随空格）穷举比较运算符，
  /// 全不命中即抛 "Could not read query operator."。C# 无 nin 分支，故 `nin` 按该
  /// 口径直接报错上抛（rust 旧实现把 "nin " 当独立运算符、吞首字母后按 In 求值，
  /// 语义反转，现与 C# 对齐）。
  fn parse_operator(&mut self) -> Result<QueryOperator> {
    if self.match_token(b"===") {
      Ok(QueryOperator::StrictEquals)
    } else if self.match_token(b"==") {
      Ok(QueryOperator::Equals)
    } else if self.match_token(b"=~") {
      Ok(QueryOperator::RegexEquals)
    } else if self.match_token(b"!==") {
      Ok(QueryOperator::StrictNotEquals)
    } else if self.match_token(b"!=") || self.match_token(b"<>") {
      // C# JsonPath.cs:ParseOperator 将 <> 与 != 等价识别为 NotEquals
      Ok(QueryOperator::NotEquals)
    } else if self.match_token(b"<=") {
      Ok(QueryOperator::LessThanOrEquals)
    } else if self.match_token(b"<") {
      Ok(QueryOperator::LessThan)
    } else if self.match_token(b">=") {
      Ok(QueryOperator::GreaterThanOrEquals)
    } else if self.match_token(b">") {
      Ok(QueryOperator::GreaterThan)
    } else if self.match_token_ci(b"in") {
      // C# Match("in") 大小写不敏感且无空格要求：`in[1,2]` 与 `in [1,2]` 同结果
      Ok(QueryOperator::In)
    } else {
      Err(Error::InvalidPath(
        "Could not read query operator.".to_string(),
      ))
    }
  }

  /// 在 garnet 中的相对路径:modules/GarnetJSON/JSONPath/JsonPath.cs:Match
  ///
  /// C# Match 为 OrdinalIgnoreCase 大小写不敏感匹配，命中即前进。
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

  /// 大小写不敏感的 [`match_token`]（对位 C# Match 的 OrdinalIgnoreCase 口径）
  fn match_token_ci(&mut self, token: &[u8]) -> bool {
    if self.pos + token.len() <= self.bytes.len()
      && self.bytes[self.pos..self.pos + token.len()].eq_ignore_ascii_case(token)
    {
      self.pos += token.len();
      true
    } else {
      false
    }
  }

  /// 当前位置是否以词边界起头命中 `token`（大小写不敏感、不消费）：
  /// 紧邻后继字节为 ASCII 字母数字/下划线时视为更长成员名的一部分而非独立词
  fn at_word(&self, token: &[u8]) -> bool {
    let end = self.pos + token.len();
    end <= self.bytes.len()
      && self.bytes[self.pos..end].eq_ignore_ascii_case(token)
      && !matches!(self.bytes.get(end), Some(&c) if c.is_ascii_alphanumeric() || c == b'_')
  }
}
