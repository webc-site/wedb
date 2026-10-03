//! 轻量顶级 JSON 字段提取器（对标 diskann-garnet/AttributeExtractor.cs）
//!
//! 单趟扫描提取顶级字段，值直接产出为零拷贝 [`ExprToken`]：
//! 字符串以 (绝对偏移， 长度) 引用源 JSON 字节，不做任何字符串分配。

use super::{
  expression::{ExprProgram, ExprToken, ExprTokenType},
  parse_f64_exact, slice_at,
};

/// 超过该元素个数的数组将被拒绝（跳过并产出 Null）。
const MAX_ARRAY_ELEMENTS: usize = 64;

/// 选择器区间（过滤表达式字节中的 (起始偏移， 长度)）。
pub type SelectorRange = (i32, i32);

/// libs/server/Resp/Vector/AttributeExtractor.cs:ExtractFields
/// libs/server/Resp/Vector/AttributeExtractor.cs:ExtractField
///
/// 单趟从 JSON 对象提取多个顶级字段；选择器为过滤字节中的区间，
/// 提取值为 JSON 字节中的区间；`results[i].is_none()` 表示未命中。
/// 返回成功提取的字段数。
pub fn extract_fields(
  json: &[u8],
  filter_bytes: &[u8],
  selector_ranges: &[SelectorRange],
  results: &mut [ExprToken],
  program: &mut ExprProgram,
) -> usize {
  for r in results.iter_mut().take(selector_ranges.len()) {
    *r = ExprToken::default();
  }

  let mut pos = 0;
  trim_white_space(json, &mut pos);
  if pos >= json.len() || json[pos] != b'{' {
    return 0;
  }
  pos += 1;

  let mut found = 0;
  let needed = selector_ranges.len();

  loop {
    trim_white_space(json, &mut pos);
    if pos >= json.len() || json[pos] == b'}' {
      return found;
    }
    if json[pos] != b'"' {
      return found;
    }

    // 键名（不含引号）
    let key_start = pos + 1;
    if !skip_string(json, &mut pos) {
      return found;
    }
    let key_content = &json[key_start..pos - 1];

    // 匹配尚未命中的选择器（重复字段取首见值）
    let mut match_index = None;
    for (i, range) in selector_ranges.iter().enumerate() {
      if results[i].is_none() && key_content == slice_at(filter_bytes, range.0, range.1) {
        match_index = Some(i);
        break;
      }
    }

    trim_white_space(json, &mut pos);
    if pos >= json.len() || json[pos] != b':' {
      return found;
    }
    pos += 1;

    trim_white_space(json, &mut pos);
    if pos >= json.len() {
      return found;
    }

    if let Some(i) = match_index {
      // 解析失败产出 None 词元并照常计数（对齐 C# 的 results[i] = ParseValueToken(...)）
      results[i] = parse_value_token_inner(json, &mut pos, Some(program)).unwrap_or_default();
      found += 1;
      if found == needed {
        return found;
      }
    } else if !skip_value(json, &mut pos) {
      return found;
    }

    trim_white_space(json, &mut pos);
    if pos >= json.len() {
      return found;
    }
    match json[pos] {
      b',' => pos += 1,
      _ => return found,
    }
  }
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:ParseValueToken
///
/// 按值首字节分派解析；嵌套对象不支持（产出 None）。
/// `program` 为 `None` 时无程序上下文（对应 C# 无池重载）：数组经
/// `parse_array_token_no_pool` 跳过并产出 Null。
fn parse_value_token_inner(
  json: &[u8],
  pos: &mut usize,
  program: Option<&mut ExprProgram>,
) -> Option<ExprToken> {
  trim_white_space(json, pos);
  let c = *json.get(*pos)?;
  match c {
    b'"' => parse_string_token(json, pos),
    b'[' => match program {
      Some(program) => parse_array_token(json, pos, program),
      None => parse_array_token_no_pool(json, pos),
    },
    b'{' => None,
    b't' => parse_literal_token(json, pos, b"true", ExprTokenType::Num, 1.0),
    b'f' => parse_literal_token(json, pos, b"false", ExprTokenType::Num, 0.0),
    b'n' => parse_literal_token(json, pos, b"null", ExprTokenType::Null, 0.0),
    _ if is_digit(c) || c == b'-' || c == b'+' => parse_number_token(json, pos),
    _ => None,
  }
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:ParseStringToken
///
/// 产出引用 JSON 字节的 Str 词元（不含引号），记录是否存在转义。
fn parse_string_token(json: &[u8], pos: &mut usize) -> Option<ExprToken> {
  if *pos >= json.len() || json[*pos] != b'"' {
    return None;
  }
  *pos += 1;
  let content_start = *pos;
  let mut has_escape = false;

  while *pos < json.len() {
    match json[*pos] {
      b'\\' => {
        has_escape = true;
        *pos += 2;
      }
      b'"' => {
        let token = ExprToken::new_str(
          content_start as i32,
          (*pos - content_start) as i32,
          has_escape,
        );
        *pos += 1;
        return Some(token);
      }
      _ => *pos += 1,
    }
  }
  None
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:ParseNumberToken
///
/// 数值化交共用 [`parse_f64_exact`]（Utf8Parser 同形：全量消费、溢出折
/// ±Inf 成功、词法层拒 inf/nan 词形），与本文件 IsNumberChar 扫描臂同 C#。
fn parse_number_token(json: &[u8], pos: &mut usize) -> Option<ExprToken> {
  let start = *pos;
  while *pos < json.len() && is_number_char(json[*pos]) {
    *pos += 1;
  }
  let span = &json[start..*pos];
  if span.is_empty() {
    return None;
  }
  let value = parse_f64_exact(span)?;
  Some(ExprToken::new_num(value))
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:ParseLiteralToken
///
/// 字面量后必须跟随空白、`,`、`]` 或 `}`，防止 `truex` 这类误配。
fn parse_literal_token(
  json: &[u8],
  pos: &mut usize,
  literal: &[u8],
  token_type: ExprTokenType,
  num: f64,
) -> Option<ExprToken> {
  if json.len() < *pos + literal.len() {
    return None;
  }
  if &json[*pos..*pos + literal.len()] != literal {
    return None;
  }
  if let Some(&next) = json.get(*pos + literal.len())
    && !is_white_space(next)
    && next != b','
    && next != b']'
    && next != b'}'
  {
    return None;
  }
  *pos += literal.len();
  Some(if token_type == ExprTokenType::Null {
    ExprToken::new_null()
  } else {
    ExprToken::new_num(num)
  })
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:ParseArrayToken
///
/// 解析 JSON 数组为运行期 Tuple：元素写入 program 运行期元组池，
/// 供 `IN` 求值迭代；池满时优雅降级为 Null（跳过数组）。
fn parse_array_token(json: &[u8], pos: &mut usize, program: &mut ExprProgram) -> Option<ExprToken> {
  if *pos >= json.len() || json[*pos] != b'[' {
    return None;
  }
  *pos += 1;
  trim_white_space(json, pos);

  // 空数组
  if json.get(*pos) == Some(&b']') {
    *pos += 1;
    return Some(ExprToken::new_tuple(0, 0));
  }

  let mut local_buf: Vec<ExprToken> = Vec::with_capacity(MAX_ARRAY_ELEMENTS);

  loop {
    trim_white_space(json, pos);
    if *pos >= json.len() {
      return None;
    }
    if local_buf.len() >= MAX_ARRAY_ELEMENTS {
      let _ = skip_bracketed(json, pos, b'[', b']');
      return Some(ExprToken::new_null());
    }

    // C# 元素解析走无程序上下文的重载：嵌套数组降级为 Null 元素
    let elem = parse_value_token_inner(json, pos, None)?;
    local_buf.push(elem);

    trim_white_space(json, pos);
    match json.get(*pos) {
      Some(b']') => {
        *pos += 1;
        break;
      }
      Some(b',') => *pos += 1,
      _ => return None,
    }
  }

  let start = program.runtime_pool_len;
  if start + local_buf.len() > super::compiler::MAX_RUNTIME_POOL {
    // 池耗尽 —— 跳过数组优雅降级
    return Some(ExprToken::new_null());
  }
  program.runtime_pool.truncate(start);
  program.runtime_pool.extend_from_slice(&local_buf);
  program.runtime_pool_len += local_buf.len();
  Some(ExprToken::new_runtime_tuple(
    start as i32,
    local_buf.len() as i32,
  ))
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:ParseArrayTokenNoPool
///
/// 无程序上下文的独立提取 —— 仅跳过数组并产出 Null。
fn parse_array_token_no_pool(json: &[u8], pos: &mut usize) -> Option<ExprToken> {
  if skip_value(json, pos) {
    Some(ExprToken::new_null())
  } else {
    None
  }
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:SkipValue
fn skip_value(json: &[u8], pos: &mut usize) -> bool {
  trim_white_space(json, pos);
  let Some(&c) = json.get(*pos) else {
    return false;
  };
  match c {
    b'"' => skip_string(json, pos),
    b'{' => skip_bracketed(json, pos, b'{', b'}'),
    b'[' => skip_bracketed(json, pos, b'[', b']'),
    b't' => skip_literal(json, pos, b"true"),
    b'f' => skip_literal(json, pos, b"false"),
    b'n' => skip_literal(json, pos, b"null"),
    _ => skip_number(json, pos),
  }
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:SkipString
///
/// `pos` 指向开引号；成功后推进到闭引号之后。
fn skip_string(json: &[u8], pos: &mut usize) -> bool {
  if *pos >= json.len() || json[*pos] != b'"' {
    return false;
  }
  *pos += 1;
  while *pos < json.len() {
    match json[*pos] {
      b'\\' => *pos += 2,
      b'"' => {
        *pos += 1;
        return true;
      }
      _ => *pos += 1,
    }
  }
  false
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:SkipBracketed
///
/// 深度计数跳过括号块，字符串内部的括号不参与计数。
fn skip_bracketed(json: &[u8], pos: &mut usize, opener: u8, closer: u8) -> bool {
  let mut depth = 1;
  *pos += 1;
  while *pos < json.len() && depth > 0 {
    match json[*pos] {
      b'"' => {
        if !skip_string(json, pos) {
          return false;
        }
        continue;
      }
      c if c == opener => depth += 1,
      c if c == closer => depth -= 1,
      _ => {}
    }
    *pos += 1;
  }
  depth == 0
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:SkipLiteral
fn skip_literal(json: &[u8], pos: &mut usize, literal: &[u8]) -> bool {
  if json.len() < *pos + literal.len() {
    return false;
  }
  if &json[*pos..*pos + literal.len()] != literal {
    return false;
  }
  *pos += literal.len();
  true
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:SkipNumber
fn skip_number(json: &[u8], pos: &mut usize) -> bool {
  let start = *pos;
  while *pos < json.len() && is_number_char(json[*pos]) {
    *pos += 1;
  }
  *pos > start
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:IsDigit
#[inline]
pub const fn is_digit(b: u8) -> bool {
  b.is_ascii_digit()
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:IsLetter
#[inline]
pub const fn is_letter(b: u8) -> bool {
  b.is_ascii_alphabetic()
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:IsLetterOrDigit
#[inline]
pub const fn is_letter_or_digit(b: u8) -> bool {
  b.is_ascii_alphanumeric()
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:IsWhiteSpace
#[inline]
pub(crate) const fn is_white_space(b: u8) -> bool {
  matches!(b, b' ' | b'\t' | b'\n' | b'\r')
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:TrimWhiteSpace
#[inline]
pub fn trim_white_space(json: &[u8], pos: &mut usize) {
  while *pos < json.len() && is_white_space(json[*pos]) {
    *pos += 1;
  }
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:IsNumberChar
#[inline]
const fn is_number_char(b: u8) -> bool {
  is_digit(b) || matches!(b, b'-' | b'+' | b'.' | b'e' | b'E')
}
