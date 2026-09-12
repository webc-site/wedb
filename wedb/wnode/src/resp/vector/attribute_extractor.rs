//! 轻量顶级 JSON 字段提取器（对标 libs/server/Resp/Vector/AttributeExtractor.cs）
//!
//! 单趟扫描提取顶级字段，值直接产出为零拷贝 [`ExprToken`]：
//! 字符串以 (绝对偏移， 长度) 引用源 JSON 字节，不做任何字符串分配。
//! 同时承载预提取二进制属性格式（`0xFF` 标记 + 字段表）的编解码，
//! 供过滤求值的快速通道使用。

use super::vector_filter_expression::{ExprProgram, ExprToken, ExprTokenType};

/// 超过该元素个数的数组将被拒绝（跳过并产出 Null）。
const MAX_ARRAY_ELEMENTS: usize = 64;

/// 二进制属性格式的起始标记字节。
pub const BINARY_MARKER: u8 = 0xFF;

const BIN_TYPE_STRING: u8 = 0;
const BIN_TYPE_NUMBER: u8 = 1;
const BIN_TYPE_BOOL_TRUE: u8 = 2;
const BIN_TYPE_BOOL_FALSE: u8 = 3;
const BIN_TYPE_NULL: u8 = 4;

/// 选择器区间（过滤表达式字节中的 (起始偏移， 长度)）。
pub type SelectorRange = (i32, i32);

/// libs/server/Resp/Vector/AttributeExtractor.cs:ExtractFields
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
      results[i] = parse_value_token(json, &mut pos, program).unwrap_or_default();
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

/// libs/server/Resp/Vector/AttributeExtractor.cs:ExtractField
///
/// 提取单个顶级字段；未找到时返回 `ExprToken::default()`（is_none）。
/// 本路径无程序上下文，数组按 `parse_array_token_no_pool` 降级为 Null。
pub fn extract_field(json: &[u8], field_name_utf8: &[u8]) -> ExprToken {
  let mut pos = 0;
  trim_white_space(json, &mut pos);
  if pos >= json.len() || json[pos] != b'{' {
    return ExprToken::default();
  }
  pos += 1;

  loop {
    trim_white_space(json, &mut pos);
    if pos >= json.len() || json[pos] == b'}' || json[pos] != b'"' {
      return ExprToken::default();
    }

    let key_start = pos + 1;
    if !skip_string(json, &mut pos) {
      return ExprToken::default();
    }
    let matched = &json[key_start..pos - 1] == field_name_utf8;

    trim_white_space(json, &mut pos);
    if pos >= json.len() || json[pos] != b':' {
      return ExprToken::default();
    }
    pos += 1;

    trim_white_space(json, &mut pos);
    if pos >= json.len() {
      return ExprToken::default();
    }

    if matched {
      return parse_value_token_no_pool(json, &mut pos).unwrap_or_default();
    }
    if !skip_value(json, &mut pos) {
      return ExprToken::default();
    }

    trim_white_space(json, &mut pos);
    if pos >= json.len() {
      return ExprToken::default();
    }
    match json[pos] {
      b',' => pos += 1,
      _ => return ExprToken::default(),
    }
  }
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:ParseValueToken
///
/// 按值首字节分派解析；嵌套对象不支持（产出 None）。
fn parse_value_token(json: &[u8], pos: &mut usize, program: &mut ExprProgram) -> Option<ExprToken> {
  trim_white_space(json, pos);
  let c = *json.get(*pos)?;
  match c {
    b'"' => parse_string_token(json, pos),
    b'[' => parse_array_token(json, pos, program),
    b'{' => None,
    b't' => parse_literal_token(json, pos, b"true", ExprTokenType::Num, 1.0),
    b'f' => parse_literal_token(json, pos, b"false", ExprTokenType::Num, 0.0),
    b'n' => parse_literal_token(json, pos, b"null", ExprTokenType::Null, 0.0),
    _ if is_digit(c) || c == b'-' || c == b'+' => parse_number_token(json, pos),
    _ => None,
  }
}

/// 对应 ParseValueToken 无程序上下文的重载：数组经 `parse_array_token_no_pool` 跳过。
fn parse_value_token_no_pool(json: &[u8], pos: &mut usize) -> Option<ExprToken> {
  trim_white_space(json, pos);
  let c = *json.get(*pos)?;
  match c {
    b'"' => parse_string_token(json, pos),
    b'[' => parse_array_token_no_pool(json, pos),
    b'{' => None, // 嵌套对象不支持
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
    let elem = parse_value_token_no_pool(json, pos)?;
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
  if start + local_buf.len() > super::expr_compiler::MAX_RUNTIME_POOL {
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
pub fn is_digit(b: u8) -> bool {
  b.is_ascii_digit()
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:IsLetter
#[inline]
pub fn is_letter(b: u8) -> bool {
  b.is_ascii_alphabetic()
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:IsLetterOrDigit
#[inline]
pub fn is_letter_or_digit(b: u8) -> bool {
  b.is_ascii_alphanumeric()
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:IsWhiteSpace
#[inline]
pub fn is_white_space(b: u8) -> bool {
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
fn is_number_char(b: u8) -> bool {
  is_digit(b) || matches!(b, b'-' | b'+' | b'.' | b'e' | b'E')
}

/// 严格全量解析 f64（对齐 C# Utf8Parser + consumed == length 语义）。
fn parse_f64_exact(span: &[u8]) -> Option<f64> {
  let text = str::from_utf8(span).ok()?;
  // 拒绝 inf/NaN 字面量：JSON 数值语法不含它们
  let value = text.parse::<f64>().ok()?;
  value.is_finite().then_some(value)
}

/// 按绝对偏移切片（防越界辅助）。
#[inline]
fn slice_at(buf: &[u8], start: i32, len: i32) -> &[u8] {
  let start = start.max(0) as usize;
  let len = len.max(0) as usize;
  buf
    .get(start..)
    .map_or(&[] as &[u8], |tail| &tail[..len.min(tail.len())])
}

// ======================== 二进制属性格式 ========================
//
// 快速过滤求值用的预提取格式：
//   [0xFF 标记]
//   [num_fields: u8]
//   每字段:
//     [field_name_len: u8]
//     [field_name: N 字节]          ← 原始 UTF-8
//     [value_type: u8]              ← 0=字符串 1=数字 2=true 3=false 4=null
//     [value_len: u16 LE]
//     [value_bytes: N 字节]         ← UTF-8 字符串或 8 字节 f64 LE

/// libs/server/Resp/Vector/AttributeExtractor.cs:ConvertJsonToBinary
///
/// 顶级 JSON 对象 → 预提取二进制格式；输出过小或含不支持结构时返回 -1。
pub fn convert_json_to_binary(json: &[u8], output: &mut [u8]) -> i32 {
  let mut pos = 0;
  trim_white_space(json, &mut pos);
  if pos >= json.len() || json[pos] != b'{' {
    return -1;
  }
  pos += 1;

  if output.len() < 2 {
    return -1;
  }
  output[0] = BINARY_MARKER;
  // output[1] = num_fields，结束时回填
  let mut out_pos = 2;
  let mut field_count: u8 = 0;

  loop {
    trim_white_space(json, &mut pos);
    if pos >= json.len() {
      return -1;
    }
    if json[pos] == b'}' {
      break;
    }
    if json[pos] != b'"' {
      return -1;
    }

    // 键
    let key_start = pos + 1;
    if !skip_string(json, &mut pos) {
      return -1;
    }
    let key_content = &json[key_start..pos - 1];
    if key_content.len() > 255 || key_content.contains(&b'\\') {
      // 键含转义序列不支持
      return -1;
    }
    if out_pos + 1 + key_content.len() + 1 + 2 > output.len() {
      return -1;
    }
    output[out_pos] = key_content.len() as u8;
    out_pos += 1;
    output[out_pos..out_pos + key_content.len()].copy_from_slice(key_content);
    out_pos += key_content.len();

    // 冒号
    trim_white_space(json, &mut pos);
    if pos >= json.len() || json[pos] != b':' {
      return -1;
    }
    pos += 1;

    trim_white_space(json, &mut pos);
    if pos >= json.len() {
      return -1;
    }

    match json[pos] {
      // 字符串值 —— 需反转义后写出
      b'"' => {
        pos += 1;
        let content_start = pos;
        let mut has_escape = false;
        while pos < json.len() {
          match json[pos] {
            b'\\' => {
              has_escape = true;
              pos += 2;
              continue;
            }
            b'"' => break,
            _ => pos += 1,
          }
        }
        if pos >= json.len() {
          return -1;
        }
        let str_content = &json[content_start..pos];
        pos += 1;

        output[out_pos] = BIN_TYPE_STRING;
        out_pos += 1;

        if !has_escape {
          // 无转义 —— 直接拷贝
          if out_pos + 2 + str_content.len() > output.len() {
            return -1;
          }
          write_u16_le(&mut output[out_pos..], str_content.len() as u16);
          out_pos += 2;
          output[out_pos..out_pos + str_content.len()].copy_from_slice(str_content);
          out_pos += str_content.len();
        } else {
          // 反转义写入输出
          let value_len_pos = out_pos;
          out_pos += 2;
          let value_start = out_pos;
          let mut si = 0;
          while si < str_content.len() {
            if out_pos >= output.len() {
              return -1;
            }
            if str_content[si] == b'\\' && si + 1 < str_content.len() {
              si += 1;
              output[out_pos] = match str_content[si] {
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                other => other, // \", \\, \/ 等
              };
            } else {
              output[out_pos] = str_content[si];
            }
            out_pos += 1;
            si += 1;
          }
          let value_len = (out_pos - value_start) as u16;
          write_u16_le(&mut output[value_len_pos..], value_len);
        }
      }
      // 数字值 —— 8 字节 f64 LE
      c if is_digit(c) || c == b'-' || c == b'+' => {
        let num_start = pos;
        while pos < json.len() && is_number_char(json[pos]) {
          pos += 1;
        }
        let Some(num_val) = parse_f64_exact(&json[num_start..pos]) else {
          return -1;
        };
        output[out_pos] = BIN_TYPE_NUMBER;
        out_pos += 1;
        if out_pos + 2 + 8 > output.len() {
          return -1;
        }
        write_u16_le(&mut output[out_pos..], 8);
        out_pos += 2;
        output[out_pos..out_pos + 8].copy_from_slice(&num_val.to_le_bytes());
        out_pos += 8;
      }
      b't' => {
        if !skip_literal(json, &mut pos, b"true") {
          return -1;
        }
        output[out_pos] = BIN_TYPE_BOOL_TRUE;
        out_pos += 1;
        if out_pos + 2 > output.len() {
          return -1;
        }
        write_u16_le(&mut output[out_pos..], 0);
        out_pos += 2;
      }
      b'f' => {
        if !skip_literal(json, &mut pos, b"false") {
          return -1;
        }
        output[out_pos] = BIN_TYPE_BOOL_FALSE;
        out_pos += 1;
        if out_pos + 2 > output.len() {
          return -1;
        }
        write_u16_le(&mut output[out_pos..], 0);
        out_pos += 2;
      }
      b'n' => {
        if !skip_literal(json, &mut pos, b"null") {
          return -1;
        }
        output[out_pos] = BIN_TYPE_NULL;
        out_pos += 1;
        if out_pos + 2 > output.len() {
          return -1;
        }
        write_u16_le(&mut output[out_pos..], 0);
        out_pos += 2;
      }
      // 嵌套对象/数组 —— 二进制格式不支持
      _ => return -1,
    }

    field_count += 1;

    trim_white_space(json, &mut pos);
    if pos >= json.len() {
      return -1;
    }
    match json[pos] {
      b',' => pos += 1,
      b'}' => break,
      _ => return -1,
    }
  }

  output[1] = field_count;
  out_pos as i32
}

/// libs/server/Resp/Vector/AttributeExtractor.cs:ExtractFieldsBinary
///
/// 从预提取二进制属性数据提取字段；语义同 [`extract_fields`] 但无需解析 JSON。
pub fn extract_fields_binary(
  binary: &[u8],
  filter_bytes: &[u8],
  selector_ranges: &[SelectorRange],
  results: &mut [ExprToken],
) -> usize {
  for r in results.iter_mut().take(selector_ranges.len()) {
    *r = ExprToken::default();
  }

  if binary.len() < 2 || binary[0] != BINARY_MARKER {
    return 0;
  }

  let num_fields = binary[1];
  let mut pos = 2;
  let mut found = 0;
  let needed = selector_ranges.len();

  for _ in 0..num_fields {
    if pos >= binary.len() {
      break;
    }
    let name_len = binary[pos] as usize;
    pos += 1;
    if pos + name_len > binary.len() {
      break;
    }
    let field_name = &binary[pos..pos + name_len];
    pos += name_len;

    if pos >= binary.len() {
      break;
    }
    let value_type = binary[pos];
    pos += 1;

    if pos + 2 > binary.len() {
      break;
    }
    let value_len = u16::from_le_bytes([binary[pos], binary[pos + 1]]) as usize;
    pos += 2;
    if pos + value_len > binary.len() {
      break;
    }

    let mut match_index = None;
    for (i, range) in selector_ranges.iter().enumerate() {
      if results[i].is_none() && field_name == slice_at(filter_bytes, range.0, range.1) {
        match_index = Some(i);
        break;
      }
    }

    if let Some(i) = match_index {
      match value_type {
        BIN_TYPE_STRING => {
          results[i] = ExprToken::new_str(pos as i32, value_len as i32, false);
        }
        BIN_TYPE_NUMBER if value_len == 8 => {
          results[i] =
            ExprToken::new_num(f64::from_le_bytes(binary[pos..pos + 8].try_into().unwrap()));
        }
        BIN_TYPE_BOOL_TRUE => results[i] = ExprToken::new_num(1.0),
        BIN_TYPE_BOOL_FALSE => results[i] = ExprToken::new_num(0.0),
        BIN_TYPE_NULL => results[i] = ExprToken::new_null(),
        _ => {}
      }

      found += 1;
      if found == needed {
        return found;
      }
    }

    pos += value_len;
  }

  found
}

/// 在 `buf` 头部写出 u16 LE。
fn write_u16_le(buf: &mut [u8], value: u16) {
  buf[..2].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
  use ExprTokenType::Str;

  use super::*;

  #[test]
  fn extract_single_field() {
    let json = br#"{"year": 2000, "rating": 7.5, "name": "k\"x", "ok": true, "no": null}"#;
    assert_eq!(extract_field(json, b"year"), ExprToken::new_num(2000.0));
    assert_eq!(extract_field(json, b"rating"), ExprToken::new_num(7.5));
    assert_eq!(extract_field(json, b"ok"), ExprToken::new_num(1.0));
    assert_eq!(extract_field(json, b"no"), ExprToken::new_null());
    assert_eq!(extract_field(json, b"missing"), ExprToken::default());

    let t = extract_field(json, b"name");
    assert_eq!(t.token_type, Str);
    assert!(t.has_escape());
    let s = &json[t.utf8_start as usize..(t.utf8_start + t.utf8_length) as usize];
    assert_eq!(s, b"k\\\"x");
  }

  #[test]
  fn extract_field_array_no_pool_degrades_to_null() {
    let json = br#"{"xs": [1,2,3]}"#;
    let t = extract_field(json, b"xs");
    assert_eq!(t, ExprToken::new_null());
  }

  #[test]
  fn extract_multi_fields_single_pass() {
    let json = br#" { "a": 1, "b": "two", "c": [3, 4] } "#;
    // 选择器引用下方 filter 字节中的字段名（与 expr_compiler 产出一致的区间）
    let filter = b"aXXXXbXXXXcXXXX"; // 区间占位：(0,1) (5,1) (10,1)
    let selectors = [(0i32, 1i32), (5, 1), (10, 1)];
    let mut program = ExprProgram::default();
    let mut results = [ExprToken::default(); 3];
    let found = extract_fields(json, filter, &selectors, &mut results, &mut program);
    assert_eq!(found, 3);
    assert_eq!(results[0], ExprToken::new_num(1.0));
    assert_eq!(results[1].token_type, Str);
    assert!(results[2].is_runtime_tuple());
    assert_eq!(results[2].utf8_length, 2);
    // 运行期池已收录数组元素
    assert_eq!(program.runtime_pool_len, 2);
    assert_eq!(program.runtime_pool[0], ExprToken::new_num(3.0));
  }

  #[test]
  fn skip_helpers() {
    let raw: &[u8] = b"  \"esc \\\" x\" tail";
    let mut pos = 2; // 跳过前导空白，SkipString 要求 pos 指向开引号
    assert!(skip_string(raw, &mut pos));
    assert_eq!(&raw[pos..], b" tail");

    let raw: &[u8] = b"[1,[2,{\"x\":\"}\"}],3] end";
    let mut pos = 0;
    assert!(skip_bracketed(raw, &mut pos, b'[', b']'));
    assert_eq!(&raw[pos..], b" end");

    // SkipLiteral 仅做前缀匹配（尾随字符校验由 ParseLiteralToken 负责）
    let mut pos = 0;
    assert!(skip_literal(b"truex", &mut pos, b"true"));
    assert_eq!(pos, 4);
    let mut pos = 0;
    assert!(skip_literal(b"true", &mut pos, b"true"));

    let mut pos = 0;
    assert!(skip_value(b"{\"a\":1}", &mut pos));
    assert_eq!(pos, 7);

    let mut pos = 0;
    assert!(skip_number(b"-12.5e3|", &mut pos));
    assert_eq!(pos, 7);
  }

  #[test]
  fn char_classes() {
    assert!(is_digit(b'0') && is_digit(b'9'));
    assert!(!is_digit(b'/'));
    assert!(is_letter(b'a') && is_letter(b'Z') && !is_letter(b'0'));
    assert!(is_letter_or_digit(b'5'));
    assert!(is_white_space(b'\t') && is_white_space(b'\r'));
    assert!(is_number_char(b'e') && is_number_char(b'.') && !is_number_char(b'x'));
  }

  #[test]
  fn json_to_binary_roundtrip() {
    let json = br#"{"s":"hello","n":-3.5e2,"t":true,"f":false,"z":null}"#;
    let mut buf = vec![0u8; 256];
    let written = convert_json_to_binary(json, &mut buf);
    assert!(written > 0);
    let bin = &buf[..written as usize];
    assert_eq!(bin[0], BINARY_MARKER);
    assert_eq!(bin[1], 5);

    for (name, expect) in [
      ("s", ExprToken::new_str(0, 5, false)), // 偏移断言在下方单独做
      ("n", ExprToken::new_num(-350.0)),
      ("t", ExprToken::new_num(1.0)),
      ("f", ExprToken::new_num(0.0)),
      ("z", ExprToken::new_null()),
    ] {
      let ranges = vec![(0i32, name.len() as i32)];
      let mut results = [ExprToken::default()];
      let found = extract_fields_binary(bin, name.as_bytes(), &ranges, &mut results);
      assert_eq!(found, 1, "{name}");
      if name == "s" {
        assert_eq!(results[0].token_type, Str);
        assert_eq!(results[0].utf8_length, 5);
        let s = &bin[results[0].utf8_start as usize..];
        assert_eq!(&s[..5], b"hello");
      } else {
        assert_eq!(results[0], expect, "{name}");
      }
    }
  }

  #[test]
  fn binary_string_unescape() {
    let json = br#"{"k":"a\nb"}"#;
    let mut buf = vec![0u8; 64];
    let written = convert_json_to_binary(json, &mut buf);
    assert!(written > 0);
    let bin = &buf[..written as usize];
    let ranges = vec![(0i32, 1i32)];
    let mut results = [ExprToken::default()];
    assert_eq!(extract_fields_binary(bin, b"k", &ranges, &mut results), 1);
    assert_eq!(results[0].token_type, Str);
    // 二进制缓冲中应为反转义后的换行
    assert_eq!(bin[results[0].utf8_start as usize], b'a');
    assert_eq!(bin[results[0].utf8_start as usize + 1], b'\n');
    assert_eq!(bin[results[0].utf8_start as usize + 2], b'b');
  }

  #[test]
  fn binary_rejects_bad_input() {
    let mut buf = vec![0u8; 64];
    // 非对象
    assert_eq!(convert_json_to_binary(b"[1,2]", &mut buf), -1);
    // 嵌套对象
    assert_eq!(convert_json_to_binary(b"{\"a\":{\"b\":1}}", &mut buf), -1);
    // 输出过小
    assert_eq!(convert_json_to_binary(b"{\"a\":1}", &mut buf[..3]), -1);
    // 数字非法
    assert_eq!(convert_json_to_binary(b"{\"a\":1.2.3}", &mut buf), -1);
  }
}
