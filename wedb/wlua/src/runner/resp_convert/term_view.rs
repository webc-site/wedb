//! RESP 报文 → Lua 栈值单项解析（ProcessRespResponse 的分派主体）
//!
//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:ProcessSingleRespTerm
//!
//! 类型族：Common（`+` simple / `:` integer / `-` error / `$` bulk /
//! `*` array）与 RESP3（`%` map / `_` null / `~` set / `#` boolean /
//! `,` double / `(` big number / `=` verbatim）。巨型 match 按整块保留：
//! 拆臂将逐臂增加跳转，违纯结构拆分约束。

use std::str;

use wresp::{
  ext::is_resp3,
  read::{
    try_read_as_span, try_read_signed_array_length, try_read_signed_map_length,
    try_read_signed_set_length, try_read_verbatim_string_length, try_slice_with_length_header,
  },
};

use super::{lua_wrapped_error_view, read_resp_int};
use crate::{LuaState, strings::ConstantStrings};

/// 查找 CRLF 位置。
fn find_crlf(data: &[u8]) -> Option<usize> {
  data.windows(2).position(|w| w == b"\r\n")
}

/// libs/server/Lua/LuaRunner.cs:ProcessSingleRespTerm
pub(crate) fn process_single_resp_term_view(
  state: &mut LuaState,
  resp_protocol_version: u8,
  cursor: &mut &[u8],
) -> i32 {
  let Some(&indicator) = cursor.first() else {
    log::error!("Unexpected response, this should never happen");
    return lua_wrapped_error_view(state, 1, ConstantStrings::UNEXPECTED_ERROR);
  };

  match indicator {
    // Simple reply (Common)
    b'+' => {
      *cursor = &cursor[1..];
      let mut result_span: &[u8] = &[];
      if matches!(try_read_as_span(&mut result_span, cursor), Ok(true)) {
        // Construct a table = { 'ok': value }
        state.create_table(0, 1);
        // 对标 C# RawSet(1, curTop + 1)：新建表的栈绝对索引（C# LuaRunner.cs:615）
        let table_index = state.get_top() as i32;
        state.push_buffer(ConstantStrings::OK_LOWER);
        state.push_buffer(result_span);
        state.raw_set(table_index);
        return 1;
      }
      default_resp_term_view(state)
    }

    // Integer (Common)
    b':' => {
      if let Some(number) = read_resp_int(cursor) {
        state.push_integer(number);
        return 1;
      }
      default_resp_term_view(state)
    }

    // Error (Common)
    b'-' => {
      *cursor = &cursor[1..];
      let mut err_span: &[u8] = &[];
      if matches!(try_read_as_span(&mut err_span, cursor), Ok(true)) {
        if err_span == ConstantStrings::RESP_ERR_GENERIC_UNK_CMD {
          // Gets a special response
          return lua_wrapped_error_view(state, 1, ConstantStrings::ERR_UNKNOWN);
        }

        return lua_wrapped_error_view(state, 1, err_span);
      }
      default_resp_term_view(state)
    }

    // Bulk string or null bulk string (Common)
    b'$' => {
      // "$-1\r\n" → RESP2 null bulk → false
      if cursor.len() >= 5 && &cursor[1..5] == b"-1\r\n" {
        // Bulk null strings are mapped to FALSE
        state.push_boolean(false);
        *cursor = &cursor[5..];
        return 1;
      }
      let mut bulk_span: &[u8] = &[];
      if matches!(
        try_slice_with_length_header(&mut bulk_span, cursor),
        Ok(true)
      ) {
        state.push_buffer(bulk_span);
        return 1;
      }
      default_resp_term_view(state)
    }

    // Array (Common)
    b'*' => {
      let mut array_item_count = 0i32;
      if matches!(
        try_read_signed_array_length(&mut array_item_count, cursor),
        Ok(true)
      ) {
        if array_item_count == -1 {
          state.push_boolean(false);
        } else {
          let count = array_item_count as usize;
          state.create_table(count, 0);
          let table_index = state.get_top() as i32;

          for item_ix in 0..count {
            // Pushes the item to the top of the stack
            _ = process_single_resp_term_view(state, resp_protocol_version, cursor);

            // Store the item into the table（值随 raw_set_integer 弹出）
            state.raw_set_integer(table_index, item_ix as i64 + 1);
          }
        }

        return 1;
      }
      default_resp_term_view(state)
    }

    // Map (RESP3 only)
    b'%' if is_resp3(resp_protocol_version) => {
      let mut map_pair_count = 0i32;
      if matches!(
        try_read_signed_map_length(&mut map_pair_count, cursor),
        Ok(true)
      ) && map_pair_count >= 0
      {
        // Response is a two level table, where { map = { ... } }
        state.create_table(0, 1);
        let parent_index = state.get_top() as i32;

        state.push_buffer(ConstantStrings::MAP);
        state.create_table(0, map_pair_count as usize);
        let sub_index = parent_index + 2;

        for _ in 0..map_pair_count {
          // Read key
          _ = process_single_resp_term_view(state, resp_protocol_version, cursor);
          // Read value
          _ = process_single_resp_term_view(state, resp_protocol_version, cursor);

          // Set t[k] = v
          state.raw_set(sub_index);
        }

        // Store the sub-table into the parent table
        state.raw_set(parent_index);
        return 1;
      }
      default_resp_term_view(state)
    }

    // Null (RESP3 only)
    b'_' if is_resp3(resp_protocol_version) => {
      if cursor.len() >= 3 && &cursor[1..3] == b"\r\n" {
        *cursor = &cursor[3..];
        state.push_nil();
        return 1;
      }
      default_resp_term_view(state)
    }

    // Set (RESP3 only)
    b'~' if is_resp3(resp_protocol_version) => {
      let mut set_item_count = 0i32;
      if matches!(
        try_read_signed_set_length(&mut set_item_count, cursor),
        Ok(true)
      ) && set_item_count >= 0
      {
        // Response is a two level table, where { set = { ... } }
        state.create_table(0, 1);
        let parent_index = state.get_top() as i32;

        state.push_buffer(ConstantStrings::SET);
        state.create_table(0, set_item_count as usize);
        let sub_index = parent_index + 2;

        for _ in 0..set_item_count {
          // Read value, which we use as key
          _ = process_single_resp_term_view(state, resp_protocol_version, cursor);
          // Unconditionally the value under the key is true
          state.push_boolean(true);

          // Set t[value] = true
          state.raw_set(sub_index);
        }

        // Store the sub-table into the parent table
        state.raw_set(parent_index);
        return 1;
      }
      default_resp_term_view(state)
    }

    // Boolean (RESP3 only)
    b'#' if is_resp3(resp_protocol_version) => {
      if cursor.len() >= 4 {
        let as_int = &cursor[0..4];
        if as_int == b"#t\r\n" {
          *cursor = &cursor[4..];
          state.push_boolean(true);
          return 1;
        } else if as_int == b"#f\r\n" {
          *cursor = &cursor[4..];
          state.push_boolean(false);
          return 1;
        }
      }
      default_resp_term_view(state)
    }

    // Double (RESP3 only)
    b',' if is_resp3(resp_protocol_version) => {
      if let Some(end_of_double_ix) = find_crlf(cursor) {
        let double_span = &cursor[..end_of_double_ix + 2];
        let body = &double_span[1..double_span.len() - 2];
        let parsed = match body {
          b"inf" => Some(f64::INFINITY),
          b"nan" => Some(f64::NAN),
          b"-inf" => Some(f64::NEG_INFINITY),
          b"-nan" => Some(f64::NAN),
          text => str::from_utf8(text).ok().and_then(|t| t.parse().ok()),
        };
        if let Some(parsed) = parsed {
          *cursor = &cursor[double_span.len()..];

          // Create table like { double = <parsed> }
          state.create_table(0, 1);
          // 对标 C# RawSet(1, curTop + 1)（C# LuaRunner.cs:988）
          let table_index = state.get_top() as i32;
          state.push_buffer(ConstantStrings::DOUBLE);
          state.push_number(parsed);
          state.raw_set(table_index);
          return 1;
        }
      }
      default_resp_term_view(state)
    }

    // Big number (RESP3 only)
    b'(' if is_resp3(resp_protocol_version) => {
      if let Some(end_of_big_num) = find_crlf(cursor) {
        let big_num_span = &cursor[..end_of_big_num + 2];
        if big_num_span.len() >= 4 {
          let big_num_buf = &big_num_span[1..big_num_span.len() - 2];
          if big_num_buf.iter().all(u8::is_ascii_digit) {
            *cursor = &cursor[big_num_span.len()..];

            // Create table like { big_number = <bigNumBuf> }
            state.create_table(0, 1);
            // 对标 C# RawSet(1, curTop + 1)（C# LuaRunner.cs:1044）
            let table_index = state.get_top() as i32;
            state.push_buffer(ConstantStrings::BIG_NUMBER);
            state.push_buffer(big_num_buf);
            state.raw_set(table_index);
            return 1;
          }
        }
      }
      default_resp_term_view(state)
    }

    // Verbatim strings (RESP3 only)
    b'=' if is_resp3(resp_protocol_version) => {
      let mut verbatim_string_length = 0i32;
      if matches!(
        try_read_verbatim_string_length(&mut verbatim_string_length, cursor),
        Ok(true)
      ) && verbatim_string_length >= 4
      {
        let verbatim = verbatim_string_length as usize;
        if cursor.len() >= verbatim + 2 {
          let format = &cursor[0..3];
          let data = &cursor[4..verbatim];

          let advanced = *cursor;
          *cursor = &cursor[verbatim..];
          if &cursor[0..2] != b"\r\n" {
            *cursor = advanced;
            return default_resp_term_view(state);
          }
          *cursor = &cursor[2..];

          // create table like { format = <format>, string = <data> }
          state.create_table(0, 2);
          // 对标 C# RawSet(2, curTop + 1) 两次（C# LuaRunner.cs:1102, 1111）
          let table_index = state.get_top() as i32;

          state.push_buffer(ConstantStrings::FORMAT);
          state.push_buffer(format);
          state.raw_set(table_index);

          state.push_buffer(ConstantStrings::STRING);
          state.push_buffer(data);
          state.raw_set(table_index);

          return 1;
        }
      }
      default_resp_term_view(state)
    }

    _ => default_resp_term_view(state),
  }
}

/// default 分支（意外响应 → UnexpectedError）。
fn default_resp_term_view(state: &mut LuaState) -> i32 {
  log::error!("Unexpected response, this should never happen");
  lua_wrapped_error_view(state, 1, ConstantStrings::UNEXPECTED_ERROR)
}
