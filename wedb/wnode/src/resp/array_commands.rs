use wresp::{
  RespVecExt, cmd_strings as cs,
  cmd_strings::{abort_with_error_message, abort_with_wrong_number_of_arguments},
};

use crate::{
  resp::{
    parser::session_parse_state::{strict_i32, strict_i64},
    resp_server_session::RespServerSession,
  },
  storage::session::storage_session::StorageSession,
};

/// libs/server/Servers/GarnetServerOptions.cs:MaxDatabases
const MAX_DATABASES: i32 = 16;

impl RespServerSession {
  /// libs/server/Resp/ArrayCommands.cs:NetworkDEL
  ///
  /// C# 无 arity 校验（0 参即空循环回 :0），1:1 保留。
  pub fn network_del<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let mut deleted_count = 0i64;
    for key in parse_state {
      match store.try_delete_sync(key) {
        Ok(Ok(deleted)) => deleted_count += deleted as i64,
        // 环形页翻转 / 复合对象元数据：须降级完整异步路由，本次不产生输出
        Ok(Err(_)) => return Ok(false),
        Err(_) => {
          output.write_resp_error("generic error");
          return Ok(true);
        }
      }
    }

    output.write_resp_int(deleted_count);
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkMGET
  ///
  /// C# 无 arity 校验（0 参即 `*0`），1:1 保留。
  pub fn network_mget<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 先收集全部键的读取结果再一次性写出：中途遇须异步裁决的键
    // （Ok(None)）时整体降级，避免已写出的部分应答无法撤回
    let mut vals = Vec::with_capacity(parse_state.len());
    for key in parse_state {
      match store.try_read_sync(key, |v| v.to_vec()) {
        Ok(Some(Some(val))) => vals.push(Some(val)),
        Ok(Some(None)) | Err(_) => vals.push(None),
        Ok(None) => return Ok(false),
      }
    }

    output.write_resp_array_len(vals.len());
    for val in &vals {
      match val {
        Some(val) => output.write_resp_bulk_string(val),
        None => output.write_resp_null(),
      }
    }
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkMSET
  pub fn network_mset<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 || !parse_state.len().is_multiple_of(2) {
      output.write_resp_error("wrong number of arguments for 'MSET' command");
      return Ok(true);
    }

    // 对齐 NetworkSET：Ok(Err(page_id)) 为环形页翻转/异步闭环信号，
    // 吞掉即静默丢写，须整体降级（此时尚未写出任何应答，可安全重试）
    for chunk in parse_state.as_chunks::<2>().0 {
      match store.try_upsert_sync(chunk[0], chunk[1]) {
        Ok(Ok(_)) => {}
        Ok(Err(_)) => return Ok(false),
        Err(_) => {
          output.write_resp_error("generic error");
          return Ok(true);
        }
      }
    }

    output.write_resp_simple_string("OK");
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkMSETNX
  pub fn network_msetnx<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() || !parse_state.len().is_multiple_of(2) {
      output.write_resp_error("wrong number of arguments for 'MSETNX' command");
      return Ok(true);
    }

    // 检查是否有任何键已存在
    for chunk in parse_state.as_chunks::<2>().0 {
      match store.try_read_sync(chunk[0], |_| ()) {
        Ok(Some(Some(_))) => {
          output.write_resp_int(0);
          return Ok(true);
        }
        Ok(Some(None)) | Err(_) => {}
        Ok(None) => return Ok(false),
      }
    }

    // 均不存在，写入所有键值
    for chunk in parse_state.as_chunks::<2>().0 {
      match store.try_upsert_sync(chunk[0], chunk[1]) {
        Ok(Ok(_)) => {}
        Ok(Err(_)) => return Ok(false),
        Err(_) => {
          output.write_resp_error("generic error");
          return Ok(true);
        }
      }
    }

    output.write_resp_int(1);
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkSELECT
  ///
  /// C# 校验序：arity → 整数 → 集群门槛 → MaxDatabases 上界 → 切库；本层为
  /// 存储无关快路径，承接 arity/整数/范围校验（集群门槛与切库由宿主接线）。
  pub fn network_select(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "SELECT");
      return Ok(true);
    }
    // C# TryGetInt 校验（CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER）
    let Some(index) = strict_i32(parse_state[0]) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };
    // 负下标或超出 MaxDatabases 上界均越界（C# RESP_ERR_DB_INDEX_OUT_OF_RANGE）
    if !(0..MAX_DATABASES).contains(&index) {
      abort_with_error_message(output, cs::RESP_ERR_DB_INDEX_OUT_OF_RANGE);
      return Ok(true);
    }
    // Assuming successfully switched to DB
    output.extend_from_slice(cs::RESP_OK);
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkSWAPDB
  ///
  /// C# 校验序：arity → 集群门槛 → 两个整数（invalid first/second DB index）
  /// → 负下标与 MaxDatabases 上界 → 实际换库；本层承接前四面的错误口径
  ///（集群门槛与换库执行由宿主接线）。
  pub fn network_swapdb(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      abort_with_wrong_number_of_arguments(output, "SWAPDB");
      return Ok(true);
    }
    let Some(idx1) = strict_i32(parse_state[0]) else {
      abort_with_error_message(output, cs::RESP_ERR_INVALID_FIRST_DB_INDEX);
      return Ok(true);
    };
    let Some(idx2) = strict_i32(parse_state[1]) else {
      abort_with_error_message(output, cs::RESP_ERR_INVALID_SECOND_DB_INDEX);
      return Ok(true);
    };
    if !(0..MAX_DATABASES).contains(&idx1) || !(0..MAX_DATABASES).contains(&idx2) {
      abort_with_error_message(output, cs::RESP_ERR_DB_INDEX_OUT_OF_RANGE);
      return Ok(true);
    }
    output.extend_from_slice(cs::RESP_OK);
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkDBSIZE
  pub fn network_dbsize(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if !parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "DBSIZE");
      return Ok(true);
    }
    // 全库扫描无法在同步快路径完成，对标 C# 降级异步路径执行
    Ok(false)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkKEYS
  pub fn network_keys(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "KEYS");
      return Ok(true);
    }
    // 键空间扫描无法在同步快路径完成，降级异步路径执行
    Ok(false)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkSCAN
  pub fn network_scan(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "SCAN");
      return Ok(true);
    }
    let Some(cursor) = strict_i64(parse_state[0]) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_INVALIDCURSOR);
      return Ok(true);
    };
    if cursor < 0 {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_INVALIDCURSOR);
      return Ok(true);
    }

    let mut token_idx = 1;
    while token_idx < parse_state.len() {
      let param = parse_state[token_idx];
      token_idx += 1;

      if param.eq_ignore_ascii_case(b"MATCH") {
        if token_idx >= parse_state.len() {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        }
        token_idx += 1;
      } else if param.eq_ignore_ascii_case(b"COUNT") {
        if token_idx >= parse_state.len() {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        }
        if strict_i64(parse_state[token_idx]).is_none() {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return Ok(true);
        }
        token_idx += 1;
      } else if param.eq_ignore_ascii_case(b"TYPE") {
        if token_idx >= parse_state.len() {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        }
        token_idx += 1;
      } else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
    }

    // 参数校验通过后，游标扫描降级异步路径执行
    Ok(false)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkTYPE
  pub fn network_type<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      output.write_resp_error("wrong number of arguments for 'TYPE' command");
      return Ok(true);
    }
    let key = parse_state[0];
    match store.try_read_sync(key, |_| ()) {
      Ok(Some(Some(_))) => {
        output.write_resp_simple_string("string");
      }
      Ok(Some(None)) | Err(_) => {
        output.write_resp_simple_string("none");
      }
      Ok(None) => return Ok(false),
    }
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:WriteOutputForScan
  pub fn write_output_for_scan(cursor_value: i64, keys: &[&[u8]], output: &mut Vec<u8>) {
    output.write_resp_array_len(2);
    let mut cur_buf = itoa::Buffer::new();
    let cur_str = cur_buf.format(cursor_value);
    output.write_resp_bulk_string(cur_str.as_bytes());
    output.write_resp_array_len(keys.len());
    for key in keys {
      output.write_resp_bulk_string(key);
    }
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkArrayPING
  pub fn network_array_ping(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() > 1 {
      output.write_resp_error("wrong number of arguments for 'PING' command");
      return Ok(true);
    }
    if parse_state.is_empty() {
      output.write_resp_simple_string("PONG");
    } else {
      output.write_resp_bulk_string(parse_state[0]);
    }
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkLCS
  pub fn network_lcs<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      output.write_resp_error("wrong number of arguments for 'LCS' command");
      return Ok(true);
    }
    let key1 = parse_state[0];
    let key2 = parse_state[1];

    let mut len_only = false;
    let mut with_idx = false;
    let mut min_match_len = 0usize;
    let mut with_match_len = false;

    let mut idx = 2;
    while idx < parse_state.len() {
      let opt = parse_state[idx];
      if opt.eq_ignore_ascii_case(b"LEN") {
        len_only = true;
      } else if opt.eq_ignore_ascii_case(b"IDX") {
        with_idx = true;
      } else if opt.eq_ignore_ascii_case(b"MINMATCHLEN") {
        idx += 1;
        if idx >= parse_state.len() {
          cs::write_error_raw(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        }
        // C# TryGetInt（有符号）：负值钳 0 而非报错
        let Some(min_len) = strict_i32(parse_state[idx]) else {
          cs::write_error_raw(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return Ok(true);
        };
        min_match_len = min_len.max(0) as usize;
      } else if opt.eq_ignore_ascii_case(b"WITHMATCHLEN") {
        with_match_len = true;
      } else {
        cs::write_error_raw(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
      idx += 1;
    }

    if len_only && with_idx {
      cs::write_error_raw(
        output,
        "ERR If you want both the length and indexes, please just use IDX.",
      );
      return Ok(true);
    }

    let s1 = match store.try_read_sync(key1, |v| v.to_vec()) {
      Ok(Some(Some(v))) => Some(v),
      Ok(Some(None)) | Err(_) => None,
      Ok(None) => return Ok(false),
    };
    let s2 = match store.try_read_sync(key2, |v| v.to_vec()) {
      Ok(Some(Some(v))) => Some(v),
      Ok(Some(None)) | Err(_) => None,
      Ok(None) => return Ok(false),
    };

    let resp3 = self.resp_protocol_version >= 3;
    match (s1.as_deref(), s2.as_deref()) {
      (Some(v1), Some(v2)) => {
        if len_only {
          let len = StorageSession::<D>::compute_lcs_length(v1, v2, min_match_len);
          output.write_resp_int(len as i64);
        } else if with_idx {
          let (total, matches) =
            StorageSession::<D>::compute_lcs_with_indices(v1, v2, min_match_len);
          StorageSession::<D>::write_lcs_matches(&matches, with_match_len, total, output, resp3);
        } else {
          let lcs = StorageSession::<D>::compute_lcs(v1, v2, min_match_len);
          output.write_resp_bulk_string(&lcs);
        }
      }
      _ => {
        if len_only {
          output.write_resp_int(0);
        } else if with_idx {
          StorageSession::<D>::write_lcs_matches(&[], with_match_len, 0, output, resp3);
        } else {
          output.write_resp_bulk_string(b"");
        }
      }
    }
    Ok(true)
  }
}
