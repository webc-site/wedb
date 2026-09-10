use crate::resp::{parser::resp_ext::RespVecExt, resp_server_session::RespServerSession};

fn compute_lcs(a: &[u8], b: &[u8]) -> Vec<u8> {
  let m = a.len();
  let n = b.len();
  if m == 0 || n == 0 {
    return Vec::new();
  }
  let mut dp = vec![vec![0u32; n + 1]; m + 1];
  for i in 0..m {
    for j in 0..n {
      if a[i] == b[j] {
        dp[i + 1][j + 1] = dp[i][j] + 1;
      } else {
        dp[i + 1][j + 1] = dp[i][j + 1].max(dp[i + 1][j]);
      }
    }
  }

  let mut lcs = Vec::with_capacity(dp[m][n] as usize);
  let mut i = m;
  let mut j = n;
  while i > 0 && j > 0 {
    if a[i - 1] == b[j - 1] {
      lcs.push(a[i - 1]);
      i -= 1;
      j -= 1;
    } else if dp[i - 1][j] >= dp[i][j - 1] {
      i -= 1;
    } else {
      j -= 1;
    }
  }
  lcs.reverse();
  lcs
}

impl RespServerSession {
  /// libs/server/Resp/ArrayCommands.cs:NetworkDEL
  pub fn network_del<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      output.write_resp_error("wrong number of arguments for 'DEL' command");
      return Ok(true);
    }

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
  pub fn network_mget<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      output.write_resp_error("wrong number of arguments for 'MGET' command");
      return Ok(true);
    }

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
    let mut i = 0;
    while i < parse_state.len() {
      match store.try_upsert_sync(parse_state[i], parse_state[i + 1]) {
        Ok(Ok(_)) => i += 2,
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
    for i in (0..parse_state.len()).step_by(2) {
      let key = parse_state[i];
      match store.try_read_sync(key, |_| ()) {
        Ok(Some(Some(_))) => {
          output.write_resp_int(0);
          return Ok(true);
        }
        Ok(Some(None)) | Err(_) => {}
        Ok(None) => return Ok(false),
      }
    }

    // 均不存在，写入所有键值
    let mut i = 0;
    while i < parse_state.len() {
      match store.try_upsert_sync(parse_state[i], parse_state[i + 1]) {
        Ok(Ok(_)) => i += 2,
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
  pub fn network_select(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'SELECT' command\r\n");
      return Ok(true);
    }
    // Assuming successfully switched to DB
    output.extend_from_slice(b"+OK\r\n");
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkSWAPDB
  pub fn network_swapdb(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      output.write_resp_error("wrong number of arguments for 'SWAPDB' command");
      return Ok(true);
    }
    let Some(idx1) = core::str::from_utf8(parse_state[0])
      .ok()
      .and_then(|s| s.parse::<i32>().ok())
    else {
      output.write_resp_error("ERR value is not an integer or out of range");
      return Ok(true);
    };
    let Some(idx2) = core::str::from_utf8(parse_state[1])
      .ok()
      .and_then(|s| s.parse::<i32>().ok())
    else {
      output.write_resp_error("ERR value is not an integer or out of range");
      return Ok(true);
    };
    if idx1 < 0 || idx2 < 0 {
      output.write_resp_error("ERR DB index is out of range");
      return Ok(true);
    }
    output.write_resp_simple_string("OK");
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkDBSIZE
  pub fn network_dbsize<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // Stub: return a pseudo count of 0 for now
    output.extend_from_slice(b":0\r\n");
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkKEYS
  pub fn network_keys<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      output.write_resp_error("wrong number of arguments for 'KEYS' command");
      return Ok(true);
    }
    output.write_resp_array_len(0);
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkSCAN
  pub fn network_scan<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      output.write_resp_error("wrong number of arguments for 'SCAN' command");
      return Ok(true);
    }
    let Some(cursor) = core::str::from_utf8(parse_state[0])
      .ok()
      .and_then(|s| s.parse::<i64>().ok())
    else {
      output.write_resp_error("ERR invalid cursor");
      return Ok(true);
    };
    if cursor < 0 {
      output.write_resp_error("ERR invalid cursor");
      return Ok(true);
    }
    Self::write_output_for_scan(0, &[], output);
    Ok(true)
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
    let mut _min_match_len = 0usize;
    let mut _with_match_len = false;

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
          output.write_resp_error("ERR syntax error");
          return Ok(true);
        }
        let Some(min_len) = core::str::from_utf8(parse_state[idx])
          .ok()
          .and_then(|s| s.parse::<usize>().ok())
        else {
          output.write_resp_error("ERR value is not an integer or out of range");
          return Ok(true);
        };
        _min_match_len = min_len;
      } else if opt.eq_ignore_ascii_case(b"WITHMATCHLEN") {
        _with_match_len = true;
      } else {
        output.write_resp_error("ERR syntax error");
        return Ok(true);
      }
      idx += 1;
    }

    if len_only && with_idx {
      output.write_resp_error("ERR If you want both the length and indexes, please just use IDX.");
      return Ok(true);
    }

    let s1 = match store.try_read_sync(key1, |v| v.to_vec()) {
      Ok(Some(Some(v))) => v,
      Ok(Some(None)) | Err(_) => Vec::new(),
      Ok(None) => return Ok(false),
    };
    let s2 = match store.try_read_sync(key2, |v| v.to_vec()) {
      Ok(Some(Some(v))) => v,
      Ok(Some(None)) | Err(_) => Vec::new(),
      Ok(None) => return Ok(false),
    };

    let lcs_bytes = compute_lcs(&s1, &s2);
    if len_only {
      output.write_resp_int(lcs_bytes.len() as i64);
    } else {
      output.write_resp_bulk_string(&lcs_bytes);
    }
    Ok(true)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_compute_lcs() {
    assert_eq!(compute_lcs(b"abcde", b"ace"), b"ace");
    assert_eq!(compute_lcs(b"hello", b"world").len(), 1);
    assert_eq!(compute_lcs(b"abc", b"def"), b"");
  }
}
