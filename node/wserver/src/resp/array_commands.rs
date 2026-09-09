use crate::resp::{
  parser::resp_ext::RespVecExt,
  resp_server_session::RespServerSession,
};

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
  pub fn network_msetnx() {
    unimplemented!()
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
  pub fn network_swapdb() {
    unimplemented!()
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
  pub fn network_keys() {
    unimplemented!()
  }
  /// libs/server/Resp/ArrayCommands.cs:NetworkSCAN
  pub fn network_scan() {
    unimplemented!()
  }
  /// libs/server/Resp/ArrayCommands.cs:NetworkTYPE
  pub fn network_type() {
    unimplemented!()
  }
  /// libs/server/Resp/ArrayCommands.cs:WriteOutputForScan
  pub fn write_output_for_scan() {
    unimplemented!()
  }
  /// libs/server/Resp/ArrayCommands.cs:NetworkArrayPING
  pub fn network_array_ping() {
    unimplemented!()
  }
  /// libs/server/Resp/ArrayCommands.cs:NetworkLCS
  pub fn network_lcs() {
    unimplemented!()
  }
}
