use crate::resp::{
  parser::resp_ext::{RespSliceExt, RespVecExt},
  resp_server_session::RespServerSession,
};

/// 字符串类命令负载上限（libs/server/Resp/Bitmap/BitmapManager.cs:MaxBitmapPayloadBytes）
const MAX_STRING_PAYLOAD_BYTES: usize = 512 * 1024 * 1024;
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER
const ERR_NOT_INTEGER: &str = "ERR value is not an integer or out of range.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_OFFSETOUTOFRANGE
const ERR_OFFSET_OUT_OF_RANGE: &str = "ERR offset is out of range";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_STRING_EXCEEDS_MAX_SIZE
const ERR_STRING_EXCEEDS_MAX: &str = "ERR string exceeds maximum allowed size (proto-max-bulk-len)";

impl RespServerSession {
  /// libs/server/Resp/BasicCommands.cs:GetPendingScratchOutput
  pub fn get_pending_scratch_output<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkGET
  pub fn network_get<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      return Ok(false);
    }
    let key = parse_state[0];
    match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(val))) => {
        output.write_resp_bulk_string(&val);
      }
      Ok(Some(None)) => {
        output.extend_from_slice(b"$-1\r\n");
      }
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkGETEX
  pub fn network_getex<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      output.write_resp_error("wrong number of arguments for 'GETEX' command");
      return Ok(true);
    }
    let key = parse_state[0];
    match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(val))) => {
        output.write_resp_bulk_string(&val);
      }
      Ok(Some(None)) => {
        output.extend_from_slice(b"$-1\r\n");
      }
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkGETAsync
  pub fn network_get_async<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkGET_SG
  pub fn network_get_sg<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkGETSET
  /// libs/server/Resp/BasicCommands.cs:NetworkSET
  pub fn network_set<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      output.write_resp_error("wrong number of arguments for 'SET' command");
      return Ok(true);
    }
    let key = parse_state[0];
    let value = parse_state[1];
    match store.try_upsert_sync(key, value) {
      Ok(Ok(_)) => output.write_resp_simple_string("OK"),
      Ok(Err(_)) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }

  pub fn network_getset<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkSetRange
  pub fn network_set_range<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 3 {
      output.write_resp_error("wrong number of arguments for 'SETRANGE' command");
      return Ok(true);
    }
    let key = parse_state[0];
    // 对标 C#：偏移须为可解析整数，负值越界报错，offset + value 不得越过
    // 512MB 负载上限（全程 i64 口径，杜绝 usize 溢出 panic）
    let Some(offset) = parse_state[1].try_parse_i64() else {
      output.write_resp_error(ERR_NOT_INTEGER);
      return Ok(true);
    };
    let val = parse_state[2];
    if offset < 0 {
      output.write_resp_error(ERR_OFFSET_OUT_OF_RANGE);
      return Ok(true);
    }
    if offset as u64 + val.len() as u64 > MAX_STRING_PAYLOAD_BYTES as u64 {
      output.write_resp_error(ERR_STRING_EXCEEDS_MAX);
      return Ok(true);
    }
    let offset = offset as usize;

    match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(mut existing))) => {
        if offset + val.len() > existing.len() {
          existing.resize(offset + val.len(), 0);
        }
        existing[offset..offset + val.len()].copy_from_slice(val);
        match store.try_upsert_sync(key, &existing) {
          Ok(Ok(_)) => output.write_resp_int(existing.len() as i64),
          Ok(Err(_)) => return Ok(false),
          Err(_) => output.write_resp_error("generic error"),
        }
      }
      Ok(Some(None)) => {
        let mut new_val = vec![0; offset + val.len()];
        new_val[offset..offset + val.len()].copy_from_slice(val);
        match store.try_upsert_sync(key, &new_val) {
          Ok(Ok(_)) => output.write_resp_int(new_val.len() as i64),
          Ok(Err(_)) => return Ok(false),
          Err(_) => output.write_resp_error("generic error"),
        }
      }
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkGetRange
  pub fn network_get_range<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 3 {
      output.write_resp_error("wrong number of arguments for 'GETRANGE' command");
      return Ok(true);
    }
    // 对标 C#：start/end 须可解析为整数，否则报 value is not an integer
    let Some(mut start) = parse_state[1].try_parse_i64() else {
      output.write_resp_error(ERR_NOT_INTEGER);
      return Ok(true);
    };
    let Some(mut end) = parse_state[2].try_parse_i64() else {
      output.write_resp_error(ERR_NOT_INTEGER);
      return Ok(true);
    };
    let key = parse_state[0];

    match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(val))) => {
        let len = val.len() as i64;
        // 负下标归一化用 saturating_add：调用方可传任意 i64，杜绝溢出 panic
        if start < 0 {
          start = start.saturating_add(len);
        }
        if end < 0 {
          end = end.saturating_add(len);
        }
        if start < 0 {
          start = 0;
        }
        if end >= len {
          end = len - 1;
        }

        if start > end || start >= len {
          output.write_resp_bulk_string(b"");
        } else {
          output.write_resp_bulk_string(&val[(start as usize)..=(end as usize)]);
        }
      }
      Ok(Some(None)) => output.write_resp_bulk_string(b""),
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkSETEX
  pub fn network_setex<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 3 {
      output.write_resp_error("wrong number of arguments for 'SETEX' command");
      return Ok(true);
    }
    let key = parse_state[0];
    let val = parse_state[2];
    // 对齐 NetworkSET：异步闭环信号须整体降级，吞掉即静默丢写
    match store.try_upsert_sync(key, val) {
      Ok(Ok(_)) => output.write_resp_simple_string("OK"),
      Ok(Err(_)) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkSETNX
  pub fn network_setnx<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      output.write_resp_error("wrong number of arguments for 'SETNX' command");
      return Ok(true);
    }
    let key = parse_state[0];
    let val = parse_state[1];

    match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(_))) => output.write_resp_int(0),
      Ok(Some(None)) => match store.try_upsert_sync(key, val) {
        Ok(Ok(_)) => output.write_resp_int(1),
        Ok(Err(_)) => return Ok(false),
        Err(_) => output.write_resp_error("generic error"),
      },
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkSETEXNX
  pub fn network_setexnx<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkSET_EX
  pub fn network_set_ex<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkSET_Conditional
  pub fn network_set__conditional<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkIncrement
  pub fn network_increment<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      output.write_resp_error("wrong number of arguments for command");
      return Ok(true);
    }
    output.write_resp_int(1); // Stub
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkIncrementByFloat
  pub fn network_increment_by_float<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      output.write_resp_error("wrong number of arguments for command");
      return Ok(true);
    }
    output.extend_from_slice(b"+1.0\r\n"); // Stub
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkAppend
  pub fn network_append<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      output.write_resp_error("wrong number of arguments for 'APPEND' command");
      return Ok(true);
    }
    let key = parse_state[0];
    let val = parse_state[1];

    match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(mut existing))) => {
        existing.extend_from_slice(val);
        match store.try_upsert_sync(key, &existing) {
          Ok(Ok(_)) => output.write_resp_int(existing.len() as i64),
          Ok(Err(_)) => return Ok(false),
          Err(_) => output.write_resp_error("generic error"),
        }
      }
      Ok(Some(None)) => match store.try_upsert_sync(key, val) {
        Ok(Ok(_)) => output.write_resp_int(val.len() as i64),
        Ok(Err(_)) => return Ok(false),
        Err(_) => output.write_resp_error("generic error"),
      },
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkPING
  pub fn network_ping(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      output.extend_from_slice(b"+PONG\r\n");
    } else {
      let msg = parse_state[0];
      output.write_resp_bulk_string(msg);
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkASKING
  pub fn network_asking<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkQUIT
  pub fn network_quit(
    &mut self,
    _parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_simple_string("OK");
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkFLUSHDB
  pub fn network_flushdb(
    &mut self,
    _parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_simple_string("OK");
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkFLUSHALL
  pub fn network_flushall<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkREADONLY
  pub fn network_readonly<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkREADWRITE
  pub fn network_readwrite<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkSTRLEN
  pub fn network_strlen<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      output.write_resp_error("wrong number of arguments for 'STRLEN' command");
      return Ok(true);
    }
    let key = parse_state[0];

    match store.try_read_sync(key, |v| v.len()) {
      Ok(Some(Some(len))) => output.write_resp_int(len as i64),
      Ok(Some(None)) | Ok(None) => output.write_resp_int(0),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:WriteCOMMANDResponse
  pub fn write_command_response<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND
  pub fn network_command<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_COUNT
  pub fn network_command_count<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_DOCS
  pub fn network_command_docs<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_INFO
  pub fn network_command_info<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_GETKEYS
  pub fn network_command_getkeys<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_GETKEYSANDFLAGS
  pub fn network_command_getkeysandflags<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkECHO
  pub fn network_echo(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      output.write_resp_error("wrong number of arguments for 'ECHO' command");
      return Ok(true);
    }
    let msg = parse_state[0];
    output.write_resp_bulk_string(msg);
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkHELLO
  pub fn network_hello<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkTIME
  pub fn network_time(
    &mut self,
    _parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // Stub: return a mocked timestamp
    output.extend_from_slice(b"*2\r\n$10\r\n1700000000\r\n$6\r\n000000\r\n");
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkAUTH
  pub fn network_auth<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkMemoryUsage
  pub fn network_memory_usage<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkOBJECT
  pub fn network_object<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkOBJECTHELP
  pub fn network_objecthelp<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkASYNC
  pub fn network_async<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:ProcessHelloCommand
  pub fn process_hello_command<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:FlushDb
  pub fn flush_db<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:ExecuteFlushDb
  pub fn execute_flush_db<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:WriteClientInfo
  pub fn write_client_info<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:ParseGETAndKey
  pub fn parse_get_and_key<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NextCommandMaybeGet
  pub fn next_command_maybe_get<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:TryGetSimpleCommandInfo
  pub fn try_get_simple_command_info<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:SetResult
  pub fn set_result<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("not implemented");
    Ok(true)
  }
}
