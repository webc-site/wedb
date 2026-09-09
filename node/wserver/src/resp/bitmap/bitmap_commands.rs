use std::str;

use crate::resp::resp_server_session::RespServerSession;

impl RespServerSession {
  pub fn network_string_set_bit<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 3 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'SETBIT' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let offset_str = str::from_utf8(parse_state[1]).unwrap_or("");
    let bit_str = str::from_utf8(parse_state[2]).unwrap_or("");

    let offset = match offset_str.parse::<usize>() {
      Ok(o) => o,
      Err(_) => {
        output.extend_from_slice(b"-ERR bit offset is not an integer or out of range\r\n");
        return Ok(true);
      }
    };
    let bit = match bit_str.parse::<u8>() {
      Ok(b) if b == 0 || b == 1 => b,
      _ => {
        output.extend_from_slice(b"-ERR bit is not an integer or out of range\r\n");
        return Ok(true);
      }
    };

    let status = store.try_read_sync(key, |v| v.to_vec());
    let mut val = match status {
      Ok(Some(Some(v))) => v,
      _ => Vec::new(),
    };

    let byte_idx = offset / 8;
    let bit_idx = 7 - (offset % 8);

    if byte_idx >= val.len() {
      val.resize(byte_idx + 1, 0);
    }

    let old_byte = val[byte_idx];
    let old_bit = (old_byte >> bit_idx) & 1;

    if bit == 1 {
      val[byte_idx] |= 1 << bit_idx;
    } else {
      val[byte_idx] &= !(1 << bit_idx);
    }

    let _ = store.try_upsert_sync(key, &val);

    let count_str = format!(":{}\r\n", old_bit);
    output.extend_from_slice(count_str.as_bytes());
    Ok(true)
  }

  pub fn network_string_get_bit<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'GETBIT' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let offset_str = str::from_utf8(parse_state[1]).unwrap_or("");

    let offset = match offset_str.parse::<usize>() {
      Ok(o) => o,
      Err(_) => {
        output.extend_from_slice(b"-ERR bit offset is not an integer or out of range\r\n");
        return Ok(true);
      }
    };

    let status = store.try_read_sync(key, |v| v.to_vec());
    let bit = match status {
      Ok(Some(Some(val))) => {
        let byte_idx = offset / 8;
        let bit_idx = 7 - (offset % 8);
        if byte_idx < val.len() {
          (val[byte_idx] >> bit_idx) & 1
        } else {
          0
        }
      }
      _ => 0,
    };

    let count_str = format!(":{}\r\n", bit);
    output.extend_from_slice(count_str.as_bytes());
    Ok(true)
  }

  pub fn network_string_bit_count<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'BITCOUNT' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];

    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut start = 0;
        let mut end = val.len() as isize - 1;

        if parse_state.len() >= 3 {
          let start_str = str::from_utf8(parse_state[1]).unwrap_or("");
          let end_str = str::from_utf8(parse_state[2]).unwrap_or("");
          if let Ok(s) = start_str.parse::<isize>() {
            start = if s < 0 { val.len() as isize + s } else { s };
          }
          if let Ok(e) = end_str.parse::<isize>() {
            end = if e < 0 { val.len() as isize + e } else { e };
          }
        }

        let mut count = 0;
        let start_idx = start.max(0) as usize;
        let end_idx = end.min(val.len() as isize - 1).max(0) as usize;

        if start_idx <= end_idx && start_idx < val.len() {
          for byte in &val[start_idx..=end_idx.min(val.len() - 1)] {
            count += byte.count_ones();
          }
        }

        let count_str = format!(":{}\r\n", count);
        output.extend_from_slice(count_str.as_bytes());
      }
      Ok(Some(None)) | Ok(None) => output.extend_from_slice(b":0\r\n"),
      Err(_) => output.extend_from_slice(b"-ERR generic error\r\n"),
    }
    Ok(true)
  }

  pub fn network_string_bit_position() {
    unimplemented!()
  }
  pub fn network_string_bit_operation() {
    unimplemented!()
  }
  pub fn string_bit_field() {
    unimplemented!()
  }
  pub fn string_bit_field_read_only() {
    unimplemented!()
  }
  pub fn string_bit_field_action() {
    unimplemented!()
  }
  pub fn handle_first_sub_command() {
    unimplemented!()
  }
}
