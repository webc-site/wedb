use core::str;

use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};
use wcustom::CustomObjectFns;
use wresp::{ext::RespVecExt, resp_memory_writer::format_double};
use zmij::Buffer;

use super::JsonCommands;
use crate::{json_object::GarnetJsonObject, json_path::JsonPath};

impl JsonCommands {
  pub const JSON_RESP: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_read_only_initial,
    updater: Self::reject_read_only_update,
    reader: Self::json_resp_reader,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  // ---- JSON.RESP ----
  fn json_resp_reader(
    payload: &[u8],
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    let path_str = args
      .first()
      .and_then(|p| str::from_utf8(p).ok())
      .unwrap_or("$");
    let obj = match GarnetJsonObject::deserialize(&mut &payload[..]) {
      Ok(o) => o,
      Err(_) => {
        output.write_resp_null_ver(resp_version);
        return true;
      }
    };

    let Some(root) = obj.root_node.as_ref() else {
      output.write_resp_null_ver(resp_version);
      return true;
    };

    let Ok(json_path) = JsonPath::parse(path_str) else {
      output.write_resp_null_ver(resp_version);
      return true;
    };

    let matches = json_path.evaluate(root);
    if matches.is_empty() {
      output.write_resp_null_ver(resp_version);
      return true;
    }

    if matches.len() == 1 {
      encode_val_resp(matches[0], output, resp_version);
    } else {
      output.write_resp_array_len(matches.len());
      for m in matches {
        encode_val_resp(m, output, resp_version);
      }
    }
    true
  }
}

fn encode_val_resp(val: &Value, output: &mut Vec<u8>, resp_version: u8) {
  if val.is_null() {
    output.write_resp_null_ver(resp_version);
  } else if let Some(b) = val.as_bool() {
    output.write_resp_simple_string(if b { "true" } else { "false" });
  } else if let Some(i) = val.as_i64() {
    output.write_resp_int(i);
  } else if let Some(f) = val.as_f64() {
    let mut buf = Buffer::new();
    output.write_resp_bulk_string(format_double(f, &mut buf).as_bytes());
  } else if let Some(s) = val.as_str() {
    output.write_resp_bulk_string(s.as_bytes());
  } else if let Some(arr) = val.as_array() {
    output.write_resp_array_len(arr.len());
    for item in arr.iter() {
      encode_val_resp(item, output, resp_version);
    }
  } else if let Some(obj) = val.as_object() {
    output.write_resp_array_len(obj.len() * 2);
    for (k, v) in obj.iter() {
      output.write_resp_bulk_string(k.as_bytes());
      encode_val_resp(v, output, resp_version);
    }
  }
}
