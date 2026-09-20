use core::str;

use sonic_rs::JsonContainerTrait;
use wcustom::CustomObjectFns;
use wresp::ext::RespVecExt;

use super::JsonCommands;
use crate::{json_object::GarnetJsonObject, json_path::JsonPath};

impl JsonCommands {
  pub const JSON_TYPE: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_read_only_initial,
    updater: Self::reject_read_only_update,
    reader: Self::json_type_reader,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_OBJKEYS: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_read_only_initial,
    updater: Self::reject_read_only_update,
    reader: Self::json_objkeys_reader,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_OBJLEN: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_read_only_initial,
    updater: Self::reject_read_only_update,
    reader: Self::json_objlen_reader,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  // ---- JSON.TYPE ----
  fn json_type_reader(
    payload: &[u8],
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    let obj = match GarnetJsonObject::deserialize(&mut &payload[..]) {
      Ok(o) => o,
      Err(_) => {
        output.write_resp_null_ver(resp_version);
        return true;
      }
    };

    let path = args.first().copied();
    match obj.type_of(path) {
      Some(types) => {
        if types.len() == 1 && (path.is_none() || path == Some(b"$") || path == Some(b"")) {
          output.write_resp_simple_string(types[0]);
        } else {
          output.write_resp_array_len(types.len());
          for t in types {
            output.write_resp_simple_string(t);
          }
        }
      }
      None => output.write_resp_null_ver(resp_version),
    }
    true
  }

  // ---- JSON.OBJKEYS ----
  fn json_objkeys_reader(
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
    let mut key_lists: Vec<Option<Vec<&str>>> = Vec::with_capacity(matches.len());
    for m in matches {
      if let Some(o) = m.as_object() {
        key_lists.push(Some(o.iter().map(|(k, _)| k).collect()));
      } else {
        key_lists.push(None);
      }
    }

    let out_bytes = sonic_rs::to_vec(&key_lists).unwrap_or_default();
    output.write_resp_bulk_string(&out_bytes);
    true
  }

  // ---- JSON.OBJLEN ----
  fn json_objlen_reader(
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
    let lens: Vec<Option<usize>> = matches
      .into_iter()
      .map(|m| m.as_object().map(|o| o.len()))
      .collect();
    let out_bytes = sonic_rs::to_vec(&lens).unwrap_or_default();
    output.write_resp_bulk_string(&out_bytes);
    true
  }
}
