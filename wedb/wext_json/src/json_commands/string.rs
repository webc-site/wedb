use core::str;

use sonic_rs::{JsonValueTrait, Value};
use wcustom::CustomObjectFns;
use wresp::{cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR, ext::RespVecExt, wrong_num_args};

use super::JsonCommands;
use crate::{error::ERR_INVALID_JSON_PATH, json_object::GarnetJsonObject, json_path::JsonPath};

impl JsonCommands {
  pub const JSON_STRAPPEND: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_write_missing,
    updater: Self::json_strappend_updater,
    reader: Self::reject_write_only,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_STRLEN: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_read_only_initial,
    updater: Self::reject_read_only_update,
    reader: Self::json_strlen_reader,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  // ---- JSON.STRAPPEND ----
  fn json_strappend_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    if args.is_empty() || args.len() > 2 {
      output.write_resp_error(wrong_num_args!("json.strappend"));
      return false;
    }
    let (path_str, append_val) = if args.len() == 2 {
      (str::from_utf8(args[0]).unwrap_or("$"), args[1])
    } else {
      ("$", args[0])
    };

    let append_str = match sonic_rs::from_slice::<&str>(append_val) {
      Ok(s) => s,
      Err(_) => match str::from_utf8(append_val) {
        Ok(s) => s,
        Err(_) => {
          output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
          return false;
        }
      },
    };

    let mut obj = match GarnetJsonObject::deserialize(&mut &payload[..]) {
      Ok(o) => o,
      Err(_) => {
        output.write_resp_null_ver(resp_version);
        return true;
      }
    };

    let Some(root) = obj.root_node.as_mut() else {
      output.write_resp_null_ver(resp_version);
      return true;
    };

    let Ok(json_path) = JsonPath::parse(path_str) else {
      output.write_resp_error(ERR_INVALID_JSON_PATH);
      return true;
    };

    let mut lens = Vec::new();
    json_path.mutate_recursive(root, 0, &mut |target| {
      if let Some(s) = target.as_str() {
        let mut new_s = String::with_capacity(s.len() + append_str.len());
        new_s.push_str(s);
        new_s.push_str(append_str);
        lens.push(new_s.len());
        *target = Value::from(new_s.as_str());
      }
    });

    payload.clear();
    let _ = obj.serialize_object(payload);
    let out_bytes = sonic_rs::to_vec(&lens).unwrap_or_default();
    output.write_resp_bulk_string(&out_bytes);
    true
  }

  // ---- JSON.STRLEN ----
  fn json_strlen_reader(
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
      .map(|m| m.as_str().map(|s| s.len()))
      .collect();
    let out_bytes = sonic_rs::to_vec(&lens).unwrap_or_default();
    output.write_resp_bulk_string(&out_bytes);
    true
  }
}
