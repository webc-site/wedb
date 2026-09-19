use core::str;

use sonic_rs::{JsonContainerTrait, JsonValueMutTrait, Value};
use wcustom::CustomObjectFns;
use wresp::{cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR, ext::RespVecExt, wrong_num_args};

use super::JsonCommands;
use crate::{error::ERR_INVALID_JSON_PATH, json_object::GarnetJsonObject, json_path::JsonPath};

impl JsonCommands {
  pub const JSON_ARRLEN: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_read_only_initial,
    updater: Self::reject_read_only_update,
    reader: Self::json_arrlen_reader,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_ARRAPPEND: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_write_missing,
    updater: Self::json_arrappend_updater,
    reader: Self::reject_write_only,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_ARRPOP: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_write_missing,
    updater: Self::json_arrpop_updater,
    reader: Self::reject_write_only,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_ARRINDEX: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_read_only_initial,
    updater: Self::reject_read_only_update,
    reader: Self::json_arrindex_reader,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_ARRINSERT: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_write_missing,
    updater: Self::json_arrinsert_updater,
    reader: Self::reject_write_only,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_ARRTRIM: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_write_missing,
    updater: Self::json_arrtrim_updater,
    reader: Self::reject_write_only,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  // ---- JSON.ARRLEN ----
  fn json_arrlen_reader(
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
      .map(|m| m.as_array().map(|a| a.len()))
      .collect();
    let out_bytes = sonic_rs::to_vec(&lens).unwrap_or_default();
    output.write_resp_bulk_string(&out_bytes);
    true
  }

  // ---- JSON.ARRAPPEND ----
  fn json_arrappend_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    if args.len() < 2 {
      output.write_resp_error(wrong_num_args!("json.arrappend"));
      return false;
    }
    let path_str = match str::from_utf8(args[0]) {
      Ok(s) => s,
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
        return false;
      }
    };

    let mut values_to_append = Vec::new();
    for &val_bytes in &args[1..] {
      let Ok(v) = sonic_rs::from_slice::<Value>(val_bytes) else {
        output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
        return false;
      };
      values_to_append.push(v);
    }

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
      if let Some(arr) = target.as_array_mut() {
        for v in &values_to_append {
          arr.push(v.clone());
        }
        lens.push(arr.len());
      }
    });

    payload.clear();
    let _ = obj.serialize_object(payload);
    let out_bytes = sonic_rs::to_vec(&lens).unwrap_or_default();
    output.write_resp_bulk_string(&out_bytes);
    true
  }

  // ---- JSON.ARRPOP ----
  fn json_arrpop_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    let (path_str, pop_idx) = if args.is_empty() {
      ("$", -1)
    } else if args.len() == 1 {
      (str::from_utf8(args[0]).unwrap_or("$"), -1)
    } else {
      let p = str::from_utf8(args[0]).unwrap_or("$");
      let idx = str::from_utf8(args[1])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(-1);
      (p, idx)
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

    let mut popped = Vec::new();
    json_path.mutate_recursive(root, 0, &mut |target| {
      if let Some(arr) = target.as_array_mut() {
        if arr.is_empty() {
          popped.push(Value::from(()));
          return;
        }
        let actual_idx = if pop_idx < 0 {
          arr.len() as i64 + pop_idx
        } else {
          pop_idx
        };
        if actual_idx >= 0 && (actual_idx as usize) < arr.len() {
          let item = arr
            .get(actual_idx as usize)
            .cloned()
            .unwrap_or(Value::from(()));
          arr.remove(actual_idx as usize);
          popped.push(item);
        } else {
          popped.push(Value::from(()));
        }
      }
    });

    payload.clear();
    let _ = obj.serialize_object(payload);
    let out_bytes = sonic_rs::to_vec(&popped).unwrap_or_default();
    output.write_resp_bulk_string(&out_bytes);
    true
  }

  // ---- JSON.ARRINDEX ----
  fn json_arrindex_reader(
    payload: &[u8],
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    if args.len() < 2 {
      output.write_resp_error(wrong_num_args!("json.arrindex"));
      return false;
    }
    let path_str = match str::from_utf8(args[0]) {
      Ok(s) => s,
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
        return false;
      }
    };
    let target_val = match sonic_rs::from_slice::<Value>(args[1]) {
      Ok(v) => v,
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
        return false;
      }
    };

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
    let mut indices: Vec<i64> = Vec::new();
    for m in matches {
      if let Some(arr) = m.as_array() {
        let mut idx: i64 = -1;
        for (i, item) in arr.iter().enumerate() {
          if item == &target_val {
            idx = i as i64;
            break;
          }
        }
        indices.push(idx);
      }
    }

    let out_bytes = sonic_rs::to_vec(&indices).unwrap_or_default();
    output.write_resp_bulk_string(&out_bytes);
    true
  }

  // ---- JSON.ARRINSERT ----
  fn json_arrinsert_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    if args.len() < 3 {
      output.write_resp_error(wrong_num_args!("json.arrinsert"));
      return false;
    }
    let path_str = match str::from_utf8(args[0]) {
      Ok(s) => s,
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
        return false;
      }
    };
    let Some(insert_idx) = str::from_utf8(args[1])
      .ok()
      .and_then(|s| s.parse::<i64>().ok())
    else {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return false;
    };

    let mut values_to_insert = Vec::new();
    for &val_bytes in &args[2..] {
      let Ok(v) = sonic_rs::from_slice::<Value>(val_bytes) else {
        output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
        return false;
      };
      values_to_insert.push(v);
    }

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
      if let Some(arr) = target.as_array_mut() {
        let actual_idx = if insert_idx < 0 {
          arr.len() as i64 + insert_idx
        } else {
          insert_idx
        };
        if actual_idx >= 0 && (actual_idx as usize) <= arr.len() {
          for (pos, v) in (actual_idx as usize..).zip(&values_to_insert) {
            arr.insert(pos, v.clone());
          }
          lens.push(arr.len());
        }
      }
    });

    payload.clear();
    let _ = obj.serialize_object(payload);
    let out_bytes = sonic_rs::to_vec(&lens).unwrap_or_default();
    output.write_resp_bulk_string(&out_bytes);
    true
  }

  // ---- JSON.ARRTRIM ----
  fn json_arrtrim_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    if args.len() != 3 {
      output.write_resp_error(wrong_num_args!("json.arrtrim"));
      return false;
    }
    let path_str = match str::from_utf8(args[0]) {
      Ok(s) => s,
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
        return false;
      }
    };
    let Some(start_idx) = str::from_utf8(args[1])
      .ok()
      .and_then(|s| s.parse::<i64>().ok())
    else {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return false;
    };
    let Some(stop_idx) = str::from_utf8(args[2])
      .ok()
      .and_then(|s| s.parse::<i64>().ok())
    else {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return false;
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
      if let Some(arr) = target.as_array_mut() {
        let len = arr.len() as i64;
        let s = if start_idx < 0 {
          len + start_idx
        } else {
          start_idx
        }
        .max(0);
        let e = if stop_idx < 0 {
          len + stop_idx
        } else {
          stop_idx
        }
        .min(len - 1);
        if s > e || s >= len {
          arr.clear();
        } else {
          let mut new_arr = Vec::new();
          for (idx, item) in arr.iter().enumerate() {
            if (idx as i64) >= s && (idx as i64) <= e {
              new_arr.push(item.clone());
            }
          }
          arr.clear();
          for v in new_arr {
            arr.push(v);
          }
        }
        lens.push(arr.len());
      }
    });

    payload.clear();
    let _ = obj.serialize_object(payload);
    let out_bytes = sonic_rs::to_vec(&lens).unwrap_or_default();
    output.write_resp_bulk_string(&out_bytes);
    true
  }
}
