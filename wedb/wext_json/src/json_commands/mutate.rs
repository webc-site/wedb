use core::str;

use sonic_rs::{JsonValueMutTrait, JsonValueTrait, Value};
use wcustom::CustomObjectFns;
use wresp::{cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR, ext::RespVecExt, wrong_num_args};

use super::JsonCommands;
use crate::{
  error::{ERR_INVALID_JSON_PATH, ERR_NUMBER_NOT_VALID_FLOAT},
  json_object::GarnetJsonObject,
  json_path::{JsonPath, val_from_f64},
};

impl JsonCommands {
  pub const JSON_DEL: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::json_del_need_initial_update,
    updater: Self::json_del_updater,
    reader: Self::reject_write_only,
    not_found: Self::json_del_not_found,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_NUMINCRBY: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_write_missing,
    updater: Self::json_numincrby_updater,
    reader: Self::reject_write_only,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_NUMMULTBY: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_write_missing,
    updater: Self::json_nummultby_updater,
    reader: Self::reject_write_only,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_TOGGLE: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_write_missing,
    updater: Self::json_toggle_updater,
    reader: Self::reject_write_only,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_CLEAR: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_write_missing,
    updater: Self::json_clear_updater,
    reader: Self::reject_write_only,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  // ---- JSON.DEL ----
  fn json_del_need_initial_update(
    _args: &[&[u8]],
    output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
    output.write_resp_int(0);
    false
  }

  fn json_del_not_found(_args: &[&[u8]], output: &mut Vec<u8>, _resp_version: u8) {
    output.write_resp_int(0);
  }

  fn json_del_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
    if payload.is_empty() {
      output.write_resp_int(0);
      return true;
    }

    let mut obj = match GarnetJsonObject::deserialize(&mut &payload[..]) {
      Ok(o) => o,
      Err(_) => {
        output.write_resp_int(0);
        return true;
      }
    };

    let path = args.first().copied();
    let count = obj.del(path);
    payload.clear();
    let _ = obj.serialize_object(payload);
    output.write_resp_int(count as i64);
    true
  }

  // ---- JSON.NUMINCRBY ----
  fn json_numincrby_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    if args.len() != 2 {
      output.write_resp_error(wrong_num_args!("json.numincrby"));
      return false;
    }
    let path_str = match str::from_utf8(args[0]) {
      Ok(s) => s,
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
        return false;
      }
    };
    let Ok(inc) = str::from_utf8(args[1]).map(|s| s.parse::<f64>()) else {
      output.write_resp_error(ERR_NUMBER_NOT_VALID_FLOAT);
      return false;
    };
    let Ok(inc) = inc else {
      output.write_resp_error(ERR_NUMBER_NOT_VALID_FLOAT);
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

    let mut results = Vec::new();
    json_path.mutate_recursive(root, 0, &mut |target| {
      if let Some(i) = target.as_i64() {
        let new_val = i as f64 + inc;
        if new_val.fract() == 0.0 {
          *target = Value::from(new_val as i64);
        } else {
          *target = val_from_f64(new_val);
        }
        results.push(target.clone());
      } else if let Some(f) = target.as_f64() {
        let new_val = f + inc;
        *target = val_from_f64(new_val);
        results.push(target.clone());
      }
    });

    payload.clear();
    let _ = obj.serialize_object(payload);
    let out_bytes = sonic_rs::to_vec(&results).unwrap_or_default();
    output.write_resp_bulk_string(&out_bytes);
    true
  }

  // ---- JSON.NUMMULTBY ----
  fn json_nummultby_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    if args.len() != 2 {
      output.write_resp_error(wrong_num_args!("json.nummultby"));
      return false;
    }
    let path_str = match str::from_utf8(args[0]) {
      Ok(s) => s,
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
        return false;
      }
    };
    let Ok(mult) = str::from_utf8(args[1]).map(|s| s.parse::<f64>()) else {
      output.write_resp_error(ERR_NUMBER_NOT_VALID_FLOAT);
      return false;
    };
    let Ok(mult) = mult else {
      output.write_resp_error(ERR_NUMBER_NOT_VALID_FLOAT);
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

    let mut results = Vec::new();
    json_path.mutate_recursive(root, 0, &mut |target| {
      if let Some(i) = target.as_i64() {
        let new_val = i as f64 * mult;
        if new_val.fract() == 0.0 {
          *target = Value::from(new_val as i64);
        } else {
          *target = val_from_f64(new_val);
        }
        results.push(target.clone());
      } else if let Some(f) = target.as_f64() {
        let new_val = f * mult;
        *target = val_from_f64(new_val);
        results.push(target.clone());
      }
    });

    payload.clear();
    let _ = obj.serialize_object(payload);
    let out_bytes = sonic_rs::to_vec(&results).unwrap_or_default();
    output.write_resp_bulk_string(&out_bytes);
    true
  }

  // ---- JSON.TOGGLE ----
  fn json_toggle_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    let path_str = args
      .first()
      .and_then(|p| str::from_utf8(p).ok())
      .unwrap_or("$");
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

    let mut results = Vec::new();
    json_path.mutate_recursive(root, 0, &mut |target| {
      if let Some(b) = target.as_bool() {
        *target = Value::from(!b);
        results.push(!b);
      }
    });

    payload.clear();
    let _ = obj.serialize_object(payload);
    let out_bytes = sonic_rs::to_vec(&results).unwrap_or_default();
    output.write_resp_bulk_string(&out_bytes);
    true
  }

  // ---- JSON.CLEAR ----
  fn json_clear_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
    let path_str = args
      .first()
      .and_then(|p| str::from_utf8(p).ok())
      .unwrap_or("$");
    let mut obj = match GarnetJsonObject::deserialize(&mut &payload[..]) {
      Ok(o) => o,
      Err(_) => {
        output.write_resp_int(0);
        return true;
      }
    };

    let Some(root) = obj.root_node.as_mut() else {
      output.write_resp_int(0);
      return true;
    };

    let Ok(json_path) = JsonPath::parse(path_str) else {
      output.write_resp_int(0);
      return true;
    };

    let mut cleared_count = 0;
    json_path.mutate_recursive(root, 0, &mut |target| {
      if let Some(arr) = target.as_array_mut() {
        if !arr.is_empty() {
          arr.clear();
          cleared_count += 1;
        }
      } else if let Some(o) = target.as_object_mut() {
        if !o.is_empty() {
          o.clear();
          cleared_count += 1;
        }
      } else if let Some(i) = target.as_i64() {
        if i != 0 {
          *target = Value::from(0);
          cleared_count += 1;
        }
      } else if let Some(f) = target.as_f64()
        && f != 0.0
      {
        *target = val_from_f64(0.0);
        cleared_count += 1;
      }
    });

    payload.clear();
    let _ = obj.serialize_object(payload);
    output.write_resp_int(cleared_count);
    true
  }
}
