use core::str;

use sonic_rs::Value;
use wcustom::CustomObjectFns;
use wresp::{cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR, ext::RespVecExt, wrong_num_args};

use super::JsonCommands;
use crate::{
  error::{Error, RESP_NEW_OBJECT_AT_ROOT, RESP_WRONG_STATIC_PATH},
  json_object::{ExistOptions, GarnetJsonObject, SetResult},
};

impl JsonCommands {
  pub const JSON_SET: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::json_set_need_initial_update,
    updater: Self::json_set_updater,
    reader: Self::reject_write_only,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_GET: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_read_only_initial,
    updater: Self::reject_read_only_update,
    reader: Self::json_get_reader,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  // ---- JSON.SET ----
  pub(super) fn json_set_need_initial_update(
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    if args.len() < 2 || args.len() > 3 {
      output.write_resp_error(wrong_num_args!("json.set"));
      return false;
    }
    let path = args[0];
    let val_bytes = args[1];

    let mut exist_opts = ExistOptions::None;
    if args.len() == 3 {
      if args[2].eq_ignore_ascii_case(b"NX") {
        exist_opts = ExistOptions::NX;
      } else if args[2].eq_ignore_ascii_case(b"XX") {
        exist_opts = ExistOptions::XX;
      } else {
        output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
        return false;
      }
    }

    if exist_opts == ExistOptions::XX {
      output.write_resp_null_ver(resp_version);
      return false;
    }

    if path != b"$" && !path.is_empty() {
      output.write_resp_error(RESP_NEW_OBJECT_AT_ROOT);
      return false;
    }

    if let Err(e) = sonic_rs::from_slice::<Value>(val_bytes) {
      output.write_resp_error(&e.to_string());
      return false;
    }

    true
  }

  pub(super) fn json_set_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    if args.len() < 2 || args.len() > 3 {
      output.write_resp_error(wrong_num_args!("json.set"));
      return false;
    }
    let path = args[0];
    let val_bytes = args[1];

    let mut exist_opts = ExistOptions::None;
    if args.len() == 3 {
      if args[2].eq_ignore_ascii_case(b"NX") {
        exist_opts = ExistOptions::NX;
      } else if args[2].eq_ignore_ascii_case(b"XX") {
        exist_opts = ExistOptions::XX;
      } else {
        output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
        return false;
      }
    }

    let mut obj = if payload.is_empty() {
      GarnetJsonObject::create()
    } else {
      match GarnetJsonObject::deserialize(&mut &payload[..]) {
        Ok(o) => o,
        Err(_) => {
          output.write_resp_error("ERR JSON object decode failed");
          return false;
        }
      }
    };

    match obj.set(path, val_bytes, exist_opts) {
      Ok(SetResult::Success) => {
        payload.clear();
        let _ = obj.serialize_object(payload);
        output.write_resp_simple_string("OK");
        true
      }
      Ok(SetResult::ConditionNotMet) => {
        output.write_resp_null_ver(resp_version);
        true
      }
      Ok(SetResult::Error(e)) => {
        output.write_resp_error(&e);
        true
      }
      Err(Error::InvalidPath(e)) => {
        output.write_resp_error(&e);
        true
      }
      Err(Error::NewObjectAtRoot) => {
        output.write_resp_error(RESP_NEW_OBJECT_AT_ROOT);
        true
      }
      Err(Error::WrongStaticPath) => {
        output.write_resp_error(RESP_WRONG_STATIC_PATH);
        true
      }
      Err(Error::Sonic(e)) => {
        output.write_resp_error(&e.to_string());
        true
      }
      Err(Error::SyntaxError) => {
        output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
        true
      }
      Err(e) => {
        output.write_resp_error(&e.to_string());
        true
      }
    }
  }

  // ---- JSON.GET ----
  pub(super) fn json_get_reader(
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

    let mut offset = 0;
    let mut indent: Option<&str> = None;
    let mut new_line: Option<&str> = None;
    let mut space: Option<&str> = None;

    while offset < args.len() {
      let opt = args[offset];
      if opt.eq_ignore_ascii_case(b"INDENT") && offset + 1 < args.len() {
        indent = str::from_utf8(args[offset + 1]).ok();
        offset += 2;
      } else if opt.eq_ignore_ascii_case(b"NEWLINE") && offset + 1 < args.len() {
        new_line = str::from_utf8(args[offset + 1]).ok();
        offset += 2;
      } else if opt.eq_ignore_ascii_case(b"SPACE") && offset + 1 < args.len() {
        space = str::from_utf8(args[offset + 1]).ok();
        offset += 2;
      } else {
        break;
      }
    }

    let paths = &args[offset..];
    match obj.try_get(paths, output, indent, new_line, space, resp_version) {
      Ok(_) => true,
      Err(e) => {
        match &e {
          Error::InvalidPath(p) => output.write_resp_error(p),
          Error::Sonic(err) => output.write_resp_error(&err.to_string()),
          _ => output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR),
        }
        true
      }
    }
  }
}
