use core::str;

use wcustom::CustomObjectFns;
use wresp::{
  cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR,
  ext::RespVecExt,
  options::{ExistOptions, try_get_exist_options},
  wrong_num_args,
};

use super::JsonCommands;
use crate::{
  error::{Error, RESP_NEW_OBJECT_AT_ROOT, RESP_WRONG_STATIC_PATH},
  json_object::{GarnetJsonObject, SetResult, parse_dom},
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
      match try_get_exist_options(args[2]) {
        Some(opt) => exist_opts = opt,
        None => {
          output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
          return false;
        }
      }
    }

    if exist_opts == ExistOptions::Xx {
      output.write_resp_null_ver(resp_version);
      return false;
    }

    // 缺键建根门仅认 "$"（对标 C# Set :358 根替换形）：空路径 "" 缺键回
    // RESP_NEW_OBJECT_AT_ROOT 错误帧且不落库（对齐 C#/RedisJSON 拒空路径建根，
    // 守 §19 缺键不落空壳红线），杜绝 rust 旧形「"" 同走根分支建根回 OK」。
    if path != b"$" {
      output.write_resp_error(RESP_NEW_OBJECT_AT_ROOT);
      return false;
    }

    if let Err(e) = parse_dom(val_bytes) {
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
      match try_get_exist_options(args[2]) {
        Some(opt) => exist_opts = opt,
        None => {
          output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
          return false;
        }
      }
    }

    // 空载荷 = 键缺失新建：from_slice 内已短路为 create()，不再另判
    let mut obj = match GarnetJsonObject::from_slice(payload) {
      Ok(o) => o,
      Err(_) => {
        output.write_resp_error("ERR JSON object decode failed");
        return false;
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
    let obj = match GarnetJsonObject::from_slice(payload) {
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

    while let [opt, val, ..] = &args[offset..] {
      let slot = if opt.eq_ignore_ascii_case(b"INDENT") {
        &mut indent
      } else if opt.eq_ignore_ascii_case(b"NEWLINE") {
        &mut new_line
      } else if opt.eq_ignore_ascii_case(b"SPACE") {
        &mut space
      } else {
        break;
      };
      *slot = str::from_utf8(val).ok();
      offset += 2;
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
