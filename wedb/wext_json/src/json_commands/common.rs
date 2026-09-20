use core::str;

use wresp::{
  cmd_strings::{RESP_ERR_COMMAND_READ_ONLY, RESP_ERR_COMMAND_WRITE_ONLY},
  ext::RespVecExt,
};

use super::JsonCommands;

impl JsonCommands {
  /// modules/GarnetJSON/JsonCommands.cs:NeedInitialUpdate
  pub fn need_initial_update(args: &[&[u8]], output: &mut Vec<u8>, resp_version: u8) -> bool {
    Self::json_set_need_initial_update(args, output, resp_version)
  }

  /// modules/GarnetJSON/JsonCommands.cs:Updater
  pub fn updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    Self::json_set_updater(payload, args, output, resp_version)
  }

  /// modules/GarnetJSON/JsonCommands.cs:Reader
  pub fn reader(payload: &[u8], args: &[&[u8]], output: &mut Vec<u8>, resp_version: u8) -> bool {
    Self::json_get_reader(payload, args, output, resp_version)
  }

  /// modules/GarnetJSON/JsonCommands.cs:AbortWithErrorMessage
  pub fn abort_with_error_message(output: &mut Vec<u8>, msg: &[u8]) -> bool {
    output.write_resp_error(str::from_utf8(msg).unwrap_or("ERR error"));
    false
  }

  // ---- 共用闸口 helper（各族命令常量组合钩子时复用，禁在族文件里各抄一份）----

  pub(super) fn payload_is_empty(payload: &[u8]) -> bool {
    payload.is_empty()
  }

  pub(super) fn not_found_null(_args: &[&[u8]], output: &mut Vec<u8>, resp_version: u8) {
    output.write_resp_null_ver(resp_version);
  }

  pub(super) fn reject_read_only_initial(
    _args: &[&[u8]],
    output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
    output.write_resp_error(RESP_ERR_COMMAND_READ_ONLY);
    false
  }

  pub(super) fn reject_read_only_update(
    _payload: &mut Vec<u8>,
    _args: &[&[u8]],
    output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
    output.write_resp_error(RESP_ERR_COMMAND_READ_ONLY);
    false
  }

  pub(super) fn reject_write_only(
    _payload: &[u8],
    _args: &[&[u8]],
    output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
    output.write_resp_error(RESP_ERR_COMMAND_WRITE_ONLY);
    false
  }

  pub(super) fn reject_write_missing(
    _args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    output.write_resp_null_ver(resp_version);
    false
  }
}
