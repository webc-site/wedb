//! RESP 命令层共享错误文案与应答写出辅助（对标 libs/server/Resp/CmdStrings.cs）
//!
//! `write_resp_error`（见 [`super::parser::resp_ext::RespVecExt`]）固定前置 `-ERR `，
//! 而 C# 大量错误常量自带 `ERR`/`WRONGTYPE` 等完整前缀（经 RespWriteUtils.
//! TryWriteError 以 `-<msg>\r\n` 原样写出），故此处提供不加工前缀的原样写出。

use super::parser::resp_ext::RespVecExt;

/// libs/server/Resp/CmdStrings.cs:RESP_OK
pub const RESP_OK: &[u8] = b"+OK\r\n";
/// libs/server/Resp/CmdStrings.cs:RESP_PONG
pub const RESP_PONG: &[u8] = b"+PONG\r\n";
/// libs/server/Resp/CmdStrings.cs:RESP_EMPTYLIST
pub const RESP_EMPTYLIST: &[u8] = b"*0\r\n";
/// libs/server/Resp/CmdStrings.cs:RESP_RETURN_VAL_0
pub const RESP_RETURN_VAL_0: &[u8] = b":0\r\n";
/// libs/server/Resp/CmdStrings.cs:RESP_RETURN_VAL_1
pub const RESP_RETURN_VAL_1: &[u8] = b":1\r\n";
/// libs/server/Resp/CmdStrings.cs:RESP_RETURN_VAL_N1
pub const RESP_RETURN_VAL_N1: &[u8] = b":-1\r\n";
/// libs/server/Resp/CmdStrings.cs:RESP_RETURN_VAL_N2
pub const RESP_RETURN_VAL_N2: &[u8] = b":-2\r\n";

/// libs/server/Resp/CmdStrings.cs:RESP_ERR_NOAUTH
pub const RESP_ERR_NOAUTH: &str = "NOAUTH Authentication required.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_WRONG_TYPE
pub const RESP_ERR_WRONG_TYPE: &str =
  "WRONGTYPE Operation against a key holding the wrong kind of value.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_UNK_CMD
pub const RESP_ERR_GENERIC_UNK_CMD: &str = "ERR unknown command";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_CLUSTER_DISABLED
pub const RESP_ERR_GENERIC_CLUSTER_DISABLED: &str =
  "ERR This instance has cluster support disabled";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_NOSUCHKEY
pub const RESP_ERR_GENERIC_NOSUCHKEY: &str = "ERR no such key";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_INVALIDEXP_IN_SET
pub const RESP_ERR_GENERIC_INVALIDEXP_IN_SET: &str = "ERR invalid expire time in 'set' command";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_SYNTAX_ERROR
pub const RESP_ERR_GENERIC_SYNTAX_ERROR: &str = "ERR syntax error";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_NAN_INFINITY_INCR
pub const RESP_ERR_GENERIC_NAN_INFINITY_INCR: &str = "ERR increment would produce NaN or Infinity";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_OFFSETOUTOFRANGE
pub const RESP_ERR_GENERIC_OFFSETOUTOFRANGE: &str = "ERR offset is out of range";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER
pub const RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER: &str =
  "ERR value is not an integer or out of range.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE
pub const RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE: &str =
  "ERR value is out of range, must be positive.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER
pub const RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER: &str = "ERR bit is not an integer or out of range";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER
pub const RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER: &str =
  "ERR bit offset is not an integer or out of range";
/// SELECT/SWAPDB 族的整数解析失败文案（无句点变体，本仓库多域复用）
pub const RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER_NO_PERIOD: &str =
  "ERR value is not an integer or out of range";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_PROTOCOL_VALUE_IS_NOT_INTEGER
pub const RESP_ERR_PROTOCOL_VALUE_IS_NOT_INTEGER: &str =
  "ERR Protocol version is not an integer or out of range.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_UNSUPPORTED_PROTOCOL_VERSION
pub const RESP_ERR_UNSUPPORTED_PROTOCOL_VERSION: &str = "ERR Unsupported protocol version";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_NOT_VALID_FLOAT
pub const RESP_ERR_NOT_VALID_FLOAT: &str = "ERR value is not a valid float";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_TIMEOUT_NOT_VALID_FLOAT
pub const RESP_ERR_TIMEOUT_NOT_VALID_FLOAT: &str = "ERR timeout is not a float or out of range";
/// libs/server/Resp/CmdStrings.cs:RESP_WRONGPASS_INVALID_PASSWORD
pub const RESP_WRONGPASS_INVALID_PASSWORD: &str = "WRONGPASS Invalid password";
/// libs/server/Resp/CmdStrings.cs:RESP_WRONGPASS_INVALID_USERNAME_PASSWORD
pub const RESP_WRONGPASS_INVALID_USERNAME_PASSWORD: &str =
  "WRONGPASS Invalid username/password combination";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_BUSSYKEY
pub const RESP_ERR_BUSSYKEY: &str = "BUSYKEY Target key name already exists.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_INVALID_EXPIRE_TIME
pub const RESP_ERR_INVALID_EXPIRE_TIME: &str = "ERR invalid expire time, must be >= 0";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_HCOLLECT_ALREADY_IN_PROGRESS
pub const RESP_ERR_HCOLLECT_ALREADY_IN_PROGRESS: &str = "ERR HCOLLECT scan already in progress";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_ZCOLLECT_ALREADY_IN_PROGRESS
pub const RESP_ERR_ZCOLLECT_ALREADY_IN_PROGRESS: &str = "ERR ZCOLLECT scan already in progress";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_OBJECT_FREQ_UNSUPPORTED
pub const RESP_ERR_OBJECT_FREQ_UNSUPPORTED: &str = "ERR OBJECT FREQ is not supported: Garnet does not track access frequency (no LFU maxmemory policy).";
/// libs/server/Resp/CmdStrings.cs:RESP_INVALID_COMMAND_SPECIFIED
pub const RESP_INVALID_COMMAND_SPECIFIED: &str = "Invalid command specified";
/// libs/server/Resp/CmdStrings.cs:RESP_COMMAND_HAS_NO_KEY_ARGS
pub const RESP_COMMAND_HAS_NO_KEY_ARGS: &str = "The command has no key arguments";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_NOT_SUPPORTED_RESP2
pub const RESP_ERR_NOT_SUPPORTED_RESP2: &str = "ERR command not supported in RESP2";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_ASYNC_PROTOCOL_CHANGE
pub const RESP_ERR_ASYNC_PROTOCOL_CHANGE: &str =
  "ERR protocol change is not allowed with pending async operations";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_CANNOT_LIST_CLIENTS
pub const RESP_ERR_CANNOT_LIST_CLIENTS: &str = "ERR Clients cannot be listed.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_UBLOCKING_CLINET
pub const RESP_ERR_UBLOCKING_CLINET: &str = "ERR Unable to unblock client because of error.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_NO_SUCH_CLIENT
pub const RESP_ERR_NO_SUCH_CLIENT: &str = "ERR No such client";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_INVALID_CLIENT_ID
pub const RESP_ERR_INVALID_CLIENT_ID: &str = "ERR Invalid client ID";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_INVALID_CLIENT_NAME
pub const RESP_ERR_INVALID_CLIENT_NAME: &str =
  "ERR Client names cannot contain spaces, newlines or special characters.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_INVALID_CLIENT_UNBLOCK_REASON
pub const RESP_ERR_INVALID_CLIENT_UNBLOCK_REASON: &str =
  "ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_EXPDELSCAN_INVALID
pub const RESP_ERR_EXPDELSCAN_INVALID: &str =
  "ERR Cannot execute EXPDELSCAN with background expired key deletion scan enabled";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_CHECKPOINT_ALREADY_IN_PROGRESS
pub const RESP_ERR_CHECKPOINT_ALREADY_IN_PROGRESS: &str = "ERR checkpoint already in progress";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_DB_INDEX_OUT_OF_RANGE
pub const RESP_ERR_DB_INDEX_OUT_OF_RANGE: &str = "ERR DB index is out of range.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_DB_ID_CLUSTER_MODE
pub const RESP_ERR_DB_ID_CLUSTER_MODE: &str =
  "ERR specifying non-zero DBID is not allowed in cluster mode";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_FLUSHALL_READONLY_REPLICA
pub const RESP_ERR_FLUSHALL_READONLY_REPLICA: &str =
  "ERR You can't write against a read only replica.";
/// libs/server/Resp/CmdStrings.cs:GenericErrWrongNumArgs
pub const GENERIC_ERR_WRONG_NUM_ARGS: &str = "ERR wrong number of arguments for '{0}' command";
/// LMPOP/SMPOP/BZMPOP 等命令的 numkeys 校验文案（跨 list/set/sortedset 三域复用）
pub const RESP_ERR_GENERIC_NUMKEYS: &str = "ERR numkeys should be greater than 0";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_INVALIDCURSOR
pub const RESP_ERR_GENERIC_INVALIDCURSOR: &str = "ERR invalid cursor";
/// libs/server/Objects/GarnetObjectBase.cs 对象命令不支持的通用文案
/// （跨 hash/set/list/sortedset 四对象域复用）
pub const RESP_ERR_GENERIC_UNSUPPORTED_OPERATION: &str = "ERR unsupported operation";
/// libs/server/Resp/CmdStrings.cs:GenericErrUnsupportedOption
pub const GENERIC_ERR_UNSUPPORTED_OPTION: &str = "ERR Unsupported option {0}";
/// libs/server/Resp/CmdStrings.cs:GenericErrUnknownSubCommand
pub const GENERIC_ERR_UNKNOWN_SUB_COMMAND: &str = "ERR unknown subcommand '{0}'. Try {1} HELP";
/// libs/server/Resp/CmdStrings.cs:GenericErrUnknownSubCommandNoHelp
pub const GENERIC_ERR_UNKNOWN_SUB_COMMAND_NO_HELP: &str = "ERR unknown subcommand '{0}'.";
/// libs/server/Resp/CmdStrings.cs:GenericErrUnknownSubCommandOrWrongNumberOfArguments
pub const GENERIC_ERR_UNKNOWN_SUB_COMMAND_OR_WRONG_NUM_ARGS: &str =
  "ERR unknown subcommand or wrong number of arguments for '{0}'. Try {1} HELP";
/// libs/server/Resp/CmdStrings.cs:GenericErrCommandDisallowedWithOption
pub const GENERIC_ERR_COMMAND_DISALLOWED_WITH_OPTION: &str = "ERR {0} command not allowed. If the {1} option is set to \"local\", you can run it from a local connection, otherwise you need to set this option in the configuration file, and then restart the server.";
/// libs/server/Resp/CmdStrings.cs:GenericUnknownClientType
pub const GENERIC_UNKNOWN_CLIENT_TYPE: &str = "ERR Unknown client type '{0}'";
/// libs/server/Resp/CmdStrings.cs:GenericErrDuplicateFilter
pub const GENERIC_ERR_DUPLICATE_FILTER: &str = "ERR Filter '{0}' defined multiple times";
/// libs/server/Resp/CmdStrings.cs:GenericErrShouldBeGreaterThanZero
pub const GENERIC_ERR_SHOULD_BE_GREATER_THAN_ZERO: &str = "ERR {0} should be greater than 0";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_INSTANTIATING_CLASS
pub const RESP_ERR_GENERIC_INSTANTIATING_CLASS: &str =
  "ERR unable to instantiate one or more classes from given assemblies.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_REGISTERCS_UNSUPPORTED_CLASS
pub const RESP_ERR_GENERIC_REGISTERCS_UNSUPPORTED_CLASS: &str =
  "ERR unable to register one or more unsupported classes.";
/// libs/server/Resp/CmdStrings.cs:GenericErrUnknownOptionConfigSet
pub const GENERIC_ERR_UNKNOWN_OPTION_CONFIG_SET: &str =
  "ERR Unknown option or number of arguments for CONFIG SET - '{0}'";

/// 原样写出错误行 `-<msg>\r\n`（msg 自带 `ERR`/`WRONGTYPE` 等完整前缀，
/// 对标 libs/common/RespWriteUtils.cs:TryWriteError）
#[inline]
pub fn write_error_raw(output: &mut Vec<u8>, msg: &str) {
  output.push(b'-');
  output.extend_from_slice(msg.as_bytes());
  output.extend_from_slice(b"\r\n");
}

/// 以 `GenericErrWrongNumArgs` 格式化命令名并写出错误应答
pub fn abort_with_wrong_number_of_arguments(output: &mut Vec<u8>, cmd_name: &str) {
  write_error_raw(output, &GENERIC_ERR_WRONG_NUM_ARGS.replace("{0}", cmd_name));
}

/// 原样写出错误应答并终止命令处理
pub fn abort_with_error_message(output: &mut Vec<u8>, error_message: &str) {
  write_error_raw(output, error_message);
}

/// 以 `+PONG\r\n` 等已含类型前缀的原始字节帧写出应答
#[inline]
pub fn write_raw(output: &mut Vec<u8>, frame: &[u8]) {
  output.extend_from_slice(frame);
}

/// RESP2 口径写 map 头（RESP2 分支：map 退化为双倍长度数组）
#[inline]
pub fn write_map_len_resp2(output: &mut Vec<u8>, len: usize) {
  output.write_resp_array_len(len * 2);
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn write_error_raw_frames_message() {
    let mut out = Vec::new();
    write_error_raw(&mut out, RESP_ERR_GENERIC_NOSUCHKEY);
    assert_eq!(out, b"-ERR no such key\r\n");
  }

  #[test]
  fn wrong_num_args_formats_command_name() {
    let mut out = Vec::new();
    abort_with_wrong_number_of_arguments(&mut out, "GETEX");
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'GETEX' command\r\n"
    );
  }

  #[test]
  fn unknown_subcommand_formats() {
    // C# GenericErrUnknownSubCommandNoHelp 自带句号;GenericErrUnknownSubCommand
    // 以 "Try <父命令> HELP" 收尾 —— 逐字节对齐 CmdStrings.cs
    assert_eq!(
      GENERIC_ERR_UNKNOWN_SUB_COMMAND_NO_HELP.replace("{0}", "NOPE"),
      "ERR unknown subcommand 'NOPE'."
    );
    assert_eq!(
      GENERIC_ERR_UNKNOWN_SUB_COMMAND
        .replace("{0}", "NOPE")
        .replace("{1}", "CLUSTER"),
      "ERR unknown subcommand 'NOPE'. Try CLUSTER HELP"
    );
  }

  #[test]
  fn map_len_resp2_doubles_array_length() {
    let mut out = Vec::new();
    write_map_len_resp2(&mut out, 2);
    assert_eq!(out, b"*4\r\n");
  }

  #[test]
  fn raw_frames_passthrough() {
    let mut out = Vec::new();
    write_raw(&mut out, RESP_OK);
    write_raw(&mut out, RESP_RETURN_VAL_N2);
    assert_eq!(out, b"+OK\r\n:-2\r\n");
  }
}
