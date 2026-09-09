//! 常量字符串基建（对标 libs/server/Lua/LuaRunner.Strings.cs:ConstantStringRegistryIndexes）。
//!
//! C# 把高频字符串预载进 VM Registry 换取零拷贝；Rust 侧宿主回调可直接以
//! `&'static [u8]` 建串，故常量表以静态字节承载；[`LuaRunner_Strings::constant_string_to_registry`]
//! 保留 1:1 注册语义（压栈 + TryRef），供注册表索引形态的调用方使用。

use super::lua_state_wrapper::LuaStateWrapper;

/// 高频常量串（字段名对齐 C# ConstantStringRegistryIndexes 属性）。
pub struct ConstantStrings;

impl ConstantStrings {
  /// <see cref="CmdStrings.LUA_OK"/>
  pub const OK: &[u8] = b"OK";
  /// <see cref="CmdStrings.LUA_ok"/>
  pub const OK_LOWER: &[u8] = b"ok";
  /// <see cref="CmdStrings.LUA_err"/>
  pub const ERR: &[u8] = b"err";
  /// <see cref="CmdStrings.LUA_No_session_available"/>
  pub const NO_SESSION_AVAILABLE: &[u8] = b"No session available";
  /// <see cref="CmdStrings.LUA_ERR_Please_specify_at_least_one_argument_for_this_redis_lib_call"/>
  pub const PLEASE_SPECIFY_REDIS_CALL: &[u8] =
    b"ERR Please specify at least one argument for this redis lib call";
  /// <see cref="CmdStrings.RESP_ERR_NOPERM"/>
  pub const ERR_NO_PERM: &[u8] = b"NOPERM this user has no permissions to run the command";
  /// <see cref="CmdStrings.LUA_ERR_Unknown_Redis_command_called_from_script"/>
  pub const ERR_UNKNOWN: &[u8] = b"ERR Unknown Redis command called from script";
  /// <see cref="CmdStrings.RESP_ERR_GENERIC_UNK_CMD"/>
  ///
  /// 通用未知命令错误文本（ProcessSingleRespTerm 的特判比对串）。
  pub const RESP_ERR_GENERIC_UNK_CMD: &[u8] = b"ERR unknown command";
  /// <see cref="CmdStrings.LUA_ERR_Lua_redis_lib_command_arguments_must_be_strings_or_integers"/>
  pub const ERR_BAD_ARG: &[u8] = b"ERR Lua redis lib command arguments must be strings or integers";
  /// <see cref="CmdStrings.LUA_ERR_wrong_number_of_arguments"/>
  pub const ERR_WRONG_NUMBER_OF_ARGS: &[u8] = b"ERR wrong number of arguments";
  /// <see cref="CmdStrings.LUA_ERR_redis_log_requires_two_arguments_or_more"/>
  pub const ERR_REDIS_LOG_REQUIRED: &[u8] = b"ERR redis.log() requires two arguments or more.";
  /// <see cref="CmdStrings.LUA_ERR_First_argument_must_be_a_number_log_level"/>
  pub const ERR_FIRST_ARG_MUST_BE_NUMBER: &[u8] =
    b"ERR First argument must be a number (log level).";
  /// <see cref="CmdStrings.LUA_ERR_Invalid_debug_level"/>
  pub const ERR_INVALID_DEBUG_LEVEL: &[u8] = b"ERR Invalid debug level.";
  /// <see cref="CmdStrings.LUA_ERR_Invalid_command_passed_to_redis_acl_check_cmd"/>
  pub const ERR_INVALID_COMMAND: &[u8] = b"ERR Invalid command passed to redis.acl_check_cmd()";
  /// <see cref="CmdStrings.LUA_ERR_redis_setresp_requires_one_argument"/>
  pub const ERR_REDIS_SETRESP_ARG: &[u8] = b"ERR redis.setresp() requires one argument.";
  /// <see cref="CmdStrings.LUA_ERR_RESP_version_must_be_2_or_3"/>
  pub const ERR_RESP_VERSION: &[u8] = b"ERR RESP version must be 2 or 3.";
  /// <see cref="CmdStrings.LUA_ERR_redis_log_disabled"/>
  pub const ERR_LOGGING_DISABLED: &[u8] = b"ERR redis.log(...) disabled in Garnet config";
  /// <see cref="CmdStrings.LUA_double"/>
  pub const DOUBLE: &[u8] = b"double";
  /// <see cref="CmdStrings.LUA_map"/>
  pub const MAP: &[u8] = b"map";
  /// <see cref="CmdStrings.Lua_set"/>
  pub const SET: &[u8] = b"set";
  /// <see cref="CmdStrings.LUA_big_number"/>
  pub const BIG_NUMBER: &[u8] = b"big_number";
  /// <see cref="CmdStrings.LUA_format"/>
  pub const FORMAT: &[u8] = b"format";
  /// <see cref="CmdStrings.LUA_string"/>
  pub const STRING: &[u8] = b"string";
  /// <see cref="CmdStrings.LUA_bad_arg_atan2"/>
  pub const BAD_ARG_ATAN2: &[u8] = b"bad argument to atan2";
  /// <see cref="CmdStrings.LUA_bad_arg_cosh"/>
  pub const BAD_ARG_COSH: &[u8] = b"bad argument to cosh";
  /// <see cref="CmdStrings.LUA_bad_arg_frexp"/>
  pub const BAD_ARG_FREXP: &[u8] = b"bad argument to frexp";
  /// <see cref="CmdStrings.LUA_bad_arg_ldexp"/>
  pub const BAD_ARG_LDEXP: &[u8] = b"bad argument to ldexp";
  /// <see cref="CmdStrings.LUA_bad_arg_log10"/>
  pub const BAD_ARG_LOG10: &[u8] = b"bad argument to log10";
  /// <see cref="CmdStrings.LUA_bad_arg_pow"/>
  pub const BAD_ARG_POW: &[u8] = b"bad argument to pow";
  /// <see cref="CmdStrings.LUA_bad_arg_sinh"/>
  pub const BAD_ARG_SINH: &[u8] = b"bad argument to sinh";
  /// <see cref="CmdStrings.LUA_bad_arg_tanh"/>
  pub const BAD_ARG_TANH: &[u8] = b"bad argument to tanh";
  /// <see cref="CmdStrings.LUA_bad_arg_maxn"/>
  pub const BAD_ARG_MAXN: &[u8] = b"bad argument to maxn";
  /// <see cref="CmdStrings.LUA_bad_arg_loadstring"/>
  pub const BAD_ARG_LOAD_STRING: &[u8] = b"bad argument to loadstring";
  /// <see cref="CmdStrings.LUA_bad_arg_loadstring_null_byte"/>
  pub const BAD_ARG_LOAD_STRING_NULL_BYTE: &[u8] =
    b"bad argument to loadstring, interior null byte";
  /// <see cref="CmdStrings.LUA_bad_arg_tobit"/>
  pub const BAD_ARG_TO_BIT: &[u8] = b"bad argument to tobit";
  /// <see cref="CmdStrings.LUA_bad_arg_tohex"/>
  pub const BAD_ARG_TO_HEX: &[u8] = b"bad argument to tohex";
  /// <see cref="CmdStrings.LUA_bad_arg_bswap"/>
  pub const BAD_ARG_BSWAP: &[u8] = b"bad argument to bswap";
  /// <see cref="CmdStrings.LUA_bad_arg_bnot"/>
  pub const BAD_ARG_BNOT: &[u8] = b"bad argument to bnot";
  /// <see cref="CmdStrings.LUA_bad_arg_encode"/>
  pub const BAD_ARG_ENCODE: &[u8] = b"bad argument to encode";
  /// <see cref="CmdStrings.LUA_bad_arg_decode"/>
  pub const BAD_ARG_DECODE: &[u8] = b"bad argument to decode";
  /// <see cref="CmdStrings.LUA_bad_arg_pack"/>
  pub const BAD_ARG_PACK: &[u8] = b"bad argument to pack";
  /// <see cref="CmdStrings.LUA_bad_arg_unpack"/>
  pub const BAD_ARG_UNPACK: &[u8] = b"bad argument to unpack";
  /// <see cref="CmdStrings.LUA_bad_arg_format"/>
  pub const BAD_ARG_FORMAT: &[u8] = b"bad argument to format";
  /// <see cref="CmdStrings.LUA_bad_arg_bor"/>
  pub const BAD_ARG_BOR: &[u8] = b"bad argument to bor";
  /// <see cref="CmdStrings.LUA_bad_arg_band"/>
  pub const BAD_ARG_BAND: &[u8] = b"bad argument to band";
  /// <see cref="CmdStrings.LUA_bad_arg_bxor"/>
  pub const BAD_ARG_BXOR: &[u8] = b"bad argument to bxor";
  /// <see cref="CmdStrings.LUA_bad_arg_lshift"/>
  pub const BAD_ARG_LSHIFT: &[u8] = b"bad argument to lshift";
  /// <see cref="CmdStrings.LUA_bad_arg_rshift"/>
  pub const BAD_ARG_RSHIFT: &[u8] = b"bad argument to rshift";
  /// <see cref="CmdStrings.LUA_bad_arg_arshift"/>
  pub const BAD_ARG_ARSHIFT: &[u8] = b"bad argument to arshift";
  /// <see cref="CmdStrings.LUA_bad_arg_rol"/>
  pub const BAD_ARG_ROL: &[u8] = b"bad argument to rol";
  /// <see cref="CmdStrings.LUA_bad_arg_ror"/>
  pub const BAD_ARG_ROR: &[u8] = b"bad argument to ror";
  /// <see cref="CmdStrings.LUA_unexpected_json_value_kind"/>
  pub const UNEXPECTED_JSON_VALUE_KIND: &[u8] = b"Unexpected json value kind";
  /// <see cref="CmdStrings.LUA_cannot_serialise_to_json"/>
  pub const CANNOT_SERIALISE_TO_JSON: &[u8] = b"Cannot serialise Lua type to JSON";
  /// <see cref="CmdStrings.LUA_unexpected_error"/>
  pub const UNEXPECTED_ERROR: &[u8] = b"Unexpected Lua error";
  /// <see cref="CmdStrings.LUA_cannot_serialise_excessive_nesting"/>
  pub const CANNOT_SERIALISE_NESTING: &[u8] = b"Cannot serialise, excessive nesting (1001)";
  /// <see cref="CmdStrings.LUA_unable_to_format_number"/>
  pub const UNABLE_TO_FORMAT_NUMBER: &[u8] = b"Unable to format number";
  /// <see cref="CmdStrings.LUA_found_too_many_nested"/>
  pub const FOUND_TOO_MANY_NESTED: &[u8] = b"Found too many nested data structures (1001)";
  /// <see cref="CmdStrings.LUA_expected_value_but_found_invalid"/>
  pub const EXPECTED_VALUE_BUT_FOUND: &[u8] = b"Expected value but found invalid token.";
  /// <see cref="CmdStrings.LUA_missing_bytes_in_input"/>
  pub const MISSING_BYTES_IN_INPUT: &[u8] = b"Missing bytes in input.";
  /// <see cref="CmdStrings.LUA_unexpected_msgpack_sigil"/>
  pub const UNEXPECTED_MSGPACK_SIGIL: &[u8] = b"Unexpected MsgPack sigil";
  /// <see cref="CmdStrings.LUA_msgpack_string_too_long"/>
  pub const MSGPACK_STRING_TOO_LONG: &[u8] = b"MsgPack string is too long";
  /// <see cref="CmdStrings.LUA_msgpack_array_too_long"/>
  pub const MSGPACK_ARRAY_TOO_LONG: &[u8] = b"MsgPack array is too long";
  /// <see cref="CmdStrings.LUA_msgpack_map_too_long"/>
  pub const MSGPACK_MAP_TOO_LONG: &[u8] = b"MsgPack map is too long";
  /// <see cref="CmdStrings.LUA_insufficient_lua_stack_space"/>
  pub const INSUFFICIENT_LUA_STACK_SPACE: &[u8] = b"Insufficient Lua stack space";
  /// <see cref="CmdStrings.LUA_parameter_reset_failed_memory"/>
  pub const PARAMETER_RESET_FAILED_MEMORY: &[u8] =
    b"Resetting parameters to Lua script failed: Memory";
  /// <see cref="CmdStrings.LUA_parameter_reset_failed_syntax"/>
  pub const PARAMETER_RESET_FAILED_SYNTAX: &[u8] =
    b"Resetting parameters to Lua script failed: Syntax";
  /// <see cref="CmdStrings.LUA_parameter_reset_failed_runtime"/>
  pub const PARAMETER_RESET_FAILED_RUNTIME: &[u8] =
    b"Resetting parameters to Lua script failed: Runtime";
  /// <see cref="CmdStrings.LUA_parameter_reset_failed_other"/>
  pub const PARAMETER_RESET_FAILED_OTHER: &[u8] =
    b"Resetting parameters to Lua script failed: Other";
  /// <see cref="CmdStrings.LUA_out_of_memory"/>
  pub const OUT_OF_MEMORY: &[u8] = b"Lua VM ran out of memory";
  /// <see cref="CmdStrings.RESP_ERR_GENERIC_UNK_CMD"/>（前缀形态，用于识别 unknown command）
  pub const ERR_UNKNOWN_TEXT: &[u8] = b"ERR unknown command";
  /// <see cref="CmdStrings.LUA_load_string_error"/>
  pub const LOAD_STRING_ERROR: &[u8] = b"load_string encountered error";
  /// <see cref="CmdStrings.LUA_AND"/>
  pub const AND: &[u8] = b"AND";
  /// <see cref="CmdStrings.LUA_OR"/>
  pub const OR: &[u8] = b"OR";
  /// <see cref="CmdStrings.LUA_XOR"/>
  pub const XOR: &[u8] = b"XOR";
  /// <see cref="CmdStrings.LUA_NOT"/>
  pub const NOT: &[u8] = b"NOT";
  /// <see cref="CmdStrings.LUA_DIFF"/>
  pub const DIFF: &[u8] = b"DIFF";
  /// <see cref="CmdStrings.LUA_KEYS"/>
  pub const KEYS: &[u8] = b"KEYS";
  /// <see cref="CmdStrings.LUA_ARGV"/>
  pub const ARGV: &[u8] = b"ARGV";
}

pub struct LuaRunner_Strings;

impl LuaRunner_Strings {
  /// libs/server/Lua/LuaRunner.Strings.cs:ConstantStringToRegistry
  ///
  /// 高频字符串压栈并转入注册表，返回引用 id（C# TryRef 形态）。
  pub fn constant_string_to_registry(state: &mut LuaStateWrapper, string: &[u8]) -> i32 {
    if !state.try_push_buffer(string) {
      panic!("Insufficient space in Lua VM for constant string");
    }
    state
      .try_ref()
      .expect("Insufficient space in Lua VM for constant string")
  }
}

#[cfg(test)]
mod tests {
  use super::{ConstantStrings, LuaRunner_Strings};
  use crate::lua::lua_state_wrapper::LuaStateWrapper;

  #[test]
  fn constant_string_texts_match_cmd_strings() {
    assert_eq!(ConstantStrings::OK_LOWER, b"ok");
    assert_eq!(
      ConstantStrings::ERR_UNKNOWN,
      b"ERR Unknown Redis command called from script"
    );
    assert_eq!(
      ConstantStrings::CANNOT_SERIALISE_NESTING,
      b"Cannot serialise, excessive nesting (1001)"
    );
    assert_eq!(ConstantStrings::KEYS, b"KEYS");
    assert_eq!(ConstantStrings::ARGV, b"ARGV");
  }

  #[test]
  fn constant_string_to_registry_roundtrip() {
    let mut state = LuaStateWrapper::new();
    let id = LuaRunner_Strings::constant_string_to_registry(&mut state, ConstantStrings::OK_LOWER);
    assert!(state.expect_lua_stack_empty());
    assert!(state.push_ref(id));
    assert_eq!(state.known_string_to_buffer(-1).unwrap(), b"ok");
  }
}
