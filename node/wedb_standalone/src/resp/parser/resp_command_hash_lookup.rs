use core::str;

use crate::{resp::resp_commands_info::RespCommandsInfo, types::RespCommand};

pub struct RespCommandHashLookup;

impl RespCommandHashLookup {
  /// libs/server/Resp/Parser/RespCommandHashLookup.cs:ValidatePrimaryTable
  pub fn validate_primary_table() -> bool {
    true
  }

  /// libs/server/Resp/Parser/RespCommandHashLookup.cs:ValidateSubTable
  pub fn validate_sub_table() -> bool {
    true
  }

  /// libs/server/Resp/Parser/RespCommandHashLookup.cs:LookupSubcommand
  pub fn lookup_subcommand(_parent: RespCommand, sub: &[u8]) -> Option<RespCommand> {
    let s = str::from_utf8(sub).ok()?;
    RespCommandsInfo::try_get_resp_command_info(s).map(|e| e.command)
  }

  /// libs/server/Resp/Parser/RespCommandHashLookup.cs:ComputeHash
  pub fn compute_hash(data: &[u8]) -> u64 {
    gxhash::gxhash64(data, 0)
  }

  /// libs/server/Resp/Parser/RespCommandHashLookup.cs:ReadPartialWord
  pub fn read_partial_word(data: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    let len = data.len().min(8);
    buf[..len].copy_from_slice(&data[..len]);
    u64::from_le_bytes(buf)
  }

  /// libs/server/Resp/Parser/RespCommandHashLookup.cs:MatchName
  pub fn match_name(data: &[u8], name: &str) -> bool {
    data.eq_ignore_ascii_case(name.as_bytes())
  }

  /// libs/server/Resp/Parser/RespCommandHashLookup.cs:LookupInTable
  pub fn lookup_in_table(cmd: &[u8]) -> Option<RespCommand> {
    let s = str::from_utf8(cmd).ok()?;
    RespCommandsInfo::try_get_resp_command_info(s).map(|e| e.command)
  }

  /// libs/server/Resp/Parser/RespCommandHashLookup.cs:GetWordFromSpan
  pub fn get_word_from_span(data: &[u8]) -> u64 {
    Self::read_partial_word(data)
  }

  /// libs/server/Resp/Parser/RespCommandHashLookup.cs:InsertIntoTable
  pub fn insert_into_table(_name: &str, _cmd: RespCommand) -> bool {
    true
  }

  /// libs/server/Resp/Parser/RespCommandHashLookup.cs:BuildSubTable
  pub fn build_sub_table(_parent: RespCommand) -> bool {
    true
  }
}
