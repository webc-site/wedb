use crate::acl::command_catalog_data::{CmdEntry, CMD_ENTRIES};
use crate::types::RespCommand;

pub struct RespCommandsInfo;

impl RespCommandsInfo {
  /// libs/server/Resp/RespCommandsInfo.cs:TryInitialize
  pub fn try_initialize() -> bool {
    true
  }

  /// libs/server/Resp/RespCommandsInfo.cs:TryInitializeRespCommandsInfo
  pub fn try_initialize_resp_commands_info() -> bool {
    true
  }

  /// libs/server/Resp/RespCommandsInfo.cs:IndividualAcls
  pub fn individual_acls() -> bool {
    true
  }

  /// libs/server/Resp/RespCommandsInfo.cs:TryGetCommandsforAclCategory
  pub fn try_get_commandsfor_acl_category(_cat: u32) -> Vec<&'static str> {
    Vec::new()
  }

  /// libs/server/Resp/RespCommandsInfo.cs:TryGetRespCommandsInfoCount
  pub fn try_get_resp_commands_info_count() -> usize {
    CMD_ENTRIES.len()
  }

  /// libs/server/Resp/RespCommandsInfo.cs:TryGetRespCommandsInfo
  pub fn try_get_resp_commands_info() -> &'static [CmdEntry] {
    CMD_ENTRIES
  }

  /// libs/server/Resp/RespCommandsInfo.cs:TryGetRespCommandNames
  pub fn try_get_resp_command_names() -> Vec<&'static str> {
    CMD_ENTRIES.iter().map(|e| e.name).collect()
  }

  /// libs/server/Resp/RespCommandsInfo.cs:TryGetRespCommandInfo
  pub fn try_get_resp_command_info(name: &str) -> Option<&'static CmdEntry> {
    CMD_ENTRIES.iter().find(|e| e.name.eq_ignore_ascii_case(name))
  }

  /// libs/server/Resp/RespCommandsInfo.cs:TryFastGetRespCommandInfo
  pub fn try_fast_get_resp_command_info(cmd: RespCommand) -> Option<&'static CmdEntry> {
    CMD_ENTRIES.iter().find(|e| e.cmd == cmd)
  }

  /// libs/server/Resp/RespCommandsInfo.cs:TryGetRespSubCommandsInfo
  pub fn try_get_resp_sub_commands_info(parent: RespCommand) -> Vec<&'static CmdEntry> {
    CMD_ENTRIES.iter().filter(|e| e.parent == Some(parent)).collect()
  }

  /// libs/server/Resp/RespCommandsInfo.cs:TryGetSimpleRespCommandInfo
  pub fn try_get_simple_resp_command_info(name: &str) -> Option<&'static CmdEntry> {
    Self::try_get_resp_command_info(name)
  }

  /// libs/server/Resp/RespCommandsInfo.cs:GetRespCommandName
  pub fn get_resp_command_name(cmd: RespCommand) -> Option<&'static str> {
    Self::try_fast_get_resp_command_info(cmd).map(|e| e.name)
  }

  /// libs/server/Resp/RespCommandsInfo.cs:ToRespFormat
  pub fn to_resp_format(_output: &mut Vec<u8>) {}
}
