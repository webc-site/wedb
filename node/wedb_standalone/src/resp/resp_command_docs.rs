pub struct RespCommandDocs;

impl RespCommandDocs {
  /// libs/server/Resp/RespCommandDocs.cs:TryInitialize
  pub fn try_initialize() -> bool {
    true
  }

  /// libs/server/Resp/RespCommandDocs.cs:TryInitializeRespCommandsDocs
  pub fn try_initialize_resp_commands_docs() -> bool {
    true
  }

  /// libs/server/Resp/RespCommandDocs.cs:TryGetRespCommandsDocs
  pub fn try_get_resp_commands_docs() -> Vec<&'static str> {
    Vec::new()
  }

  /// libs/server/Resp/RespCommandDocs.cs:TryGetRespCommandDocs
  pub fn try_get_resp_command_docs(_cmd: &str) -> Option<&'static str> {
    None
  }

  /// libs/server/Resp/RespCommandDocs.cs:TryGetRespSubCommandsDocs
  pub fn try_get_resp_sub_commands_docs(_parent: &str) -> Vec<&'static str> {
    Vec::new()
  }

  /// libs/server/Resp/RespCommandDocs.cs:ToRespFormat
  pub fn to_resp_format(_output: &mut Vec<u8>) {}
}
