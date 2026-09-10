pub struct CustomCommandManagerSession;

impl CustomCommandManagerSession {
  /// libs/server/Custom/CustomCommandManagerSession.cs:GetCustomProcedure
  pub fn get_custom_procedure(_id: u8) -> Option<()> {
    None
  }

  /// libs/server/Custom/CustomCommandManagerSession.cs:GetCustomTransactionProcedure
  pub fn get_custom_transaction_procedure(_id: u8) -> Option<()> {
    None
  }

  /// libs/server/Custom/CustomCommandManagerSession.cs:TryGetCustomCommandInfo
  pub fn try_get_custom_command_info(_name: &str) -> bool {
    false
  }

  /// libs/server/Custom/CustomCommandManagerSession.cs:TryGetCustomCommandDocs
  pub fn try_get_custom_command_docs(_name: &str) -> bool {
    false
  }

  /// libs/server/Custom/CustomCommandManagerSession.cs:GetAllCustomCommandsInfos
  pub fn get_all_custom_commands_infos() -> Vec<&'static str> {
    Vec::new()
  }

  /// libs/server/Custom/CustomCommandManagerSession.cs:GetAllCustomCommandsDocs
  pub fn get_all_custom_commands_docs() -> Vec<&'static str> {
    Vec::new()
  }

  /// libs/server/Custom/CustomCommandManagerSession.cs:GetCustomCommandInfoCount
  pub fn get_custom_command_info_count() -> usize {
    0
  }

  /// libs/server/Custom/CustomCommandManagerSession.cs:GetCustomRespCommand
  pub fn get_custom_resp_command(_name: &str) -> Option<u16> {
    None
  }

  /// libs/server/Custom/CustomCommandManagerSession.cs:GetCustomGarnetObjectType
  pub fn get_custom_garnet_object_type(_name: &str) -> Option<u8> {
    None
  }
}
