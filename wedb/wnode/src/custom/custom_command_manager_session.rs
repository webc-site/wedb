pub struct CustomCommandManagerSession;

impl CustomCommandManagerSession {
  /// libs/server/Custom/CustomCommandManagerSession.cs:GetCustomProcedure
  /// 满足 C# 自定义存储过程查找规范，当前会话未注册扩展过程故保留 _id 形参
  pub fn get_custom_procedure(_id: u8) -> Option<()> {
    None
  }

  /// libs/server/Custom/CustomCommandManagerSession.cs:GetCustomTransactionProcedure
  /// 满足 C# 自定义事务存储过程查找规范，当前会话未注册扩展事务故保留 _id 形参
  pub fn get_custom_transaction_procedure(_id: u8) -> Option<()> {
    None
  }

  /// libs/server/Custom/CustomCommandManagerSession.cs:TryGetCustomCommandInfo
  /// 满足 C# 自定义命令信息查询规范，保留 _name 形参
  pub fn try_get_custom_command_info(_name: &str) -> bool {
    false
  }

  /// libs/server/Custom/CustomCommandManagerSession.cs:TryGetCustomCommandDocs
  /// 满足 C# 自定义命令文档查询规范，保留 _name 形参
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
  /// 满足 C# 自定义 RESP 命令查找规范，保留 _name 形参
  pub fn get_custom_resp_command(_name: &str) -> Option<u16> {
    None
  }

  /// libs/server/Custom/CustomCommandManagerSession.cs:GetCustomGarnetObjectType
  /// 满足 C# 自定义对象类型查找规范，保留 _name 形参
  pub fn get_custom_garnet_object_type(_name: &str) -> Option<u8> {
    None
  }
}
