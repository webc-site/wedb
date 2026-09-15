//! ACL 认证设置基座（对标 libs/server/Auth/Settings/AclAuthenticationSettings.cs）

/// ACL 认证设置（公共底座：配置文件位置 + 默认口令；具体档位经组合派生）
pub struct AclAuthenticationSettings {
  /// ACL 配置文件位置（None 表示未配置 --acl-file）
  pub acl_configuration_file: Option<String>,
  /// 默认用户口令（配置文件未定义默认口令时的兜底）
  pub default_password: String,
}

impl AclAuthenticationSettings {
  /// 构造底座
  pub fn new(acl_configuration_file: Option<String>, default_password: String) -> Self {
    Self {
      acl_configuration_file,
      default_password,
    }
  }
}
