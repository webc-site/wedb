/// libs/server/ACL/AccessControlList.cs:AccessControlList
pub struct AccessControlList {}

impl AccessControlList {
  pub fn new() -> Self {
    Self {}
  }
}

impl Default for AccessControlList {
  fn default() -> Self {
    Self::new()
  }
}

/// libs/server/ACL/ACLParser.cs:ACLParser
pub struct AclParser {}

/// libs/server/ACL/ACLPassword.cs:ACLPassword
pub struct AclPassword {}

/// libs/server/ACL/CommandPermissionSet.cs:CommandPermissionSet
pub struct CommandPermissionSet {}

/// libs/server/ACL/SecretsUtility.cs:SecretsUtility
pub struct SecretsUtility {}

/// libs/server/ACL/User.cs:User
pub struct User {}

/// libs/server/ACL/UserHandle.cs:UserHandle
pub struct UserHandle {}
