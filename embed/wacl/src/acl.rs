use std::collections::HashMap;

/// garnet相对路径:garnet/libs/server/ACL/AccessControlList.cs:AccessControlList
pub struct AccessControlList {
  users: HashMap<String, User>,
}

impl AccessControlList {
  pub fn new() -> Self {
    Self {
      users: HashMap::new(),
    }
  }

  pub fn get_user(&self, username: &str) -> Option<&User> {
    self.users.get(username)
  }

  pub fn add_user(&mut self, user: User) {
    self.users.insert(user.name.clone(), user);
  }
}

impl Default for AccessControlList {
  fn default() -> Self {
    Self::new()
  }
}

/// garnet相对路径:garnet/libs/server/ACL/ACLParser.cs:ACLParser
pub struct AclParser {}

/// garnet相对路径:garnet/libs/server/ACL/ACLPassword.cs:ACLPassword
#[derive(Debug, Clone)]
pub struct AclPassword {
  pub hash: Vec<u8>,
  pub salt: Vec<u8>,
}

/// garnet相对路径:garnet/libs/server/ACL/CommandPermissionSet.cs:CommandPermissionSet
#[derive(Debug, Clone)]
pub struct CommandPermissionSet {
  pub allowed_commands: std::collections::HashSet<String>,
}

/// garnet相对路径:garnet/libs/server/ACL/SecretsUtility.cs:SecretsUtility
pub struct SecretsUtility {}

/// garnet相对路径:garnet/libs/server/ACL/User.cs:User
#[derive(Debug, Clone)]
pub struct User {
  pub name: String,
  pub passwords: Vec<AclPassword>,
  pub permissions: CommandPermissionSet,
  pub is_enabled: bool,
}

impl User {
  pub fn new(name: String) -> Self {
    Self {
      name,
      passwords: Vec::new(),
      permissions: CommandPermissionSet {
        allowed_commands: std::collections::HashSet::new(),
      },
      is_enabled: true,
    }
  }
}

/// garnet相对路径:garnet/libs/server/ACL/UserHandle.cs:UserHandle
pub struct UserHandle {}
