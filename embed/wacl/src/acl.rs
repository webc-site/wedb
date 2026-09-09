use std::collections::{HashMap, HashSet};
use crate::error::{Error, Result};

/// garnet相对路径:garnet/libs/server/ACL/AccessControlList.cs:AccessControlList
pub struct AccessControlList {
    users: HashMap<String, User>,
}

impl AccessControlList {
    pub fn new() -> Self {
        let mut acl = Self {
            users: HashMap::new(),
        };
        // default user
        let mut default_user = User::new("default".to_string());
        default_user.permissions.allow_all = true;
        acl.add_user(default_user);
        acl
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

impl AclParser {
    pub fn parse_rules(user: &mut User, rules: &[&str]) -> Result<()> {
        for rule in rules {
            if rule.starts_with('+') {
                user.permissions.allowed_commands.insert(rule[1..].to_string());
            } else if rule.starts_with('-') {
                user.permissions.allowed_commands.remove(&rule[1..].to_string());
            } else if *rule == "on" {
                user.is_enabled = true;
            } else if *rule == "off" {
                user.is_enabled = false;
            } else if rule.starts_with('>') {
                let pwd = rule[1..].to_string();
                user.passwords.push(AclPassword { hash: pwd.into_bytes(), salt: vec![] });
            }
        }
        Ok(())
    }
}

/// garnet相对路径:garnet/libs/server/ACL/ACLPassword.cs:ACLPassword
#[derive(Debug, Clone)]
pub struct AclPassword {
    pub hash: Vec<u8>,
    pub salt: Vec<u8>,
}

/// garnet相对路径:garnet/libs/server/ACL/CommandPermissionSet.cs:CommandPermissionSet
#[derive(Debug, Clone)]
pub struct CommandPermissionSet {
    pub allow_all: bool,
    pub allowed_commands: HashSet<String>,
}

impl CommandPermissionSet {
    pub fn check_permission(&self, cmd: &str) -> bool {
        self.allow_all || self.allowed_commands.contains(cmd)
    }
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
                allow_all: false,
                allowed_commands: HashSet::new(),
            },
            is_enabled: true,
        }
    }
}

/// garnet相对路径:garnet/libs/server/ACL/UserHandle.cs:UserHandle
pub struct UserHandle {}
