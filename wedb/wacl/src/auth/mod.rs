//! 认证层（对标 garnet libs/server/Auth）
//!
//! 认证器接口与实现（NoAuth / Password / Acl / AclWithPassword），以及装配设置族。

pub mod garnet_acl_authenticator;
pub mod garnet_acl_with_password_authenticator;
pub mod garnet_authenticator;
pub mod garnet_no_auth_authenticator;
pub mod garnet_password_authenticator;
pub mod i_garnet_authenticator;
pub mod settings;

pub use garnet_acl_authenticator::GarnetAclAuthenticator;
pub use garnet_acl_with_password_authenticator::GarnetAclWithPasswordAuthenticator;
pub use garnet_authenticator::GarnetAuthenticator;
pub use garnet_no_auth_authenticator::GarnetNoAuthAuthenticator;
pub use garnet_password_authenticator::GarnetPasswordAuthenticator;
pub use i_garnet_authenticator::IGarnetAuthenticator;

/// 字节串按 ASCII 规范化（>0x7F 折 '?'，对标 C# Encoding.ASCII.GetString）
pub(crate) fn ascii_sanitize(bytes: &[u8]) -> String {
  bytes
    .iter()
    .map(|&b| if b.is_ascii() { b as char } else { '?' })
    .collect()
}
