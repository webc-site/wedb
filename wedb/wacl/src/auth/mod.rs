//! 认证层（对标 garnet libs/server/Auth）
//!
//! 认证器接口与 ACL 实现（[`GarnetAclAuthenticator`] 基座 +
//! [`GarnetAclWithPasswordAuthenticator`] 口令档）。
//!
//! 免认证与单一固定口令两档在 rust 不设独立类型：服务器装配只有 ACL 一个
//! 认证源（`requirepass` 在 `wnode::service::StorageSessionProvider::with_requirepass`
//! 直接落成带口令的 default 用户），未装配即免认证，见该处注释与
//! `js/check/ignore/garnet/libs/server/Auth/` 下的登记理由。

pub mod garnet_acl_authenticator;
pub mod garnet_acl_with_password_authenticator;
pub mod i_garnet_authenticator;

pub use garnet_acl_authenticator::GarnetAclAuthenticator;
pub use garnet_acl_with_password_authenticator::{
  GarnetAclWithPasswordAuthenticator, acl_password_check,
};
pub use i_garnet_authenticator::IGarnetAuthenticator;

/// 字节串按 ASCII 规范化（>0x7F 折 '?'，对标 C# Encoding.ASCII.GetString）
pub(crate) fn ascii_sanitize(bytes: &[u8]) -> String {
  bytes
    .iter()
    .map(|&b| if b.is_ascii() { b as char } else { '?' })
    .collect()
}
