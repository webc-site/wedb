//! 认证层（对标 garnet libs/server/Auth）
//!
//! ACL 认证器单档（[`GarnetAclAuthenticator`] 基座 + 注入式口令校验
//! [`acl_password_check`]）。
//!
//! 认证器接口（IGarnetAuthenticator）与 ACL + 口令子档
//! （GarnetAclWithPasswordAuthenticator）在 rust 不设独立类型：会话真实机制是
//! `authenticator_can_authenticate` 字段由挂载与否直接推导，AUTH 经基座
//! `authenticate` + 注入的 [`acl_password_check`] 内联承接组合职责（见
//! wnode/src/resp/resp_server_session.rs:authenticate_user）；免认证与单一
//! 固定口令两档亦不设独立类型：服务器装配只有 ACL 一个认证源（`requirepass`
//! 在 `wnode::service::StorageSessionProvider::with_requirepass` 直接落成带
//! 口令的 default 用户），未装配即免认证，见该处注释与
//! `js/check/ignore/garnet/libs/server/Auth/` 下的登记理由。

pub mod garnet_acl_authenticator;

pub use garnet_acl_authenticator::{GarnetAclAuthenticator, acl_password_check};

/// 字节串按 ASCII 规范化（>0x7F 折 '?'，对标 C# Encoding.ASCII.GetString）
pub(crate) fn ascii_sanitize(bytes: &[u8]) -> String {
  bytes
    .iter()
    .map(|&b| if b.is_ascii() { b as char } else { '?' })
    .collect()
}
