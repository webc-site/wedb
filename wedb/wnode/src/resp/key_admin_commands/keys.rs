//! KEYS / RENAME / RENAMENX / EXPIRE / TTL 键名与生命周期管理命令（对标 libs/server/Resp/KeyAdminCommands.cs）
//!
//! 目录化拆分（门面：模块声明 + 转出口，外部路径不变）：
//! - [`rename`]：RENAME / RENAMENX 双键跨命名空间搬移事务
//! - [`delete`]：GETDEL 取删一体（DEL 族同步臂）
//! - [`expire`]：EXPIRE / PERSIST / EXISTS / TTL / EXPIRETIME 生存期族

mod delete;
mod expire;
mod rename;

pub use self::expire::{ExpireCmd, ExpireTimeCmd, TtlCmd};
pub(crate) use self::{
  expire::{ExpireArgs, FLAG_FRAMES, parse_expire_args},
  rename::reply_renamed,
};
