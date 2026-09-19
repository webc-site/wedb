//! 键管理与生命周期管理命令模块（对标 libs/server/Resp/KeyAdminCommands.cs）
//!
//! 目录化拆分：
//! - [`keys`]：KEYS, RENAME, RENAMENX, COPY, TOUCH, EXPIRE, TTL 键名操作与生存期管理
//! - [`types`]：EXISTS, TYPE, DUMP, RESTORE, OBJECT 类型判定与对象原语
//!
//! SCAN / DBSIZE 在 resp/array_commands.rs（C# ArrayCommands.cs 同域）；
//! RANDOMKEY 两侧一致无（C# 全仓无此命令），不实现。

mod keys;
pub mod slow;
mod types;

pub use self::keys::{ExpireCmd, ExpireTimeCmd, TtlCmd};
