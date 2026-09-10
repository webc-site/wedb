//! 事务域（对标 libs/server/Transaction/*）
//!
//! C# TransactionManager 为 partial class，按文件拆面；Rust 侧以单一
//! [`transaction_manager::TransactionManager`] 结构 + 跨文件 `impl` 块
//! 承接同构拆分（键管理 / 集群槽校验 / RESP 命令面）。WATCH 版本校验经
//! [`watch_version_map::WatchVersionMap`] 键级版本表，与存储会话侧的
//! wkv 尾地址保守代理同向（过估多中止，不漏检）。

pub mod transaction_manager;
pub mod txn_cluster_slot_check;
pub mod txn_key_entry;
pub mod txn_key_entry_comparison;
pub mod txn_key_manager;
pub mod txn_lock_table;
pub mod txn_resp_commands;
pub mod txn_watched_keys_container;
pub mod watch_version_map;
