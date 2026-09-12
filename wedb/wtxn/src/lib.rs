//! Transaction并发控制核心库（对标 Garnet `libs/server/Transaction`）
//!
//! 包含事务管理器 [`TransactionManager`]、条带化并发读写锁表 [`TxnLockTable`]、
//! WATCH 乐观并发原子版本表 [`WatchVersionMap`]、会话级被监视键容器 [`TxnWatchedKeysContainer`]、
//! 键锁条目集合 [`TxnKeyEntries`] 与排序比较器、RESP 事务命令面与会话交互接口 [`TxnSession`]。

pub mod store_type;
pub mod transaction_manager;
pub mod txn_key_entry;
pub mod txn_key_entry_comparison;
pub mod txn_key_manager;
pub mod txn_key_spec;
pub mod txn_lock_table;
pub mod txn_resp_commands;
pub mod txn_session;
pub mod txn_state;
pub mod txn_watched_keys_container;
pub mod watch_version_map;

pub use store_type::StoreType;
pub use transaction_manager::{
  REPLAY_TASK_ACCESS_VECTOR_BYTES, SublogAccess, SublogVirtualVectors, TransactionGuard,
  TransactionManager, TransactionStoreTypes, TxnAofLog, TxnProcedure,
};
pub use txn_key_entry::{LockType, TxnKeyEntries, TxnKeyEntry};
pub use txn_key_entry_comparison::TxnKeyEntryComparison;
pub use txn_key_manager::TxnCommandKeys;
pub use txn_key_spec::TxnKeySpec;
pub use txn_lock_table::{STRIPE_COUNT, TxnKeyLockGuard, TxnLockTable};
pub use txn_resp_commands::{TxnProcHandle, TxnProcResolver, TxnQueuedCommandInfo};
pub use txn_session::{MockTxnSession, TxnSession};
pub use txn_state::TxnState;
pub use txn_watched_keys_container::TxnWatchedKeysContainer;
pub use watch_version_map::WatchVersionMap;
