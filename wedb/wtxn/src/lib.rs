//! Transaction并发控制核心库（对标 Garnet `libs/server/Transaction`）
//!
//! 包含事务管理器 [`TransactionManager`]、引擎实例锁表 [`TxnLockTable`]
//!（锁源为 store 侧注入的 windex 哈希桶内嵌闩，句柄克隆共享，构造期注入事务管理器与键集）、
//! WATCH 乐观并发原子版本表 [`WatchVersionMap`]、会话级被监视键容器 [`TxnWatchedKeysContainer`]、
//! 键锁条目集合 [`TxnKeyEntries`] 与排序比较器、事务过程解析面 [`TxnProcResolver`]
//! 与会话交互接口 [`TxnSession`]（RESP 应答面由宿主侧承接）。

pub mod transaction_manager;
pub mod txn_key_entry;
pub mod txn_key_entry_comparison;
pub mod txn_key_manager;
pub mod txn_key_spec;
pub mod txn_keys_buffer;
pub mod txn_lock_table;
pub mod txn_proc;
pub mod txn_session;
pub mod txn_slot_verify;
pub mod txn_state;
pub mod txn_watched_keys_container;
pub mod watch_version_map;

pub use transaction_manager::{
  SublogAccess, SublogVirtualVectors, TransactionGuard, TransactionManager, TransactionStoreTypes,
  TxnAofLog, TxnEntryType, TxnProcApi, TxnProcReadApi, TxnProcedure, TxnWatchApi,
};
pub use txn_key_entry::{LockType, TxnKeyEntries, TxnKeyEntry};
pub use txn_key_entry_comparison::TxnKeyEntryComparison;
pub use txn_key_manager::TxnCommandKeys;
pub use txn_key_spec::TxnKeySpec;
pub use txn_keys_buffer::{TXN_KEYS_INLINE_CAPACITY, TxnKeysBuffer, TxnKeysIter};
pub use txn_lock_table::TxnLockTable;
pub use txn_proc::{TxnProcHandle, TxnProcResolver, TxnQueuedCommandInfo};
pub use txn_session::TxnSession;
pub use txn_slot_verify::{SlotVerifyHandle, TxnSlotVerifyFace};
pub use txn_state::TxnState;
pub use txn_watched_keys_container::TxnWatchedKeysContainer;
pub use watch_version_map::{DEFAULT_VERSION_MAP_SIZE, WatchVersionMap};
