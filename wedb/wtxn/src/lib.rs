//! Transaction并发控制核心库（对标 Garnet `libs/server/Transaction`）
//!
//! 包含事务管理器 [`TransactionManager`]、引擎实例锁表 [`TxnLockTable`]
//!（锁源为 store 侧注入的 windex 哈希桶内嵌闩，句柄克隆共享，构造期注入事务管理器与键集；
//! 扩容 PREPARE_GROW 全事务屏障经 [`TxnBarrier`] 票据同址挂接同一引擎），
//! WATCH 乐观并发原子版本表 [`WatchVersionMap`]、会话级被监视键容器 [`TxnWatchedKeysContainer`]、
//! 键锁条目集合 [`TxnKeyEntries`] 与排序比较器
//! 与会话交互接口 [`TxnSession`]（RESP 应答面由宿主侧承接）。

pub(crate) mod transaction_manager;
pub(crate) mod txn_key_entry;
pub(crate) mod txn_key_entry_comparison;
pub(crate) mod txn_key_manager;
pub(crate) mod txn_key_spec;
pub(crate) mod txn_keys_buffer;
pub(crate) mod txn_lock_table;
pub(crate) mod txn_proc;
pub(crate) mod txn_session;
pub(crate) mod txn_state;
pub(crate) mod txn_watched_keys_container;
pub(crate) mod watch_version_map;

pub use transaction_manager::{
  ExecRun, SublogAccess, SublogVirtualVectors, TransactionManager, TransactionStoreTypes,
  TxnAofLog, TxnEntryType,
};
pub use txn_key_entry::{LockType, TxnKeyEntries, TxnKeyEntry};
pub use txn_key_entry_comparison::TxnKeyEntryComparison;
pub use txn_key_manager::TxnCommandKeys;
pub use txn_key_spec::TxnKeySpec;
pub use txn_keys_buffer::{TxnKeysBuffer, TxnKeysIter};
pub use txn_lock_table::{TxnBarrier, TxnBarrierTicket, TxnLockTable};
pub use txn_proc::TxnQueuedCommandInfo;
pub use txn_session::TxnSession;
pub use txn_state::TxnState;
pub use txn_watched_keys_container::TxnWatchedKeysContainer;
pub use watch_version_map::{DEFAULT_VERSION_MAP_SIZE, WatchVersionMap};
