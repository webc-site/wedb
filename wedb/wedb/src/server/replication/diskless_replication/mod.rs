//! 主端无盘复制同步 (DisklessReplication)
//!
//! 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/
//!
//! 目录拓扑对标 C# PrimaryOps/DisklessReplication：
//! - [`sync_status`]             ↔ SyncStatus.cs
//! - [`replica_sync_session`]    ↔ ReplicaSyncSession.cs（diskless 分部）
//! - [`replication_sync_manager`] ↔ ReplicationSyncManager.cs + ReplicaSyncSessionTaskStore.cs
//! - [`replication_snapshot_iterator`] ↔ ReplicationSnapshotIterator.cs
//!
//! 核心口径：多副本 attach 经 leader 攒批（REPL_DISKLESS_SYNC_DELAY 窗口）合成
//! 一批，批内单遍存储活扫描 + 逐会话锁步扇出（同一字节流广播全部活跃会话，
//! 任一会话缓冲满即全批冲刷等齐后续写），批内共享一枚快照覆盖锚——与 C#
//! SnapshotIteratorManager / MainStreamingSnapshotDriverAsync 一致。rust 无
//! Tsavorite 流式检查点封版语义（活扫描 + 锚定口径已有），C# WaitOrDieAsync
//! 的迭代进度看门狗在 rust 由逐扇出点的停等超时（30s）承接：单会话挂死即
//! 该会话判败摘除，全员判败即中止扫描，无需独立看门狗任务。
//!
//! C# 的 syncInProgress 读写锁 / cts 取消链在 rust 以会话册子原子的批量标志
//! + 会话终态收敛等价承载（rust 无 cts 撤销链，attach 处理任务不被外部取消）。

pub mod replica_sync_session;
pub mod replication_snapshot_iterator;
pub mod replication_sync_manager;
pub mod sync_status;

pub use replica_sync_session::DisklessSyncSession;
pub use replication_sync_manager::ReplicationSyncManager;
pub use sync_status::{SyncStatus, SyncStatusInfo};
