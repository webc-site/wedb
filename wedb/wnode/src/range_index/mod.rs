//! 范围索引复制 / 迁移编排域（对标 libs/server/Resp/RangeIndex/ 的
//! RangeIndexManager partial：Replication / Migration 与
//! RangeIndexReplicationActivities）。
//!
//! C# 中该管理器是 Garnet.server 层组件（直接依赖 GarnetAppendOnlyFile /
//! StorageSession / StringInput），非树文件层；rust 侧对应 wnode 顶层独立
//! 域，不再挂于 resp 命令域之下。RESP 线协议解析薄层见
//! [`crate::resp::range_index`]（对标 RespServerSessionRangeIndex.cs）。
//! 目录拼写统一为 range_index，协议名仍为 RI.*、磁盘目录名仍为 rangeindex。

pub mod range_index_manager_migration;
pub mod range_index_manager_replication;
pub mod range_index_migration_activities;
pub mod range_index_migration_receive_state;
pub mod range_index_replication_activities;

pub use range_index_manager_migration::{
  MigrationError, PublishMigratedIndexResult, RangeIndexManagerMigration, TreeStreamMeta,
};
pub use range_index_manager_replication::{
  RangeIndexChunkArgs, RangeIndexManagerReplication, RangeIndexStreamArgs, ReplicationError,
};
pub use range_index_migration_activities::{MigrateActivity, ReceiveActivity, TransmitActivity};
pub use range_index_migration_receive_state::RangeIndexMigrationReceiveState;
pub use range_index_replication_activities::{ReassemblyActivity, StreamActivity};
