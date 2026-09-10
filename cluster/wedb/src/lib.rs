//! WeDB 集群数据面（对标 Garnet `libs/cluster` 的 Replication/Migration/Failover）
//!
//! 共识算法层已拆分至独立库 `wraft`（纯逻辑、零 IO、零引擎依赖，对标
//! `etcd/raft` 的库化形态），本包只做数据面与接线：
//!
//! - [`aof_sync`] — 主从 AOF 同步驱动：已提交 WAL 流按记录帧发货
//!   （对标 Replication/AofSyncDriver 位点推进语义）
//! - [`migration`] — 范围索引树文件分块流：帧编解码与接收端重组
//!   （对标 Migration/RangeIndexFileDataSource/RangeIndexFileDataSink）
//! - [`failover`] — 主从切换决策：以 `wraft::Role` 驱动数据面行为门控
//!   （对标 libs/client/GarnetClientAPI/GarnetClientClusterCommands.cs:Failover/FailoverManager，Garnet 为配置驱动，此处由共识引擎裁决）
//!
//! 依赖铁律：生产依赖只允许 `wedb_standalone`、`wraft` 与 `thiserror`，不直接触碰
//! embed 引擎 crate（dev-dependencies 的 embed 路径仅供本包测试装配，
//! 不进下游依赖图）

pub mod aof_sync;
pub mod failover;
pub mod migration;

pub use aof_sync::{AofSyncDriver, AofTransport, Error as SyncError, Result as SyncResult};
pub use failover::{Error as FailoverError, FailoverManager, Result as FailoverResult};
pub use migration::{
  Error as MigrationError, Result as MigrationResult, TreeChunkFrame, TreeFileSink,
};
/// 算法层关键类型经本包转出口，数据面接线免引 wraft 全路径；
/// 状态机与错误类型详见 wraft 自身（保持算法库 API 单一来源）
pub use wraft::{ConsensusEngine, NoopConsensus, Role};
