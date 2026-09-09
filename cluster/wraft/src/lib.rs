//! WeDB 共识算法库
//!
//! 对应角色类似 `etcd/raft`、`tikv/raft-rs`：纯算法层，零 IO、零时钟、
//! 零引擎依赖——时间由调用方注入，传输经 [`ElectionTransport`] 抽象，
//! 因此可独立单测、fuzz 与模型检测。
//!
//! Garnet `libs/cluster` 无内建共识（Failover 为配置驱动），本库是
//! WeDB 在其蓝图上的算法扩展；数据面接线（AOF 同步、迁移、故障转移
//! 编排）在 [`wedb`](https://crates.io/crates/wedb) 中完成。
//!
//! - [`election`] — 选主：Raft 风格任期/投票状态机
//! - [`consensus`] — 共识引擎接口：propose/role/leader，算法可插拔

pub mod consensus;
pub mod election;

pub use consensus::{
  ConsensusEngine, Error as ConsensusError, NoopConsensus, Result as ConsensusResult,
};
pub use election::{
  ElectionState, ElectionTransport, Error as ElectionError, NodeId, Result as ElectionResult, Role,
  Term, VoteReply, VoteRequest,
};
