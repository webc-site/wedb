//! wedb 集群层集中错误定义
//!
//! 所有集群层错误集中于此（thiserror），依赖库错误用
//! `#[error(transparent)]` 透明转发；会话层负责按需附加 `ERR ` 前缀
//! 转 RESP，错误本体不携带协议痕迹

use std::{io, result};

use thiserror::Error;
use wkv;

/// 集中层错误
#[derive(Debug, Error)]
pub enum Error {
  /// 集群配置 bitcode 编解码错误
  #[error(transparent)]
  Codec(#[from] bitcode::Error),
  /// 配置载荷为空（不足以容纳版本字节）
  #[error("cluster config payload too short to contain a version")]
  PayloadTooShort,
  /// 配置载荷缺失 worker 条目（线格式自 1 号本地 worker 起序列化，空列表即结构
  /// 损坏；放行会产出无本地位的配置，后续 LOCAL_WORKER_ID 索引将 panic）
  #[error("cluster config payload has no workers")]
  MissingWorkers,
  /// 配置格式版本不兼容
  #[error("incompatible cluster config version: got {got}, expect {expect}")]
  Version { got: u8, expect: u8 },
  /// RLE 槽位段长度越界（累计覆盖超过 16384 槽）
  #[error("cluster config slot segments overflow 16384 slots")]
  SlotOverflow,
  /// 槽位状态字节非法
  #[error("invalid slot state byte: {0}")]
  SlotState(u8),
  /// RLE 槽位段指向不存在的 worker 下标（越界属主会击穿槽位投影方法）
  #[error("slot segment references unknown worker id: {0}")]
  SlotWorkerId(u16),
  /// 集群 worker 尚未初始化（无本地节点）
  #[error("workers not initialized")]
  NoWorkers,
  /// config epoch 设置被拒：仅允许从 0 初始化且新值必须更大
  #[error("config epoch not set: current epoch is non-zero or value not greater")]
  EpochNotSet,
  /// 添加槽位被拒：槽位已被占用（附带冲突槽位号）
  #[error("slot {0} is not free")]
  SlotNotFree(usize),
  /// 移除槽位被拒：槽位不归属本地（附带槽位号）
  #[error("slot {0} is not owned by local node")]
  SlotNotLocal(usize),
  /// 节点未找到
  #[error("node {0} not found")]
  NodeNotFound(String),
  /// 不能遗忘自身
  #[error("cannot forget myself")]
  CannotForgetMyself,
  /// 副本不能遗忘其主节点
  #[error("cannot forget primary node")]
  CannotForgetPrimary,
  /// 不能向自身迁移槽位
  #[error("cannot migrate to myself")]
  MigrateToMyself,
  /// 目标节点非主节点
  #[error("target node {0} is not a primary")]
  TargetNotPrimary(String),
  /// 槽位不归本地主节点所有
  #[error("slot {0} is not owned by this node")]
  SlotNotOwned(usize),
  /// 槽位已排定迁移或导入
  #[error("slot {0} already scheduled for migration or import")]
  SlotAlreadyScheduled(usize),
  /// 获取恢复锁失败
  #[error("cannot acquire recovery lock")]
  CannotAcquireRecoveryLock,
  /// Gossip 错误
  #[error("gossip error: {0}")]
  Gossip(String),
  /// 网络连接错误透明转发
  #[error(transparent)]
  Conn(#[from] wconn::Error),
  /// 存储错误透明转发
  #[error(transparent)]
  Storage(#[from] wkv::Error),
  /// IO 错误透明转发
  #[error(transparent)]
  Io(#[from] io::Error),
}

pub type Result<T> = result::Result<T, Error>;
