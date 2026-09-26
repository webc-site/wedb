//! wedb 集群层集中错误定义
//!
//! 所有集群层错误集中于此（thiserror），依赖库错误用
//! `#[error(transparent)]` 透明转发；会话层负责按需附加 `ERR ` 前缀
//! 转 RESP，错误本体不携带协议痕迹

use std::{io, result};

use thiserror::Error;
use wkv;
use wnode::range_index::MigrationError;
use wresp::cmd_strings::cluster::{
  ERR_GENERIC_CANNOT_REPLICATE_SELF, ERR_GENERIC_MIGRATE_TO_MYSELF, ERR_UNKNOWN_NODE_PREFIX,
};

use crate::server::cluster_manager_worker_state::ERR_RECOVERY_LOCK;

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
  /// CLUSTER RESET 被拒：本地主节点槽位上仍有键（C#
  /// ClusterManagerWorkerState.TryReset 的 HasKeysInSlots 拒否臂，
  /// RESP 文案单源在 wresp::cmd_strings::cluster）
  #[error("CLUSTER RESET can't be called with master nodes containing keys")]
  ResetWithKeysAssigned,
  /// 不能向自身迁移槽位
  #[error("cannot migrate to myself")]
  MigrateToMyself,
  /// 目标节点非主节点（槽位语境）
  #[error("target node {0} is not a primary")]
  TargetNotPrimary(String),
  /// 本地已是副本（C# ClusterManagerWorkerState.cs:173-178）
  #[error("already replica of {0}")]
  AlreadyReplica(String),
  /// 复制目标非主节点（C# ClusterManagerWorkerState.cs:195-200）
  #[error("trying to replicate node ({0}) that is not a primary")]
  ReplicateTargetNotPrimary(String),
  /// 槽位不归本地主节点所有
  #[error("slot {0} is not owned by this node")]
  SlotNotOwned(usize),
  /// MIGRATING 排期拒绝：槽位状态非 Stable，已排定迁移（C#
  /// libs/cluster/Server 下 ClusterManagerSlotState 的单槽臂
  /// TryPrepareSlotForMigration :116 与批量臂 TryPrepareSlotsForMigration
  /// :188，`node_id` 取 GetNodeIdFromSlot 投影的既有 migrating 源节点 ID；
  /// 本变体不登记映射，对 C# 的法定映射单点在
  /// server/cluster_manager_slot_state.rs 的
  /// try_prepare_slot_for_migration / try_prepare_slots_for_migration 实现注释）
  #[error("slot {slot} already scheduled for migration from node {node_id}")]
  SlotAlreadyScheduled { slot: usize, node_id: String },
  /// REPLICATE 拒绝：本节点仍持有分配槽不得降为副本（C#
  /// libs/cluster/Server/ClusterManagerWorkerState.cs:TryAddReplicaAsync
  /// :180-183，RESP 文案单源在 [`cluster_err_text`] 副本域臂）
  #[error("primary has been assigned slots and cannot be a replica")]
  PrimaryHasAssignedSlots,
  /// SETSLOT <slot> NODE 处于 IMPORTING 的槽时传入非本地节点 ID（C#
  /// libs/cluster/Server/ClusterManagerSlotState.cs:TryPrepareSlotForOwnershipChange
  /// :356-362；节点在拓扑中合法已知，仅因并非接收命令的本地节点被拒）
  #[error("input nodeid {input} different from local nodeid {local}")]
  InputNodeNotLocal { input: String, local: String },
  /// IMPORTING 接收方本地已持有该槽，无需导入（C#
  /// libs/cluster/Server/ClusterManagerSlotState.cs:TryPrepareSlotForImport
  /// :232-236 / TryPrepareSlotsForImport :292-296）
  #[error("slot {0} is a local hash slot and is already imported")]
  LocalSlotAlreadyImported(usize),
  /// IMPORTING 指定的源节点与槽位在当前拓扑的属主不符（C#
  /// libs/cluster/Server/ClusterManagerSlotState.cs:TryPrepareSlotForImport
  /// :238-243 / TryPrepareSlotsForImport :299-304）
  #[error("slot {slot} is not owned by node {node_id}")]
  SlotNotOwnedByNode { slot: usize, node_id: String },
  /// IMPORTING 接收侧本地节点非主角色（C#
  /// libs/cluster/Server/ClusterManagerSlotState.cs:TryPrepareSlotForImport
  /// :226-230 / TryPrepareSlotsForImport :282-286，携带 C# `NodeRole`
  /// 枚举名形态的角色文本）
  #[error("importing node {0} is not a master node")]
  ImportingNodeNotPrimary(String),
  /// IMPORTING 时槽位状态非 Stable，已排定自该源节点的导入（C#
  /// libs/cluster/Server/ClusterManagerSlotState.cs:TryPrepareSlotForImport
  /// :245-249 / TryPrepareSlotsForImport :307-311）
  #[error("slot {slot} already scheduled for import from node {node_id}")]
  SlotAlreadyScheduledForImport { slot: usize, node_id: String },
  /// 获取恢复锁失败
  #[error("cannot acquire recovery lock")]
  CannotAcquireRecoveryLock,
  /// Gossip 错误
  #[error("gossip error: {0}")]
  Gossip(String),
  /// 网络连接错误透明转发
  #[error(transparent)]
  Conn(#[from] wconn::Error),
  /// 节点运行时与服务错误透明转发
  #[error(transparent)]
  Node(#[from] wnode::Error),
  /// 存储错误透明转发
  #[error(transparent)]
  Storage(#[from] wkv::Error),
  /// IO 错误透明转发
  #[error(transparent)]
  Io(#[from] io::Error),
  /// 参数无效
  #[error("invalid argument: {0}")]
  InvalidArgument(String),
  /// 宣告端点与监听端点不匹配（对标 C# Options.cs:810 GarnetException
  /// "Cluster announce endpoint does not match list of listen endpoints
  /// provided!"：宣告 IP 非法、主机名非本机、端口或地址不落监听列表同拒）
  #[error("Cluster announce endpoint does not match list of listen endpoints provided!")]
  AnnounceMismatch,
  /// Any 绑定出口 IP 探测失败（对标 C# StoreWrapper.cs:356-373 Socket.Connect
  /// 的 SocketException 外抛拒启；rust 显式报错拒启，不回退保护模式——回退会把
  /// 127.0.0.1 扩散给对端，建连必败，比拒启更隐蔽，选型证据见 announce 模块文档）
  #[error("cluster announce outbound IP probe failed: {0}")]
  AnnounceProbe(io::Error),
  /// 集群未初始化
  #[error("cluster not initialized")]
  ClusterNotInitialized,
  /// 操作被取消
  #[error("operation cancelled")]
  OperationCancelled,
  /// 范围索引迁移错误透明转发
  #[error(transparent)]
  Migration(#[from] MigrationError),
}

pub type Result<T> = result::Result<T, Error>;

/// [`cluster_err_text`] 映射语境（票 zcode-r135c 案三）：C# 同族句模板在两域
/// 本异（SlotState 槽位命令域 vs WorkerState 副本域），双语境因异文而立，
/// 非过度设计——如 MigrateToMyself 臂 `"ERR Can't MIGRATE to myself"` 与
/// `"ERR Can't replicate myself"`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RespScope {
  /// 槽位管理命令域（C# ClusterManagerSlotState.cs TryPrepareSlotFor* 各
  /// errorMessage 臂的应答语境）
  SlotState,
  /// 副本接入域（C# ClusterManagerWorkerState.cs TryAddReplicaAsync 与
  /// ReplicaOfCommand.cs 错误臂的应答语境；会话应答与副本同步发起端共用）
  WorkerReplica,
}

/// wedb::Error → 集群 RESP 错误文案的唯一映射入口（票 zcode-r135c 案三：
/// 收编 slot_mgmt::slot_state_err_text 与 cluster_manager_worker_state::
/// replicate_err_text 两套并行手抄，同族句模板同步提为
/// `wresp::cmd_strings::cluster` 常量；逐变体文案原样平移零行为变更，
/// 词项契约对拍归 r28）。other 兜底臂保留现状：未经登记变体仍以 thiserror
/// Display 直通应答，收编到统一入口后新增变体的登记面可穷举审计
#[must_use]
pub fn cluster_err_text(e: Error, scope: RespScope) -> String {
  use Error as E;
  match (e, scope) {
    // 两域同源句
    (E::NodeNotFound(id), _) => format!("{ERR_UNKNOWN_NODE_PREFIX}{id}"),
    (E::TargetNotPrimary(id), _) => format!("ERR Target node {id} is not a master node."),
    // 同变体两域异文（RespScope 因这两臂而立）
    (E::MigrateToMyself, RespScope::SlotState) => ERR_GENERIC_MIGRATE_TO_MYSELF.to_string(),
    (E::MigrateToMyself, RespScope::WorkerReplica) => ERR_GENERIC_CANNOT_REPLICATE_SELF.to_string(),
    // 槽位命令域登记臂
    (E::SlotNotOwned(slot), RespScope::SlotState) => {
      format!("ERR I'm not the owner of hash slot {slot}")
    }
    // C# ClusterManagerSlotState.cs:116/:188：无槽号、无 or import，
    // 携带既有 migrating 源节点 ID（Migrating 槽 eff 属主投影）
    (E::SlotAlreadyScheduled { node_id, .. }, RespScope::SlotState) => {
      format!("ERR Slot already scheduled for migration from {node_id}")
    }
    (E::InputNodeNotLocal { input, local }, RespScope::SlotState) => {
      format!("ERR Input nodeid {input} different from local nodeid {local}.")
    }
    (E::LocalSlotAlreadyImported(slot), RespScope::SlotState) => {
      format!("ERR This is a local hash slot {slot} and is already imported")
    }
    (E::SlotNotOwnedByNode { slot, node_id }, RespScope::SlotState) => {
      format!("ERR Slot {slot} is not owned by {node_id}")
    }
    (E::ImportingNodeNotPrimary(role), RespScope::SlotState) => {
      format!("ERR Importing node {role} is not a master node.")
    }
    (E::SlotAlreadyScheduledForImport { node_id, .. }, RespScope::SlotState) => {
      format!("ERR Slot already scheduled for import from {node_id}")
    }
    (E::NoWorkers, RespScope::SlotState) => "ERR workers not initialized".to_string(),
    // 副本接入域登记臂
    (E::AlreadyReplica(id), RespScope::WorkerReplica) => {
      format!("ERR I am already replica of {id}.")
    }
    (E::ReplicateTargetNotPrimary(id), RespScope::WorkerReplica) => {
      format!("ERR Trying to replicate node ({id}) that is not a primary.")
    }
    (E::PrimaryHasAssignedSlots, RespScope::WorkerReplica) => {
      "ERR Primary has been assigned slots and cannot be a replica".to_string()
    }
    (E::CannotAcquireRecoveryLock, RespScope::WorkerReplica) => ERR_RECOVERY_LOCK.to_string(),
    // 兜底臂现状保留（帧行为零变更），未登记变体经 Display 直通
    (other, _) => other.to_string(),
  }
}
