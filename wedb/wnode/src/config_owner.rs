//! CONFIG SET → 生产后台任务域的调停执行器（libs/server/StoreWrapper.cs 的
//! rust 形态）
//!
//! C# 以 `StoreWrapper` 具体类型承接 `RuntimeServerConfig` 的更新回调
//!（`ReconcilePrimaryTask` / `ApplyAofSyncMaxLagBytes`）；rust 侧
//! `RuntimeServerConfig::try_set` 产出 [`ConfigReconcile`] 调停消息，由持有
//! 存储引擎引用的 server 层（CONFIG SET 命令域）经本模块
//! [`apply_config_reconcile`] 就地 match 执行——封闭枚举静态分发，
//! 无 trait object 与运行时动态查表。

use std::sync::Arc;

use wconf::ConfigReconcile;
use wdev::Device;
use wkv::WedbStore;

use crate::{
  aof::garnet_append_only_file::GarnetAppendOnlyFile, cluster_provider::ClusterProviderHandle,
  primary_tasks::PrimaryTasks,
};

/// 执行 CONFIG SET 产出的调停消息（对标 StoreWrapper.ReconcilePrimaryTask
/// 家族 + ApplyAofSyncMaxLagBytes 的动作分派）
///
/// - `primary_tasks`：Primary 类后台任务生命周期域（C# owner 即 StoreWrapper
///   的任务域投影；None = 纯协议层 mock 形态，任务域调停无目标仅落槽位）
/// - `aof`：AOF 追加日志门面（C# storeWrapper.appendOnlyFile 共享依赖；
///   None = 无 AOF / mock 形态，背压预算调停无目标仅落槽位）
/// - `cluster`：集群提供者句柄（C# storeWrapper.clusterProvider；单机 /
///   None 形态无 gossip / failover 消费面，节点超时投影仅落槽位）
pub fn apply_config_reconcile<D>(
  primary_tasks: Option<&Arc<PrimaryTasks>>,
  store: &Arc<WedbStore<D>>,
  aof: Option<&Arc<GarnetAppendOnlyFile>>,
  cluster: Option<&ClusterProviderHandle>,
  msg: ConfigReconcile,
) where
  D: Device + 'static,
{
  match msg {
    // aof-commit-freq 变更落点（对标 ReconcilePrimaryTask(CommitTask) →
    // TryStartCommitTask）。rust 提交模型为同步 group commit（wbase
    // `GroupCommitPipeline` 统一驱动 wkv 刷盘与 WaofSublog 提交）+ 独立
    // 周期刷盘任务（primary_tasks 域，弱引用常驻 + 副本角色门检）；提交
    // 间隔在 AOF 日志构造期固化（`single_log_aof`），0 值切换已被
    // `RuntimeServerConfig` 拒绝（CommitFreqZero / CommitFreqAutoCommitStart），
    // 安全值域 {-1, >0} 间变更无新周期任务需重启——提交间隔由装配终态
    // 固化，槽位存值即终态。
    ConfigReconcile::CommitTask { .. } => {}

    // expired-object-collection-freq 变更落点（对标
    // ReconcilePrimaryTask(ObjectCollectTask) → TryStartObjectCollectTask：
    // 取消 + 按新值重启）。rust 周期对象收集任务每轮重读频率槽位：
    // 新值 <= 0 时循环下轮自退出（取消语义）；新值 > 0 时绑定执行域 +
    // 幂等重拉确保任务在跑（禁用态启动后经 CONFIG SET 启用的唯一拉起
    // 路径）。副本角色不拉起（对标 C# ReconcilePrimaryTask 副本分支），
    // 升主恢复点（集群层 resume_primary_tasks）按新配置重拉。
    ConfigReconcile::ObjectCollect { .. } => {
      if let Some(tasks) = primary_tasks {
        tasks.try_start_object_collect_task();
      }
    }

    // aof-sync-max-lag-bytes 变更落点（对标 ApplyAofSyncMaxLagBytes → 逐库
    // `AppendOnlyFile?.backpressure?.SetBudget`）。rust 单日志门面直达背压
    // 闸门：预算即时重调（含禁用态 ↔ 启用态切换），无需重启，不涉生命周期
    // 任务（闸门读取裸原子字段，对标 RuntimeServerConfig.cs:168-171 注释
    // 「pushes a CONFIG SET straight into the live gate」）。
    ConfigReconcile::AofSyncMaxLag { max_lag_bytes } => {
      if let Some(gate) = aof.and_then(|aof| aof.backpressure()) {
        gate.set_budget(max_lag_bytes);
      }
    }

    // cluster-node-timeout 变更落点（rust 特有投影：C# 消费面每轮
    // GetTimeSpan(CLUSTER_NODE_TIMEOUT) 现取，rust 消费面统一读 provider
    // 原子槽）。推入后 gossip / failover / 集群管理全部臂即时随动。
    // 本消息仅承载有限正间隔（非正值经 get_time_span 归一为无限、不产消息，
    // 见 wconf apply_cluster_node_timeout_update）；原子的 0 = 无限超时哨兵
    // （cluster_node_timeout() 的 None 分支）仅由启动播种 args.cluster_node_timeout_ms
    // 置入，运行期由有限值改回无限需消费面现取运行时表，属未接线残余。
    ConfigReconcile::ClusterNodeTimeout { ms } => {
      if let Some(cluster) = cluster {
        cluster.set_cluster_node_timeout_ms(ms);
      }
    }

    // expired-key-deletion-scan-freq 变更落点（对标
    // ReconcilePrimaryTask(ExpiredKeyDeletionTask) →
    // TryStartExpiredKeyDeletionTask）。`scan_frequency_secs > 0`：按新间隔
    //（秒 → 毫秒）启用扫描并确保循环在跑（未启动/已停则拉起，对标
    // `RegisterAndRun`）；`<= 0`：禁用并停循环（对标 `taskLifecycleLock` 下
    // 的 `CancelAsync`）。间隔与开关经共享 `GcConfig` 由循环每轮重读，
    // 下一轮生效。
    ConfigReconcile::ExpiredKeyDeletionScan {
      scan_frequency_secs,
    } => {
      if scan_frequency_secs > 0 {
        store.reconcile_gc_scan(true, Some(scan_frequency_secs as u64 * 1000));
      } else {
        store.reconcile_gc_scan(false, None);
      }
    }

    // compaction-max-segments 变更落点（C# 无对应调停：DatabaseManagerBase.
    // DoCompactionAsync 每轮 `GetInt(COMPACTION_MAX_SEGMENTS)` 现取；rust 引擎
    // 读 GcConfig 快照，此处把刚落槽的新值投影进去，GcManager 每轮重读同效，
    // 下一轮判定即生效）。
    ConfigReconcile::CompactionMaxSegments { max_segments } => {
      store.update_gc_config(|c| {
        c.compaction_max_segments = max_segments.max(0) as usize;
      });
    }

    // compaction-type 变更落点（同上，对标每轮
    // `GetEnum<LogCompactionType>(COMPACTION_TYPE)` 现取；调停消息已收敛为
    // 具体枚举，直接投影）。
    ConfigReconcile::CompactionType { compaction_type } => {
      store.update_gc_config(|c| c.compaction_type = compaction_type);
    }
  }
}
