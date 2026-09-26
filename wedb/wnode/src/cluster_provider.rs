//! 集群提供者抽象面与单机零开销桩实现
//!
//! 在单机模式（Standalone）下注入 [`NoopClusterProvider`]，
//! 编译器将内联所有空操作方法，静态消除分支与多态运行时开销；
//! 在集群模式（Cluster）下由 `wedb` 注入完整的分布式集群协调实现。
//! 对标 C# 直接持 `IClusterProvider` 接口引用的虚分派，rust 等价物即
//! [`Arc<dyn ClusterProvider>`]（[`ClusterProviderHandle`]） trait 对象。

use std::sync::Arc;

use waof::AofAddress;
use wresp::metrics::MetricsItem;

use crate::{RoleInfo, resp::slow_path::SlowFuture, session_parse_state_extensions::ManagerType};

/// 集群提供者多态抽象面（对标 Garnet IClusterProvider，与 SessionProviderFace / MessageConsumerFace 命名同构）
pub trait ClusterProviderFace: Send + Sync + 'static {
  /// 是否启用集群模式
  #[inline]
  fn is_cluster_enabled(&self) -> bool {
    false
  }

  /// 给定槽位是否由当前本地节点掌管且处于 Stable 态（SWAPDB 库级门禁消费面，
  /// doc/zh/db.md SWAPDB 条款）：属主口径取 C# ClusterConfig.IsLocal 的写面
  ///（不含副本读放行扩展，MIGRATING 源端 eff 计入本地），但 MIGRATING/IMPORTING
  /// 迁移窗口一律拒绝——换号只换 logic_db→vdb 指针、物理数据原地，窗口内换库
  /// 会将在途搬迁数据与另一库归属对调，打破换号与迁移的互斥。
  /// 单机形态恒 false（门禁先判 [`Self::is_cluster_enabled`]，单机不会走到此查询）
  #[inline]
  fn is_slot_local_stable(&self, _slot: u16) -> bool {
    false
  }

  /// 启动集群后台治理任务（Gossip 探测、心跳维持、故障转移监听等）
  #[inline]
  fn start(&self) {}

  /// 刷盘并持久化当前集群拓扑配置
  #[inline]
  fn flush_config(&self) {}

  /// 停机清理集群后台任务
  #[inline]
  fn dispose(&self) {}

  /// 更新集群节点间相互访问的认证凭据
  ///
  /// libs/server/Cluster/IClusterProvider.cs:UpdateClusterAuth
  #[inline]
  fn update_cluster_auth(&self, _username: Option<String>, _password: Option<String>) {}

  /// 推入集群节点超时毫秒数（CONFIG SET cluster-node-timeout 的调停落点；
  /// C# 消费面 Gossip.cs:25 / GarnetServerNode.cs:90 / FailoverManager.cs:24
  /// 每轮 runtimeConfig.GetTimeSpan/GetInt(CLUSTER_NODE_TIMEOUT) 现取，rust
  /// 消费面统一读 provider 原子槽，由本口承接投影；0 = 无限超时哨兵。
  /// 单机 / Noop 形态无 gossip / failover 消费面，默认空操作零开销
  #[inline]
  fn set_cluster_node_timeout_ms(&self, _ms: u64) {}

  /// 判定当前节点是否为主节点（Primary）
  #[inline]
  fn is_primary(&self) -> bool {
    true
  }

  /// 判定当前节点是否为副本节点（Replica）
  #[inline]
  fn is_replica(&self) -> bool {
    false
  }

  /// 判定给定节点 ID 是否为副本节点
  #[inline]
  fn is_replica_node(&self, _node_id: u128) -> bool {
    false
  }

  /// 获取当前节点的唯一运行 ID（RunId）或集群复制 ID
  ///
  /// libs/server/Cluster/IClusterProvider.cs:GetRunId
  #[inline]
  fn get_run_id(&self) -> String {
    String::new()
  }

  /// 获取主节点复制信息与全部挂载副本列表
  ///
  /// libs/server/Cluster/IClusterProvider.cs:GetPrimaryInfo
  #[inline]
  fn get_primary_info(&self) -> (AofAddress, Vec<RoleInfo>) {
    (AofAddress::default(), Vec::new())
  }

  /// 获取自身角色信息
  #[inline]
  fn get_replica_info(&self) -> RoleInfo {
    RoleInfo::default()
  }

  /// 获取当前复制监控信息段
  #[inline]
  fn get_replication_info(&self) -> Vec<MetricsItem> {
    Vec::new()
  }

  /// 获取检查点监控信息段
  ///
  /// libs/server/Cluster/IClusterProvider.cs:GetCheckpointInfo
  #[inline]
  fn get_checkpoint_info(&self) -> Vec<MetricsItem> {
    Vec::new()
  }

  /// 获取 Gossip 监控信息段
  ///
  /// libs/server/Cluster/IClusterProvider.cs:GetGossipStats
  #[inline]
  fn get_gossip_stats(&self, _metrics_disabled: bool) -> Vec<MetricsItem> {
    Vec::new()
  }

  /// 获取缓冲池监控信息段
  #[inline]
  fn get_buffer_pool_stats(&self) -> Vec<MetricsItem> {
    Vec::new()
  }

  /// 清空指定类型缓冲池
  #[inline]
  fn purge_buffer_pool(&self, _manager_type: ManagerType) {}

  /// 重置 Gossip 统计指标
  ///
  /// libs/server/Cluster/IClusterProvider.cs:ResetGossipStats
  #[inline]
  fn reset_gossip_stats(&self) {}

  /// AOF 物理子日志数（ROLE 命令 usingShardedLog 判定输入）
  #[inline]
  fn aof_sublog_count(&self) -> usize {
    1
  }

  /// 全租户 FLUSHALL 跨主节点换号广播（doc/zh/db.md 4.5 Cluster Bus
  /// Broadcast，本端口多租户扩展；C# 无 ns 维度无对应函数）
  ///
  /// 集群形态返回协调者 future（收齐全部 Primary ack 才 Ready），单机 /
  /// Noop 形态返回 None = 无远端可广播，调用方本地换号即完成
  #[inline]
  fn flushall_broadcast(&self, _ns: u64) -> Option<SlowFuture> {
    None
  }

  /// 检查点版本切换开始通知（对标 C# ReplicationManager.CheckpointVersionShiftStart）
  ///
  /// 主库在快照状态机 IN_PROGRESS（版本号推进）处向 AOF 追加 CheckpointStartCommit
  /// 标记，副本据此进入模糊区缓冲新代条目。单机 / Noop 形态无复制域、无标记面，
  /// 默认空实现零开销；集群形态由 wedb ClusterProvider 转发至 ReplicationManager
  #[inline]
  fn checkpoint_version_shift_start(&self, _new_version: i64) {}

  /// 检查点版本切换结束通知（对标 C# ReplicationManager.CheckpointVersionShiftEnd）
  ///
  /// 主库在快照状态机 WAIT_FLUSH（快照落盘、截断之前）处向 AOF 追加
  /// CheckpointEndCommit 标记，副本据此退出模糊区并重放缓冲条目。默认空实现同
  /// [`Self::checkpoint_version_shift_start`]
  #[inline]
  fn checkpoint_version_shift_end(&self, _new_version: i64) {}

  /// 检查点发起回调：由复制域给出检查点覆盖的 AOF 地址（对标 C# ClusterProvider.OnCheckpointInitiated）
  ///
  /// 集群形态 PRIMARY 取当前复制位点、REPLICA 取检查点开始标记位点，并同步
  /// 更新提交安全地址（UpdateCommitSafeAofAddress）；检查点内核
  /// （`database::database_manager_base` 的 take_database_checkpoint_async）
  /// 按 cluster 句柄在位时下达。单机 / Noop 形态无复制域，内核自持直取 AOF
  /// 尾地址分支（C# else 分支），默认空实现不触达
  #[inline]
  fn on_checkpoint_initiated(&self, _covered: &mut AofAddress) {}

  /// 检查点完成后登记条目并安全截断（对标 C# ClusterProvider.AddNewCheckpointEntry）
  ///
  /// 登记 CheckpointEntry 历史（供副本 attach 新主时清理旧检查点）并经
  /// SafeTruncateAOF 截断；异步截断口经既有 [`SlowFuture`] 擦除壳承载
  /// （与 [`Self::flushall_broadcast`] 同形），调用方须 await 收口。单机 /
  /// Noop 形态返回 None，内核自持 TruncateUntil + Commit 分支（C# else 分支）
  #[inline]
  fn add_new_checkpoint_entry(
    &self,
    _full: bool,
    _covered: AofAddress,
    _store_checkpoint_token: u128,
    _object_store_checkpoint_token: u128,
  ) -> Option<SlowFuture> {
    None
  }

  /// 设备是否处于污染状态（如副本检查点接收失败导致底层设备部分被覆写）
  #[inline]
  fn is_device_contaminated(&self) -> bool {
    false
  }
}

/// 空操作集群提供者（单机模式零开销桩实现，直接继承 trait 默认实现）
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NoopClusterProvider;

impl ClusterProvider for NoopClusterProvider {}

/// 共享句柄直入泛型装配位的转发层（`ServerBootstrap<A, C>` 单机 Noop 静态
/// 分发 / 集群 `Arc<具体 provider>` 共享句柄同一约束位；纯 Deref 转发零开销）
/// `Arc<T>` 全端口纯转发实现（原逐方法手写样板，宏单点展开，签名与
/// [`ClusterProvider`] 现声明逐条一致）
macro_rules! forward_cluster_provider {
  ($(fn $name:ident ($($arg:ident : $ty:ty),*) -> $ret:ty;)*) => {
    impl<T: ClusterProvider + ?Sized> ClusterProvider for Arc<T> {
      $(
        #[inline]
        fn $name(&self, $($arg: $ty),*) -> $ret {
          (**self).$name($($arg),*)
        }
      )*
    }
  };
}

forward_cluster_provider! {
  fn is_cluster_enabled() -> bool;
  fn is_slot_local_stable(slot: u16) -> bool;
  fn start() -> ();
  fn flush_config() -> ();
  fn dispose() -> ();
  fn update_cluster_auth(username: Option<String>, password: Option<String>) -> ();
  fn set_cluster_node_timeout_ms(ms: u64) -> ();
  fn is_primary() -> bool;
  fn is_replica() -> bool;
  fn is_replica_node(node_id: u128) -> bool;
  fn get_run_id() -> String;
  fn get_primary_info() -> (AofAddress, Vec<RoleInfo>);
  fn get_replica_info() -> RoleInfo;
  fn get_replication_info() -> Vec<MetricsItem>;
  fn get_checkpoint_info() -> Vec<MetricsItem>;
  fn get_gossip_stats(metrics_disabled: bool) -> Vec<MetricsItem>;
  fn get_buffer_pool_stats() -> Vec<MetricsItem>;
  fn purge_buffer_pool(manager_type: ManagerType) -> ();
  fn reset_gossip_stats() -> ();
  fn aof_sublog_count() -> usize;
  fn flushall_broadcast(ns: u64) -> Option<SlowFuture>;
  fn checkpoint_version_shift_start(new_version: i64) -> ();
  fn checkpoint_version_shift_end(new_version: i64) -> ();
  fn on_checkpoint_initiated(covered: &mut AofAddress) -> ();
  fn is_device_contaminated() -> bool;
}

/// 集群提供者拥有态句柄（`Arc<dyn ClusterProviderFace>` trait 对象，对标 C#
/// 直接持 `IClusterProvider` 接口引用；单机注入 `Arc<NoopClusterProvider>`，
/// 集群由 wedb 注入完整实现）
pub type ClusterProviderHandle = Arc<dyn ClusterProviderFace>;

/// 兼容既有命名的 trait 别名
pub use ClusterProviderFace as ClusterProvider;
