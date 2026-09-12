//! 集群提供者抽象面（对标 libs/server/Cluster/IClusterProvider.cs）

use std::{future::Future, pin::Pin};

use waof::AofAddress;
use wnode::MetricsItem;
use wresp::RespCommand;

use super::{i_cluster_session::IClusterSession, role_info::RoleInfo};

/// 检查点发起与完成回调切面
pub trait CheckpointCallbackFace: Send + Sync {
  /// 检查点发起时通知
  fn on_checkpoint_initiated(&self, checkpoint_covered_aof_address: &mut AofAddress);

  /// 登记新检查点条目
  fn add_new_checkpoint_entry(
    &self,
    full: bool,
    checkpoint_covered_aof_address: AofAddress,
    store_checkpoint_token: u128,
    object_store_checkpoint_token: u128,
  );
}

/// 缓冲池所属管理器（对齐 C# ManagerType：主存储 / 对象存储 / AOF）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagerType {
  /// 主存储
  Main,
  /// 对象存储
  Object,
  /// AOF
  Aof,
}

/// 集群提供者抽象接口（dyn 兼容，契合 compio 单线程 Future 特性）
pub trait IClusterProvider: Send + Sync + CheckpointCallbackFace {
  /// 创建集群会话
  fn create_cluster_session(&self) -> Box<dyn IClusterSession>;

  /// 判定是否为主节点
  fn is_primary(&self) -> bool;

  /// 判定是否为副本节点
  fn is_replica(&self) -> bool;

  /// 判定给定节点 ID 是否为副本节点
  fn is_replica_node(&self, node_id: &str) -> bool;

  /// 获取当前节点运行 ID / 复制 ID
  fn get_run_id(&self) -> String;

  /// 获取主节点复制信息与全部挂载副本列表
  fn get_primary_info(&self) -> (AofAddress, Vec<RoleInfo>);

  /// 获取自身角色信息
  fn get_replica_info(&self) -> RoleInfo;

  /// 获取当前复制监控信息段
  fn get_replication_info(&self) -> Vec<MetricsItem>;

  /// 获取检查点监控信息段
  fn get_checkpoint_info(&self) -> Vec<MetricsItem>;

  /// 获取 Gossip 监控信息段
  fn get_gossip_stats(&self, metrics_disabled: bool) -> Vec<MetricsItem>;

  /// 获取缓冲池监控信息段
  fn get_buffer_pool_stats(&self) -> Vec<MetricsItem>;

  /// 清空指定类型缓冲池
  fn purge_buffer_pool(&self, manager_type: ManagerType);

  /// 异步集群发布（跨分片/节点消息投递）
  fn cluster_publish_async<'a>(
    &'a self,
    cmd: RespCommand,
    channel: &'a [u8],
    message: &'a [u8],
  ) -> Pin<Box<dyn Future<Output = ()> + 'a>>;

  /// 刷盘/同步集群配置
  fn flush_config(&self);

  /// 更新集群认证凭据
  fn update_cluster_auth(&self, cluster_username: Option<String>, cluster_password: Option<String>);

  /// 启动集群后台线程与任务
  fn start(&self);

  /// 阻止角色变更（进入故障转移/恢复状态）
  fn prevent_role_change(&self) -> bool;

  /// 允许角色变更
  fn allow_role_change(&self);

  /// 安全截断 AOF
  fn safe_truncate_aof(&self, truncate_until: &AofAddress);

  /// 是否允许数据丢失
  fn allow_data_loss(&self) -> bool;

  /// 异步恢复
  fn recover_async<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + 'a>>;

  /// 重置 Gossip 统计指标
  fn reset_gossip_stats(&self);
}
