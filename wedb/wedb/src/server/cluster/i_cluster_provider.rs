//! 集群提供者抽象面（对标 libs/server/Cluster/IClusterProvider.cs）

use std::{future::Future, sync::Arc};

use waof::AofAddress;
use wresp::command::RespCommand;

use crate::server::cluster_session::ClusterSession;

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
  ) -> impl Future<Output = ()>;
}

/// libs/server/Cluster/IClusterProvider.cs:IClusterProvider
///
/// 集群提供者抽象接口，继承 [`wnode::ClusterProvider`] 与 [`CheckpointCallbackFace`]
pub trait IClusterProvider: wnode::ClusterProvider + CheckpointCallbackFace {
  /// 创建集群会话（注册进 provider 活跃会话表，返回共享句柄）
  fn create_cluster_session(&self) -> Arc<ClusterSession>;

  /// 异步集群发布（跨分片/节点消息投递）
  fn cluster_publish_async<'a>(
    &'a self,
    cmd: RespCommand,
    channel: &'a [u8],
    message: &'a [u8],
  ) -> impl Future<Output = ()> + 'a;

  /// 阻止角色变更（进入故障转移/恢复状态）
  fn prevent_role_change(&self) -> bool;

  /// 允许角色变更
  fn allow_role_change(&self);

  /// 安全截断 AOF
  fn safe_truncate_aof(&self, truncate_until: &AofAddress) -> impl Future<Output = ()>;

  /// 是否允许数据丢失
  fn allow_data_loss(&self) -> bool;

  /// 异步恢复
  fn recover_async<'a>(&'a self) -> impl Future<Output = ()> + 'a;
}
