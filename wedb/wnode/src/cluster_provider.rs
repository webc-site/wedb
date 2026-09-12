//! 集群提供者抽象面与单机零开销桩实现
//!
//! 在单机模式（Standalone）下注入 [`NoopClusterProvider`]，
//! 编译器将内联所有空操作方法，静态消除分支与多态运行时开销；
//! 在集群模式（Cluster）下由 `wedb` 注入完整的分布式集群协调实现。

use std::sync::Arc;

/// 集群提供者多态抽象（对标 Garnet IClusterProvider）
pub trait ClusterProvider: Send + Sync + 'static {
  /// 是否启用集群模式
  #[inline]
  fn is_cluster_enabled(&self) -> bool {
    false
  }

  /// 启动集群后台治理任务（Gossip 探测、心跳维持、故障转移监听等）
  #[inline]
  fn start(&self) {}

  /// 刷盘并持久化当前集群拓扑配置
  #[inline]
  fn flush_config(&self) {}

  /// 更新集群节点间相互访问的认证凭据
  #[inline]
  fn update_cluster_auth(&self, _username: Option<String>, _password: Option<String>) {}

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

  /// 获取当前节点的唯一运行 ID（RunId）或集群复制 ID
  #[inline]
  fn get_run_id(&self) -> String {
    String::new()
  }
}

/// 空操作集群提供者（单机模式零开销桩实现）
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NoopClusterProvider;

impl ClusterProvider for NoopClusterProvider {
  #[inline(always)]
  fn is_cluster_enabled(&self) -> bool {
    false
  }

  #[inline(always)]
  fn start(&self) {}

  #[inline(always)]
  fn flush_config(&self) {}

  #[inline(always)]
  fn update_cluster_auth(&self, _username: Option<String>, _password: Option<String>) {}

  #[inline(always)]
  fn is_primary(&self) -> bool {
    true
  }

  #[inline(always)]
  fn is_replica(&self) -> bool {
    false
  }

  #[inline(always)]
  fn get_run_id(&self) -> String {
    String::new()
  }
}

impl<T: ClusterProvider + ?Sized> ClusterProvider for Arc<T> {
  #[inline]
  fn is_cluster_enabled(&self) -> bool {
    (**self).is_cluster_enabled()
  }

  #[inline]
  fn start(&self) {
    (**self).start();
  }

  #[inline]
  fn flush_config(&self) {
    (**self).flush_config();
  }

  #[inline]
  fn update_cluster_auth(&self, username: Option<String>, password: Option<String>) {
    (**self).update_cluster_auth(username, password);
  }

  #[inline]
  fn is_primary(&self) -> bool {
    (**self).is_primary()
  }

  #[inline]
  fn is_replica(&self) -> bool {
    (**self).is_replica()
  }

  #[inline]
  fn get_run_id(&self) -> String {
    (**self).get_run_id()
  }
}
