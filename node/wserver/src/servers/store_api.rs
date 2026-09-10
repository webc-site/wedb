//! 存储 API（对标 libs/server/Servers/StoreApi.cs:StoreApi）
//!
//! C# 持 StoreWrapper 并以 PreventRoleChangeLock 在副本态拦截提交/清库；
//! 托管面以 [`StoreCommitFace`]（StoreWrapper 提交/清库面的本域投影，
//! store_wrapper 模块接线后由其类型适配）+ [`ClusterRoleGate`]（集群角色
//! 闸门，集群域接线时实现）组合承接同一守卫语义。

use std::{future::Future, pin::Pin, sync::Arc};

/// 集群角色闸门（C# clusterProvider.PreventRoleChange / AllowRoleChange 投影；
/// 非集群部署为恒放行）
pub trait ClusterRoleGate: Send + Sync {
  /// 请求在闸门存活期内禁止角色变更（false = 当前状态不允许获取）
  fn prevent_role_change(&self) -> bool;
  /// 释放角色变更禁止
  fn allow_role_change(&self);
}

/// 恒放行闸门（非集群部署；C# `cluster == null → acquired = true` 分支）
pub struct AlwaysAllowGate;

impl ClusterRoleGate for AlwaysAllowGate {
  fn prevent_role_change(&self) -> bool {
    true
  }
  fn allow_role_change(&self) {}
}

/// 角色闸门守卫：drop 即放行（C# PreventRoleChangeLock.Dispose）
pub struct RoleChangeGuard<'a> {
  gate: Option<&'a dyn ClusterRoleGate>,
}

impl Drop for RoleChangeGuard<'_> {
  fn drop(&mut self) {
    if let Some(gate) = self.gate {
      gate.allow_role_change();
    }
  }
}

/// 存储提交面（C# StoreWrapper.WaitForCommitAsync / CommitAOFAsync /
/// FlushDatabase 的本域投影）
pub trait StoreCommitFace: Send + Sync {
  /// 等待 AOF 提交（false = 提交被配置忽略）
  fn wait_for_commit(&self) -> bool;
  /// 提交 AOF（刷盘 + 推进提交地址）
  fn commit_aof(&self, db_id: i64) -> wkv::Result<()>;
  /// 清库（unsafe_truncate_log 为破坏性日志截断）
  fn flush_database<'a>(
    &'a self,
    unsafe_truncate_log: bool,
    db_id: i64,
  ) -> Pin<Box<dyn Future<Output = wkv::Result<()>> + Send + 'a>>;
}

/// 存储 API
pub struct StoreApi {
  /// 存储提交面（C# storeWrapper）
  store: Arc<dyn StoreCommitFace>,
  /// 集群角色闸门（C# clusterProvider；None = 非集群恒放行）
  cluster_gate: Option<Arc<dyn ClusterRoleGate>>,
  /// 副本态判定（C# IsReplica；闸门实现承载副本语义）
  is_replica: bool,
}

impl StoreApi {
  /// 构造存储 API
  ///
  /// libs/server/Servers/StoreApi.cs:StoreApi（主构造）
  pub fn new(
    store: Arc<dyn StoreCommitFace>,
    cluster_gate: Option<Arc<dyn ClusterRoleGate>>,
  ) -> Self {
    Self {
      store,
      cluster_gate,
      is_replica: false,
    }
  }

  /// 置副本态判定（集群域接线入口；C# 由 clusterProvider.IsReplica() 查询）
  pub fn set_replica(&mut self, is_replica: bool) {
    self.is_replica = is_replica;
  }

  /// 角色闸门获取（C# PreventRoleChange(out acquired)）
  ///
  /// 返回 None 表示当前状态不允许（闸门拒绝），调用方按"忽略提交"处置；
  /// 非集群（无闸门）恒为已获取（C# `cluster == null` 分支）。
  fn prevent_role_change(&self) -> Option<RoleChangeGuard<'_>> {
    let Some(gate) = self.cluster_gate.as_deref() else {
      return Some(RoleChangeGuard { gate: None });
    };
    if !gate.prevent_role_change() {
      return None;
    }
    Some(RoleChangeGuard { gate: Some(gate) })
  }

  /// 等待 AOF 提交
  ///
  /// libs/server/Servers/StoreApi.cs:WaitForCommitAsync
  ///
  /// 闸门未获取或副本态返回 false（提交被忽略）。
  pub fn wait_for_commit(&self) -> bool {
    let _guard = match self.prevent_role_change() {
      Some(guard) => guard,
      None => return false,
    };
    if self.is_replica {
      return false;
    }
    self.store.wait_for_commit()
  }

  /// 提交 AOF（刷盘 + 推进提交地址）
  ///
  /// libs/server/Servers/StoreApi.cs:CommitAOFAsync
  ///
  /// 闸门未获取或副本态返回 Ok(false)（提交被忽略）。
  pub fn commit_aof(&self, db_id: i64) -> wkv::Result<bool> {
    let _guard = match self.prevent_role_change() {
      Some(guard) => guard,
      None => return Ok(false),
    };
    if self.is_replica {
      return Ok(false);
    }
    self.store.commit_aof(db_id).map(|()| true)
  }

  /// 清库（删除全部键；unsafe_truncate_log 为破坏性日志截断）
  ///
  /// libs/server/Servers/StoreApi.cs:FlushDB
  ///
  /// 闸门未获取或副本态返回 Ok(false)（清库被忽略）。
  pub async fn flush_db(&self, db_id: i64, unsafe_truncate_log: bool) -> wkv::Result<bool> {
    let _guard = match self.prevent_role_change() {
      Some(guard) => guard,
      None => return Ok(false),
    };
    if self.is_replica {
      return Ok(false);
    }
    self
      .store
      .flush_database(unsafe_truncate_log, db_id)
      .await
      .map(|()| true)
  }
}

#[cfg(test)]
mod tests {
  use std::sync::atomic::{AtomicBool, Ordering};

  use compio::runtime::Runtime;

  use super::*;

  /// 记录闸门调用的测试闸门
  struct RecordingGate {
    allowed: AtomicBool,
    prevented: AtomicBool,
    released: AtomicBool,
  }

  impl RecordingGate {
    fn new() -> Arc<Self> {
      Arc::new(Self {
        allowed: AtomicBool::new(true),
        prevented: AtomicBool::new(false),
        released: AtomicBool::new(false),
      })
    }
  }

  impl ClusterRoleGate for RecordingGate {
    fn prevent_role_change(&self) -> bool {
      self.prevented.store(true, Ordering::SeqCst);
      self.allowed.load(Ordering::SeqCst)
    }
    fn allow_role_change(&self) {
      self.released.store(true, Ordering::SeqCst);
    }
  }

  /// 记录提交调用的测试存储面
  struct MockStore {
    waited: AtomicBool,
    committed: AtomicBool,
    flushed: AtomicBool,
  }

  impl MockStore {
    fn new() -> Arc<Self> {
      Arc::new(Self {
        waited: AtomicBool::new(false),
        committed: AtomicBool::new(false),
        flushed: AtomicBool::new(false),
      })
    }
  }

  impl StoreCommitFace for MockStore {
    fn wait_for_commit(&self) -> bool {
      self.waited.store(true, Ordering::SeqCst);
      true
    }
    fn commit_aof(&self, _db_id: i64) -> wkv::Result<()> {
      self.committed.store(true, Ordering::SeqCst);
      Ok(())
    }
    fn flush_database<'a>(
      &'a self,
      _unsafe_truncate_log: bool,
      _db_id: i64,
    ) -> Pin<Box<dyn Future<Output = wkv::Result<()>> + Send + 'a>> {
      Box::pin(async {
        self.flushed.store(true, Ordering::SeqCst);
        Ok(())
      })
    }
  }

  #[test]
  fn commit_gates_allow_and_reject() {
    let store = MockStore::new();
    let gate = RecordingGate::new();
    let api = StoreApi::new(store.clone(), Some(gate.clone()));

    assert!(api.commit_aof(0).expect("提交成功"));
    assert!(gate.prevented.load(Ordering::SeqCst));
    assert!(gate.released.load(Ordering::SeqCst));
    assert!(store.committed.load(Ordering::SeqCst));

    // 闸门拒绝 → 提交被忽略（不触达存储面）
    gate.allowed.store(false, Ordering::SeqCst);
    store.committed.store(false, Ordering::SeqCst);
    assert!(!api.commit_aof(0).expect("路径闭环"));
    assert!(!store.committed.load(Ordering::SeqCst));
  }

  #[test]
  fn replica_state_ignores_commits() {
    let store = MockStore::new();
    let gate = RecordingGate::new();
    let mut api = StoreApi::new(store.clone(), Some(gate));
    api.set_replica(true);
    assert!(!api.commit_aof(0).expect("路径闭环"));
    assert!(!api.wait_for_commit());
    assert!(!store.committed.load(Ordering::SeqCst));
    assert!(!store.waited.load(Ordering::SeqCst));
  }

  #[test]
  fn non_cluster_always_allowed() {
    let store = MockStore::new();
    let api = StoreApi::new(store.clone(), None);
    assert!(api.wait_for_commit());
    assert!(api.commit_aof(0).expect("提交成功"));
    assert!(store.waited.load(Ordering::SeqCst));
    assert!(store.committed.load(Ordering::SeqCst));
  }

  #[test]
  fn flush_db_respects_gate() {
    let store = MockStore::new();
    let gate = RecordingGate::new();
    let api = StoreApi::new(store.clone(), Some(gate.clone()));
    let rt = Runtime::new().expect("运行时");
    rt.block_on(async {
      assert!(api.flush_db(0, false).await.expect("清库成功"));
      assert!(store.flushed.load(Ordering::SeqCst));

      gate.allowed.store(false, Ordering::SeqCst);
      store.flushed.store(false, Ordering::SeqCst);
      assert!(!api.flush_db(0, false).await.expect("路径闭环"));
      assert!(!store.flushed.load(Ordering::SeqCst));
    });
  }
}
