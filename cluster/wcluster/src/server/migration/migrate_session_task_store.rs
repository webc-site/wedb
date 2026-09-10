use std::sync::Arc;

use gxhash::HashSet;
use parking_lot::RwLock;

use crate::server::{
  cluster_config::MAX_HASH_SLOT_VALUE,
  cluster_provider::ClusterProvider,
  migration::{
    migrate_session::{MigrateSession, MigrateTaskSpec},
    sketch::Sketch,
  },
};

/// libs/cluster/Server/Migration/MigrateSessionTaskStore.cs:MigrateSessionTaskStore
///
/// 与 C# 一致用单把锁同时保护 `disposed` 与 `sessions`：原实现拆成两把锁且
/// dispose（disposed→sessions）与 try_add（sessions→disposed）获取顺序相反，
/// 并发下存在死锁窗口
pub struct MigrateSessionTaskStore {
  state: RwLock<StoreState>,
}

struct StoreState {
  disposed: bool,
  sessions: Vec<Option<Arc<MigrateSession>>>,
}

impl MigrateSessionTaskStore {
  /// libs/cluster/Server/Migration/MigrateSessionTaskStore.cs:MigrateSessionTaskStore
  pub fn new() -> Self {
    Self {
      state: RwLock::new(StoreState {
        disposed: false,
        sessions: vec![None; MAX_HASH_SLOT_VALUE],
      }),
    }
  }

  /// libs/cluster/Server/Migration/MigrateSessionTaskStore.cs:Dispose
  pub fn dispose(&self) {
    let mut state = self.state.write();
    if state.disposed {
      return;
    }
    state.disposed = true;
    for s in state.sessions.iter_mut().flatten() {
      s.dispose();
    }
    state.sessions.clear();
  }

  /// libs/cluster/Server/Migration/MigrateSessionTaskStore.cs:GetNumSessions
  ///
  /// 按会话去重计数（C# 以 HashSet<MigrateSession> 引用相等去重），
  /// 而非按槽计数——单会话可覆盖任意多槽
  pub fn get_num_sessions(&self) -> usize {
    let state = self.state.read();
    if state.disposed {
      return 0;
    }
    let mut seen: HashSet<usize> = HashSet::default();
    state
      .sessions
      .iter()
      .flatten()
      .filter(|s| seen.insert(Arc::as_ptr(s) as usize))
      .count()
  }

  /// libs/cluster/Server/Migration/MigrateSessionTaskStore.cs:TryAddMigrateSession
  pub fn try_add_migrate_session(
    &self,
    cluster_provider: Arc<ClusterProvider>,
    spec: MigrateTaskSpec<'_>,
    slots: HashSet<i32>,
    sketch: Sketch,
  ) -> Option<Arc<MigrateSession>> {
    // 先拿写锁整体校验槽位无占用，再构造会话统一占位：
    // 槽集合所有权直移会话，免 HashMap 克隆
    let mut state = self.state.write();
    if state.disposed {
      return None;
    }
    if slots
      .iter()
      .any(|&slot| state.sessions[slot as usize].is_some())
    {
      return None;
    }

    let m_session = Arc::new(MigrateSession::new(cluster_provider, spec, slots, sketch));
    for slot in m_session.get_slots() {
      state.sessions[*slot as usize] = Some(Arc::clone(&m_session));
    }
    Some(m_session)
  }

  /// libs/cluster/Server/Migration/MigrateSessionTaskStore.cs:TryRemove
  pub fn try_remove(&self, m_session: Arc<MigrateSession>) -> bool {
    let mut state = self.state.write();
    if state.disposed {
      return false;
    }
    for slot in m_session.get_slots() {
      state.sessions[*slot as usize] = None;
    }
    m_session.dispose();
    true
  }

  /// Overload of [Self::try_remove] taking target_node_id (MigrateSessionTaskStore.cs:TryRemove)
  pub fn try_remove_node(&self, target_node_id: &str) -> bool {
    let mut state = self.state.write();
    if state.disposed {
      return false;
    }
    for s in state.sessions.iter_mut() {
      if let Some(sess) = s.as_ref()
        && sess.target_node_id == target_node_id
      {
        sess.dispose();
        *s = None;
      }
    }
    true
  }

  /// libs/cluster/Server/Migration/MigrateSessionTaskStore.cs:CanAccessKey
  pub fn can_access_key(&self, key: &[u8], slot: i32, read_only: bool) -> bool {
    let state = self.state.read();
    if state.disposed {
      return true;
    }
    match &state.sessions[slot as usize] {
      Some(s) => s.can_access_key(key, slot, read_only),
      None => true,
    }
  }
}

impl Default for MigrateSessionTaskStore {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::server::migration::{migrate_session::MigrateTaskSpec, migrate_state::MigrateState};

  /// 默认入参：仅目标节点 id 可变
  fn spec(target_node_id: &str) -> MigrateTaskSpec<'_> {
    MigrateTaskSpec {
      source_node_id: "src",
      target_address: "10.0.0.1",
      target_port: 7000,
      target_node_id,
      username: "",
      passwd: "",
      copy_option: false,
      replace_option: false,
      timeout: 0,
    }
  }

  fn session(slots: HashSet<i32>) -> Arc<MigrateSession> {
    Arc::new(MigrateSession::new(
      Arc::new(ClusterProvider {}),
      spec("dst"),
      slots,
      Sketch::new(),
    ))
  }

  fn slots(items: &[i32]) -> HashSet<i32> {
    items.iter().copied().collect()
  }

  #[test]
  fn add_count_remove_lifecycle() {
    let store = MigrateSessionTaskStore::new();
    // 单会话覆盖 3 槽：去重计数为 1 而非 3
    let s1 = store
      .try_add_migrate_session(
        Arc::new(ClusterProvider {}),
        spec("dst"),
        slots(&[1, 2, 3]),
        Sketch::new(),
      )
      .unwrap();
    assert_eq!(store.get_num_sessions(), 1);

    // 槽位重叠的新会话被拒
    assert!(
      store
        .try_add_migrate_session(
          Arc::new(ClusterProvider {}),
          spec("dst2"),
          slots(&[3, 4]),
          Sketch::new(),
        )
        .is_none()
    );
    assert_eq!(store.get_num_sessions(), 1, "被拒会话不留残留");

    // 不重叠会话可加，去重后为 2
    store
      .try_add_migrate_session(
        Arc::new(ClusterProvider {}),
        spec("dst2"),
        slots(&[4]),
        Sketch::new(),
      )
      .unwrap();
    assert_eq!(store.get_num_sessions(), 2);

    assert!(store.try_remove(s1));
    assert_eq!(store.get_num_sessions(), 1);
    assert!(store.can_access_key(b"k", 1, false), "槽位释放后可访问");
  }

  #[test]
  fn remove_node_and_dispose() {
    let store = MigrateSessionTaskStore::new();
    store
      .try_add_migrate_session(
        Arc::new(ClusterProvider {}),
        spec("dst"),
        slots(&[1]),
        Sketch::new(),
      )
      .unwrap();
    assert!(store.try_remove_node("dst"));
    assert_eq!(store.get_num_sessions(), 0);

    store.dispose();
    // dispose 后拒绝新会话且计数归零
    assert!(
      store
        .try_add_migrate_session(
          Arc::new(ClusterProvider {}),
          spec("dst"),
          slots(&[1]),
          Sketch::new(),
        )
        .is_none()
    );
    assert_eq!(store.get_num_sessions(), 0);
    assert!(store.can_access_key(b"k", 1, false));
    // 会话状态默认值（防止构造漂移）
    assert_eq!(session(slots(&[1])).status, MigrateState::Pending);
  }
}
