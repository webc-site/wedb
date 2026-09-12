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
  /// 按会话去重计数（C# 以 `HashSet<MigrateSession>` 引用相等去重），
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
    if slots.iter().any(|&slot| {
      slot < 0 || slot as usize >= MAX_HASH_SLOT_VALUE || state.sessions[slot as usize].is_some()
    }) {
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
    if slot < 0 || slot as usize >= MAX_HASH_SLOT_VALUE {
      return true;
    }
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
