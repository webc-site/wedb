use std::sync::Arc;

use gxhash::HashSet;
use parking_lot::RwLock;

use crate::server::{
  cluster_provider::ClusterProvider,
  migration::{migrate_session::MigrateSession, migration_manager::TransferOption, sketch::Sketch},
};

/// libs/cluster/Server/Migration/MigrateSessionTaskStore.cs:MigrateSessionTaskStore
pub struct MigrateSessionTaskStore {
  sessions: RwLock<Vec<Option<Arc<MigrateSession>>>>,
  disposed: RwLock<bool>,
}

impl MigrateSessionTaskStore {
  /// libs/cluster/Server/Migration/MigrateSessionTaskStore.cs:MigrateSessionTaskStore
  pub fn new() -> Self {
    Self {
      sessions: RwLock::new(vec![None; 16384]),
      disposed: RwLock::new(false),
    }
  }

  /// libs/cluster/Server/Migration/MigrateSessionTaskStore.cs:Dispose
  pub fn dispose(&mut self) {
    let mut d = self.disposed.write();
    let skip_dispose = *d;
    *d = true;
    if skip_dispose {
      return;
    }
    let mut sessions = self.sessions.write();
    for s in sessions.iter_mut() {
      if let Some(session) = s.take() {
        session.dispose();
      }
    }
  }

  /// libs/cluster/Server/Migration/MigrateSessionTaskStore.cs:GetNumSessions
  pub fn get_num_sessions(&self) -> usize {
    if *self.disposed.read() {
      return 0;
    }
    let mut count = 0;
    let sessions = self.sessions.read();
    for s in sessions.iter() {
      if s.is_some() {
        count += 1;
      }
    }
    count
  }

  /// libs/cluster/Server/Migration/MigrateSessionTaskStore.cs:TryAddMigrateSession
  #[allow(clippy::too_many_arguments)]
  pub fn try_add_migrate_session(
    &self,
    cluster_provider: Arc<ClusterProvider>,
    source_node_id: &str,
    target_address: &str,
    target_port: i32,
    target_node_id: &str,
    username: &str,
    passwd: &str,
    copy_option: bool,
    replace_option: bool,
    timeout: i32,
    slots: HashSet<i32>,
    sketch: Sketch,
    transfer_option: TransferOption,
  ) -> Option<Arc<MigrateSession>> {
    let m_session = Arc::new(MigrateSession::new(
      cluster_provider,
      source_node_id,
      target_address,
      target_port,
      target_node_id,
      username,
      passwd,
      copy_option,
      replace_option,
      timeout,
      slots.clone(),
      sketch,
      transfer_option,
    ));

    let mut sessions = self.sessions.write();
    if *self.disposed.read() {
      return None;
    }

    for &slot in &slots {
      if sessions[slot as usize].is_some() {
        return None;
      }
    }

    for slot in slots {
      sessions[slot as usize] = Some(m_session.clone());
    }

    Some(m_session)
  }

  /// libs/cluster/Server/Migration/MigrateSessionTaskStore.cs:TryRemove
  pub fn try_remove(&self, m_session: Arc<MigrateSession>) -> bool {
    let mut sessions = self.sessions.write();
    if *self.disposed.read() {
      return false;
    }
    for slot in m_session.get_slots() {
      sessions[*slot as usize] = None;
    }
    m_session.dispose();
    true
  }

  /// libs/cluster/Server/Migration/MigrateSessionTaskStore.cs:TryRemove
  pub fn try_remove_node(&self, target_node_id: &str) -> bool {
    let mut sessions = self.sessions.write();
    if *self.disposed.read() {
      return false;
    }
    for i in 0..sessions.len() {
      if let Some(ref s) = sessions[i]
        && s.target_node_id == target_node_id
      {
        s.dispose();
        sessions[i] = None;
      }
    }
    true
  }

  /// libs/cluster/Server/Migration/MigrateSessionTaskStore.cs:CanAccessKey
  pub fn can_access_key(&self, key: &[u8], slot: i32, read_only: bool) -> bool {
    let sessions = self.sessions.read();
    if *self.disposed.read() {
      return true;
    }
    if let Some(ref s) = sessions[slot as usize] {
      s.can_access_key(key, slot, read_only)
    } else {
      true
    }
  }
}

impl Default for MigrateSessionTaskStore {
  fn default() -> Self {
    Self::new()
  }
}
