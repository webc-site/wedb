//! 集群会话实现（对标 libs/cluster/Session/ClusterSession.cs）

use std::sync::{
  Arc,
  atomic::{AtomicBool, Ordering},
};

use parking_lot::{Mutex, RwLock};
use wbase::hash_slot::hash_slot as cluster_slot;

use crate::server::{
  cluster::{ClusterPreferredEndpointType, ClusterSlotVerificationInput, IClusterSession},
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  slot_verify::ClusterSlotVerificationState,
};

/// 集群 RESP 会话实现
pub struct ClusterSession {
  cluster_provider: Arc<ClusterProvider>,
  remote_node_id: RwLock<Option<String>>,
  read_only: AtomicBool,
  internal_write: AtomicBool,
  is_replicating: AtomicBool,
  cached_slot_error: Mutex<Option<Vec<u8>>>,
}

impl ClusterSession {
  pub fn new(cluster_provider: Arc<ClusterProvider>) -> Self {
    Self {
      cluster_provider,
      remote_node_id: RwLock::new(None),
      read_only: AtomicBool::new(false),
      internal_write: AtomicBool::new(false),
      is_replicating: AtomicBool::new(false),
      cached_slot_error: Mutex::new(None),
    }
  }

  fn cluster_manager(&self) -> Option<Arc<ClusterManager>> {
    self.cluster_provider.cluster_manager()
  }
}

impl IClusterSession for ClusterSession {
  fn remote_node_id(&self) -> Option<String> {
    self.remote_node_id.read().clone()
  }

  fn set_remote_node_id(&self, id: Option<String>) {
    *self.remote_node_id.write() = id;
  }

  fn is_read_write_session(&self) -> bool {
    !self.read_only.load(Ordering::Relaxed)
  }

  fn set_read_write_session(&self, rw: bool) {
    self.read_only.store(!rw, Ordering::Relaxed);
  }

  fn is_replicating(&self) -> bool {
    self.is_replicating.load(Ordering::Relaxed)
  }

  fn set_replicating(&self, rep: bool) {
    self.is_replicating.store(rep, Ordering::Relaxed);
  }

  fn internal_write(&self) -> bool {
    self.internal_write.load(Ordering::Relaxed)
  }

  fn set_internal_write(&self, val: bool) {
    self.internal_write.store(val, Ordering::Relaxed);
  }

  fn acquire_current_epoch(&self) {
    // 对齐 C# ClusterSession.AcquireCurrentEpoch
  }

  fn release_current_epoch(&self) {
    // 对齐 C# ClusterSession.ReleaseCurrentEpoch
  }

  fn process_cluster_commands(&self, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    if args.is_empty() {
      output.extend_from_slice(b"-ERR unknown cluster subcommand\r\n");
      return true;
    }

    let subcmd = args[0];
    if subcmd.eq_ignore_ascii_case(b"NODES") {
      let info = self
        .cluster_manager()
        .map(|m| {
          m.current_config()
            .get_cluster_info(Some(&self.cluster_provider))
        })
        .unwrap_or_default();
      let mut buf = itoa::Buffer::new();
      output.push(b'$');
      output.extend_from_slice(buf.format(info.len()).as_bytes());
      output.extend_from_slice(b"\r\n");
      output.extend_from_slice(info.as_bytes());
      output.extend_from_slice(b"\r\n");
      return true;
    }

    if subcmd.eq_ignore_ascii_case(b"KEYSLOT") {
      if args.len() < 2 {
        output
          .extend_from_slice(b"-ERR wrong number of arguments for 'cluster|keyslot' command\r\n");
        return true;
      }
      let slot = cluster_slot(args[1]);
      let mut buf = itoa::Buffer::new();
      output.push(b':');
      output.extend_from_slice(buf.format(slot).as_bytes());
      output.extend_from_slice(b"\r\n");
      return true;
    }

    if subcmd.eq_ignore_ascii_case(b"MYID") {
      let mut buf = itoa::Buffer::new();
      if let Some(m) = self.cluster_manager() {
        let config = m.current_config();
        let myid = config.local_node_id().unwrap_or("");
        output.push(b'$');
        output.extend_from_slice(buf.format(myid.len()).as_bytes());
        output.extend_from_slice(b"\r\n");
        output.extend_from_slice(myid.as_bytes());
        output.extend_from_slice(b"\r\n");
      } else {
        output.extend_from_slice(b"$0\r\n\r\n");
      }
      return true;
    }

    if subcmd.eq_ignore_ascii_case(b"SLOTS") {
      let info = self
        .cluster_manager()
        .map(|m| {
          m.current_config()
            .get_slots_info(ClusterPreferredEndpointType::Ip)
        })
        .unwrap_or_default();
      output.extend_from_slice(info.as_bytes());
      return true;
    }

    if subcmd.eq_ignore_ascii_case(b"SHARDS") {
      let info = self
        .cluster_manager()
        .map(|m| {
          m.current_config()
            .get_shards_info(None, ClusterPreferredEndpointType::Ip)
        })
        .unwrap_or_default();
      output.extend_from_slice(info.as_bytes());
      return true;
    }

    if subcmd.eq_ignore_ascii_case(b"INFO") {
      let info = self
        .cluster_manager()
        .map(|m| m.get_info())
        .unwrap_or_default();
      let mut buf = itoa::Buffer::new();
      output.push(b'$');
      output.extend_from_slice(buf.format(info.len()).as_bytes());
      output.extend_from_slice(b"\r\n");
      output.extend_from_slice(info.as_bytes());
      output.extend_from_slice(b"\r\n");
      return true;
    }

    if subcmd.eq_ignore_ascii_case(b"BUMPEPOCH") {
      if let Some(m) = self.cluster_manager() {
        if m.try_bump_cluster_epoch() {
          output.extend_from_slice(b"+BUMPED\r\n");
        } else {
          output.extend_from_slice(b"+STILL\r\n");
        }
      } else {
        output.extend_from_slice(b"-ERR Cluster not initialized\r\n");
      }
      return true;
    }

    output.extend_from_slice(b"-ERR unknown subcommand or not implemented for 'CLUSTER'\r\n");
    true
  }

  fn network_iterative_slot_verify(&self, key: &[u8], read_only: bool, asking: bool) -> bool {
    let Some(cm) = self.cluster_manager() else {
      return true;
    };
    let state = cm.verify_key(key, read_only, asking, ClusterPreferredEndpointType::Ip);
    if state == ClusterSlotVerificationState::Ok {
      true
    } else {
      let mut err = Vec::new();
      state.write_resp_error(&mut err);
      *self.cached_slot_error.lock() = Some(err);
      false
    }
  }

  fn network_multi_key_slot_verify(
    &self,
    _input: &ClusterSlotVerificationInput,
    _args: &[&[u8]],
  ) -> bool {
    true
  }

  fn take_cached_slot_error(&self) -> Option<Vec<u8>> {
    self.cached_slot_error.lock().take()
  }

  fn dispose(&self) {}
}
