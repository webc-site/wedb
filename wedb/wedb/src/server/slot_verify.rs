use crate::server::{
  cluster_config::{ClusterConfig, ClusterPreferredEndpointType},
  hash_slot::SlotState,
  worker::NodeRole,
};

/// 槽位验证结果状态（对标 libs/cluster/Session/SlotVerifiedState.cs）
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterSlotVerificationState {
  /// 验证通过
  Ok,
  /// 槽位未提供服务（单节点故障或主节点处于恢复态）
  ClusterDown,
  /// 槽位已转移至远端节点（MOVED 重定向）
  Moved {
    slot: u16,
    endpoint: String,
    port: i32,
  },
  /// 槽位迁移进行中目标节点重定向（ASK 重定向）
  Ask {
    slot: u16,
    endpoint: String,
    port: i32,
  },
  /// 多键命令中存在不同槽位的键
  CrossSlot,
  /// 多键命令涉及重哈希中状态不一致的槽位
  TryAgain,
}

impl ClusterSlotVerificationState {
  /// libs/cluster/Session/SlotVerification/RespClusterSlotVerify.cs:WriteClusterSlotVerificationMessage
  ///
  /// 按照 Redis Cluster 规范零堆分配输出 RESP 错误字节序列
  pub fn write_resp_error(&self, output: &mut Vec<u8>) {
    match self {
      Self::Ok => {}
      Self::Moved {
        slot,
        endpoint,
        port,
      } => {
        let mut buf = itoa::Buffer::new();
        output.extend_from_slice(b"-MOVED ");
        output.extend_from_slice(buf.format(*slot).as_bytes());
        output.push(b' ');
        output.extend_from_slice(endpoint.as_bytes());
        output.push(b':');
        output.extend_from_slice(buf.format(*port).as_bytes());
        output.extend_from_slice(b"\r\n");
      }
      Self::Ask {
        slot,
        endpoint,
        port,
      } => {
        let mut buf = itoa::Buffer::new();
        output.extend_from_slice(b"-ASK ");
        output.extend_from_slice(buf.format(*slot).as_bytes());
        output.push(b' ');
        output.extend_from_slice(endpoint.as_bytes());
        output.push(b':');
        output.extend_from_slice(buf.format(*port).as_bytes());
        output.extend_from_slice(b"\r\n");
      }
      Self::ClusterDown => {
        output.extend_from_slice(b"-CLUSTERDOWN Hash slot not served\r\n");
      }
      Self::CrossSlot => {
        output.extend_from_slice(b"-CROSSSLOT Keys in request don't hash to the same slot\r\n");
      }
      Self::TryAgain => {
        output.extend_from_slice(b"-TRYAGAIN Multiple keys request during rehashing of slot\r\n");
      }
    }
  }
}

/// libs/cluster/Session/SlotVerification/ClusterSlotVerify.cs:SingleKeySlotVerify
///
/// 1:1 对标 C# SingleKeySlotVerify 槽位验证状态机
pub fn single_key_slot_verify(
  config: &ClusterConfig,
  slot: u16,
  read_only: bool,
  session_asking: bool,
  is_recovering: bool,
  can_access_key: bool,
  pref_type: ClusterPreferredEndpointType,
) -> ClusterSlotVerificationState {
  if read_only {
    single_key_read_slot_verify(
      config,
      slot,
      is_recovering,
      can_access_key,
      session_asking,
      pref_type,
    )
  } else {
    single_key_read_write_slot_verify(
      config,
      slot,
      is_recovering,
      can_access_key,
      session_asking,
      pref_type,
    )
  }
}

/// libs/cluster/Session/SlotVerification/ClusterSlotVerify.cs:SingleKeyReadSlotVerify
fn single_key_read_slot_verify(
  config: &ClusterConfig,
  slot: u16,
  is_recovering: bool,
  can_access_key: bool,
  session_asking: bool,
  pref_type: ClusterPreferredEndpointType,
) -> ClusterSlotVerificationState {
  let is_local = config.is_local(slot, false);
  let state = config.get_state(slot);

  if is_local {
    if is_recovering {
      return match config.local_node_role() {
        NodeRole::Replica => {
          let (endpoint, port) = config.get_endpoint_from_slot(slot, pref_type);
          ClusterSlotVerificationState::Moved {
            slot,
            endpoint,
            port,
          }
        }
        NodeRole::Primary | NodeRole::Unassigned => ClusterSlotVerificationState::ClusterDown,
      };
    }

    match state {
      SlotState::Stable => ClusterSlotVerificationState::Ok,
      SlotState::Migrating => {
        if can_access_key {
          ClusterSlotVerificationState::Ok
        } else {
          let (endpoint, port) = config.ask_endpoint_from_slot(slot, pref_type);
          ClusterSlotVerificationState::Ask {
            slot,
            endpoint,
            port,
          }
        }
      }
      _ => ClusterSlotVerificationState::ClusterDown,
    }
  } else {
    match state {
      SlotState::Stable => {
        let (endpoint, port) = config.get_endpoint_from_slot(slot, pref_type);
        ClusterSlotVerificationState::Moved {
          slot,
          endpoint,
          port,
        }
      }
      SlotState::Importing => {
        if session_asking {
          ClusterSlotVerificationState::Ok
        } else {
          let (endpoint, port) = config.get_endpoint_from_slot(slot, pref_type);
          ClusterSlotVerificationState::Moved {
            slot,
            endpoint,
            port,
          }
        }
      }
      _ => ClusterSlotVerificationState::ClusterDown,
    }
  }
}

/// libs/cluster/Session/SlotVerification/ClusterSlotVerify.cs:SingleKeyReadWriteSlotVerify
fn single_key_read_write_slot_verify(
  config: &ClusterConfig,
  slot: u16,
  is_recovering: bool,
  can_access_key: bool,
  session_asking: bool,
  pref_type: ClusterPreferredEndpointType,
) -> ClusterSlotVerificationState {
  if config.local_node_role() == NodeRole::Replica {
    let (endpoint, port) = config.get_endpoint_from_slot(slot, pref_type);
    return ClusterSlotVerificationState::Moved {
      slot,
      endpoint,
      port,
    };
  }

  let is_local = config.is_local(slot, false);
  let state = config.get_state(slot);

  if is_local {
    if is_recovering {
      return match config.local_node_role() {
        NodeRole::Replica => {
          let (endpoint, port) = config.get_endpoint_from_slot(slot, pref_type);
          ClusterSlotVerificationState::Moved {
            slot,
            endpoint,
            port,
          }
        }
        NodeRole::Primary | NodeRole::Unassigned => ClusterSlotVerificationState::ClusterDown,
      };
    }

    match state {
      SlotState::Stable => ClusterSlotVerificationState::Ok,
      SlotState::Migrating => {
        if can_access_key {
          ClusterSlotVerificationState::Ok
        } else {
          let (endpoint, port) = config.ask_endpoint_from_slot(slot, pref_type);
          ClusterSlotVerificationState::Ask {
            slot,
            endpoint,
            port,
          }
        }
      }
      _ => ClusterSlotVerificationState::ClusterDown,
    }
  } else {
    match state {
      SlotState::Stable => {
        let (endpoint, port) = config.get_endpoint_from_slot(slot, pref_type);
        ClusterSlotVerificationState::Moved {
          slot,
          endpoint,
          port,
        }
      }
      SlotState::Importing => {
        if session_asking {
          ClusterSlotVerificationState::Ok
        } else {
          let (endpoint, port) = config.get_endpoint_from_slot(slot, pref_type);
          ClusterSlotVerificationState::Moved {
            slot,
            endpoint,
            port,
          }
        }
      }
      _ => ClusterSlotVerificationState::ClusterDown,
    }
  }
}

/// libs/cluster/Session/SlotVerification/ClusterSlotVerify.cs:MultiKeySlotVerify
pub fn multi_key_slot_verify<F>(
  config: &ClusterConfig,
  slots: &[u16],
  read_only: bool,
  session_asking: bool,
  is_recovering: bool,
  pref_type: ClusterPreferredEndpointType,
  mut can_access_key: F,
) -> ClusterSlotVerificationState
where
  F: FnMut(usize) -> bool,
{
  if slots.is_empty() {
    return ClusterSlotVerificationState::Ok;
  }
  let first_slot = slots[0];
  let first_res = single_key_slot_verify(
    config,
    first_slot,
    read_only,
    session_asking,
    is_recovering,
    can_access_key(0),
    pref_type,
  );

  for (i, &slot) in slots.iter().enumerate().skip(1) {
    if slot != first_slot {
      return ClusterSlotVerificationState::CrossSlot;
    }
    let res = single_key_slot_verify(
      config,
      slot,
      read_only,
      session_asking,
      is_recovering,
      can_access_key(i),
      pref_type,
    );
    if res != first_res {
      return ClusterSlotVerificationState::TryAgain;
    }
  }

  first_res
}
