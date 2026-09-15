use itoa::Buffer;

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
        let mut buf = Buffer::new();
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
        let mut buf = Buffer::new();
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

/// 槽位验证会话态（C# ClusterSession 会话标志在验证链的投影）
///
/// - `session_asking`：ASKING 命令剩余计数（导入槽位放行）
/// - `read_only_session`：READONLY 命令会话级只读态（读路径 enableReplicaReads）
/// - `internal_write`：AOF 重放内部写会话态（读写路径免副本重定向）
#[derive(Debug, Clone, Copy, Default)]
pub struct SlotVerifySessionState {
  /// 会话 ASKING 标记
  pub session_asking: bool,
  /// 会话只读（READONLY）
  pub read_only_session: bool,
  /// 内部写会话（AOF 重放）
  pub internal_write: bool,
}

/// libs/cluster/Session/SlotVerification/ClusterSlotVerify.cs:SingleKeySlotVerify
///
/// 1:1 对标 C# SingleKeySlotVerify 槽位验证状态机。C# 的 CanOperateOnKey
/// （等待至可访问 + Exists 存在性判定）与 WaitForSlotToStabalize（槽位稳定
/// 等待）为调用方内联自旋，rust 侧等待外提为宿主挂起重评，本状态机接收
/// 等待完成后的裁决布尔（`can_operate`）
pub fn single_key_slot_verify(
  config: &ClusterConfig,
  slot: u16,
  read_only: bool,
  session: SlotVerifySessionState,
  is_recovering: bool,
  can_operate: bool,
  pref_type: ClusterPreferredEndpointType,
) -> ClusterSlotVerificationState {
  if read_only {
    single_key_read_slot_verify(config, slot, session, is_recovering, can_operate, pref_type)
  } else {
    single_key_read_write_slot_verify(config, slot, session, is_recovering, can_operate, pref_type)
  }
}

/// libs/cluster/Session/SlotVerification/ClusterSlotVerify.cs:SingleKeyReadSlotVerify
///
/// `can_operate` 为 C# CanOperateOnKey 裁决（自旋等待至键可访问后按
/// Exists 判定：键仍在本地 → true；已迁走/等待超时 → false）
fn single_key_read_slot_verify(
  config: &ClusterConfig,
  slot: u16,
  session: SlotVerifySessionState,
  is_recovering: bool,
  can_operate: bool,
  pref_type: ClusterPreferredEndpointType,
) -> ClusterSlotVerificationState {
  let is_local = config.is_local(slot, session.read_only_session);
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
      // MIGRATING：CanOperateOnKey 等待结束后键仍存在才放行，否则 ASK 重定向
      SlotState::Migrating => {
        if can_operate {
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
        if session.session_asking {
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
///
/// `can_operate` 语义同 [`single_key_read_slot_verify`]；C# 的
/// waitForStableSlot 等待循环（IMPORTING/MIGRATING 期间让出重试）由宿主
/// 外提承接，本函数只在槽位已稳定（或命令不要求稳定等待）后进入
fn single_key_read_write_slot_verify(
  config: &ClusterConfig,
  slot: u16,
  session: SlotVerifySessionState,
  is_recovering: bool,
  can_operate: bool,
  pref_type: ClusterPreferredEndpointType,
) -> ClusterSlotVerificationState {
  // 副本重定向读写请求至主节点（内部写会话 AOF 重放除外）
  if config.local_node_role() == NodeRole::Replica && !session.internal_write {
    let (endpoint, port) = config.get_endpoint_from_slot(slot, pref_type);
    return ClusterSlotVerificationState::Moved {
      slot,
      endpoint,
      port,
    };
  }

  let is_local = config.is_local(slot, session.internal_write);
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
      // MIGRATING：CanOperateOnKey 等待结束后键仍存在才放行，否则 ASK 重定向
      SlotState::Migrating => {
        if can_operate {
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
        if session.session_asking {
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
///
/// `can_operate` 逐键裁决 C# CanOperateOnKey 结果（键下标 → bool）；
/// 键间裁决不一致对齐 C# VerifyKeysInRange 的 TRYAGAIN
pub fn multi_key_slot_verify<F>(
  config: &ClusterConfig,
  slots: &[u16],
  read_only: bool,
  session: SlotVerifySessionState,
  is_recovering: bool,
  pref_type: ClusterPreferredEndpointType,
  mut can_operate: F,
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
    session,
    is_recovering,
    can_operate(0),
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
      session,
      is_recovering,
      can_operate(i),
      pref_type,
    );
    if res != first_res {
      return ClusterSlotVerificationState::TryAgain;
    }
  }

  first_res
}
