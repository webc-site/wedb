//! 集群会话抽象面（对标 libs/server/Cluster/IClusterSession.cs；语义对标
//! libs/cluster/Session/ClusterSession.cs 及 SlotVerification/ 分片）
//!
//! C# 为接口 + 分布式实现（ClusterSession：gossip / 迁移 / 复制回放驱动）；
//! 本域按单机 provider 语义对标（见 [`super::i_cluster_provider`] 头注）：
//! 角色恒为主（`ReadWriteSession` 经 `CurrentConfig.IsPrimary` 短路恒真）、
//! 全部槽位归属本节点（单键验证恒 OK，MOVED/ASK/CLUSTERDOWN 不可能）、
//! 集群命令族（MIGRATE / FAILOVER / REPLICAOF / CLUSTER 子命令）与主复制
//! 流为直通空操作。迭代式槽位验证状态机按 C# 原样保留——单机形态下其
//! 判定恒通过，但跨槽 / 重试状态迁移逻辑与分布式实现逐位一致，供上层在
//! 接线分布式 provider 时零改动复用。

use std::sync::Arc;

use crate::{
  acl::user_handle::UserHandle,
  storage::session::common::array_key_iteration_functions::cluster_slot, types::RespCommand,
};

/// 单机形态的当前纪元（C# clusterProvider.GarnetCurrentEpoch 的单机承接；
/// 分布式 epoch 由 ClusterConfig 版本化，单机恒为 1）
pub const SINGLE_NODE_CURRENT_EPOCH: i64 = 1;

/// 槽位验证结果状态（对标 libs/cluster/Session/SlotVerification/ClusterSlotVerificationResult.cs
/// 引用的 SlotVerifiedState）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotVerifiedState {
  /// 验证通过
  Ok,
  /// 多键请求跨槽
  CrossSlot,
  /// 槽位迁移中需重试
  TryAgain,
  /// 槽位归属他节点
  Moved,
}

/// 迭代式验证的缓存结果（C# cachedVerificationResult：状态 + 槽位）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CachedSlotVerification {
  state: SlotVerifiedState,
  slot: u16,
}

/// 缓存槽位验证错误应答文案（C# CmdStrings.RESP_ERR_CROSSSLOT / RESP_ERR_TRYAGAIN）
const RESP_ERR_CROSSSLOT: &str = "CROSSSLOT Keys in request don't hash to the same slot";
const RESP_ERR_TRYAGAIN: &str = "TRYAGAIN Multiple keys request during rehashing of slot";

/// 集群 RESP 会话（单机 provider 语义）
pub struct IClusterSession {
  /// 最近一次 GOSSIP 呈现的远端节点 id（单机无 gossip：恒 None）
  remote_node_id: Option<String>,
  /// 读 / 写会话标志（C# readWriteSession 字段）
  read_write_session: bool,
  /// 是否处于活动复制流（首个 APPENDLOG 握手后置位；单机恒 false）
  is_replicating: bool,
  /// 本地当前纪元（Acquire/Release 配对维护）
  local_current_epoch: i64,
  /// 会话当前鉴权用户（权限检查用）
  user_handle: Option<Arc<UserHandle>>,
  /// 迭代式验证缓存（initialized = C# `initialized` 字段）
  cached_verification: Option<CachedSlotVerification>,
}

impl Default for IClusterSession {
  fn default() -> Self {
    Self::new()
  }
}

impl IClusterSession {
  /// 构造单机集群会话（C# 构造子装配子集：无 provider / txnManager /
  /// 网络发送器依赖，验证缓存未初始化）
  pub fn new() -> Self {
    Self {
      remote_node_id: None,
      read_write_session: false,
      is_replicating: false,
      local_current_epoch: 0,
      user_handle: None,
      cached_verification: None,
    }
  }

  /// libs/server/Cluster/IClusterSession.cs:RemoteNodeId
  ///
  /// 最近一次 GOSSIP 呈现的远端节点 id（单机恒 None）
  pub fn remote_node_id(&self) -> Option<&str> {
    self.remote_node_id.as_deref()
  }

  /// libs/server/Cluster/IClusterSession.cs:ReadWriteSession
  ///
  /// C# `CurrentConfig.IsPrimary || readWriteSession`——单机恒为主，
  /// 本属性恒真（标志位本体见 [`Self::read_write_session_flag`]）
  pub fn read_write_session(&self) -> bool {
    true
  }

  /// 读 / 写会话标志位本体（SetReadOnly/SetReadWrite 的直接产物）
  pub fn read_write_session_flag(&self) -> bool {
    self.read_write_session
  }

  /// libs/server/Cluster/IClusterSession.cs:IsReplicating
  ///
  /// 是否处于活动复制流（单机无复制流：恒 false）
  pub fn is_replicating(&self) -> bool {
    self.is_replicating
  }

  /// libs/server/Cluster/IClusterSession.cs:LocalCurrentEpoch
  pub fn local_current_epoch(&self) -> i64 {
    self.local_current_epoch
  }

  /// libs/server/Cluster/IClusterSession.cs:SetReadOnlySession
  pub fn set_read_only_session(&mut self) {
    self.read_write_session = false;
  }

  /// libs/server/Cluster/IClusterSession.cs:SetReadWriteSession
  pub fn set_read_write_session(&mut self) {
    self.read_write_session = true;
  }

  /// libs/server/Cluster/IClusterSession.cs:AcquireCurrentEpoch
  ///
  /// 采样当前纪元到本地（单机纪元恒 [`SINGLE_NODE_CURRENT_EPOCH`]）
  pub fn acquire_current_epoch(&mut self) {
    self.local_current_epoch = SINGLE_NODE_CURRENT_EPOCH;
  }

  /// libs/server/Cluster/IClusterSession.cs:ReleaseCurrentEpoch
  pub fn release_current_epoch(&mut self) {
    self.local_current_epoch = 0;
  }

  /// libs/server/Cluster/IClusterSession.cs:ProcessClusterCommands
  ///
  /// 集群命令族分派（CLUSTER 子命令 / MIGRATE / FAILOVER / REPLICAOF /
  /// SECONDARYOF）。单机无集群命令面：直通空操作并返回 false（未处理，
  /// 调用方按普通命令继续），与 C# `!EnableCluster` 时 clusterSession
  /// 不接线的语义一致。命令参数仅作入参形态承接
  pub fn process_cluster_commands(&mut self, _command: RespCommand) -> bool {
    false
  }

  /// libs/server/Cluster/IClusterSession.cs:ResetCachedSlotVerificationResult
  ///
  /// 迭代式验证缓存复位（调用方在每个多键命令验证前调用）
  pub fn reset_cached_slot_verification_result(&mut self) {
    self.cached_verification = None;
  }

  /// libs/server/Cluster/IClusterSession.cs:NetworkIterativeSlotVerify
  ///
  /// 迭代式单键验证（跨调用缓存结果；调用方须配对 Reset）：
  /// 首次验证初始化缓存；缓存非 OK 提前短路（捕获首错）；槽位变化 →
  /// CROSSSLOT；状态变化 → TRYAGAIN。单机全槽位自有 → 单键验证恒 OK，
  /// 故正常路径恒真
  pub fn network_iterative_slot_verify(
    &mut self,
    key: &[u8],
    read_only: bool,
    session_asking: u8,
    wait_for_stable_slot: bool,
  ) -> bool {
    let slot = cluster_slot(key);
    let verify = CachedSlotVerification {
      state: self.verify_single_key(slot, read_only, session_asking != 0, wait_for_stable_slot),
      slot,
    };

    match self.cached_verification {
      None => {
        self.cached_verification = Some(verify);
        verify.state == SlotVerifiedState::Ok
      }
      Some(cached) if cached.state != SlotVerifiedState::Ok => false,
      Some(cached) => {
        if verify.slot != cached.slot {
          self.cached_verification = Some(CachedSlotVerification {
            state: SlotVerifiedState::CrossSlot,
            slot: cached.slot,
          });
          return false;
        }
        if verify.state != cached.state {
          self.cached_verification = Some(CachedSlotVerification {
            state: SlotVerifiedState::TryAgain,
            slot: cached.slot,
          });
          return false;
        }
        verify.state == SlotVerifiedState::Ok
      }
    }
  }

  /// 单键槽位归属验证（C# SingleKeySlotVerify 的单机承接）：
  /// 全部槽位归属本节点 → 恒 OK；MOVED/ASK/CLUSTERDOWN 仅分布式形态可达
  fn verify_single_key(
    &self,
    _slot: u16,
    _read_only: bool,
    _session_asking: bool,
    _wait_for_stable_slot: bool,
  ) -> SlotVerifiedState {
    SlotVerifiedState::Ok
  }

  /// libs/server/Cluster/IClusterSession.cs:WriteCachedSlotVerificationMessage
  ///
  /// 缓存验证失败时写错误应答（OK 时不写任何字节）。单机可达状态仅
  /// CROSSSLOT / TRYAGAIN（MOVED 归属分布式）
  pub fn write_cached_slot_verification_message(&self, output: &mut Vec<u8>) {
    if let Some(cached) = self
      .cached_verification
      .filter(|c| c.state != SlotVerifiedState::Ok)
    {
      let message = match cached.state {
        SlotVerifiedState::CrossSlot => RESP_ERR_CROSSSLOT,
        SlotVerifiedState::TryAgain => RESP_ERR_TRYAGAIN,
        SlotVerifiedState::Moved => "MOVED",
        SlotVerifiedState::Ok => unreachable!("filtered above"),
      };
      output.push(b'-');
      output.extend_from_slice(message.as_bytes());
      output.extend_from_slice(b"\r\n");
    }
  }

  /// libs/server/Cluster/IClusterSession.cs:NetworkMultiKeySlotVerify
  ///
  /// 多键槽位验证（验证失败写应答并返回 true；通过返回 false 继续）。
  /// C# 跳过条件：`!EnableCluster || 事务运行中` → false；单机形态
  /// EnableCluster 恒 false → 恒跳过验证
  pub fn network_multi_key_slot_verify(
    &mut self,
    _keys: &[&[u8]],
    _is_txn: bool,
    _output: &mut Vec<u8>,
  ) -> bool {
    false
  }

  /// libs/server/Cluster/IClusterSession.cs:NetworkMultiKeySlotVerifyNoResponse
  ///
  /// 多键槽位验证的无应答变体（true = 验证失败，应答交由调用方）。
  /// 单机形态恒 false（同上跳过语义）
  pub fn network_multi_key_slot_verify_no_response(
    &mut self,
    _keys: &[&[u8]],
    _is_txn: bool,
  ) -> bool {
    false
  }

  /// libs/server/Cluster/IClusterSession.cs:SetUserHandle
  ///
  /// 更新会话当前鉴权用户（ACL 权限检查用）
  pub fn set_user_handle(&mut self, user_handle: Arc<UserHandle>) {
    self.user_handle = Some(user_handle);
  }

  /// 当前鉴权用户句柄
  pub fn user_handle(&self) -> Option<&Arc<UserHandle>> {
    self.user_handle.as_ref()
  }

  /// libs/server/Cluster/IClusterSession.cs:ProcessPrimaryStream
  ///
  /// 主复制流回放入口（微基准挂钩；分布式经 ReplicaReplayDriver 承接）。
  /// 单机无主复制流：直通空操作
  pub fn process_primary_stream(
    &mut self,
    _physical_sublog_idx: usize,
    _record: &[u8],
    _previous_address: i64,
    _current_address: i64,
    _next_address: i64,
  ) {
  }

  /// libs/server/Cluster/IClusterSession.cs:Dispose
  ///
  /// 释放会话资源（C# 另含迁移接收状态与副本回放驱动的释放；单机无
  /// 此类资源，复位状态即可）
  pub fn dispose(&mut self) {
    self.is_replicating = false;
    self.cached_verification = None;
    self.remote_node_id = None;
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::acl::user::User;

  #[test]
  fn read_write_semantics_follow_primary_short_circuit() {
    let mut s = IClusterSession::new();
    // C# IsPrimary || readWriteSession：单机恒为主 → 可写恒真
    assert!(s.read_write_session());
    assert!(!s.read_write_session_flag());
    s.set_read_write_session();
    assert!(s.read_write_session_flag());
    // 置只读后标志位翻转，但单机可写语义仍为真
    s.set_read_only_session();
    assert!(!s.read_write_session_flag());
    assert!(s.read_write_session());
  }

  #[test]
  fn epoch_acquire_release_roundtrip() {
    let mut s = IClusterSession::new();
    assert_eq!(s.local_current_epoch(), 0);
    s.acquire_current_epoch();
    assert_eq!(s.local_current_epoch(), SINGLE_NODE_CURRENT_EPOCH);
    s.release_current_epoch();
    assert_eq!(s.local_current_epoch(), 0);
  }

  #[test]
  fn cluster_command_family_is_passthrough_false() {
    let mut s = IClusterSession::new();
    // 单机无集群命令面：MIGRATE / CLUSTER 子命令均未处理
    assert!(!s.process_cluster_commands(RespCommand::None));
    assert!(!s.process_cluster_commands(RespCommand::Migrate));
  }

  #[test]
  fn iterative_verify_passes_same_slot_and_flags_cross_slot() {
    let mut s = IClusterSession::new();
    s.reset_cached_slot_verification_result();
    // 同哈希标签（同槽位）的多键逐键验证恒真（单机全槽位自有）
    assert!(s.network_iterative_slot_verify(b"user:{100}", true, 0, false));
    assert!(s.network_iterative_slot_verify(b"order:{100}", true, 0, false));

    // 跨槽（不同槽位）：单键验证仍 OK，但缓存按 C# 状态机进入 CROSSSLOT，
    // 本键及后续键均返回 false，缓存消息给出 CROSSSLOT 应答
    let slot = cluster_slot(b"user:{100}");
    let mut probe = 0u32;
    let other_key = loop {
      let key = format!("probe-{probe}");
      if cluster_slot(key.as_bytes()) != slot {
        break key;
      }
      probe += 1;
    };
    assert!(!s.network_iterative_slot_verify(other_key.as_bytes(), true, 0, false));
    assert!(!s.network_iterative_slot_verify(b"another", true, 0, false));
    let mut out = Vec::new();
    s.write_cached_slot_verification_message(&mut out);
    assert!(String::from_utf8_lossy(&out).contains("CROSSSLOT"));
  }

  #[test]
  fn cached_verification_message_silent_on_ok() {
    let mut s = IClusterSession::new();
    s.reset_cached_slot_verification_result();
    assert!(s.network_iterative_slot_verify(b"k", false, 0, false));
    let mut out = Vec::new();
    s.write_cached_slot_verification_message(&mut out);
    // OK 缓存：不写任何字节
    assert!(out.is_empty());
  }

  #[test]
  fn reset_clears_cached_state() {
    let mut s = IClusterSession::new();
    s.reset_cached_slot_verification_result();
    assert!(s.network_iterative_slot_verify(b"k", false, 0, false));
    s.reset_cached_slot_verification_result();
    // 复位后重新以首键初始化缓存
    assert!(s.network_iterative_slot_verify(b"fresh", false, 0, false));
  }

  #[test]
  fn multi_key_verify_skipped_without_cluster() {
    let mut s = IClusterSession::new();
    let keys = [b"k1".as_slice(), b"k2".as_slice()];
    let mut out = Vec::new();
    // !EnableCluster → 跳过验证：返回 false（继续命令）且不写应答
    assert!(!s.network_multi_key_slot_verify(&keys, false, &mut out));
    assert!(out.is_empty());
    assert!(!s.network_multi_key_slot_verify_no_response(&keys, true));
  }

  #[test]
  fn primary_stream_and_remote_id_are_single_node_noops() {
    let mut s = IClusterSession::new();
    assert_eq!(s.remote_node_id(), None);
    assert!(!s.is_replicating());
    // 主复制流直通空操作
    s.process_primary_stream(0, b"record", 0, 64, 128);
    assert!(!s.is_replicating());
  }

  #[test]
  fn user_handle_swap_and_dispose() {
    let mut s = IClusterSession::new();
    assert!(s.user_handle().is_none());
    s.set_user_handle(Arc::new(UserHandle::new(Arc::new(User::new(
      "default".to_string(),
    )))));
    assert!(s.user_handle().is_some());
    s.dispose();
    assert!(s.user_handle().is_some());
    assert!(!s.is_replicating());
    assert_eq!(s.remote_node_id(), None);
  }
}
