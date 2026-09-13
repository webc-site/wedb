//! 集群会话实现（对标 libs/cluster/Session/ClusterSession.cs）
//!
//! 实现 wnode 的 [`ClusterSession`] 切面（IClusterSession 会话侧子集），
//! 向 `RespServerSession` 提供槽位验证、重定向、CLUSTER 命令族与 ROLE
//! 集群分支数据。

use std::sync::{
  Arc,
  atomic::{AtomicBool, Ordering},
};

use parking_lot::Mutex;
use wbase::hash_slot::hash_slot as cluster_slot;
use wkv::WedbStore;
use wnode::{
  ClusterSlotVerificationInput, RoleInfo, StorageSession, cluster_session::ClusterSessionFace,
  extract_keys_from_slice, resp::slow_path::SlowWait,
};
use wresp::RespCommand;

use crate::server::{
  cluster::{ClusterPreferredEndpointType, IClusterProvider},
  cluster_config::LOCAL_WORKER_ID,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  slot_verify::{ClusterSlotVerificationState, SlotVerifySessionState},
};

/// 集群 RESP 会话实现
pub struct ClusterSession {
  cluster_provider: Arc<ClusterProvider>,
  read_only: AtomicBool,
  internal_write: AtomicBool,
  is_replicating: AtomicBool,
  /// CLUSTER RESET 等需异步闭环命令挂起的慢路径执行体
  ///（会话侧经 [`ClusterSessionFace::take_pending_slow`] 取走驱动）
  pending_slow: Mutex<Option<SlowWait>>,
}

impl ClusterSession {
  /// libs/cluster/Session/ClusterSession.cs:ClusterSession（构造）
  pub fn new(cluster_provider: Arc<ClusterProvider>) -> Self {
    Self {
      cluster_provider,
      read_only: AtomicBool::new(false),
      internal_write: AtomicBool::new(false),
      is_replicating: AtomicBool::new(false),
      pending_slow: Mutex::new(None),
    }
  }

  fn cluster_manager(&self) -> Option<Arc<ClusterManager>> {
    self.cluster_provider.cluster_manager()
  }

  /// 槽位验证会话态快照（READONLY / ASKING / 内部写三标志投影）
  fn slot_verify_session_state(&self, session_asking: bool) -> SlotVerifySessionState {
    SlotVerifySessionState {
      session_asking,
      read_only_session: self.read_only.load(Ordering::Relaxed),
      internal_write: self.internal_write.load(Ordering::Relaxed),
    }
  }

  /// libs/cluster/Session/ClusterSession.cs:IsReplicating
  pub fn is_replicating(&self) -> bool {
    self.is_replicating.load(Ordering::Relaxed)
  }

  /// libs/cluster/Session/ClusterSession.cs:SetReplicating
  pub fn set_replicating(&self, rep: bool) {
    self.is_replicating.store(rep, Ordering::Relaxed);
  }

  /// libs/cluster/Session/ClusterSession.cs:IsInternalWriteSession
  pub fn internal_write(&self) -> bool {
    self.internal_write.load(Ordering::Relaxed)
  }

  /// libs/cluster/Session/ClusterSession.cs:SetInternalWriteSession
  pub fn set_internal_write(&self, val: bool) {
    self.internal_write.store(val, Ordering::Relaxed);
  }

  /// libs/cluster/Session/ClusterSession.cs:AcquireCurrentEpoch
  ///
  /// 纪元保护由 wepoch 域独立承接（会话无自旋等待语义）
  pub fn acquire_current_epoch(&self) {}

  /// libs/cluster/Session/ClusterSession.cs:ReleaseCurrentEpoch
  pub fn release_current_epoch(&self) {}
}

impl ClusterSessionFace for ClusterSession {
  /// libs/cluster/Session/ClusterSession.cs:SetReadOnlySession
  fn set_read_only_session(&self) {
    self.read_only.store(true, Ordering::Relaxed);
  }

  /// libs/cluster/Session/ClusterSession.cs:SetReadWriteSession
  fn set_read_write_session(&self) {
    self.read_only.store(false, Ordering::Relaxed);
  }

  /// libs/cluster/Session/SlotVerification/RespClusterSlotVerify.cs:NetworkMultiKeySlotVerify
  ///
  /// 依键规格提取键位做多键槽位校验；键规格未命中键（参数形态不含键）按
  /// C# default 结果放行。`wait_for_stable_slot` 的迁移等待自旋由迁移域
  /// 承接，此处按当前迁移态直接判定（安全重定向）。
  fn network_multi_key_slot_verify(
    &self,
    input: &ClusterSlotVerificationInput<'_>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    let Some(cm) = self.cluster_manager() else {
      return false;
    };
    let keys = extract_keys_from_slice(args, input.key_specs, input.is_sub_command);
    if keys.is_empty() {
      return false;
    }
    let state = cm.verify_multi_key(
      &keys,
      input.read_only,
      self.slot_verify_session_state(input.session_asking > 0),
      ClusterPreferredEndpointType::Ip,
    );
    if state == ClusterSlotVerificationState::Ok {
      false
    } else {
      // MOVED/ASK/CLUSTERDOWN/CROSSSLOT/TRYAGAIN 直写输出（C# WriteClusterSlotVerificationMessage）
      state.write_resp_error(output);
      true
    }
  }

  /// libs/cluster/Session/ClusterSession.cs:ProcessClusterCommands
  ///
  /// CLUSTER 子命令族（`cmd` 为解析器解析后的子命令枚举，对标 C# switch）
  fn process_cluster_commands(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    match cmd {
      RespCommand::ClusterNodes => {
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
        true
      }
      RespCommand::ClusterKeyslot => {
        if args.len() != 1 {
          output
            .extend_from_slice(b"-ERR wrong number of arguments for 'cluster|keyslot' command\r\n");
          return true;
        }
        let slot = cluster_slot(args[0]);
        let mut buf = itoa::Buffer::new();
        output.push(b':');
        output.extend_from_slice(buf.format(slot).as_bytes());
        output.extend_from_slice(b"\r\n");
        true
      }
      RespCommand::ClusterMyid => {
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
        true
      }
      RespCommand::ClusterSlots => {
        let info = self
          .cluster_manager()
          .map(|m| {
            m.current_config()
              .get_slots_info(ClusterPreferredEndpointType::Ip)
          })
          .unwrap_or_default();
        output.extend_from_slice(info.as_bytes());
        true
      }
      RespCommand::ClusterShards => {
        let info = self
          .cluster_manager()
          .map(|m| {
            m.current_config()
              .get_shards_info(None, ClusterPreferredEndpointType::Ip)
          })
          .unwrap_or_default();
        output.extend_from_slice(info.as_bytes());
        true
      }
      RespCommand::ClusterInfo => {
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
        true
      }
      RespCommand::ClusterBumpepoch => {
        if let Some(m) = self.cluster_manager() {
          if m.try_bump_cluster_epoch() {
            output.extend_from_slice(b"+BUMPED\r\n");
          } else {
            output.extend_from_slice(b"+STILL\r\n");
          }
        } else {
          output.extend_from_slice(b"-ERR Cluster not initialized\r\n");
        }
        true
      }
      // libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterReset
      //
      // 同步段仅校验参数（0/1/2 参：SOFT|HARD + 可选过期秒数）；实际闭环
      // （HasKeysInSlots 槽键判定 → TryReset → HARD 清库）为异步域，挂
      // 慢路径执行体由网络泵驱动——对标 C# 网络线程内联 TryReset（含
      // ReleaseCurrentEpoch 纪元让渡）的整段语义
      RespCommand::ClusterReset => {
        if args.len() > 2 {
          output
            .extend_from_slice(b"-ERR wrong number of arguments for 'cluster|reset' command\r\n");
          return true;
        }
        // C# soft = option.EqualsUpperCaseSpanIgnoringCase("SOFT")：仅显式
        // SOFT 为软重置，其余（含 HARD）均为硬重置
        let mut soft = true;
        if let Some(opt) = args.first()
          && !opt.eq_ignore_ascii_case(b"SOFT")
        {
          soft = false;
        }
        let mut expiry_secs: i64 = 60;
        if let Some(exp) = args.get(1) {
          match wresp::strict_i64(exp) {
            Some(v) => expiry_secs = v,
            None => {
              output.extend_from_slice(b"-ERR value is not an integer or out of range.\r\n");
              return true;
            }
          }
        }
        match (self.cluster_manager(), self.cluster_provider.try_store()) {
          (Some(m), Some(store)) => {
            let slots: Vec<u16> = m
              .current_config()
              .get_slot_list(LOCAL_WORKER_ID as u16)
              .into_iter()
              .map(|s| s as u16)
              .collect();
            *self.pending_slow.lock() = Some(SlowWait::new(async move {
              cluster_reset_slow(m, store, slots, soft, expiry_secs).await
            }));
          }
          // 集群管理器或存储未装配：明确报错，绝不静默吞命令
          _ => output.extend_from_slice(b"-ERR Cluster not initialized\r\n"),
        }
        true
      }
      _ => {
        output.extend_from_slice(b"-ERR unknown subcommand or not implemented for 'CLUSTER'\r\n");
        true
      }
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:IsPrimary
  fn is_primary(&self) -> bool {
    IClusterProvider::is_primary(&*self.cluster_provider)
  }

  /// 副本判定（委派 ClusterProvider 实现）
  fn is_replica(&self) -> bool {
    IClusterProvider::is_replica(&*self.cluster_provider)
  }

  /// 主节点复制信息（委派 ClusterProvider 实现）
  fn get_primary_info(&self) -> (waof::AofAddress, Vec<RoleInfo>) {
    IClusterProvider::get_primary_info(&*self.cluster_provider)
  }

  /// 副本自身角色信息（委派 ClusterProvider 实现）
  fn get_replica_info(&self) -> RoleInfo {
    IClusterProvider::get_replica_info(&*self.cluster_provider)
  }

  /// ROLE 命令 usingShardedLog 判定输入（C# serverOptions.AofPhysicalSublogCount，
  /// 经 ReplicationManager sublog 计数）
  fn aof_sublog_count(&self) -> usize {
    self
      .cluster_provider
      .replication_manager()
      .map(|rm| rm.sublog_count())
      .unwrap_or(1)
  }

  /// libs/cluster/Session/ClusterSession.cs:Dispose
  ///
  /// 会话不持网络发送器与存储上下文（并行域各自管理生命周期），无清理动作
  fn dispose(&self) {}

  /// 取走 CLUSTER RESET 等挂起的慢路径执行体（会话主循环转挂网络泵驱动）
  fn take_pending_slow(&self) -> Option<SlowWait> {
    self.pending_slow.lock().take()
  }
}

/// CLUSTER RESET 慢路径执行段（对标 C# `TryReset` + `FlushDB(true)` 整链）
///
/// 1. HasKeysInSlots 槽键判定（C# TryReset 首段）：本节点持有槽上仍有键
///    即拒绝（保守口径：不过滤墓碑与到期键）；
/// 2. `try_reset`：SuspendConfigMerge → 复位恢复态 → 关闭全部集群连接 →
///    新配置（SOFT 保留 nodeId/epoch，HARD 换新 id 且 epoch 归零）；
/// 3. HARD 清库（C# `!soft → clusterProvider.FlushDB(true)`）：删除全部
///    用户键（对标 DatabaseManagerBase.ResetDatabase 清库段；本装配无
///    AOF 实例，无截断动作）。
///
/// 返回完整 RESP 应答字节
async fn cluster_reset_slow(
  manager: Arc<ClusterManager>,
  store: Arc<WedbStore<wdev::SegmentedDevice>>,
  slots: Vec<u16>,
  soft: bool,
  expiry_secs: i64,
) -> Vec<u8> {
  let mut out = Vec::new();
  let Ok(session) = store.new_session() else {
    out.extend_from_slice(b"-ERR slow path storage error\r\n");
    return out;
  };
  {
    let batch = session.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    // 槽键判定（libs/cluster/Server/ClusterManagerWorkerState.cs:TryReset 首段）
    match storage.has_keys_in_slots(&slots).await {
      Ok(true) => {
        out.extend_from_slice(
          b"-ERR CLUSTER RESET can't be called with master nodes containing keys\r\n",
        );
        return out;
      }
      Err(_) => {
        out.extend_from_slice(b"-ERR slow path storage error\r\n");
        return out;
      }
      Ok(false) => {}
    }
    // HARD 清库（C# FlushDB(true)）：删除全部用户键（String/Meta 及其旁路
    // 子键随版本栅栏逻辑失效；与 FLUSHDB 慢路径同一清库入口
    // `StorageSession::delete_all_user_keys`——Meta 键走版本栅栏 + 树文件
    // 排空的完整异步删除，杜绝 try_delete_sync 对复合对象的静默降级丢失）
    if !soft && storage.delete_all_user_keys().await.is_err() {
      out.extend_from_slice(b"-ERR slow path storage error\r\n");
      return out;
    }
  }
  match manager.try_reset(soft, expiry_secs.max(0) as u64) {
    Ok(()) => out.extend_from_slice(b"+OK\r\n"),
    Err(_) => out.extend_from_slice(b"-ERR Cluster reset failed\r\n"),
  }
  out
}
