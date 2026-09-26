//! 槽位校验会话侧实现（对标 libs/cluster/Session/SlotVerification/）

use std::sync::{Arc, atomic::Ordering};

use wnode::{ClusterSlotVerificationInput, SlotVerifyGate, resp::slow_path::SlowWait};
use wresp::{
  catalog::extract_keys_from_slice,
  cmd_strings::{
    cluster::{ERR_CLUSTERDOWN, write_redirect_error},
    write_error_raw,
  },
};

use super::ClusterSession;
use crate::server::{
  cluster_manager::{ClusterManager, GateVerdict, SlotVerifyRequest, SlotWaitMemo},
  slot_verify::{
    ClusterSlotVerificationState, SlotVerifySessionState, write_slot_verification_message,
  },
};

impl ClusterSession {
  /// 槽位验证会话态快照（READONLY / ASKING 双标志投影）
  #[inline]
  pub(super) fn slot_verify_session_state(&self, session_asking: bool) -> SlotVerifySessionState {
    SlotVerifySessionState {
      session_asking,
      read_only_session: self.read_only.load(Ordering::Relaxed),
    }
  }

  /// libs/cluster/Session/SlotVerification/RespClusterSlotVerify.cs:Redirect
  /// 槽位非本地属主重定向：port != 0 写入 MOVED，port == 0 未指派兜底 CLUSTERDOWN
  pub(super) fn redirect_slot(&self, slot: u16, output: &mut Vec<u8>) {
    if let Some(m) = self.cluster_manager() {
      let config = m.current_config();
      let (endpoint, port) = config.get_endpoint_from_slot(slot, self.preferred_endpoint_type());
      if port != 0 {
        write_redirect_error(output, "MOVED", slot, &endpoint, port);
      } else {
        write_error_raw(output, ERR_CLUSTERDOWN);
      }
    }
  }

  /// libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs:ResetCachedSlotVerificationResult
  /// libs/server/Cluster/IClusterSession.cs:ResetCachedSlotVerificationResult
  ///（接口声明折叠：rust 单实现，接口层默认钩子在 wnode ClusterSessionFace）
  ///
  /// 新事务批次起点重置缓存（C# 顺带取 CurrentConfig 快照；rust 逐键取读锁
  /// 无需快照，见 slot_verify 迭代缓存态注释）
  pub fn reset_cached_slot_verification_result(&self) {
    self.iterative_slot_verify.lock().reset();
    self.slot_wait_memo.lock().take();
  }

  /// libs/cluster/Session/SlotVerification/RespClusterSlotVerify.cs:NetworkMultiKeySlotVerify
  ///
  /// 依键规格提取键位做多键槽位校验；键规格未命中键（参数形态不含键）按
  /// C# default 结果放行。C# 的 CanOperateOnKey / WaitForSlotToStabalize 在
  /// 网络线程内联自旋，rust 侧不可同步判定时登记挂起等待体
  ///（[`SlotVerifyGate::Wait`]）交网络泵驱动，等待完成由消费循环重评本命令
  ///
  /// 判定与渲染两分：裁决全在 [`Self::evaluate_multi_key_slot_gate`]（无应答
  /// 臂 [`Self::network_multi_key_slot_verify_no_response`] 共用同一判定核），
  /// 本口只承接 Redirect 态的渲染出口——C# 同名两方法的分离形态
  pub(super) fn network_multi_key_slot_verify(
    &self,
    input: &ClusterSlotVerificationInput<'_>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> SlotVerifyGate {
    match self.evaluate_multi_key_slot_gate(input, args) {
      SlotGateDecision::Serve => SlotVerifyGate::Serve,
      SlotGateDecision::Redirect { cm, vres } => {
        // MOVED/ASK/CLUSTERDOWN/TRYAGAIN 经渲染单点写输出
        //（C# WriteClusterSlotVerificationMessage）
        write_slot_verification_message(
          &cm.current_config(),
          vres,
          self.preferred_endpoint_type(),
          output,
        );
        SlotVerifyGate::Redirected
      }
      SlotGateDecision::Wait => SlotVerifyGate::Wait,
    }
  }

  /// 多键槽位校验无应答臂：只取判定核的裁决，不写任何输出字节
  ///（C# 的 NetworkMultiKeySlotVerifyNoResponse 与有应答臂共用判定、
  /// 不共用渲染）；true = 不可服务（非放行态，含键门挂起）
  pub(super) fn network_multi_key_slot_verify_no_response(
    &self,
    input: &ClusterSlotVerificationInput<'_>,
    args: &[&[u8]],
  ) -> bool {
    !matches!(
      self.evaluate_multi_key_slot_gate(input, args),
      SlotGateDecision::Serve
    )
  }

  /// 多键槽位判定单点（有应答/无应答两臂共用，对位 C# 判定核 MultiKeySlotVerify）：
  /// 迭代缓存重置 → 键规格提键 → `ClusterManager::evaluate_multi_key_gate` 裁决，
  /// 挂起态就地登记等待体；不触碰调用方输出缓冲，重定向字节由有应答臂独占渲染
  fn evaluate_multi_key_slot_gate(
    &self,
    input: &ClusterSlotVerificationInput<'_>,
    args: &[&[u8]],
  ) -> SlotGateDecision {
    // 新命令槽校验起点重置迭代缓存（C# ResetCachedSlotVerificationResult）
    self.reset_cached_slot_verification_result();
    let Some(cm) = self.cluster_manager() else {
      return SlotGateDecision::Serve;
    };
    let extracted;
    let keys = if input.key_specs.is_empty() {
      args
    } else {
      extracted = extract_keys_from_slice(args, input.key_specs, input.is_sub_command);
      extracted.as_slice()
    };
    if keys.is_empty() {
      return SlotGateDecision::Serve;
    }
    let session = self.slot_verify_session_state(input.session_asking > 0);
    // 等待体交接记忆：超时旗标 + 异步存在性裁决缓存（首评无记忆）
    let memo = self.slot_wait_memo.lock().take();
    match cm.evaluate_multi_key_gate(
      keys,
      input.slot,
      input.read_only,
      session,
      input.wait_for_stable_slot,
      memo.as_deref(),
    ) {
      GateVerdict::Serve => SlotGateDecision::Serve,
      // manager 句柄随裁决上抛：渲染臂取当次配置的读锁，不再二次取 manager
      GateVerdict::Redirect(vres) => SlotGateDecision::Redirect { cm, vres },
      GateVerdict::Wait { .. } => {
        self.park_gate_wait(
          &cm,
          ParkGateArgs {
            slot: input.slot,
            keys,
            read_only: input.read_only,
            session,
            wait_for_stable: input.wait_for_stable_slot,
          },
          memo,
        );
        SlotGateDecision::Wait
      }
    }
  }

  /// 挂起等待体登记单点（槽位门 Wait 与迭代门 Pending 共用一处）：
  /// 轮询迁移推进与存活性裁决，超时置位记忆旗标；消费循环回退游标挂起，
  /// 等待完成后重评本命令。等待体须 'static，持 ClusterManager 强引用
  /// 自持（C# CanOperateOnKey / WaitForSlotToStabalize 自旋的 compio 挂起
  /// 投影，超时/记忆口径单点 wait_key_gate）
  fn park_gate_wait(
    &self,
    cm: &Arc<ClusterManager>,
    args: ParkGateArgs<'_>,
    memo: Option<Arc<SlotWaitMemo>>,
  ) {
    let memo = memo.unwrap_or_else(|| Arc::new(SlotWaitMemo::new(args.keys.len())));
    let req = SlotVerifyRequest {
      slot: args.slot,
      keys: args.keys.iter().map(|k| k.to_vec()).collect(),
      read_only: args.read_only,
      session: args.session,
      wait_for_stable: args.wait_for_stable,
    };
    *self.slot_wait_memo.lock() = Some(Arc::clone(&memo));
    let waiter = Arc::clone(cm);
    *self.pending_slow.lock() = Some(SlowWait::new(async move {
      waiter.wait_key_gate(req, memo).await;
      Vec::new()
    }));
  }
}

/// 槽位门挂起等待入参结构体（收敛 park_gate_wait 入参，消除 too-many-arguments）
struct ParkGateArgs<'a> {
  slot: u16,
  keys: &'a [&'a [u8]],
  read_only: bool,
  session: SlotVerifySessionState,
  wait_for_stable: bool,
}

/// 多键槽位判定核出口（`GateVerdict` 的会话侧投影：Redirect 态随裁决上抛判定
/// 所用 manager，渲染臂据此取当次配置读锁，与改前同一实例同一时序）
enum SlotGateDecision {
  /// 放行（C# `vres.state == OK`）
  Serve,
  /// 重定向/错误终态：有应答臂取 manager 渲染错误字节，无应答臂只计裁决
  Redirect {
    cm: Arc<ClusterManager>,
    vres: ClusterSlotVerificationState,
  },
  /// 键门挂起：等待体已在判定核内登记（见 [`ClusterSession::park_gate_wait`]）
  Wait,
}
