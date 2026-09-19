//! 槽位校验会话侧实现（对标 libs/cluster/Session/SlotVerification/）

use std::sync::{Arc, atomic::Ordering};

use wnode::{
  ClusterSlotVerificationInput, SlotVerifyGate, extract_keys_from_slice, resp::slow_path::SlowWait,
};

use super::ClusterSession;
use crate::server::{
  cluster_manager::{ClusterManager, GateVerdict, IterativeGate, SlotVerifyRequest, SlotWaitMemo},
  slot_verify::{
    ClusterSlotVerificationState, SlotVerifiedState, SlotVerifySessionState,
    write_slot_verification_message,
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

  /// libs/cluster/Session/SlotVerification/RespClusterSlotVerify.cs:Redirect（槽位非本地属主 → MOVED）
  pub(super) fn redirect_slot(&self, slot: u16, output: &mut Vec<u8>) {
    if let Some(m) = self.cluster_manager() {
      write_slot_verification_message(
        &m.current_config(),
        ClusterSlotVerificationState::new(SlotVerifiedState::Moved, slot),
        self.preferred_endpoint_type(),
        output,
      );
    }
  }

  /// libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs:NetworkIterativeSlotVerify
  ///
  /// 事务 Prepare 段逐键迭代校验（C# TxnKeyManager.VerifyKeyOwnership 的
  /// 下游）：首键初始化缓存，后续键状态漂移 → TRYAGAIN（库级定槽下整批键恒
  /// 共 `slot`，C# 的跨槽 CROSSSLOT 裁决已废）；失败后缓存记首个错误由
  /// [`Self::write_cached_slot_verification_message`] 落线
  ///
  /// C# CanOperateOnKey 在网络线程 `Thread.Yield` 自旋等待迁移推进；compio
  /// 一线程一 CPU 下同步自旋饿死同线程迁移驱动，故 Pending 立即返回 false
  /// 中止本次 prepare（不落错误：缓存未步进保持 Ok），登记
  /// [`ClusterManager::wait_key_gate`] 等待体（同一超时/同一 memo 口径），
  /// 事务命令面取转挂起回退游标，等待迁移推进/超时后重驱命令重评
  pub fn network_iterative_slot_verify(
    &self,
    key: &[u8],
    read_only: bool,
    session_asking: bool,
    slot: u16,
  ) -> bool {
    let Some(cm) = self.cluster_manager() else {
      return true;
    };
    let session = self.slot_verify_session_state(session_asking);
    // 等待体交接记忆：超时旗标压制等待点（锁序 memo → cache，与
    // network_multi_key_slot_verify 同款）
    let memo = self.slot_wait_memo.lock().clone();
    let verdict = {
      let mut cache = self.iterative_slot_verify.lock();
      cm.iterative_slot_verify(&mut cache, key, slot, read_only, session, memo.as_deref())
    };
    match verdict {
      IterativeGate::Done(ok) => ok,
      IterativeGate::Pending => {
        self.park_gate_wait(
          &cm,
          ParkGateArgs {
            slot,
            keys: &[key],
            read_only,
            session,
            wait_for_stable: false,
          },
          memo,
        );
        false
      }
    }
  }

  /// libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs:WriteCachedSlotVerificationMessage
  ///
  /// 缓存裁决非 OK 时按缓存槽位与当前配置重造错误消息写输出
  ///（C# GetSlotVerificationMessage(config, cachedVerificationResult)）
  pub fn write_cached_slot_verification_message(&self, output: &mut Vec<u8>) {
    let vres = {
      let cache = self.iterative_slot_verify.lock();
      if !cache.initialized() || cache.state() == SlotVerifiedState::Ok {
        return;
      }
      ClusterSlotVerificationState::new(cache.state(), cache.slot())
    };
    let Some(cm) = self.cluster_manager() else {
      return;
    };
    write_slot_verification_message(
      &cm.current_config(),
      vres,
      self.preferred_endpoint_type(),
      output,
    );
  }

  /// libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs:ResetCachedSlotVerificationResult
  ///
  /// 新事务批次起点重置缓存（C# 顺带取 CurrentConfig 快照；rust 逐键取读锁
  /// 无需快照，见 slot_verify 迭代缓存态注释）
  pub fn reset_cached_slot_verification_result(&self) {
    self.iterative_slot_verify.lock().reset();
  }

  /// libs/cluster/Session/SlotVerification/RespClusterSlotVerify.cs:NetworkMultiKeySlotVerify
  ///
  /// 依键规格提取键位做多键槽位校验；键规格未命中键（参数形态不含键）按
  /// C# default 结果放行。C# 的 CanOperateOnKey / WaitForSlotToStabalize 在
  /// 网络线程内联自旋，rust 侧不可同步判定时登记挂起等待体
  ///（[`SlotVerifyGate::Wait`]）交网络泵驱动，等待完成由消费循环重评本命令
  pub(super) fn network_multi_key_slot_verify(
    &self,
    input: &ClusterSlotVerificationInput<'_>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> SlotVerifyGate {
    // 新命令槽校验起点重置迭代缓存（C# ResetCachedSlotVerificationResult）
    self.reset_cached_slot_verification_result();
    let Some(cm) = self.cluster_manager() else {
      return SlotVerifyGate::Serve;
    };
    let extracted;
    let keys = if input.key_specs.is_empty() {
      args
    } else {
      extracted = extract_keys_from_slice(args, input.key_specs, input.is_sub_command);
      extracted.as_slice()
    };
    if keys.is_empty() {
      return SlotVerifyGate::Serve;
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
      GateVerdict::Serve => SlotVerifyGate::Serve,
      GateVerdict::Redirect(vres) => {
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
        SlotVerifyGate::Wait
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
