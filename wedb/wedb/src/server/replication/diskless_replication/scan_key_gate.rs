//! 无盘全量同步扫描键门（快照活扫描窗口的写栅，TRANSMITTING 键门同款语义）
//!
//! 窗口语义（与 [`replication_snapshot_iterator`] 模块头同源陈述）：rust 快照
//! 源为活存储 live scan，快照覆盖锚取自键门闭窗排空点的日志尾——「记录效果
//! 进快照 ⟺ 记录地址 ≤ 锚」须对增量语义记录（ObjectStoreRMW 的 ReplayInput
//! 载荷，重放非幂等）结构成立，否则锚后写入被快照与 AOF 续推双重应用，主从
//! 永久发散。键门分两相栅写：
//!
//! - `Blocking`（注册 → 锚点）：**全部**写命令挂起等待（覆盖域清单须在窗口
//!   内枚举完成方能按域装载未读键集，枚举前无从按槽裁决——新域在枚举完成后
//!   始终缺席覆盖清单，其数据恰在锚后经 AOF 续推，全阻窗口放行后不损失）；
//!   纪元静止等待排空在途写后取锚，锚点即「全库写静止点」——锚前记录效果
//!   必然全部已含于其后的扫描读值；
//! - `Scanning`（锚点 → 扫描收尾）：锚后并发写按「覆盖域未读键集」栅放——
//!   门内枚举出的键在快照读值装帧完成前继续挂起（其写效果绝不进快照，记录
//!   地址恒 > 锚，副本恰经 AOF 续推应用一次），读值装帧完成即逐键释门；
//!   未枚举键（扫描窗口新建键，含 HINCRBY/LPUSH 类增量建键）与未覆盖域键
//!   （窗口新建域）不在键集、直接放行——其记录地址恒 > 锚且不入快照，副本
//!   同样经续推获得。
//!
//! 域隔离：未读键集按「库级槽位 → 键集」分域登记（库级定槽 doc/zh/db.md
//! 4.1，键内容不参与定槽）——跨域同名用户键物理互异，逐域分册杜绝 A 域读值
//! 装帧误放行 B 域同名键的挂起写（过早放行 = 锚后写先落库再被读值装帧，
//! 双重应用发散）。槽位碰撞（两域同槽）方向安全：挂起过多写、随逐键释门
//! 放行，绝不双重应用。
//!
//! 释放时机：普通键读值装帧（整值物化）完成即释门；RangeIndex 键树快照走
//! 带外分块流、非单点物化，门护至整流送达后释门；扫描扇出任一退出路径经
//! [`ScanGateGuard`] 整门注销，挂起写命令经槽位门等待体重评放行。读命令
//! 全程放行（读不产 AOF 记录、不参与主从收敛）。

use std::sync::{
  Arc,
  atomic::{AtomicU8, Ordering},
};

use wbase::map::{ConcurrentMap, new_concurrent_map};

use super::replication_sync_manager::ReplicationSyncManager;

/// 门相：锚前全阻（全部写命令挂起，纪元排空后取锚）
const PHASE_BLOCKING: u8 = 0;
/// 门相：锚后按覆盖域未读键集栅放（读值装帧即逐键释门）
const PHASE_SCANNING: u8 = 1;

/// 无盘全量同步扫描键门（单批量窗口至多一扇，注册面
/// [`ReplicationSyncManager::register_scan_gate`]；共享 Blocking 窗跨全量
/// 活跃域，逐域未读键集分册）
pub(crate) struct ScanKeyGate {
  /// 门相（Blocking → Scanning 单向推进，以锚点为界）
  phase: AtomicU8,
  /// 覆盖域未读键集（库级槽位 → 键集）：Blocking 相内逐域装载，快照读值
  /// 装帧完成逐键摘除；未收录键/域 = 锚后放行（窗口新建键/新建域，记录恒
  /// 经 AOF 续推）
  ///
  /// 外层与内层同为 papaya 无锁表（对位 SKILL「并发字典、set 用 papaya +
  /// gxhash」，与 C# 侧 ConcurrentDictionary 直取容器同形）：外层承担的全部
  /// 工作只是按 db 号取内层表，故不再套读写锁——套锁会把内层自身的并发读
  /// 能力抵消回单点串行（锁内套无锁）。逐域装载与逐键释门各自压成一次原子
  /// 操作，跨域互不牵连。
  domains: ConcurrentMap<u16, ConcurrentMap<Vec<u8>, ()>>,
}

impl ScanKeyGate {
  /// 构造键门（构造即 Blocking 相，覆盖域清单经 [`Self::admit_domain`] 逐域
  /// 装载后始可切相）
  pub(super) fn new() -> Self {
    Self {
      phase: AtomicU8::new(PHASE_BLOCKING),
      domains: new_concurrent_map(),
    }
  }

  /// 键门裁决：本键写命令是否须挂起（调用方为槽位门评，读命令不入门——
  /// 读不产 AOF 记录、不参与主从收敛）
  #[inline]
  pub(crate) fn blocks_write(&self, key: &[u8], slot: u16) -> bool {
    match self.phase.load(Ordering::Acquire) {
      PHASE_BLOCKING => true,
      _ => {
        let domains = self.domains.pin();
        domains
          .get(&slot)
          .is_some_and(|unread| unread.pin().get(key).is_some())
      }
    }
  }

  /// 装载单域未读键集（Blocking 相内逐域完成：全部覆盖域装载后始可切相，
  /// 杜绝「已切 Scanning 而键集未装载」的空窗放行）
  pub(super) fn admit_domain(&self, slot: u16, keys: &[Vec<u8>]) {
    let unread = new_concurrent_map();
    {
      let pin = unread.pin();
      for key in keys {
        pin.insert(key.clone(), ());
      }
    }
    // 单操作覆盖装载（同槽后装者胜，与原写锁 insert 同判据；勿改成「缺席才
    // 插」——那是改覆盖域判定，超出本票射程）
    self.domains.pin().insert(slot, unread);
  }

  /// 切入 Scanning 相（锚点已取、全部覆盖域未读键集已装载后调用；此后写
  /// 命令按域未读键集栅放，读值装帧释门）
  pub(super) fn begin_scan(&self) {
    self.phase.store(PHASE_SCANNING, Ordering::Release);
  }

  /// 释门：键快照读值装帧（或带外整流送达）完成，该域该键挂起写命令经
  /// 等待体重评放行（记录地址恒 > 锚，恰经 AOF 续推应用一次）
  pub(super) fn release_key(&self, slot: u16, key: &[u8]) {
    if self.phase.load(Ordering::Acquire) != PHASE_SCANNING {
      return;
    }
    // 点查取域册后逐键摘除：无外层锁可跨这两步持有，也无需——摘除本身就是
    // 内层无锁表的单次原子操作
    let domains = self.domains.pin();
    if let Some(unread) = domains.get(&slot) {
      unread.pin().remove(key);
    }
  }
}

/// 扫描键门生命周期守卫（RAII：扫描扇出任一退出路径——成功收尾、会话判败
/// 摘除、读失败上抛、panic 展开——整门注销，挂起写命令随即重评放行）
pub(super) struct ScanGateGuard {
  mgr: Arc<ReplicationSyncManager>,
}

impl ScanGateGuard {
  /// 注册键门并取守卫（已注册即拒绝：批量窗口 sync_in_progress 保证单扇，
  /// 重复注册属调用方编排缺陷，显式失败上抛）
  pub(super) fn register(
    mgr: &Arc<ReplicationSyncManager>,
    gate: Arc<ScanKeyGate>,
  ) -> Result<Self, String> {
    mgr.register_scan_gate(gate)?;
    Ok(Self {
      mgr: Arc::clone(mgr),
    })
  }
}

impl Drop for ScanGateGuard {
  fn drop(&mut self) {
    self.mgr.clear_scan_gate();
  }
}

#[cfg(test)]
mod tests {
  use std::sync::atomic::Ordering;

  use super::*;

  #[test]
  fn scan_key_gate_blocks_then_releases_per_domain() {
    let gate = ScanKeyGate::new();

    // Blocking 相：全部槽位写一律挂起（覆盖域清单枚举前无从按槽裁决）
    assert!(gate.blocks_write(b"any:key", 7));
    assert!(gate.blocks_write(b"any:key", 8));

    // 逐域装载未读键集并切相后：仅覆盖域未读键挂起，未收录键（窗口新建）
    // 与未覆盖域（窗口新建域）放行
    gate.admit_domain(7, &[b"gated".to_vec(), b"pending".to_vec()]);
    gate.admit_domain(9, &[b"other".to_vec()]);
    gate.begin_scan();
    assert!(gate.blocks_write(b"gated", 7));
    assert!(gate.blocks_write(b"pending", 7));
    assert!(gate.blocks_write(b"other", 9));
    assert!(!gate.blocks_write(b"fresh:key", 7), "窗口新建键锚后放行");
    assert!(!gate.blocks_write(b"gated", 8), "未覆盖域放行");
    assert!(
      !gate.blocks_write(b"pending", 9),
      "跨域同名键分册隔离：B 域同名键不受 A 域未读集牵连"
    );

    // 逐域逐键释门：读值装帧完成的键放行，其余仍挂起
    gate.release_key(7, b"gated");
    assert!(!gate.blocks_write(b"gated", 7));
    assert!(gate.blocks_write(b"pending", 7));
    gate.release_key(9, b"other");
    assert!(!gate.blocks_write(b"other", 9));
  }

  /// 门评集成契约：扫描键门经槽位门透出——写挂起/读放行、超时强制 TRYAGAIN
  /// 终评、释门与整门注销放行（经 ClusterManager::evaluate_key_gate，复用
  /// 槽位门挂起等待体机制）
  #[test]
  fn scan_gate_verdicts_via_slot_gate() {
    use wbase::hash_slot::{CLUSTER_SLOT_COUNT, slot_of};

    use crate::server::{
      cluster_config::ClusterConfig,
      cluster_manager::{ClusterManager, GateVerdict, SlotVerifyRequest, SlotWaitMemo},
      cluster_provider::ClusterProvider,
      hash_slot::{HashSlot, SlotState},
      slot_verify::{SlotVerifiedState, SlotVerifySessionState},
      worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole},
    };

    const SLOT0: u16 = slot_of(0, 0);

    let provider = ClusterProvider::new();
    provider.initialize_replication_manager(1, None, false);
    let cm = Arc::new(ClusterManager::new(Arc::clone(&provider)));
    {
      let mut config = ClusterConfig::new();
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: 1,
        address: "127.0.0.1",
        port: 7000,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: None,
      });
      for s in 0..CLUSTER_SLOT_COUNT {
        config.slot_map[s] = HashSlot {
          worker_id: LOCAL_WORKER_ID as u16,
          state: SlotState::Stable,
        };
      }
      *cm.current_config.write() = config;
    }
    *provider.cluster_manager.write() = Some(Arc::clone(&cm));

    let sync = Arc::clone(
      &provider
        .replication_manager()
        .unwrap()
        .replication_sync_manager,
    );
    let gate = Arc::new(ScanKeyGate::new());
    let handle = Arc::clone(&gate);
    let guard = ScanGateGuard::register(&sync, gate).unwrap();

    let req = |key: &[u8], slot: u16, read_only: bool| SlotVerifyRequest {
      slot,
      keys: vec![key.to_vec()],
      read_only,
      session: SlotVerifySessionState::default(),
      wait_for_stable: false,
    };

    // Blocking 相：全部槽位写挂起（含不存在键——门评先于存在性探测）、读放行
    assert!(matches!(
      cm.evaluate_key_gate(&req(b"any:key", SLOT0, false), None),
      GateVerdict::Wait { undecided: None }
    ));
    assert!(matches!(
      cm.evaluate_key_gate(&req(b"any:key", SLOT0 ^ 1, false), None),
      GateVerdict::Wait { undecided: None }
    ));
    assert!(matches!(
      cm.evaluate_key_gate(&req(b"any:key", SLOT0, true), None),
      GateVerdict::Serve
    ));

    // 超时强制：TRYAGAIN 终评，绝不放行执行（锚点窗口内放行即双重应用）
    let memo = SlotWaitMemo::new(1);
    memo.exhausted.store(true, Ordering::Release);
    assert!(matches!(
      cm.evaluate_key_gate(&req(b"any:key", SLOT0, false), Some(&memo)),
      GateVerdict::Redirect(v) if v.state == SlotVerifiedState::TryAgain
    ));

    // Scanning 相：未读键挂起、释门键放行、窗口新建键与未覆盖域放行
    handle.admit_domain(SLOT0, &[b"unread".to_vec(), b"pending".to_vec()]);
    handle.begin_scan();
    assert!(matches!(
      cm.evaluate_key_gate(&req(b"unread", SLOT0, false), None),
      GateVerdict::Wait { undecided: None }
    ));
    handle.release_key(SLOT0, b"unread");
    assert!(matches!(
      cm.evaluate_key_gate(&req(b"unread", SLOT0, false), None),
      GateVerdict::Serve
    ));
    assert!(matches!(
      cm.evaluate_key_gate(&req(b"fresh:key", SLOT0, false), None),
      GateVerdict::Serve
    ));
    assert!(matches!(
      cm.evaluate_key_gate(&req(b"unread", SLOT0 ^ 1, false), None),
      GateVerdict::Serve
    ));

    // 守卫 Drop：整门注销，全部挂起写放行
    assert!(matches!(
      cm.evaluate_key_gate(&req(b"pending", SLOT0, false), None),
      GateVerdict::Wait { undecided: None }
    ));
    drop(guard);
    assert!(matches!(
      cm.evaluate_key_gate(&req(b"pending", SLOT0, false), None),
      GateVerdict::Serve
    ));
  }
}
