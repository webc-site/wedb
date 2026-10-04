use std::sync::{Arc, atomic::Ordering};

use wbase::hash_slot::{CLUSTER_SLOT_COUNT, slot_of};
use wedb::server::{
  cluster_config::ClusterConfig,
  cluster_manager::{
    ClusterManager, GateVerdict, MultiKeyGateArgs, SlotVerifyRequest, SlotWaitMemo,
  },
  cluster_provider::ClusterProvider,
  hash_slot::{HashSlot, SlotState},
  replication::diskless_replication::{ScanGateGuard, ScanKeyGate},
  slot_verify::{SlotVerifiedState, SlotVerifySessionState},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole},
};

/// 门评请求束（经生产 multi 入口：单键输入直入同一单键内核）
fn evaluate_key_gate(
  cm: &ClusterManager,
  req: &SlotVerifyRequest,
  memo: Option<&SlotWaitMemo>,
) -> GateVerdict {
  let keys: Vec<&[u8]> = req.keys.iter().map(Vec::as_slice).collect();
  cm.evaluate_multi_key_gate(&keys, MultiKeyGateArgs::from_request(req, memo))
}

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
/// 终评、释门与整门注销放行（经 ClusterManager::evaluate_multi_key_gate，
/// 复用槽位门挂起等待体机制）
#[test]
fn scan_gate_verdicts_via_slot_gate() {
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
    ns: 0,
    db: 0,
    keys: vec![key.to_vec()],
    read_only,
    session: SlotVerifySessionState::default(),
    wait_for_stable: false,
  };

  // Blocking 相：全部槽位写挂起（含不存在键——门评先于存在性探测）、读放行
  assert!(matches!(
    evaluate_key_gate(&cm, &req(b"any:key", SLOT0, false), None),
    GateVerdict::Wait { undecided: None }
  ));
  assert!(matches!(
    evaluate_key_gate(&cm, &req(b"any:key", SLOT0 ^ 1, false), None),
    GateVerdict::Wait { undecided: None }
  ));
  assert!(matches!(
    evaluate_key_gate(&cm, &req(b"any:key", SLOT0, true), None),
    GateVerdict::Serve
  ));

  // 超时强制：TRYAGAIN 终评，绝不放行执行（锚点窗口内放行即双重应用）
  let memo = SlotWaitMemo::new(1);
  memo.exhausted.store(true, Ordering::Release);
  assert!(matches!(
    evaluate_key_gate(&cm, &req(b"any:key", SLOT0, false), Some(&memo)),
    GateVerdict::Redirect(v) if v.state == SlotVerifiedState::TryAgain
  ));

  // Scanning 相：未读键挂起、释门键放行、窗口新建键与未覆盖域放行
  handle.admit_domain(SLOT0, &[b"unread".to_vec(), b"pending".to_vec()]);
  handle.begin_scan();
  assert!(matches!(
    evaluate_key_gate(&cm, &req(b"unread", SLOT0, false), None),
    GateVerdict::Wait { undecided: None }
  ));
  handle.release_key(SLOT0, b"unread");
  assert!(matches!(
    evaluate_key_gate(&cm, &req(b"unread", SLOT0, false), None),
    GateVerdict::Serve
  ));
  assert!(matches!(
    evaluate_key_gate(&cm, &req(b"fresh:key", SLOT0, false), None),
    GateVerdict::Serve
  ));
  assert!(matches!(
    evaluate_key_gate(&cm, &req(b"unread", SLOT0 ^ 1, false), None),
    GateVerdict::Serve
  ));

  // 守卫 Drop：整门注销，全部挂起写放行
  assert!(matches!(
    evaluate_key_gate(&cm, &req(b"pending", SLOT0, false), None),
    GateVerdict::Wait { undecided: None }
  ));
  drop(guard);
  assert!(matches!(
    evaluate_key_gate(&cm, &req(b"pending", SLOT0, false), None),
    GateVerdict::Serve
  ));
}
