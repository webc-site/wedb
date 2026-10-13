#![recursion_limit = "256"]
//! 同槽碰撞域门评探针落域锁测
//!
//! 票 task/ing/wedb-cluster-gate-migrating-probe-root-domain-collision-db-ask.md：
//! 库级定槽把 (ns, db) 混入 14 位槽空间（wbase::hash_slot::slot_of），鸽笼下
//! 必存 slot_of(ns', db') == slot_of(0, 0) 的碰撞库；迁移域门禁只放默认域 (0,0)
//! 槽进 MIGRATING，门评按槽号裁决不甄别请求域——修复前 MIGRATING 存在性探针
//! probe_key_alive 就地 new_session() 后从不落域、恒锚根域 (0,0)，碰撞库键
//! 在其自身域的真实在场态全被漏判：在场键被误判已迁走回 ASK（在场键读到
//! 空、写落错节点），根域同名异键在场时反向误放行。
//!
//! 对标 garnet C# ClusterSlotVerify.cs:116-128 CanOperateOnKey 的 Exists 经
//! 会话当前库（C# 每库独立存储实例天然同域）；rust 单存储多域共存，探针落域
//! 由请求随携 (ns, db) 两标量 + 临时会话 set_context 承接（同 worker_state /
//! slot_mgmt 既有形态）。本套用例锁死：
//! 1. 碰撞库在场键：默认域槽 MIGRATING 窗口内本地 Serve 不回 ASK（读/写双臂、
//!    门评内核与切面入口双面）
//! 2. 根域异键在场不改判碰撞库缺席裁决（碰撞库缺席键仍按 ASK 终评）
//! 3. 异步裁决臂（磁盘候选）同域断言：碰撞库冷化键经 wait_key_gate 异步探针
//!    判活放行；根域冷化异键不反向污染碰撞库缺席终评
//! 4. 根域在迁键（已迁走视角）ASK 现状不回退

use std::sync::{Arc, atomic::Ordering};

use wbase::hash_slot::{CLUSTER_SLOT_COUNT, slot_of};
use wdev::SegmentedDevice;
use wedb::{
  IClusterProvider,
  server::{
    cluster_manager::{GateVerdict, MultiKeyGateArgs, SlotVerifyRequest, SlotWaitMemo},
    cluster_provider::ClusterProvider,
    cluster_session::ClusterSession,
    hash_slot::{HashSlot, SlotState},
    migration::{migrate_session::MigrateTaskSpec, sketch::Sketch, sketch_status::SketchStatus},
    slot_verify::{SlotVerifiedState, SlotVerifySessionState},
    worker::LOCAL_WORKER_ID,
  },
};
use wedb_test::{
  cluster_seed::seed_local_worker,
  store_node::{StoreNode, open_store},
};
use wkv::WedbStore;
use wnode::{
  ClusterSessionFace, ClusterSlotVerificationInput, SlotVerifyGate, storage::StorageSession,
};

/// 默认会话 (0,0) 库槽位（库级定槽 doc/zh/db.md 4.1：键内容不参与定槽）
const SLOT0: u16 = slot_of(0, 0);

/// 会话标志默认快照（无 ASKING / 非 READONLY）
const SESSION_DEFAULT: SlotVerifySessionState = SlotVerifySessionState {
  session_asking: false,
  read_only_session: false,
};

/// 现算与根域 (0,0) 同槽的碰撞库号（鸽笼必存在；slot_of 纯函数，期望万级内命中）
fn collision_db() -> u64 {
  (1..u64::MAX)
    .find(|&db| slot_of(0, db) == SLOT0)
    .expect("14 位槽空间内 (0, db) 与 (0, 0) 碰撞对必存在")
}

/// 装配本地主节点拓扑（0..16384 全部槽位归本地，node_tgt 为迁移目标）
fn setup_primary() -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().unwrap();
  {
    let mut config = m.current_config.write();
    seed_local_worker(
      &mut config,
      0x0DE1_0000_0000_0000_0000_0000_0000_0001,
      7000,
      1,
      Some((0x0000_0000_0000_0000_0000_0000_0000_2E72, 7001, 1)),
      false,
    );
    for s in 0..CLUSTER_SLOT_COUNT {
      config.slot_map[s] = HashSlot {
        worker_id: LOCAL_WORKER_ID as u16,
        state: SlotState::Stable,
      };
    }
  }
  cp
}

/// 置默认域槽位 MIGRATING（迁移域门禁只放 (0,0) 域槽进迁，碰撞库共享同一槽号）
fn prepare_migrating(cp: &ClusterProvider, slot: u16) {
  cp.cluster_manager()
    .unwrap()
    .try_prepare_slot_for_migration(slot as usize, 0x0000_0000_0000_0000_0000_0000_0000_2E72)
    .unwrap();
}

/// 注册管辖槽位的迁移任务（sketch 收录根域在迁键，Migrated = 已发走视角；
/// 碰撞库键不入 sketch：can_access_key 放行，裁决全落在存在性探针）
fn add_migration_task(cp: &ClusterProvider, slots: &[u16], sketch: Sketch) {
  let spec = MigrateTaskSpec {
    source_node_id: 0x0DE1_0000_0000_0000_0000_0000_0000_0001,
    target_address: "127.0.0.1".to_string(),
    target_port: 7001,
    target_node_id: 0x0000_0000_0000_0000_0000_0000_0000_2E72,
    username: "".to_string(),
    passwd: "".to_string(),
    copy_option: false,
    replace_option: false,
    timeout: 0,
  };
  let slot_set = slots.iter().map(|&s| s as i32).collect();
  cp.migration_manager()
    .unwrap()
    .try_add_migration_task(spec, slot_set, sketch)
    .expect("注册迁移任务失败");
}

/// 指定逻辑域内存键（探针落域同形态：set_context 后批处理会话写 String 域）
async fn put_string_in(
  store: &Arc<WedbStore<SegmentedDevice>>,
  ns: u64,
  db: u64,
  key: &[u8],
  val: &[u8],
) {
  let session = store.new_session().unwrap();
  session.set_context(ns, db);
  let batch = session.enter_batch();
  StorageSession::new_readonly(batch)
    .upsert_string(key, val)
    .await
    .unwrap();
}

/// 断言裁决为 ASK 重定向
fn assert_ask(verdict: GateVerdict) {
  match verdict {
    GateVerdict::Redirect(v) if v.state == SlotVerifiedState::Ask => {}
    other => panic!("应 ASK 重定向，实得 {other:?}"),
  }
}

/// 用例 1：碰撞库在场键在默认域迁移窗口内本地 Serve——修复前探针恒锚根域，
/// 在场态漏判误回 ASK（跨域误路由与数据发散的入口）
#[compio::test]
async fn collision_domain_live_key_serves_under_root_migration() {
  let col_db = collision_db();
  assert_eq!(slot_of(0, col_db), SLOT0, "前置：碰撞库与根域同槽");

  let key = b"col:live";
  let cp = setup_primary();
  let StoreNode { _dir, store } = open_store("domain_collision.db");
  cp.set_store(Arc::clone(&store));
  prepare_migrating(&cp, SLOT0);
  let cm = cp.cluster_manager().unwrap();

  // 默认域迁移窗口在场：根域键已发走（Migrated 视角）
  let sketch = Sketch::new();
  sketch.hash_and_store(b"root:mig");
  sketch.set_status(SketchStatus::Migrated);
  add_migration_task(&cp, &[SLOT0], sketch);

  // 碰撞库存键（其自身 (0, col_db) 域）
  put_string_in(&store, 0, col_db, key, b"v1").await;

  // 门评内核双臂（GET 读 / SET 写）：键在场 → Serve，不回 ASK
  for read_only in [true, false] {
    match cm.evaluate_multi_key_gate(
      &[key],
      MultiKeyGateArgs::new(SLOT0, (0, col_db), read_only, SESSION_DEFAULT, false, None),
    ) {
      GateVerdict::Serve => {}
      other => panic!("碰撞库在场键应本地 Serve（read_only={read_only}），实得 {other:?}"),
    }
  }

  // 请求束臂（multi 入口单键投影，挂起体重评同一请求形态）同判
  let req = SlotVerifyRequest {
    slot: SLOT0,
    ns: 0,
    db: col_db,
    keys: vec![key.to_vec()],
    read_only: true,
    session: SESSION_DEFAULT,
    wait_for_stable: false,
  };
  let req_keys: Vec<&[u8]> = req.keys.iter().map(Vec::as_slice).collect();
  assert!(matches!(
    cm.evaluate_multi_key_gate(&req_keys, MultiKeyGateArgs::from_request(&req, None)),
    GateVerdict::Serve
  ));

  // 切面入口（组装臂 input → 判定核全链）：放行且零重定向字节
  let cs: Arc<ClusterSession> = cp.create_cluster_session();
  let input = ClusterSlotVerificationInput {
    slot: SLOT0,
    ns: 0,
    db: col_db,
    key_specs: &[],
    is_sub_command: false,
    read_only: true,
    session_asking: 0,
    wait_for_stable_slot: false,
  };
  let mut out = Vec::new();
  assert!(matches!(
    cs.network_multi_key_slot_verify(&input, &[key], &mut out),
    SlotVerifyGate::Serve
  ));
  assert!(out.is_empty(), "放行不得写重定向字节");
}

/// 用例 2：根域异键在场不改判碰撞库缺席裁决——修复前探针读根域，根域同名
/// 键在场即反向误放行（碰撞库读 miss 回 nil / 写静默发散的入口）
#[compio::test]
async fn root_key_presence_never_flips_collision_absent_verdict() {
  let col_db = collision_db();
  let ghost = b"col:ghost";
  let absent = b"col:none";
  let cp = setup_primary();
  let StoreNode { _dir, store } = open_store("domain_collision.db");
  cp.set_store(Arc::clone(&store));
  prepare_migrating(&cp, SLOT0);
  let cm = cp.cluster_manager().unwrap();

  let sketch = Sketch::new();
  sketch.hash_and_store(b"root:mig");
  sketch.set_status(SketchStatus::Migrated);
  add_migration_task(&cp, &[SLOT0], sketch);

  // 根域存同名异键；碰撞库两键皆缺席
  put_string_in(&store, 0, 0, ghost, b"root-value").await;

  // 根域同名键在场：碰撞库视角仍缺席 → ASK（不得借根域在场态放行）
  assert_ask(cm.evaluate_multi_key_gate(
    &[ghost],
    MultiKeyGateArgs::new(SLOT0, (0, col_db), true, SESSION_DEFAULT, false, None),
  ));
  // 双域皆缺席 → ASK
  assert_ask(cm.evaluate_multi_key_gate(
    &[absent],
    MultiKeyGateArgs::new(SLOT0, (0, col_db), true, SESSION_DEFAULT, false, None),
  ));
}

/// 用例 3：异步裁决臂（磁盘候选）同域断言——碰撞库冷化键同步探针回降级态，
/// wait_key_gate 异步探针须落碰撞库域判活放行；根域冷化异键不得反向污染
/// 碰撞库缺席终评（修复前异步臂同样恒锚根域：冷化在场键判死误 ASK、
/// 根域冷化键误放行）
#[compio::test]
async fn async_probe_resolves_disk_candidate_in_own_domain() {
  let col_db = collision_db();
  let cold = b"col:cold";
  let root_cold = b"root:cold";
  let cp = setup_primary();
  let StoreNode { _dir, store } = open_store("domain_collision.db");
  cp.set_store(Arc::clone(&store));
  prepare_migrating(&cp, SLOT0);
  let cm = cp.cluster_manager().unwrap();

  let sketch = Sketch::new();
  sketch.hash_and_store(b"root:mig");
  sketch.set_status(SketchStatus::Migrated);
  add_migration_task(&cp, &[SLOT0], sketch);

  put_string_in(&store, 0, col_db, cold, b"v1").await;
  put_string_in(&store, 0, 0, root_cold, b"root-value").await;
  // 冷化：内存记录落盘为磁盘候选，同步探针整体降级交异步收尾
  store.flush_and_evict_all().await.unwrap();

  // 同步首评：磁盘候选 → 存活性未决挂起（undecided 指向该键）
  match cm.evaluate_multi_key_gate(
    &[cold],
    MultiKeyGateArgs::new(SLOT0, (0, col_db), true, SESSION_DEFAULT, false, None),
  ) {
    GateVerdict::Wait { undecided: Some(0) } => {}
    other => panic!("磁盘候选应挂起异步裁决，实得 {other:?}"),
  }

  // 等待体驱动：异步探针落碰撞域判活入缓存，重评放行
  let memo = Arc::new(SlotWaitMemo::new(1));
  let req = SlotVerifyRequest {
    slot: SLOT0,
    ns: 0,
    db: col_db,
    keys: vec![cold.to_vec()],
    read_only: true,
    session: SESSION_DEFAULT,
    wait_for_stable: false,
  };
  cm.wait_key_gate(req, Arc::clone(&memo)).await;
  assert!(
    !memo.exhausted.load(Ordering::Acquire),
    "碰撞域键判活不得走超时终评"
  );
  assert!(matches!(
    cm.evaluate_multi_key_gate(
      &[cold],
      MultiKeyGateArgs::new(
        SLOT0,
        (0, col_db),
        true,
        SESSION_DEFAULT,
        false,
        Some(&memo)
      ),
    ),
    GateVerdict::Serve
  ));

  // 根域冷化异键：碰撞库域内缺席（哈希桶空，非磁盘候选）→ 同步即按 ASK
  // 终评，不得借根域在场态放行
  assert_ask(cm.evaluate_multi_key_gate(
    &[root_cold],
    MultiKeyGateArgs::new(SLOT0, (0, col_db), true, SESSION_DEFAULT, false, None),
  ));
}

/// 用例 4：根域在迁键（已发走视角、源端已删）ASK 现状不回退
#[compio::test]
async fn root_migrated_key_still_redirects_ask() {
  let key = b"root:mig";
  let cp = setup_primary();
  let StoreNode { _dir, store } = open_store("domain_collision.db");
  cp.set_store(store);
  prepare_migrating(&cp, SLOT0);
  let cm = cp.cluster_manager().unwrap();

  let sketch = Sketch::new();
  sketch.hash_and_store(key);
  sketch.set_status(SketchStatus::Migrated);
  add_migration_task(&cp, &[SLOT0], sketch);

  assert_ask(cm.evaluate_multi_key_gate(
    &[key],
    MultiKeyGateArgs::new(SLOT0, (0, 0), true, SESSION_DEFAULT, false, None),
  ));
}
