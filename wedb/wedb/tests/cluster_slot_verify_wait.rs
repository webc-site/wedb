//! 集群槽位校验等待语义集成测试
//!
//! 对标 garnet/libs/cluster/Session/SlotVerification/ClusterSlotVerify.cs 的
//! CanOperateOnKey（MIGRATING 槽键传输/删除期间自旋等待 + Exists 存在性裁决）
//! 与 WaitForSlotToStabalize（向量集写命令槽位稳定等待）。C# 在网络线程内联
//! 自旋，rust 侧为 compio 协作调度，等待外提为挂起轮询 + 超时终评，本套用例
//! 驱动 ClusterManager 门评与等待体，验证：
//! 1. MIGRATING + 键存在 → OK；键不存在 → ASK（String / ObjectEnvelope 双域）
//! 2. TRANSMITTING 写等待：迁移推进后放行（等待后继续）
//! 3. 等待超时：按 ASK 终评，不得永久挂起
//! 4. DELETING 读等待：键删除完成后 ASK
//! 5. wait_for_stable_slot：MIGRATING 期间等待、稳定后放行
//! 6. RESP 会话端到端：挂起 → 等待体驱动 → 游标回退重评 → 命令执行
use std::{
  sync::{Arc, atomic::Ordering},
  time::{Duration, Instant},
};

use compio::{runtime::spawn, time::sleep};
use wbase::hash_slot::{CLUSTER_SLOT_COUNT, slot_of};

/// 默认会话 (0,0) 库槽位（库级定槽 doc/zh/db.md 4.1：键内容不参与定槽）
const SLOT0: u16 = slot_of(0, 0);
/// 远端节点承载的槽位（与 SLOT0 异槽）
const REMOTE_SLOT: u16 = SLOT0 ^ 1;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_manager::{ClusterManager, GateVerdict, SlotVerifyRequest, SlotWaitMemo},
  cluster_provider::ClusterProvider,
  cluster_session::ClusterSession,
  hash_slot::{HashSlot, SlotState},
  migration::{
    migrate_session::{MigrateSession, MigrateTaskSpec},
    sketch::Sketch,
    sketch_status::SketchStatus,
    transfer_option::TransferOption,
  },
  slot_verify::{SlotVerifiedState, SlotVerifySessionState},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wkv::WedbStore;
use wnode::{
  ClusterSessionFace, ClusterSlotVerificationInput, MessageConsumerFace, RespSessionConsumer,
  SlotVerifyGate,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  storage::StorageSession,
};
use wnode_test::pump;
use wresp::{catalog::try_get_simple_resp_command_info, command::RespCommand};
use wtest_base::test_store_config;
use wval::KeyTag;

/// 会话标志默认快照（无 ASKING / 非 READONLY）
const SESSION_DEFAULT: SlotVerifySessionState = SlotVerifySessionState {
  session_asking: false,
  read_only_session: false,
};

/// 装配本地主节点拓扑（0..16384 全部槽位归本地，node_tgt 为迁移目标）
fn setup_primary() -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().unwrap();
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: 0x0DE1_0000_0000_0000_0000_0000_0000_0001,
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
    config.workers.push(Worker {
      nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_2E72),
      address: "127.0.0.1".into(),
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: None,
    });
    for s in 0..CLUSTER_SLOT_COUNT {
      config.slot_map[s] = HashSlot {
        worker_id: LOCAL_WORKER_ID as u16,
        state: SlotState::Stable,
      };
    }
  }
  cp
}

/// 置槽位 MIGRATING（走正常迁移准备 API，worker_id 指向目标节点）
fn prepare_migrating(cp: &ClusterProvider, slot: u16) {
  cp.cluster_manager()
    .unwrap()
    .try_prepare_slot_for_migration(slot as usize, 0x0000_0000_0000_0000_0000_0000_0000_2E72)
    .unwrap();
  assert_eq!(
    cp.cluster_manager()
      .unwrap()
      .current_config
      .read()
      .get_state(slot),
    SlotState::Migrating
  );
}

/// 经集群提供者自带迁移管理器注册管辖槽位的迁移任务
fn add_migration_task(cp: &ClusterProvider, slots: &[u16], sketch: Sketch) -> Arc<MigrateSession> {
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
    transfer_option: TransferOption::Slots,
  };
  let slot_set = slots.iter().map(|&s| s as i32).collect();
  cp.migration_manager()
    .unwrap()
    .try_add_migration_task(spec, slot_set, sketch)
    .expect("注册迁移任务失败")
}

/// 打开临时存储引擎并注入集群提供者（GC 关闭）
fn open_store(cp: &ClusterProvider) -> Arc<WedbStore<SegmentedDevice>> {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("verify_wait.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  cp.set_store(Arc::clone(&store));
  store
}

/// 存储会话写键（String 域）
async fn put_string(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8], val: &[u8]) {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  StorageSession::new_readonly(batch)
    .upsert_string(key, val)
    .await
    .unwrap();
}

/// 存储会话读键（String 域）
async fn read_string(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  StorageSession::new_readonly(batch)
    .read_string(key)
    .await
    .unwrap()
}

/// 构造单键校验请求
fn key_request(key: &[u8], read_only: bool) -> SlotVerifyRequest {
  SlotVerifyRequest {
    slot: SLOT0,
    keys: vec![key.to_vec()],
    read_only,
    session: SESSION_DEFAULT,
    wait_for_stable: false,
  }
}

/// 同步门评单键（无等待记忆）
fn evaluate_once(
  cm: &ClusterManager,
  key: &[u8],
  read_only: bool,
  wait_for_stable: bool,
) -> GateVerdict {
  let mut req = key_request(key, read_only);
  req.wait_for_stable = wait_for_stable;
  cm.evaluate_key_gate(&req, None)
}

/// 断言裁决为 ASK 重定向
fn assert_ask(verdict: GateVerdict) {
  match verdict {
    GateVerdict::Redirect(v) if v.state == SlotVerifiedState::Ask => {}
    other => panic!("应 ASK 重定向，实得 {other:?}"),
  }
}

#[compio::test]
async fn migrating_key_exists_serves_and_missing_redirects_ask() {
  let key = b"verify_wait_user_1";
  let obj_key = b"verify_wait_obj_1";
  let slot = SLOT0;
  let cp = setup_primary();
  let store = open_store(&cp);
  prepare_migrating(&cp, slot);
  let cm = cp.cluster_manager().unwrap();

  // 库级定槽下双域键同库同槽（同一迁移任务管辖，同槽重复排程被
  // SlotAlreadyScheduled 拒绝）
  let sketch = Sketch::new();
  sketch.hash_and_store(key);
  sketch.hash_and_store(obj_key);
  sketch.set_status(SketchStatus::Migrated);
  let _session = add_migration_task(&cp, &[slot], sketch);

  // 键不存在（已迁走视角）→ ASK 重定向
  assert_ask(evaluate_once(&cm, key, true, false));

  // String 域键存在 → OK（CanOperateOnKey：等待后 Exists 命中）
  put_string(&store, key, b"v1").await;
  assert!(matches!(
    evaluate_once(&cm, key, true, false),
    GateVerdict::Serve
  ));
  assert!(matches!(
    evaluate_once(&cm, key, false, false),
    GateVerdict::Serve
  ));

  // ObjectEnvelope 域键（对象信封带外化双域）存在 → OK
  {
    let session = store.new_session().unwrap();
    let batch = session.enter_batch();
    StorageSession::new_readonly(batch)
      .upsert_tag(obj_key, KeyTag::ObjectEnvelope, &[])
      .await
      .unwrap();
  }
  assert!(matches!(
    evaluate_once(&cm, obj_key, true, false),
    GateVerdict::Serve
  ));
}

/// 用例 2：TRANSMITTING 写等待后继续——迁移推进（MIGRATED）即放行
#[compio::test]
async fn transmitting_write_waits_then_proceeds() {
  let key = b"verify_wait_tx_1";
  let slot = SLOT0;
  let cp = setup_primary();
  let store = open_store(&cp);
  prepare_migrating(&cp, slot);
  let cm = cp.cluster_manager().unwrap();

  let sketch = Sketch::new();
  sketch.hash_and_store(key);
  sketch.set_status(SketchStatus::Transmitting);
  let session = add_migration_task(&cp, &[slot], sketch);

  put_string(&store, key, b"v1").await;

  // 写命令首评：TRANSMITTING 拦写 → 挂起等待（无存活性未决键）
  match evaluate_once(&cm, key, false, false) {
    GateVerdict::Wait { undecided: None } => {}
    other => panic!("TRANSMITTING 写应挂起等待，实得 {other:?}"),
  }

  // 读命令同态即时放行（CanAccessKey TRANSMITTING 仅读放行 + 键存在）
  assert!(matches!(
    evaluate_once(&cm, key, true, false),
    GateVerdict::Serve
  ));

  // 后台推进迁移状态，等待体应在超时前终评
  let adv = spawn(async move {
    sleep(Duration::from_millis(10)).await;
    session.sketch.set_status(SketchStatus::Migrated);
  });
  let memo = Arc::new(SlotWaitMemo::new(1));
  let req = key_request(key, false);
  cm.wait_key_gate(req, Arc::clone(&memo)).await;
  let _ = adv.await;
  assert!(
    !memo.exhausted.load(Ordering::Acquire),
    "迁移推进后不应超时"
  );

  // 重评（带记忆）：可访问 + 键存在 → OK
  match cm.evaluate_key_gate(&key_request(key, false), Some(&memo)) {
    GateVerdict::Serve => {}
    other => panic!("等待后写应放行，实得 {other:?}"),
  }
}

/// 用例 3：等待超时——迁移不推进时按 ASK 终评，不得永久挂起
#[compio::test]
async fn transmit_wait_times_out_to_ask() {
  let key = b"verify_wait_timeout_1";
  let slot = SLOT0;
  let cp = setup_primary();
  // 超时上限压至 30ms（测试观测窗口）
  cp.set_cluster_node_timeout_ms(30);
  open_store(&cp);
  prepare_migrating(&cp, slot);
  let cm = cp.cluster_manager().unwrap();

  let sketch = Sketch::new();
  sketch.hash_and_store(key);
  sketch.set_status(SketchStatus::Transmitting);
  let _session = add_migration_task(&cp, &[slot], sketch);

  let memo = Arc::new(SlotWaitMemo::new(1));
  let req = key_request(key, false);
  let started = Instant::now();
  cm.wait_key_gate(req, Arc::clone(&memo)).await;
  let elapsed = started.elapsed();
  assert!(elapsed < Duration::from_secs(5), "等待须有界返回");
  assert!(
    elapsed < Duration::from_millis(500),
    "单调时钟域保证回拨注入或墙钟波动下门评仍按 node_timeout 终评，实耗 {elapsed:?}"
  );
  assert!(memo.exhausted.load(Ordering::Acquire), "超时旗标应置位");

  // 超时重评：等待点全部压制 → TRANSMITTING 拦写按不可访问 → ASK
  assert_ask(cm.evaluate_key_gate(&key_request(key, false), Some(&memo)));
}

/// 用例 4：DELETING 读等待——键删除完成后按 ASK 重定向至目标
#[compio::test]
async fn deleting_read_waits_until_key_removed() {
  let key = b"verify_wait_del_1";
  let slot = SLOT0;
  let cp = setup_primary();
  let store = open_store(&cp);
  prepare_migrating(&cp, slot);
  let cm = cp.cluster_manager().unwrap();

  put_string(&store, key, b"v1").await;

  let sketch = Sketch::new();
  sketch.hash_and_store(key);
  sketch.set_status(SketchStatus::Deleting);
  let session = add_migration_task(&cp, &[slot], sketch);

  // DELETING 拦读写 → 挂起等待
  match evaluate_once(&cm, key, true, false) {
    GateVerdict::Wait { undecided: None } => {}
    other => panic!("DELETING 读应挂起等待，实得 {other:?}"),
  }

  // 后台完成删除（sketch 复位 + 源端删键），等待体终评
  let store2 = Arc::clone(&store);
  let adv = spawn(async move {
    sleep(Duration::from_millis(10)).await;
    session.sketch.clear();
    let s = store2.new_session().unwrap();
    let batch = s.enter_batch();
    StorageSession::new_readonly(batch)
      .delete_string(key)
      .await
      .unwrap();
  });
  let memo = Arc::new(SlotWaitMemo::new(1));
  let req = key_request(key, true);
  cm.wait_key_gate(req, memo.clone()).await;
  let _ = adv.await;
  assert!(
    !memo.exhausted.load(Ordering::Acquire),
    "删除完成后不应超时"
  );

  // 重评：sketch 未管辖（probe 未命中）+ 键不存在 → ASK（C# Exists false）
  assert_ask(cm.evaluate_key_gate(&key_request(key, true), Some(&memo)));
}

/// 用例 5：wait_for_stable_slot——MIGRATING 期间等待、稳定后放行
#[compio::test]
async fn wait_for_stable_slot_waits_until_stable() {
  let key = b"verify_wait_stable_1";
  let slot = SLOT0;
  let cp = setup_primary();
  let store = open_store(&cp);
  prepare_migrating(&cp, slot);
  let cm = cp.cluster_manager().unwrap();

  put_string(&store, key, b"v1").await;

  // 键不受迁移管辖（sketch 未收录）：无稳定等待要求时本可直接放行
  assert!(matches!(
    evaluate_once(&cm, key, false, false),
    GateVerdict::Serve
  ));

  // 向量集写命令（wait_for_stable）要求槽位先稳定 → 挂起等待
  match cm.evaluate_multi_key_gate(&[key], SLOT0, false, SESSION_DEFAULT, true, None) {
    GateVerdict::Wait { undecided: None } => {}
    other => panic!("稳定等待应挂起，实得 {other:?}"),
  }

  // 后台完成槽位复位（回 STABLE），等待体终评
  let cp2 = Arc::clone(&cp);
  let adv = spawn(async move {
    sleep(Duration::from_millis(10)).await;
    cp2
      .cluster_manager()
      .unwrap()
      .try_reset_slot_state(slot as usize);
  });
  let memo = Arc::new(SlotWaitMemo::new(1));
  let req = key_request(key, false);
  cm.wait_key_gate(req, memo.clone()).await;
  let _ = adv.await;
  assert!(
    !memo.exhausted.load(Ordering::Acquire),
    "槽位稳定后不应超时"
  );

  // 稳定后放行（向量集写命令继续走本地执行）
  let mut req = key_request(key, false);
  req.wait_for_stable = true;
  match cm.evaluate_key_gate(&req, Some(&memo)) {
    GateVerdict::Serve => {}
    other => panic!("槽位稳定后应放行，实得 {other:?}"),
  }
}

/// 用例 6：RESP 会话端到端——挂起等待体登记、游标回退、等待后重评执行
#[compio::test]
async fn resp_session_defers_set_until_migration_advances() {
  let key = b"verify_wait_resp_1";
  let slot = SLOT0;
  let cp = setup_primary();
  let store = open_store(&cp);
  prepare_migrating(&cp, slot);

  let sketch = Sketch::new();
  sketch.hash_and_store(key);
  sketch.set_status(SketchStatus::Transmitting);
  let session = add_migration_task(&cp, &[slot], sketch);

  put_string(&store, key, b"v0").await;

  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("resp_api.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let cfg = test_store_config();
  let exec_store = Arc::new(WedbStore::open(cfg, device).unwrap());
  let api = Arc::new(StoreGarnetApi::new(exec_store.new_session().unwrap()));
  let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
  let mut consumer = RespSessionConsumer::with_cluster(
    1,
    RespServerSessionOptions::default(),
    cluster_session,
    cp.provider_handle(),
    api,
  );

  // TRANSMITTING 拦写：命令不执行、无应答、游标回退零消费
  let frame = b"*3\r\n$3\r\nSET\r\n$18\r\nverify_wait_resp_1\r\n$2\r\nv1\r\n";
  let (consumed, out) = pump(&mut consumer, frame);
  assert_eq!(
    consumed,
    Some(frame.len()),
    "挂起时游标应回退（字节驻留缓冲零消费）"
  );
  assert!(out.is_empty(), "挂起时不应有应答");

  // 取走等待体并驱动：后台推进迁移状态（TRANSMITTING → MIGRATED）
  let slow = consumer.take_slow_wait().expect("等待体应已登记");
  let adv = spawn(async move {
    sleep(Duration::from_millis(10)).await;
    session.sketch.set_status(SketchStatus::Migrated);
  });
  let reply = slow.resolve().await;
  let _ = adv.await;
  assert!(reply.is_empty(), "等待体不产出应答字节");

  // 游标已回退：驻留字节原位续解析（泵下一轮直读同一缓冲），门评放行
  // → 命令执行 +OK（持久游标模型：重放不重喂字节）
  let mut out = Vec::new();
  let consumed = consumer.try_consume_messages_into(&mut out);
  assert_eq!(consumed, Some(0), "重评后应完整消费");
  assert_eq!(out, b"+OK\r\n", "out={:?}", String::from_utf8_lossy(&out));

  // 写落在执行域源端（键可访问且存在 → OK，未丢写）
  assert_eq!(read_string(&exec_store, key).await, Some(b"v1".to_vec()));
}

/// 挂起等待期间读命令仍即时放行（TRANSMITTING 读放行语义，对标 C#
/// CanAccessKey 的 readOnly 分支）
#[compio::test]
async fn transmitting_read_passes_without_wait() {
  let key = b"verify_wait_ro_1";
  let slot = SLOT0;
  let cp = setup_primary();
  let store = open_store(&cp);
  prepare_migrating(&cp, slot);
  let cm = cp.cluster_manager().unwrap();

  put_string(&store, key, b"v1").await;

  let sketch = Sketch::new();
  sketch.hash_and_store(key);
  sketch.set_status(SketchStatus::Transmitting);
  let _session = add_migration_task(&cp, &[slot], sketch);

  // 读 + 键存在 → 即时放行（零等待）
  assert!(matches!(
    evaluate_once(&cm, key, true, false),
    GateVerdict::Serve
  ));
  // 写 → 挂起
  assert!(matches!(
    evaluate_once(&cm, key, false, false),
    GateVerdict::Wait { .. }
  ));
}

/// 装配本地主节点拓扑（库级定槽：SLOT0 归本地并配置给定状态）
fn manager_with_slot(state: SlotState) -> ClusterManager {
  let cm = ClusterManager::new(Arc::new(ClusterProvider::default()));
  let slot = SLOT0;
  {
    let mut config = cm.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: 0x10CA1,
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
    config.workers.push(Worker {
      nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_2E74),
      address: "127.0.0.1".into(),
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: None,
    });
    config.slot_map[slot as usize] = HashSlot {
      worker_id: LOCAL_WORKER_ID as u16,
      state,
    };
  }
  cm
}

/// 门评基础形态：STABLE 本地槽放行、远端槽 MOVED（热路径零探测零等待）
#[test]
fn test_gate_verdict_stable_and_remote() {
  let local_key = b"gate_local_key";
  let cm = manager_with_slot(SlotState::Stable);
  let session = SlotVerifySessionState::default();
  let req = |key: &'static [u8], slot: u16| SlotVerifyRequest {
    slot,
    keys: vec![key.to_vec()],
    read_only: false,
    session,
    wait_for_stable: false,
  };

  // 本地 STABLE → Serve
  assert!(matches!(
    cm.evaluate_key_gate(&req(local_key, SLOT0), None),
    GateVerdict::Serve
  ));

  // 远端槽（构造远端 STABLE）
  let remote_key = b"gate_remote_key";
  let remote_slot = REMOTE_SLOT;
  let mut config = cm.current_config.write();
  config.slot_map[remote_slot as usize] = HashSlot {
    worker_id: 2,
    state: SlotState::Stable,
  };
  drop(config);
  match cm.evaluate_key_gate(&req(remote_key, REMOTE_SLOT), None) {
    GateVerdict::Redirect(v) if v.state == SlotVerifiedState::Moved => {
      assert_eq!(v.slot, remote_slot);
    }
    other => panic!("远端槽应 MOVED，实得 {other:?}"),
  }
}

/// 门评超时强制形态：MIGRATING + memo 超时旗标 → 不再等待，按 ASK 终评
///（迁移会话管辖的键即使 can_access 判定未决也压制等待点）
#[test]
fn test_gate_verdict_forced_migrating_redirects() {
  let key = b"gate_mig_key";
  let cm = manager_with_slot(SlotState::Migrating);
  let memo = SlotWaitMemo::new(1);
  memo.exhausted.store(true, Ordering::Release);

  // 无迁移会话管辖（can_access = true）+ 键存在性无从探测（无 store）：
  // 超时强制下存活性未决按 NotOperable → ASK
  let verdict = cm.evaluate_key_gate(
    &SlotVerifyRequest {
      slot: SLOT0,
      keys: vec![key.to_vec()],
      read_only: true,
      session: SlotVerifySessionState::default(),
      wait_for_stable: false,
    },
    Some(&memo),
  );
  assert!(matches!(
    verdict,
    GateVerdict::Redirect(v) if v.state == SlotVerifiedState::Ask
  ));
}

/// 门评 wait_for_stable：IMPORTING/MIGRATING 未超时时挂起等待
#[test]
fn test_gate_verdict_wait_for_stable_defers() {
  let key = b"gate_vset_key";
  let slot = SLOT0;
  let cm = manager_with_slot(SlotState::Importing);
  let req = SlotVerifyRequest {
    slot: SLOT0,
    keys: vec![key.to_vec()],
    read_only: false,
    session: SlotVerifySessionState::default(),
    wait_for_stable: true,
  };
  let verdict = cm.evaluate_key_gate(&req, None);
  assert!(matches!(verdict, GateVerdict::Wait { undecided: None }));

  // 非稳定等待命令不受影响（IMPORTING 非本地 → MOVED 至源属主）
  let mut config = cm.current_config.write();
  config.slot_map[slot as usize].worker_id = 2;
  drop(config);
  let verdict = cm.evaluate_key_gate(
    &SlotVerifyRequest {
      wait_for_stable: false,
      ..req.clone()
    },
    None,
  );
  assert!(matches!(
    verdict,
    GateVerdict::Redirect(v) if v.state == SlotVerifiedState::Moved
  ));
}

/// 用例 7：node-timeout=0（无限哨兵）+ TRANSMITTING 拦写：多键门立即回 Wait
/// 裁决，零同步自旋（迭代门清退后同一无限上界护栏由活路
/// `evaluate_multi_key_gate` 承接——修复前同步 loop 在 0 哨兵 deadline=u64::MAX
/// 下永不脱出，且 thread::yield_now 自旋饿死同线程迁移驱动 → 活锁）
#[compio::test]
async fn multi_key_gate_waits_immediately_under_zero_timeout() {
  let key = b"verify_wait_iter_zero_1";
  let slot = SLOT0;
  let cp = setup_primary();
  // 0 哨兵：cluster_node_timeout() = None（无限，cluster_provider_epoch
  // 测试锁定的刻意决策）——同步门评面不得消费该无限上界自旋
  cp.set_cluster_node_timeout_ms(0);
  let store = open_store(&cp);
  prepare_migrating(&cp, slot);
  let cm = cp.cluster_manager().unwrap();

  put_string(&store, key, b"v1").await;
  let sketch = Sketch::new();
  sketch.hash_and_store(key);
  sketch.set_status(SketchStatus::Transmitting);
  let _session = add_migration_task(&cp, &[slot], sketch);

  // 写命令 TRANSMITTING 拦截：立即 Wait（若仍在同步自旋，本断言在无限
  // deadline 下永远执行不到）
  let started = Instant::now();
  let verdict = cm.evaluate_multi_key_gate(&[key], slot, false, SESSION_DEFAULT, false, None);
  assert!(
    started.elapsed() < Duration::from_millis(500),
    "多键门不得同步自旋"
  );
  assert!(matches!(verdict, GateVerdict::Wait { undecided: None }));

  // 读命令同态即时放行（TRANSMITTING 仅拦写）
  assert!(matches!(
    cm.evaluate_multi_key_gate(&[key], slot, true, SESSION_DEFAULT, false, None),
    GateVerdict::Serve
  ));
}

/// 用例 8：多键门入口挂起臂——写命令遇迁移推进未决 → `SlotVerifyGate::Wait`，
/// 切面登记 `wait_key_gate` 等待体（park_gate_wait）→ 迁移推进后驱动 → 重驱门评
/// 放行（同线程不死锁闭环；迭代 Pending 臂清退后由活路多键门 Wait 臂等价承接，
/// core.rs 通用慢路径桥承接挂起-重评，见用例 6 RESP 端到端同臂形态）
#[compio::test]
async fn multi_key_gate_parks_waiter_then_serves_after_advance() {
  let key = b"verify_wait_iter_adv_1";
  let slot = SLOT0;
  let cp = setup_primary();
  let store = open_store(&cp);
  prepare_migrating(&cp, slot);
  let cs: Arc<ClusterSession> = cp.create_cluster_session();

  put_string(&store, key, b"v1").await;
  let sketch = Sketch::new();
  sketch.hash_and_store(key);
  sketch.set_status(SketchStatus::Transmitting);
  let session = add_migration_task(&cp, &[slot], sketch);

  // 多键门入口（有应答臂）：写命令被拦 → Wait + 登记挂起等待体
  let input = ClusterSlotVerificationInput {
    slot,
    key_specs: &[],
    is_sub_command: false,
    read_only: false,
    session_asking: 0,
    wait_for_stable_slot: false,
  };
  let mut out = Vec::new();
  assert!(matches!(
    cs.network_multi_key_slot_verify(&input, &[key], &mut out),
    SlotVerifyGate::Wait
  ));
  let slow = cs.take_pending_slow().expect("Wait 应登记等待体");

  // 后台推进迁移（TRANSMITTING → MIGRATED），等待体驱动至放行
  let adv = spawn(async move {
    sleep(Duration::from_millis(10)).await;
    session.sketch.set_status(SketchStatus::Migrated);
  });
  let reply = slow.resolve().await;
  let _ = adv.await;
  assert!(reply.is_empty(), "等待体不产出应答字节");

  // 重驱门评（新命令起点已 reset 缓存，迁移已推进 + 键存在）：放行、无重定向字节
  let mut out = Vec::new();
  assert!(matches!(
    cs.network_multi_key_slot_verify(&input, &[key], &mut out),
    SlotVerifyGate::Serve
  ));
  assert!(out.is_empty(), "放行不写重定向字节");
}

/// 用例 9：多键门等待超时闭环——迁移不推进时 `wait_key_gate` 置位 exhausted，
/// 带记忆重评压制等待点按超时终评（TRANSMITTING 写不可访问 → ASK），不无限挂起
#[compio::test]
async fn multi_key_gate_wait_timeout_forces_ask() {
  let key = b"verify_wait_iter_to_1";
  let slot = SLOT0;
  let cp = setup_primary();
  cp.set_cluster_node_timeout_ms(30);
  open_store(&cp);
  prepare_migrating(&cp, slot);
  let cm = cp.cluster_manager().unwrap();

  let sketch = Sketch::new();
  sketch.hash_and_store(key);
  sketch.set_status(SketchStatus::Transmitting);
  let _session = add_migration_task(&cp, &[slot], sketch);

  let memo = Arc::new(SlotWaitMemo::new(1));
  let req = key_request(key, false);
  let started = Instant::now();
  cm.wait_key_gate(req, Arc::clone(&memo)).await;
  assert!(started.elapsed() < Duration::from_secs(5), "等待须有界返回");
  assert!(memo.exhausted.load(Ordering::Acquire), "超时旗标应置位");

  // 带记忆重评多键门：exhausted 压制等待点 → TRANSMITTING 写不可访问 → ASK 终评
  assert_ask(cm.evaluate_multi_key_gate(&[key], slot, false, SESSION_DEFAULT, false, Some(&memo)));
}

/// 用例 10（deviations §149 锁测，严禁回改）：EVAL numkeys=0 空键区过集群槽位门
/// ——C# 槽校验核在规格命中后无条件取 firstIdx 读同会话残留槽定槽（属主非本地即
/// -MOVED），rust 提键双闸短路判无键、`keys.is_empty()` 即 Serve 零渲染零等待。
/// args 不含命令名（live parseState 形），第三参他槽残槽字节在 C# 形即首键直取
/// 值：若双闸被回改，键区非空 → 远端槽判 MOVED 渲染字节，本测即红；同规格
/// numkeys=1 正形对照（对照用例 8 有应答臂 Redirected 形态）排除空门。
#[compio::test]
async fn eval_numkeys_zero_gate_serves_without_redirect() {
  let cp = setup_primary();
  // REMOTE_SLOT 属主置为远端主节点（workers[2]，nodeid 2E72@7001）：
  // 一旦键区非空，门评必落远端 → Redirected
  {
    let cm = cp.cluster_manager().unwrap();
    let mut config = cm.current_config.write();
    config.slot_map[REMOTE_SLOT as usize] = HashSlot {
      worker_id: 2,
      state: SlotState::Stable,
    };
  }
  let cs: Arc<ClusterSession> = cp.create_cluster_session();
  // 目录真源 EVAL 规格（bs Index=2、keynum idx=0、first=1），不手搭 mock
  let info = try_get_simple_resp_command_info(RespCommand::Eval).expect("目录真源 EVAL 条目");
  let input = ClusterSlotVerificationInput {
    slot: REMOTE_SLOT,
    key_specs: &info.key_specs,
    is_sub_command: false,
    read_only: false,
    session_asking: 0,
    wait_for_stable_slot: false,
  };

  // numkeys=0：声明零键、残槽字节不入定槽，放行且零输出字节
  let mut out = Vec::new();
  let args: &[&[u8]] = &[b"return 1", b"0", b"remote_slot_residual"];
  assert!(matches!(
    cs.network_multi_key_slot_verify(&input, args, &mut out),
    SlotVerifyGate::Serve
  ));
  assert!(out.is_empty(), "无键放行不得写任何重定向字节");

  // 正形对照：numkeys=1 提键命中真实键区，远端属主 → Redirected + MOVED 字节
  let mut out = Vec::new();
  let args: &[&[u8]] = &[b"return KEYS[1]", b"1", b"remote_slot_key"];
  assert!(matches!(
    cs.network_multi_key_slot_verify(&input, args, &mut out),
    SlotVerifyGate::Redirected
  ));
  assert!(!out.is_empty(), "远端槽真实键应渲染重定向字节");
}
