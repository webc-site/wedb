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
  sync::Arc,
  time::{Duration, Instant},
};

use compio::{
  runtime::{Runtime, spawn},
  time::sleep,
};
use wbase::hash_slot::hash_slot as cluster_slot;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_config::ClusterPreferredEndpointType,
  cluster_manager::{ClusterManager, GateVerdict, SlotVerifyRequest, SlotWaitMemo},
  cluster_provider::ClusterProvider,
  cluster_session::ClusterSession,
  hash_slot::{HashSlot, SlotState},
  migration::{
    migrate_session::{MigrateSession, MigrateTaskSpec},
    sketch::Sketch,
    sketch_status::SketchStatus,
  },
  slot_verify::{ClusterSlotVerificationState, SlotVerifySessionState},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wedb_test::test_store_config;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  storage::StorageSession,
};
use wval::KeyTag;

/// 会话标志默认快照（无 ASKING / 非 READONLY / 非内部写）
const SESSION_DEFAULT: SlotVerifySessionState = SlotVerifySessionState {
  session_asking: false,
  read_only_session: false,
  internal_write: false,
};

/// 装配本地主节点拓扑（0..16384 全部槽位归本地，node_tgt 为迁移目标）
fn setup_primary() -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().unwrap();
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: "node_src",
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
    config.workers.push(Worker {
      nodeid: Some("node_tgt".to_string()),
      address: "127.0.0.1".to_string(),
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: None,
    });
    for s in 0..16384 {
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
    .try_prepare_slot_for_migration(slot as usize, "node_tgt")
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
    source_node_id: "node_src",
    target_address: "127.0.0.1",
    target_port: 7001,
    target_node_id: "node_tgt",
    username: "",
    passwd: "",
    copy_option: false,
    replace_option: false,
    timeout: 0,
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
  let mut config = test_store_config();
  config.gc.enabled = false;
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
    keys: vec![key.to_vec()],
    read_only,
    session: SESSION_DEFAULT,
    wait_for_stable: false,
    pref_type: ClusterPreferredEndpointType::Ip,
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
    GateVerdict::Redirect(ClusterSlotVerificationState::Ask { .. }) => {}
    other => panic!("应 ASK 重定向，实得 {other:?}"),
  }
}

/// 用例 1：MIGRATING + 键存在 → OK；键不存在 → ASK；对象信封域同语义
#[test]
fn migrating_key_exists_serves_and_missing_redirects_ask() {
  Runtime::new().unwrap().block_on(async {
    let key = b"verify_wait_user_1";
    let slot = cluster_slot(key);
    let cp = setup_primary();
    let store = open_store(&cp);
    prepare_migrating(&cp, slot);
    let cm = cp.cluster_manager().unwrap();

    let sketch = Sketch::new();
    sketch.hash_and_store(key);
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
    let obj_key = b"verify_wait_obj_1";
    let obj_slot = cluster_slot(obj_key);
    prepare_migrating(&cp, obj_slot);
    let obj_sketch = Sketch::new();
    obj_sketch.hash_and_store(obj_key);
    obj_sketch.set_status(SketchStatus::Migrated);
    let _obj_session = add_migration_task(&cp, &[obj_slot], obj_sketch);
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
  });
}

/// 用例 2：TRANSMITTING 写等待后继续——迁移推进（MIGRATED）即放行
#[test]
fn transmitting_write_waits_then_proceeds() {
  Runtime::new().unwrap().block_on(async {
    let key = b"verify_wait_tx_1";
    let slot = cluster_slot(key);
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
    assert!(!memo.is_exhausted(), "迁移推进后不应超时");

    // 重评（带记忆）：可访问 + 键存在 → OK
    match cm.evaluate_key_gate(&key_request(key, false), Some(&memo)) {
      GateVerdict::Serve => {}
      other => panic!("等待后写应放行，实得 {other:?}"),
    }
  });
}

/// 用例 3：等待超时——迁移不推进时按 ASK 终评，不得永久挂起
#[test]
fn transmit_wait_times_out_to_ask() {
  Runtime::new().unwrap().block_on(async {
    let key = b"verify_wait_timeout_1";
    let slot = cluster_slot(key);
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
    assert!(started.elapsed() < Duration::from_secs(5), "等待须有界返回");
    assert!(memo.is_exhausted(), "超时旗标应置位");

    // 超时重评：等待点全部压制 → TRANSMITTING 拦写按不可访问 → ASK
    assert_ask(cm.evaluate_key_gate(&key_request(key, false), Some(&memo)));
  });
}

/// 用例 4：DELETING 读等待——键删除完成后按 ASK 重定向至目标
#[test]
fn deleting_read_waits_until_key_removed() {
  Runtime::new().unwrap().block_on(async {
    let key = b"verify_wait_del_1";
    let slot = cluster_slot(key);
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
    assert!(!memo.is_exhausted(), "删除完成后不应超时");

    // 重评：sketch 未管辖（probe 未命中）+ 键不存在 → ASK（C# Exists false）
    assert_ask(cm.evaluate_key_gate(&key_request(key, true), Some(&memo)));
  });
}

/// 用例 5：wait_for_stable_slot——MIGRATING 期间等待、稳定后放行
#[test]
fn wait_for_stable_slot_waits_until_stable() {
  Runtime::new().unwrap().block_on(async {
    let key = b"verify_wait_stable_1";
    let slot = cluster_slot(key);
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
    match cm.evaluate_multi_key_gate(
      &[key],
      false,
      SESSION_DEFAULT,
      true,
      ClusterPreferredEndpointType::Ip,
      None,
    ) {
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
    assert!(!memo.is_exhausted(), "槽位稳定后不应超时");

    // 稳定后放行（向量集写命令继续走本地执行）
    let mut req = key_request(key, false);
    req.wait_for_stable = true;
    match cm.evaluate_key_gate(&req, Some(&memo)) {
      GateVerdict::Serve => {}
      other => panic!("槽位稳定后应放行，实得 {other:?}"),
    }
  });
}

/// 用例 6：RESP 会话端到端——挂起等待体登记、游标回退、等待后重评执行
#[test]
fn resp_session_defers_set_until_migration_advances() {
  Runtime::new().unwrap().block_on(async {
    let key = b"verify_wait_resp_1";
    let slot = cluster_slot(key);
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
    let mut cfg = test_store_config();
    cfg.gc.enabled = false;
    let exec_store = Arc::new(WedbStore::open(cfg, device).unwrap());
    let api = Arc::new(StoreGarnetApi::new(exec_store.new_session().unwrap()));
    let cluster_session: Arc<ClusterSession> = Arc::new(cp.create_cluster_session());
    let mut consumer = RespSessionConsumer::with_cluster_session(
      1,
      RespServerSessionOptions {
        max_databases: 2,
        ..RespServerSessionOptions::default()
      },
      cluster_session,
      api,
    );

    // TRANSMITTING 拦写：命令不执行、无应答、游标回退零消费
    let frame = b"*3\r\n$3\r\nSET\r\n$18\r\nverify_wait_resp_1\r\n$2\r\nv1\r\n";
    let (consumed, out) = consumer.try_consume_messages(frame);
    assert_eq!(consumed, 0, "挂起时游标应回退（零消费）");
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

    // 游标已回退：重新消费同一帧，门评放行 → 命令执行 +OK
    let (consumed, out) = consumer.try_consume_messages(frame);
    assert_eq!(consumed, frame.len(), "重评后应完整消费");
    assert_eq!(out, b"+OK\r\n", "out={:?}", String::from_utf8_lossy(&out));

    // 写落在执行域源端（键可访问且存在 → OK，未丢写）
    assert_eq!(read_string(&exec_store, key).await, Some(b"v1".to_vec()));
  });
}

/// 挂起等待期间读命令仍即时放行（TRANSMITTING 读放行语义，对标 C#
/// CanAccessKey 的 readOnly 分支）
#[test]
fn transmitting_read_passes_without_wait() {
  Runtime::new().unwrap().block_on(async {
    let key = b"verify_wait_ro_1";
    let slot = cluster_slot(key);
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
  });
}
