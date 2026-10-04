//! 迁移故障注入收口（r19-migrate 审查票发现二/三）
//!
//! DELETE_FAIL_INJECT / PERSIST_FAIL_INJECT 为进程级一次性钩子（对标
//! wbftree::SCAN_FAIL_INJECT），本文件独立成测试二进制与 cluster_migration
//! 等并发进程物理隔离，杜绝其他测试文件的删除/清退在注入窗口内互抢消费；
//! 文件内多注入测试经 [`INJECT_SERIAL`] 互斥驱动

// 注入面为 wnode debug-only 导出，release 整文件剔除
#![cfg(debug_assertions)]

use wedb_test::{
  cluster_consumer_fresh_store::cluster_consumer_fresh_store,
  two_primary_provider_100ms::two_primary_provider_100ms,
};

#[path = "common/migrate_fixture.rs"]
mod migrate_fixture;
use migrate_fixture::{migrate_spec, port_of};

#[path = "common/scripted_migrate_target.rs"]
mod scripted_migrate_target_core;

use std::sync::{Arc, atomic::Ordering};

use compio::runtime::Runtime;
use parking_lot::Mutex;
use scripted_migrate_target_core::{ScriptedTargetOptions, scripted_migrate_target};
use wbase::{hash_slot::slot_of, map::HashSet};
use wconn::record::{BatchItem, MigrateVal, encode_migration_payload};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_provider::ClusterProvider,
  hash_slot::{HashSlot, SlotState},
  migration::{
    migrate_driver::{run_slots_migration_task, try_add_slots_migration_task},
    migrate_session::MigrateTaskSpec,
  },
  worker::LOCAL_WORKER_ID,
};
use wedb_test::{de12_node_id::DE12_NODE_ID, resp_drive_scratch::drive};
use wkv::WedbStore;
use wnode::{
  RespSessionConsumer,
  resp::resp_server_session::RespServerSessionOptions,
  storage::{DELETE_FAIL_INJECT, PERSIST_FAIL_INJECT, session::storage_session::StorageSession},
};
use wtest_base::{resp_frame, test_store_config};
use wval::KeyTag;

/// 故障注入测试互斥锁（进程级一次性钩子，文件内串行驱动）
static INJECT_SERIAL: Mutex<()> = Mutex::new(());

/// 默认会话 (0,0) 库槽位（库级定槽 doc/zh/db.md 4.1：键内容不参与定槽）
const SLOT0: u16 = slot_of(0, 0);
/// 会话库槽集合
const SLOT0_LIST: &[u8] = b"0";
/// 接收端 CLUSTER MIGRATE 帧源节点 hex（本地节点 id 0x…DE11 的 32 字符渲染）
const MIGRATE_SRC_NODE_HEX: &[u8] = b"0000000000000000000000000000de11";
/// 远端节点承载的槽位（与 SLOT0 异槽）
const REMOTE_SLOT: u16 = SLOT0 ^ 1;

/// 装配双主节点拓扑：node_1（本地）持全量槽、node_2@7001 接管 REMOTE_SLOT，
/// 100ms 栅栏超时（装配主体 `wedb_test::two_primary_provider` 单源）
fn two_primary_provider() -> Arc<ClusterProvider> {
  two_primary_provider_100ms(REMOTE_SLOT)
}

/// 打开迁移测试存储（复活启用位由调用方裁决）
fn open_migrate_store(tag: &str, reviv_enabled: bool) -> Arc<WedbStore<SegmentedDevice>> {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join(tag)).unwrap());
  let config = test_store_config().with_revivification(reviv_enabled);
  Arc::new(WedbStore::open(config, device).unwrap())
}

/// 复活启用态存储（is_enabled 断言用例须以本夹具建店）
fn migrate_store_reviv(tag: &str) -> Arc<WedbStore<SegmentedDevice>> {
  open_migrate_store(tag, true)
}

/// 挂共享存储的集群会话消费者
fn migrate_consumer(
  cp: &ClusterProvider,
) -> (RespSessionConsumer, Arc<WedbStore<SegmentedDevice>>) {
  cluster_consumer_fresh_store(cp, "mig_inject.db", RespServerSessionOptions::default())
}

/// 构造会话库键（库级定槽下键内容不参与定槽）
fn key_in_slot(prefix: &str, _slot: u16) -> String {
  format!("{prefix}0")
}

/// 本地库键实例
fn local_slot_key(prefix: &str) -> String {
  format!("{prefix}0")
}

/// 迁移驱动发送侧 spec
/// 读库内 string（驱动用例断言键权用）
async fn read_str(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  storage.read_string(key).await.unwrap()
}

/// 假目标端行为面（全默认：纯脚本弹答，无 RESERVE 合成臂；单源见
/// common/scripted_migrate_target.rs 按册直挂，按连接分配脚本逐帧弹答）
static TARGET_OPTS: ScriptedTargetOptions = ScriptedTargetOptions {
  reserve_ctx_base: None,
  seen_with_payload_len: false,
  frame_archive: None,
};

/// 接收端 CLUSTER MIGRATE 帧（头 4 参：源节点 + replace + 会话槽集 + 载荷）
fn migrate_recv_frame(replace: &[u8], payload: &[u8]) -> Vec<u8> {
  resp_frame(&[
    b"CLUSTER",
    b"MIGRATE",
    MIGRATE_SRC_NODE_HEX,
    replace,
    SLOT0_LIST,
    payload,
  ])
}

/// 经 HSET 构建真实 Hash 信封整值（对象信封记录编码与存储读取同源）
fn encode_hash_envelope(rt: &Runtime, fields: &[(&[u8], &[u8])]) -> Vec<u8> {
  let cp = two_primary_provider();
  let (mut consumer, store) = migrate_consumer(&cp);
  let key = b"__envelope_builder__";
  for (f, v) in fields {
    assert_eq!(
      drive(rt, &mut consumer, &resp_frame(&[b"HSET", key, f, v])),
      b":1\r\n"
    );
  }
  rt.block_on(async {
    let session = store.new_session().unwrap();
    let batch = session.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    storage
      .read_tag_with(key, KeyTag::ObjectEnvelope, |raw| raw.to_vec())
      .await
      .unwrap()
      .expect("Hash 信封应已写入")
  })
}

/// SLOTS 删除环部分失败：delete_string Err 键登记 untouchable 留痕收敛——
/// 任务 Ok 完成 + reviv 恢复 + 任务移除 + 槽交权，删除失败键保留源端
/// （对标 C# DeleteKeys 吞删除失败的孤儿键投影，但本驱动以 untouchable
/// 剔除承接槽头重扫收敛，杜绝「重传-删除-再失败」死循环）
#[test]
fn slots_delete_failure_registers_untouchable_and_converges() {
  let _serial = INJECT_SERIAL.lock();
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let store = migrate_store_reviv("st_delfail.db");
    let slot = SLOT0;
    let k1 = key_in_slot("st_df1", slot);
    let k2 = key_in_slot("st_df2", slot);
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new(batch);
      storage.upsert_string(k1.as_bytes(), b"v1").await.unwrap();
      storage.upsert_string(k2.as_bytes(), b"v2").await.unwrap();
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    // 脚本（单连接）：握手×2 + IMPORTING + 批次 + 哨兵 + NODE 全 +OK；汇聚×2
    let addr = scripted_migrate_target(
      vec![
        vec![b"+OK\r\n"; 6],
        vec![b"+OK\r\n"; 3],
        vec![b"+OK\r\n"; 3],
      ],
      Arc::clone(&seen),
      TARGET_OPTS,
    )
    .await;
    let spec = MigrateTaskSpec {
      ..migrate_spec(port_of(&addr), 5000)
    };
    let slots: HashSet<i32> = [i32::from(slot)].into_iter().collect();
    let session = try_add_slots_migration_task(&cp, spec.clone(), &slots).unwrap();

    // 一次性注入：删除环首键消费即 Err，次键真实删除成功（部分失败）
    DELETE_FAIL_INJECT.store(true, Ordering::SeqCst);
    let migrated = run_slots_migration_task(Arc::clone(&store), spec, Arc::clone(&session))
      .await
      .unwrap();
    assert_eq!(migrated, 2, "两键均应计数迁移");
    assert!(
      !DELETE_FAIL_INJECT.load(Ordering::SeqCst),
      "钩子必须被删除环消费"
    );

    // 收敛终态：任务移除 + reviv 恢复 + 槽位交权
    assert!(
      store.reviv_pool.is_enabled(),
      "删除失败收敛后复活池必须恢复启用状态"
    );
    assert_eq!(
      cp.migration_manager().unwrap().get_migration_task_count(),
      0,
      "删除失败路径同样必须移除任务"
    );
    let m = cp.cluster_manager().unwrap();
    let remote_wid = m
      .current_config
      .read()
      .get_worker_id_from_node_id(DE12_NODE_ID);
    assert_eq!(m.current_config.read().get_state(slot), SlotState::Stable);
    assert_eq!(
      m.current_config.read().get_worker_id_from_slot(slot),
      remote_wid as usize
    );

    // 键权：删除失败键保留源端（孤儿键投影），删除成功键消失
    let k1_left = read_str(&store, k1.as_bytes()).await.is_some();
    let k2_left = read_str(&store, k2.as_bytes()).await.is_some();
    assert!(
      k1_left ^ k2_left,
      "恰一键删除失败保留源端: k1_left={k1_left} k2_left={k2_left}"
    );
    assert!(
      seen.lock().iter().any(|f| f.contains("SETSLOTSRANGE NODE")),
      "部分失败收敛仍应交权 NODE: {:?}",
      seen.lock()
    );
  });
}

/// SLOTS 删除环整批失败：单键任务删除 Err 即整批失败判败——recover STABLE +
/// Err 透出 + 任务移除 + reviv 恢复 + 源端键保留，绝不空转重扫
#[test]
fn slots_delete_batch_failure_recovers_and_terminates() {
  let _serial = INJECT_SERIAL.lock();
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let store = migrate_store_reviv("st_delbatch.db");
    let slot = SLOT0;
    let k1 = key_in_slot("st_dbs", slot);
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new(batch);
      storage.upsert_string(k1.as_bytes(), b"v1").await.unwrap();
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    // 脚本（单连接）：握手×2 + IMPORTING + 批次 + 哨兵 + STABLE（recover 复用
    // 连接，对标 slots_migration_task_batch_reject_recovers 单段形态）
    let addr =
      scripted_migrate_target(vec![vec![b"+OK\r\n"; 6]], Arc::clone(&seen), TARGET_OPTS).await;
    let spec = MigrateTaskSpec {
      ..migrate_spec(port_of(&addr), 5000)
    };
    let slots: HashSet<i32> = [i32::from(slot)].into_iter().collect();
    let session = try_add_slots_migration_task(&cp, spec.clone(), &slots).unwrap();

    // 单键任务：唯一删除即整批失败
    DELETE_FAIL_INJECT.store(true, Ordering::SeqCst);
    let err = run_slots_migration_task(Arc::clone(&store), spec, Arc::clone(&session))
      .await
      .unwrap_err();
    assert!(
      format!("{err:?}").contains("整批失败"),
      "应透出删除环整批失败: {err:?}"
    );
    assert!(
      store.reviv_pool.is_enabled(),
      "整批失败判败后复活池必须恢复启用状态"
    );
    assert!(
      seen
        .lock()
        .iter()
        .any(|f| f.contains("SETSLOTSRANGE STABLE")),
      "整批失败必须 recover STABLE: {:?}",
      seen.lock()
    );
    assert_eq!(
      read_str(&store, k1.as_bytes()).await,
      Some(b"v1".to_vec()),
      "删除失败键必须保留源端"
    );
    assert_eq!(
      cp.migration_manager().unwrap().get_migration_task_count(),
      0,
      "判败路径必须移除任务"
    );
  });
}

/// 接收端旧 TTL 清退失败即判错拒绝：persist_key Err → 帧导入判错应答错误帧、
/// 键不落库（对标 C# 单步 basicGarnetApi.SET 原子写 TTL 无中间态；RI 带外
/// 通道同口径判错，消除同流双通道静默/判错分叉）
#[test]
fn cluster_migrate_recv_persist_fail_rejects_payload() {
  let _serial = INJECT_SERIAL.lock();
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let (mut consumer, _store) = migrate_consumer(&cp);

  // 目标键槽位置 IMPORTING（接收端头级门控前提）
  let m = cp.cluster_manager().unwrap();
  m.current_config.write().slot_map[SLOT0 as usize] = HashSlot {
    worker_id: LOCAL_WORKER_ID as u16,
    state: SlotState::Importing,
  };

  let env_key = local_slot_key("mig_persist_fail");
  let env = encode_hash_envelope(&rt, &[(b"f1", b"v1")]);
  let payload = encode_migration_payload(&[BatchItem {
    key: env_key.as_bytes(),
    val: MigrateVal::Env(env),
    expire_ticks: 0,
  }]);

  // 一次性注入：Env 臂写前清退消费即 Err → 判错，upsert_tag 不再执行
  PERSIST_FAIL_INJECT.store(true, Ordering::SeqCst);
  let out = drive(&rt, &mut consumer, &migrate_recv_frame(b"F", &payload));
  assert!(out.starts_with(b"-ERR "), "清退失败必须判错拒绝: {out:?}");
  assert!(
    !PERSIST_FAIL_INJECT.load(Ordering::SeqCst),
    "钩子必须被帧导入消费"
  );

  // 键权零污染：判错后值未写入，目标端键不存在
  m.current_config.write().slot_map[SLOT0 as usize] = HashSlot {
    worker_id: LOCAL_WORKER_ID as u16,
    state: SlotState::Stable,
  };
  let out = drive(
    &rt,
    &mut consumer,
    &resp_frame(&[b"GET", env_key.as_bytes()]),
  );
  assert!(
    out == b"$-1\r\n" || out == b"_1\r\n",
    "判错后键不得落库: {out:?}"
  );
}
