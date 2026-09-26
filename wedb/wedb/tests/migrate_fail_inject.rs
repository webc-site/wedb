//! 迁移故障注入收口（r19-migrate 审查票发现二/三）
//!
//! DELETE_FAIL_INJECT / PERSIST_FAIL_INJECT 为进程级一次性钩子（对标
//! wbftree::SCAN_FAIL_INJECT），本文件独立成测试二进制与 cluster_migration
//! 等并发进程物理隔离，杜绝其他测试文件的删除/清退在注入窗口内互抢消费；
//! 文件内多注入测试经 [`INJECT_SERIAL`] 互斥驱动

use std::{
  collections::VecDeque,
  str::from_utf8,
  sync::{Arc, atomic::Ordering},
};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpListener,
  runtime::{Runtime, spawn},
};
use parking_lot::Mutex;
use wbase::{hash_slot::slot_of, map::HashSet};
use wconn::record::{BatchItem, MigrateVal, encode_migration_payload};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_provider::ClusterProvider,
  cluster_session::ClusterSession,
  hash_slot::{HashSlot, SlotState},
  migration::{
    migrate_driver::{run_slots_migration_task, try_add_slots_migration_task},
    migrate_session::MigrateTaskSpec,
    transfer_option::TransferOption,
  },
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  storage::{DELETE_FAIL_INJECT, PERSIST_FAIL_INJECT, session::storage_session::StorageSession},
};
use wnode_test::pump;
use wtest_base::{resp_frame, test_store_config};
use wtxn::{TxnLockTable, WatchVersionMap};
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

/// 装配双主节点拓扑：node_1（本地）持 0..8192，node_2@7001 持 8192..16384
fn two_primary_provider() -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().unwrap();
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
    let remote_worker_id = config.workers.len() as u16;
    config.workers.push(Worker {
      nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_DE12),
      address: "127.0.0.1".into(),
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: None,
    });
    for slot in 0..16384 {
      config.slot_map[slot] = HashSlot {
        worker_id: LOCAL_WORKER_ID as u16,
        state: SlotState::Stable,
      };
    }
    config.slot_map[REMOTE_SLOT as usize] = HashSlot {
      worker_id: remote_worker_id,
      state: SlotState::Stable,
    };
  }
  cp.set_cluster_node_timeout_ms(100);
  cp
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
  let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
  let store = open_migrate_store("mig_inject.db", false);
  cp.set_store(Arc::clone(&store));
  let mut consumer = RespSessionConsumer::with_cluster(
    1,
    RespServerSessionOptions::default(),
    cluster_session,
    cp.provider_handle(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  (consumer, store)
}

/// 慢命令往返：同步段消费，挂起慢路径时 block_on 驱动应答
fn drive(rt: &Runtime, c: &mut RespSessionConsumer, frame_bytes: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = pump(c, frame_bytes);
  assert_eq!(consumed, Some(0), "帧应被完整消费");
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async {
      out.extend_from_slice(&slow.resolve().await);
    });
  }
  out
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
fn migrate_spec(port: i32, timeout_ms: i32) -> MigrateTaskSpec {
  MigrateTaskSpec {
    source_node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
    target_address: "127.0.0.1".to_string(),
    target_port: port,
    target_node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE12,
    username: String::new(),
    passwd: String::new(),
    copy_option: false,
    replace_option: false,
    timeout: timeout_ms,
    transfer_option: TransferOption::Keys,
  }
}

/// 解析假端监听地址的端口号
fn port_of(addr: &str) -> i32 {
  addr.rsplit(':').next().unwrap().parse().unwrap()
}

/// 读库内 string（驱动用例断言键权用）
async fn read_str(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  storage.read_string(key).await.unwrap()
}

/// 解析缓冲中首个完整 RESP2 数组帧：返回 (帧总字节数, 全部参数切片)
fn try_parse_frame_args(buf: &[u8]) -> Option<(usize, Vec<&[u8]>)> {
  if buf.first() != Some(&b'*') {
    return None;
  }
  let header_end = buf.iter().position(|b| *b == b'\n')? + 1;
  let argc: usize = from_utf8(&buf[1..header_end - 2]).ok()?.parse().ok()?;
  let mut pos = header_end;
  let mut args = Vec::with_capacity(argc);
  for _ in 0..argc {
    if buf.get(pos) != Some(&b'$') {
      return None;
    }
    let len_line_end = buf[pos + 1..].iter().position(|b| *b == b'\n')? + pos + 2;
    let len: usize = from_utf8(&buf[pos + 1..len_line_end - 2])
      .ok()?
      .parse()
      .ok()?;
    let end = len_line_end + len;
    if end + 2 > buf.len() {
      return None;
    }
    args.push(&buf[len_line_end..end]);
    pos = end + 2;
  }
  Some((pos, args))
}

/// 假迁移目标端（脚本化应答，按连接分配脚本逐帧弹答）
async fn scripted_migrate_target(
  conn_replies: Vec<Vec<&'static [u8]>>,
  seen: Arc<Mutex<Vec<String>>>,
) -> String {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  let scripts = Arc::new(Mutex::new(VecDeque::from(
    conn_replies
      .into_iter()
      .map(VecDeque::from)
      .collect::<Vec<_>>(),
  )));
  spawn(async move {
    while let Ok((mut stream, _)) = listener.accept().await {
      let scripts = Arc::clone(&scripts);
      let seen = Arc::clone(&seen);
      spawn(async move {
        let mut script = scripts.lock().pop_front().unwrap_or_default();
        let mut acc: Vec<u8> = Vec::new();
        let mut buf = vec![0u8; 65536];
        loop {
          let BufResult(res, next) = stream.read(buf).await;
          buf = next;
          let n = match res {
            Ok(n) if n > 0 => n,
            _ => break,
          };
          acc.extend_from_slice(&buf[..n]);
          while let Some((frame_len, args)) = try_parse_frame_args(&acc) {
            seen.lock().push(
              args
                .iter()
                .take(3)
                .map(|a| String::from_utf8_lossy(a).into_owned())
                .collect::<Vec<_>>()
                .join(" "),
            );
            acc.drain(..frame_len);
            let Some(reply) = script.pop_front() else {
              continue;
            };
            if stream.write_all(reply.to_vec()).await.is_err() {
              return;
            }
          }
        }
      })
      .detach();
    }
  })
  .detach();
  addr
}

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
      let storage = StorageSession::new_readonly(batch);
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
    )
    .await;
    let spec = MigrateTaskSpec {
      transfer_option: TransferOption::Slots,
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
      .get_worker_id_from_node_id(0x0000_0000_0000_0000_0000_0000_0000_DE12);
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
      let storage = StorageSession::new_readonly(batch);
      storage.upsert_string(k1.as_bytes(), b"v1").await.unwrap();
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    // 脚本（单连接）：握手×2 + IMPORTING + 批次 + 哨兵 + STABLE（recover 复用
    // 连接，对标 slots_migration_task_batch_reject_recovers 单段形态）
    let addr = scripted_migrate_target(vec![vec![b"+OK\r\n"; 6]], Arc::clone(&seen)).await;
    let spec = MigrateTaskSpec {
      transfer_option: TransferOption::Slots,
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
    expire_unix_ms: 0,
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
