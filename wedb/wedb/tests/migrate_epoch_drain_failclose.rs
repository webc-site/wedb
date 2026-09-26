//! 集群迁移链纪元静止栅栏返值承判（fail-close）回归锁面
//! （工单 wedb-migrate-epoch-drain-return-ignored-revive-window，§95 迁移族收口）
//!
//! 背景：rust 将 C# `ClusterProvider.BumpAndWaitForEpochTransitionAsync`（无限
//! 自旋恒真）有界化为超时返 false 后，迁移数据收敛相九处栅栏曾一律 `let _ =`
//! 弃返值——排空未达成照样取数/落删除，开「已 ACK 写丢失 + 源端复活键」双害窗。
//! 本锁面复用 diskless_epoch_drain_failclose 的恒不追平夹具思想（注册集群会话
//! 批首纪元快照 + 极小 cluster_node_timeout），按链相位注入落后会话，断言
//! KEYS / SLOTS / RangeIndex / 向量集四链的 TRANSMITTING 与 DELETING 八栅栏
//! 返值承判即 FAIL：recover 收口（远端 STABLE）、源端键零删除、TRANSMITTING
//! 相目标端零导入帧。
//!
//! 相位注入三夹具（纪元判定 entry==0 空闲放行 / entry>=目标追平，落后会话一旦
//! 落后则对其后每次推进恒落后）：
//! - 静态落后：驱动启动前注册并快照——链上首个带纪元等待的栅栏即判败
//!   （KEYS TRANSMITTING 相，KEYS begin 门 epoch_gate=false 不推纪元）；
//! - 活值钩落后：TEST_LIVE_VALUE_READ_HOOK（read_live_value 一次性消费点，
//!   严格落在 TRANSMITTING 栅栏之后、DELETING 栅栏之前的传输相内）注册快照
//!   ——TRANSMITTING 相放行、DELETING 相判败；
//! - 阻塞者 + 纪元哨兵：SLOTS 族 begin 纪元门先行推纪元，静态落后必先撞 begin
//!   门无从区分栅栏位点——预置阻塞会话逼 begin 门自旋让出执行器，哨兵任务于
//!   begin 门纪元恰成「追平 begin、落后后继」相位放行 begin 再判败目标栅栏
//!   （RI/向量 DELETING 相另加二级哨兵，于 TRANSMITTING 栅栏自旋窗内放行一级
//!   落后会话并补二级快照）。
//! 判败位点以纪元推进增量精确指认（delta 计数见各用例断言），杜绝「其实是
//! begin 门判败」的假锁。
//!
//! revert-proof（任一处栅栏还原 `let _ =` 即对应用例转红）：
//! - keys.rs TRANSMITTING → keys_transmitting…（还原后传输帧发出+DELETING 相
//!   仍判败，零导入断言红）；
//! - keys.rs DELETING → keys_deleting…（还原后删源键整链 Success，判败与零删
//!   断言红）；
//! - slots.rs TRANSMITTING/DELETING → slots_transmitting… / slots_deleting…；
//! - migrate_session_range_index.rs 两处 → slots_range_index_transmitting… /
//!   slots_range_index_deleting…；
//! - migrate_session_vector_set.rs 两处 → slots_vector_set_transmitting… /
//!   slots_vector_set_deleting…；
//! - keys.rs MIGRATED 归位臂为无收敛不变量放行位点（§95 同口径声明），不入
//!   锁面。

use std::{
  collections::{BTreeSet, VecDeque},
  fmt,
  str::from_utf8,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  time::Duration,
};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpListener,
  runtime::{Runtime, spawn},
  time::sleep,
};
use parking_lot::Mutex;
use wbase::{hash_slot::slot_of, map::HashSet};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_provider::ClusterProvider,
  cluster_session::ClusterSession,
  hash_slot::{HashSlot, SlotState},
  migration::{
    migrate_driver::{
      LiveValue, TEST_LIVE_VALUE_READ_HOOK, read_live_value, run_keys_migration_driver,
      run_slots_migration_task, try_add_slots_migration_task,
    },
    migrate_session::MigrateTaskSpec,
    migrate_state::MigrateState,
    transfer_option::TransferOption,
  },
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wkv::WedbStore;
use wnode::{
  ClusterSessionFace,
  resp::vector::{
    vector_manager::{VectorAddArgs, VectorManager, VectorManagerOptions, VectorManagerResult},
    vector_manager_locking::CreateIndexParams,
    vector_store_callbacks::{ActiveVectorSessionGuard, WedbVectorStoreCallbacks},
  },
  storage::session::storage_session::StorageSession,
};
use wtest_base::{resp_frame, test_store_config};
use wval::{GarnetObjectType, SessionPrefixBuf};
use wvector::{Callbacks, VectorDistanceMetricType, VectorQuantType, VectorValueType};

/// 默认会话 (0,0) 库槽位（库级定槽：键内容不参与定槽）
const SLOT0: u16 = slot_of(0, 0);
/// 纪元静止等待上限毫秒（落后夹具下超时必达；留足哨兵任务 1ms 节拍的让渡窗）
const FENCE_TIMEOUT_MS: u64 = 300;
/// 源端键值（判败后读取比对零删直证）
const VAL: &[u8] = b"v1";

/// 故障注入钩子互斥（TEST_LIVE_VALUE_READ_HOOK 为进程级一次性钩子，
/// 用毕两用例经本锁串行，杜绝互抢消费）
static HOOK_SERIAL: Mutex<()> = Mutex::new(());

/// 落后夹具会话保管槽（Arc 存活期内注册有效，用例末随槽 drop 释放）
type LagSlot = Arc<Mutex<Vec<Arc<ClusterSession>>>>;

/// 装配双主节点拓扑（对标 migrate_fail_inject 同名件）：node_1（本地）持全槽，
/// node_2@7001 持 REMOTE_SLOT 一槽；纪元静止等待上限按用例入参
fn two_primary_provider(fence_timeout_ms: u64) -> Arc<ClusterProvider> {
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
    config.slot_map[(SLOT0 ^ 1) as usize] = HashSlot {
      worker_id: remote_worker_id,
      state: SlotState::Stable,
    };
  }
  cp.set_cluster_node_timeout_ms(fence_timeout_ms);
  cp
}

/// 打开迁移测试存储（复活启用位由调用方裁决，SLOTS 链用启用态）
fn open_migrate_store(tag: &str, reviv_enabled: bool) -> Arc<WedbStore<SegmentedDevice>> {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join(tag)).unwrap());
  let config = test_store_config().with_revivification(reviv_enabled);
  Arc::new(WedbStore::open(config, device).unwrap())
}

/// 迁移驱动发送侧 spec
fn migrate_spec(port: i32, transfer: TransferOption) -> MigrateTaskSpec {
  MigrateTaskSpec {
    source_node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
    target_address: "127.0.0.1".to_string(),
    target_port: port,
    target_node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE12,
    username: String::new(),
    passwd: String::new(),
    copy_option: false,
    replace_option: false,
    timeout: 5000,
    transfer_option: transfer,
  }
}

/// 解析假端监听地址的端口号
fn port_of(addr: &str) -> i32 {
  addr.rsplit(':').next().unwrap().parse().unwrap()
}

/// 会话槽位集（库级定槽：全部用例键恒共 SLOT0）
fn slot_set() -> HashSet<i32> {
  [i32::from(SLOT0)].into_iter().collect()
}

/// 预置 string 键
async fn seed_str(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  StorageSession::new_readonly(batch)
    .upsert_string(key, VAL)
    .await
    .unwrap();
}

/// 读库内 string（判败断言键权用）
async fn read_str(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  StorageSession::new_readonly(batch)
    .read_string(key)
    .await
    .unwrap()
}

/// 读活值分类（带外树键零删直证：判败后仍判 TieredTree 即存根在位）
async fn live_kind(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> LiveValue {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  read_live_value(&storage, None, key).await.unwrap()
}

/// 注册集群会话并批首纪元快照（diskless 恒不追平夹具同形：此后每次栅栏纪元
/// 推进对其恒落后）
fn acquire_lag(cp: &Arc<ClusterProvider>) -> Arc<ClusterSession> {
  let s = cp.create_cluster_session();
  s.acquire_current_epoch();
  s
}

/// 纪元哨兵任务：自旋至 provider 纪元推进到 `at`（含）后执行一次性动作
/// （动作同步完成，必落在等待栅栏自旋让步窗内、下一轮静止检查之前）
fn spawn_at_epoch(cp: &Arc<ClusterProvider>, at: i64, action: impl FnOnce() + Send + 'static) {
  let cp = Arc::clone(cp);
  spawn(async move {
    while cp.current_epoch() < at {
      sleep(Duration::from_millis(1)).await;
    }
    action();
  })
  .detach();
}

/// begin 门阻塞夹具（SLOTS 族专用）：预置落后会话逼 begin 纪元门自旋让出
/// 执行器，哨兵于门纪元追平 begin、落后后继栅栏——据此区分 begin 门与
/// TRANSMITTING/DELETING 栅栏位点（KEYS 链 begin 无门，静态落后即可）
fn install_begin_gate_ladder(cp: &Arc<ClusterProvider>, gate_epoch: i64, slot: &LagSlot) {
  let blocker = acquire_lag(cp);
  let cp2 = Arc::clone(cp);
  let slot2 = Arc::clone(slot);
  spawn_at_epoch(cp, gate_epoch, move || {
    slot2.lock().push(acquire_lag(&cp2));
    drop(blocker);
  });
}

/// 解析缓冲中首个完整 RESP2 数组帧（对标 cluster_migration 同名件）
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

/// 合成 RESERVE 应答的上下文 id 发号器
static RESERVE_CTX_ID: AtomicU64 = AtomicU64::new(9_000_000);

/// CLUSTER RESERVE VECTOR_SET_CONTEXTS 合成应答（向量集收尾需真实上下文 id
/// 完成重映射，脚本 +OK 非合法应答；对标 cluster_migration 同名件）
fn reserve_reply(args: &[&[u8]]) -> Vec<u8> {
  let count: usize = args
    .get(3)
    .and_then(|a| from_utf8(a).ok())
    .and_then(|s| s.parse().ok())
    .unwrap_or(0);
  let ids: Vec<Vec<u8>> = (0..count)
    .map(|_| {
      RESERVE_CTX_ID
        .fetch_add(1, Ordering::Relaxed)
        .to_string()
        .into_bytes()
    })
    .collect();
  let refs: Vec<&[u8]> = ids.iter().map(Vec::as_slice).collect();
  resp_frame(&refs)
}

/// 假迁移目标端（对标 cluster_migration 同名件）：逐帧解析按脚本弹答，
/// CLUSTER RESERVE 合成就地应答不耗脚本；每帧前 3 参记入 seen 供帧序断言
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
            let is_reserve = args.len() >= 4
              && args[0].eq_ignore_ascii_case(b"CLUSTER")
              && args[1].eq_ignore_ascii_case(b"RESERVE");
            let reply = if is_reserve {
              Some(reserve_reply(&args))
            } else {
              script.pop_front().map(|r| r.to_vec())
            };
            let head = args
              .iter()
              .take(3)
              .map(|a| String::from_utf8_lossy(a).into_owned())
              .collect::<Vec<_>>()
              .join(" ");
            acc.drain(..frame_len);
            seen.lock().push(head);
            if let Some(reply) = reply
              && stream.write_all(reply).await.is_err()
            {
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

/// 数据导入帧计数（CLUSTER MIGRATE 载荷帧——「目标零导入」直证件）
fn import_frames(seen: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
  seen
    .lock()
    .iter()
    .filter(|f| f.starts_with("CLUSTER MIGRATE"))
    .cloned()
    .collect()
}

/// 栅栏判败共性断言：Err 文案 + recover STABLE + 任务移除
fn assert_fence_failure(
  err: &impl fmt::Debug,
  seen: &Arc<Mutex<Vec<String>>>,
  cp: &ClusterProvider,
) {
  let text = format!("{err:?}");
  assert!(
    text.contains("迁移纪元转换等待失败"),
    "应透出纪元排空未达成判败文案: {text}"
  );
  assert!(
    seen
      .lock()
      .iter()
      .any(|f| f.contains("SETSLOTSRANGE STABLE")),
    "判败必须走 recover 远端 STABLE: {:?}",
    seen.lock()
  );
  assert_eq!(
    cp.migration_manager().unwrap().get_migration_task_count(),
    0,
    "判败路径必须移除任务"
  );
}

/// KEYS 链 TRANSMITTING 栅栏判败：静态落后会话（begin 无纪元门）——取数前
/// 即 FAIL，源端键零删、目标端零导入帧；夹具释放后排空恢复放行
#[test]
fn keys_transmitting_fence_fails_closed() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider(FENCE_TIMEOUT_MS);
    let store = open_migrate_store("mef_kt.db", false);
    cp.set_store(Arc::clone(&store));
    let k1 = b"mef_kt1";
    seed_str(&store, k1).await;

    let lag = acquire_lag(&cp);
    let epoch_before = cp.current_epoch();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr = scripted_migrate_target(vec![vec![b"+OK\r\n"; 8]], Arc::clone(&seen)).await;
    let err = run_keys_migration_driver(
      Arc::clone(&cp),
      Arc::clone(&store),
      migrate_spec(port_of(&addr), TransferOption::Keys),
      &slot_set(),
      &[k1.to_vec()],
    )
    .await
    .unwrap_err();
    assert_fence_failure(&err, &seen, &cp);
    // 位点指认：KEYS 链仅 TRANSMITTING 栅栏推过一次纪元
    assert_eq!(
      cp.current_epoch() - epoch_before,
      1,
      "判败位点应为 TRANSMITTING 栅栏（恰 1 次纪元推进）"
    );
    assert!(
      import_frames(&seen).is_empty(),
      "取数前即判败，目标端零导入帧: {seen:?}"
    );
    assert_eq!(
      read_str(&store, k1).await.as_deref(),
      Some(VAL),
      "源端键零删"
    );

    // 夹具释放后排空恢复放行（注册面无残留锁死静止判定）
    drop(lag);
    cp.set_cluster_node_timeout_ms(2_000);
    assert!(
      cp.bump_and_wait_for_epoch_transition_async().await,
      "夹具释放后排空等待须恢复放行"
    );
  });
}

/// KEYS 链 DELETING 栅栏判败：活值钩在传输相内补落后快照——TRANSMITTING 相
/// 照常传输（目标端有导入帧），落删除前排空未达成即 FAIL，源端键零删
#[test]
fn keys_deleting_fence_fails_closed() {
  let _serial = HOOK_SERIAL.lock();
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider(FENCE_TIMEOUT_MS);
    let store = open_migrate_store("mef_kd.db", false);
    cp.set_store(Arc::clone(&store));
    let k1 = b"mef_kd1";
    seed_str(&store, k1).await;

    let keeper: LagSlot = Arc::new(Mutex::new(Vec::new()));
    {
      let cp2 = Arc::clone(&cp);
      let keeper2 = Arc::clone(&keeper);
      *TEST_LIVE_VALUE_READ_HOOK.lock() = Some(Box::new(move || {
        keeper2.lock().push(acquire_lag(&cp2));
      }));
    }
    let epoch_before = cp.current_epoch();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr = scripted_migrate_target(vec![vec![b"+OK\r\n"; 8]], Arc::clone(&seen)).await;
    let err = run_keys_migration_driver(
      Arc::clone(&cp),
      Arc::clone(&store),
      migrate_spec(port_of(&addr), TransferOption::Keys),
      &slot_set(),
      &[k1.to_vec()],
    )
    .await
    .unwrap_err();
    assert!(
      TEST_LIVE_VALUE_READ_HOOK.lock().is_none(),
      "钩子必须已被传输相消费"
    );
    assert_fence_failure(&err, &seen, &cp);
    assert_eq!(
      cp.current_epoch() - epoch_before,
      2,
      "判败位点应为 DELETING 栅栏（TRANSMITTING+DELETING 恰 2 次纪元推进）"
    );
    assert!(
      !import_frames(&seen).is_empty(),
      "TRANSMITTING 相应已放行传输: {seen:?}"
    );
    assert_eq!(
      read_str(&store, k1).await.as_deref(),
      Some(VAL),
      "源端键零删"
    );
    drop(keeper);
  });
}

/// SLOTS 链 TRANSMITTING 栅栏判败：begin 门放行（阻塞者+哨兵阶梯）后，批内
/// 取数前排空未达成即 FAIL——目标零导入帧、源端键零删、会话终态 Fail
#[test]
fn slots_transmitting_fence_fails_closed() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider(FENCE_TIMEOUT_MS);
    let store = open_migrate_store("mef_st.db", true);
    cp.set_store(Arc::clone(&store));
    let k1 = b"mef_st1";
    seed_str(&store, k1).await;

    let epoch_before = cp.current_epoch();
    let slot: LagSlot = Arc::new(Mutex::new(Vec::new()));
    install_begin_gate_ladder(&cp, epoch_before + 1, &slot);

    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr = scripted_migrate_target(vec![vec![b"+OK\r\n"; 8]], Arc::clone(&seen)).await;
    let spec = migrate_spec(port_of(&addr), TransferOption::Slots);
    let session = try_add_slots_migration_task(&cp, spec.clone(), &slot_set()).unwrap();
    let err = run_slots_migration_task(Arc::clone(&store), spec, Arc::clone(&session))
      .await
      .unwrap_err();
    assert_fence_failure(&err, &seen, &cp);
    assert_eq!(
      cp.current_epoch() - epoch_before,
      2,
      "判败位点应为批内 TRANSMITTING 栅栏（begin 门+TRANSMITTING 恰 2 次推进）"
    );
    assert!(
      import_frames(&seen).is_empty(),
      "取数前即判败，目标端零导入帧: {seen:?}"
    );
    assert_eq!(
      read_str(&store, k1).await.as_deref(),
      Some(VAL),
      "源端键零删"
    );
    assert_eq!(
      *session.status.read(),
      MigrateState::Fail,
      "会话终态须 Fail"
    );
    drop(slot);
  });
}

/// SLOTS 链 DELETING 栅栏判败：活值钩在批传输相内补落后快照——TRANSMITTING
/// 照常传输，落删除前排空未达成即 FAIL，源端键零删
#[test]
fn slots_deleting_fence_fails_closed() {
  let _serial = HOOK_SERIAL.lock();
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider(FENCE_TIMEOUT_MS);
    let store = open_migrate_store("mef_sd.db", true);
    cp.set_store(Arc::clone(&store));
    let k1 = b"mef_sd1";
    seed_str(&store, k1).await;

    let keeper: LagSlot = Arc::new(Mutex::new(Vec::new()));
    {
      let cp2 = Arc::clone(&cp);
      let keeper2 = Arc::clone(&keeper);
      *TEST_LIVE_VALUE_READ_HOOK.lock() = Some(Box::new(move || {
        keeper2.lock().push(acquire_lag(&cp2));
      }));
    }
    let epoch_before = cp.current_epoch();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr = scripted_migrate_target(vec![vec![b"+OK\r\n"; 10]], Arc::clone(&seen)).await;
    let spec = migrate_spec(port_of(&addr), TransferOption::Slots);
    let session = try_add_slots_migration_task(&cp, spec.clone(), &slot_set()).unwrap();
    let err = run_slots_migration_task(Arc::clone(&store), spec, Arc::clone(&session))
      .await
      .unwrap_err();
    assert!(
      TEST_LIVE_VALUE_READ_HOOK.lock().is_none(),
      "钩子必须已被批传输相消费"
    );
    assert_fence_failure(&err, &seen, &cp);
    assert_eq!(
      cp.current_epoch() - epoch_before,
      3,
      "判败位点应为批内 DELETING 栅栏（begin+TRANSMITTING+DELETING 恰 3 次推进）"
    );
    assert!(
      !import_frames(&seen).is_empty(),
      "TRANSMITTING 相应已放行传输: {seen:?}"
    );
    assert_eq!(
      read_str(&store, k1).await.as_deref(),
      Some(VAL),
      "源端键零删"
    );
    assert_eq!(
      *session.status.read(),
      MigrateState::Fail,
      "会话终态须 Fail"
    );
    drop(keeper);
  });
}

/// 造 wbftree 升阶页存储集合键（SLOTS 发现面 load_collection_stub 收录、
/// 带外分块流通道迁移的判据形态，对标 tiered_sync_migration 造键件）
async fn seed_tree(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) {
  let sess = store.new_session().unwrap();
  sess
    .promote_collection_to_bftree(
      key,
      GarnetObjectType::Hash,
      vec![
        (b"f1".to_vec(), b"v1".to_vec()),
        (b"f2".to_vec(), b"v2".to_vec()),
      ],
      i64::MAX,
      false,
    )
    .await
    .unwrap();
}

/// SLOTS 链 RangeIndex 带外子链 TRANSMITTING 栅栏判败：纯树键批次跳过带内
/// 段直达带外，begin 门放行后快照取数前排空未达成即 FAIL——零 RI 流帧、
/// 源端存根零删
#[test]
fn slots_range_index_transmitting_fence_fails_closed() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider(FENCE_TIMEOUT_MS);
    let store = open_migrate_store("mef_rit.db", true);
    cp.set_store(Arc::clone(&store));
    let k1 = b"mef_rit1";
    seed_tree(&store, k1).await;

    let epoch_before = cp.current_epoch();
    let slot: LagSlot = Arc::new(Mutex::new(Vec::new()));
    install_begin_gate_ladder(&cp, epoch_before + 1, &slot);

    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr = scripted_migrate_target(vec![vec![b"+OK\r\n"; 8]], Arc::clone(&seen)).await;
    let spec = migrate_spec(port_of(&addr), TransferOption::Slots);
    let session = try_add_slots_migration_task(&cp, spec.clone(), &slot_set()).unwrap();
    let err = run_slots_migration_task(Arc::clone(&store), spec, Arc::clone(&session))
      .await
      .unwrap_err();
    assert_fence_failure(&err, &seen, &cp);
    assert_eq!(
      cp.current_epoch() - epoch_before,
      2,
      "判败位点应为带外 TRANSMITTING 栅栏（begin+RI-T 恰 2 次推进）"
    );
    assert!(
      import_frames(&seen).is_empty(),
      "快照取数前即判败，零 RI 流帧: {seen:?}"
    );
    assert!(
      matches!(live_kind(&store, k1).await, LiveValue::TieredTree),
      "源端树键存根零删"
    );
    assert_eq!(
      *session.status.read(),
      MigrateState::Fail,
      "会话终态须 Fail"
    );
    drop(slot);
  });
}

/// SLOTS 链 RangeIndex 带外子链 DELETING 栅栏判败：二级哨兵在 RI
/// TRANSMITTING 栅栏自旋窗内放行一级落后并补二级快照——整流传输照常，落删除
/// 前排空未达成即 FAIL，源端存根与数据零删（Err 上抛由调用侧既有 recover 臂
/// 收敛，同形不另设判点）
#[test]
fn slots_range_index_deleting_fence_fails_closed() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider(FENCE_TIMEOUT_MS);
    let store = open_migrate_store("mef_rid.db", true);
    cp.set_store(Arc::clone(&store));
    let k1 = b"mef_rid1";
    seed_tree(&store, k1).await;

    let epoch_before = cp.current_epoch();
    let slot: LagSlot = Arc::new(Mutex::new(Vec::new()));
    install_begin_gate_ladder(&cp, epoch_before + 1, &slot);
    // 二级哨兵：RI TRANSMITTING 栅栏自旋窗内（纪元 = begin+2）放行一级落后、
    // 补二级快照 → 该栅栏追平、DELETING 栅栏（begin+3）恒落后
    {
      let cp2 = Arc::clone(&cp);
      let slot2 = Arc::clone(&slot);
      spawn_at_epoch(&cp, epoch_before + 2, move || {
        let l2 = acquire_lag(&cp2);
        if let Some(l1) = slot2.lock().pop() {
          l1.release_current_epoch();
        }
        slot2.lock().push(l2);
      });
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr = scripted_migrate_target(vec![vec![b"+OK\r\n"; 12]], Arc::clone(&seen)).await;
    let spec = migrate_spec(port_of(&addr), TransferOption::Slots);
    let session = try_add_slots_migration_task(&cp, spec.clone(), &slot_set()).unwrap();
    let err = run_slots_migration_task(Arc::clone(&store), spec, Arc::clone(&session))
      .await
      .unwrap_err();
    assert_fence_failure(&err, &seen, &cp);
    assert_eq!(
      cp.current_epoch() - epoch_before,
      3,
      "判败位点应为带外 DELETING 栅栏（begin+RI-T+RI-D 恰 3 次推进）"
    );
    assert!(
      !import_frames(&seen).is_empty(),
      "RI 整流传输应已发出: {seen:?}"
    );
    assert!(
      matches!(live_kind(&store, k1).await, LiveValue::TieredTree),
      "源端树键存根零删"
    );
    assert_eq!(
      *session.status.read(),
      MigrateState::Fail,
      "会话终态须 Fail"
    );
    drop(slot);
  });
}

/// 测试向量管理器（生产装配同形态，对标 cluster_migration 同名件）
fn vector_manager_for() -> Arc<VectorManager> {
  let callbacks = Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new()));
  Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..VectorManagerOptions::default()
    },
    callbacks,
  ))
}

/// VADD 直调建向量集（登记表+内存图+磁盘记录；wkv 槽扫描对向量集键不可见，
/// SLOTS 链按登记表独立发现）
async fn seed_vector_set(
  vm: &Arc<VectorManager>,
  store: &Arc<WedbStore<SegmentedDevice>>,
  key: &[u8],
) {
  let bind_sess = store.new_session().unwrap();
  let _domain = ActiveVectorSessionGuard::bind(&bind_sess);
  let params = CreateIndexParams {
    hash_slot: SLOT0,
    dims: 2,
    reduce_dims: 0,
    quant: VectorQuantType::NoQuant,
    build_exploration_factor: 200,
    num_links: 8,
    distance_metric: VectorDistanceMetricType::L2,
  };
  let (index, _lock) = vm
    .read_or_create_vector_index(SessionPrefixBuf::ROOT.as_slice(), key, Some(&params))
    .await
    .unwrap();
  let index_value = index.to_bytes();
  drop(_lock);
  let values: [u8; 8] = [0, 0, 128, 63, 0, 0, 0, 64]; // f32 [1.0, 2.0]
  let args = VectorAddArgs::new(b"elem1".as_slice(), VectorValueType::FP32, &values, b"");
  assert_eq!(
    vm.try_add(SessionPrefixBuf::ROOT.as_slice(), key, &index_value, &args)
      .await,
    Ok(VectorManagerResult::OK)
  );
}

/// 向量集登记表发现（源端零删直证件）
fn vector_registered(vm: &Arc<VectorManager>) -> usize {
  let mut slots = BTreeSet::new();
  slots.insert(i32::from(SLOT0));
  vm.get_vector_set_keys_for_slots(&slots).len()
}

/// SLOTS 链向量集带外子链 TRANSMITTING 栅栏判败：槽内仅向量集键（扫描空批
/// 直达收尾段），begin 门放行后导出前排空未达成即 FAIL——RESERVE/索引/元素
/// 帧零发出、登记表零删
#[test]
fn slots_vector_set_transmitting_fence_fails_closed() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider(FENCE_TIMEOUT_MS);
    let store = open_migrate_store("mef_vt.db", true);
    let vm = vector_manager_for();
    cp.set_store(Arc::clone(&store));
    cp.set_vector_manager(Arc::clone(&vm));
    let k1 = b"mef_vt1";
    seed_vector_set(&vm, &store, k1).await;

    let epoch_before = cp.current_epoch();
    let slot: LagSlot = Arc::new(Mutex::new(Vec::new()));
    install_begin_gate_ladder(&cp, epoch_before + 1, &slot);

    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr = scripted_migrate_target(vec![vec![b"+OK\r\n"; 8]], Arc::clone(&seen)).await;
    let spec = migrate_spec(port_of(&addr), TransferOption::Slots);
    let session = try_add_slots_migration_task(&cp, spec.clone(), &slot_set()).unwrap();
    let err = run_slots_migration_task(Arc::clone(&store), spec, Arc::clone(&session))
      .await
      .unwrap_err();
    assert_fence_failure(&err, &seen, &cp);
    assert_eq!(
      cp.current_epoch() - epoch_before,
      2,
      "判败位点应为向量集带外 TRANSMITTING 栅栏（begin+VT 恰 2 次推进）"
    );
    assert!(
      import_frames(&seen).is_empty(),
      "导出前排空未达成，零向量帧: {seen:?}"
    );
    assert_eq!(vector_registered(&vm), 1, "源端向量集登记表零删");
    assert_eq!(
      *session.status.read(),
      MigrateState::Fail,
      "会话终态须 Fail"
    );
    drop(slot);
  });
}

/// SLOTS 链向量集带外子链 DELETING 栅栏判败：二级哨兵放行向量
/// TRANSMITTING 栅栏后补二级快照——RESERVE/索引/元素帧照常导出，源端删除前
/// 排空未达成即 FAIL，登记表零删
#[test]
fn slots_vector_set_deleting_fence_fails_closed() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider(FENCE_TIMEOUT_MS);
    let store = open_migrate_store("mef_vd.db", true);
    let vm = vector_manager_for();
    cp.set_store(Arc::clone(&store));
    cp.set_vector_manager(Arc::clone(&vm));
    let k1 = b"mef_vd1";
    seed_vector_set(&vm, &store, k1).await;

    let epoch_before = cp.current_epoch();
    let slot: LagSlot = Arc::new(Mutex::new(Vec::new()));
    install_begin_gate_ladder(&cp, epoch_before + 1, &slot);
    {
      let cp2 = Arc::clone(&cp);
      let slot2 = Arc::clone(&slot);
      spawn_at_epoch(&cp, epoch_before + 2, move || {
        let l2 = acquire_lag(&cp2);
        if let Some(l1) = slot2.lock().pop() {
          l1.release_current_epoch();
        }
        slot2.lock().push(l2);
      });
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr = scripted_migrate_target(vec![vec![b"+OK\r\n"; 15]], Arc::clone(&seen)).await;
    let spec = migrate_spec(port_of(&addr), TransferOption::Slots);
    let session = try_add_slots_migration_task(&cp, spec.clone(), &slot_set()).unwrap();
    let err = run_slots_migration_task(Arc::clone(&store), spec, Arc::clone(&session))
      .await
      .unwrap_err();
    assert_fence_failure(&err, &seen, &cp);
    assert_eq!(
      cp.current_epoch() - epoch_before,
      3,
      "判败位点应为向量集带外 DELETING 栅栏（begin+VT+VD 恰 3 次推进）"
    );
    assert!(!import_frames(&seen).is_empty(), "向量帧应已导出: {seen:?}");
    assert_eq!(vector_registered(&vm), 1, "源端向量集登记表零删");
    assert_eq!(
      *session.status.read(),
      MigrateState::Fail,
      "会话终态须 Fail"
    );
    drop(slot);
  });
}
