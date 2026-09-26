//! 集群迁移 DELETING 收口「只删不释」回归锁面
//! （工单 wedb-migrate-deleting-unheld-release-tree-claim，r25 案 4）
//!
//! 背景：本仓迁移链对树键 claim 注册表（wbftree RangeIndexManager migrating）
//! 零登记，C# 对位 DeleteKeysAsync / MigrateOperation.DeleteRangeIndex 的
//! DELETING 收口只调 DELETE 不触碰任何键级登记（键级 claim 系 C# 上游
//! TODO）。rust 曾有两处盲释残留（keys.rs tree_keys 逐键
//! release_migration_claim 与 migrate_session_range_index.rs 同款循环节）：
//! release 按 key_id 判等移除、不校验持有者（持有者纪律成文于
//! wkv/src/range_index/migration.rs rename_range_index 释放配对表——仅限
//! try_claim 成功后的持有者调用），盲释即窃取并发 RENAME/换入窗（含后台懒
//! 降阶持窗跨全 await 的合法重合形）的同源键 claim，九写闸（stub/heal/ops/
//! DEL 两臂/TTL 闸）全以 migration_claimed 单判据，解锁瞬间自家 delete
//! 直穿 DEL 臂排空销毁他人正在搬运的树——终态源端复活树/双域残留＋主从发散。
//!
//! 锁形：树键 K 于 DELETING 栅栏自旋窗内经 try_swap_in_window 挂换入窗 claim
//! （对位 tiered_demote.rs 后台降阶持窗形态；换入窗守卫由他人在册持有），
//! 夹具复用 migrate_epoch_drain_failclose 的恒不追平落后会话 + 纪元哨兵阶梯
//! 使 DELETING 栅栏恰于 claim 登记后放行——
//! - KEYS 链：修复形收口只删不释，K 的删除被 DEL 闸 MigrationBusy 挡下、
//!   log::error 留痕落源（控制流不变），整链照常收敛（对照键零删直证收口
//!   已跑完）；claim 全程在册、守卫持有者收尾释放后装载门收敛。
//! - SLOTS 链 RangeIndex 带外子链：修复形删除臂 Err 上抛沿调用侧既有
//!   recover 臂收敛（判败回 STABLE），K 不排空、claim 仍在册。
//!
//! revert-proof（还原任一盲释循环节即对应用例转红，实测见工单回报）：
//! - keys.rs 盲释还原 → keys_deleting_keeps_foreign_claim：claim 被窃、
//!   DEL 闸解除、K 被静默排空销毁（存活断言＋在册断言双红）；
//! - migrate_session_range_index.rs 盲释还原 → slots_range_index_deleting
//!   _keeps_foreign_claim：同形窃取，删除得手、链不再判败（unwrap_err
//!   断言＋存活＋在册三面转红）。

use std::{
  collections::VecDeque,
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
use wkv::{Error as WkvError, SwapInWindowGuard, WedbStore};
use wnode::{ClusterSessionFace, storage::session::storage_session::StorageSession};
use wtest_base::{resp_frame, test_store_config};
use wval::GarnetObjectType;

/// 默认会话 (0,0) 库槽位（库级定槽：键内容不参与定槽）
const SLOT0: u16 = slot_of(0, 0);
/// 纪元静止等待上限毫秒（落后夹具下超时必达；留足哨兵任务 1ms 节拍的让渡窗）
const FENCE_TIMEOUT_MS: u64 = 300;
/// 源端 string 键值（对照键零删直证）
const VAL: &[u8] = b"v1";

/// 故障注入钩子互斥（TEST_LIVE_VALUE_READ_HOOK 为进程级一次性钩子，
/// 与 migrate_epoch_drain_failclose 同池语义，本册用毕即清）
static HOOK_SERIAL: Mutex<()> = Mutex::new(());

/// 落后夹具会话保管槽（Arc 存活期内注册有效，释放经本槽摘除）
type LagSlot = Arc<Mutex<Vec<Arc<ClusterSession>>>>;

/// 装配双主节点拓扑（对标 migrate_epoch_drain_failclose 同名件）
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

/// 迁移驱动发送侧 spec（非 copy：必触 DELETING 收口臂）
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

/// 读库内 string（对照键零删断言用）
async fn read_str(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  StorageSession::new_readonly(batch)
    .read_string(key)
    .await
    .unwrap()
}

/// 读活值分类（树键零删直证：仍判 TieredTree 即 Meta 存根在位；
/// Meta 域原读不经 claim 门，持 claim 下仍可达，正合观测位形）
async fn live_kind(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> LiveValue {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  read_live_value(&storage, None, key).await.unwrap()
}

/// 造 wbftree 升阶页存储集合键（对标 migrate_epoch_drain_failclose 同名件）
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

/// 注册集群会话并批首纪元快照（恒不追平夹具：此后每次纪元推进对其恒落后）
fn acquire_lag(cp: &Arc<ClusterProvider>) -> Arc<ClusterSession> {
  let s = cp.create_cluster_session();
  s.acquire_current_epoch();
  s
}

/// 纪元哨兵任务：自旋至 provider 纪元推进到 `at`（含）后执行一次性动作
/// （动作同步完成，必落在等待栅栏自旋让步窗内、下一轮静止检查之前；
/// 对标 migrate_epoch_drain_failclose 同名件）
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

/// begin 门阻塞夹具（SLOTS 链专用）：预置落后会话逼 begin 纪元门自旋让出
/// 执行器，哨兵于门纪元恰成「追平 begin、落后后继」相位放行 begin 再交棒
/// （对标 migrate_epoch_drain_failclose 同名件）
fn install_begin_gate_ladder(cp: &Arc<ClusterProvider>, gate_epoch: i64, slot: &LagSlot) {
  let blocker = acquire_lag(cp);
  let cp2 = Arc::clone(cp);
  let slot2 = Arc::clone(slot);
  spawn_at_epoch(cp, gate_epoch, move || {
    slot2.lock().push(acquire_lag(&cp2));
    drop(blocker);
  });
}

/// 解析缓冲中首个完整 RESP2 数组帧（对标 migrate_epoch_drain_failclose 同名件）
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

/// 合成应答帧 id 发号器（CLUSTER RESERVE 臂用，对标同名件；本册键集不含
/// 向量集，仍留合成臂防脚本面意外触达）
static RESERVE_CTX_ID: AtomicU64 = AtomicU64::new(9_200_000);

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

/// 假迁移目标端（对标同名件）：逐帧解析按脚本弹答
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

/// 他人在册换入窗断言件：claim 经装载门禁正当生效（load_collection_stub
/// 回 MigrationBusy，即九写闸封堵面在位，非仅注册表字面计数）
async fn assert_window_blocks_load(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) {
  let sess = store.new_session().unwrap();
  match sess.load_collection_stub(key).await {
    Err(WkvError::MigrationBusy) => {}
    other => panic!(
      "claim 在册期装载门应回 MigrationBusy，实际 {:?}",
      err_dbg(other)
    ),
  }
}

/// claim 已收敛释放断言件：装载门放行、存根在位
async fn assert_window_converged(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) {
  let sess = store.new_session().unwrap();
  match sess.load_collection_stub(key).await {
    Ok(Some(_)) => {}
    other => panic!(
      "持有者释放后装载门应放行且存根在位，实际 {:?}",
      err_dbg(other)
    ),
  }
}

fn err_dbg<T: fmt::Debug, E: fmt::Debug>(r: Result<T, E>) -> String {
  format!("{r:?}")
}

/// KEYS 链 DELETING 收口不窃他人在册 claim：树键 K 与对照 string 键共批迁移，
/// 活值钩于传输相登记落后会话逼 DELETING 栅栏自旋，栅栏纪元哨兵于窗内以
/// try_swap_in_window 挂 K 的换入窗 claim 再放行栅栏（对位后台懒降阶持窗跨
/// 全 await 与 DELETING 窗合法重合形）——修复形：收口只删不释，K 的删除撞
/// DEL 闸留痕落源、对照键照删、整链收敛；claim 全程在册且由持有者自身收尾
/// 释放后收敛。盲释还原即 claim 被窃、K 被静默排空（revert-proof 双红）
#[test]
fn keys_deleting_keeps_foreign_claim() {
  let _serial = HOOK_SERIAL.lock();
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider(FENCE_TIMEOUT_MS);
    let store = open_migrate_store("uc_keys.db", true);
    cp.set_store(Arc::clone(&store));
    let k_str = b"uc_kstr";
    let k_tree = b"uc_ktree";
    seed_str(&store, k_str).await;
    seed_tree(&store, k_tree).await;

    // 树身份键 = 物理 Meta 键（登记侧同派生单点）
    let id_key = store.new_session().unwrap().session_meta_key(k_tree);
    let mgr = Arc::clone(store.range_index());

    let keeper: LagSlot = Arc::new(Mutex::new(Vec::new()));
    {
      let cp2 = Arc::clone(&cp);
      let keeper2 = Arc::clone(&keeper);
      *TEST_LIVE_VALUE_READ_HOOK.lock() = Some(Box::new(move || {
        keeper2.lock().push(acquire_lag(&cp2));
      }));
    }
    let guard_slot: Arc<Mutex<Option<SwapInWindowGuard>>> = Arc::new(Mutex::new(None));
    let epoch_before = cp.current_epoch();
    // DELETING 栅栏（恰第 2 次推进）自旋窗内挂窗再放行：登记先于放行，
    // 收口循环节必然面对在册他人 claim
    {
      let store2 = Arc::clone(&store);
      let k = k_tree.to_vec();
      let guard2 = Arc::clone(&guard_slot);
      let keeper2 = Arc::clone(&keeper);
      spawn_at_epoch(&cp, epoch_before + 2, move || {
        let sess = store2.new_session().unwrap();
        let guard = sess.try_swap_in_window(&k).expect("换入窗 claim 应可登记");
        drop(sess);
        *guard2.lock() = Some(guard);
        for l in keeper2.lock().drain(..) {
          l.release_current_epoch();
        }
      });
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr = scripted_migrate_target(vec![vec![b"+OK\r\n"; 16]], Arc::clone(&seen)).await;
    let res = run_keys_migration_driver(
      Arc::clone(&cp),
      Arc::clone(&store),
      migrate_spec(port_of(&addr), TransferOption::Keys),
      &slot_set(),
      &[k_str.to_vec(), k_tree.to_vec()],
    )
    .await;
    assert!(
      TEST_LIVE_VALUE_READ_HOOK.lock().is_none(),
      "钩子必须已被传输相消费"
    );
    res.expect("只删不释形下 K 的删除被 DEL 闸挡下仅留痕，整链应正常收敛");

    // 在册直证：盲释还原即此处转红
    assert!(
      mgr.migration_claimed(&id_key),
      "DELETING 收口不得未持有即释他人换入窗 claim"
    );
    // 封堵正当生效（非空转在册位）
    assert_window_blocks_load(&store, k_tree).await;
    // 源端不静默排空：K 存根在位；对照键已删，直证收口臂跑到位
    assert!(
      matches!(live_kind(&store, k_tree).await, LiveValue::TieredTree),
      "源端树键不得被盲释后排空销毁"
    );
    assert_eq!(read_str(&store, k_str).await, None, "对照 string 键应删");
    // 位点指认：TRANSMITTING+DELETING+MIGRATED 归位恰 3 次推进（收口全程走完）
    assert_eq!(
      cp.current_epoch() - epoch_before,
      3,
      "DELETING 收口应完整走完"
    );

    // 持有者自身收尾释放 → 收敛（守卫 Drop 单点释 claim）
    drop(guard_slot.lock().take());
    assert!(!mgr.migration_claimed(&id_key), "持有者释放后 claim 应摘除");
    assert_window_converged(&store, k_tree).await;
    assert!(
      matches!(live_kind(&store, k_tree).await, LiveValue::TieredTree),
      "收敛后源端树键完整在位"
    );
  });
}

/// SLOTS 链 RangeIndex 带外子链同款锁面：纯树键批次，begin 门阶梯 +
/// RI TRANSMITTING 二级哨兵交棒落后会话，RI DELETING 栅栏窗内挂 K 的换入窗
/// claim 再放行——修复形：只删不释，delete_string 撞 DEL 闸 Err 上抛沿调用侧
/// 既有 recover 臂收敛（远端回 STABLE、会话判败），K 零删且 claim 在册；
/// 持有者释放后装载门收敛。盲释还原即删除得手、链不再判败（revert-proof
/// 三面转红）
#[test]
fn slots_range_index_deleting_keeps_foreign_claim() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider(FENCE_TIMEOUT_MS);
    let store = open_migrate_store("uc_ri.db", true);
    cp.set_store(Arc::clone(&store));
    let k_tree = b"uc_ritree";
    seed_tree(&store, k_tree).await;

    let id_key = store.new_session().unwrap().session_meta_key(k_tree);
    let mgr = Arc::clone(store.range_index());

    let epoch_before = cp.current_epoch();
    let slot: LagSlot = Arc::new(Mutex::new(Vec::new()));
    // 一级：begin 门（第 1 推）放行交棒 l1
    install_begin_gate_ladder(&cp, epoch_before + 1, &slot);
    // 二级：RI TRANSMITTING 栅栏（第 2 推）窗内放行 l1、补 l2 落后快照，
    // 逼 RI DELETING 栅栏（第 3 推）自旋
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
    // 三级：RI DELETING 栅栏窗内挂 K 换入窗 claim 再放行 l2
    let guard_slot: Arc<Mutex<Option<SwapInWindowGuard>>> = Arc::new(Mutex::new(None));
    {
      let store2 = Arc::clone(&store);
      let k = k_tree.to_vec();
      let guard2 = Arc::clone(&guard_slot);
      let slot2 = Arc::clone(&slot);
      spawn_at_epoch(&cp, epoch_before + 3, move || {
        let sess = store2.new_session().unwrap();
        let guard = sess.try_swap_in_window(&k).expect("换入窗 claim 应可登记");
        drop(sess);
        *guard2.lock() = Some(guard);
        for l in slot2.lock().drain(..) {
          l.release_current_epoch();
        }
      });
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr = scripted_migrate_target(vec![vec![b"+OK\r\n"; 16]], Arc::clone(&seen)).await;
    let spec = migrate_spec(port_of(&addr), TransferOption::Slots);
    let session = try_add_slots_migration_task(&cp, spec.clone(), &slot_set()).unwrap();
    let err = run_slots_migration_task(Arc::clone(&store), spec, Arc::clone(&session))
      .await
      .expect_err("持 claim 键删除应被 DEL 闸挡下 Err 上抛判败（只删不释形）");
    let text = format!("{err:?}");
    assert!(
      text.contains("MigrationBusy") || text.contains("忙"),
      "判败文案应透出装载/删除忙闸: {text}"
    );
    assert!(
      seen
        .lock()
        .iter()
        .any(|f| f.contains("SETSLOTSRANGE STABLE")),
      "Err 上抛须走 recover 远端 STABLE: {:?}",
      seen.lock()
    );
    // 位点指认：begin+RI-T+RI-D 恰 3 次推进（收口臂跑到位，非 begin 门假锁）
    assert_eq!(
      cp.current_epoch() - epoch_before,
      3,
      "判败位点应为 RI DELETING 收口"
    );
    assert_eq!(
      *session.status.read(),
      MigrateState::Fail,
      "会话终态须 Fail"
    );

    // 在册直证 + 封堵在位 + 源端零删（盲释还原即三面转红）
    assert!(
      mgr.migration_claimed(&id_key),
      "RI DELETING 收口不得未持有即释他人换入窗 claim"
    );
    assert_window_blocks_load(&store, k_tree).await;
    assert!(
      matches!(live_kind(&store, k_tree).await, LiveValue::TieredTree),
      "源端树键不得被盲释后排空销毁"
    );

    // 持有者自身收尾释放 → 收敛
    drop(guard_slot.lock().take());
    assert!(!mgr.migration_claimed(&id_key), "持有者释放后 claim 应摘除");
    assert_window_converged(&store, k_tree).await;
    assert!(
      matches!(live_kind(&store, k_tree).await, LiveValue::TieredTree),
      "收敛后源端树键完整在位"
    );
  });
}
