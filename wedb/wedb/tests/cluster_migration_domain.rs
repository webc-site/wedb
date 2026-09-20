//! 迁移域门禁集成测试（库级分片 doc/zh/db.md 4.1，task/ing/r3-cluster-migration-domain.md）
//!
//! 缺陷形态：SLOTS/KEYS 迁移驱动自建会话后从不 set_context，会话恒锚默认域 (0,0)，
//! 而 wnode get_keys_in_slot_with 的判据是「会话库槽不符即零扫描早退」——非默认域
//! 槽位取键恒空、循环体零执行，收尾却照常走完成哨兵 + 远端 NODE + 本端 relinquish，
//! 把持有非默认库数据的槽以「零键迁移」移交目标，源端键滞留为不可达孤儿。
//!
//! 本文件锁住的不变量：非默认域槽位绝不允许「已交权 + 键仍滞留源端」并存；止血期
//! 由解析期显式拒绝承接（MIGRATE 与 CLUSTER SETSLOT/SETSLOTSRANGE MIGRATING 两臂）。

use std::{collections::VecDeque, str::from_utf8, sync::Arc};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpListener,
  runtime::{Runtime, spawn},
};
use parking_lot::Mutex;
use wbase::{future::yield_now, hash_slot::slot_of};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_provider::ClusterProvider,
  cluster_session::ClusterSession,
  hash_slot::{HashSlot, SlotState},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  storage::session::storage_session::StorageSession,
};
use wtest_base::{resp_frame, test_store_config};
use wtxn::{TxnLockTable, WatchVersionMap};

/// 默认域 (0,0) 库槽位（库级定槽：迁移驱动会话的唯一可承接槽位）
const SLOT0: u16 = slot_of(0, 0);
/// 远端节点独占的异槽（本地槽位让位，用于「非本端槽」判定回归）
const REMOTE_SLOT: u16 = SLOT0 ^ 1;
/// 本地节点 id
const NODE1: u128 = 0x0000_0000_0000_0000_0000_0000_0000_DE11;
/// 迁移目标节点 id
const NODE2: u128 = 0x0000_0000_0000_0000_0000_0000_0000_DE12;

/// 找一个非默认域逻辑库：槽位既非默认域槽位亦非远端让位槽
fn non_default_db() -> u64 {
  (1..1024u64)
    .find(|&db| slot_of(0, db) != SLOT0 && slot_of(0, db) != REMOTE_SLOT)
    .expect("1024 库内必有命中非默认域槽位的 db")
}

/// 打开迁移测试专用存储（小预算、GC 关闭、reviv 关闭）
fn migrate_store(tag: &str) -> Arc<WedbStore<SegmentedDevice>> {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join(tag)).unwrap());
  Arc::new(WedbStore::open(test_store_config(), device).unwrap())
}

/// 装配双主拓扑：本端持全部槽位，仅 REMOTE_SLOT 让位 node_2
fn two_primary_provider() -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().unwrap();
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: NODE1,
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
    let remote_worker_id = config.workers.len() as u16;
    config.workers.push(Worker {
      nodeid: Some(NODE2),
      address: "127.0.0.1".into(),
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: None,
    });
    for slot in 0..config.slot_map.len() {
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

/// 挂共享存储的集群会话消费者
fn migrate_consumer(
  cp: &ClusterProvider,
) -> (RespSessionConsumer, Arc<WedbStore<SegmentedDevice>>) {
  let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
  let store = migrate_store("mig_domain.db");
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

/// 泵等价消费（直填会话接收缓冲 → 唯一入口 → 应答取出）
fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> (Option<usize>, Vec<u8>) {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  (remaining, resp)
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

/// 读指定逻辑库内 string（键权断言用；默认域读法会漏看非默认库数据）
async fn read_str_in_db(
  store: &Arc<WedbStore<SegmentedDevice>>,
  ns: u64,
  db: u64,
  key: &[u8],
) -> Option<Vec<u8>> {
  let session = store.new_session().unwrap();
  assert!(session.set_context(ns, db), "落库上下文应成功");
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

/// 假迁移目标端：按连接弹答 +OK（脚本耗尽即静默），每帧记
/// 「前三参 + 第 6 参字节数」供帧序与载荷非空断言
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
            let head = args
              .iter()
              .take(3)
              .map(|a| String::from_utf8_lossy(a).into_owned())
              .collect::<Vec<_>>()
              .join(" ");
            let payload_len = args.get(5).map_or(0, |p| p.len());
            seen.lock().push(format!("{head} payload={payload_len}"));
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

/// 把 node_2 端口改写为假目标端端口（命令解析按集群配置取 target_node_id）
fn retarget_remote_port(cp: &ClusterProvider, port: i32) {
  let m = cp.cluster_manager().unwrap();
  let mut config = m.current_config.write();
  if let Some(w) = config.workers.iter_mut().find(|w| w.nodeid == Some(NODE2)) {
    w.port = port;
  }
}

/// 端口取自 "127.0.0.1:port"
fn port_of(addr: &str) -> i32 {
  addr.rsplit(':').next().unwrap().parse().unwrap()
}

/// 轮询至谓词成立（有限轮次，超时即返 false）
async fn poll_until(mut pred: impl FnMut() -> bool) -> bool {
  for _ in 0..2000 {
    if pred() {
      return true;
    }
    yield_now().await;
  }
  false
}

/// 非默认域槽位迁移的静默交权复现（缺陷锁，修复前必红）：
/// 键落 db≠0（槽位 slot_of(0,db)≠SLOT0）→ 对该槽发起 MIGRATE SLOTS →
/// 断言不得出现「槽已移交远端 + 键仍滞留源端」的孤儿形态
#[test]
fn slots_migration_non_default_db_must_not_hand_off_silently() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let (mut consumer, store) = migrate_consumer(&cp);
    let db = non_default_db();
    let slot = slot_of(0, db);
    let key = format!("dom_a{db}");

    let db_str = db.to_string();
    assert_eq!(
      drive(
        &rt,
        &mut consumer,
        &resp_frame(&[b"SELECT", db_str.as_bytes()])
      ),
      b"+OK\r\n"
    );
    assert_eq!(
      drive(
        &rt,
        &mut consumer,
        &resp_frame(&[b"SET", key.as_bytes(), b"v1"]),
      ),
      b"+OK\r\n"
    );
    assert_eq!(
      read_str_in_db(&store, 0, db, key.as_bytes()).await,
      Some(b"v1".to_vec()),
      "前置：键应在非默认库内可读"
    );

    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr = scripted_migrate_target(vec![vec![b"+OK\r\n"; 12]], Arc::clone(&seen)).await;
    let port = port_of(&addr);
    retarget_remote_port(&cp, port);

    let port_str = port.to_string();
    let slot_str = slot.to_string();
    let out = drive(
      &rt,
      &mut consumer,
      &resp_frame(&[
        b"MIGRATE",
        b"127.0.0.1",
        port_str.as_bytes(),
        b"",
        b"0",
        b"0",
        b"SLOTS",
        slot_str.as_bytes(),
      ]),
    );
    let m = cp.cluster_manager().unwrap();
    let remote_wid = m.current_config.read().get_worker_id_from_node_id(NODE2);
    // 收口判据：任务在命令同步段注册、驱动 finally 移除（拒绝路径全程无任务），
    // 计数归零即迁移链走到底，避免与 detached 后台任务抢跑
    let settled = poll_until(|| {
      cp.migration_manager()
        .is_some_and(|mm| mm.get_migration_task_count() == 0)
    })
    .await;
    assert!(settled, "迁移驱动应在有限轮次内收口");
    let handed_off = {
      let cfg = m.current_config.read();
      cfg.get_state(slot) == SlotState::Stable
        && cfg.get_worker_id_from_slot(slot) == remote_wid as usize
    };
    let stranded = read_str_in_db(&store, 0, db, key.as_bytes())
      .await
      .is_some();
    // 非空批次帧数（哨兵帧 payload 恒为 4 字节记录计数）：0 即「零键迁移」
    let batches = seen
      .lock()
      .iter()
      .filter(|f| f.starts_with("CLUSTER MIGRATE") && !f.ends_with("payload=4"))
      .count();
    let text = String::from_utf8_lossy(&out).into_owned();
    // 判据前置：本次迁移要么被显式拒绝，要么真完成交权，否则本用例断言空转
    assert!(
      text.starts_with("-ERR") || handed_off,
      "本次迁移既未显式拒绝也未完成交权，判据失效：应答 {text:?}，帧 {:#?}",
      seen.lock()
    );
    assert!(
      !(handed_off && stranded),
      "非默认域槽位被静默空迁交权：应答 {text:?}，槽 {slot} 移交远端 = {handed_off}，\
       源端键 {key} 滞留 = {stranded}，非空批次帧 = {batches}\
       （既未随槽移交、也未显式拒绝 = 不可达孤儿）"
    );
  });
}

/// 止血门禁回归：以下用例锁「显式拒绝 + 零副作用」，修复前必红（当时回 +OK 并交权）
#[test]
fn migrate_slots_non_default_db_slot_rejected() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let (mut consumer, store) = migrate_consumer(&cp);
    let db = non_default_db();
    let slot = slot_of(0, db);
    let key = format!("dom_b{db}");

    let db_str = db.to_string();
    assert_eq!(
      drive(
        &rt,
        &mut consumer,
        &resp_frame(&[b"SELECT", db_str.as_bytes()])
      ),
      b"+OK\r\n"
    );
    assert_eq!(
      drive(
        &rt,
        &mut consumer,
        &resp_frame(&[b"SET", key.as_bytes(), b"v1"]),
      ),
      b"+OK\r\n"
    );

    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr = scripted_migrate_target(vec![vec![b"+OK\r\n"; 12]], Arc::clone(&seen)).await;
    let port = port_of(&addr);
    retarget_remote_port(&cp, port);

    let port_str = port.to_string();
    let slot_str = slot.to_string();
    let out = drive(
      &rt,
      &mut consumer,
      &resp_frame(&[
        b"MIGRATE",
        b"127.0.0.1",
        port_str.as_bytes(),
        b"",
        b"0",
        b"0",
        b"SLOTS",
        slot_str.as_bytes(),
      ]),
    );
    let text = String::from_utf8_lossy(&out).into_owned();
    assert!(
      text.starts_with("-ERR migration of slot") && text.contains(&slot_str),
      "非默认域槽位应回域拒绝文案，实得 {text:?}"
    );
    assert!(
      read_str_in_db(&store, 0, db, key.as_bytes())
        .await
        .is_some(),
      "拒绝路径源端键不得消失"
    );
    assert!(seen.lock().is_empty(), "拒绝路径远端不应被触达");
    assert_eq!(
      cp.migration_manager()
        .map(|mm| mm.get_migration_task_count())
        .unwrap_or(0),
      0,
      "拒绝路径不得注册迁移任务"
    );
  });
}

/// 止血门禁回归：SLOTSRANGE 形态同样整批拒绝
#[test]
fn migrate_slotsrange_non_default_db_slot_rejected() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let (mut consumer, _store) = migrate_consumer(&cp);
    let db = non_default_db();
    let slot = i64::from(slot_of(0, db));

    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr = scripted_migrate_target(vec![vec![b"+OK\r\n"; 12]], Arc::clone(&seen)).await;
    let port = port_of(&addr);
    retarget_remote_port(&cp, port);

    let port_str = port.to_string();
    let range = slot.to_string();
    let out = drive(
      &rt,
      &mut consumer,
      &resp_frame(&[
        b"MIGRATE",
        b"127.0.0.1",
        port_str.as_bytes(),
        b"",
        b"0",
        b"0",
        b"SLOTSRANGE",
        range.as_bytes(),
        range.as_bytes(),
      ]),
    );
    let text = String::from_utf8_lossy(&out).into_owned();
    assert!(
      text.starts_with("-ERR migration of slot"),
      "SLOTSRANGE 非默认域应回域拒绝文案，实得 {text:?}"
    );
    assert!(seen.lock().is_empty(), "拒绝路径远端不应被触达");
  });
}

/// 止血门禁回归：KEYS 形态（含 Redis 单键形态）由签发会话域判定，非默认域显式拒绝
#[test]
fn migrate_keys_from_non_default_db_rejected() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let (mut consumer, store) = migrate_consumer(&cp);
    let db = non_default_db();
    let slot_str = slot_of(0, db).to_string();
    let key = format!("dom_c{db}");

    let db_str = db.to_string();
    assert_eq!(
      drive(
        &rt,
        &mut consumer,
        &resp_frame(&[b"SELECT", db_str.as_bytes()])
      ),
      b"+OK\r\n"
    );
    assert_eq!(
      drive(
        &rt,
        &mut consumer,
        &resp_frame(&[b"SET", key.as_bytes(), b"v1"]),
      ),
      b"+OK\r\n"
    );

    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr = scripted_migrate_target(vec![vec![b"+OK\r\n"; 12]], Arc::clone(&seen)).await;
    let port = port_of(&addr);
    retarget_remote_port(&cp, port);
    let port_str = port.to_string();

    // Redis 单键形态（无 KEYS 选项，签发会话在非默认域）
    let out = drive(
      &rt,
      &mut consumer,
      &resp_frame(&[
        b"MIGRATE",
        b"127.0.0.1",
        port_str.as_bytes(),
        key.as_bytes(),
        b"0",
        b"5000",
      ]),
    );
    let text = String::from_utf8_lossy(&out).into_owned();
    assert!(
      text.starts_with("-ERR migration of slot") && text.contains(&slot_str),
      "非默认域会话签发单键 MIGRATE 应回域拒绝文案，实得 {text:?}"
    );
    assert!(seen.lock().is_empty(), "拒绝路径远端不应被触达");
    assert!(
      read_str_in_db(&store, 0, db, key.as_bytes())
        .await
        .is_some(),
      "拒绝路径源端键不得消失"
    );
  });
}

/// 止血门禁回归：CLUSTER SETSLOT / SETSLOTSRANGE 的 MIGRATING 臂拒绝非默认域槽位，
/// 其余态（IMPORTING / NODE / STABLE）不受门禁影响
#[test]
fn setslot_migrating_non_default_db_slot_rejected() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let (mut consumer, _store) = migrate_consumer(&cp);
    let m = cp.cluster_manager().unwrap();
    let db = non_default_db();
    let slot = slot_of(0, db);
    let slot_str = slot.to_string();
    let node2_hex = format!("{NODE2:032x}");

    let out = drive(
      &rt,
      &mut consumer,
      &resp_frame(&[
        b"CLUSTER",
        b"SETSLOT",
        slot_str.as_bytes(),
        b"MIGRATING",
        node2_hex.as_bytes(),
      ]),
    );
    let text = String::from_utf8_lossy(&out).into_owned();
    assert!(
      text.starts_with("-ERR migration of slot"),
      "非默认域槽位置 MIGRATING 应回域拒绝文案，实得 {text:?}"
    );
    assert_eq!(
      m.current_config.read().get_state(slot),
      SlotState::Stable,
      "拒绝路径槽状态不得翻转"
    );

    let range = slot_str.as_bytes();
    let out = drive(
      &rt,
      &mut consumer,
      &resp_frame(&[
        b"CLUSTER",
        b"SETSLOTSRANGE",
        b"MIGRATING",
        node2_hex.as_bytes(),
        range,
        range,
      ]),
    );
    let text = String::from_utf8_lossy(&out).into_owned();
    assert!(
      text.starts_with("-ERR migration of slot"),
      "SETSLOTSRANGE MIGRATING 非默认域应回域拒绝文案，实得 {text:?}"
    );

    // IMPORTING / NODE / STABLE 三臂不在门禁内（驱动内部编排与运维收口均走它们）：
    // 非默认域本地槽置 IMPORTING 由 manager 自己的「槽不空闲」前置拒绝，
    // 回的不是域文案即证明门禁未越界拦截该臂
    let out = drive(
      &rt,
      &mut consumer,
      &resp_frame(&[
        b"CLUSTER",
        b"SETSLOT",
        slot_str.as_bytes(),
        b"IMPORTING",
        node2_hex.as_bytes(),
      ]),
    );
    let text = String::from_utf8_lossy(&out).into_owned();
    assert!(
      !text.contains("migration of slot"),
      "IMPORTING 臂不应被域门禁拦截，实得 {text:?}"
    );
    // 属主在远端的槽位置 IMPORTING 放行（+OK），门禁只作用于 MIGRATING
    let remote_str = REMOTE_SLOT.to_string();
    let out = drive(
      &rt,
      &mut consumer,
      &resp_frame(&[
        b"CLUSTER",
        b"SETSLOT",
        remote_str.as_bytes(),
        b"IMPORTING",
        node2_hex.as_bytes(),
      ]),
    );
    assert_eq!(out, b"+OK\r\n", "IMPORTING 臂不应被门禁拒绝");
    assert_eq!(
      m.current_config.read().get_state(REMOTE_SLOT),
      SlotState::Importing
    );
    // STABLE 复位亦放行
    let out = drive(
      &rt,
      &mut consumer,
      &resp_frame(&[b"CLUSTER", b"SETSLOT", slot_str.as_bytes(), b"STABLE"]),
    );
    assert_eq!(out, b"+OK\r\n");
  });
}

/// 越界判定先行回归：负槽位仍回越界文案（门禁不得抢在参数校验之前）
#[test]
fn setslot_migrating_out_of_range_precedes_domain_gate() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let (mut consumer, _store) = migrate_consumer(&cp);
    let node2_hex = format!("{NODE2:032x}");
    let out = drive(
      &rt,
      &mut consumer,
      &resp_frame(&[
        b"CLUSTER",
        b"SETSLOT",
        b"16384",
        b"MIGRATING",
        node2_hex.as_bytes(),
      ]),
    );
    assert_eq!(out, b"-ERR Slot out of range\r\n");
  });
}

/// 默认域通路不得被门禁误伤：MIGRATE SLOTS SLOT0 后台驱动照常完成搬键与交权
#[test]
fn slots_migration_default_db_still_migrates() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let (mut consumer, store) = migrate_consumer(&cp);
    let key = "dom_ok";
    assert_eq!(
      drive(
        &rt,
        &mut consumer,
        &resp_frame(&[b"SET", key.as_bytes(), b"v1"]),
      ),
      b"+OK\r\n"
    );

    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let addr = scripted_migrate_target(vec![vec![b"+OK\r\n"; 12]], Arc::clone(&seen)).await;
    let port = port_of(&addr);
    retarget_remote_port(&cp, port);

    let port_str = port.to_string();
    let slot_str = SLOT0.to_string();
    let out = drive(
      &rt,
      &mut consumer,
      &resp_frame(&[
        b"MIGRATE",
        b"127.0.0.1",
        port_str.as_bytes(),
        b"",
        b"0",
        b"0",
        b"SLOTS",
        slot_str.as_bytes(),
      ]),
    );
    assert_eq!(out, b"+OK\r\n", "默认域 SLOTS 迁移应立即 +OK");

    let m = cp.cluster_manager().unwrap();
    let remote_wid = m.current_config.read().get_worker_id_from_node_id(NODE2);
    let done = poll_until(|| {
      let cfg = m.current_config.read();
      drop(cfg);
      cp.migration_manager()
        .is_some_and(|mm| mm.get_migration_task_count() == 0)
        && m.current_config.read().get_state(SLOT0) == SlotState::Stable
        && m.current_config.read().get_worker_id_from_slot(SLOT0) == remote_wid as usize
    })
    .await;
    assert!(done, "默认域后台驱动应完成交权");
    assert_eq!(
      read_str_in_db(&store, 0, 0, key.as_bytes()).await,
      None,
      "默认域键应随迁移从源端删除"
    );
    let frames = seen.lock();
    assert!(
      frames
        .iter()
        .any(|f| f.starts_with("CLUSTER MIGRATE") && !f.ends_with("payload=4")),
      "默认域链路应发出非空批次帧: {frames:?}"
    );
  });
}
