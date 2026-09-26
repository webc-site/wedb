//! PERSIST 无 TTL 命中臂（:0 臂）观测面防回摆锁（deviations §123 锁面，
//! 票 task/ing/zcode-r133c-ttlwrite.md）
//!
//! 立案面：C# 统一存 PERSIST 命中存活键即无条件推进观察者版本并落 AOF
//! 条目——:0 臂与 :1 臂同构收口（UnifiedStore/RMWMethods.cs InPlaceUpdater
//! :157-174 / PostCopyUpdater :119/:151，WriteLogRMW PrivateMethods.cs:
//! 104-118）；rust 该臂采真 Redis 形零推进零镜像（快臂 keys.rs
//! persist_apply_sync NotFound/Success(None) 直返 Some(0)、慢臂 wkv
//! persist None 臂不回推进、journal 旁路写监听单源无写即无事件无条目
//! wkv/src/session/raw/mod.rs:281-339、会话包装 persist_key applied>0
//! 门）。裁决=登记偏差不补齐：**严禁后审按 C# 向 rust 补齐 :0 臂 no-op
//! bump 或手动注条目**（补齐即旁路写驱动 journal 单机制另起第二落笔口）。
//!
//! 本组三把锁（全部真实存储会话/真实 WAL 流水/真实事务状态机，零假 mock）：
//! 1. watch_nottl_key_persist_zero_exec_commits——钉死 rust 现状形：
//!    WATCH 存活无 TTL 键 → 他连接 PERSIST 得 :0 → EXEC 提交成功
//!    （C# 形观察者版本已推进必 abort，本锁防按 C# 回摆）；
//! 2. watch_persist_real_delete_one_exec_aborts——同址对照：:1 真实删除
//!    臂 bump 在位、WATCH 栅栏必中止（证明锁 1 之绿非接线假绿）；
//! 3. aof_persist_zero_arm_produces_no_entry——AOF 开启下 :0 PERSIST
//!    零条目差分计数锁：同一流水先重放计 N，后行 PERSIST :1 + PERSIST
//!    :0 两臂再重放计 N+1（:1 恰一条目、:0 恰零条目）；从端重放终态
//!    键值与 TTL 缺席全等断言随批（无蒸发无发散）。

use std::sync::Arc;

use compio::runtime::Runtime;
use waof::{WalConfig, WalLog};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  service::NodeService,
  storage::session::{common::ttl_sync::ttl_of_sync, storage_session::version_map_watch_hook},
};
use wnode_test::{auto_exec, drain_output};
use wresp::command::RespCommand;
use wtest_base::open_test_store;
use wtxn::{TxnLockTable, WatchVersionMap};

/// 喂一整批帧并冲出应答（消费循环 + 停车臂同步闭环的泵替身；
/// select_switch_db_invalidates_watch 同款）
fn feed(s: &mut RespServerSession, frame: &[u8]) -> Vec<u8> {
  s.recv_buffer.extend_from_slice(frame);
  let mut resp_buf = Vec::new();
  let consumed = s.try_consume_messages();
  assert!(consumed.is_some(), "帧应被完整消费: {frame:?}");
  s.take_output_into(&mut resp_buf);
  Runtime::new()
    .unwrap()
    .block_on(wnode_test::drive_pending_parks(s, &mut resp_buf, true));
  s.output.extend_from_slice(&resp_buf);
  drain_output(s)
}

/// WATCH 方（挂事务组件、与会话共用同一版本表）与操作方（异 RESP 会话，
/// 经引擎级 watch 钩子真实推进版本槽）双连接共享存储对
fn session_pair() -> (RespServerSession, RespServerSession, tempfile::TempDir) {
  let (dir, store) = open_test_store("wnode-persist-zeroarm-watch.db").unwrap();
  let map = Arc::new(WatchVersionMap::new(64));
  store.set_watch_hook(version_map_watch_hook(Arc::clone(&map)));

  let mut watcher = RespServerSession::new(1, RespServerSessionOptions::default());
  watcher.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));
  watcher.attach_transaction_components(map, TxnLockTable::new());

  let mut other = RespServerSession::new(2, RespServerSessionOptions::default());
  other.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));

  (watcher, other, dir)
}

/// 锁 1（现状形钉死，deviations §123 主判据）：WATCH 存活无 TTL 键 →
/// 他连接 PERSIST 得 :0（两侧应答值面同形）→ EXEC **提交成功**。
/// C# 该臂无条件 IncrementVersion 必令本事务 abort（*-1）；rust 采真
/// Redis 形零推进，本锁防后审按 C# 补齐 no-op bump 的回摆
#[test]
fn watch_nottl_key_persist_zero_exec_commits() {
  let (mut w, mut o, _dir) = session_pair();

  // 键存活且无 TTL（SET 先行，其版本推进落在 WATCH 之前）
  assert_eq!(
    feed(&mut o, b"*3\r\n$3\r\nSET\r\n$4\r\nz:k1\r\n$2\r\nv1\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    feed(&mut w, b"*2\r\n$5\r\nWATCH\r\n$4\r\nz:k1\r\n"),
    b"+OK\r\n",
    "WATCH 登记存活无 TTL 键位点"
  );

  // 他连接 PERSIST：应答值面两侧同为 :0（genexpire1 已判净面，零改动）
  assert_eq!(
    feed(&mut o, b"*2\r\n$7\r\nPERSIST\r\n$4\r\nz:k1\r\n"),
    b":0\r\n",
    "无 TTL 命中臂答 :0"
  );

  // 核心判据：rust :0 臂零推进 → EXEC 提交成功（C# 形此处必 *-1）
  assert_eq!(
    feed(
      &mut w,
      b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$4\r\nz:k1\r\n$2\r\nv2\r\n",
    ),
    b"+OK\r\n+QUEUED\r\n"
  );
  assert_eq!(
    feed(&mut w, b"*1\r\n$4\r\nEXEC\r\n"),
    b"*1\r\n+OK\r\n",
    "PERSIST :0 不推进观察者版本：EXEC 必须提交成功（钉死 rust 现状形/\
     真 Redis 形，按 C# UnifiedStore/RMWMethods.cs:157-174 补齐推进即本锁红）"
  );
  assert_eq!(
    feed(&mut w, b"*2\r\n$3\r\nGET\r\n$4\r\nz:k1\r\n"),
    b"$2\r\nv2\r\n",
    "事务真实落库"
  );
}

/// 锁 1 同址对照（接线活性证明）：:1 真实删除臂 bump 在位——WATCH 带
/// TTL 键 → 他连接 PERSIST 得 :1 → EXEC 必中止 *-1（两侧同向面，
/// del_ttl_sync 真实删除才推进单点）。若本锁与锁 1 同绿为假象（版本表
/// 接线断开则双双 commit），本锁必红——证明锁 1 的 commit 出自 :0 臂
/// 零推进而非栅栏失效
#[test]
fn watch_persist_real_delete_one_exec_aborts() {
  let (mut w, mut o, _dir) = session_pair();

  assert_eq!(
    feed(&mut o, b"*3\r\n$3\r\nSET\r\n$4\r\nz:k2\r\n$2\r\nv1\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    feed(&mut o, b"*3\r\n$6\r\nEXPIRE\r\n$4\r\nz:k2\r\n$3\r\n100\r\n"),
    b":1\r\n",
    "置 TTL 成功（EXPIRE 臂自身推进落在 WATCH 之前）"
  );
  assert_eq!(
    feed(&mut w, b"*2\r\n$5\r\nWATCH\r\n$4\r\nz:k2\r\n"),
    b"+OK\r\n"
  );

  // 真实删除臂：答 :1 且经 del_ttl_sync deleted==true 推进版本（同向面）
  assert_eq!(
    feed(&mut o, b"*2\r\n$7\r\nPERSIST\r\n$4\r\nz:k2\r\n"),
    b":1\r\n"
  );
  assert_eq!(
    feed(
      &mut w,
      b"*1\r\n$5\r\nMULTI\r\n*2\r\n$3\r\nGET\r\n$4\r\nz:k2\r\n",
    ),
    b"+OK\r\n+QUEUED\r\n"
  );
  assert_eq!(
    feed(&mut w, b"*1\r\n$4\r\nEXEC\r\n"),
    b"*-1\r\n",
    ":1 真实删除推进版本，WATCH 栅栏必中止（锁 1 接线活性对照）"
  );
}

// ---------------------------------------------------------------------------
// 锁 3：AOF 开启下 :0 PERSIST 零条目差分计数锁（主副双 NodeService WAL
// 夹具沿 ttl_ticks_aof_mirror 形制）
// ---------------------------------------------------------------------------

/// 主/副各一套：存储 + WAL（重放经 NodeService 统一闭环）
struct Node {
  store: Arc<wkv::WedbStore<SegmentedDevice>>,
  service: NodeService<SegmentedDevice>,
  wal: Arc<WalLog<SegmentedDevice>>,
  _dir: tempfile::TempDir,
}

fn open_node(tag: &str) -> aok::Result<Node> {
  let dir = tempfile::tempdir()?;
  let store_device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{tag}.db")),
  )?);
  let wal_device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{tag}.wal")),
  )?);
  let mut config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5)?;
  config.range_index_dir = Some(dir.path().to_path_buf());
  let store = Arc::new(WedbStore::open(config, store_device)?);
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::default())?);
  let service = NodeService::with_wal(Arc::clone(&store), Arc::clone(&wal))?;
  Ok(Node {
    store,
    service,
    wal,
    _dir: dir,
  })
}

fn api_of(store: &Arc<wkv::WedbStore<SegmentedDevice>>) -> aok::Result<GarnetApi> {
  Ok(Arc::new(StoreGarnetApi::new(store.new_session()?)))
}

fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 落库 TTL 原始 ticks 直读（None = 无 TTL）
fn raw_ttl(store: &Arc<wkv::WedbStore<SegmentedDevice>>, key: &[u8]) -> aok::Result<Option<i64>> {
  let session = store.new_session()?;
  let batch = session.enter_batch();
  Ok(ttl_of_sync(&batch, key)?.value().flatten())
}

/// 锁 3（AOF 条目面钉死 rust 现状形）：同一主库流水两段提交、各重放至
/// 全新从端计数——基线段（SET/EXPIRE/SET）重放计 N；随后 PERSIST :1（真实
/// 删除臂，产恰一条 Persist 镜像）+ PERSIST :0（无 TTL 命中臂）两段间
/// 零其它写，再重放计 M，断言 M == N + 1：:0 臂零条目（按 C# 补齐 no-op
/// 落条即本锁红）；从端终态两键值在、TTL 俱缺（终态全等，无蒸发无发散）
#[test]
fn aof_persist_zero_arm_produces_no_entry() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let primary = open_node("zero-aof-primary")?;
    let api = api_of(&primary.store)?;
    let mut s = session_with(&api);

    // 基线流水：k1 走完整 EXPIRE→PERSIST :1 生命周期，k2 置备为「存活
    // 无 TTL」待 :0 命中臂
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Set, &[b"za:k1", b"v1"]),
      b"+OK\r\n"
    );
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Expire, &[b"za:k1", b"100"]),
      b":1\r\n"
    );
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Set, &[b"za:k2", b"v2"]),
      b"+OK\r\n"
    );
    primary.wal.commit().await?;

    // 基线重放计数 N（从端 A 全新实例全量消费）
    let replica_a = open_node("zero-aof-replica-a")?;
    let base = primary
      .service
      .replay_into_session(replica_a.service.session())
      .await?;
    assert!(base > 0, "基线流水应产出条目");
    assert!(
      raw_ttl(&primary.store, b"za:k1")?.is_some(),
      "前置：k1 基线段末 TTL 在场"
    );

    // 差分两臂：PERSIST k1 → :1（真实删除，产一条目）；
    // PERSIST k2 → :0（无 TTL 命中臂，rust 零写零事件零条目）
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Persist, &[b"za:k1"]),
      b":1\r\n"
    );
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Persist, &[b"za:k2"]),
      b":0\r\n"
    );
    primary.wal.commit().await?;

    // 差分重放计数 M（从端 B 全新实例全量消费）：M 恰比 N 多 :1 臂一
    // 条 Persist 镜像——:0 臂贡献恰为零条目（若按 C# UnifiedStore/
    // RMWMethods.cs:199-201/250-259 无条件 NeedAofLog 形补齐落条，M 将
    // == N + 2，本锁必红）
    let replica_b = open_node("zero-aof-replica-b")?;
    let after = primary
      .service
      .replay_into_session(replica_b.service.session())
      .await?;
    assert_eq!(
      after,
      base + 1,
      ":1 臂产恰一条目、:0 臂零条目（差值 {}/{})",
      after,
      base
    );

    // 从端终态全等：两键值俱在、TTL 俱缺（条目序列分叉限观测面，无
    // 蒸发无发散——deviations §123 危害定性断言）
    let rb = replica_b.store.new_session()?;
    assert_eq!(rb.read(b"za:k1").await?, Some(b"v1".to_vec()));
    assert_eq!(rb.read(b"za:k2").await?, Some(b"v2".to_vec()));
    assert_eq!(raw_ttl(&replica_b.store, b"za:k1")?, None);
    assert_eq!(raw_ttl(&replica_b.store, b"za:k2")?, None);
    Ok(())
  })
}
