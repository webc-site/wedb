//! AOF 回放面**虚拟域**端到端集成测试（doc/zh/db.md「主从物理镜像与异步屏障」）
//!
//! 四条链（票面验收 2、3 的端到端面）：
//! 1. `tail_flushdb_replay_swaps_inherited_domain`——「checkpoint 基线 + AOF
//!    尾部增量」形态下非零 vns/vdb 的换号条目回放：从库经镜像继承主库映射，
//!    FlushDb(vns, 换号前旧 vdb) 条目在本节点**在册**路由格上换号并与主库锁步
//!    同号——旧域键不可读、清后新号写入可见、他租户完好；
//! 2. `full_replay_nonzero_domain_lands_in_entry_domain`——非零 vns/vdb 形态的
//!    全量回放：每条条目严格落回自身物理前缀（物理镜像承诺），映射体系与旧域
//!    判死一律由主库 `KeyTag::DbMeta` 镜像条目承接（全新副本回放后租户表 / 库路
//!    由表 / 分配水位逐值等于主库末态，本地取号器零触发），FlushDb 条目本身只
//!    作屏障；
//! 3. `tail_flushns_replay_retires_inherited_namespace`——FlushNs(旧 vns) 条目
//!    经 `active_vns` 逆表反查逻辑命名空间后，按主库同一事务体整空间换号；
//! 4. `replay_face_slot_matches_online_face_slot`——回放面向量登记槽位与在线面
//!    `slot_of(逻辑域)` 逐值同值（库级定槽确定性，doc/zh/db.md 4.1）。
//!
//! 对标 C#：`garnet/libs/server/AOF/AofProcessor.cs:147/:164
//! SwitchActiveDatabaseContext`（切至**既有**库实例，从不重解析域号）与
//! `:483-545 ReplayOp` keyed 分支。C# 每库独立 store、条目键无域前缀，本无域
//! 换算面；rust 单日志以物理前缀承载域，故回放端只能直设虚拟域。
//!
//! 主库段一律走真实生产面（`NodeService` 事件写端口 + `SingleDatabaseManager`
//! 清库漏斗），条目键前缀与 FLUSH 载荷取自主库**真实映射**，测试不手写域值。
//! 逻辑域常量刻意取千位量级，使虚拟号（个位起顺序分配）与逻辑号值域严格不相交
//! ——「回放侧把条目物理号当逻辑号物化」这一失败模式在断言里无可遮蔽的数值重合。

use std::{
  collections::BTreeSet,
  fs::create_dir_all,
  path::{Path, PathBuf},
  sync::{Arc, atomic::Ordering},
};

use aok::{Error, OK, Void};
use compio::runtime::Runtime;
use tempfile::TempDir;
use waof::{AofEntryType, AofHeader, WalConfig, WalLog};
use wbase::{align::DEFAULT_SECTOR_SIZE, cfg::MAX_DATABASES_MAX, hash_slot::slot_of};
use wconf::RuntimeServerOptions;
use wcpr::{CheckpointType, find_latest_checkpoint, next_token_above};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  GarnetAppendOnlyFile,
  aof::{
    aof_processor::{AofProcessor, ReplayTarget, parse_flush_domain},
    recover::aof_recover::AofRecover,
    waof_sublog::single_log_aof,
  },
  database::{GarnetDatabase, SingleDatabaseManager, checkpoint_version},
  resp::vector::{
    resp_server_session_vectors::{RespServerSessionVectors, VectorReply},
    vector_manager::{VectorManager, VectorManagerOptions},
    vector_manager_replication::VectorAofSink,
    vector_store_callbacks::{
      ActiveDedicatedVectorSession, OwnedActiveVectorSession, WedbVectorStoreCallbacks,
    },
  },
  service::NodeService,
  storage::session::storage_session::StorageSession,
};
use wtest_base::{open_test_store, test_store_config};
use wval::SessionPrefixBuf;
use wvector::Callbacks;

/// 被测租户 A 的逻辑域（非零 vns、非零 vdb 形态）
const NS_A: u64 = 1003;
const DB_A: u64 = 1007;
/// 重启 / 检查点基线回放面的逻辑库号：**必须**落在协议层逻辑库地址空间
/// `0..MAX_DATABASES_MAX` 内——磁盘 DbMeta 权威的库级路由快照回建按该协议绝对
/// 上界逐格点查（`wkv/src/store/vdb_load.rs:load_routes_of_vns` 的完备性论证：
/// 0x02 记录的 logic_db 只能由 `try_switch_active_database_session` 的
/// `db_id >= max_databases` 门禁放行后产生）。协议外库号本节点永不在册，
/// 反查为 None 是正解而非缺陷。租户号仍取千位量级，与个位起顺序分配的虚拟号
/// 值域严格不相交，判别性不受影响
const DB_A_RESTART: u64 = 100;
/// 他租户 B 的逻辑域（跨租户不受波及判据）
const NS_B: u64 = 2005;
const DB_B: u64 = 2001;

/// 真装配节点：store + 独立段式 AOF 设备 + 检查点目录 + 接线完成的服务面
///
/// 调用方以解构取用各字段（`let Node { store, aof, .. } = open_node(..)`），
/// 恢复面须先释放主库侧全部句柄再在同一设备上开恢复 store。
struct Node {
  _dir: TempDir,
  device: Arc<SegmentedDevice>,
  store: Arc<WedbStore<SegmentedDevice>>,
  aof: Arc<GarnetAppendOnlyFile>,
  cp_dir: PathBuf,
  service: NodeService<SegmentedDevice>,
}

/// 打开真装配节点（`NodeService::new` 注册全部 AOF 写监听端口，与生产同形：
/// 会话写入即经事件端口产出带真实物理前缀的条目）
fn open_node(tag: &str) -> aok::Result<Node> {
  let dir = TempDir::new()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{tag}.db")),
  )?);
  let wal_device = Arc::new(SegmentedDevice::new(
    dir.path().join(format!("{tag}.wal")),
    64 * 1024,
    DEFAULT_SECTOR_SIZE,
  )?);
  let cp_dir = dir.path().join("ckpt");
  create_dir_all(&cp_dir)?;
  let store = Arc::new(WedbStore::open(test_store_config(), Arc::clone(&device))?);
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::new(1 << 20))?);
  let aof = single_log_aof(Arc::clone(&wal), &RuntimeServerOptions::default())
    .expect("装配 single_log_aof");
  let service = NodeService::new(Arc::clone(&store), Arc::clone(&aof))?;
  Ok(Node {
    _dir: dir,
    device,
    store,
    aof,
    cp_dir,
    service,
  })
}

/// 清库唯一漏斗（真实 FLUSHDB / FLUSHNS 生产者：换号事务体 + 广播条目入队）
fn flush_funnel(
  store: &Arc<WedbStore<SegmentedDevice>>,
  device: &Arc<SegmentedDevice>,
  aof: &Arc<GarnetAppendOnlyFile>,
  cp_dir: &Path,
) -> Arc<SingleDatabaseManager<SegmentedDevice>> {
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(store),
    Arc::clone(device),
    cp_dir.to_path_buf(),
    Some(Arc::clone(aof)),
  ));
  Arc::new(SingleDatabaseManager::new(cp_dir.to_path_buf(), db))
}

/// 日志内指定 FLUSH 族条目载荷按地址序取出（条目侧真值复核）
fn flush_payloads(aof: &GarnetAppendOnlyFile, op: AofEntryType) -> Vec<(u64, u64)> {
  aof.log().commit();
  let mut out = Vec::new();
  aof.log().scan_single_with(0, 0, i64::MAX, |rec| {
    if waof::is_commit_frame(&rec.payload) {
      return true;
    }
    if AofHeader::parse(&rec.payload).is_some_and(|h| h.op_type == op as u8) {
      out.push(parse_flush_domain(&rec.payload).expect("FLUSH 条目域载荷可读"));
    }
    true
  });
  out
}

/// 回放整段日志到目标 store（版本基线逐记录动态读取 `rstore.current_version()`：
/// 0 = 全量重放不做代际跳过，取检查点版本号 = 「基线 + AOF 尾部增量」形态，早于
/// 基线的旧代条目一律经 `record_gate::should_skip_record` 跳过；重放中检查点推进
/// 版本即刻生效；目标 store 的 AOF 写端口暂停——重放写入不得镜像回写，与生产恢复
/// 会话同闸）
async fn replay_to(
  rstore: &Arc<WedbStore<SegmentedDevice>>,
  aof: &Arc<GarnetAppendOnlyFile>,
) -> aok::Result<u64> {
  let _pause = rstore.pause_aof_listeners();
  let session = rstore.new_session()?;
  let batch = session.enter_batch();
  let storage = StorageSession::new(batch);
  let target = ReplayTarget {
    session: &storage,
    store: Arc::clone(rstore),
    aof_floor: rstore
      .recovered_aof_floor()
      .iter()
      .map(|&a| a as i64)
      .collect(),
  };
  let processor = AofProcessor::new(Arc::clone(aof));
  let replayed = AofRecover::single_log_recover(&processor, aof, 0, 0, -1, &target)
    .await
    .map_err(|e| Error::msg(e.to_string()))?;
  Ok(replayed)
}

/// 内存映射面的逻辑租户集合（判据：回放侧绝不新增逻辑租户）
fn logic_ns_set(store: &Arc<WedbStore<SegmentedDevice>>) -> Vec<u64> {
  let mut keys: Vec<u64> = store.vdb.ns_map.pin().iter().map(|(k, _)| *k).collect();
  keys.sort_unstable();
  keys
}

/// 内存映射面的租户路由表键集合（判据：回放侧绝不物化幽灵租户表）
fn routing_vns(store: &Arc<WedbStore<SegmentedDevice>>) -> Vec<u64> {
  let mut keys: Vec<u64> = store.vdb.db_routing.pin().iter().map(|(k, _)| *k).collect();
  keys.sort_unstable();
  keys
}

/// 当前虚拟号分配水位（判据：回放侧除锁步换号外零分配）
fn water_mark(store: &Arc<WedbStore<SegmentedDevice>>) -> u64 {
  store.vdb.next_virtual_id.load(Ordering::Relaxed)
}

/// checkpoint 基线 + AOF 尾部增量：非零域 FlushDb 条目在从库在册路由上锁步换号
///
/// 形态即生产从库：先镜像主库 checkpoint（含 `KeyTag::DbMeta` 映射记录），再
/// 回放其后的 AOF 尾部。尾部 FlushDb 载荷是主库换号事务返回的 `(vns, 换号前旧
/// vdb)`，与数据条目物理键前缀同域；从库据此在继承来的路由格上换号，取号与主
/// 库同一步（水位锁步），故条目后继写入的新物理前缀在从库解析到同一格。
#[compio::test]
async fn tail_flushdb_replay_swaps_inherited_domain() -> Void {
  let Node {
    _dir,
    device,
    store,
    aof,
    cp_dir,
    service,
  } = open_node("tail_flushdb")?;
  let mgr = flush_funnel(&store, &device, &aof, &cp_dir);

  // ── 基线段（检查点前）：真映射装载 + 真写入（条目经事件端口真产）──
  let sa = store.new_session()?;
  assert!(sa.set_context(NS_A, DB_A), "主库逻辑域可物化");
  let (vns, old_vdb) = store.vdb.get_virtual_ids(NS_A, DB_A);
  assert!(
    vns > 0 && old_vdb > 0,
    "非零租户须分配到非零虚拟号（本测试形态前提），实际 ({vns}, {old_vdb})"
  );
  sa.upsert(b"k1", b"v1").await?;
  sa.upsert(b"k2", b"v2").await?;
  let sb = store.new_session()?;
  assert!(sb.set_context(NS_B, DB_B));
  sb.upsert(b"keep", b"vk").await?;
  let (vns_b, vdb_b) = store.vdb.get_virtual_ids(NS_B, DB_B);

  // 基线镜像：检查点覆盖上述数据与 DbMeta 映射记录（从库继承映射体系的载体）
  let token = store
    .create_checkpoint(&cp_dir, CheckpointType::Snapshot)
    .await?
    .token;

  // ── 尾部增量段：FLUSHDB 真漏斗 + 清后新号写入 ──
  mgr.flush_database(NS_A, DB_A, false).await?;
  let (vns_after, new_vdb) = store.vdb.get_virtual_ids(NS_A, DB_A);
  assert_eq!(vns_after, vns, "FLUSHDB 只换库号，不换命名空间号");
  assert_ne!(new_vdb, old_vdb, "FLUSHDB 须换出新虚拟库号");
  sa.upsert(b"fresh", b"vf").await?;

  // 条目侧真值：FlushDb 载荷 = 主库换号事务返回的 (vns, 换号前旧 vdb)
  assert_eq!(
    flush_payloads(&aof, AofEntryType::FlushDb),
    vec![(vns, old_vdb)],
    "FLUSHDB 广播条目载荷须为换号前物理域"
  );

  // ── 从库段：镜像基线 + 尾部增量回放（主库侧句柄全释放后同设备开恢复）──
  drop(mgr);
  drop(sa);
  drop(sb);
  drop(service);
  drop(store);
  let rstore = Arc::new(WedbStore::recover(&cp_dir, token, Arc::clone(&device)).await?);
  // 继承复核：库级路由按首访点查磁盘既有映射回建（绝不另起新号）
  assert_eq!(
    rstore.resolve_context(NS_A, DB_A).await?,
    (vns, old_vdb),
    "从库须继承主库映射：点查命中磁盘既有 (vns, vdb)"
  );
  assert_eq!(
    rstore.resolve_context(NS_B, DB_B).await?,
    (vns_b, vdb_b),
    "他租户映射同样继承"
  );
  let ns_before = logic_ns_set(&rstore);
  let routing_before = routing_vns(&rstore);
  let water_before = water_mark(&rstore);

  replay_to(&rstore, &aof).await?;

  // 换号落回继承域：在册路由格指向主库同号（水位锁步）
  assert_eq!(
    rstore.vdb.route_vdb_of(vns, DB_A),
    Some(new_vdb),
    "从库换号须与主库锁步同号，条目后继写入方解析到同一格"
  );
  // 旧域键不可读、清后新号写入可见、他租户完好
  let probe = rstore.new_session()?;
  assert!(probe.set_context(NS_A, DB_A));
  assert_eq!(
    probe.read(b"k1").await?,
    None,
    "换号条目回放后旧域键须不可读"
  );
  assert_eq!(
    probe.read(b"k2").await?,
    None,
    "换号条目回放后旧域键须不可读"
  );
  assert_eq!(
    probe.read(b"fresh").await?,
    Some(b"vf".to_vec()),
    "清后新号域写入须在从库可见（尾部增量回放）"
  );
  assert!(probe.set_context(NS_B, DB_B));
  assert_eq!(
    probe.read(b"keep").await?,
    Some(b"vk".to_vec()),
    "他租户域不受换号条目波及"
  );
  // 映射面零新增：无幽灵租户 / 无幽灵路由表 / 水位只前进换号那一步
  assert_eq!(
    logic_ns_set(&rstore),
    ns_before,
    "回放不得新增逻辑命名空间（条目物理号不得物化为逻辑号）"
  );
  assert_eq!(
    routing_vns(&rstore),
    routing_before,
    "回放不得物化出幽灵租户路由表"
  );
  assert_eq!(
    water_mark(&rstore),
    water_before + 1,
    "换号条目取号与主库同一步，此外零分配"
  );
  OK
}

/// 非零 vns / 非零 vdb 形态的全量回放：条目逐部落回自身物理前缀
///
/// 全新节点全量回放形态（doc/zh/db.md「主从物理镜像与异步屏障」）：映射体系与
/// 旧域判死一律由主库 `KeyTag::DbMeta` 镜像条目承接——从库完全继承主库映射、
/// 不进行本地二次映射，故回放后租户表 / 库路由表 / 分配水位逐值等于主库末态
/// （即本地取号器零触发）。数据条目侧仍是物理镜像承诺：每条落回自身物理前缀；
/// FlushDb 条目本身只作屏障，不额外搬运映射。
#[compio::test]
async fn full_replay_nonzero_domain_lands_in_entry_domain() -> Void {
  let Node {
    _dir: _pdir,
    device: _device,
    store,
    aof,
    cp_dir: _cp_dir,
    service,
  } = open_node("full_replay")?;
  let mgr = flush_funnel(&store, &_device, &aof, &_cp_dir);

  let sa = store.new_session()?;
  assert!(sa.set_context(NS_A, DB_A));
  let (vns, old_vdb) = store.vdb.get_virtual_ids(NS_A, DB_A);
  sa.upsert(b"k1", b"v1").await?;
  let sb = store.new_session()?;
  assert!(sb.set_context(NS_B, DB_B));
  sb.upsert(b"keep", b"vk").await?;
  let (vns_b, vdb_b) = store.vdb.get_virtual_ids(NS_B, DB_B);
  mgr.flush_database(NS_A, DB_A, false).await?;
  let new_vdb = store.vdb.get_virtual_ids(NS_A, DB_A).1;
  sa.upsert(b"fresh", b"vf").await?;
  let primary_water = water_mark(&store);
  drop(mgr);
  drop(sa);
  drop(sb);
  drop(service);
  drop(store);

  // 全新节点：映射面只有根域（无任何主库信息可继承，一切在册值须来自镜像条目）
  let (_rdir, rstore) = open_test_store("full_replay_replica.db")?;

  replay_to(&rstore, &aof).await?;

  // 物理镜像承诺：逐条目落回自身物理前缀（直设探针显式携逻辑域真值，纯读零 bump）
  let probe = rstore.new_session()?;
  probe.set_virtual_context(vns, old_vdb, NS_A, DB_A);
  assert_eq!(
    probe.read(b"k1").await?,
    Some(b"v1".to_vec()),
    "数据条目须落回条目自身物理域 ({vns}, {old_vdb})"
  );
  probe.set_virtual_context(vns, new_vdb, NS_A, DB_A);
  assert_eq!(
    probe.read(b"fresh").await?,
    Some(b"vf".to_vec()),
    "清后写入须落回其条目物理域 ({vns}, {new_vdb})"
  );
  probe.set_virtual_context(vns_b, vdb_b, NS_B, DB_B);
  assert_eq!(
    probe.read(b"keep").await?,
    Some(b"vk".to_vec()),
    "他租户条目须落回自身物理域 ({vns_b}, {vdb_b})"
  );
  // 映射继承 + 换号条目仅投死亡账本（DbMeta 镜像承接映射与水位，零本地二次分配）
  assert_eq!(
    logic_ns_set(&rstore),
    vec![0, NS_A, NS_B],
    "全量回放继承主库命名空间映射"
  );
  assert_eq!(
    routing_vns(&rstore),
    vec![0, vns, vns_b],
    "全量回放继承主库租户路由表"
  );
  assert_eq!(
    water_mark(&rstore),
    primary_water,
    "全量回放侧分配水位与主库锁步同值"
  );
  assert!(
    rstore.vdb.is_dead_domain(vns, old_vdb),
    "FlushDb 条目须把旧域投递本地 GC 死亡账本"
  );
  OK
}

/// FlushNs 尾部条目：经 `active_vns` 逆表反查逻辑命名空间后整空间锁步换号
///
/// 载荷是换号前旧 `vns`；ns 标量与逆表由启动重建全量装载（`rebuild_apply_record`
/// 的 NS_MAP 臂），故逆表反查在回放射恒在册。换号走与主库同一 `flush_ns` 事务
/// 体：同一步取号、旧空间判死投账本、新空间标记权威。旧域数据经逻辑入口不可达
/// （命名空间号已换指新值），清后写入落回其条目物理域。
#[compio::test]
async fn tail_flushns_replay_retires_inherited_namespace() -> Void {
  let Node {
    _dir,
    device,
    store,
    aof,
    cp_dir,
    service,
  } = open_node("tail_flushns")?;
  let mgr = flush_funnel(&store, &device, &aof, &cp_dir);

  let sa = store.new_session()?;
  assert!(sa.set_context(NS_A, DB_A));
  let (vns, old_vdb) = store.vdb.get_virtual_ids(NS_A, DB_A);
  sa.upsert(b"k1", b"v1").await?;
  sa.upsert(b"k2", b"v2").await?;
  let sb = store.new_session()?;
  assert!(sb.set_context(NS_B, DB_B));
  sb.upsert(b"keep", b"vk").await?;
  let (vns_b, vdb_b) = store.vdb.get_virtual_ids(NS_B, DB_B);
  let token = store
    .create_checkpoint(&cp_dir, CheckpointType::Snapshot)
    .await?
    .token;

  // 非 0 租户 FLUSHALL 真漏斗：整空间换号 + FlushNs(旧 vns, 0) 广播条目
  mgr.flush_namespace(NS_A, false).await?;
  let (new_vns, new_vdb) = store.vdb.get_virtual_ids(NS_A, DB_A);
  assert_ne!(new_vns, vns, "FlushNs 须换出新的命名空间虚拟号");
  sa.upsert(b"reborn", b"vr").await?;
  assert_eq!(
    flush_payloads(&aof, AofEntryType::FlushNs),
    vec![(vns, 0)],
    "FlushNs 条目载荷须为换号前旧 vns"
  );

  drop(mgr);
  drop(sa);
  drop(sb);
  drop(service);
  drop(store);
  let rstore = Arc::new(WedbStore::recover(&cp_dir, token, Arc::clone(&device)).await?);
  assert_eq!(
    rstore.resolve_context(NS_A, DB_A).await?,
    (vns, old_vdb),
    "从库继承主库映射（检查点镜像内的 DbMeta 权威）"
  );
  let ns_before = logic_ns_set(&rstore);
  let water_before = water_mark(&rstore);

  replay_to(&rstore, &aof).await?;

  // 整空间换号：与主库锁步同号，旧空间判死投账本
  assert_eq!(
    rstore.vdb.vns_of_ns(NS_A),
    Some(new_vns),
    "从库命名空间换号须与主库同号（水位锁步）"
  );
  assert!(
    rstore.vdb.is_dead_ns(vns),
    "FlushNs 条目须把旧空间投递本地 GC 死亡账本"
  );
  // 映射面零新增（须在下方探针会话之前判读：探针自身的 set_context 会按
  // 引擎口径物化清后新空间的库格，与本判据无关）
  assert_eq!(
    logic_ns_set(&rstore),
    ns_before,
    "FlushNs 回放不得物化出幽灵租户（条目物理号不得物化为逻辑号）"
  );
  assert_eq!(
    water_mark(&rstore),
    water_before + 1,
    "换号取号与主库同一步，回放侧此外零分配"
  );
  // 旧域经逻辑入口整体不可达；他租户映射同样继承（库级路由以磁盘 DbMeta 为权威，须先点查装载再走
  // 逻辑入口——冷路由表上 set_context 是盲分配面，直取它会把「未装载」误报
  // 成「被波及」）
  assert_eq!(
    rstore.resolve_context(NS_B, DB_B).await?,
    (vns_b, vdb_b),
    "他租户映射须原样继承，不受 FlushNs 条目换号影响"
  );
  let probe = rstore.new_session()?;
  assert!(probe.set_context(NS_A, DB_A));
  assert_eq!(probe.read(b"k1").await?, None, "整空间换号后旧域键须不可读");
  assert_eq!(probe.read(b"k2").await?, None, "整空间换号后旧域键须不可读");
  assert!(probe.set_context(NS_B, DB_B), "他租户逻辑入口可物化");
  assert_eq!(
    probe.read(b"keep").await?,
    Some(b"vk".to_vec()),
    "他租户不受 FlushNs 波及"
  );
  // 同一份数据仍在其条目物理域上（退役判据未越域误伤）
  probe.set_virtual_context(vns_b, vdb_b, NS_B, DB_B);
  assert_eq!(
    probe.read(b"keep").await?,
    Some(b"vk".to_vec()),
    "他租户数据须原样留在其物理域 ({vns_b}, {vdb_b})"
  );
  assert!(
    !rstore.vdb.is_dead_domain(vns_b, vdb_b),
    "FlushNs 条目不得把他租户活域判进死亡账本"
  );
  // 清后写入落回其条目物理域（新空间的库映射由下一次镜像承接，AOF 不载映射）
  let mirror = rstore.new_session()?;
  mirror.set_virtual_context(new_vns, new_vdb, NS_A, DB_A);
  assert_eq!(
    mirror.read(b"reborn").await?,
    Some(b"vr".to_vec()),
    "FlushNs 后条目须落回其自身物理域 ({new_vns}, {new_vdb})"
  );
  OK
}

/// 4 维 FP32 向量字节
fn fp32_bytes(vals: [f32; 4]) -> Vec<u8> {
  vals.iter().flat_map(|f| f.to_le_bytes()).collect()
}

fn arg_refs(v: &[Vec<u8>]) -> Vec<&[u8]> {
  v.iter().map(Vec::as_slice).collect()
}

/// 单元素 VADD 参数组：`[键, 值类型, 向量字节, 元素名, 量化档位]`（与既有用例
/// 内联形态逐字段同值，重启面用例两次写入须以键判别）
fn vadd_args(key: &[u8]) -> Vec<Vec<u8>> {
  vec![
    key.to_vec(),
    b"FP32".to_vec(),
    fp32_bytes([1.0, 0.0, 0.0, 0.0]),
    b"e1".to_vec(),
    b"NOQUANT".to_vec(),
  ]
}

/// 向量管理器（生产装配同形态：回调无状态，会话按执行域绑定，直调/重放臂经
/// 专用会话工厂自备会话，见 [`VectorManager::bind_dedicated_session`]）
fn vector_manager(store: &Arc<WedbStore<SegmentedDevice>>) -> Arc<VectorManager> {
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new())),
  ));
  let s = Arc::clone(store);
  vm.attach_dedicated_session_factory(Arc::new(move || {
    s.new_session()
      .ok()
      .map(OwnedActiveVectorSession::new)
      .map(ActiveDedicatedVectorSession::from_bound)
  }));
  vm
}

/// 回放面槽位与在线面槽位逐值同值（库级定槽确定性）
///
/// 在线面 `slot_of(逻辑域)`（会话槽口径）；回放面条目只带物理前缀 `(vns, vdb)`，
/// 须经 `VirtualDbManager::logic_domain_of` 逆表反查真逻辑域再定槽，两端算出的
/// 槽位必须同一值——否则同一逻辑库在主从两端落进不同迁移槽，向量登记表按槽分片
/// 即发散（票面第 7 条槽位分叉判据）。
#[compio::test]
async fn replay_face_slot_matches_online_face_slot() -> Void {
  let Node {
    _dir,
    device: _device,
    store,
    aof,
    cp_dir: _cp_dir,
    service,
  } = open_node("slot_parity")?;
  // _dir / _device / _cp_dir 全程持有：TempDir 一 drop 即删目录，段式设备
  // 其后按需新建段文件即 ENOENT（用例只跑得快不撞该窗口，属运气）
  drop(service);

  // 真映射装载（在线面解析入口）→ 条目物理前缀
  let (vns, vdb) = store.resolve_context(NS_A, DB_A).await?;
  assert!(vns > 0 && vdb > 0, "非零租户须分配到非零虚拟号");
  let online_slot = slot_of(NS_A, DB_A);
  let physical_slot = slot_of(vns, vdb);
  assert_ne!(
    online_slot, physical_slot,
    "本用例前提：逻辑域与物理域直算槽位须可判别（测试常量取值失效）"
  );

  let vm = vector_manager(&store);
  // 本执行域绑定专用向量会话（测试单任务段持至用例尾，直调回调臂经线程槽取会话）
  let _vector_domain = vm
    .bind_dedicated_session()
    .expect("专用向量会话工厂应已注入");
  vm.set_aof_sink(Arc::new(VectorAofSink::new(
    &aof,
    Arc::clone(store.current_version_atomic()),
  )));
  aof.set_vector_manager(Arc::clone(&vm));
  let online = RespServerSessionVectors::new(Arc::clone(&vm));
  let prefix = SessionPrefixBuf::new(vns, vdb);
  let args: Vec<Vec<u8>> = vec![
    b"vs".to_vec(),
    b"FP32".to_vec(),
    fp32_bytes([1.0, 0.0, 0.0, 0.0]),
    b"e1".to_vec(),
    b"NOQUANT".to_vec(),
  ];
  assert!(
    matches!(
      online
        .network_vadd(prefix.as_slice(), &arg_refs(&args), online_slot, false)
        .await,
      VectorReply::Integer(1)
    ),
    "在线面 VADD 须成功"
  );

  // 回放面：全新管理器（重启 / 副本语义，登记表为空）
  let replayed_vm = vector_manager(&store);
  // 重放段与重启后直调检查的会话绑定（同上口径）
  let _replay_domain = replayed_vm
    .bind_dedicated_session()
    .expect("专用向量会话工厂应已注入");
  aof.set_vector_manager(Arc::clone(&replayed_vm));
  replay_to(&store, &aof).await?;

  // 登记落回条目自身物理域（守卫直设虚拟域）
  assert!(
    replayed_vm
      .read_stored_index(prefix.as_slice(), b"vs")
      .is_some(),
    "向量登记须落回条目物理域 ({vns}, {vdb})，不得落进重解析的伪域"
  );
  let at_online =
    replayed_vm.get_namespaces_for_hash_slots(&BTreeSet::from([i32::from(online_slot)]));
  let at_physical =
    replayed_vm.get_namespaces_for_hash_slots(&BTreeSet::from([i32::from(physical_slot)]));
  assert!(
    !at_online.is_empty(),
    "回放面登记的 context 须挂在在线面同一槽位 {online_slot} 上"
  );
  assert!(
    at_physical.is_empty(),
    "回放面不得按条目物理域直算槽位 {physical_slot} 登记（主从槽位分叉形态）"
  );
  OK
}

/// 重启面（检查点基线之后回放）向量登记槽位仍取在线面同值
///
/// 链即生产重启装配（`database_manager_base.rs:recover_database_checkpoint_async`）：
/// 映射随检查点落盘 → 版本基线抬至检查点代 → 同设备 + 检查点目录重建 store →
/// 只重放基线之后的 AOF 尾部。此形态下非根租户路由表**必冷**，两条装载事实叠加：
/// 1. 重建面对非根域库级路由表按「冷租户 0 内存常驻」条款刻意不装载
///    （`store/mod.rs:rebuild_apply_record` DbMap / DbSwap 臂仅 `ROOT_VIRTUAL_ID`
///    执行 `insert_db_mapping`，首访须经 `resolve_context` 点查回建）；
/// 2. 早于基线的 `KeyTag::DbMeta` 镜像条目被版本闸挡在应用面之外
///    （`aof_processor.rs:replay_op_dispatch` 的 `record_gate::should_skip_record`
///    先于 `replay_op` 内的 DbMeta 分派返回，`apply_dbmeta_record` 的非根域补格
///    因此不生效）。
///
/// 于是回放面 `logic_domain_of` 的 DB 腿无格可查。库级定槽（doc/zh/db.md 4.1
/// 「同一个 DB 是同一个槽位」）以**逻辑域**现算，故 context 的 hash_slot 必须等于
/// 在线面 `slot_of(NS_A, DB_A_RESTART)`；回退物理号即与在线面分叉，该向量集此后按槽迁移
/// 枚举恒取不到（整键漏发）。判据的过滤对位见 C# AofProcessor 的 ShouldSkipRecord
/// 与 VectorManager.ContextMetadata 的 GetNamespacesForHashSlots（rust 正式锚分别在
/// wnode record_gate.rs 与 vector_manager_context_metadata.rs，此处不重复挂锚）
/// ——C# 每库独立 store、槽位随会话上下文，回放面无域反查形态，rust 单日志物理前缀
/// 承载域才生出本判据。
#[compio::test]
async fn restart_baseline_replay_keeps_online_slot() -> Void {
  let Node {
    _dir,
    device,
    store,
    aof,
    cp_dir,
    service,
  } = open_node("restart_slot")?;

  // 常量取值前提：逻辑库号须在协议地址空间内，否则本用例测的是协议外形态
  assert!(
    DB_A_RESTART < u64::try_from(MAX_DATABASES_MAX).expect("上界为正常量"),
    "本用例前提：逻辑库号须落在协议库号地址空间内（回建枚举面覆盖）"
  );

  // ── 基线段：真映射装载（DbMap 镜像条目落本代）+ 在线面建向量集 ──
  let (vns, vdb) = store.resolve_context(NS_A, DB_A_RESTART).await?;
  assert!(vns > 0 && vdb > 0, "非零租户须分配到非零虚拟号");
  let online_slot = slot_of(NS_A, DB_A_RESTART);
  // 回退臂判别值：ns 腿重建全量装载恒在册，只有 DB 腿可能回退物理号
  let fallback_slot = slot_of(NS_A, vdb);
  assert_ne!(
    online_slot, fallback_slot,
    "本用例前提：逻辑库号与物理库号须算出不同槽（常量取值失效）"
  );
  assert_ne!(
    online_slot,
    slot_of(vns, vdb),
    "本用例前提：双腿齐回退的槽位亦须可判别"
  );

  let vm = vector_manager(&store);
  // 本执行域绑定专用向量会话（测试单任务段持至用例尾，直调回调臂经线程槽取会话）
  let _vector_domain = vm
    .bind_dedicated_session()
    .expect("专用向量会话工厂应已注入");
  vm.set_aof_sink(Arc::new(VectorAofSink::new(
    &aof,
    Arc::clone(store.current_version_atomic()),
  )));
  aof.set_vector_manager(Arc::clone(&vm));
  let online = RespServerSessionVectors::new(Arc::clone(&vm));
  let prefix = SessionPrefixBuf::new(vns, vdb);
  assert!(
    matches!(
      online
        .network_vadd(
          prefix.as_slice(),
          &arg_refs(&vadd_args(b"vs_base")),
          online_slot,
          false,
        )
        .await,
      VectorReply::Integer(1)
    ),
    "在线面 VADD 须成功"
  );

  // ── 拍检查点并抬升版本基线（生产漏斗同款：预签 Token + checkpoint_version；
  // 位窗口先于版本推进打开、快照落盘后即关，杜绝窗口内写入双重生效）──
  let floor = find_latest_checkpoint(&cp_dir)?.unwrap_or(0);
  let token = next_token_above(floor);
  let baseline = checkpoint_version(token);
  let index_start = store.begin_version_shift(baseline as u64);
  let cp_res = store
    .create_checkpoint_with_token(&cp_dir, CheckpointType::Snapshot, token, index_start)
    .await;
  store.end_version_shift();
  cp_res?;

  // 基线后的向量条目（store_version == 基线，重放端必应用）：新键 vs_tail
  assert!(
    matches!(
      online
        .network_vadd(
          prefix.as_slice(),
          &arg_refs(&vadd_args(b"vs_tail")),
          online_slot,
          false,
        )
        .await,
      VectorReply::Integer(1)
    ),
    "基线后在线面 VADD 须成功"
  );

  // ── 重启面：主库句柄全释放 → 同设备 + 检查点目录重建（全程零点查该租户）──
  drop(online);
  drop(vm);
  drop(service);
  drop(store);
  let rstore = Arc::new(WedbStore::recover(&cp_dir, token, Arc::clone(&device)).await?);
  rstore.set_current_version(baseline);
  // 前提复核：非根域路由表在重启面即冷，反查 DB 腿无格可查（回退臂入口）
  assert_eq!(
    rstore.vdb.route_vdb_of(vns, DB_A_RESTART),
    None,
    "重启面非根租户库级路由表须未装载（冷租户 0 内存常驻条款）"
  );

  // 重放面：全新 VectorManager（登记表空），基线位点只放尾部条目
  let replayed_vm = vector_manager(&rstore);
  // 重放段与重启后直调检查的会话绑定（同上口径）
  let _replay_domain = replayed_vm
    .bind_dedicated_session()
    .expect("专用向量会话工厂应已注入");
  aof.set_vector_manager(Arc::clone(&replayed_vm));
  replay_to(&rstore, &aof).await?;

  // 条目侧代际判别：基线前旧代条目被闸跳过，基线后条目确被重放
  assert!(
    replayed_vm
      .read_stored_index(prefix.as_slice(), b"vs_base")
      .is_none(),
    "基线前的向量条目须被版本闸跳过（用例形态前提：回放面确为尾部增量）"
  );
  assert!(
    replayed_vm
      .read_stored_index(prefix.as_slice(), b"vs_tail")
      .is_some(),
    "基线后的向量条目须登记落回条目物理域 ({vns}, {vdb})"
  );

  let at_online =
    replayed_vm.get_namespaces_for_hash_slots(&BTreeSet::from([i32::from(online_slot)]));
  let at_fallback =
    replayed_vm.get_namespaces_for_hash_slots(&BTreeSet::from([i32::from(fallback_slot)]));
  assert!(
    at_fallback.is_empty(),
    "回退实锤：context 挂在物理号直算槽 {fallback_slot}（slot_of({NS_A}, {vdb})），\
     与在线面 {online_slot} 分叉——同一逻辑库主从两端落进不同迁移槽"
  );
  assert!(
    !at_online.is_empty(),
    "重启后回放登记的 context 须挂在线面同一槽 {online_slot}（slot_of({NS_A}, {DB_A_RESTART})）"
  );

  // 暖表来源甄别：本租户快照只能由回放面的磁盘 DbMeta 点查回建建立——
  // 1. 该域 0x02 镜像条目早于基线，已被上面的版本闸跳过（vs_base 同代未落回即
  //    为证），回放路径的 apply_dbmeta_record 补格臂无从触发；
  // 2. insert_db_mapping 只写格、不落权威标记，权威位仅
  //    load_routes_of_vns 的完整枚举收尾会置位。
  // 故此断言成立即证明反查用的是磁盘映射权威，不是别的路径顺手暖到的。
  assert_eq!(
    rstore.vdb.route_vdb_of(vns, DB_A_RESTART),
    Some(vdb),
    "回放面须以磁盘 DbMeta 权威回建该租户的逻辑库格"
  );
  assert!(
    rstore.vdb.is_route_authoritative(vns),
    "回建须按本运行期权威全量落标记（未落即回放面未走点查回建臂）"
  );
  OK
}

/// 连续两次快照后崩溃重启：旧代（两快照覆盖区间）AOF 条目被版本闸精准跳过，
/// 新代尾部增量恰一次重放，无任何重复重放（票面：checkpoint 版本地板单调性）
///
/// 代 1 Token 经远超当前墙钟的目录 floor 人为抬升高位（模拟跨进程重启后墙钟
/// 滞后于目录历史检查点，候选形态与墙钟回拨同构），代 2 签发必然走钳制臂——
/// 修复前 `floor + 1` 仅低位进位，两代版本号完全相等，g2 条目（携带代 1 版本）
/// 在代 2 基线重放时对 `is_old_version_record` 不可见而重复应用；修复后版本
/// 投影（token >> 64）逐代严格 +1，旧代条目全部跳过（对标 C# VersionChangeSM
/// `nextState.Version = start.Version + 1` 的严格代际推进）。
///
/// 重放计数判据：`single_log_recover` 返回值只累计**实际应用**条目
/// （`recover_log_driver.rs:replay_one` 在 `process_aof_record_internal` 成功后
/// 自增），故恰等于 1 即证明 g1/g2 全部被 `should_skip_record` 跳过。
#[test]
fn two_generations_replay_skips_old_snapshot_region_exactly() -> Void {
  /// 模拟目录历史 floor 的高位：高于当前墙钟纳秒量级（~1.8e18 @2026）、
  /// 低于 i64 正上界（9.22e18，wnode 版本投影恒正且 fetch_max 可推进）
  const STALE_CLOCK_FLOOR_HI: u64 = 8_000_000_000_000_000_000;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let Node {
      _dir,
      device,
      store,
      aof,
      cp_dir,
      service,
    } = open_node("two_gen_floor")?;

    // ── 代 1：写入 g1（携带初代版本 0）→ 抬高基线签发 → 快照 1 ──
    let sa = store.new_session()?;
    assert!(sa.set_context(NS_A, DB_A), "主库逻辑域可物化");
    sa.upsert(b"g1", b"v1").await?;
    let stale_floor = (u128::from(STALE_CLOCK_FLOOR_HI) << 64) | 42;
    let token1 = next_token_above(stale_floor);
    let baseline1 = checkpoint_version(token1);
    assert!(
      baseline1 > 0,
      "代 1 版本投影须为正（floor 高位取值前提）: {baseline1}"
    );
    let index_start = store.begin_version_shift(baseline1 as u64);
    let cp1 = store
      .create_checkpoint_with_token(&cp_dir, CheckpointType::Snapshot, token1, index_start)
      .await;
    store.end_version_shift();
    cp1?;

    // ── 代 2：写入 g2（携带代 1 版本）→ 快照 2，版本必须严格递增 ──
    sa.upsert(b"g2", b"v2").await?;
    let floor2 = find_latest_checkpoint(&cp_dir)?.unwrap_or(0);
    assert_eq!(floor2, token1, "目录下界须为代 1 检查点");
    let token2 = next_token_above(floor2);
    let baseline2 = checkpoint_version(token2);
    assert!(
      baseline2 > baseline1,
      "连续两代检查点版本必须严格递增（修复判据）: {baseline1} -> {baseline2}"
    );
    let index_start = store.begin_version_shift(baseline2 as u64);
    let cp2 = store
      .create_checkpoint_with_token(&cp_dir, CheckpointType::Snapshot, token2, index_start)
      .await;
    store.end_version_shift();
    cp2?;

    // ── 代 2 尾部增量（重放端必应用）：g3 ──
    sa.upsert(b"g3", b"v3").await?;
    drop(sa);
    drop(service);
    drop(store);

    // ── 重启面：恢复代 2 检查点，版本基线 = baseline2（生产恢复漏斗同款）──
    let rstore = Arc::new(WedbStore::recover(&cp_dir, token2, Arc::clone(&device)).await?);
    rstore.set_current_version(baseline2);

    // 全段重放：对标 C# SingleLogRecover 返回全段扫描条目数（6 条），
    // 旧代条目在内部经版本闸跳过，新代尾部增量 g3 恰一次应用
    let replayed = replay_to(&rstore, &aof).await?;
    assert_eq!(
      replayed, 6,
      "全段扫描处理 6 条 AOF 记录（对标 C# SingleLogRecover 扫描计数契约）"
    );

    // 值域复核：g1/g2 由快照 2 物化，g3 由重放承接，三者须同真
    let (vns, vdb) = rstore.resolve_context(NS_A, DB_A).await?;
    assert!(vns > 0 && vdb > 0, "恢复面须继承主库映射");
    let rsa = rstore.new_session()?;
    assert!(rsa.set_context(NS_A, DB_A));
    assert_eq!(
      rsa.read(b"g1").await?.as_deref(),
      Some(b"v1".as_slice()),
      "快照物化的初代数据完好"
    );
    assert_eq!(
      rsa.read(b"g2").await?.as_deref(),
      Some(b"v2".as_slice()),
      "快照物化的代 1 数据完好"
    );
    assert_eq!(
      rsa.read(b"g3").await?.as_deref(),
      Some(b"v3".as_slice()),
      "重放承接的代 2 尾部增量恰一次生效"
    );
    OK
  })
}
