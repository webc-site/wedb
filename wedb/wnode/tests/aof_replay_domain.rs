//! AOF 回放面**虚拟域**端到端集成测试（doc/zh/db.md「主从物理镜像与异步屏障」）
//!
//! 四条链（票面验收 2、3 的端到端面）：
//! 1. `tail_flushdb_replay_swaps_inherited_domain`——「checkpoint 基线 + AOF
//!    尾部增量」形态下非零 vns/vdb 的换号条目回放：从库经镜像继承主库映射，
//!    FlushDb(vns, 换号前旧 vdb) 条目在本节点**在册**路由格上换号并与主库锁步
//!    同号——旧域键不可读、清后新号写入可见、他租户完好；
//! 2. `full_replay_nonzero_domain_lands_in_entry_domain`——非零 vns/vdb 形态的
//!    全量回放：每条条目严格落回自身物理前缀（物理镜像承诺），回放侧对映射面
//!    零分配零落盘（分配水位 / 租户表 / 库路由表全程不动），换号条目只投本地
//!    GC 死亡账本；
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
use wbase::{align::DEFAULT_SECTOR_SIZE, hash_slot::slot_of};
use wconf::RuntimeServerOptions;
use wcpr::CheckpointType;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  GarnetAppendOnlyFile,
  aof::{
    aof_processor::{AofProcessor, ReplayTarget, parse_flush_domain},
    recover::aof_recover::AofRecover,
    waof_sublog::single_log_aof,
  },
  database::{GarnetDatabase, SingleDatabaseManager},
  resp::vector::{
    resp_server_session_vectors::{RespServerSessionVectors, VectorReply},
    vector_manager::{VectorManager, VectorManagerOptions},
    vector_manager_replication::VectorAofSink,
    vector_store_callbacks::WedbVectorStoreCallbacks,
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
    Some(64 * 1024),
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

/// 回放整段日志到目标 store（`store_version` 取 0 = 全量重放，不做代际跳过；
/// 目标 store 的 AOF 写端口暂停——重放写入不得镜像回写，与生产恢复会话同闸）
async fn replay_all(
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
    store_version: 0,
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
#[test]
fn tail_flushdb_replay_swaps_inherited_domain() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
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

    replay_all(&rstore, &aof).await?;

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
  })
}

/// 非零 vns / 非零 vdb 形态的全量回放：条目逐部落回自身物理前缀
///
/// 全新节点（无映射可继承）全量回放形态：回放面对映射面零分配、零落盘——
/// 物理号不会被当作逻辑号盲分配，故条目落域即条目自身前缀（物理镜像承诺），
/// 换号条目仅把旧域投本地 GC 死亡账本（doc/zh/db.md「换号条目即屏障」）。
#[test]
fn full_replay_nonzero_domain_lands_in_entry_domain() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
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
    drop(mgr);
    drop(sa);
    drop(sb);
    drop(service);
    drop(store);

    // 全新节点：映射面只有根域，回放前快照其水位与在册集合
    let (_rdir, rstore) = open_test_store("full_replay_replica.db")?;
    let ns_before = logic_ns_set(&rstore);
    let routing_before = routing_vns(&rstore);
    let water_before = water_mark(&rstore);

    replay_all(&rstore, &aof).await?;

    // 物理镜像承诺：逐条目落回自身物理前缀
    let probe = rstore.new_session()?;
    probe.set_virtual_context(vns, old_vdb);
    assert_eq!(
      probe.read(b"k1").await?,
      Some(b"v1".to_vec()),
      "数据条目须落回条目自身物理域 ({vns}, {old_vdb})"
    );
    probe.set_virtual_context(vns, new_vdb);
    assert_eq!(
      probe.read(b"fresh").await?,
      Some(b"vf".to_vec()),
      "清后写入须落回其条目物理域 ({vns}, {new_vdb})"
    );
    probe.set_virtual_context(vns_b, vdb_b);
    assert_eq!(
      probe.read(b"keep").await?,
      Some(b"vk".to_vec()),
      "他租户条目须落回自身物理域 ({vns_b}, {vdb_b})"
    );
    // 映射面零污染 + 换号条目仅投死亡账本
    assert_eq!(logic_ns_set(&rstore), ns_before, "全量回放零映射新增");
    assert_eq!(routing_vns(&rstore), routing_before, "全量回放零路由表新增");
    assert_eq!(
      water_mark(&rstore),
      water_before,
      "全量回放侧分配水位绝不受扰动"
    );
    assert!(
      rstore.vdb.is_dead_domain(vns, old_vdb),
      "FlushDb 条目须把旧域投递本地 GC 死亡账本"
    );
    OK
  })
}

/// FlushNs 尾部条目：经 `active_vns` 逆表反查逻辑命名空间后整空间锁步换号
///
/// 载荷是换号前旧 `vns`；ns 标量与逆表由启动重建全量装载（`rebuild_apply_record`
/// 的 NS_MAP 臂），故逆表反查在回放射恒在册。换号走与主库同一 `flush_ns` 事务
/// 体：同一步取号、旧空间判死投账本、新空间标记权威。旧域数据经逻辑入口不可达
/// （命名空间号已换指新值），清后写入落回其条目物理域。
#[test]
fn tail_flushns_replay_retires_inherited_namespace() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
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

    replay_all(&rstore, &aof).await?;

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
    probe.set_virtual_context(vns_b, vdb_b);
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
    mirror.set_virtual_context(new_vns, new_vdb);
    assert_eq!(
      mirror.read(b"reborn").await?,
      Some(b"vr".to_vec()),
      "FlushNs 后条目须落回其自身物理域 ({new_vns}, {new_vdb})"
    );
    OK
  })
}

/// 4 维 FP32 向量字节
fn fp32_bytes(vals: [f32; 4]) -> Vec<u8> {
  vals.iter().flat_map(|f| f.to_le_bytes()).collect()
}

fn arg_refs(v: &[Vec<u8>]) -> Vec<&[u8]> {
  v.iter().map(Vec::as_slice).collect()
}

/// 绑 wkv 会话的向量管理器（生产装配同形态）
fn vector_manager(store: &Arc<WedbStore<SegmentedDevice>>) -> Arc<VectorManager> {
  let session = Arc::new(store.new_session().expect("向量回调会话"));
  Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new(session))),
  ))
}

/// 回放面槽位与在线面槽位逐值同值（库级定槽确定性）
///
/// 在线面 `slot_of(逻辑域)`（会话槽口径）；回放面条目只带物理前缀 `(vns, vdb)`，
/// 须经 `VirtualDbManager::logic_domain_of` 逆表反查真逻辑域再定槽，两端算出的
/// 槽位必须同一值——否则同一逻辑库在主从两端落进不同迁移槽，向量登记表按槽分片
/// 即发散（票面第 7 条槽位分叉判据）。
#[test]
fn replay_face_slot_matches_online_face_slot() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
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
        online.network_vadd(prefix.as_slice(), &arg_refs(&args), online_slot),
        VectorReply::Integer(1)
      ),
      "在线面 VADD 须成功"
    );

    // 回放面：全新管理器（重启 / 副本语义，登记表为空）
    let replayed_vm = vector_manager(&store);
    aof.set_vector_manager(Arc::clone(&replayed_vm));
    replay_all(&store, &aof).await?;

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
  })
}
