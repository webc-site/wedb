//! FLUSH 族 AOF 条目生产 → 回放闭环集成测试（主从换号复制链路）
//!
//! 覆盖：enqueue_safe_flush_aof 域载荷 [ns: u64 LE][db: u64 LE] 经回放对称
//! 解析（u64 全宽无 u8 截断）、FlushDb/FlushNs 条目从库回放**只作屏障**（doc/zh/
//! db.md「主库换号（FlushDb/FlushNs 条目）即屏障」），映射继承与旧域判死一律由
//! 先行的 KeyTag::DbMeta 镜像条目承接（他域完好、清后新写可见、副本映射面逐值
//! 等于主库镜像值＝零本地二次取号）、主库门控（is_primary = false 不入队）、
//! FlushAll 广播全清、RESP 主路径经清库唯一漏斗真生产广播条目端到端闭环。
//!
//! 对标 C#：SingleDatabaseManager.cs:SafeFlushAOF（清库执行段原子补写广播
//! 条目）、BasicCommands.cs:ExecuteFlushDb（清库唯一入口 = 管理面）、
//! AofProcessor.cs:ReplayAOF（FlushAll/FlushDb 回放臂）。

use std::sync::Arc;

use aok::{OK, Void};
use compio::net::TcpStream;
use waof::{AofEntryType, AofHeader, AofHeaderType};
use wbase::align::DEFAULT_SECTOR_SIZE;
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wkv::{DbMetaRecord, WedbStore};
use wnode::{
  aof::{
    aof_processor::{AofProcessor, ReplayTarget, parse_flush_domain},
    garnet_append_only_file::GarnetAppendOnlyFile,
    garnet_log::{GarnetLog, RecordShape},
    recover::aof_recover::AofRecover,
  },
  database::{GarnetDatabase, SingleDatabaseManager},
  resp::garnet_api::{GarnetApiFace, StoreGarnetApi},
  service::StorageSessionProvider,
  storage::session::storage_session::StorageSession,
};
use wresp::command::RespCommand;
use wtest_base::{open_test_store, test_store_config};
use wval::{KeyTag, NamespaceDbCodec};

/// 单物理日志拓扑的 AOF 装配
fn aof_fixture(tag: &str) -> aok::Result<Arc<GarnetAppendOnlyFile>> {
  let options = RuntimeServerOptions::default();
  let backends = {
    let (_dirs, backends) = wnode_test::test_sublogs(tag, 1);
    backends
  };
  Ok(Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
    &options,
    None,
  )))
}

/// 指定域前缀 (ns, db) 的物理键（条目 key 统一 wkv 物理键形态，模拟主库
/// 虚拟 ID 前缀域值）
fn physical_at(ns: u64, db: u64, user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(ns, db, KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// 数据条目入队（key 为完整物理键）
fn enqueue_upsert_at(
  log: &GarnetLog,
  ns: u64,
  db: u64,
  key: &[u8],
  value: &[u8],
) -> aok::Result<i64> {
  Ok(log.enqueue(&RecordShape {
    op_type: AofEntryType::StoreUpsert,
    version: 1,
    session_id: 1,
    key: &physical_at(ns, db, key),
    value,
    input: &[],
    database_id: 0,
  })?)
}

/// 主库换号事务的 DbMeta 镜像条目入队（生产形态复刻，副本映射体系的唯一同步
/// 通道）
///
/// 主库 `WedbStore::commit_swap` 原子批按安全顺序 `[新映射, 旧域退役墓碑?,
/// 0x05 分配水位]` 落盘（doc/zh/db.md「即时原子提交」段），每条 `KeyTag::DbMeta`
/// 记录经 `service.rs:on_aof_store_event` 放行的写端口镜像为一条 StoreUpsert
/// 条目：键 = 根域前缀 (0, 0) + DbMeta 标签 + 记录键载荷、值 = 定长记录值
///（布局单点 `wkv::DbMetaRecord`，本函数据其 `key()/value()` 组条目，不手写
/// 字节）。落盘先于同事务的 FlushDb / FlushNs 广播条目入队，故回放按序到达即
/// 映射已就位。
///
/// 对标 C# 无对位（单租户、每库独立 store，无虚拟域映射可镜像）；本仓对位口径
/// 见 doc/zh/db.md「主从物理镜像与异步屏障」：「物理日志复制与 Checkpoint 直接
/// 镜像主库的 KeyTag::DbMeta 与数据记录。从库完全继承主库的映射体系，不进行
/// 本地二次映射」
fn enqueue_dbmeta_mirror(log: &GarnetLog, records: &[DbMetaRecord]) -> aok::Result<()> {
  for rec in records {
    let key = NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::DbMeta, rec.key().as_slice());
    log.enqueue(&RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 1,
      key: key.as_slice(),
      value: rec.value().as_slice(),
      input: &[],
      database_id: 0,
    })?;
  }
  Ok(())
}

/// 从库空库全量回放（提交位点 0 起）
async fn replica_replay(
  rstore: &Arc<wkv::WedbStore<wdev::SegmentedDevice>>,
  aof: &Arc<GarnetAppendOnlyFile>,
) -> aok::Result<()> {
  let session = rstore.new_session()?;
  let batch = session.enter_batch();
  let storage = StorageSession::new(batch);
  let target = ReplayTarget {
    session: &storage,
    store: Arc::clone(rstore),
    aof_floor: vec![],
  };
  let processor = AofProcessor::new(Arc::clone(aof));
  AofRecover::single_log_recover(&processor, aof, 0, 0, -1, &target).await?;
  Ok(())
}

/// 映射面快照（回放侧「绝不本地二次映射」判据：租户表 / 路由表 / 分配水位）
#[derive(PartialEq, Debug)]
struct MappingFace {
  logic_ns: Vec<u64>,
  routing_vns: Vec<u64>,
  water_mark: u64,
}

fn mapping_face(store: &Arc<wkv::WedbStore<wdev::SegmentedDevice>>) -> MappingFace {
  use std::sync::atomic::Ordering::Relaxed;
  let mut logic_ns: Vec<u64> = store.vdb.ns_map.pin().iter().map(|(k, _)| *k).collect();
  logic_ns.sort_unstable();
  let mut routing_vns: Vec<u64> = store.vdb.db_routing.pin().iter().map(|(k, _)| *k).collect();
  routing_vns.sort_unstable();
  MappingFace {
    logic_ns,
    routing_vns,
    water_mark: store.vdb.next_virtual_id.load(Relaxed),
  }
}

/// 域载荷 u64 全宽对称闭环：FlushDb(vns=0x0102_0304_0506_0708, 换号前旧 vdb=42)
/// 若经 C# databaseId 1 字节形态必截断为 0x08；换号批镜像条目（0x04 库级退役
/// 墓碑）把该旧域按**全宽 vns** 记进副本死亡账本（`GcDeadEntry::vns`），截断
/// 形态只能落成 8；FlushDb 条目自身只作屏障（doc/zh/db.md「主库换号
///（FlushDb/FlushNs 条目）即屏障」）。
///
/// 探针一律经物理域直设（回放面唯一口径，见 `KeyContextGuard` 文档）：条目键
/// 前缀即主库换号前的虚拟号，从库绝不本地二次映射——映射与判死全部来自镜像
/// 条目，故副本映射面逐值等于镜像值、本地分配器零触发。
#[compio::test]
async fn test_flush_entry_payload_u64_domain() -> Void {
  let vns: u64 = 0x0102_0304_0506_0708;
  let (_dir, _store) = open_test_store("flush-payload.db")?;
  let aof = aof_fixture("flush_payload")?;
  let log = aof.log();

  enqueue_upsert_at(log, vns, 42, b"hit", b"v")?;
  enqueue_upsert_at(log, 8, 43, b"decoy", b"v")?;
  // 主库换号批镜像：逻辑租户 5 → vns 全宽值，逻辑库 0 换指新号 44，旧域
  // (vns, 42) 判死，水位抬到 45
  enqueue_dbmeta_mirror(
    log,
    &[
      DbMetaRecord::NsMap { logic_ns: 5, vns },
      DbMetaRecord::DbMap {
        vns,
        logic_db: 0,
        vdb: 44,
      },
      DbMetaRecord::GcDeadDb {
        expired_at: 1,
        vns,
        old_vdb: 42,
        tail_address: 0,
      },
      DbMetaRecord::NextId {
        next_virtual_id: 45,
      },
    ],
  )?;
  let _ = log.enqueue_safe_flush_aof(AofEntryType::FlushDb, true, vns, 42)?;
  log.commit();

  let (_rdir, rstore) = open_test_store("flush-payload-replica.db")?;
  replica_replay(&rstore, &aof).await?;

  // 条目逐部落回自身物理前缀，他域条目不被错域退役波及
  // （直设探针逻辑槽取物理对本值——回放条目域无本地逻辑入口，孤域替身口径，
  // 纯读零 bump；版本轨=逻辑域种子契约下同值即安全代位）
  let probe = rstore.new_session()?;
  probe.set_virtual_context(8, 43, 8, 43);
  assert_eq!(
    probe.read(b"decoy").await?,
    Some(b"v".to_vec()),
    "他域条目须原样落回自身物理域 (8, 43)"
  );
  assert!(!rstore.vdb.is_dead_domain(8, 43), "他域不得判死");
  // 退役墓碑记在全宽 vns 上（u8 截断形态只会记成 8）
  let dead = rstore
    .vdb
    .gc_dead
    .get(&42)
    .expect("换号批 0x04 墓碑镜像条目须落旧域退役墓碑");
  assert_eq!(
    dead.vns,
    Some(vns),
    "墓碑所属虚拟命名空间须等于换号批载荷全宽值（截断即落 8）"
  );
  assert!(
    rstore.vdb.is_dead_domain(vns, 42),
    "载荷域 (全宽 vns, 42) 须判死"
  );
  // 回放侧零二次映射：映射面逐值等于主库镜像值，条目物理号绝不物化为逻辑号
  // （水位取 `fetch_max` 单调抬升：镜像 NsMap/DbMap 落域即把水位推到 `号+1`，
  // 其后 0x05 记录只前进不回退——本测试刻意用全宽 u64 虚拟号（远超真实取号
  // 序列），故水位由该号抬升主导；u8 截断形态只会得到 routing_vns=[0,8] 与
  // 水位 45，两面同时失配）
  assert_eq!(
    mapping_face(&rstore),
    MappingFace {
      logic_ns: vec![0, 5],
      routing_vns: vec![0, vns],
      water_mark: vns + 1,
    },
    "副本映射面须逐值等于主库镜像值（继承映射体系，本地取号器零触发）"
  );
  // 继承后的逻辑入口解析到主库新号，旧域键经逻辑入口不可达
  assert!(
    probe.set_context(5, 0),
    "镜像映射在册：逻辑域 (5, 0) 可物化"
  );
  assert_eq!(
    rstore.vdb.route_vdb_of(vns, 0),
    Some(44),
    "逻辑库 0 须换指主库镜像的新虚拟号"
  );
  assert_eq!(
    probe.read(b"hit").await?,
    None,
    "换号后旧域 (全宽 vns, 42) 键经逻辑入口不可达"
  );
  OK
}

/// 域载荷对称解析：头 16B 之后为 [ns: u64 LE][db: u64 LE]，缺失即显式报错
#[test]
fn test_flush_domain_parse_roundtrip() -> Void {
  let ns: u64 = 0x0102_0304_0506_0708;
  let mut entry = [0u8; AofHeader::TOTAL_SIZE + 16];
  entry[AofHeader::TOTAL_SIZE..AofHeader::TOTAL_SIZE + 8].copy_from_slice(&ns.to_le_bytes());
  entry[AofHeader::TOTAL_SIZE + 8..].copy_from_slice(&42u64.to_le_bytes());
  let (got_ns, got_db) = parse_flush_domain(&entry).expect("完整载荷可解析");
  assert_eq!((got_ns, got_db), (ns, 42));

  // 载荷缺失（纯头旧形态）显式失败，绝不静默按零域清库
  let bare = [0u8; AofHeader::TOTAL_SIZE];
  assert!(parse_flush_domain(&bare).is_err());
  // 截断载荷显式失败
  assert!(parse_flush_domain(&entry[..AofHeader::TOTAL_SIZE + 8]).is_err());
  OK
}

/// 广播条目头型与域载荷读端配对：单物理日志 + 多重放任务拓扑下
/// `enqueue_safe_flush_aof` 经 `enqueue_broadcast_entry` 把头型改写为
/// SingleLogTransactionHeader（对标 C# GarnetLog.cs:1200-1211；分片拓扑同理落
/// ShardedLogTransactionHeader :1215-1225），域载荷起点随头型移动。写侧 C# 把
/// 库号放头字段 `databaseId`（GarnetLog.cs:1263）与头型无关，rust 以头后 16B
/// 载荷承载 ns/db 全宽，读端游标必须按头型定长——按 16B 硬编码即读到事务头的
/// participantCount/访问向量，副本清错域。
#[test]
fn test_flush_domain_under_transaction_header() -> Void {
  let options = RuntimeServerOptions {
    aof_replay_task_count: 2,
    ..Default::default()
  };
  let (_dirs, backends) = wnode_test::test_sublogs("flush_txn_hdr", 1);
  let log = GarnetLog::new(&options, backends, None).expect("构造 GarnetLog");

  let ns: u64 = 0x0102_0304_0506_0708;
  let db: u64 = 42;
  let _ = log.enqueue_safe_flush_aof(AofEntryType::FlushDb, false, ns, db)?;
  log.commit();

  let mut seen: Vec<(AofHeaderType, (u64, u64))> = Vec::new();
  log.scan_single_with(0, 0, i64::MAX, |rec| {
    if !waof::is_commit_frame(&rec.payload)
      && let Some(header) = AofHeader::parse(&rec.payload)
      && header.op_type == AofEntryType::FlushDb as u8
    {
      seen.push((
        header.header_type().expect("已知头型"),
        parse_flush_domain(&rec.payload).expect("事务头形态 FlushDb 条目域载荷可读"),
      ));
    }
    true
  });
  assert_eq!(
    seen.len(),
    1,
    "恰一条 FlushDb 广播条目（判据非空，杜绝扫描零命中恒真）"
  );
  let (header_type, got) = seen[0];
  assert_eq!(
    header_type,
    AofHeaderType::SingleLogTransactionHeader,
    "预置：多重放任务拓扑的广播条目须为事务头形态（否则本用例退回 16B 同形）"
  );
  assert_eq!(got, (ns, db), "域载荷须按头型定长读到原 ns/db 全宽值");
  OK
}

/// 主从换号条目回放闭环（物理域口径）：主库域 (vns=3, vdb=7) 两条数据条目 →
/// 换号批 DbMeta 镜像条目 → FlushDb(3, 7) 屏障条目 → 清后新号域 (3, 8) 写入
/// 条目。副本侧每条条目严格落回自身物理前缀（物理镜像承诺）、载荷旧域 (3, 7)
/// 经镜像墓碑判死、他租户域 (5, 1) 完好，且映射面逐值等于主库镜像值（从库继承
/// 主库映射体系，绝无本地二次取号）。
///
/// 「旧域经逻辑入口不可达 + 副本换号与主库锁步同号」正由镜像继承形态判读——
/// 副本逻辑入口 (2, 0) 解析到主库换入的新号 8，与主库同号。
#[compio::test]
async fn test_flush_db_replica_replays_entry_domains_without_local_remap() -> Void {
  let (_dir, _store) = open_test_store("flush-follow.db")?;
  let aof = aof_fixture("flush_follow")?;
  let log = aof.log();

  // ── 主库生产段（数据条目 key 前缀 = 主库虚拟 ID 对）──
  enqueue_upsert_at(log, 3, 7, b"k1", b"v1")?;
  enqueue_upsert_at(log, 3, 7, b"k2", b"v2")?;
  enqueue_upsert_at(log, 5, 1, b"keep", b"vk")?;
  // 主库 FLUSHDB 换号批镜像：逻辑租户 2 → vns 3、逻辑库 0 换指新号 8、
  // 旧域 (3, 7) 判死、水位抬到 9（commit_swap 落盘先于本事务的 FlushDb 条目）
  enqueue_dbmeta_mirror(
    log,
    &[
      DbMetaRecord::NsMap {
        logic_ns: 2,
        vns: 3,
      },
      DbMetaRecord::DbMap {
        vns: 3,
        logic_db: 0,
        vdb: 8,
      },
      DbMetaRecord::GcDeadDb {
        expired_at: 1,
        vns: 3,
        old_vdb: 7,
        tail_address: 0,
      },
      DbMetaRecord::NextId { next_virtual_id: 9 },
    ],
  )?;
  // FLUSHDB 清库执行段：主库换号后原子补写 FlushDb(3, 7)（回放射仅屏障）
  let _ = log.enqueue_safe_flush_aof(AofEntryType::FlushDb, false, 3, 7)?;
  // 清后主库按新虚拟号 (3, 8) 写入
  enqueue_upsert_at(log, 3, 8, b"fresh", b"vf")?;
  log.commit();

  // ── 从库回放段 ──
  let (_rdir, rstore) = open_test_store("flush-follow-replica.db")?;
  replica_replay(&rstore, &aof).await?;

  // 条目落回自身物理域，载荷旧域判死，他租户完好
  let probe = rstore.new_session()?;
  probe.set_virtual_context(3, 8, 3, 8);
  assert_eq!(
    probe.read(b"fresh").await?,
    Some(b"vf".to_vec()),
    "清后新号域条目须落回其自身物理域 (3, 8)"
  );
  assert!(
    rstore.vdb.is_dead_domain(3, 7),
    "换号批墓碑镜像条目须把载荷旧域 (3, 7) 投进本地 GC 死亡账本"
  );
  assert!(!rstore.vdb.is_dead_domain(3, 8), "清后新域 (3, 8) 不得判死");
  probe.set_virtual_context(5, 1, 5, 1);
  assert_eq!(
    probe.read(b"keep").await?,
    Some(b"vk".to_vec()),
    "他租户域不受 FlushDb 波及"
  );
  assert!(!rstore.vdb.is_dead_domain(5, 1), "他租户域不得判死");
  // 回放侧零二次映射：映射面逐值等于主库镜像条目值，条目物理号绝不物化为
  // 本地新号（他租户物理域 vns 5 未在册即证镜像之外的号不落逻辑面）
  assert_eq!(
    mapping_face(&rstore),
    MappingFace {
      logic_ns: vec![0, 2],
      routing_vns: vec![0, 3],
      water_mark: 9,
    },
    "副本映射面须逐值等于主库镜像值（继承映射体系，本地取号器零触发）"
  );
  assert_eq!(
    rstore.vdb.route_vdb_of(3, 0),
    Some(8),
    "副本路由格须直指主库换入的新号（锁步同号）"
  );
  // 继承映射后旧域经逻辑入口不可达、清后新号可读
  assert!(
    probe.set_context(2, 0),
    "镜像映射在册：逻辑域 (2, 0) 可物化"
  );
  assert_eq!(
    probe.read(b"k1").await?,
    None,
    "换号后旧域键经逻辑入口不可达"
  );
  assert_eq!(
    probe.read(b"k2").await?,
    None,
    "换号后旧域键经逻辑入口不可达"
  );
  assert_eq!(
    probe.read(b"fresh").await?,
    Some(b"vf".to_vec()),
    "清后新号域写入经逻辑入口可读"
  );
  OK
}

/// FlushNs 回放闭环（物理域口径）：ns 5 域两库数据条目 → 整空间换号批 DbMeta
/// 镜像条目（0x01 新空间映射 + 0x03 旧空间退役墓碑 + 0x05 水位）→ FlushNs(旧
/// vns=5) 屏障条目 → 载荷空间判死（其内条目域再无在册逻辑入口），他空间 (3, 1)
/// 条目与存续态完好，副本映射面逐值等于镜像值（本地取号器零触发）
#[compio::test]
async fn test_flush_ns_replica_replay() -> Void {
  let (_dir, _store) = open_test_store("flush-ns.db")?;
  let aof = aof_fixture("flush_ns")?;
  let log = aof.log();

  enqueue_upsert_at(log, 5, 1, b"n1", b"v1")?;
  enqueue_upsert_at(log, 5, 2, b"n2", b"v2")?;
  enqueue_upsert_at(log, 3, 1, b"safe", b"vs")?;
  // 主库 flush_namespace(逻辑 ns 9) 换号批镜像：旧空间 vns 5 判死、逻辑 ns 9
  // 换指新空间 vns 6、水位抬到 7
  enqueue_dbmeta_mirror(
    log,
    &[
      DbMetaRecord::NsMap {
        logic_ns: 9,
        vns: 6,
      },
      DbMetaRecord::GcDeadNs {
        expired_at: 1,
        old_vns: 5,
        tail_address: 0,
      },
      DbMetaRecord::NextId { next_virtual_id: 7 },
    ],
  )?;
  // 非 0 租户 FLUSHALL 清库执行段：整 ns 换号后补写 FlushNs(旧 vns=5)（屏障）
  let _ = log.enqueue_safe_flush_aof(AofEntryType::FlushNs, false, 5, 0)?;
  log.commit();

  let (_rdir, rstore) = open_test_store("flush-ns-replica.db")?;
  replica_replay(&rstore, &aof).await?;

  let probe = rstore.new_session()?;
  probe.set_virtual_context(5, 1, 5, 1);
  assert_eq!(
    probe.read(b"n1").await?,
    Some(b"v1".to_vec()),
    "n1 须落回其条目物理域 (5, 1)"
  );
  probe.set_virtual_context(5, 2, 5, 2);
  assert_eq!(
    probe.read(b"n2").await?,
    Some(b"v2".to_vec()),
    "n2 须落回其条目物理域 (5, 2)"
  );
  assert!(
    rstore.vdb.is_dead_ns(5),
    "0x03 空间退役墓碑镜像条目须把旧空间判死"
  );
  assert!(
    rstore.vdb.is_dead_domain(5, 1) && rstore.vdb.is_dead_domain(5, 2),
    "退役空间内的条目域整体判死"
  );
  probe.set_virtual_context(3, 1, 3, 1);
  assert_eq!(
    probe.read(b"safe").await?,
    Some(b"vs".to_vec()),
    "ns 3 域不受 FlushNs 波及"
  );
  assert!(
    !rstore.vdb.is_dead_domain(3, 1) && !rstore.vdb.is_dead_ns(3),
    "他空间不得判死（退役墓碑只记载荷旧 vns）"
  );
  // 回放侧零二次映射：映射面逐值等于镜像换号批值（条目物理号 vns 3/5 不
  // 物化为逻辑租户）
  assert_eq!(
    mapping_face(&rstore),
    MappingFace {
      logic_ns: vec![0, 9],
      routing_vns: vec![0],
      water_mark: 7,
    },
    "FlushNs 回放侧映射面须逐值等于主库镜像值（本地取号器零触发）"
  );
  assert_eq!(
    rstore.vdb.vns_of_ns(9),
    Some(6),
    "换号后逻辑空间须直指主库新空间号（锁步同号）"
  );
  OK
}

/// FlushAll 广播全清 + 主库门控：is_primary = false 不入队（副本清库经回放
/// 条目承接，绝不二次入队自激放大）
#[compio::test]
async fn test_flush_all_replay_and_primary_gate() -> Void {
  let (_dir, _store) = open_test_store("flush-all.db")?;
  let aof = aof_fixture("flush_all")?;
  let log = aof.log();

  enqueue_upsert_at(log, 3, 7, b"doomed", b"v")?;
  enqueue_upsert_at(log, 5, 1, b"doomed2", b"v")?;
  let _ = log.enqueue_safe_flush_aof(AofEntryType::FlushAll, false, 0, 0)?;
  enqueue_upsert_at(log, 3, 7, b"reborn", b"v")?;
  log.commit();

  let (_rdir, rstore) = open_test_store("flush-all-replica.db")?;
  replica_replay(&rstore, &aof).await?;

  // FlushAll 为物理截断（非换号退役）：同域清前条目灭迹、清后条目原地可读
  let probe = rstore.new_session()?;
  probe.set_virtual_context(3, 7, 3, 7);
  assert_eq!(probe.read(b"doomed").await?, None, "全域清空");
  assert_eq!(
    probe.read(b"reborn").await?,
    Some(b"v".to_vec()),
    "FlushAll 后新写可见"
  );
  probe.set_virtual_context(5, 1, 5, 1);
  assert_eq!(probe.read(b"doomed2").await?, None, "全域清空");

  // 主库门控：副本角色不入队
  let tail_before = log.tail_address();
  aof.enqueue_safe_flush_aof_if_primary(false, AofEntryType::FlushAll, false, 0, 0)?;
  assert_eq!(
    log.tail_address(),
    tail_before,
    "副本角色绝不二次入队 FLUSH 条目"
  );
  aof.enqueue_safe_flush_aof_if_primary(true, AofEntryType::FlushAll, false, 0, 0)?;
  assert_ne!(log.tail_address(), tail_before, "主库角色入队 FLUSH 条目");
  OK
}

/// 主路径漏斗闭环（RESP FLUSHDB → SingleDatabaseManager 换号 + 广播条目）：
/// 慢路径经常驻 manager 一处漏斗执行（对标 C# ExecuteFlushDb →
/// storeWrapper.FlushDatabase → databaseManager.FlushDatabase + SafeFlushAOF），
/// AOF 尾部出现 FlushDb 广播条目（单机形态 is_primary 恒真入队），主库旧域
/// 读不到，从库回放承接换号
#[compio::test]
async fn test_flush_db_resp_funnel_broadcast() -> Void {
  let (dir, store) = open_test_store("flush-funnel.db")?;
  let aof = aof_fixture("flush_funnel")?;
  let cp_dir = dir.path().join("cp");
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&store),
    Arc::clone(&store.device),
    cp_dir.clone(),
    Some(Arc::clone(&aof)),
  ));
  let mgr = Arc::new(SingleDatabaseManager::new(cp_dir, db));
  let api = StoreGarnetApi::new(store.new_session()?).with_database_manager(mgr);

  // 主库旧域写键（写会话直写；广播验证聚焦 FlushDb 条目，数据条目不经
  // 独立 aof_fixture——store 事件流未接线）
  let writer = store.new_session()?;
  writer.set_context(0, 0);
  writer.upsert(b"doomed", b"v").await?;

  // RESP FLUSHDB 慢路径 → manager 换号 + FlushDb 条目入队 → +OK
  //（单 sublog 拓扑，尾地址取 0 号槽标量比较）
  let tail_before = aof.log().tail_address()[0];
  let out = Arc::new(api)
    .exec_slow(RespCommand::Flushdb, vec![], wconf::DEFAULT_RESP_VERSION)
    .await;
  assert_eq!(&out[..], b"+OK\r\n", "FLUSHDB 经漏斗成功");
  assert!(
    aof.log().tail_address()[0] > tail_before,
    "AOF 尾部必须出现 FlushDb 广播条目"
  );

  // 主库清库生效：换号后旧域键读不到
  let probe = store.new_session()?;
  probe.set_context(0, 0);
  assert_eq!(probe.read(b"doomed").await?, None, "换号后旧域清空");

  // 从库全量回放：FlushDb 条目被回放臂消费（域载荷 16B 完整可解析，
  // 空域换号幂等）
  let (_rdir, rstore) = open_test_store("flush-funnel-replica.db")?;
  replica_replay(&rstore, &aof).await?;
  OK
}

/// 单命令 TCP 往返（行式应答文案）
async fn call(stream: &mut TcpStream, args: &[&[u8]]) -> Vec<u8> {
  wnode_test::send_cmd(stream, args).await.expect("send");
  wnode_test::read_line_reply(stream).await
}

/// RESP 主路径清库 → 日志 FlushDb 广播条目 → 副本回放跟随（生产端到端）
///
/// 真装配（`StorageSessionProvider::open_with_config_and_aof`：AOF 写监听端口
/// 与常驻管理面及网络泵）下复核本条修复判据：FLUSHDB 执行段必须在日志落
/// FlushDb 条目（清库唯一漏斗 = SingleDatabaseManager，store 直调面只写
/// KeyTag::DbMeta 映射记录、被写端口按标签滤除，副本永无换号条目），且载荷
/// 域值为换号前的 (vns, 旧 vdb)——第二次清库的载荷库号是第一次换号后的新
/// 虚拟号而非会话逻辑库号 0（取错即副本永远清不掉当前域）；副本全量回放后
/// 旧域读路径不可见、清后新域可见。
#[compio::test]
async fn test_resp_flushdb_broadcast_entry_end_to_end() -> Void {
  let dir = tempfile::tempdir()?;
  let provider = Arc::new(StorageSessionProvider::open_with_config_and_aof(
    test_store_config(),
    dir.path().join("node").join("flush_e2e.db"),
    None,
    RuntimeServerOptions::default(),
    wnode_test::session_factory,
  )?);
  let (server, addr) = wnode_test::start_server(Arc::clone(&provider));
  let mut stream = TcpStream::connect(addr).await?;

  // 写 → 清 → 写 → 清 → 写：三轮各自的虚拟域号在日志读端复核
  assert_eq!(call(&mut stream, &[b"SET", b"a", b"v"]).await, b"+OK\r\n");
  assert_eq!(call(&mut stream, &[b"FLUSHDB"]).await, b"+OK\r\n");
  assert_eq!(call(&mut stream, &[b"SET", b"b", b"v"]).await, b"+OK\r\n");
  assert_eq!(call(&mut stream, &[b"FLUSHDB"]).await, b"+OK\r\n");
  assert_eq!(call(&mut stream, &[b"SET", b"c", b"v"]).await, b"+OK\r\n");

  // ── 日志读端：FlushDb 条目按地址序载荷 = (vns, 换号前旧 vdb) ──
  let aof = Arc::clone(provider.aof().expect("aof enabled"));
  aof.log().commit();
  let mut domains: Vec<(u64, u64)> = Vec::new();
  aof.log().scan_single_with(0, 0, i64::MAX, |rec| {
    if !waof::is_commit_frame(&rec.payload)
      && AofHeader::parse(&rec.payload).is_some_and(|h| h.op_type == AofEntryType::FlushDb as u8)
    {
      domains.push(parse_flush_domain(&rec.payload).expect("FlushDb 条目域载荷可读"));
    }
    true
  });
  assert_eq!(
    domains,
    vec![(0, 0), (0, 1)],
    "两次 FLUSHDB 须各落一条 FlushDb 条目，载荷库号为各自换号前的旧虚拟库号"
  );

  // ── 副本回放段：换号锁步 + 逻辑入口末态判读 ──
  // 条目载荷是物理域 (vns, 换号前旧 vdb)，而本副本 store 与主库同起点（根租户
  // 逻辑库 0 即 (0, 0)，两侧取号顺序同），故副本侧同一逻辑入口经锁步换号后
  // 解析到的正是主库末态物理域——清前两条落进的旧域经该入口不可达
  let (_rdir, rstore) = open_test_store("flush-e2e-replica.db")?;
  replica_replay(&rstore, &aof).await?;
  assert_eq!(
    rstore.vdb.route_vdb_of(0, 0),
    Some(domains[1].1 + 1),
    "副本两次换号须与主库锁步同号（格内末态 = 第二次换出的新虚拟库号）"
  );
  let probe = rstore.new_session()?;
  assert!(probe.set_context(0, 0), "根租户逻辑库 0 入口在册");
  assert_eq!(
    probe.read(b"a").await?,
    None,
    "第一次 FlushDb 换号后其旧域 (0, 0) 经逻辑入口不可达"
  );
  assert_eq!(
    probe.read(b"b").await?,
    None,
    "第二次 FlushDb 换号后其旧域 (0, 1) 同样不可达"
  );
  assert_eq!(
    probe.read(b"c").await?,
    Some(b"v".to_vec()),
    "清后主库新号域的写入须在副本可见"
  );
  // 换号条目把两个旧域逐格投进死亡账本，新域不判死
  assert!(
    rstore.vdb.is_dead_domain(0, domains[0].1) && rstore.vdb.is_dead_domain(0, domains[1].1),
    "两次 FlushDb 的载荷旧域均须判死"
  );
  assert!(
    !rstore.vdb.is_dead_domain(0, domains[1].1 + 1),
    "清后新域不得判死"
  );

  drop(stream);
  drop(server);
  OK
}

/// 辅助装配：创建分段存储实例并在段 0 与段 1 填充数据，全量落盘后返回句柄
async fn setup_segmented_replica(
  tag: &str,
  seg_size: u64,
) -> aok::Result<(
  tempfile::TempDir,
  Arc<wkv::WedbStore<wdev::SegmentedDevice>>,
  Arc<wdev::SegmentedDevice>,
)> {
  let dir = tempfile::tempdir()?;
  let device = Arc::new(SegmentedDevice::new(
    dir.path().join(format!("{tag}.log")),
    seg_size,
    DEFAULT_SECTOR_SIZE,
  )?);
  let mut config = test_store_config();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device))?);
  let session = store.new_session()?;
  for i in 0..120 {
    session
      .upsert(
        format!("pad_{i}").as_bytes(),
        b"0123456789012345678901234567890123456789012345678901234567890123456789",
      )
      .await?;
  }
  store.flush_all().await?;
  assert!(store.tail_address() > seg_size);
  assert!(device.segment_path(0).exists());
  Ok((dir, store, device))
}

/// UNSAFETRUNCATELOG 标志在 FlushAll AOF 条目回放时触发 store.truncate() 物理截断：
/// 1. 验证带 unsafe_truncate_log=true 的 FlushAll 广播条目在回放时成功执行，
///    断言 begin_address 推进至 tail，且在删段地板抬升下段 0 被物理删除；
/// 2. 普通无 flag 的 FlushAll 行为保持回归锁定（原有全库清空与主库门控行为不变）。
#[compio::test]
async fn test_unsafe_truncate_log_flush_all_replay() -> Void {
  let seg_size = 2 * DEFAULT_SECTOR_SIZE as u64;

  let aof_trunc = aof_fixture("flush_all_trunc_active")?;
  aof_trunc
    .log()
    .enqueue_safe_flush_aof(AofEntryType::FlushAll, true, 0, 0)?;
  aof_trunc.log().commit();

  let (_dir, store, dev) = setup_segmented_replica("r_all_trunc", seg_size).await?;
  let tail_before = store.tail_address();
  store.hlog().raise_delete_floor(seg_size);

  replica_replay(&store, &aof_trunc).await?;

  // 校验 begin_address 推进至尾且段 0 被物理删除
  assert!(store.begin_address() >= tail_before);
  assert!(
    !dev.segment_path(0).exists(),
    "unsafe_truncate_log=true FlushAll 回放须触发 store.truncate() 物理删除段 0"
  );

  OK
}

/// UNSAFETRUNCATELOG 标志在 FlushDb AOF 条目回放时触发 store.truncate() 物理截断：
/// 对比普通 FlushDb 与带 unsafe_truncate_log 的 FlushDb 回放行为。
#[compio::test]
async fn test_unsafe_truncate_log_flush_db_triggers_store_truncate() -> Void {
  let seg_size = 2 * DEFAULT_SECTOR_SIZE as u64;

  // AOF 1: 普通 FlushDb (unsafe_truncate_log = false)
  let aof_normal = aof_fixture("flush_db_trunc_normal")?;
  aof_normal
    .log()
    .enqueue_safe_flush_aof(AofEntryType::FlushDb, false, 0, 0)?;
  aof_normal.log().commit();

  let (_dir1, store1, dev1) = setup_segmented_replica("r_db_normal", seg_size).await?;
  store1.shift_begin_address(seg_size).await?;
  store1.hlog().raise_delete_floor(seg_size);
  replica_replay(&store1, &aof_normal).await?;
  assert!(
    dev1.segment_path(0).exists(),
    "unsafe_truncate_log=false 回放不得删除段 0"
  );

  // AOF 2: 带 UNSAFETRUNCATELOG 的 FlushDb (unsafe_truncate_log = true)
  let aof_trunc = aof_fixture("flush_db_trunc_active")?;
  aof_trunc
    .log()
    .enqueue_safe_flush_aof(AofEntryType::FlushDb, true, 0, 0)?;
  aof_trunc.log().commit();

  let (_dir2, store2, dev2) = setup_segmented_replica("r_db_trunc", seg_size).await?;
  store2.shift_begin_address(seg_size).await?;
  store2.hlog().raise_delete_floor(seg_size);
  replica_replay(&store2, &aof_trunc).await?;
  assert!(
    !dev2.segment_path(0).exists(),
    "unsafe_truncate_log=true 回放须触发 store.truncate() 删除段 0"
  );

  OK
}

/// UNSAFETRUNCATELOG 标志在 FlushNs AOF 条目回放时触发 store.truncate() 物理截断：
/// 对比普通 FlushNs 与带 unsafe_truncate_log 的 FlushNs 回放行为。
#[compio::test]
async fn test_unsafe_truncate_log_flush_ns_triggers_store_truncate() -> Void {
  let seg_size = 2 * DEFAULT_SECTOR_SIZE as u64;

  // AOF 1: 普通 FlushNs (unsafe_truncate_log = false)
  let aof_normal = aof_fixture("flush_ns_trunc_normal")?;
  aof_normal
    .log()
    .enqueue_safe_flush_aof(AofEntryType::FlushNs, false, 5, 0)?;
  aof_normal.log().commit();

  let (_dir1, store1, dev1) = setup_segmented_replica("r_ns_normal", seg_size).await?;
  store1.shift_begin_address(seg_size).await?;
  store1.hlog().raise_delete_floor(seg_size);
  replica_replay(&store1, &aof_normal).await?;
  assert!(
    dev1.segment_path(0).exists(),
    "unsafe_truncate_log=false 回放不得删除段 0"
  );

  // AOF 2: 带 UNSAFETRUNCATELOG 的 FlushNs (unsafe_truncate_log = true)
  let aof_trunc = aof_fixture("flush_ns_trunc_active")?;
  aof_trunc
    .log()
    .enqueue_safe_flush_aof(AofEntryType::FlushNs, true, 5, 0)?;
  aof_trunc.log().commit();

  let (_dir2, store2, dev2) = setup_segmented_replica("r_ns_trunc", seg_size).await?;
  store2.shift_begin_address(seg_size).await?;
  store2.hlog().raise_delete_floor(seg_size);
  replica_replay(&store2, &aof_trunc).await?;
  assert!(
    !dev2.segment_path(0).exists(),
    "unsafe_truncate_log=true 回放须触发 store.truncate() 删除段 0"
  );

  OK
}

/// 发现三（P1）闭环测试：safe_flush_aof 入队失败时 FlushDb/FlushNs 广播条目补偿与不丢失
///
/// 场景：
/// 1. 注入 AOF 入队失败，首次 flush_database 换号完成但广播条目入队失败并报错
/// 2. 验证失败条目被登记到 pending_flush_aof 账本中（不永久丢失）
/// 3. 第二次 flush_database 执行成功，自动清偿补偿先前未入队的广播条目
/// 4. 验证 AOF 按序包含两次换号的 FlushDb 广播条目（两次换号前的旧 vdb 均未丢失）
/// 5. 再次对 flush_namespace 注入故障，验证经 drain_pending_flush_aof 亦可成功补偿 FlushNs
/// 6. 副本全量回放，验证两次换号的旧库域与命名空间均被屏障并在死亡账本中判死
#[compio::test]
async fn test_safe_flush_aof_failure_retry_and_compensation() -> Void {
  let (dir, store) = open_test_store("flush-retry-comp.db")?;
  let aof = aof_fixture("flush_retry_comp")?;
  let cp_dir = dir.path().join("cp");
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&store),
    Arc::clone(&store.device),
    cp_dir.clone(),
    Some(Arc::clone(&aof)),
  ));
  let mgr = Arc::new(SingleDatabaseManager::new(cp_dir, db));

  // 预热并写入初始键
  let session = store.new_session()?;
  session.set_context(0, 0);
  session.upsert(b"k1", b"v1").await?;
  let vdb_0 = store.vdb.route_vdb_of(0, 0).expect("logic db 0 必已映射");

  // 步骤 1：注入 safe_flush_aof 故障，首次 flush_database 换号成功但广播条目入队失败
  mgr.set_enqueue_fault_injected(true);
  let res = mgr.flush_database(0, 0, false).await;
  assert!(res.is_err(), "注入故障后 flush_database 必须返回错误");
  assert_eq!(
    mgr.pending_flush_aof_count(),
    1,
    "失败条目必须被暂存到待补投账本中"
  );

  // 换号已在内存与磁盘落定，逻辑库 0 换到了 vdb_1
  let vdb_1 = store.vdb.route_vdb_of(0, 0).expect("已换新号");
  assert_ne!(vdb_0, vdb_1, "虚拟库号已换号推进");

  // 在新号域写入第二笔数据
  let session2 = store.new_session()?;
  session2.set_context(0, 0);
  session2.upsert(b"k2", b"v2").await?;

  // 步骤 2：第二次 flush_database（无故障），必须自动补投先前残留的广播条目
  mgr.flush_database(0, 0, false).await?;
  assert_eq!(
    mgr.pending_flush_aof_count(),
    0,
    "成功执行后待补投账本必须被排空"
  );
  let vdb_2 = store.vdb.route_vdb_of(0, 0).expect("已换第二轮新号");

  // 步骤 3：验证 flush_namespace 的入队失败与 drain 补投
  let session_ns = store.new_session()?;
  session_ns.set_context(5, 0);
  session_ns.upsert(b"ns_k", b"ns_v").await?;
  let old_vns = store.vdb.vns_of_ns(5).expect("ns 5 必在册");

  mgr.set_enqueue_fault_injected(true);
  let res_ns = mgr.flush_namespace(5, false).await;
  assert!(res_ns.is_err(), "注入故障后 flush_namespace 必须返回错误");
  assert_eq!(
    mgr.pending_flush_aof_count(),
    1,
    "flush_namespace 失败条目必须记录在 pending 账本"
  );

  // 显式触发 drain_pending_flush_aof 补投
  mgr.drain_pending_flush_aof()?;
  assert_eq!(
    mgr.pending_flush_aof_count(),
    0,
    "drain 后 pending 账本必须清零"
  );

  // 步骤 4：复核 AOF 尾部按序出现的 FlushDb 与 FlushNs 广播条目
  aof.log().commit();
  let mut flush_db_domains: Vec<(u64, u64)> = Vec::new();
  let mut flush_ns_domains: Vec<(u64, u64)> = Vec::new();
  aof.log().scan_single_with(0, 0, i64::MAX, |rec| {
    if !waof::is_commit_frame(&rec.payload)
      && let Some(header) = AofHeader::parse(&rec.payload)
    {
      if header.op_type == AofEntryType::FlushDb as u8 {
        flush_db_domains.push(parse_flush_domain(&rec.payload).expect("FlushDb 载荷可读"));
      } else if header.op_type == AofEntryType::FlushNs as u8 {
        flush_ns_domains.push(parse_flush_domain(&rec.payload).expect("FlushNs 载荷可读"));
      }
    }
    true
  });

  assert_eq!(
    flush_db_domains,
    vec![(0, vdb_0), (0, vdb_1)],
    "AOF 必须按序包含两次换号的 FlushDb 广播条目，第一次失败的旧域未丢失"
  );
  assert_eq!(
    flush_ns_domains,
    vec![(old_vns, 0)],
    "AOF 必须包含补偿入队的 FlushNs 广播条目"
  );

  // 步骤 5：副本回放承接广播屏障
  let (_rdir, rstore) = open_test_store("flush-retry-comp-replica.db")?;
  replica_replay(&rstore, &aof).await?;

  // 副本回放后，FlushDb 屏障把两次换号的旧域均判死
  assert!(
    rstore.vdb.is_dead_domain(0, vdb_0),
    "第一次换号的旧域在副本死亡账本中判死"
  );
  assert!(
    rstore.vdb.is_dead_domain(0, vdb_1),
    "第二次换号的旧域在副本死亡账本中判死"
  );
  assert!(
    !rstore.vdb.is_dead_domain(0, vdb_2),
    "清后当前在用新域不得判死"
  );

  // FlushNs 屏障把旧命名空间判死
  assert!(
    rstore.vdb.is_dead_ns(old_vns),
    "补偿入队的 FlushNs 广播条目使副本旧空间判死"
  );

  OK
}

/// 主备回放形换号窗 WATCH 中止闭环（票 task/ing/wtxn-watch-version-slot-
/// freeze-after-swapnum.md 方案 4「主备回放形」）：副本侧在途 WATCH（逻辑域
/// (0,0) 登记）横跨回放窗——主库 FLUSHDB 换号批镜像（DbMap 0→8 + 旧域 7 判死
/// + 水位）→ FlushDb 屏障 → 清后新代域 (0, 8) 改写同逻辑键；副本回放经
/// KeyContextGuard 直设物理域并显式透传 version_domain_of 换算的逻辑入账域，
/// bump 必落版本轨逻辑槽 (0,0) → EXEC 中止。修复前回放 bump 落物理 (0,8) 槽、
/// 逻辑登记槽永不受触 → 假通过（乐观锁静默丢失副本侧复现）。
#[compio::test]
async fn test_replica_replay_swapnum_aborts_inflight_watch() -> Void {
  use std::time::Duration;

  use wnode::storage::session::storage_session::version_map_watch_hook;
  use wtxn::{TransactionManager, TxnLockTable, WatchVersionMap};
  use wval::SessionPrefixBuf;

  let (_dir, _store) = open_test_store("swapwatch.db")?;
  let aof = aof_fixture("swapwatch")?;
  let log = aof.log();

  // 主库 FLUSHDB(0,0) 换号批镜像：逻辑根库换指新号 8、旧域 (0, 7) 判死
  enqueue_dbmeta_mirror(
    log,
    &[
      DbMetaRecord::DbMap {
        vns: 0,
        logic_db: 0,
        vdb: 8,
      },
      DbMetaRecord::GcDeadDb {
        expired_at: 1,
        vns: 0,
        old_vdb: 7,
        tail_address: 0,
      },
      DbMetaRecord::NextId { next_virtual_id: 9 },
    ],
  )?;
  let _ = log.enqueue_safe_flush_aof(AofEntryType::FlushDb, false, 0, 7)?;
  // 清后主库按新代虚拟号 (0, 8) 改写同逻辑键
  enqueue_upsert_at(log, 0, 8, b"sw:k", b"v2")?;
  log.commit();

  // 副本：挂版本表钩子后全量回放；在途 WATCH 先于回放登记（逻辑根域种子）
  let (_rdir, rstore) = open_test_store("swapwatch-replica.db")?;
  let map = Arc::new(WatchVersionMap::new(1 << 10));
  rstore.set_watch_hook(version_map_watch_hook(Arc::clone(&map)));
  let mut txn = TransactionManager::new(TxnLockTable::new(), Arc::clone(&map), None);
  txn.watch(SessionPrefixBuf::ROOT.as_slice(), b"sw:k");

  replica_replay(&rstore, &aof).await?;

  let exec_ok = txn.run(
    SessionPrefixBuf::ROOT.as_slice(),
    false,
    true,
    Duration::ZERO,
  );
  assert!(
    !exec_ok,
    "副本回放换号后同逻辑键改写必须使在途 WATCH EXEC 中止（版本轨=逻辑域透传闭环）"
  );
  // 回放落位旁证：逻辑入口读回清后新值
  let probe = rstore.new_session()?;
  assert!(probe.set_context(0, 0), "逻辑根库入口可物化");
  assert_eq!(
    probe.read(b"sw:k").await?,
    Some(b"v2".to_vec()),
    "回放条目须落新代域且经逻辑入口可读"
  );
  OK
}
