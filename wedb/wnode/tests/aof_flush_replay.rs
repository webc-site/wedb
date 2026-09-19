//! FLUSH 族 AOF 条目生产 → 回放闭环集成测试（主从换号复制链路）
//!
//! 覆盖：enqueue_safe_flush_aof 域载荷 [ns: u64 LE][db: u64 LE] 经回放对称
//! 解析（u64 全宽无 u8 截断）、FlushDb/FlushNs 条目从库回放按**物理域**退役
//! （条目落回自身前缀、载荷旧域判死、他域完好、清后新写可见、映射面零改动）、主库门控（is_primary = false 不入队）、
//! FlushAll 广播全清、RESP 主路径经清库唯一漏斗真生产广播条目端到端闭环。
//!
//! 对标 C#：SingleDatabaseManager.cs:SafeFlushAOF（清库执行段原子补写广播
//! 条目）、BasicCommands.cs:ExecuteFlushDb（清库唯一入口 = 管理面）、
//! AofProcessor.cs:ReplayAOF（FlushAll/FlushDb 回放臂）。

use std::sync::Arc;

use aok::{OK, Void};
use compio::{net::TcpStream, runtime::Runtime};
use waof::{AofEntryType, AofHeader, AofHeaderType};
use wconf::RuntimeServerOptions;
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
    store_version: 0,
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
/// 若经 C# databaseId 1 字节形态必截断为 0x08；回放臂按**物理域**退役条目载荷
/// 域，退役墓碑因此记在全宽 vns 上（`GcDeadEntry::vns`），截断形态只能落成 8。
///
/// 探针一律经物理域直设（回放面唯一口径，见 `KeyContextGuard` 文档）：条目键
/// 前缀即入账会话当时的虚拟号，从库绝不本地二次映射——在无继承映射的全新副本
/// 上拿逻辑入口 `set_context` 判读只会盲分配新号，读到的从来不是条目域。
#[test]
fn test_flush_entry_payload_u64_domain() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let vns: u64 = 0x0102_0304_0506_0708;
    let (_dir, _store) = open_test_store("flush-payload.db")?;
    let aof = aof_fixture("flush_payload")?;
    let log = aof.log();

    enqueue_upsert_at(log, vns, 42, b"hit", b"v")?;
    enqueue_upsert_at(log, 8, 43, b"decoy", b"v")?;
    let _ = log.enqueue_safe_flush_aof(AofEntryType::FlushDb, true, vns, 42)?;
    log.commit();

    let (_rdir, rstore) = open_test_store("flush-payload-replica.db")?;
    let face_before = mapping_face(&rstore);
    replica_replay(&rstore, &aof).await?;

    // 条目逐部落回自身物理前缀，他域条目不被错域退役波及
    let probe = rstore.new_session()?;
    probe.set_virtual_context(8, 43);
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
      .expect("FlushDb 条目须落旧域退役墓碑");
    assert_eq!(
      dead.vns,
      Some(vns),
      "墓碑所属虚拟命名空间须等于条目载荷全宽值（截断即落 8）"
    );
    assert!(
      rstore.vdb.is_dead_domain(vns, 42),
      "载荷域 (全宽 vns, 42) 须判死"
    );
    // 回放侧零二次映射：条目物理号绝不物化为逻辑号
    assert_eq!(
      mapping_face(&rstore),
      face_before,
      "回放侧映射面（租户表 / 路由表 / 分配水位）须零改动"
    );
    OK
  })
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
/// FlushDb(3, 7) 条目 → 清后新号域 (3, 8) 写入条目。副本侧每条条目严格落回
/// 自身物理前缀（物理镜像承诺）、载荷旧域 (3, 7) 判死、他租户域 (5, 1) 完好，
/// 且映射面全程零改动（从库继承主库映射体系，绝不本地二次映射）。
///
/// 「旧域经逻辑入口不可达 + 副本换号与主库锁步同号」须真映射继承形态方能判读
///（无继承映射的副本上逻辑入口只会盲分配新号），见
/// `aof_replay_domain.rs::tail_flushdb_replay_swaps_inherited_domain`。
#[test]
fn test_flush_db_replica_replays_entry_domains_without_local_remap() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, _store) = open_test_store("flush-follow.db")?;
    let aof = aof_fixture("flush_follow")?;
    let log = aof.log();

    // ── 主库生产段（数据条目 key 前缀 = 主库虚拟 ID 对）──
    enqueue_upsert_at(log, 3, 7, b"k1", b"v1")?;
    enqueue_upsert_at(log, 3, 7, b"k2", b"v2")?;
    enqueue_upsert_at(log, 5, 1, b"keep", b"vk")?;
    // FLUSHDB 清库执行段：主库换号后原子补写 FlushDb(3, 7)
    let _ = log.enqueue_safe_flush_aof(AofEntryType::FlushDb, false, 3, 7)?;
    // 清后主库按新虚拟号 (3, 8) 写入
    enqueue_upsert_at(log, 3, 8, b"fresh", b"vf")?;
    log.commit();

    // ── 从库回放段 ──
    let (_rdir, rstore) = open_test_store("flush-follow-replica.db")?;
    let face_before = mapping_face(&rstore);
    replica_replay(&rstore, &aof).await?;

    // 条目落回自身物理域，载荷旧域判死，他租户完好
    let probe = rstore.new_session()?;
    probe.set_virtual_context(3, 8);
    assert_eq!(
      probe.read(b"fresh").await?,
      Some(b"vf".to_vec()),
      "清后新号域条目须落回其自身物理域 (3, 8)"
    );
    assert!(
      rstore.vdb.is_dead_domain(3, 7),
      "FlushDb 条目须把载荷旧域 (3, 7) 投进本地 GC 死亡账本"
    );
    assert!(!rstore.vdb.is_dead_domain(3, 8), "清后新域 (3, 8) 不得判死");
    probe.set_virtual_context(5, 1);
    assert_eq!(
      probe.read(b"keep").await?,
      Some(b"vk".to_vec()),
      "他租户域不受 FlushDb 波及"
    );
    assert!(!rstore.vdb.is_dead_domain(5, 1), "他租户域不得判死");
    // 回放侧零二次映射：条目物理号绝不物化为逻辑库/租户
    assert_eq!(
      mapping_face(&rstore),
      face_before,
      "无继承映射的副本回放侧映射面须零改动"
    );
    OK
  })
}

/// FlushNs 回放闭环（物理域口径）：ns 5 域两库数据条目 → FlushNs(旧 vns=5)
/// 条目 → 载荷空间判死（其内条目域再无在册逻辑入口），他空间 (3, 1) 条目与
/// 存续态完好，副本映射面零改动
#[test]
fn test_flush_ns_replica_replay() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, _store) = open_test_store("flush-ns.db")?;
    let aof = aof_fixture("flush_ns")?;
    let log = aof.log();

    enqueue_upsert_at(log, 5, 1, b"n1", b"v1")?;
    enqueue_upsert_at(log, 5, 2, b"n2", b"v2")?;
    enqueue_upsert_at(log, 3, 1, b"safe", b"vs")?;
    // 非 0 租户 FLUSHALL 清库执行段：整 ns 换号后补写 FlushNs(旧 vns=5)
    let _ = log.enqueue_safe_flush_aof(AofEntryType::FlushNs, false, 5, 0)?;
    log.commit();

    let (_rdir, rstore) = open_test_store("flush-ns-replica.db")?;
    let face_before = mapping_face(&rstore);
    replica_replay(&rstore, &aof).await?;

    let probe = rstore.new_session()?;
    probe.set_virtual_context(5, 1);
    assert_eq!(
      probe.read(b"n1").await?,
      Some(b"v1".to_vec()),
      "n1 须落回其条目物理域 (5, 1)"
    );
    probe.set_virtual_context(5, 2);
    assert_eq!(
      probe.read(b"n2").await?,
      Some(b"v2".to_vec()),
      "n2 须落回其条目物理域 (5, 2)"
    );
    assert!(rstore.vdb.is_dead_ns(5), "FlushNs 条目须把旧空间判死");
    assert!(
      rstore.vdb.is_dead_domain(5, 1) && rstore.vdb.is_dead_domain(5, 2),
      "退役空间内的条目域整体判死"
    );
    probe.set_virtual_context(3, 1);
    assert_eq!(
      probe.read(b"safe").await?,
      Some(b"vs".to_vec()),
      "ns 3 域不受 FlushNs 波及"
    );
    assert!(
      !rstore.vdb.is_dead_domain(3, 1) && !rstore.vdb.is_dead_ns(3),
      "他空间不得判死（退役墓碑只记载荷旧 vns）"
    );
    assert_eq!(
      mapping_face(&rstore),
      face_before,
      "FlushNs 回放侧映射面须零改动（条目物理号不得物化为逻辑租户）"
    );
    OK
  })
}

/// FlushAll 广播全清 + 主库门控：is_primary = false 不入队（副本清库经回放
/// 条目承接，绝不二次入队自激放大）
#[test]
fn test_flush_all_replay_and_primary_gate() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
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
    probe.set_virtual_context(3, 7);
    assert_eq!(probe.read(b"doomed").await?, None, "全域清空");
    assert_eq!(
      probe.read(b"reborn").await?,
      Some(b"v".to_vec()),
      "FlushAll 后新写可见"
    );
    probe.set_virtual_context(5, 1);
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
  })
}

/// 主路径漏斗闭环（RESP FLUSHDB → SingleDatabaseManager 换号 + 广播条目）：
/// 慢路径经常驻 manager 一处漏斗执行（对标 C# ExecuteFlushDb →
/// storeWrapper.FlushDatabase → databaseManager.FlushDatabase + SafeFlushAOF），
/// AOF 尾部出现 FlushDb 广播条目（单机形态 is_primary 恒真入队），主库旧域
/// 读不到，从库回放承接换号
#[test]
fn test_flush_db_resp_funnel_broadcast() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
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
  })
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
#[test]
fn test_resp_flushdb_broadcast_entry_end_to_end() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let provider = Arc::new(StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      dir.path().join("node").join("flush_e2e.db"),
      None,
      None,
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
  })
}
