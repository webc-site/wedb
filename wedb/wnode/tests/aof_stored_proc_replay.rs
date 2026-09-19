//! AOF 集成回放测试（自 src/aof/aof_processor.rs 内嵌测试迁出）
//!
//! compio Runtime + 临时目录真存储全链回放：入队 → recover 扫描 → 处理器
//! 分发 → 存储过程工厂重建 / 目标库清理。
//! 对标 C# libs/server/AOF/AofProcessor.cs：ReplayStoredProc →
//! StoredProcRunnerBase → RunCustomTxnProcAtReplica，与 ReplayAOF case
//! FlushDb → storeWrapper.FlushDatabase(unsafeTruncateLog, dbId)。

use std::{
  env::temp_dir,
  fs::{create_dir_all, remove_dir_all},
  path::Path,
  process::id,
  sync::Arc,
};

use aok::{OK, Void};
use waof::AofEntryType;
use wconf::RuntimeServerOptions;
use wcustom::txn_proc_slot;
use wdev::SegmentedDevice;
use wkv::{DbMetaRecord, WedbStore};
use wnode::{
  aof::{
    aof_processor::{AofProcessor, ReplayTarget},
    garnet_append_only_file::GarnetAppendOnlyFile,
    garnet_log::{GarnetLog, RecordShape},
    recover::aof_recover::AofRecover,
    replaycoordinator::stored_proc_replay::{StoredProcRegistryReplayer, stored_proc_args},
  },
  storage::StorageSession,
};
use wtest_base::test_store_config;
use wtxn::{SublogAccess, TxnLockTable};
use wval::{KeyTag, NamespaceDbCodec};

/// 主库换号事务的 DbMeta 镜像条目入队（生产形态复刻：`WedbStore::commit_swap`
/// 原子批 `[新映射, 旧域退役墓碑?, 0x05 分配水位]` 逐条落盘，每条
/// `KeyTag::DbMeta` 记录经 `service.rs:on_aof_store_event` 放行的写端口镜像为
/// 一条 StoreUpsert 条目；键 = 根域前缀 (0,0) + DbMeta 标签 + 记录键载荷、值 =
/// 定长记录值，布局单点 `wkv::DbMetaRecord`）
///
/// 对标 C# 无对位（单租户每库独立 store，无虚拟域映射可镜像）；本仓对位口径见
/// doc/zh/db.md「主从物理镜像与异步屏障」：「从库完全继承主库的映射体系，不
/// 进行本地二次映射」，FlushDb / FlushNs 条目只作屏障
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

/// 内存 AOF（单物理日志拓扑；回放形状断言用）
fn memory_aof() -> Arc<GarnetAppendOnlyFile> {
  let options = RuntimeServerOptions::default();
  let backends = {
    let (_dirs, backends) = wnode_test::test_sublogs("aof_proc", 1);
    backends
  };
  Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
    &options,
    None,
  ))
}

/// 临时目录小容量真存储（收尾自清；小预算配置对标 C# 16MB 基线）
fn open_store(dir: &Path) -> aok::Result<Arc<WedbStore<SegmentedDevice>>> {
  let device = Arc::new(SegmentedDevice::single_file(dir.join("data.db"))?);
  Ok(Arc::new(WedbStore::open(test_store_config(), device)?))
}

/// AOF 存储过程条目端到端回放：入队 → recover 扫描 → 处理器分发 →
/// 注册表工厂重建过程执行全链闭环
///（对标 C# ReplayStoredProc → StoredProcRunnerBase → RunCustomTxnProcAtReplica）。
#[test]
fn stored_proc_entry_replays_via_recover() -> Void {
  use compio::runtime::Runtime;

  let rt = Runtime::new()?;
  rt.block_on(async {
    // DEFAULT 空事务过程（静态表内唯一内置过程，对标 C# 产品库零内置过程）
    let proc_id = txn_proc_slot::DEFAULT;

    let aof = memory_aof();
    let log = aof.log();

    // 入队存储过程条目（载荷 = 过程参数序列）
    let mut body = Vec::new();
    stored_proc_args::encode(&[b"pk".to_vec(), b"pv".to_vec()], &mut body);
    log
      .enqueue_stored_proc(
        AofEntryType::StoredProcedure,
        5,
        7,
        proc_id,
        &body,
        &SublogAccess::default(),
      )
      .unwrap();
    log.commit();

    // 处理器装配静态表执行面并恢复回放
    let mut processor = AofProcessor::new(Arc::clone(&aof));
    processor.set_stored_proc_replayer(Arc::new(StoredProcRegistryReplayer::new(
      TxnLockTable::new(),
    )));

    let dir = temp_dir().join(format!("wnode-proc-replay-{}", id()));
    create_dir_all(&dir)?;
    let store = open_store(&dir)?;
    let session = store.new_session()?;
    let storage = StorageSession::new(session.enter_batch());
    let target = ReplayTarget::new(&storage, &store);

    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
    assert_eq!(replayed, 1, "存储过程条目应被重放");

    let _ = remove_dir_all(&dir);
    OK
  })
}

/// FLUSHDB 回放单库边界：多库写入 → 主库换号批 DbMeta 镜像条目 + FLUSHDB 屏障
/// 条目 → 回放 → 仅库 0 被清空（旧域判死、路由换指），库 1 数据完好
/// （对标 C# AofProcessor 重放内核 case FlushDb（精确锚点见 src/aof/aof_processor.rs）
/// → storeWrapper.FlushDatabase(unsafeTruncateLog, dbId: header.databaseId)：
/// C# 该臂是「把主库该库的物理清退在副本复刻」，wedb 的清退是 O(1) 换号，
/// 其状态面（新映射 + 旧域墓碑 + 水位）由先行 DbMeta 镜像条目复刻，条目本身
/// 只承接 C# 同臂的栅栏与登记表回收职责）
#[test]
fn flush_db_entry_replays_targeted_database() -> Void {
  use compio::runtime::Runtime;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let aof = memory_aof();
    let log = aof.log();

    // 多库写入：库 0 与库 1 各一键（直写存储面，不经 AOF）
    let dir = temp_dir().join(format!("wnode-flushdb-replay-{}", id()));
    create_dir_all(&dir)?;
    let store = open_store(&dir)?;
    let db0 = store.new_session()?;
    db0.set_context(0, 0);
    db0.upsert(b"db0key", b"v0").await?;
    let db1 = store.new_session()?;
    db1.set_context(0, 1);
    db1.upsert(b"db1key", b"v1").await?;

    // 主库 flush_database(0, 0) 换号批的镜像条目：新虚拟库号即分配器当前值
    //（库 1 已占 1，故换号取 2、水位抬到 3），旧域 (0, 0) 落 0x04 退役墓碑
    let swapped = store
      .vdb
      .next_virtual_id
      .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(swapped, 2, "预置：库 1 已占虚拟号 1，换号新号须为 2");
    enqueue_dbmeta_mirror(
      log,
      &[
        DbMetaRecord::DbMap {
          vns: 0,
          logic_db: 0,
          vdb: swapped,
        },
        DbMetaRecord::GcDeadDb {
          expired_at: 1,
          vns: 0,
          old_vdb: 0,
          tail_address: 0,
        },
        DbMetaRecord::NextId {
          next_virtual_id: swapped + 1,
        },
      ],
    )?;

    // 入队 FLUSHDB 条目（域载荷 (vns=0, 换号前旧 vdb=0)，回放射仅屏障）
    log
      .enqueue_safe_flush_aof(AofEntryType::FlushDb, false, 0, 0)
      .unwrap();
    log.commit();

    // 处理器装配与恢复回放
    let processor = AofProcessor::new(Arc::clone(&aof));
    let session = store.new_session()?;
    let storage = StorageSession::new(session.enter_batch());
    let target = ReplayTarget::new(&storage, &store);

    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
    assert_eq!(
      replayed, 4,
      "三条换号批镜像条目 + 一条 FLUSHDB 条目应被重放"
    );

    // 仅 databaseId 指定库被清（旧域判死 + 路由换指新号）
    db0.set_context(0, 0);
    assert_eq!(db0.read(b"db0key").await?, None, "库 0 应被清空");
    assert!(
      store.vdb.is_dead_domain(0, 0),
      "换号批墓碑须把库 0 旧域判死"
    );
    assert_eq!(
      store.vdb.route_vdb_of(0, 0),
      Some(swapped),
      "库 0 路由须直指镜像换入的新虚拟号"
    );
    db1.set_context(0, 1);
    assert_eq!(
      db1.read(b"db1key").await?,
      Some(b"v1".to_vec()),
      "库 1 数据必须完好"
    );
    assert!(!store.vdb.is_dead_domain(0, 1), "库 1 不得判死");

    let _ = remove_dir_all(&dir);
    OK
  })
}

/// 存储过程重放执行面（StoredProcRegistryReplayer::replay 直调）：
/// DEFAULT 空事务过程经回放落点存储会话跑通三段式，无键登记即无哈希收集
#[test]
fn replayer_replays_default_proc_with_replay_target() -> Void {
  use compio::runtime::Runtime;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = temp_dir().join(format!("wnode-replayer-direct-{}", id()));
    create_dir_all(&dir)?;
    let store = open_store(&dir)?;
    let session = store.new_session()?;
    let storage = StorageSession::new(session.enter_batch());
    let target = ReplayTarget::new(&storage, &store);

    let mut hashes = Vec::new();
    StoredProcRegistryReplayer::new(TxnLockTable::new())
      .replay(
        txn_proc_slot::DEFAULT,
        7,
        &[b"k1".to_vec()],
        &mut hashes,
        &target,
      )
      .unwrap();
    assert!(hashes.is_empty(), "DEFAULT 过程零键登记即零哈希收集");

    let _ = remove_dir_all(&dir);
    OK
  })
}

/// 未注册的 AOF 存储过程 id：重放执行面拒绝重建（C# GarnetException 回放失败路径）
#[test]
fn replayer_rejects_unregistered_proc_id() -> Void {
  use compio::runtime::Runtime;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = temp_dir().join(format!("wnode-replayer-unregistered-{}", id()));
    create_dir_all(&dir)?;
    let store = open_store(&dir)?;
    let session = store.new_session()?;
    let storage = StorageSession::new(session.enter_batch());
    let target = ReplayTarget::new(&storage, &store);

    let mut hashes = Vec::new();
    let err = StoredProcRegistryReplayer::new(TxnLockTable::new())
      .replay(9, 0, &[], &mut hashes, &target)
      .unwrap_err();
    assert!(err.to_string().contains("未注册"));

    let _ = remove_dir_all(&dir);
    OK
  })
}
