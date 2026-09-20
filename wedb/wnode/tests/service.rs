//! NodeService AOF 端口与重放闭环集成测试
//!
//! 单一机制验证：写入端统一经 GarnetLog::enqueue（标准 AofHeader +
//! SpanByte key + ReplayInput 载荷），重放端统一经 AofProcessor
//! （NodeService::replay_into_session），不再有独立条目编码/重放器。

use std::sync::{Arc, atomic::Ordering};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use waof::{AofEntryType, AofHeader, WalConfig, WalLog};
use wbftree::{StorageBackendType, TreeTuning};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, TtlOpt, WedbStore};
use wnode::{aof::replay_input::ReplayInput, service::NodeService};
use wresp::command::RespCommand;
use wval::{KeyTag, NamespaceDbCodec};

const TUNE: TreeTuning = TreeTuning {
  cache_size: 65536,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

type TestEnv = (
  TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  Arc<WalLog<SegmentedDevice>>,
);

fn open_node(name: &str) -> aok::Result<TestEnv> {
  let dir = tempdir()?;
  let store_device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{name}.db")),
  )?);
  let wal_device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{name}.wal")),
  )?);
  let mut config = StoreConfig::new(2048, 64 * 1024, 16, 0.5)?;
  config.range_index_dir = Some(dir.path().to_path_buf());
  let store = Arc::new(WedbStore::open(config, store_device)?);
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::default())?);
  Ok((dir, store, wal))
}

/// 已解码的 AOF 条目视图（GarnetLog 写入端布局的读取镜像）
struct EntryView {
  op: AofEntryType,
  /// 物理键（含 ns/db 前缀）
  key: Vec<u8>,
  /// RMW 形状的命令载荷
  input: Option<ReplayInput>,
}

/// 解析单条 AOF 记录：`[AofHeader][u32 key_len][key][u32 val_len (Upsert)][value][input]`
fn parse_entry(payload: &[u8]) -> EntryView {
  let header = AofHeader::parse(payload).expect("header decodable");
  let op = AofEntryType::try_from(header.op_type).expect("op known");
  let body = &payload[AofHeader::TOTAL_SIZE..];
  let key_len = u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
  let key = body[4..4 + key_len].to_vec();
  let mut rest = &body[4 + key_len..];
  if op.has_chunk_value() {
    let val_len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
    rest = &rest[4 + val_len..];
  }
  let input = (!rest.is_empty()).then(|| ReplayInput::deserialize(rest).expect("input decodable"));
  EntryView { op, key, input }
}

/// 扫描 NodeService 的 AOF 流并解析
fn scan_entries(service: &NodeService<SegmentedDevice>) -> Vec<EntryView> {
  let mut records = Vec::new();
  service.aof().log().scan_single_with(0, 0, i64::MAX, |r| {
    records.push(r.clone());
    true
  });
  records
    .iter()
    // commit 元数据帧随批写出于日志尾部，重放/检视面跳过
    .filter(|r| !waof::is_commit_frame(&r.payload))
    .map(|r| parse_entry(&r.payload))
    .collect()
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RISetAndGetBasicTest
#[test]
fn ri_ops_apply_then_log_and_replay() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, wal) = open_node("ri_replay")?;
    let service = NodeService::with_wal(Arc::clone(&store), Arc::clone(&wal))?;

    service
      .ri_create(b"idx", StorageBackendType::Memory, TUNE)
      .await?;
    service.ri_set(b"idx", b"field-1", b"value-0001").await?;
    service.ri_set(b"idx", b"field-2", b"value-0002").await?;
    service.ri_del(b"idx", b"field-2").await?;

    // 条目形状：RICREATE/RISET/RISET/RIDEL 四条 StoreRMW，Deterministic 标志
    let entries = scan_entries(&service);
    assert_eq!(entries.len(), 4);
    for e in &entries {
      assert_eq!(e.op, AofEntryType::StoreRMW);
      assert!(e.input.as_ref().is_some_and(|i| i.flags & 64 != 0));
    }
    let cmds: Vec<_> = entries
      .iter()
      .map(|e| e.input.as_ref().unwrap().cmd)
      .collect();
    assert_eq!(
      cmds,
      vec![
        RespCommand::Ricreate,
        RespCommand::Riset,
        RespCommand::Riset,
        RespCommand::Ridel,
      ]
    );
    OK
  })
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIDelFieldTest
#[test]
fn ri_del_missing_field_still_logs() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, wal) = open_node("ri_skip")?;
    let service = NodeService::with_wal(Arc::clone(&store), Arc::clone(&wal))?;

    service
      .ri_create(b"idx", StorageBackendType::Memory, TUNE)
      .await?;
    let deleted = service.ri_del(b"idx", b"absent").await?;
    assert!(deleted);

    let entries = scan_entries(&service);
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].op, AofEntryType::StoreRMW);
    assert_eq!(entries[1].op, AofEntryType::StoreRMW);
    OK
  })
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RISetAndGetBasicTest
#[test]
fn ri_set_reaches_bftree() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, wal) = open_node("ri_data")?;
    let service = NodeService::with_wal(Arc::clone(&store), Arc::clone(&wal))?;

    service
      .ri_create(b"idx", StorageBackendType::Disk, TUNE)
      .await?;
    service.ri_set(b"idx", b"field-1", b"value-0001").await?;

    let got: Option<Vec<u8>> = service
      .session()
      .range_index_get(b"idx", b"field-1")
      .await?;
    assert_eq!(got.as_deref(), Some(&b"value-0001"[..]));
    OK
  })
}

/// RI.GET 便捷读取（错误抹平进 anyhow，断言用）
async fn ri_get(
  session: &wkv::StoreSession<SegmentedDevice>,
  key: &[u8],
  field: &[u8],
) -> aok::Result<Option<Vec<u8>>> {
  Ok(session.range_index_get(key, field).await?)
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIAofOnlyRecoveryTest
/// RIAofReplay 端到端节点级副本收敛验证（单一机制：replay_into_session →
/// AofProcessor → RangeIndexManagerReplication 实际执行）
#[test]
fn ri_aof_replay_converges_replica() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, wal) = open_node("ri_aof_primary")?;
    let service = NodeService::with_wal(Arc::clone(&store), Arc::clone(&wal))?;

    // 基态数据 (C# 段：RI.CREATE + RI.SET key1..key3 + SAVE)
    service
      .ri_create(b"aoftest", StorageBackendType::Disk, TUNE)
      .await?;
    service.ri_set(b"aoftest", b"key1", b"val1").await?;
    service.ri_set(b"aoftest", b"key2", b"val2").await?;
    service.ri_set(b"aoftest", b"key3", b"val3").await?;
    wal.commit().await?;

    // 检查点后变更——仅存在于 AOF 流 (C# 段：COMMITAOF 之前的三笔变更)
    service.ri_set(b"aoftest", b"key4", b"val4").await?;
    service.ri_set(b"aoftest", b"key1", b"val1-updated").await?;
    service.ri_del(b"aoftest", b"key2").await?;
    wal.commit().await?;

    // 从节点：全新引擎实例 + 独立 WAL，零内存状态；统一重放链路闭环
    let (_replica_dir, replica_store, replica_wal) = open_node("ri_aof_replica")?;
    let replica = NodeService::with_wal(Arc::clone(&replica_store), Arc::clone(&replica_wal))?;
    let replayed = service.replay_into_session(replica.session()).await?;
    assert_eq!(replayed, 7, "create + 5 sets + 1 del");

    // 数据一致性断言 (对标 C# 四项 RI.GET 断言)
    let primary = service.session();
    let replica_session = replica.session();
    let key1 = ri_get(primary, b"aoftest", b"key1").await?;
    let key2 = ri_get(primary, b"aoftest", b"key2").await?;
    let key3 = ri_get(primary, b"aoftest", b"key3").await?;
    let key4 = ri_get(primary, b"aoftest", b"key4").await?;

    // key1 应为回放后的更新值
    assert_eq!(key1.as_deref(), Some(&b"val1-updated"[..]));
    // key2 应已被回放删除
    assert_eq!(key2, None);
    // key3 应保有基态值
    assert_eq!(key3.as_deref(), Some(&b"val3"[..]));
    // key4 应为回放新增
    assert_eq!(key4.as_deref(), Some(&b"val4"[..]));

    // 主从两端逐字段一致
    for (field, expect) in [
      (b"key1".as_slice(), Some(&b"val1-updated"[..])),
      (b"key2", None),
      (b"key3", Some(&b"val3"[..])),
      (b"key4", Some(&b"val4"[..])),
    ] {
      assert_eq!(
        ri_get(primary, b"aoftest", field).await?,
        ri_get(replica_session, b"aoftest", field).await?,
        "field {field:?} diverged between primary and replica"
      );
      assert_eq!(
        ri_get(replica_session, b"aoftest", field).await?,
        expect.map(<[u8]>::to_vec)
      );
    }
    OK
  })
}

/// test/standalone/Garnet.test/ExpiredKeyDeletionTests.cs:TestOnDemandExpiredKeyDeletionScan
/// TTL 过期物理清除单条化集成测试（对标 C# DELIFEXPIM RMW +
/// Expired|Deterministic 标志的单条确定性条目语义）：
///
/// 1. 主端 TTL purge 端口在场时，purge 链的两条物理墓碑（TTL 记录 + 数据）
///    被会话级抑制，**用户域面**流中恰好一条 DELIFEXPIM StoreRMW 条目；
/// 2. 副本端（全新实例）经统一重放链路消费该条目（AofProcessor 分支
///    Deterministic|Expired 确定性执行统一 DEL），与主端等效且幂等；
/// 3. 映射面独立分面判读（doc/zh/db.md「主从物理镜像与异步屏障」：物理日志
///    直接镜像主库的 `KeyTag::DbMeta` 与数据记录，从库完全继承、不做本地二次
///    映射）：`set_context(7, 3)` 的换号批即 NsMap/DbMap/NextId 三条 DbMeta
///    条目随流镜像，副本回放后逐值等于主库映射、本地取号器零触发。
#[test]
fn ttl_purge_single_deterministic_entry() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, wal) = open_node("ttl_purge")?;
    let service = NodeService::with_wal(Arc::clone(&store), Arc::clone(&wal))?;

    // ns=7 db=3 写入后立即过期（过去时间戳 → purge 返回 2 = TTL 记录 + 数据）
    let session = service.session();
    session.set_context(7, 3);
    session.upsert(b"glitch", b"v1").await?;
    assert_eq!(session.expire_at(b"glitch", 1, TtlOpt::NONE).await?, 2);
    assert_eq!(session.read(b"glitch").await?, None, "过期键必须已物理清除");
    wal.commit().await?;

    // 分面：`KeyTag::DbMeta` 映射镜像条目 vs 用户域数据条目（同一物理日志、
    // 两套键前缀，判据互不遮蔽）
    let (dbmeta, domain): (Vec<EntryView>, Vec<EntryView>) = scan_entries(&service)
      .into_iter()
      .partition(|e| NamespaceDbCodec::decode_tag(&e.key) == Some(KeyTag::DbMeta));

    // 映射面：一次换号批 = NsMap + DbMap + NextId 三条，全为 StoreUpsert
    assert_eq!(dbmeta.len(), 3, "映射面须恰好一条 set_context 换号批");
    assert!(
      dbmeta.iter().all(|e| e.op == AofEntryType::StoreUpsert),
      "换号批镜像条目须为 Upsert"
    );

    // 用户域面：SET 物理镜像（StoreUpsert，物理键含 ns/db 前缀）+
    // 单条 DELIFEXPIM StoreRMW（Deterministic|Expired，arg1 携带到期时间戳）；
    // purge 链的两条物理墓碑（TTL 记录 + 数据）绝不出现在任何面内
    assert_eq!(domain.len(), 2, "purge 链墓碑镜像必须为零");
    assert_eq!(domain[0].op, AofEntryType::StoreUpsert);
    let glitch_physical = session.session_string_key(b"glitch");
    assert_eq!(domain[0].key, glitch_physical.to_vec());

    assert_eq!(domain[1].op, AofEntryType::StoreRMW);
    let purge_input = domain[1].input.as_ref().unwrap();
    assert_eq!(purge_input.cmd, RespCommand::Delifexpim);
    assert_eq!(purge_input.flags, (64 | 128), "Deterministic|Expired");
    // expire_at 入口 4-bit coarse 粗化（ExpirationWithOption.cs:22-23）：
    // 输入 1 tick 粗化为 0 后随 arg1 携带，重放端恒删不评估该值
    assert_eq!(purge_input.arg1, 0, "expire_at_ticks 粗化后随 arg1 携带");

    // 副本端：全新引擎实例，统一重放链路本地执行过期清除
    let (_replica_dir, replica_store, replica_wal) = open_node("ttl_purge_replica")?;
    let replica = NodeService::with_wal(Arc::clone(&replica_store), Arc::clone(&replica_wal))?;
    let replayed = service.replay_into_session(replica.session()).await?;
    assert_eq!(replayed, 5, "映射面 3 条 + 用户域面 2 条须全数重放");

    // 映射继承判读（须在下方 set_context 探针之前：探针自身会按引擎口径物化）：
    // 副本 (7,3) → (1,2) 逐值等于主库，水位锁步至 3，零本地二次取号
    assert_eq!(
      replica_store.vdb.vns_of_ns(7),
      Some(1),
      "副本须继承主库命名空间映射，不得另起新号"
    );
    assert_eq!(
      replica_store.vdb.route_vdb_of(1, 3),
      Some(2),
      "副本须继承主库库级路由格"
    );
    assert_eq!(
      replica_store.vdb.next_virtual_id.load(Ordering::Relaxed),
      3,
      "副本分配水位须锁步自主库换号批的 0x05 记录"
    );

    // 副本以 (ns=7, db=3) 上下文断言键已被确定性清除（幂等）
    let replica_session = replica.session();
    replica_session.set_context(7, 3);
    assert_eq!(
      replica_session.read(b"glitch").await?,
      None,
      "副本回放 DELIFEXPIM 后键必须物理清除"
    );
    OK
  })
}
