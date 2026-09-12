//! AOF 域统一集成测试（第三轮收口）
//!
//! 三个闭环：
//! 1. 磁盘段扫描——写入超出环形缓冲容量的历史段后 recover/重放能取回全部
//!    记录（对标 C# TsavoriteLog.Scan 的跨段设备面恢复扫描）；
//! 2. checkpoint 版本基线——检查点后旧代条目（store_version 低于基线）重放
//!    跳过（对标 C# AofProcessor.ShouldSkipRecord 的 IsOldVersionRecord 分支）；
//! 3. SET 带 EX 的随键 TTL——TTL 旁路记录写入分流为 PEXPIREAT RMW 条目，
//!    重放后副本 TTL 与主端一致（对标 C# SETEX 条目随行 expiration 的等价
//!    两跳形态）。

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use waof::{AofEntryType, WalConfig, WalLog};
use wbase::time::now_ticks;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  aof::{aof_header::AofHeader, aof_processor::ReplayInput, waof_sublog::single_log_aof},
  config::runtime_server_options::RuntimeServerOptions,
  databases::{
    database_manager_base::DatabaseManagerBase, garnet_database::GarnetDatabase,
  },
  service::NodeService,
  types::RespCommand,
};

/// 环形缓冲容量（64KB）：写入约 3 倍容量即产生被挤出内存窗的历史磁盘段
const SMALL_RING: usize = 64 * 1024;

type NodeEnv = (
  TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  Arc<WalLog<SegmentedDevice>>,
);

/// 打开节点（store + 小环形 wal）
fn open_node(name: &str, ring: usize) -> aok::Result<NodeEnv> {
  let dir = tempdir()?;
  let store_device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{name}.db")),
  )?);
  // 段式 AOF 设备：物理截断 / 跨段扫描均走真实段文件
  let wal_device = Arc::new(SegmentedDevice::segmented(
    dir.path().join(format!("{name}.wal")),
    64 * 1024,
  )?);
  let mut config = StoreConfig::new(2048, 64 * 1024, 16, 0.5)?;
  config.range_index_dir = Some(dir.path().to_path_buf());
  let store = Arc::new(WedbStore::open(config, store_device)?);
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::new(ring))?);
  Ok((dir, store, wal))
}

/// 已解码条目视图
struct EntryView {
  op: AofEntryType,
  input: Option<ReplayInput>,
}

fn parse_entry(payload: &[u8]) -> EntryView {
  let header = AofHeader::parse(payload).expect("header decodable");
  let op = AofEntryType::try_from(header.op_type).expect("op known");
  let body = &payload[AofHeader::TOTAL_SIZE..];
  let key_len = u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
  let mut rest = &body[4 + key_len..];
  if op.has_chunk_value() {
    let val_len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
    rest = &rest[4 + val_len..];
  }
  let input = (!rest.is_empty()).then(|| ReplayInput::deserialize(rest).expect("input decodable"));
  EntryView { op, input }
}

/// 异步全量扫描（恢复链路权威入口：跨环形窗口与历史磁盘段）
async fn scan_all(service: &NodeService<SegmentedDevice>) -> Vec<EntryView> {
  service
    .aof()
    .log()
    .scan_single_async(0, 0, i64::MAX)
    .await
    .iter()
    .map(|r| parse_entry(&r.payload))
    .collect()
}

/// 缺口 2：写入超出环形缓冲容量后，异步扫描跨磁盘段取回全部记录、
/// 重放恢复全部数据
#[test]
fn disk_segment_scan_replays_history_beyond_ring_window() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, wal) = open_node("disk_seg", SMALL_RING)?;
    let service = NodeService::with_wal(Arc::clone(&store), Arc::clone(&wal))?;

    // 3 批 × 每批 ~1 倍环形容量：第 1/2 批必然被挤出内存窗（历史磁盘段）
    let batches = 3u32;
    let per_batch = 48u32;
    let value = vec![b'x'; 1024];
    let mut expected = 0usize;
    for b in 0..batches {
      for i in 0..per_batch {
        let key = format!("k:{b}:{i}");
        service.session().upsert(key.as_bytes(), &value).await?;
        expected += 1;
      }
      // 每批物理刷盘：flushed 推进后环形窗口方可滚动复用
      wal.commit().await?;
    }
    // 校验历史段确实已越出内存窗（同步快路径覆盖不了全量）
    let tail = wal.tail_address();
    assert!(
      tail > SMALL_RING as u64 + wal.begin_address(),
      "写入量须超出环形容量 (tail={tail})"
    );

    // 异步全量扫描：跨磁盘段取回全部条目
    let entries = scan_all(&service).await;
    assert_eq!(
      entries.len(),
      expected,
      "跨磁盘段扫描须取回全部 {expected} 条（含越窗历史段）"
    );

    // 副本重放恢复全部数据（scan_single_async 恢复链路）
    let (_rdir, replica_store, replica_wal) = open_node("disk_seg_replica", SMALL_RING)?;
    let replica = NodeService::with_wal(Arc::clone(&replica_store), Arc::clone(&replica_wal))?;
    let replica_session = replica.session();
    let replayed = service.replay_into_session(replica_session).await?;
    assert_eq!(replayed as usize, expected, "重放条数须与写入条数一致");
    assert_eq!(
      replica_session.read(b"k:0:0").await?,
      Some(value.clone()),
      "最早批次（远超环形窗口的历史段）须重放成功"
    );
    assert_eq!(
      replica_session
        .read(format!("k:{}:{}", batches - 1, per_batch - 1).as_bytes())
        .await?,
      Some(value),
      "最新批次须重放成功"
    );
    OK
  })
}

/// 缺口 3：checkpoint 版本基线——检查点前写入的旧代条目（store_version <
/// 基线）重放跳过；检查点后的新代条目正常重放
#[test]
fn checkpoint_version_baseline_skips_old_generation() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, store, wal) = open_node("ver_base", 1 << 20)?;
    let aof = single_log_aof(Arc::clone(&wal), &RuntimeServerOptions::default());
    // 数据写入面与库管理面共享同一 AOF 域实例（域统一装配形态）
    let service = NodeService::new(Arc::clone(&store), Arc::clone(&aof))?;
    let db = Arc::new(GarnetDatabase::with_garnet_aof(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      dir.path().join("ckpt"),
      Some(Arc::clone(&aof)),
    ));
    let base = DatabaseManagerBase::new(dir.path().join("ckpt"));

    // 旧代写入（版本 0，经数据写入面端口入队）
    service.session().upsert(b"old", b"v0").await?;
    wal.commit().await?;

    // 拍检查点：版本推进 + AOF 截断至尾（单机形态 TruncateUntil(Tail)）
    assert!(base.take_database_checkpoint_async(&db).await?);
    let baseline = store.current_version();
    assert!(
      baseline > 0,
      "checkpoint 后版本基线须 > 0（实际 {baseline}）"
    );

    // 新代写入（版本 = 基线）：拍检查点后 AOF 为空，新条目携带新版本
    service.session().upsert(b"new", b"v1").await?;
    wal.commit().await?;

    // 清空数据（暂停镜像：删除不产生 AOF 墓碑条目——对标 C# 重放会话
    // recordToAof: false 的等价闸）
    {
      let _pause = store.pause_aof_listeners();
      service.session().delete(b"old").await?;
      service.session().delete(b"new").await?;
    }
    assert_eq!(service.session().read(b"old").await?, None);
    assert_eq!(service.session().read(b"new").await?, None);

    // 重放：段内残留的旧代条目被版本基线跳过（数据不复活），新代条目正常应用
    let replayed = base.replay_database_aof(&db, u64::MAX).await?;
    assert_eq!(
      service.session().read(b"new").await?,
      Some(b"v1".to_vec()),
      "新代条目（版本 = 基线）须正常重放"
    );
    assert_eq!(
      service.session().read(b"old").await?,
      None,
      "旧代条目（版本 0 < 基线）须被 ShouldSkipRecord 跳过"
    );
    assert!(
      replayed >= 1,
      "新代条目须计入重放（扫描计数含跳过条目，至少 1 条应用）"
    );
    OK
  })
}

/// 缺口 4：SET k v EX——TTL 旁路写入分流为 PEXPIREAT RMW 条目，重放后
/// 副本 TTL 与主端一致；PERSIST 清除路径入队对应条目
#[test]
fn set_ex_ttl_replays_to_replica() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, wal) = open_node("ttl_ex", 1 << 20)?;
    let service = NodeService::with_wal(Arc::clone(&store), Arc::clone(&wal))?;
    let session = store.new_session()?;

    // SET k v EX：值写入 + TTL 旁路记录（两跳：StoreUpsert + PEXPIREAT）
    session.upsert(b"ttl-key", b"val").await?;
    let expire_at = now_ticks() + 60_000_000_000; // 60s（ticks）
    session.put_ttl(b"ttl-key", expire_at).await?;
    wal.commit().await?;

    let entries = scan_all(&service).await;
    // 值条目 + TTL RMW 条目（PEXPIREAT，arg1 = 绝对 Unix 毫秒）
    let upserts = entries
      .iter()
      .filter(|e| e.op == AofEntryType::StoreUpsert)
      .count();
    assert_eq!(upserts, 1, "值写入须入队 StoreUpsert");
    let rmw = entries
      .iter()
      .find(|e| e.op == AofEntryType::StoreRMW)
      .expect("TTL 旁路须入队 StoreRMW（PEXPIREAT）");
    let input = rmw.input.as_ref().expect("RMW 条目带 input");
    assert_eq!(input.cmd, RespCommand::Pexpireat);
    assert!(input.arg1 > 0, "PEXPIREAT arg1 = 绝对 Unix 毫秒");

    // 副本重放：值与 TTL 均一致
    let (_rdir, replica_store, replica_wal) = open_node("ttl_ex_replica", 1 << 20)?;
    let replica = NodeService::with_wal(Arc::clone(&replica_store), Arc::clone(&replica_wal))?;
    let replica_session = replica.session();
    service.replay_into_session(replica_session).await?;
    assert_eq!(
      replica_session.read(b"ttl-key").await?,
      Some(b"val".to_vec()),
      "副本须重放值"
    );
    let replica_ttl: Option<i64> = replica_session.ttl_of(b"ttl-key").await?;
    assert!(
      replica_ttl.is_some_and(|t| (t - expire_at).abs() < 10_000_000),
      "副本 TTL 须与主端一致（±1s 容差），实际 {replica_ttl:?} vs {expire_at}"
    );

    // PERSIST 清除：TTL 旁路墓碑分流为 PERSIST RMW 条目，重放后副本无 TTL
    session.persist(b"ttl-key").await?;
    wal.commit().await?;
    let entries = scan_all(&service).await;
    let persist = entries
      .iter()
      .rev()
      .find(|e| e.op == AofEntryType::StoreRMW)
      .expect("TTL 清除须入队 StoreRMW（PERSIST）");
    let input = persist.input.as_ref().expect("PERSIST 条目带 input");
    assert_eq!(input.cmd, RespCommand::Persist);
    OK
  })
}
