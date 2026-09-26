//! AOF 域统一集成测试（第三轮收口）
//!
//! 三个闭环：
//! 1. 磁盘段扫描——写入超出环形缓冲容量的历史段后 recover/重放能取回全部
//!    记录（对标 C# TsavoriteLog.Scan 的跨段设备面恢复扫描）；
//! 2. checkpoint 版本基线——检查点后旧代条目（store_version 低于基线）重放
//!    跳过（对标 C# AofProcessor.ShouldSkipRecord 的 IsOldVersionRecord 分支）；
//! 3. SET 带 EX 的随键 TTL——TTL 旁路记录写入分流为 PEXPIREAT RMW 条目，
//!    重放后副本 TTL 与主端一致（对标 C# SETEX 条目随行 expiration 的等价
//!    两跳形态）；
//! 4. SETEX / PSETEX / EXPIREAT / GETDEL 命令端写侧形态闭包——主侧 AOF 只出
//!    StoreUpsert / StoreDelete / StoreRMW(PEXPIREAT|PERSIST) 形态，写侧恒不产
//!    已删重放臂的 Setex/Psetex/Expireat/Getdel 条目形态；
//! 5. RMW 终值 StoreUpsert 重放臂纯值镜像闭包（票 zcode-r123c-hllchain1，
//!    deviations.md §17/§18）——PFADD/INCR/SETBIT 写回条目流重放在副本与
//!    重启恢复两臂恒保留随键 TTL，SET 覆写清退经 Persist/信封 StoreDelete
//!    条目自述收敛、回放臂禁随动裁决。

use std::{mem::take, sync::Arc};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use waof::{AofAddress, AofEntryType, AofHeader, WalConfig, WalLog};
use wbase::{
  align::DEFAULT_SECTOR_SIZE,
  convert::TICKS_PER_SECOND,
  time::{now_ms, now_ticks},
};
use wconf::RuntimeServerOptions;
use wcpr::CheckpointType;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  aof::{replay_input::ReplayInput, waof_sublog::single_log_aof},
  database::{DatabaseManagerBase, GarnetDatabase, checkpoint_version},
  resp::{
    basic_commands::IncrCmd, key_admin_commands::ExpireCmd, resp_server_session::RespServerSession,
  },
  service::NodeService,
};
use wresp::command::RespCommand;
use wval::{KeyTag, NamespaceDbCodec};

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
  let wal_device = Arc::new(SegmentedDevice::new(
    dir.path().join(format!("{name}.wal")),
    64 * 1024,
    DEFAULT_SECTOR_SIZE,
  )?);
  let mut config = StoreConfig::new(2048, 64 * 1024, 16, 0.5)?;
  config.range_index_dir = Some(dir.path().to_path_buf());
  let store = Arc::new(WedbStore::open(config, store_device)?);
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::new(ring))?);
  Ok((dir, store, wal))
}

/// 已解码条目视图（key 保留条目物理键，供标签分域断言）
struct EntryView {
  op: AofEntryType,
  input: Option<ReplayInput>,
  key: Vec<u8>,
}

fn parse_entry(payload: &[u8]) -> EntryView {
  // commit 元数据帧非 AOF 条目，重放/检视面跳过（与 AofProcessor 过滤一致）
  assert!(!waof::is_commit_frame(payload), "commit 帧不入条目检视面");
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
  EntryView { op, input, key }
}

/// 异步全量扫描（恢复链路权威入口的迭代器直取形态：跨环形窗口与历史磁盘段；
/// IO 失败即 panic，不折叠为半截集合）
async fn scan_all(service: &NodeService<SegmentedDevice>) -> Vec<EntryView> {
  let mut iter = service.aof().log().scan_single_iter(0, 0, i64::MAX);
  let mut entries = Vec::new();
  loop {
    match iter.next().await {
      Ok(Some(r)) => {
        // commit 元数据帧随批写出于日志尾部，重放/检视面跳过
        if !waof::is_commit_frame(&r.payload) {
          entries.push(parse_entry(&r.payload));
        }
      }
      Ok(None) => break,
      Err(err) => panic!("恢复链路全量扫描 IO 失败: {err:?}"),
    }
  }
  entries
}

/// 缺口 2：写入超出环形缓冲容量后，异步扫描跨磁盘段取回全部记录、
/// 重放恢复全部数据
#[compio::test]
async fn disk_segment_scan_replays_history_beyond_ring_window() -> Void {
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

  // 副本重放恢复全部数据（恢复链路 recover 驱动 scan_single_async_with）
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
}

/// 缺口 3：checkpoint 版本基线——检查点前写入的旧代条目（store_version <
/// 基线）重放跳过；检查点后的新代条目正常重放
#[test]
fn checkpoint_version_baseline_skips_old_generation() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, store, wal) = open_node("ver_base", 1 << 20)?;
    let aof = single_log_aof(Arc::clone(&wal), &RuntimeServerOptions::default())
      .expect("装配 single_log_aof");
    // 数据写入面与库管理面共享同一 AOF 域实例（域统一装配形态）
    let service = NodeService::new(Arc::clone(&store), Arc::clone(&aof))?;
    let db = Arc::new(GarnetDatabase::new(
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
#[compio::test]
async fn set_ex_ttl_replays_to_replica() -> Void {
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
}

/// 缺口 5：GarnetLog 重置路由——single_log_aof 装配链路
/// GarnetLog.reset_async → SingleLog.reset_async → WaofSublog.reset_async
/// → WalLog::reset（持提交锁原子复位 + sync_data），位点归零后日志可复用
/// （对标 C# GarnetLog.Reset → SingleLog.Reset → TsavoriteLog.Reset 路由）
#[compio::test]
async fn garnet_log_reset_async_zeroes_wal_and_reusable() -> Void {
  let (_dir, store, wal) = open_node("garnet_reset", SMALL_RING)?;
  let service = NodeService::with_wal(Arc::clone(&store), Arc::clone(&wal))?;

  // 写入并提交，形成非零位点
  let value = vec![b'v'; 256];
  for i in 0..16u32 {
    service
      .session()
      .upsert(format!("rk:{i}").as_bytes(), &value)
      .await?;
  }
  wal.commit().await?;
  assert!(wal.tail_address() > 0, "重置前尾位点须非零");

  // GarnetLog 路由重置：WaofSublog 转发 WalLog::reset（持锁原子复位）
  service.aof().log().reset_async().await;
  let begin = wal.begin_address();
  assert_eq!(begin, wal.tail_address(), "重置后尾位点须归零");
  assert_eq!(begin, wal.flushed_until_address(), "重置后刷盘位点须归零");
  assert_eq!(begin, wal.committed_until_address(), "重置后提交位点须归零");
  assert_eq!(wal.total_size(), 0);

  // 复用：重置后新写入可提交并扫描
  service.session().upsert(b"rk:after", b"fresh").await?;
  wal.commit().await?;
  let entries = scan_all(&service).await;
  assert_eq!(entries.len(), 1, "重置后仅新写入可扫描");
  assert_eq!(entries[0].op, AofEntryType::StoreUpsert);

  OK
}

/// WaofSublog 容量/占用双方法 + CommittedBeginAddress 独立快照：
/// max 恒环形窗口容量（TsavoriteLog.MaxMemorySizeBytes，TsavoriteLog.cs:196）、
/// memory = tail - begin 随写入推进随截断收缩（:201 当前占用）；
/// committed_begin 初值 FirstValidAddress、commit 采样 begin（:2696）、
/// safe_initialize 恢复（:528/:596）、reset 归 1（:244-246）
#[test]
fn waof_sublog_memory_watermark_and_committed_begin() -> Void {
  use wnode::aof::waof_sublog::WaofSublog;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, _store, wal) = open_node("waof_watermark", SMALL_RING)?;
    let backend = WaofSublog::new(Arc::clone(&wal));

    // 空日志：容量恒 buffer_size，占用为 begin→tail 间距（初始 0）
    assert_eq!(backend.max_memory_size_bytes(), SMALL_RING as i64);
    let initial_span = backend.tail_address() - backend.begin_address();
    assert!(initial_span >= 0);

    // 写入 + 提交：占用 = tail - begin 严格增长；committed_begin 随 commit 采样
    let before = backend.tail_address();
    let _ = backend.enqueue(&[b'x'; 512]);
    assert_eq!(
      backend.memory_size_bytes(),
      backend.tail_address() - backend.begin_address(),
      "占用 = 环形窗口有效字节"
    );
    assert!(backend.memory_size_bytes() > initial_span);
    backend.commit(backend.tail_address(), 0);
    assert_eq!(
      backend.committed_begin_address(),
      before,
      "commit 快照 = 提交时刻 begin"
    );

    // 恢复链：safe_initialize 以 begin 参数恢复 committed_begin
    let begin = backend.begin_address();
    backend.safe_initialize(begin, backend.committed_until_address(), 0);
    assert_eq!(backend.committed_begin_address(), begin);

    // reset 归 FirstValidAddress（真实段设备首地址 0）
    backend.reset_async().await;
    assert_eq!(backend.committed_begin_address(), 0);
    assert_eq!(
      backend.memory_size_bytes(),
      backend.tail_address() - backend.begin_address()
    );

    OK
  })
}

/// 快照发起（版本提前推进）后至快照写盘完成窗口内写入的数据，携带新版本号，
/// 在崩溃恢复后经 AOF 重放完整保留（防回归：旧时序下快照落盘后才推进版本，
/// 窗口内写入携带旧版本 0，重放时被 is_old_version_record 误判跳过导致静默丢数据）
#[test]
fn snapshot_window_writes_preserved_on_recovery() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, store, wal) = open_node("ver_window", 1 << 20)?;
    let aof = single_log_aof(Arc::clone(&wal), &RuntimeServerOptions::default())
      .expect("装配 single_log_aof");
    let service = NodeService::new(Arc::clone(&store), Arc::clone(&aof))?;
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      dir.path().join("ckpt"),
      Some(Arc::clone(&aof)),
    ));
    let base = DatabaseManagerBase::new(dir.path().join("ckpt"));

    // 1. 检查点前旧写入（版本 0）
    service.session().upsert(b"k_before", b"v_before").await?;
    wal.commit().await?;

    // 2. 模拟检查点发起：预签 Token，covered 采样先行（生产漏斗同款，
    // C# InitiateCheckpointAsync :503-519 同序），位窗口随后打开捕获模糊地板
    let floor = wcpr::find_latest_checkpoint(&db.checkpoint_dir)?.unwrap_or(0);
    let token = wcpr::next_token_above(floor);
    let new_ver = checkpoint_version(token);
    let covered = AofAddress::create(1, aof.log().tail_address().max());
    let index_start = db.store().begin_version_shift(new_ver as u64);

    // 3. 核心：在快照写盘前（窗口期）前台写入数据（携带新版本 new_ver，位点在 covered 之后，
    // 且记录已置纪元位——恢复期快照副本须被 undoNextVersion 回滚，仅经 AOF 重放生效一次）
    service.session().upsert(b"k_window", b"v_window").await?;
    wal.commit().await?;

    // 4. 执行快照落盘并关闭位窗口，随后截断 AOF 至 covered 位点
    let cp_res = db
      .store()
      .create_checkpoint_with_token(
        &db.checkpoint_dir,
        CheckpointType::Snapshot,
        token,
        index_start,
      )
      .await;
    db.store().end_version_shift();
    cp_res?;
    aof.log().truncate_until_async(&covered).await;
    aof.log().commit_async().await;
    db.update_last_save(now_ms());

    // 5. 模拟崩溃重启：从检查点恢复 store，版本基线对齐到快照版本
    let recovered_store = base
      .recover_database_checkpoint_async(&db, None)
      .await?
      .expect("快照必须恢复成功");
    assert_eq!(recovered_store.current_version(), new_ver);

    let recovered_db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&recovered_store),
      Arc::clone(&store.device),
      dir.path().join("ckpt"),
      Some(Arc::clone(&aof)),
    ));

    // 重放 AOF：k_window 版本号 >= new_ver，绝不能被跳过
    let replayed = base.replay_database_aof(&recovered_db, u64::MAX).await?;
    assert!(replayed >= 1, "窗口期写入必须被重放");

    let rec_session = recovered_store.new_session()?;
    assert_eq!(
      rec_session.read(b"k_before").await?,
      Some(b"v_before".to_vec()),
      "检查点内数据正常恢复"
    );
    assert_eq!(
      rec_session.read(b"k_window").await?,
      Some(b"v_window".to_vec()),
      "快照窗口期新写入经 AOF 重放成功恢复，未被静默丢弃"
    );

    OK
  })
}

/// 非幂等窗口写防回归（检查点重复写票）：与生产 take_database_checkpoint_async
/// 同序的发起漏斗（预签 Token → 开位窗口捕获模糊地板 → 推进版本 → 采样 covered）
/// 下，窗口期 INCR / LPUSH / RPUSH 同时物化进快照日志与 AOF 尾部。崩溃重启恢复时
/// 内核必须按 undoNextVersion 回滚模糊区窗口副本，令其仅经 AOF 重放恰好生效一次：
/// 计数器取严格终值、列表取严格长度与元素次序。
/// （实证红相：关闭回滚即双重生效，winlist 长 6、nlist 长 4——列表增量形态先翻红；
/// INCR 落 AOF 为绝对值 upsert 形态，重放覆写同值不足以自曝，非幂等面以列表为准）
#[test]
fn window_nonidempotent_writes_undo_then_replay_once() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, store, wal) = open_node("dup_write", 1 << 20)?;
    let aof = single_log_aof(Arc::clone(&wal), &RuntimeServerOptions::default())
      .expect("装配 single_log_aof");
    let service = NodeService::new(Arc::clone(&store), Arc::clone(&aof))?;
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      dir.path().join("ckpt"),
      Some(Arc::clone(&aof)),
    ));
    let base = DatabaseManagerBase::new(dir.path().join("ckpt"));

    // 1. 窗口前基线写（版本 0，AOF 条目先于 covered 恒被截断，无双写面）
    service.session().upsert(b"ctr", b"5").await?;
    {
      let session = service.session();
      let batch = session.enter_batch();
      let mut resp = RespServerSession::default();
      let mut out = Vec::new();
      assert!(
        resp
          .list_push(&[b"winlist", b"a"], &batch, &mut out, false)
          .expect("RPUSH 基线执行")
      );
      assert_eq!(out, b":1\r\n");
      out.clear();
      assert!(
        resp
          .list_push(&[b"winlist", b"b"], &batch, &mut out, false)
          .expect("RPUSH 基线执行")
      );
      assert_eq!(out, b":2\r\n");
    }
    wal.commit().await?;

    // 2. 检查点发起（生产漏斗同序）：covered 采样先行，位窗口随后打开捕获模糊地板
    let floor = wcpr::find_latest_checkpoint(&db.checkpoint_dir)?.unwrap_or(0);
    let token = wcpr::next_token_above(floor);
    let new_ver = checkpoint_version(token);
    let covered = AofAddress::create(1, aof.log().tail_address().max());
    let index_start = db.store().begin_version_shift(new_ver as u64);

    // 3. 窗口期非幂等写：INCR×3 于既有键（5→8）与新键（0→3）、
    //    LPUSH/RPUSH 于既有列表（[a,b]→[x,a,b,y]）与新键列表（[e1,e2]）
    {
      let session = service.session();
      let batch = session.enter_batch();
      let mut resp = RespServerSession::default();
      let mut out = Vec::new();
      for (key, expects) in [
        (b"ctr".as_slice(), [6i64, 7, 8]),
        (b"ctr_new".as_slice(), [1, 2, 3]),
      ] {
        for expect in expects {
          out.clear();
          assert!(
            resp
              .network_increment(IncrCmd::Incr, &[key], &batch, &mut out)
              .expect("INCR 窗口执行")
          );
          assert_eq!(
            out,
            format!(":{expect}\r\n").into_bytes(),
            "窗口期 INCR 增量生效"
          );
        }
      }
      let pushes: [(&[u8], &[u8], bool, i64); 4] = [
        (b"winlist", b"x", true, 3),
        (b"winlist", b"y", false, 4),
        (b"nlist", b"e1", true, 1),
        (b"nlist", b"e2", false, 2),
      ];
      for (key, val, left, expect) in pushes {
        out.clear();
        assert!(
          resp
            .list_push(&[key, val], &batch, &mut out, left)
            .expect("PUSH 窗口执行")
        );
        assert_eq!(
          out,
          format!(":{expect}\r\n").into_bytes(),
          "窗口期 PUSH 增量生效"
        );
      }
    }
    wal.commit().await?;

    // 4. 快照落盘（窗口记录同时物化进快照日志与 AOF 尾部），随即关闭位窗口并截断 AOF
    let cp_res = db
      .store()
      .create_checkpoint_with_token(
        &db.checkpoint_dir,
        CheckpointType::Snapshot,
        token,
        index_start,
      )
      .await;
    db.store().end_version_shift();
    cp_res?;
    aof.log().truncate_until_async(&covered).await;
    aof.log().commit_async().await;
    db.update_last_save(now_ms());

    // 5. 崩溃重启：从检查点恢复（内核回滚模糊区窗口副本）+ 重放 AOF 尾部
    let recovered_store = base
      .recover_database_checkpoint_async(&db, None)
      .await?
      .expect("快照必须恢复成功");
    let recovered_db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&recovered_store),
      Arc::clone(&store.device),
      dir.path().join("ckpt"),
      Some(Arc::clone(&aof)),
    ));
    let replayed = base.replay_database_aof(&recovered_db, u64::MAX).await?;
    assert!(replayed >= 1, "窗口期非幂等条目必须被重放");

    // 6. 严格终值：每条窗口写恰好生效一次（快照副本已回滚，AOF 重放生效）
    let rec_session = recovered_store.new_session()?;
    assert_eq!(
      rec_session.read(b"ctr").await?,
      Some(b"8".to_vec()),
      "既有键计数器 5+3 必须恰为 8（未回滚则重放再 +3 变 11）"
    );
    assert_eq!(
      rec_session.read(b"ctr_new").await?,
      Some(b"3".to_vec()),
      "新键计数器必须恰为 3（未回滚则快照副本 3 + 重放 3 = 6）"
    );
    let batch = rec_session.enter_batch();
    let mut resp = RespServerSession::default();
    let mut out = Vec::new();
    assert!(
      resp
        .list_length(&[b"winlist"], &batch, &mut out)
        .expect("LLEN 执行")
    );
    assert_eq!(
      out, b":4\r\n",
      "列表长度必须恰为 4（未回滚则重放补双份变 6）"
    );
    out.clear();
    assert!(
      resp
        .list_range(&[b"winlist", b"0", b"-1"], &batch, &mut out)
        .expect("LRANGE 执行")
    );
    assert_eq!(
      out, b"*4\r\n$1\r\nx\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\ny\r\n",
      "列表元素与次序必须严格：LPUSH x 占头、RPUSH y 追加于尾"
    );
    out.clear();
    assert!(
      resp
        .list_range(&[b"nlist", b"0", b"-1"], &batch, &mut out)
        .expect("LRANGE 执行")
    );
    assert_eq!(
      out, b"*2\r\n$2\r\ne1\r\n$2\r\ne2\r\n",
      "新键列表必须经「快照副本回滚 → 仅凭重放建链」，元素次序严格"
    );

    OK
  })
}

/// 截断前宕机 + 恢复位点闸纵深（检查点重复写票的截断前形态）：快照发布后、
/// AOF 截断执行前异常宕机（publish_checkpoint_aof_address 已把 covered 持久化
/// 进检查点元数据，truncate_until 未及执行），恢复重放以元数据位点
/// （recovered_aof_floor）为扫描下界：
/// - 位点 < covered 的条目（快照已物化面）必须跳过——含位点在 covered 之前
///   却携带检查点新版本戳的幽灵条目（模拟截断面撕裂的版本戳异常形态）：
///   版本闸对其失效（store_version 不低于基线），唯位点闸兜底；
/// - 位点 > covered 的窗口非幂等条目（INCR / LPUSH / RPUSH）经
///   undoNextVersion 回滚 + AOF 重放恰一次承接。
///
/// 断言计数器严格终值、列表严格长度与次序、幽灵键绝不出现
#[test]
fn crash_before_truncate_floor_gate_skips_materialized_entries() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, store, wal) = open_node("crash_pre_trunc", 1 << 20)?;
    let aof = single_log_aof(Arc::clone(&wal), &RuntimeServerOptions::default())
      .expect("装配 single_log_aof");
    let service = NodeService::new(Arc::clone(&store), Arc::clone(&aof))?;
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      dir.path().join("ckpt"),
      Some(Arc::clone(&aof)),
    ));
    let base = DatabaseManagerBase::new(dir.path().join("ckpt"));

    // 1. 预签 Token（幽灵条目需要检查点版本号，先于基线写入取得）
    let floor = wcpr::find_latest_checkpoint(&db.checkpoint_dir)?.unwrap_or(0);
    let token = wcpr::next_token_above(floor);
    let new_ver = checkpoint_version(token);

    // 2. 幽灵条目：位点将落在 covered 之前、版本戳伪造为检查点新版本
    //    （截断面撕裂的异常形态；写路径不可能产出，此处手工注入验证闸面）
    let ghost_addr = aof.enqueue_raw(AofEntryType::StoreUpsert, new_ver, b"ghost", b"gv", &[])?;

    // 3. 基线写（版本 0，位点 > 幽灵条目、< covered）
    service.session().upsert(b"ctr", b"5").await?;
    {
      let session = service.session();
      let batch = session.enter_batch();
      let mut resp = RespServerSession::default();
      let mut out = Vec::new();
      assert!(
        resp
          .list_push(&[b"winlist", b"a"], &batch, &mut out, false)
          .expect("RPUSH 基线执行")
      );
      out.clear();
      assert!(
        resp
          .list_push(&[b"winlist", b"b"], &batch, &mut out, false)
          .expect("RPUSH 基线执行")
      );
    }
    wal.commit().await?;

    // 4. 检查点发起（生产漏斗同序：covered 采样先行 → 开窗 → 推进版本）
    let covered = AofAddress::create(1, aof.log().tail_address().max());
    assert!(
      ghost_addr >= 0 && ghost_addr < covered.get(0).unwrap_or_default(),
      "幽灵条目位点必须严格落在 covered 之前（位点闸作用域内）"
    );
    let index_start = db.store().begin_version_shift(new_ver as u64);

    // 5. 窗口期非幂等写：INCR×3（ctr 5→8、ctr_new 0→3）、
    //    LPUSH x / RPUSH y 于 winlist、RPUSH e1/e2 于 nlist
    {
      let session = service.session();
      let batch = session.enter_batch();
      let mut resp = RespServerSession::default();
      let mut out = Vec::new();
      for (key, expects) in [
        (b"ctr".as_slice(), [6i64, 7, 8]),
        (b"ctr_new".as_slice(), [1, 2, 3]),
      ] {
        for expect in expects {
          out.clear();
          assert!(
            resp
              .network_increment(IncrCmd::Incr, &[key], &batch, &mut out)
              .expect("INCR 窗口执行")
          );
          assert_eq!(out, format!(":{expect}\r\n").into_bytes());
        }
      }
      let pushes: [(&[u8], &[u8], bool, i64); 4] = [
        (b"winlist", b"x", true, 3),
        (b"winlist", b"y", false, 4),
        (b"nlist", b"e1", false, 1),
        (b"nlist", b"e2", false, 2),
      ];
      for (key, val, left, expect) in pushes {
        out.clear();
        assert!(
          resp
            .list_push(&[key, val], &batch, &mut out, left)
            .expect("PUSH 窗口执行")
        );
        assert_eq!(out, format!(":{expect}\r\n").into_bytes());
      }
    }
    wal.commit().await?;

    // 6. 快照落盘 + 关闭位窗口 + covered 持久化进元数据；截断【不执行】
    //    ——模拟宕机于 publish 之后、truncate_until 之前
    let cp_res = db
      .store()
      .create_checkpoint_with_token(
        &db.checkpoint_dir,
        CheckpointType::Snapshot,
        token,
        index_start,
      )
      .await;
    db.store().end_version_shift();
    cp_res?;
    wcpr::publish_checkpoint_aof_address(
      &db.checkpoint_dir,
      token,
      &[covered.get(0).unwrap_or_default() as u64],
    )
    .await?;

    // 7. 崩溃重启恢复：AOF 全量在盘（未截断），恢复基线 = 检查点版本，
    //    recovered_aof_floor = covered
    let recovered_store = base
      .recover_database_checkpoint_async(&db, None)
      .await?
      .expect("快照必须恢复成功");
    assert_eq!(recovered_store.current_version(), new_ver);
    assert_eq!(
      recovered_store.recovered_aof_floor(),
      &[covered.get(0).unwrap_or_default() as u64],
      "恢复位点下界必须按向量取自检查点元数据覆盖边界"
    );

    let recovered_db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&recovered_store),
      Arc::clone(&store.device),
      dir.path().join("ckpt"),
      Some(Arc::clone(&aof)),
    ));
    let replayed = base.replay_database_aof(&recovered_db, u64::MAX).await?;
    assert!(replayed >= 1, "窗口期非幂等条目必须被重放");

    // 8. 严格终值：幽灵键绝不出现（位点闸跳过版本戳异常条目），
    //    窗口写恰一次（回滚 + 重放互补），基线数据随快照恢复
    let rec_session = recovered_store.new_session()?;
    assert_eq!(
      rec_session.read(b"ghost").await?,
      None,
      "位点在 covered 之前的版本戳异常条目必须被位点闸跳过（无闸则重放复活）"
    );
    assert_eq!(
      rec_session.read(b"ctr").await?,
      Some(b"8".to_vec()),
      "既有键计数器 5+3 必须恰为 8"
    );
    assert_eq!(
      rec_session.read(b"ctr_new").await?,
      Some(b"3".to_vec()),
      "新键计数器必须恰为 3（重放恰一次）"
    );
    let batch = rec_session.enter_batch();
    let mut resp = RespServerSession::default();
    let mut out = Vec::new();
    assert!(
      resp
        .list_length(&[b"winlist"], &batch, &mut out)
        .expect("LLEN 执行")
    );
    assert_eq!(out, b":4\r\n", "列表长度必须恰为 4");
    out.clear();
    assert!(
      resp
        .list_range(&[b"winlist", b"0", b"-1"], &batch, &mut out)
        .expect("LRANGE 执行")
    );
    assert_eq!(
      out, b"*4\r\n$1\r\nx\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\ny\r\n",
      "列表元素与次序必须严格"
    );
    out.clear();
    assert!(
      resp
        .list_range(&[b"nlist", b"0", b"-1"], &batch, &mut out)
        .expect("LRANGE 执行")
    );
    assert_eq!(out, b"*2\r\n$2\r\ne1\r\n$2\r\ne2\r\n", "新键列表严格重建");

    OK
  })
}

/// 缺口 6：SETEX / PSETEX / EXPIREAT / GETDEL 的写侧形态闭包（重放侧死臂删除的
/// 对证）——四条命令在命令端分别折成「值 StoreUpsert + TTL 旁路 Pexpireat」
/// （set.rs:network_setex_impl、keys.rs:network_expire 的绝对秒换算）与
/// 「StoreDelete 墓碑」（keys.rs:network_getdel），主侧 AOF 恒不产
/// Setex/Psetex/Expireat/Getdel 形态的 StoreRMW 条目；从库回放后值与 TTL 与
/// 主端一致（绝对毫秒口径往返，容差 1s 覆盖 ticks→ms→ticks 的粗化）
#[compio::test]
async fn string_ttl_commands_log_only_live_entry_forms() -> Void {
  let (_dir, store, wal) = open_node("cmd_forms", 1 << 20)?;
  let service = NodeService::with_wal(Arc::clone(&store), Arc::clone(&wal))?;
  let session = store.new_session()?;
  let batch = session.enter_batch();
  let mut resp = RespServerSession::default();
  let mut out = Vec::new();

  // SETEX / PSETEX：值写入 + TTL 旁路两跳
  resp
    .network_setex(&[b"seex", b"120", b"v1"], &batch, None, &mut out)
    .expect("SETEX 执行");
  resp
    .network_psetex(&[b"pseex", b"240000", b"v2"], &batch, None, &mut out)
    .expect("PSETEX 执行");
  // SET + EXPIREAT：绝对 Unix 秒在命令端换算为物理 TTL 记录
  resp
    .network_set(&[b"sexp", b"v3"], &batch, None, &mut out)
    .expect("SET 执行");
  let expire_at_secs = now_ms() / 1000 + 300;
  let secs_text = expire_at_secs.to_string();
  resp
    .network_expire(
      ExpireCmd::Expireat,
      &[b"sexp", secs_text.as_bytes()],
      &batch,
      &mut out,
    )
    .expect("EXPIREAT 执行");
  // GETDEL：净效果由 StoreDelete 墓碑承载（随键 TTL 清退落 PERSIST 条目）
  resp
    .network_getdel(&[b"seex"], &batch, &mut out)
    .expect("GETDEL 执行");
  wal.commit().await?;

  // 形态闭包：StoreRMW 条目命令只允许绝对毫秒 TTL 双形态
  let entries = scan_all(&service).await;
  let mut upserts = 0;
  let mut deletes = 0;
  let mut pexpireats = 0;
  for e in &entries {
    match e.op {
      AofEntryType::StoreUpsert => upserts += 1,
      AofEntryType::StoreDelete => deletes += 1,
      AofEntryType::StoreRMW => {
        let cmd = e.input.as_ref().expect("StoreRMW 条目带 input").cmd;
        assert!(
          matches!(cmd, RespCommand::Pexpireat | RespCommand::Persist),
          "字符串写侧 StoreRMW 条目只能是 Pexpireat/PERSIST 形态，实际 {cmd:?}"
        );
        if cmd == RespCommand::Pexpireat {
          pexpireats += 1;
        }
      }
      other => panic!("字符串命令写侧不得产出 {other:?} 形态条目"),
    }
  }
  assert_eq!(upserts, 3, "SETEX/PSETEX/SET 各一条终值 StoreUpsert");
  assert_eq!(deletes, 1, "GETDEL 净效果为单条 StoreDelete");
  assert_eq!(
    pexpireats, 3,
    "SETEX/PSETEX/EXPIREAT 各一条绝对毫秒 Pexpireat TTL 条目"
  );

  // 主端 TTL 基准（回放侧按同一绝对口径重建）
  let main_pseex = service.session().ttl_of(b"pseex").await?;
  let main_sexp = service.session().ttl_of(b"sexp").await?;

  // 从库回放：值与 TTL 一致，GETDEL 键不复现
  let (_rdir, replica_store, replica_wal) = open_node("cmd_forms_replica", 1 << 20)?;
  let replica = NodeService::with_wal(Arc::clone(&replica_store), Arc::clone(&replica_wal))?;
  service.replay_into_session(replica.session()).await?;
  assert_eq!(
    replica.session().read(b"seex").await?,
    None,
    "GETDEL 净效果须从库不复现"
  );
  assert_eq!(
    replica.session().read(b"pseex").await?,
    Some(b"v2".to_vec()),
    "PSETEX 值经 StoreUpsert 终值条目重放"
  );
  assert_eq!(
    replica.session().read(b"sexp").await?,
    Some(b"v3".to_vec()),
    "EXPIREAT 键值经 StoreUpsert 终值条目重放"
  );
  let r_pseex = replica.session().ttl_of(b"pseex").await?;
  let r_sexp = replica.session().ttl_of(b"sexp").await?;
  assert!(
    main_pseex.is_some_and(|m| r_pseex.is_some_and(|r| (r - m).abs() < TICKS_PER_SECOND)),
    "PSETEX 从库 TTL 与主端一致（±1s），实际 主 {main_pseex:?} 从 {r_pseex:?}"
  );
  assert!(
    main_sexp.is_some_and(|m| r_sexp.is_some_and(|r| (r - m).abs() < 2 * TICKS_PER_SECOND)),
    "EXPIREAT 从库 TTL 与主端一致（±2s，覆盖秒级粗化），实际 主 {main_sexp:?} 从 {r_sexp:?}"
  );
  OK
}

/// 缺口 7（票 zcode-r123c-hllchain1）：RMW 终值 StoreUpsert 重放臂纯值镜像
/// ——「PFADD→EXPIRE→PFADD」条目流主端 TTL 全程存活（try_rmw_sync TtlGate::Pass
/// 零 TTL 触碰，deviations §17 恒保留），副本增量与重启恢复两臂必须同态保留。
/// 修复前重放臂走 upsert_string SET 语义随动清退腿，终值条目回放即抹掉刚落的
/// TTL 记录：副本 PTTL 恒 -1 幽灵带读、重启恢复后键永生。断言三面：条目形
/// 闭式（StoreUpsert 终值 + Pexpireat 自述，主端 RMW 写回零 TTL 事件不产
/// Persist）、副本值/基数/TTL 与主端同刻度、恢复臂「清态重放」TTL 不丢。
#[compio::test]
async fn rmw_final_value_replay_preserves_key_ttl() -> Void {
  let (_dir, store, wal) = open_node("rmw_ttl", 1 << 20)?;
  let service = NodeService::with_wal(Arc::clone(&store), Arc::clone(&wal))?;

  let primary_count = {
    let session = store.new_session()?;
    let batch = session.enter_batch();
    let mut resp = RespServerSession::default();
    let mut out = Vec::new();
    resp
      .hyper_log_log_add(&[b"hll:k", b"e1"], &batch, &mut out)
      .expect("PFADD 基线执行");
    resp
      .network_expire(ExpireCmd::Expire, &[b"hll:k", b"100"], &batch, &mut out)
      .expect("EXPIRE 执行");
    resp
      .hyper_log_log_add(&[b"hll:k", b"e2"], &batch, &mut out)
      .expect("PFADD 终值写回执行");
    out.clear();
    resp
      .hyper_log_log_length(&[b"hll:k"], &batch, &mut out)
      .expect("PFCOUNT 基准");
    take(&mut out)
  };
  wal.commit().await?;
  assert_eq!(primary_count, b":2\r\n", "fixture：主端基数 2");

  // 条目形闭式：两次 PFADD 写回均只镜像终值 StoreUpsert（空 input），随键
  // TTL 唯一条目是 EXPIRE 自述的 Pexpireat——主端 RMW 写回零 TTL 触碰，
  // 恒不产 Persist（upsert 条目尾部为空 input 帧，cmd 恒 None）
  let entries = scan_all(&service).await;
  let ops: Vec<(AofEntryType, Option<RespCommand>)> = entries
    .iter()
    .map(|e| {
      let cmd = e
        .input
        .as_ref()
        .and_then(|i| (i.cmd != RespCommand::None).then_some(i.cmd));
      (e.op, cmd)
    })
    .collect();
  assert_eq!(
    ops,
    vec![
      (AofEntryType::StoreUpsert, None),
      (AofEntryType::StoreRMW, Some(RespCommand::Pexpireat)),
      (AofEntryType::StoreUpsert, None),
    ],
    "PFADD→EXPIRE→PFADD 条目流应为纯终值 + 单条 Pexpireat（主端若随动清退即多出 Persist 形）"
  );

  let primary_value = service.session().read(b"hll:k").await?;
  let primary_ttl = service.session().ttl_of(b"hll:k").await?;
  assert!(
    primary_value.is_some() && primary_ttl.is_some_and(|t| t > now_ticks()),
    "fixture：主端值与随键 TTL 均应在册"
  );

  // 副本增量回放：值逐字节一致、PFCOUNT 同帧、TTL 与主端同刻度
  let (_rdir, replica_store, replica_wal) = open_node("rmw_ttl_replica", 1 << 20)?;
  let replica = NodeService::with_wal(Arc::clone(&replica_store), Arc::clone(&replica_wal))?;
  service.replay_into_session(replica.session()).await?;
  assert_eq!(
    replica.session().read(b"hll:k").await?,
    primary_value,
    "副本终值记录回放后应与主端逐字节一致"
  );
  let replica_count = {
    let rsession = replica_store.new_session()?;
    let rbatch = rsession.enter_batch();
    let mut rresp = RespServerSession::default();
    let mut rout = Vec::new();
    rresp
      .hyper_log_log_length(&[b"hll:k"], &rbatch, &mut rout)
      .expect("副本 PFCOUNT");
    rout
  };
  assert_eq!(replica_count, primary_count, "PFCOUNT 主从逐帧一致");
  let replica_ttl = replica.session().ttl_of(b"hll:k").await?;
  let (Some(p), Some(r)) = (primary_ttl, replica_ttl) else {
    panic!(
      "主从两侧随键 TTL 均应在册（修复前副本回放臂 SET 语义恒清）：主 {primary_ttl:?} 从 {replica_ttl:?}"
    );
  };
  assert!(
    (p - r).abs() < TICKS_PER_SECOND,
    "副本 TTL 须与主端同刻度（±1s），实际 {p} vs {r}"
  );

  // 重启恢复臂：模拟「检查点带态清除 → 条目流重放承接尾部」——清态后重放
  // 同一条目流，终值 StoreUpsert 绝不能再摘回 Pexpireat 刚落的 TTL
  {
    let _pause = store.pause_aof_listeners();
    service.session().delete(b"hll:k").await?;
  }
  assert_eq!(service.session().read(b"hll:k").await?, None);
  service.replay_into_session(service.session()).await?;
  let recovered_ttl = service.session().ttl_of(b"hll:k").await?;
  let (Some(p), Some(b)) = (primary_ttl, recovered_ttl) else {
    panic!(
      "恢复回放后随键 TTL 须复活（修复前终值条目即清、键永生违 §17）：主 {primary_ttl:?} 恢复 {recovered_ttl:?}"
    );
  };
  assert!(
    (p - b).abs() < TICKS_PER_SECOND,
    "恢复回放 TTL 与主端原刻度一致（±1s），实际 {p} vs {b}"
  );
  assert_eq!(
    service.session().read(b"hll:k").await?,
    primary_value,
    "恢复回放值与主端一致"
  );
  OK
}

/// 缺口 7b：INCR 与 SETBIT 终值写回与 PF 族共用同一漏斗（rmw.rs:45 自陈），
/// 同形条目重放臂恒保留随键 TTL（族面锁，§17/§18 恒保留裁决）
#[compio::test]
async fn incr_and_setbit_final_value_replay_preserves_key_ttl() -> Void {
  let (_dir, store, wal) = open_node("rmw_ttl_fam", 1 << 20)?;
  let service = NodeService::with_wal(Arc::clone(&store), Arc::clone(&wal))?;
  {
    let session = store.new_session()?;
    let batch = session.enter_batch();
    let mut resp = RespServerSession::default();
    let mut out = Vec::new();
    resp
      .network_set(&[b"inc", b"10"], &batch, None, &mut out)
      .expect("SET inc 执行");
    resp
      .network_expire(ExpireCmd::Expire, &[b"inc", b"100"], &batch, &mut out)
      .expect("EXPIRE inc 执行");
    resp
      .network_increment(IncrCmd::Incr, &[b"inc"], &batch, &mut out)
      .expect("INCR inc 执行");
    resp
      .network_set(&[b"bit", b"abc"], &batch, None, &mut out)
      .expect("SET bit 执行");
    resp
      .network_expire(ExpireCmd::Expire, &[b"bit", b"100"], &batch, &mut out)
      .expect("EXPIRE bit 执行");
    resp
      .network_string_set_bit(&[b"bit", b"1000", b"1"], &batch, &mut out)
      .expect("SETBIT bit 执行");
  }
  wal.commit().await?;

  let (p_inc, p_bit) = (
    service.session().ttl_of(b"inc").await?,
    service.session().ttl_of(b"bit").await?,
  );
  assert!(
    p_inc.is_some() && p_bit.is_some(),
    "fixture：INCR/SETBIT 写回后主端随键 TTL 恒在册（§17/§18）"
  );

  let (_rdir, replica_store, replica_wal) = open_node("rmw_ttl_fam_replica", 1 << 20)?;
  let replica = NodeService::with_wal(Arc::clone(&replica_store), Arc::clone(&replica_wal))?;
  service.replay_into_session(replica.session()).await?;
  assert_eq!(
    replica.session().read(b"inc").await?,
    Some(b"11".to_vec()),
    "INCR 终值经条目流重放"
  );
  assert_eq!(
    replica.session().read(b"bit").await?,
    service.session().read(b"bit").await?,
    "SETBIT 终值逐字节一致"
  );
  for (key, p_ttl) in [(b"inc".as_slice(), p_inc), (b"bit".as_slice(), p_bit)] {
    let r_ttl = replica.session().ttl_of(key).await?;
    let (Some(p), Some(r)) = (p_ttl, r_ttl) else {
      panic!("键 {key:?} 主从随键 TTL 均应在册（修复前副本回放即清）：主 {p_ttl:?} 从 {r_ttl:?}");
    };
    assert!(
      (p - r).abs() < TICKS_PER_SECOND,
      "键 {key:?} 副本 TTL 与主端同刻度（±1s），实际 {p} vs {r}"
    );
  }
  OK
}

/// 缺口 7c（方案 4d）：SET 覆写的 SET 语义清退在主端已各自自述为条目
///（TTL 经 TtlWrite 分流 Persist 条目、信封墓碑经 StoreDelete 条目），
/// 重放臂改纯值后仍由条目流驱动收敛——既不再依赖回放二次裁决，也不漏清退
#[compio::test]
async fn set_overwrite_ttl_and_object_replay_converge_via_entries() -> Void {
  let (_dir, store, wal) = open_node("set_conv", 1 << 20)?;
  let service = NodeService::with_wal(Arc::clone(&store), Arc::clone(&wal))?;
  {
    let session = store.new_session()?;
    let batch = session.enter_batch();
    let mut resp = RespServerSession::default();
    let mut out = Vec::new();
    // 带 TTL 覆写：主端 SET 清退腿自述为 Persist 条目
    resp
      .network_set(&[b"s", b"v1"], &batch, None, &mut out)
      .expect("SET v1 执行");
    resp
      .network_expire(ExpireCmd::Expire, &[b"s", b"100"], &batch, &mut out)
      .expect("EXPIRE s 执行");
    resp
      .network_set(&[b"s", b"v2"], &batch, None, &mut out)
      .expect("SET v2 清退 TTL 执行");
    // 对象键覆写：信封墓碑自述为 StoreDelete(ObjectEnvelope) 条目
    resp
      .hash_set(&[b"hk", b"f", b"v"], &batch, &mut out)
      .expect("HSET hk 执行");
    resp
      .network_set(&[b"hk", b"str"], &batch, None, &mut out)
      .expect("SET hk 覆写对象键执行");
  }
  wal.commit().await?;

  let entries = scan_all(&service).await;
  assert!(
    entries.iter().any(|e| {
      e.op == AofEntryType::StoreRMW
        && e
          .input
          .as_ref()
          .is_some_and(|i| i.cmd == RespCommand::Persist)
    }),
    "SET 覆写清随键 TTL 须由主端自述为 Persist 条目（条目流唯一真值源）"
  );
  assert!(
    entries.iter().any(|e| {
      e.op == AofEntryType::StoreDelete
        && NamespaceDbCodec::decode_tag(&e.key) == Some(KeyTag::ObjectEnvelope)
    }),
    "SET 覆写对象键的信封清退须由主端自述为 StoreDelete(ObjectEnvelope) 条目"
  );
  assert_eq!(
    service.session().ttl_of(b"s").await?,
    None,
    "fixture：主端 SET 覆写后 TTL 已清（SET 语义）"
  );

  let (_rdir, replica_store, replica_wal) = open_node("set_conv_replica", 1 << 20)?;
  let replica = NodeService::with_wal(Arc::clone(&replica_store), Arc::clone(&replica_wal))?;
  service.replay_into_session(replica.session()).await?;
  assert_eq!(
    replica.session().read(b"s").await?,
    Some(b"v2".to_vec()),
    "覆写终值经 StoreUpsert 条目重放"
  );
  assert_eq!(
    replica.session().ttl_of(b"s").await?,
    None,
    "副本 TTL 清除由 Persist 条目承接：终态与主端一致（非回放臂随动裁决）"
  );
  assert_eq!(
    replica.session().read(b"hk").await?,
    Some(b"str".to_vec()),
    "对象键覆写后副本读 String 域终值"
  );
  let probe = replica_store.new_session()?;
  let env_k = probe.session_tag_key(KeyTag::ObjectEnvelope, b"hk");
  assert!(
    probe.read_raw(env_k.as_slice()).await?.is_none(),
    "副本信封记录须随 StoreDelete 条目消亡（纯值臂不探删，清除全由条目承接）"
  );
  OK
}
