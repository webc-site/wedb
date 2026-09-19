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
//!    已删重放臂的 Setex/Psetex/Expireat/Getdel 条目形态。

use std::sync::Arc;

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
  resp::{key_admin_commands::ExpireCmd, resp_server_session::RespServerSession},
  service::NodeService,
};
use wresp::command::RespCommand;

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
    Some(64 * 1024),
    DEFAULT_SECTOR_SIZE,
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
  // commit 元数据帧非 AOF 条目，重放/检视面跳过（与 AofProcessor 过滤一致）
  assert!(!waof::is_commit_frame(payload), "commit 帧不入条目检视面");
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
    // commit 元数据帧随批写出于日志尾部，重放/检视面跳过
    .filter(|r| !waof::is_commit_frame(&r.payload))
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

/// 缺口 5：GarnetLog 重置路由——single_log_aof 装配链路
/// GarnetLog.reset_async → SingleLog.reset_async → WaofSublog.reset_async
/// → WalLog::reset（持提交锁原子复位 + sync_data），位点归零后日志可复用
/// （对标 C# GarnetLog.Reset → SingleLog.Reset → TsavoriteLog.Reset 路由）
#[test]
fn garnet_log_reset_async_zeroes_wal_and_reusable() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
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
  })
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

    // 2. 模拟检查点发起：预签 Token 并推进版本，采样 covered
    let floor = wcpr::find_latest_checkpoint(&db.checkpoint_dir)?.unwrap_or(0);
    let token = wcpr::next_token_above(floor);
    let new_ver = checkpoint_version(token);
    store.set_current_version(new_ver);
    let covered = AofAddress::create(1, aof.tail_address());

    // 3. 核心：在快照写盘前（窗口期）前台写入数据（携带新版本 new_ver，位点在 covered 之后）
    service.session().upsert(b"k_window", b"v_window").await?;
    wal.commit().await?;

    // 4. 执行快照落盘并截断 AOF 至 covered 位点
    db.store
      .create_checkpoint_with_token(&db.checkpoint_dir, CheckpointType::Snapshot, token)
      .await?;
    aof.truncate_until_async(&covered).await;
    aof.commit_flush_async().await;
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

/// 缺口 6：SETEX / PSETEX / EXPIREAT / GETDEL 的写侧形态闭包（重放侧死臂删除的
/// 对证）——四条命令在命令端分别折成「值 StoreUpsert + TTL 旁路 Pexpireat」
/// （set.rs:network_setex_impl、keys.rs:network_expire 的绝对秒换算）与
/// 「StoreDelete 墓碑」（keys.rs:network_getdel），主侧 AOF 恒不产
/// Setex/Psetex/Expireat/Getdel 形态的 StoreRMW 条目；从库回放后值与 TTL 与
/// 主端一致（绝对毫秒口径往返，容差 1s 覆盖 ticks→ms→ticks 的粗化）
#[test]
fn string_ttl_commands_log_only_live_entry_forms() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, wal) = open_node("cmd_forms", 1 << 20)?;
    let service = NodeService::with_wal(Arc::clone(&store), Arc::clone(&wal))?;
    let session = store.new_session()?;
    let batch = session.enter_batch();
    let mut resp = RespServerSession::default();
    let mut out = Vec::new();

    // SETEX / PSETEX：值写入 + TTL 旁路两跳
    resp
      .network_setex(&[b"seex", b"120", b"v1"], &batch, &mut out)
      .expect("SETEX 执行");
    resp
      .network_psetex(&[b"pseex", b"240000", b"v2"], &batch, &mut out)
      .expect("PSETEX 执行");
    // SET + EXPIREAT：绝对 Unix 秒在命令端换算为物理 TTL 记录
    resp
      .network_set(&[b"sexp", b"v3"], &batch, &mut out)
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
  })
}
