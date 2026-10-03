//! RENAME 迁移失败语义与并发封堵测试（store 层，C# 无对应——本仓一物两用写序
//! 的失败臂契约，逐段语义见 `range_index/migration.rs` 头注失败语义表）：
//! - 段一快照失败：dst（String / 分层 RI 两种形态）原态完整、迁移 claim 释放
//!   （旧键元记录与树全程未触），命令报错；阻塞解除后重试对齐；
//! - 段二 emit 失败（注入恒败 sink）：claim 释放、dst（String 键与不存在键两种
//!   形态）未触、快照残件清理；
//! - 段三 publish 失败（emit 已成功）：补偿 RangeIndexDrop{new_key} 已发（副本
//!   侧既有清场臂可摘除幻影树与元记录）、claim 释放后旧键恢复可写；
//! - 源键并发写封堵：迁移窗口内 RI.SET 按键缺失语义被拒（段一非持久内存迁移
//!   claim，四入口同判，与墓碑态等价），不再「ACK 后随旧树排空丢失」；迁移前
//!   已 ACK 的写入随快照收敛于新键；
//! - dst 侧并发写封堵：dst 有存活记录（有被销毁域）时段一双键成对登记 claim，
//!   窗口内对 dst 的写被显式拒绝——RI.SET 按键缺失语义（NotFound 终态拒绝）、
//!   RI.CREATE 与分层路由探测按 MigrationBusy 锁忙/重试错误（禁「视同不存在」
//!   穿透——穿透会在 dst 信封域物化重建对象或走重建路径，换一种丢失形），
//!   不再「ACK 后随 dst 旧树换入销毁 + AOF 与流块交错致主从发散」；
//! - dst 不存在键（常见 RENAME 到新键）：无被销毁域则 dst 侧不登记 claim，
//!   失败臂双键 claim 仍成对释放、重试收敛；
//! - 迁移后旧键终态：RI.SET 恒被键缺失语义拒绝（幂等零副作用），GET / EXISTS
//!   / COUNT 读侧全部消失，迟到写不泄漏进新键；
//! - 段三 dst 清退失败（信封墓碑镜像 sink 注入）：双键 claim 均释放（禁单侧
//!   泄漏令 dst claim 滞留至重启），dst 取「存活 Meta + 信封残留」双态残形
//!   （String-only dst 不登记 claim，无泄漏面），重试收敛；
//! - 同源键并发 RENAME 互斥不误删：dst 存活性采样读失败臂未持有 claim 禁调
//!   release（按 key_id 判等的移除会误删并发持有者的同源键 claim）；两处
//!   try-claim 失败臂零误删（他人持有的 new claim 完好）。
//!
//! 在 garnet 中的相对路径: libs/server/Resp/KeyAdminCommands.cs:NetworkRENAME + test/standalone/Garnet.test/RespTests.cs（RENAME/RENAMENX）

use std::{
  fs, io,
  sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
  },
};

use aok::{OK, Void};
use compio::runtime::spawn;
use tempfile::tempdir;
use wbase::{
  convert::TICKS_PER_SECOND,
  pool::{AlignedBuf, BufferPool},
  time::now_ticks,
};
use wbftree::{StorageBackendType, TreeTuning};
use wdev::{Device, Error as WdevError, SegmentedDevice};
use wkv::{
  Error as WkvError, RangeIndexError, StoreConfig, StoreEvent, StoreEventSink, TtlOpt, WedbStore,
};
use wval::KeyTag;

use crate::support::{open_store_in, tree_id_key};

/// 与 range_index 模块测试一致的默认树调优：min_record=8 / max_record=1024 /
/// max_key_len=128
const TUNE: TreeTuning = TreeTuning {
  cache_size: 65536,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

/// 构造挂载 RangeIndex 目录的会话层引擎（返回目录句柄供占位 migration-tmp）
async fn open_store(
  dir: &tempfile::TempDir,
  name: &str,
) -> aok::Result<Arc<WedbStore<SegmentedDevice>>> {
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?
    .with_range_index_dir(dir.path().join("range_indexes"));
  open_store_in(dir, name, config)
}

/// 字段值定长 16B（满足 TUNE 的 8B 记录下限）
const VAL16: &[u8; 16] = b"vvvvvvvvvvvvvvvv";

/// 源键灌入纯 RI 索引（RI.CREATE + 逐条 RI.SET）
async fn fill_ri<D: wdev::Device>(
  session: &wkv::StoreSession<D>,
  key: &[u8],
  fields: &[&[u8]],
) -> aok::Result<()> {
  session
    .range_index_create(key, StorageBackendType::Disk, TUNE)
    .await?;
  for f in fields {
    session.range_index_set(key, f, VAL16).await?;
  }
  Ok(())
}

/// 段一快照失败路径：migration-tmp 被同名普通文件占位 → 快照文件创建必败。
/// 断言 dst（String 与分层 RI 两种形态）原态完整、迁移 claim 释放后旧键元记录
/// 与树原态可读可数（claim 方案下元记录全程未触，无需回写）、命令报错；阻塞
/// 解除后重试收敛（主端重试对齐契约）
#[compio::test]
async fn rename_snapshot_failure_keeps_dst_and_restores_source() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "rename_snap_fail.db").await?;
  let session = store.new_session()?;
  // 迁移临时目录真值取自 manager（config 目录之下还有 rangeindex 一层，手拼必错）
  let migration_tmp = store.range_index.migration_temp_dir().to_path_buf();

  fill_ri(&session, b"src", &[b"f1", b"f2"]).await?;
  session.upsert(b"dst_str", b"payload").await?;
  fill_ri(&session, b"dst_ri", &[b"g1"]).await?;

  // 占位阻塞：migration-tmp 目录换成同名普通文件，快照文件创建必败
  fs::remove_dir_all(&migration_tmp)?;
  fs::write(&migration_tmp, b"")?;

  // dst 为 String 残留：命令报错、dst 原态、src 恢复
  assert!(
    session
      .rename_range_index(b"src", b"dst_str")
      .await
      .is_err(),
    "快照失败必须上抛令 RENAME 报错"
  );
  assert_eq!(
    session.read(b"dst_str").await?.as_deref(),
    Some(b"payload".as_slice()),
    "快照失败 dst String 记录必须原态完整"
  );
  assert!(
    !session.range_index_exists(b"dst_str").await?,
    "快照失败 dst 不得残留元记录"
  );
  assert_eq!(
    session.range_index_count(b"src").await?,
    2,
    "快照失败臂释放 claim 后旧键元记录必须原态可数（claim 方案元记录全程未触）"
  );
  assert_eq!(
    session.range_index_get(b"src", b"f1").await?.as_deref(),
    Some(VAL16.as_slice()),
    "恢复后的旧键树必须原样可读"
  );

  // dst 为分层 RI：同口径（dst 计数与内容原态）
  assert!(
    session.rename_range_index(b"src", b"dst_ri").await.is_err(),
    "快照失败必须上抛令 RENAME 报错"
  );
  assert_eq!(
    session.range_index_count(b"dst_ri").await?,
    1,
    "快照失败 dst 分层树必须原态完整"
  );
  assert_eq!(
    session.range_index_get(b"dst_ri", b"g1").await?.as_deref(),
    Some(VAL16.as_slice())
  );
  assert_eq!(session.range_index_count(b"src").await?, 2);

  // 阻塞解除后重试对齐：迁移收敛、源键随段五排空持久墓碑读侧消失
  fs::remove_file(&migration_tmp)?;
  fs::create_dir_all(&migration_tmp)?;
  session.rename_range_index(b"src", b"dst_str").await?;
  assert_eq!(session.range_index_count(b"dst_str").await?, 2);
  assert_eq!(
    session.range_index_get(b"dst_str", b"f2").await?.as_deref(),
    Some(VAL16.as_slice())
  );
  assert!(matches!(
    session.range_index_count(b"src").await,
    Err(RangeIndexError::NotFound)
  ));
  OK
}

/// 仅对 RangeIndexStream 恒败的注入 sink（段二 emit 失败路径专用；其余事件
/// 直通，保证建索引 / 写字段 / 裸原语清退的镜像不受干扰）
fn fail_range_index_stream_sink(
  _: &(),
  _ver: i64,
  _aof_session_id: i32,
  event: StoreEvent<'_>,
) -> wkv::Result<()> {
  match event {
    StoreEvent::RangeIndexStream { .. } => Err(WkvError::Io(io::Error::other(
      "injected RangeIndexStream emit failure",
    ))),
    _ => Ok(()),
  }
}

/// 段二 emit 失败路径：claim 释放后旧键原态可读可数（元记录全程未触）、dst
/// 未触（String 键与不存在键两种形态）、快照残件清理
#[compio::test]
async fn rename_emit_failure_restores_source_and_keeps_dst() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "rename_emit_fail.db").await?;
  // 注入恒败 sink 须先于任何会话创建
  assert!(
    store.set_event_sink(StoreEventSink::new(
      Arc::new(()),
      fail_range_index_stream_sink
    )),
    "sink 注入应成功"
  );
  let session = store.new_session()?;
  let migration_tmp = store.range_index.migration_temp_dir().to_path_buf();

  fill_ri(&session, b"src", &[b"f1", b"f2"]).await?;
  session.upsert(b"dst_str", b"payload").await?;

  let err = session
    .rename_range_index(b"src", b"dst_str")
    .await
    .expect_err("emit 失败必须上抛令 RENAME 报错");
  assert!(
    err.to_string().contains("injected"),
    "错误面应为注入的 emit 失败，实际 {err}"
  );

  // 旧键原态：claim 已释放，元记录在册 + 树原样可读
  assert_eq!(
    session.range_index_count(b"src").await?,
    2,
    "emit 失败臂释放 claim 后旧键元记录必须原态可数"
  );
  assert_eq!(
    session.range_index_get(b"src", b"f1").await?.as_deref(),
    Some(VAL16.as_slice())
  );
  // dst 为 String 键：记录原样、无元记录
  assert_eq!(
    session.read(b"dst_str").await?.as_deref(),
    Some(b"payload".as_slice()),
    "emit 失败臂 dst 必须未触"
  );
  assert!(!session.range_index_exists(b"dst_str").await?);

  // dst 为不存在键：同口径失败语义，且不得为 dst 创建任何记录
  let err = session
    .rename_range_index(b"src", b"dst_absent")
    .await
    .expect_err("emit 失败必须上抛令 RENAME 报错");
  assert!(
    err.to_string().contains("injected"),
    "错误面应为注入的 emit 失败，实际 {err}"
  );
  assert!(
    !session.range_index_exists(b"dst_absent").await?,
    "emit 失败臂不得为不存在键创建元记录"
  );
  assert_eq!(session.read(b"dst_absent").await?, None);
  assert_eq!(
    session.range_index_count(b"src").await?,
    2,
    "二次失败后旧键仍须原态可数（claim 释放）"
  );

  // 快照残件清理：两次失败臂后 migration-tmp 均不留半写文件
  let mut entries = fs::read_dir(&migration_tmp)?;
  assert!(
    entries.next().transpose()?.is_none(),
    "emit 失败臂必须弃置快照残件"
  );
  OK
}

/// 流相关事件记录 sink（测 2b 专用）：全事件直通（不干扰建索引 / 写字段 / emit），
/// 仅按序记 RangeIndexStream / RangeIndexDrop 的键，供补偿断言
type StreamLog = Arc<Mutex<Vec<(&'static str, Vec<u8>)>>>;

fn recording_sink(log: StreamLog) -> StoreEventSink {
  StoreEventSink::new(log, |log, _ver, _aof_session_id, event| {
    match event {
      StoreEvent::RangeIndexStream { key, .. } => {
        log
          .lock()
          .expect("事件日志锁")
          .push(("stream", key.to_vec()));
      }
      StoreEvent::RangeIndexDrop { key, .. } => {
        log.lock().expect("事件日志锁").push(("drop", key.to_vec()));
      }
      _ => {}
    }
    Ok(())
  })
}

/// 段三 publish 失败路径（emit 已成功，测 2b）：失败臂必须补发
/// RangeIndexDrop{new_key} 补偿事件（副本侧既有清场臂据此摘除幻影树与元记录；
/// 原缺陷：副本已按流块实时发布新键而主端回滚，主从长期发散、重启回放复现
/// 幻影）；快照残件清理、claim 释放后旧键恢复可写、dst 无元记录残留
#[compio::test]
async fn rename_post_emit_failure_compensates_replica_and_releases_claim() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "rename_publish_fail.db").await?;
  // 注入记录 sink 须先于任何会话创建
  let log: StreamLog = Arc::new(Mutex::new(Vec::new()));
  assert!(
    store.set_event_sink(recording_sink(Arc::clone(&log))),
    "sink 注入应成功"
  );
  let session = store.new_session()?;
  let migration_tmp = store.range_index.migration_temp_dir().to_path_buf();

  fill_ri(&session, b"src", &[b"f1", b"f2"]).await?;

  // 占位阻塞：dst 数据文件路径换成同名目录 → 段三换入 rename 必败（emit 已
  // 成功，进入补偿断言的目标失败臂）
  let dst_data = store
    .range_index
    .data_file_path_for_key(&tree_id_key(0, 0, b"dst"));
  fs::create_dir_all(&dst_data)?;

  session
    .rename_range_index(b"src", b"dst")
    .await
    .expect_err("publish 失败必须上抛令 RENAME 报错");

  // 补偿断言：stream(dst) 已入账，且 drop(dst) 补偿晚于流块
  let (stream_pos, drop_pos) = {
    let entries = log.lock().expect("事件日志锁");
    let stream_pos = entries
      .iter()
      .position(|(kind, key)| *kind == "stream" && key.as_slice() == b"dst");
    let drop_pos = entries
      .iter()
      .position(|(kind, key)| *kind == "drop" && key.as_slice() == b"dst");
    (stream_pos, drop_pos)
  };
  let stream_pos = stream_pos.expect("emit 已成功，流块事件必须在册");
  let drop_pos =
    drop_pos.expect("publish 失败臂必须补发 RangeIndexDrop{new_key} 补偿（副本侧可摘除）");
  assert!(drop_pos > stream_pos, "补偿必须晚于流块入账");

  // 主端回滚原态：src 完整可读可数，claim 已释放（旧键恢复可写）
  assert_eq!(session.range_index_count(b"src").await?, 2);
  assert_eq!(
    session.range_index_get(b"src", b"f1").await?.as_deref(),
    Some(VAL16.as_slice())
  );
  session.range_index_set(b"src", b"post_fail", VAL16).await?;
  assert_eq!(session.range_index_count(b"src").await?, 3);

  // dst 无元记录残留（主端未换入未落 meta），快照残件已弃置
  assert!(!session.range_index_exists(b"dst").await?);
  let mut entries = fs::read_dir(&migration_tmp)?;
  assert!(
    entries.next().transpose()?.is_none(),
    "publish 失败臂必须弃置快照残件"
  );
  OK
}

/// 源键并发写封堵：RENAME 迁移窗口内对旧键 RI.SET 被拒（NotFound 键缺失语义，
/// 段一非持久内存迁移 claim 封堵，四入口同判，与墓碑态等价），而非 ACK 后随
/// 旧树排空销毁；迁移前已 ACK 的写入随快照收敛于新键（compio 单 worker 运行时
/// 下「已过元检查、滞留锁获取前」的写者必然整体落窗内或整体被拒，断言无调度
/// 竞态）
#[compio::test]
async fn rename_concurrent_source_writes_rejected_not_lost() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "rename_concurrent.db").await?;
  let session = store.new_session()?;

  // 种子 3000 条拉长段一快照窗，给并发写采样留窗
  const SEED: usize = 3000;
  session
    .range_index_create(b"src", StorageBackendType::Disk, TUNE)
    .await?;
  let mut chunk: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(500);
  for i in 0..SEED {
    let f = format!("s{i:05}");
    chunk.push((f.clone().into_bytes(), f.into_bytes()));
    if chunk.len() == 500 {
      let refs: Vec<(&[u8], &[u8])> = chunk
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();
      session.range_index_set_batch(b"src", &refs).await?;
      chunk.clear();
    }
  }

  // 并发写循环：首个 NotFound（键缺失语义）即停
  let writer = spawn(async move {
    let wsession = store.new_session()?;
    let mut acked = Vec::new();
    let mut rejected = None;
    for i in 0..50_000u32 {
      let f = format!("w{i:05}");
      match wsession
        .range_index_set(b"src", f.as_bytes(), f.as_bytes())
        .await
      {
        Ok(_) => acked.push(f),
        Err(e @ RangeIndexError::NotFound) => {
          rejected = Some(e);
          break;
        }
        Err(e) => return aok::Result::<_>::Err(e.into()),
      }
    }
    aok::Result::<_>::Ok((rejected.expect("迁移窗口内必须出现键缺失拒绝"), acked))
  });
  session.rename_range_index(b"src", b"dst").await?;
  let (rejected, acked) = writer.await.expect("并发写任务异常退出")?;

  // 窗口内写入被按键缺失语义拒绝，而非 ACK 后丢失
  assert!(
    matches!(rejected, RangeIndexError::NotFound),
    "窗口内写入必须按键缺失语义拒绝，实际 {rejected}"
  );

  // 全部 ACK 写入必须随快照收敛于新键（丢失即红）
  for f in &acked {
    assert_eq!(
      session
        .range_index_get(b"dst", f.as_bytes())
        .await?
        .as_deref(),
      Some(f.as_bytes()),
      "ACK 写入 {f:?} 随旧树排空丢失"
    );
  }
  // 种子数据完整迁移
  assert!(
    session.range_index_count(b"dst").await? >= SEED,
    "新键计数不得低于种子规模"
  );
  for f in ["s00000", "s01500", "s02999"] {
    assert_eq!(
      session
        .range_index_get(b"dst", f.as_bytes())
        .await?
        .as_deref(),
      Some(f.as_bytes())
    );
  }
  // 源键读侧消失（段五排空持久墓碑，claim 已释放）
  assert!(matches!(
    session.range_index_count(b"src").await,
    Err(RangeIndexError::NotFound)
  ));
  OK
}

/// dst 侧并发写封堵：RENAME 迁移窗口内对存活目标键（有被销毁域 → 段一双键
/// 成对登记 claim）的写被显式拒绝，而非 ACK 后随 dst 旧树换入销毁——RI.SET
/// 按键缺失语义（NotFound，终态拒绝零穿透）；RI.CREATE 与分层路由探测按
/// MigrationBusy 锁忙/重试错误（禁「视同不存在」穿透——穿透会在 dst 信封域
/// 物化重建对象或走重建路径，换一种丢失形）。迁移前已 ACK 的 dst 写入属
/// RENAME 覆写语义随旧树换弃，不得泄漏进新树；成功臂双键 claim 成对释放
///（compio 单 worker 运行时下断言无调度竞态，形态同源键并发写测试）
#[compio::test]
async fn rename_concurrent_dst_writes_rejected_not_lost() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "rename_dst_concurrent.db").await?;
  let session = store.new_session()?;

  // 源键种子 3000 条拉长段一快照窗，给并发写采样留窗；dst 为存活 RI 键
  const SEED: usize = 3000;
  session
    .range_index_create(b"src", StorageBackendType::Disk, TUNE)
    .await?;
  let mut chunk: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(500);
  for i in 0..SEED {
    let f = format!("s{i:05}");
    chunk.push((f.clone().into_bytes(), f.into_bytes()));
    if chunk.len() == 500 {
      let refs: Vec<(&[u8], &[u8])> = chunk
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();
      session.range_index_set_batch(b"src", &refs).await?;
      chunk.clear();
    }
  }
  fill_ri(&session, b"dst", &[b"g1"]).await?;

  // 并发写循环：首个 NotFound（键缺失语义）即采样其余两写面拒绝形态后停
  let wstore = Arc::clone(&store);
  let writer = spawn(async move {
    let wsession = wstore.new_session()?;
    let mut rejected = false;
    for i in 0..50_000u32 {
      let f = format!("w{i:05}");
      match wsession
        .range_index_set(b"dst", f.as_bytes(), f.as_bytes())
        .await
      {
        Ok(_) => {}
        Err(RangeIndexError::NotFound) => {
          assert!(
            matches!(
              wsession
                .range_index_create(b"dst", StorageBackendType::Disk, TUNE)
                .await,
              Err(RangeIndexError::Store(ref e)) if matches!(**e, WkvError::MigrationBusy)
            ),
            "RI.CREATE 在迁移窗内必须按迁移忙显式拒绝（禁重建路径穿透）"
          );
          assert!(
            matches!(
              wsession.load_collection_stub(b"dst").await,
              Err(WkvError::MigrationBusy)
            ),
            "分层路由探测在迁移窗内必须按迁移忙显式拒绝（禁物化穿透）"
          );
          rejected = true;
          break;
        }
        Err(e) => return aok::Result::<_>::Err(e.into()),
      }
    }
    aok::Result::<_>::Ok(rejected)
  });
  session.rename_range_index(b"src", b"dst").await?;
  let rejected = writer.await.expect("并发写任务异常退出")?;

  assert!(
    rejected,
    "迁移窗口内必须出现 dst 侧显式拒绝（NotFound 键缺失语义）"
  );

  // 成功臂双键 claim 均已释放
  assert!(
    !store
      .range_index
      .migration_claimed(&tree_id_key(0, 0, b"src"))
  );
  assert!(
    !store
      .range_index
      .migration_claimed(&tree_id_key(0, 0, b"dst"))
  );

  // 新树内容 = 源树内容，精确无泄漏（窗口内 dst 写零落新树，dst 旧树内容
  // 随换入消亡）
  assert_eq!(session.range_index_count(b"dst").await?, SEED);
  for f in ["s00000", "s01500", "s02999"] {
    assert_eq!(
      session
        .range_index_get(b"dst", f.as_bytes())
        .await?
        .as_deref(),
      Some(f.as_bytes())
    );
  }
  assert_eq!(
    session.range_index_get(b"dst", b"g1").await?,
    None,
    "dst 旧树内容必须随换入消亡"
  );

  // 迁移后 dst 恢复可写（claim 已释放，被拒写者重试即收敛），源键读侧消失
  session.range_index_set(b"dst", b"post", VAL16).await?;
  assert_eq!(session.range_index_count(b"dst").await?, SEED + 1);
  assert!(matches!(
    session.range_index_count(b"src").await,
    Err(RangeIndexError::NotFound)
  ));
  OK
}

/// dst 不存在键（常见 RENAME 到新键）：无被销毁域则 dst 侧不登记 claim——
/// 快照失败臂双键 claim 成对释放、dst 全程未触；阻塞解除后重试收敛（无 claim
/// 路径回归）
#[compio::test]
async fn rename_absent_dst_claims_nothing_and_recovers() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "rename_absent_dst.db").await?;
  let session = store.new_session()?;
  let migration_tmp = store.range_index.migration_temp_dir().to_path_buf();

  fill_ri(&session, b"src", &[b"f1", b"f2"]).await?;

  // 占位阻塞：migration-tmp 目录换成同名普通文件，快照文件创建必败（此窗
  // dst 不存在 → dst 侧不登记 claim）
  fs::remove_dir_all(&migration_tmp)?;
  fs::write(&migration_tmp, b"")?;

  assert!(
    session
      .rename_range_index(b"src", b"dst_absent")
      .await
      .is_err(),
    "快照失败必须上抛令 RENAME 报错"
  );
  // 失败臂双键成对释放：src 与 dst_absent 均无 claim 残留
  assert!(
    !store
      .range_index
      .migration_claimed(&tree_id_key(0, 0, b"src"))
  );
  assert!(
    !store
      .range_index
      .migration_claimed(&tree_id_key(0, 0, b"dst_absent"))
  );
  // dst 全程未触
  assert!(!session.range_index_exists(b"dst_absent").await?);

  // 阻塞解除后重试收敛（无 claim 路径回归）
  fs::remove_file(&migration_tmp)?;
  fs::create_dir_all(&migration_tmp)?;
  session.rename_range_index(b"src", b"dst_absent").await?;
  assert_eq!(session.range_index_count(b"dst_absent").await?, 2);
  assert_eq!(
    session
      .range_index_get(b"dst_absent", b"f1")
      .await?
      .as_deref(),
    Some(VAL16.as_slice())
  );
  assert!(matches!(
    session.range_index_count(b"src").await,
    Err(RangeIndexError::NotFound)
  ));
  assert!(
    !store
      .range_index
      .migration_claimed(&tree_id_key(0, 0, b"dst_absent"))
  );
  OK
}

/// 迁移成功后旧键终态：RI.SET 恒被键缺失语义拒绝（段五排空持久墓碑 + claim
/// 释放后的终态封堵面，非 WrongType——旧键无任何存活记录可撞类型门），GET /
/// EXISTS / COUNT 读侧全部消失；迟到写零副作用，不泄漏进新键
#[compio::test]
async fn rename_old_key_terminal_state_rejects_late_writes() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "rename_terminal.db").await?;
  let session = store.new_session()?;

  fill_ri(&session, b"src", &[b"f1", b"f2"]).await?;
  session.rename_range_index(b"src", b"dst").await?;

  // 新键完整换入
  assert_eq!(session.range_index_count(b"dst").await?, 2);
  assert_eq!(
    session.range_index_get(b"dst", b"f1").await?.as_deref(),
    Some(VAL16.as_slice())
  );

  // 迟到写幂等被拒：键缺失语义
  for field in [b"late1".as_slice(), b"late2".as_slice()] {
    assert!(
      matches!(
        session.range_index_set(b"src", field, VAL16).await,
        Err(RangeIndexError::NotFound)
      ),
      "迁移后旧键 RI.SET 必须按键缺失语义拒绝"
    );
  }

  // 读侧全消：GET 无值 / EXISTS false / COUNT 走同一元记录门禁报 NotFound
  assert_eq!(session.read(b"src").await?, None);
  assert!(!session.range_index_exists(b"src").await?);
  assert!(!session.contains_key(b"src").await?);
  assert!(matches!(
    session.range_index_count(b"src").await,
    Err(RangeIndexError::NotFound)
  ));

  // 拒绝零副作用：新键计数与内容不受迟到写扰动
  assert_eq!(session.range_index_count(b"dst").await?, 2);
  assert_eq!(
    session.range_index_get(b"dst", b"late1").await?,
    None,
    "迟到写不得泄漏进新键"
  );
  OK
}

/// 设备故障注入开关共享态（fail_*：0 = 关闭，N = 自第 N 次对应 I/O 起恒败，
/// 粘性；reads/writes 为设备侧 I/O 计数，开关关闭期间不计数）。测试持 Arc
/// 句柄随时拨动
struct InjectSwitches {
  fail_read_from: AtomicU64,
  fail_write_from: AtomicU64,
  reads: AtomicU64,
  writes: AtomicU64,
}

/// 单文件设备故障注入包装（全方法委托 SegmentedDevice，读写按计数定点恒败）。
/// 仅包主存 hlog/waof 设备——RI 树文件走 wbftree 自有存储不经此包装，注入
/// 计数不受树侧 I/O 干扰
struct InjectFailDevice {
  inner: SegmentedDevice,
  switches: Arc<InjectSwitches>,
}

impl InjectFailDevice {
  /// 注入判定：开关关闭直接放行（不计数）；开启时第 N 次起恒败
  fn tripped(counter: &AtomicU64, from: &AtomicU64) -> bool {
    match from.load(Ordering::Relaxed) {
      0 => false,
      n => counter.fetch_add(1, Ordering::Relaxed) + 1 >= n,
    }
  }
}

impl Device for InjectFailDevice {
  fn sector_size(&self) -> usize {
    self.inner.sector_size()
  }

  fn segment_size(&self) -> u64 {
    self.inner.segment_size()
  }

  fn direct_io(&self) -> bool {
    self.inner.direct_io()
  }

  fn start_segment(&self) -> u32 {
    self.inner.start_segment()
  }

  fn end_segment(&self) -> Option<u32> {
    self.inner.end_segment()
  }

  fn capacity(&self) -> Option<u64> {
    self.inner.capacity()
  }

  fn pool(&self) -> &Arc<BufferPool> {
    self.inner.pool()
  }

  async fn write_aligned(&self, offset: u64, buf: AlignedBuf) -> (wdev::Result<usize>, AlignedBuf) {
    if Self::tripped(&self.switches.writes, &self.switches.fail_write_from) {
      return (
        Err(WdevError::Io(io::Error::other(
          "injected device write failure",
        ))),
        buf,
      );
    }
    self.inner.write_aligned(offset, buf).await
  }

  async fn read_aligned(&self, offset: u64, buf: AlignedBuf) -> (wdev::Result<usize>, AlignedBuf) {
    if Self::tripped(&self.switches.reads, &self.switches.fail_read_from) {
      return (
        Err(WdevError::Io(io::Error::other(
          "injected device read failure",
        ))),
        buf,
      );
    }
    self.inner.read_aligned(offset, buf).await
  }

  async fn read_raw(&self, offset: u64, buf: AlignedBuf) -> (wdev::Result<usize>, AlignedBuf) {
    if Self::tripped(&self.switches.reads, &self.switches.fail_read_from) {
      return (
        Err(WdevError::Io(io::Error::other(
          "injected device read failure",
        ))),
        buf,
      );
    }
    self.inner.read_raw(offset, buf).await
  }

  async fn sync(&self) -> wdev::Result<()> {
    self.inner.sync().await
  }

  fn get_file_size(&self, segment_id: u32) -> wdev::Result<u64> {
    self.inner.get_file_size(segment_id)
  }

  async fn remove_segment(&self, segment_id: u32) -> wdev::Result<()> {
    self.inner.remove_segment(segment_id).await
  }

  async fn truncate_until_segment(&self, segment_id: u32) -> wdev::Result<()> {
    self.inner.truncate_until_segment(segment_id).await
  }

  fn reset(&self) {
    self.inner.reset();
  }

  fn recover(&self) -> wdev::Result<()> {
    self.inner.recover()
  }
}

/// 构造挂载 RangeIndex 目录的故障注入引擎（返回注入开关供测试拨动）
async fn open_injected_store(
  dir: &tempfile::TempDir,
  name: &str,
) -> aok::Result<(Arc<WedbStore<InjectFailDevice>>, Arc<InjectSwitches>)> {
  let switches = Arc::new(InjectSwitches {
    fail_read_from: AtomicU64::new(0),
    fail_write_from: AtomicU64::new(0),
    reads: AtomicU64::new(0),
    writes: AtomicU64::new(0),
  });
  let inner = SegmentedDevice::single_file(dir.path().join(name))?;
  let device = InjectFailDevice {
    inner,
    switches: Arc::clone(&switches),
  };
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?
    .with_range_index_dir(dir.path().join("range_indexes"));
  Ok((
    Arc::new(WedbStore::open(config, Arc::new(device))?),
    switches,
  ))
}

/// 仅对 dst 信封域墓碑镜像恒败的注入 sink（段三清退失败臂专用）：清退臂
/// delete_raw(env_k) 命中存活记录后经写监听口发物理墓碑镜像，sink 在此恒败
/// 即 clear_res Err 上抛失败臂。其余事件直通——RangeIndexStream 须成功入账
/// （流块先于清退），setup 写镜像（tombstone=false）不受干扰
fn fail_dst_env_tombstone_sink(
  _: &(),
  _ver: i64,
  _aof_session_id: i32,
  event: StoreEvent<'_>,
) -> wkv::Result<()> {
  match event {
    StoreEvent::Write {
      key,
      tombstone: true,
      ..
    } if key.ends_with(b"dst") => Err(WkvError::Io(io::Error::other(
      "injected dst envelope tombstone failure",
    ))),
    _ => Ok(()),
  }
}

/// 段三 dst 清退失败臂：双键 claim 成对释放（原缺陷只释放 old 侧——dst 已登记
/// claim 时清退失败即泄漏至进程重启，dst 四判点全拒、重试 RENAME 必败）。
/// dst 须取「双态残留键」形态：存活 Meta 记录（dst 侧才登记 claim）+ 信封旧
/// 快照残留（升阶崩溃残形，段三清退可触）——String-only dst 不登记 claim，无
/// 泄漏面，不在本臂射程。注入形态：选择性失败 sink 打掉 dst 信封域墓碑镜像。
/// 断言：错误面为注入失败、src 原态可数、双键 claim 均已释放；重试收敛
#[compio::test]
async fn rename_dst_clear_failure_releases_both_claims() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "rename_clear_fail.db").await?;
  // 注入选择性恒败 sink 须先于任何会话创建
  assert!(
    store.set_event_sink(StoreEventSink::new(
      Arc::new(()),
      fail_dst_env_tombstone_sink
    )),
    "sink 注入应成功"
  );
  let session = store.new_session()?;

  fill_ri(&session, b"src", &[b"f1", b"f2"]).await?;
  fill_ri(&session, b"dst", &[b"g1"]).await?;
  // 双态残留：存活 Meta 之上塞回信封旧快照（等价升阶信封删除臂失败/崩溃窗）
  session
    .upsert_tag(b"dst", KeyTag::ObjectEnvelope, b"stale-envelope")
    .await?;

  let err = session
    .rename_range_index(b"src", b"dst")
    .await
    .expect_err("清退失败必须上抛令 RENAME 报错");
  assert!(
    err.to_string().contains("injected dst envelope tombstone"),
    "错误面应为注入的信封墓碑镜像失败（他臂失败即措辞不符），实际 {err}"
  );

  // 条 1 断言：双键 claim 均已释放（dst 单侧泄漏即红）
  assert!(
    !store
      .range_index
      .migration_claimed(&tree_id_key(0, 0, b"src")),
    "old 键 claim 必须释放"
  );
  assert!(
    !store
      .range_index
      .migration_claimed(&tree_id_key(0, 0, b"dst")),
    "dst 键 claim 必须随失败臂成对释放，滞留即四判点全拒、重试必败"
  );

  // 主端原态：src 元记录在册可数（清退臂错误不遮蔽 dst 已发生部分，不回写）
  assert_eq!(session.range_index_count(b"src").await?, 2);

  // 重试收敛：信封残留已随首次失败臂墓碑化，二次清退幂等零通知直通
  session.rename_range_index(b"src", b"dst").await?;
  assert_eq!(session.range_index_count(b"dst").await?, 2);
  assert!(matches!(
    session.range_index_count(b"src").await,
    Err(RangeIndexError::NotFound)
  ));
  assert!(
    !store
      .range_index
      .migration_claimed(&tree_id_key(0, 0, b"src"))
  );
  assert!(
    !store
      .range_index
      .migration_claimed(&tree_id_key(0, 0, b"dst"))
  );
  OK
}

/// 同源键并发 RENAME 互斥不误删（条 2）：A 持 src claim 进行中，B（同源键）在
/// dst 存活性采样读失败——原缺陷臂在未持有任何 claim 时无条件 release(old_key)，
/// 按 key_id 判等的移除误删 A 的 claim（A 段一复核随即伪败，第三者抢注后 A
/// 收尾再级联误删）。注入：src/dst 元记录转冷，自第 2 次设备读起恒败（#1
/// rename 头读放行，#2 落在采样读）。断言：B 上抛且 A 的 claim 完好、dst 从未
/// 登记；A 收尾（手动释放）后真实 RENAME 收敛
#[compio::test]
async fn rename_sampling_failure_keeps_concurrent_claim_intact() -> Void {
  let dir = tempdir()?;
  let (store, switches) = open_injected_store(&dir, "rename_sample_fail.db").await?;
  let session = store.new_session()?;

  fill_ri(&session, b"src", &[b"f1", b"f2"]).await?;
  // dst 为存活 RI 键：元记录在场 → 采样读为真实磁盘读（可注入）
  fill_ri(&session, b"dst", &[b"g1"]).await?;
  store.flush_and_evict_all().await?;

  // 模拟并发 RENAME A 已持 src claim
  assert!(
    store
      .range_index
      .try_claim_migration(&tree_id_key(0, 0, b"src")),
    "A 登记 claim"
  );
  switches.fail_read_from.store(2, Ordering::Relaxed);
  let err = session
    .rename_range_index(b"src", b"dst")
    .await
    .expect_err("采样读失败必须上抛令 RENAME 报错");
  assert!(
    err.to_string().contains("injected"),
    "错误面应为注入的采样读失败，实际 {err}"
  );

  // 条 2 断言：未持有即不释放——A 的 claim 完好（误删即红），dst 从未登记
  assert!(
    store
      .range_index
      .migration_claimed(&tree_id_key(0, 0, b"src")),
    "采样失败臂禁误删并发持有者的同源键 claim"
  );
  assert!(
    !store
      .range_index
      .migration_claimed(&tree_id_key(0, 0, b"dst"))
  );

  // A 收尾释放后系统收敛：真实 RENAME 成功（dst 旧树随换入消亡）
  switches.fail_read_from.store(0, Ordering::Relaxed);
  store
    .range_index
    .release_migration_claim(&tree_id_key(0, 0, b"src"));
  session.rename_range_index(b"src", b"dst").await?;
  assert_eq!(session.range_index_count(b"dst").await?, 2);
  assert_eq!(session.range_index_get(b"dst", b"g1").await?, None);
  assert!(matches!(
    session.range_index_count(b"src").await,
    Err(RangeIndexError::NotFound)
  ));
  assert!(
    !store
      .range_index
      .migration_claimed(&tree_id_key(0, 0, b"src"))
  );
  assert!(
    !store
      .range_index
      .migration_claimed(&tree_id_key(0, 0, b"dst"))
  );
  OK
}

/// 同源键互斥两处 try-claim 失败臂零误删：release 按 key_id 判等移除、不校验
/// 持有者——旧键判点失败零释放直接上抛；dst 判点失败仅单侧释放自持 old（成对
/// 闭包在此臂会误删并发持有者的 new claim，其复核随即伪败）
#[compio::test]
async fn rename_claim_try_failure_never_deletes_others_claims() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "rename_claim_mutex.db").await?;
  let session = store.new_session()?;

  fill_ri(&session, b"src", &[b"f1", b"f2"]).await?;
  fill_ri(&session, b"dst", &[b"g1"]).await?;

  // 并发 A 持 src：B 旧键判点 try 失败——零释放直接上抛，A 的 claim 完好
  assert!(
    store
      .range_index
      .try_claim_migration(&tree_id_key(0, 0, b"src"))
  );
  let err = session
    .rename_range_index(b"src", b"dst")
    .await
    .expect_err("旧键 claim 被持必须报错");
  assert!(
    err.to_string().contains("旧键迁移已被并发 RENAME 持有"),
    "实际 {err}"
  );
  assert!(
    store
      .range_index
      .migration_claimed(&tree_id_key(0, 0, b"src")),
    "try 失败臂禁释放他人 claim"
  );
  store
    .range_index
    .release_migration_claim(&tree_id_key(0, 0, b"src"));

  // 并发 A 持 dst：B dst 判点 try 失败——仅单侧释放自持 old，他人 new 完好
  assert!(
    store
      .range_index
      .try_claim_migration(&tree_id_key(0, 0, b"dst"))
  );
  let err = session
    .rename_range_index(b"src", b"dst")
    .await
    .expect_err("dst claim 被持必须报错");
  assert!(
    err.to_string().contains("目标键迁移已被并发 RENAME 持有"),
    "实际 {err}"
  );
  assert!(
    !store
      .range_index
      .migration_claimed(&tree_id_key(0, 0, b"src")),
    "自持 old 须释放"
  );
  assert!(
    store
      .range_index
      .migration_claimed(&tree_id_key(0, 0, b"dst")),
    "他人持有的 new claim 禁误删（成对闭包误删即其复核伪败）"
  );
  store
    .range_index
    .release_migration_claim(&tree_id_key(0, 0, b"dst"));

  // 阻塞解除后重试收敛
  session.rename_range_index(b"src", b"dst").await?;
  assert_eq!(session.range_index_count(b"dst").await?, 2);
  assert_eq!(session.range_index_get(b"dst", b"g1").await?, None);
  assert!(matches!(
    session.range_index_count(b"src").await,
    Err(RangeIndexError::NotFound)
  ));
  OK
}

/// 迁移窗内字符串写零副作用（agy-r7-db 条 2 / agy-r7-design 条 1 判点前移收口）：
/// claim 窗内对被 claim 键（存活元记录 + key 级 TTL 旁路记录在册）的 SET 覆写
/// 与 DEL 在 wkv 写内核入口即拒——同步快径两臂借降级臂 Ok(Err(u64::MAX))、
/// 异步闭环两臂显式 MigrationBusy，全部零副作用。判点若后置于 TTL/信封清退
///（轮 6 旧形），被拒 SET/DEL 先剥旧键 TTL 再被拒：段二 emit 失败臂按失败
/// 语义表承诺「旧键原态完整」，TTL 却已亡而数据存活（永生键）——本测终态
/// TTL 刻度比对即判点位置的直接判据。
#[compio::test]
async fn rename_window_string_writes_rejected_zero_side_effect() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "rename_str_zero_side_effect.db").await?;
  // 注入恒败 sink（段二 emit 失败臂）：失败后旧键原态完整承诺可断言
  assert!(store.set_event_sink(StoreEventSink::new(
    Arc::new(()),
    fail_range_index_stream_sink
  )));
  let session = store.new_session()?;

  // 源键种子拉长段一快照窗（窗内采样期）；dst 为不存在键（免 dst claim 干扰）
  const SEED: usize = 3000;
  session
    .range_index_create(b"src", StorageBackendType::Disk, TUNE)
    .await?;
  let mut chunk: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(500);
  for i in 0..SEED {
    let f = format!("s{i:05}");
    chunk.push((f.clone().into_bytes(), f.into_bytes()));
    if chunk.len() == 500 {
      let refs: Vec<(&[u8], &[u8])> = chunk
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();
      session.range_index_set_batch(b"src", &refs).await?;
      chunk.clear();
    }
  }
  // 被 claim 键挂 key 级 TTL：TTL 旁路记录正是被拒写的破坏靶面
  let far = now_ticks() + 100 * TICKS_PER_SECOND;
  assert_eq!(session.expire_at(b"src", far, TtlOpt::NONE).await?, 1);
  let ttl_before = session.ttl_of(b"src").await?.expect("TTL 应在册");

  // 并发任务：只读窗信号（load_collection_stub 显式 MigrationBusy = claim
  // 在册）后立即采样四写臂拒绝形态——四采样臂判点前置后零 await 快败，
  // 单 worker 下采样相对迁移臂原子，无窗关闭竞态
  let wstore = Arc::clone(&store);
  let writer = spawn(async move {
    let wsession = wstore.new_session()?;
    let mut signaled = false;
    for _ in 0..200_000 {
      if matches!(
        wsession.load_collection_stub(b"src").await,
        Err(WkvError::MigrationBusy)
      ) {
        signaled = true;
        break;
      }
    }
    assert!(signaled, "必须进入迁移 claim 窗（信号轮询不得耗尽）");
    assert!(
      matches!(
        wsession.upsert(b"src", b"v").await,
        Err(WkvError::MigrationBusy)
      ),
      "异步 SET 在迁移窗内必须显式 MigrationBusy（入口即拒零副作用）"
    );
    assert!(
      matches!(wsession.delete(b"src").await, Err(WkvError::MigrationBusy)),
      "异步 DEL 在迁移窗内必须显式 MigrationBusy（禁穿透销毁旁域记录）"
    );
    assert!(
      matches!(wsession.try_upsert_sync(b"src", b"v"), Ok(Err(u64::MAX))),
      "同步 SET 快径在迁移窗内必须借降级臂拒绝"
    );
    assert!(
      matches!(wsession.try_delete_sync(b"src"), Ok(Err(u64::MAX))),
      "同步 DEL 快径在迁移窗内必须借降级臂拒绝"
    );
    aok::Result::<_>::Ok(())
  });
  let err = session
    .rename_range_index(b"src", b"dst_absent")
    .await
    .expect_err("emit 失败必须上抛令 RENAME 报错");
  assert!(err.to_string().contains("injected"), "实际 {err}");
  writer.await.expect("并发写任务异常退出")?;

  // 失败臂「旧键原态完整」承诺：TTL 旁路记录零损伤 + 元记录/树原样
  //（判点后置旧形在此红：被拒 SET/DEL 已先行 del_ttl → 永生键）
  assert_eq!(
    session.ttl_of(b"src").await?,
    Some(ttl_before),
    "被拒 SET/DEL 不得剥旧键 TTL 旁路记录"
  );
  assert_eq!(
    session.range_index_count(b"src").await?,
    SEED,
    "旧键元记录必须原态可数"
  );
  assert_eq!(
    session.range_index_get(b"src", b"s00000").await?.as_deref(),
    Some(b"s00000".as_slice()),
    "旧键树必须原样可读"
  );
  OK
}

/// Memory 后端树 RENAME 门禁：RI.CREATE MEMORY 后 RENAME 显式报错（C#
/// SnapshotForMigration memory-only 拒绝对位，文案与 CLUSTER MIGRATE 快照门禁
/// 共用单点常量），dst（String 残留与不存在键两形态）与旧键原态完整——门禁
/// 位于存根读出后的前置早退臂，迁移 claim 未登记、快照未产出，零补偿零残留；
/// DISK 键对照臂确认门禁不误伤正常迁移
#[compio::test]
async fn rename_memory_backend_rejected_keeps_source_and_dst() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "rename_memory_gate.db").await?;
  let session = store.new_session()?;

  session
    .range_index_create(b"mem", StorageBackendType::Memory, TUNE)
    .await?;
  session.range_index_set(b"mem", b"f1", VAL16).await?;
  session.upsert(b"dst_str", b"payload").await?;

  // dst 为 String 残留：显式报错且文案对齐单点常量，dst 原态完整
  let err = session
    .rename_range_index(b"mem", b"dst_str")
    .await
    .expect_err("Memory 后端树 RENAME 必须显式报错");
  assert!(
    err
      .to_string()
      .contains("memory-only trees cannot be migrated"),
    "错误文案须对齐 SnapshotForMigration 单点常量，实际: {err}"
  );
  assert_eq!(
    session.read(b"dst_str").await?.as_deref(),
    Some(b"payload".as_slice()),
    "门禁早退 dst String 记录必须原态完整"
  );
  assert!(
    !session.range_index_exists(b"dst_str").await?,
    "门禁早退不得残留 dst 元记录"
  );

  // 旧键原态：claim 未登记无封堵残留，计数与内容可读
  assert_eq!(session.range_index_count(b"mem").await?, 1);
  assert_eq!(
    session.range_index_get(b"mem", b"f1").await?.as_deref(),
    Some(VAL16.as_slice()),
    "门禁早退后旧键树必须原样可读"
  );

  // dst 为不存在键：同口径显式报错、零残留
  assert!(
    session
      .rename_range_index(b"mem", b"dst_absent")
      .await
      .is_err()
  );
  assert!(
    !session.range_index_exists(b"dst_absent").await?,
    "门禁早退不得在 dst 物化任何记录"
  );
  assert_eq!(session.range_index_count(b"mem").await?, 1);

  // DISK 对照臂：门禁不误伤正常迁移
  fill_ri(&session, b"src", &[b"g1", b"g2"]).await?;
  session.rename_range_index(b"src", b"dst_str").await?;
  assert_eq!(session.range_index_count(b"dst_str").await?, 2);
  assert_eq!(
    session.range_index_get(b"dst_str", b"g2").await?.as_deref(),
    Some(VAL16.as_slice())
  );
  // Memory 旧键与本次 DISK 迁移无涉，门禁早退后始终原态
  assert_eq!(session.range_index_count(b"mem").await?, 1);
  OK
}
