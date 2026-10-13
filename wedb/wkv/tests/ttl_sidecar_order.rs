#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 票 wkv-ttl-sidecar-strip-before-record-crash-window-value-immortal 回归：
//! TTL 旁路腿「主记录落笔先行、TTL 腿随后」写序
//!
//! 对标 C# 记录一体（UpsertMethods.cs:21-23 值与 Expiration 单记录一体、
//! 删除臂 DeleteMethods.cs 记录连同尾随字段一体消亡）的崩溃面不变式：
//! 任意崩溃前缀下绝不出现「TTL 已墓碑而值存活」的永生形。覆盖：
//! 1. 镜像序见证（单一镜像机制，非 mock）：SET 同步/异步臂、DEL、GETDEL
//!    的 AOF 镜像到达序均为「数据记录 → TTL 墓碑」（旧序为反序）；
//! 2. GETDEL 值契约：TTL 腿 Err 弃置不降级——取删值原样应答（异步重做对
//!    已摘键回 nil，降级即丢值）；
//! 3. 崩溃前缀恢复（checkpoint + recover 引擎唯一内容恢复入口）：定向构造
//!    「数据腿已 append、TTL 腿未执行」的持久化等价形——SET 侧残留收敛为
//!    「新值 + 旧 TTL」（有界、到期正常出账），DEL 侧残留收敛为「无主
//!    TTL 记录」良性自愈态（读面宿主缺席裁决 no-op + SET 覆写清退闭合），
//!    全前缀无一形值永生。

use std::{
  fs::create_dir_all,
  io,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use aok::{OK, Void};
use parking_lot::Mutex;
use tempfile::{TempDir, tempdir};
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wcpr::CheckpointType;
use wdev::SegmentedDevice;
use wkv::{Error as WkvError, StoreConfig, StoreEvent, StoreEventSink, WedbStore};
use wval::{KeyTag, NamespaceDbCodec};

/// 观测键前缀（多键用例共享同一对账域）
const WATCH: &[u8] = b"tso:order";
const KEY: &[u8] = b"tso:order";
const NS: u64 = 5;
const DB: u64 = 2;

/// 镜像序对账上下文：观测域内记录域 / TTL 域事件到达序 + TTL 墓碑镜像注错闩
struct OrderCtx {
  armed: AtomicBool,
  log: Mutex<Vec<(&'static str, Option<i64>)>>,
}

impl OrderCtx {
  fn new() -> Self {
    Self {
      armed: AtomicBool::new(false),
      log: Mutex::new(Vec::new()),
    }
  }
}

/// 镜像序 sink：记录域事件按物理键反解过滤观测域，TTL 域事件携用户键直达；
/// armed 期间对观测域 TTL 墓碑镜像（TtlWrite(None)）注错——物理删除已提交、
/// 镜像缺失以 Err 上抛，消费方 TTL 腿失败政策的真实注入缝
fn order_sink(ctx: &OrderCtx, _ver: i64, _aof: i32, event: StoreEvent<'_>) -> wkv::Result<()> {
  match event {
    StoreEvent::Write { key, tombstone, .. } => {
      if let Ok((_, _, tag, user_key)) = NamespaceDbCodec::decode_tagged_key(key)
        && tag == KeyTag::String
        && user_key.starts_with(WATCH)
      {
        ctx
          .log
          .lock()
          .push((if tombstone { "rec-del" } else { "rec-write" }, None));
      }
    }
    StoreEvent::TtlWrite { key, expire_at, .. } if key.starts_with(WATCH) => {
      if ctx.armed.load(Ordering::Relaxed) && expire_at.is_none() {
        return Err(WkvError::Io(io::Error::other(
          "injected ttl tombstone mirror failure",
        )));
      }
      ctx.log.lock().push(("ttl", expire_at));
    }
    _ => {}
  }
  Ok(())
}

/// 装配：真盘 wkv + 镜像序 sink（GC 关停防后台清扫干扰对账）
fn harness(
  dir_name: &str,
  ctx: Arc<OrderCtx>,
) -> aok::Result<(TempDir, Arc<WedbStore<SegmentedDevice>>)> {
  let dir = tempdir()?;
  let mut config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?;
  config.gc.enabled = false;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(dir_name))?);
  let store = Arc::new(WedbStore::open(config, device)?);
  assert!(store.set_event_sink(StoreEventSink::new(ctx, order_sink)));
  Ok((dir, store))
}

/// SET 同步臂：镜像到达序须为「数据记录写 → TTL 墓碑」（TTL 腿后置）
#[compio::test]
async fn sync_set_mirror_order_record_write_precedes_ttl_strip() -> Void {
  let ctx = Arc::new(OrderCtx::new());
  let (_dir, store) = harness("tso_set.db", Arc::clone(&ctx))?;
  let session = store.new_session()?;
  session.set_context(NS, DB);

  let exp = now_ticks() + TICKS_PER_SECOND * 600;
  session.upsert(KEY, b"old").await?;
  session.put_ttl(KEY, exp).await?;
  ctx.log.lock().clear();

  let r = session.try_upsert_sync(KEY, b"new")?;
  assert!(r.is_ok(), "可变区闭环同步 SET 须成功: {r:?}");
  assert_eq!(session.read(KEY).await?, Some(b"new".to_vec()));
  assert_eq!(session.ttl_of(KEY).await?, None, "SET 语义清 TTL");
  assert_eq!(
    ctx.log.lock().as_slice(),
    &[("rec-write", None), ("ttl", None)],
    "镜像序须为 数据记录写 → TTL 墓碑（TTL 腿后置）"
  );
  OK
}

/// SET 异步臂：同一镜像序（upsert_raw 落笔先于 del_ttl）
#[compio::test]
async fn async_upsert_mirror_order_record_write_precedes_ttl_strip() -> Void {
  let ctx = Arc::new(OrderCtx::new());
  let (_dir, store) = harness("tso_aset.db", Arc::clone(&ctx))?;
  let session = store.new_session()?;
  session.set_context(NS, DB);

  let exp = now_ticks() + TICKS_PER_SECOND * 600;
  session.upsert(KEY, b"old").await?;
  session.put_ttl(KEY, exp).await?;
  ctx.log.lock().clear();

  session.upsert(KEY, b"new").await?;
  assert_eq!(session.read(KEY).await?, Some(b"new".to_vec()));
  assert_eq!(session.ttl_of(KEY).await?, None, "SET 语义清 TTL");
  assert_eq!(
    ctx.log.lock().as_slice(),
    &[("rec-write", None), ("ttl", None)],
    "异步臂镜像序须为 数据记录写 → TTL 墓碑"
  );
  OK
}

/// DEL 同步内核：镜像到达序须为「记录墓碑 → TTL 墓碑」
#[compio::test]
async fn sync_delete_mirror_order_record_tombstone_precedes_ttl_strip() -> Void {
  let ctx = Arc::new(OrderCtx::new());
  let (_dir, store) = harness("tso_del.db", Arc::clone(&ctx))?;
  let session = store.new_session()?;
  session.set_context(NS, DB);

  let exp = now_ticks() + TICKS_PER_SECOND * 600;
  session.upsert(KEY, b"old").await?;
  session.put_ttl(KEY, exp).await?;
  ctx.log.lock().clear();

  let d = session.try_delete_sync(KEY)?;
  assert_eq!(d, Ok(true));
  assert!(!session.contains_key(KEY).await?);
  assert_eq!(session.ttl_of(KEY).await?, None, "DEL 级联清 TTL（后置）");
  assert_eq!(
    ctx.log.lock().as_slice(),
    &[("rec-del", None), ("ttl", None)],
    "镜像序须为 记录墓碑 → TTL 墓碑（TTL 腿后置）"
  );
  OK
}

/// GETDEL：取删值原样应答 + 镜像序；TTL 腿 Err（镜像注错形态）弃置不降级
/// ——值已摘除，异步重做对已墓碑键回 nil，降级即丢应答值
#[compio::test]
async fn getdel_returns_value_and_swallows_ttl_leg_failure() -> Void {
  let ctx = Arc::new(OrderCtx::new());
  let (_dir, store) = harness("tso_take.db", Arc::clone(&ctx))?;
  let session = store.new_session()?;
  session.set_context(NS, DB);

  let exp = now_ticks() + TICKS_PER_SECOND * 600;
  session.upsert(KEY, b"old").await?;
  session.put_ttl(KEY, exp).await?;
  ctx.log.lock().clear();

  let taken = session.try_take_sync(KEY)?;
  assert_eq!(
    taken,
    Ok(Some(b"old".to_vec())),
    "应答值 = 实际摘除记录的值"
  );
  assert!(!session.contains_key(KEY).await?);
  assert_eq!(session.ttl_of(KEY).await?, None);
  assert_eq!(
    ctx.log.lock().as_slice(),
    &[("rec-del", None), ("ttl", None)],
    "镜像序须为 记录墓碑 → TTL 墓碑（TTL 腿后置）"
  );

  // TTL 腿 Err 弃置（值契约优先）：armed 注错 TTL 墓碑镜像，取删值须原样返回
  let k2 = b"tso:order:2";
  session.upsert(k2, b"old2").await?;
  session.put_ttl(k2, exp).await?;
  ctx.armed.store(true, Ordering::Relaxed);
  let taken2 = session.try_take_sync(k2)?;
  ctx.armed.store(false, Ordering::Relaxed);
  assert_eq!(
    taken2,
    Ok(Some(b"old2".to_vec())),
    "TTL 腿失败须弃置（GETDEL 值已取删，严禁降级丢值）"
  );
  // 本注错形态物理删除已提交（镜像后置失败）：效果在场、仅镜像缺失
  assert!(!session.contains_key(k2).await?);
  assert_eq!(session.ttl_of(k2).await?, None);
  OK
}

/// 崩溃前缀恢复：定向构造「数据腿已 append、TTL 腿未执行」的持久化等价形
/// （直调内核同款数据腿原语后不再执行 TTL 腿 = 进程死于两 append 之间），
/// 经 checkpoint + recover 重启后逐前缀断言——全前缀无一形值永生：
/// - 前缀 0（任何 append 未发生）：键完整旧态（含 TTL）；
/// - SET 前缀 1：新值 + 残留旧 TTL（有界：到期正常出账，过期刻度形直接
///   惰性清除出账）；
/// - DEL 前缀 1：无主 TTL 记录良性自愈态（读面宿主缺席裁决 no-op，SET
///   覆写清退自愈闭合）
#[compio::test]
async fn crash_prefix_recovery_residues_never_value_immortal() -> Void {
  let dir = tempdir()?;
  let cpr_dir = dir.path().join("checkpoints");
  create_dir_all(&cpr_dir)?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("tso_crash.db"),
  )?);
  let mut config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?;
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device))?);
  let session = store.new_session()?;
  session.set_context(NS, DB);

  let future_exp = now_ticks() + TICKS_PER_SECOND * 3600;
  let past_exp = now_ticks() - TICKS_PER_SECOND * 3600;
  let ctl: &[u8] = b"tso:ctl";
  let live: &[u8] = b"tso:live";
  let dead: &[u8] = b"tso:dead";
  let gone: &[u8] = b"tso:gone";
  for (k, exp) in [
    (ctl, future_exp),
    (live, future_exp),
    (dead, future_exp),
    (gone, past_exp),
  ] {
    session.upsert(k, b"old").await?;
    session.put_ttl(k, exp).await?;
  }
  store.flush_all().await?;

  // 崩溃前缀 1 残留构造：数据腿原语直书（upsert_raw / delete_raw 与写内核
  // 数据腿同款物理原语），TTL 腿不执行
  session
    .upsert_raw(session.session_string_key(live).as_slice(), b"new")
    .await?;
  session
    .upsert_raw(session.session_string_key(gone).as_slice(), b"new")
    .await?;
  session
    .delete_raw(session.session_string_key(dead).as_slice())
    .await?;
  store.flush_all().await?;

  // 重启面走引擎唯一内容恢复入口（对位 C# RecoverCheckpointAsync）
  let token = store
    .create_checkpoint(&cpr_dir, CheckpointType::Snapshot)
    .await?
    .token;
  drop(session);
  drop(store);
  let store = Arc::new(WedbStore::recover(&cpr_dir, token, device).await?);
  // 恢复面映射权威在磁盘 DbMeta（非根域库级路由冷态在册）：上下文物化须走
  // resolve_context 点查装载（vdb_load 单点，协议层挂起面同路径）；非严格
  // set_context 直调即盲分配新 vdb 覆盖磁盘权威，会话前缀错位全键读 miss
  store.resolve_context(NS, DB).await?;
  let session = store.new_session()?;
  session.set_context(NS, DB);

  // 前缀 0：键完整旧态（含 TTL）
  assert_eq!(session.read(ctl).await?, Some(b"old".to_vec()));
  assert_eq!(session.ttl_of(ctl).await?, Some(future_exp));

  // SET 前缀 1：新值 + 残留旧 TTL——残留为「TTL 在场」形，绝非旧序
  // 「TTL 已亡而值永生」形
  assert_eq!(session.read(live).await?, Some(b"new".to_vec()));
  assert_eq!(
    session.ttl_of(live).await?,
    Some(future_exp),
    "SET 侧崩溃残留须为 TTL 在场形（有界）"
  );

  // SET 前缀 1 + 过期刻度：残留旧 TTL 到期正常出账
  assert_eq!(session.read(gone).await?, None, "残留旧 TTL 到期须裁决出账");
  assert!(!session.contains_key(gone).await?);

  // DEL 前缀 1：宿主已亡 + 无主 TTL 记录（读面宿主缺席裁决 no-op = 良性）
  assert!(!session.contains_key(dead).await?);
  assert_eq!(session.read(dead).await?, None);
  assert_eq!(
    session.ttl_of(dead).await?,
    Some(future_exp),
    "DEL 侧崩溃残留须为无主 TTL 记录（良性自愈态）"
  );

  // 自愈通道闭合：SET 覆写清退无主 TTL
  let r = session.try_upsert_sync(dead, b"reborn")?;
  assert!(r.is_ok(), "可变区闭环同步 SET 须成功: {r:?}");
  assert_eq!(session.read(dead).await?, Some(b"reborn".to_vec()));
  assert_eq!(
    session.ttl_of(dead).await?,
    None,
    "SET 覆写清退无主 TTL（自愈闭合）"
  );
  OK
}
