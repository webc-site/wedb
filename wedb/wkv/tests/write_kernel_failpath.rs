//! 写回内核失败路径与镜像时序回归（票 zcode-r34-writekernel 四条目）
//!
//! 覆盖（对标 C# 记录一体写入的「任意一步失败键保持完整旧态」不变式与
//! PostInitialWriter/PostInitialDeleter 的单一「生效后镜像」次序）：
//! 1. 条目一：SET 覆写带 TTL 键遇写镜像硬失败 → 预读的原过期刻度回填补偿
//!    （TtlWrite(Some) 补偿条目镜像入 AOF），旧值不失 TTL 判死依据而永生；
//!    解除注错重试闭环幂等清 TTL；
//! 2. 条目一：SET 覆写对象信封键遇写镜像硬失败 → 信封清退重排于数据提交
//!    之后，对象数据零销毁；重试成功后信封清退闭环；
//! 3. 条目二：等长原位臂写镜像失败 = 「已生效 + 镜像缺失」Err 契约——值
//!    字节已物理生效，不伪装 None（无 RCU 双写）、AOF 零条目；
//! 4. 条目三：AOF 镜像恰一次——并发同键写删竞争下镜像事件数 == 成功操作数
//!    （盲追加 CAS 败帧的暂存复用/弃置重试不入镜像）；
//! 5. 条目四：冷数据删除同步臂返回精确降级哨兵（u64::MAX），异步慢路径闭环
//!    幂等完成且墓碑镜像恰一条，哨兵不误入页驱逐循环。

use std::{
  io,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread,
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use parking_lot::Mutex;
use tempfile::tempdir;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wdev::SegmentedDevice;
use wkv::{Error as WkvError, StoreConfig, StoreEvent, StoreEventSink, WedbStore};
use wval::{KeyTag, NamespaceDbCodec};

const KEY: &[u8] = b"writekernel:failpath";
const NS: u64 = 3;
const DB: u64 = 1;

/// TTL 事件对账日志：按到达序记录 (ns,db,KEY) 的 expire_at（None = 墓碑）
type TtlLog = Mutex<Vec<Option<i64>>>;

/// 测试 1/2 注错上下文：armed 期间对 KEY 的 String 域写镜像恒败；TTL 域镜像
/// 放行但对账入日志（TtlWrite 是 TTL 旁路记录的写监听形态，与数据 Write 分流）。
/// string_key 为会话 set_context 后的实际物理键（经虚拟域映射，与裸
/// NamespaceDbCodec::encode_tagged_key 不同源），由测试在 set_context 后回填
struct SetFaultCtx {
  armed: AtomicBool,
  ttl_log: TtlLog,
  string_key: Mutex<Option<Vec<u8>>>,
}

fn set_string_write_fault(
  ctx: &SetFaultCtx,
  _ver: i64,
  _aof_session_id: i32,
  event: StoreEvent<'_>,
) -> wkv::Result<()> {
  match event {
    StoreEvent::Write { key, .. } => {
      if ctx.armed.load(Ordering::Relaxed)
        && ctx
          .string_key
          .lock()
          .as_ref()
          .is_some_and(|k| key == k.as_slice())
      {
        return Err(WkvError::Io(io::Error::other(
          "injected string write-mirror failure",
        )));
      }
    }
    StoreEvent::TtlWrite { key, expire_at, .. } if key == KEY => {
      ctx.ttl_log.lock().push(expire_at);
    }
    _ => {}
  }
  Ok(())
}

/// 测试 3 注错上下文：armed 期间对 KEY 的 TTL 值镜像（TtlWrite(Some)）恒败
fn ttl_write_fault(
  armed: &AtomicBool,
  _ver: i64,
  _aof_session_id: i32,
  event: StoreEvent<'_>,
) -> wkv::Result<()> {
  if let StoreEvent::TtlWrite {
    key,
    expire_at: Some(_),
    ..
  } = event
    && armed.load(Ordering::Relaxed)
    && key == KEY
  {
    return Err(WkvError::Io(io::Error::other(
      "injected ttl write-mirror failure",
    )));
  }
  Ok(())
}

/// 装配：真盘 wkv + 注错 sink（sink 注入先于会话创建，对标
/// promote_meta_save_failure_rollback 的真实失败路径注入形态）
fn harness<C>(
  dir_name: &str,
  ctx: Arc<C>,
  handler: for<'a> fn(&'a C, i64, i32, StoreEvent<'a>) -> wkv::Result<()>,
) -> aok::Result<(tempfile::TempDir, Arc<WedbStore<SegmentedDevice>>)>
where
  C: Send + Sync + 'static,
{
  let dir = tempdir()?;
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(dir_name))?);
  let store = Arc::new(WedbStore::open(config, device)?);
  assert!(store.set_event_sink(StoreEventSink::new(ctx, handler)));
  Ok((dir, store))
}

/// 条目一（TTL 腿失败补偿）：写镜像硬失败后 TTL 回填原刻度 + AOF 补偿条目同向
#[compio::test]
async fn set_overwrite_ttl_key_failure_restores_ttl() -> Void {
  let ctx = Arc::new(SetFaultCtx {
    armed: AtomicBool::new(false),
    ttl_log: Mutex::new(Vec::new()),
    string_key: Mutex::new(None),
  });
  let (_dir, store) = harness(
    "wk_ttl_restore.db",
    Arc::clone(&ctx),
    set_string_write_fault,
  )?;
  let session = store.new_session()?;
  session.set_context(NS, DB);
  ctx
    .string_key
    .lock()
    .replace(session.session_string_key(KEY).as_slice().to_vec());

  let exp = now_ticks() + TICKS_PER_SECOND * 600;
  session.upsert(KEY, b"old").await?;
  session.put_ttl(KEY, exp).await?;
  assert_eq!(session.ttl_of(KEY).await?, Some(exp));

  // 注错窗口：SET 覆写带 TTL 键，数据落笔后写镜像失败
  ctx.armed.store(true, Ordering::Relaxed);
  let written = session.try_upsert_sync(KEY, b"new");
  assert!(written.is_err(), "写镜像失败须以 Err 上抛拒绝");

  // TTL 原刻度回填：旧值不失 TTL 判死依据（旧缺陷形态 = TTL 已清而数据
  // 未写，旧值永生复活）
  assert_eq!(
    session.ttl_of(KEY).await?,
    Some(exp),
    "TTL 须回填预读的原过期刻度"
  );
  // Err = 已生效 + 镜像缺失（数据记录已在通知前提交），值面为写入值
  assert_eq!(session.read(KEY).await?, Some(b"new".to_vec()));

  // AOF 对账：预置 put_ttl Some(exp) → 删 None → 补偿回填 Some(exp)，
  // 恢复重放终态 TTL 在场，主从同向
  {
    let log = ctx.ttl_log.lock();
    assert_eq!(
      log.as_slice(),
      &[Some(exp), None, Some(exp)],
      "TTL 镜像序须为 置 → 删 → 补偿回填"
    );
  }

  // 解除注错重试：闭环幂等，SET 语义清 TTL
  ctx.armed.store(false, Ordering::Relaxed);
  session.upsert(KEY, b"new2").await?;
  assert_eq!(session.ttl_of(KEY).await?, None, "重试闭环须清除 TTL");
  assert_eq!(session.read(KEY).await?, Some(b"new2".to_vec()));
  OK
}

/// 条目一（信封清退重排）：写镜像硬失败时信封未被清退，对象数据零销毁；
/// 重试成功后信封清退闭环（SET 语义：字符串写入使键变为 string）
#[compio::test]
async fn set_overwrite_envelope_failure_keeps_envelope() -> Void {
  let ctx = Arc::new(SetFaultCtx {
    armed: AtomicBool::new(false),
    ttl_log: Mutex::new(Vec::new()),
    string_key: Mutex::new(None),
  });
  let (_dir, store) = harness("wk_env_keep.db", Arc::clone(&ctx), set_string_write_fault)?;
  let session = store.new_session()?;
  session.set_context(NS, DB);
  ctx
    .string_key
    .lock()
    .replace(session.session_string_key(KEY).as_slice().to_vec());

  // 对象信封域预置（旧缺陷形态：SET 先删信封再写数据，写失败即对象销毁）
  session
    .try_upsert_tag_sync(KEY, KeyTag::ObjectEnvelope, b"env-payload")?
    .map_err(|page_id| io::Error::other(format!("unexpected page_id {page_id}")))?;
  let env_k = session.session_tag_key(KeyTag::ObjectEnvelope, KEY);
  assert!(session.contains_key_raw(&env_k).await?, "信封预置须在场");

  // 注错窗口：SET 覆写对象键，String 域写镜像失败
  ctx.armed.store(true, Ordering::Relaxed);
  let written = session.try_upsert_sync(KEY, b"new");
  assert!(written.is_err(), "写镜像失败须以 Err 上抛拒绝");

  // 信封清退重排于数据提交之后：失败路径信封原样存活（对象数据未销毁）
  assert!(
    session.contains_key_raw(&env_k).await?,
    "数据落笔失败时信封须原样保留"
  );

  // 解除注错重试：SET 成功闭环后信封被清退（读写面域探针 String 优先，
  // 暂存窗由 obj_save_recheck 既有机制兜底）
  ctx.armed.store(false, Ordering::Relaxed);
  session.upsert(KEY, b"new2").await?;
  assert!(
    !session.contains_key_raw(&env_k).await?,
    "SET 成功后信封须被清退"
  );
  assert_eq!(session.read(KEY).await?, Some(b"new2".to_vec()));
  OK
}

/// 条目二（已提交态不撤回）：等长原位臂写镜像失败 = 「已生效 + 镜像缺失」
/// Err 契约——值字节已物理生效，不伪装 None 降级 RCU（无第二份效果），
/// AOF 零条目待补偿
#[compio::test]
async fn in_place_modify_mirror_failure_keeps_committed_value() -> Void {
  let armed = Arc::new(AtomicBool::new(false));
  let (_dir, store) = harness("wk_inplace.db", Arc::clone(&armed), ttl_write_fault)?;
  let session = store.new_session()?;
  session.set_context(NS, DB);

  // 预置 TTL 记录（放行）：8B 定长值，为等长原位改写创造可变区命中
  let first = now_ticks() + TICKS_PER_SECOND * 100;
  session.put_ttl(KEY, first).await?;
  assert_eq!(session.ttl_of(KEY).await?, Some(first));

  // 注错窗口：原位改写落笔后 TTL 值镜像失败
  let second = first + TICKS_PER_SECOND;
  armed.store(true, Ordering::Relaxed);
  let r = session.put_ttl(KEY, second).await;
  assert!(r.is_err(), "写镜像失败须以 Err 上抛（已生效+镜像缺失）");

  // 已提交态不撤回：值已物理生效（旧缺陷形态 = 伪装 None + Err 并发，
  // 调用方 RCU 兜底产生第二份效果与无镜像半变更）
  assert_eq!(
    session.ttl_of(KEY).await?,
    Some(second),
    "原位改写已落笔的值须保持生效"
  );

  // 解除注错：原位臂健康可再次改写（MODIFIED 位正常置位）
  armed.store(false, Ordering::Relaxed);
  let third = second + TICKS_PER_SECOND;
  session.put_ttl(KEY, third).await?;
  assert_eq!(session.ttl_of(KEY).await?, Some(third));
  OK
}

/// 条目三（镜像恰一次）：并发同键写删竞争下，AOF 镜像事件数 == 成功操作数——
/// 盲追加 CAS 败帧的暂存复用/弃置重试不产生重复镜像条目
#[test]
fn concurrent_blind_append_mirror_count_matches_committed_writes() -> Void {
  Runtime::new()?.block_on(async {
    let log = Arc::new(Mutex::new(Vec::<(Vec<u8>, bool, Vec<u8>)>::new()));
    let dir = tempdir()?;
    let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("wk_mirror.db"),
    )?);
    let store = Arc::new(WedbStore::open(config, device)?);
    let sink_log = Arc::clone(&log);
    assert!(store.set_event_sink(StoreEventSink::new(
      sink_log,
      |log, _ver, _aof_session_id, event| {
        if let StoreEvent::Write {
          key,
          val,
          tombstone,
          ..
        } = event
        {
          log.lock().push((key.to_vec(), tombstone, val.to_vec()));
        }
        Ok(())
      }
    )));

    let num_threads = 4;
    let rounds = 60;
    let committed = Arc::new(Mutex::new(0usize));
    let mut handles = Vec::new();
    for thread_id in 0..num_threads {
      let store = Arc::clone(&store);
      let committed = Arc::clone(&committed);
      handles.push(thread::spawn(move || -> aok::Result<()> {
        let rt = Runtime::new()?;
        rt.block_on(async move {
          let session = store.new_session()?;
          session.set_context(NS, DB);
          for i in 0..rounds {
            // 穿插写删同键：盲追加/原位/链内复活/CAS 败重试各形态在竞争下自然出现
            if i % 10 == 9 {
              if session.try_delete_sync(KEY)? == Ok(true) {
                *committed.lock() += 1;
              }
            } else {
              let val = format!("t{i}-{thread_id}");
              if session.try_upsert_sync(KEY, val.as_bytes())?.is_ok() {
                *committed.lock() += 1;
              }
            }
          }
          aok::Result::<()>::Ok(())
        })?;
        Ok(())
      }));
    }
    for h in handles {
      h.join().unwrap()?;
    }

    let committed = *committed.lock();
    let guard = log.lock();
    // 对账口径：只统计测试键的镜像（sink 面含 set_context 触发的 DbMeta 等
    // 内部元数据合法单次镜像，与用户键写镜像分账）
    let mirrored = guard
      .iter()
      .filter(|(k, ..)| {
        NamespaceDbCodec::decode_tagged_key(k).is_ok_and(|(_, _, _, user_key)| user_key == KEY)
      })
      .count();
    drop(guard);
    assert!(committed > 0, "并发竞争须产生至少一次成功写（自检前置）");
    assert_eq!(
      mirrored, committed,
      "AOF 镜像事件数须与成功操作数一一对应（CAS 败帧重试零重复镜像）"
    );
    OK
  })
}

/// 条目四（降级哨兵传播 + 异步闭环）：冷数据删除同步臂返回精确哨兵
/// u64::MAX（DEGRADE_ASYNC），异步慢路径幂等闭环且墓碑镜像恰一条
#[compio::test]
async fn cold_delete_returns_degrade_sentinel_and_closes_async() -> Void {
  let log = Arc::new(Mutex::new(Vec::<(Vec<u8>, bool)>::new()));
  let dir = tempdir()?;
  // 2 页环形缓冲：把 KEY 挤出内存驻留窗（[head, begin) 磁盘候选区）
  let config = StoreConfig::new(1024, 64 * 1024, 2, 0.5)?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("wk_sentinel.db"),
  )?);
  let store = Arc::new(WedbStore::open(config, device)?);
  let sink_log = Arc::clone(&log);
  assert!(store.set_event_sink(StoreEventSink::new(
    sink_log,
    |log, _ver, _aof_session_id, event| {
      if let StoreEvent::Write { key, tombstone, .. } = event {
        log.lock().push((key.to_vec(), tombstone));
      }
      Ok(())
    }
  )));
  let session = store.new_session()?;
  session.set_context(NS, DB);

  // 目标键先落可变区，再以大值流灌满环形缓冲触发驱逐，目标记录滑入磁盘区
  session.upsert(KEY, b"cold-target").await?;
  let filler = vec![b'x'; 32 * 1024];
  for i in 0..12u32 {
    let k = format!("wk:filler:{i:04}");
    session.upsert(k.as_bytes(), &filler).await?;
  }

  // 同步快路径：链条伸入磁盘区，返回精确降级哨兵（既非误判不存在，
  // 也非可误喂页驱逐的真实 page_id）
  let deleted = session.try_delete_sync(KEY)?;
  assert_eq!(
    deleted,
    Err(u64::MAX),
    "冷数据同步删除须返回 DEGRADE_ASYNC 精确哨兵"
  );
  assert!(
    session.contains_key(KEY).await?,
    "降级臂未落任何写入，键须存活"
  );

  // 异步闭环：delete_raw 按哨兵分岔冷数据慢路径，幂等删除 + 恰一条墓碑镜像
  assert!(session.delete(KEY).await?, "异步慢路径须真实删除冷键");
  assert!(!session.contains_key(KEY).await?, "删除后键须缺席");
  let tombstones = log
    .lock()
    .iter()
    .filter(|(k, tomb)| {
      *tomb
        && NamespaceDbCodec::decode_tagged_key(k).is_ok_and(|(_, _, _, user_key)| user_key == KEY)
    })
    .count();
  assert_eq!(tombstones, 1, "冷数据墓碑镜像须恰一条（生效后镜像）");
  OK
}
