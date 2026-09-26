//! TTL 过期物理清除的单轨事件测试（StoreEvent::TtlPurge 与会话私有物理写镜像抑制）
//!
//! 覆盖：
//! 1. 注册 StoreEventSink：purge 链内物理写通知被会话私有窗位抑制，TtlPurge 事件恰好触发一次且
//!    参数 (物理 ns, 物理 db, key, expire_at) 正确（域判据取映射权威表，非会话缓存），
//!    purge 后普通写事件不受影响（抑制窗位零残留）；
//! 2. expire_at 过去时间戳与 persist 过期分支同样分发 TtlPurge 事件，携带精确到期值；
//! 3. 未注册 StoreEventSink 时，purge 逻辑安全执行无异常；
//! 4. 双会话并发清退交错编排：条目流与调度无关——各链恰一条 TtlPurge、零裸墓碑镜像；
//! 5. 已清退会话销毁后新会话镜像照常产出（窗位随会话消亡无继承态），且新会话自身
//!    purge 链仍单条折叠；
//! 6. 内置 GC 长驻会话与用户会话读面惰性清退并发回归：每过期键恰一条 TtlPurge、
//!    零墓碑泄漏、终态两键物理亡。
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/ExpirationTests.cs + test/standalone/Garnet.test/ExpiredKeyDeletionTests.cs

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::{ResumeUnwind, spawn};
use parking_lot::Mutex;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wkv::{GcManager, StoreEvent, StoreEventSink, TtlOpt};
use wtest_base::open_test_store;
use wval::I64Codec;

#[derive(Debug, Clone, PartialEq, Eq)]
enum RecordedEvent {
  Write {
    key: Vec<u8>,
    val: Vec<u8>,
    tombstone: bool,
  },
  /// TTL 旁路记录镜像（del_ttl 墓碑即 expire_at=None 形态，泄漏进条目流即
  /// 折叠违注的直接证据）
  TtlWrite {
    key: Vec<u8>,
    expire_at: Option<i64>,
  },
  TtlPurge {
    ns: u64,
    db: u64,
    key: Vec<u8>,
    expire_at: i64,
  },
}

type EventLog = Arc<Mutex<Vec<RecordedEvent>>>;

/// 注入统一事件记录器
fn record_sink(log: EventLog) -> StoreEventSink {
  StoreEventSink::new(log, |log, _ver, _aof_session_id, event| {
    match event {
      StoreEvent::Write {
        key,
        val,
        tombstone,
      } => {
        log.lock().push(RecordedEvent::Write {
          key: key.to_vec(),
          val: val.to_vec(),
          tombstone,
        });
      }
      StoreEvent::TtlWrite { key, expire_at, .. } => {
        log.lock().push(RecordedEvent::TtlWrite {
          key: key.to_vec(),
          expire_at,
        });
      }
      StoreEvent::TtlPurge {
        ns,
        db,
        key,
        expire_at,
      } => {
        log.lock().push(RecordedEvent::TtlPurge {
          ns,
          db,
          key: key.to_vec(),
          expire_at,
        });
      }
      _ => {}
    }
    Ok(())
  })
}

/// 测试 1：未注册事件处理器时 purge 流程安全执行
#[compio::test]
async fn purge_without_event_sink_succeeds() -> Void {
  let (_dir, store) = open_test_store("no_sink")?;
  let session = store.new_session()?;
  session.set_context(5, 2);

  let key = b"k:legacy";
  session.upsert(key, b"v").await?;
  let past = now_ticks() - TICKS_PER_SECOND;
  session.put_ttl(key, past).await?;

  assert_eq!(session.read(key).await?, None);
  assert_eq!(session.pttl_ms(key).await?, -2);
  OK
}

/// 测试 2：注册事件处理器——链内物理写通知被抑制、TtlPurge 恰好一次且参数正确、
/// purge 后普通写事件不受影响（抑制标志零残留）
#[compio::test]
async fn purge_with_event_sink_suppresses_writes_and_emits_purge() -> Void {
  let (_dir, store) = open_test_store("with_sink")?;
  let log: EventLog = Arc::new(Mutex::new(Vec::new()));
  assert!(store.set_event_sink(record_sink(Arc::clone(&log))));
  let session = store.new_session()?;
  session.set_context(5, 2);
  // 事件域判据取映射权威表（vdb.get_virtual_ids），不经会话缓存，
  // 与被测实现（会话 virtual_domain()）不同源，杜绝自证
  let (vns, vdb) = store.vdb.get_virtual_ids(5, 2);
  assert_ne!(
    (vns, vdb),
    (5, 2),
    "本用例须跑在非恒等映射域上，否则逻辑域/物理域无法甄别（判据退化为自证）"
  );

  let key = b"k:lazy";
  session.upsert(key, b"v").await?;
  let past = now_ticks() - TICKS_PER_SECOND;
  session.put_ttl(key, past).await?;

  let before = log.lock().len();
  // 读路径惰性过期：物理墓碑被抑制，TtlPurge 事件恰好分发一次
  assert_eq!(session.read(key).await?, None);
  let (count, last) = {
    let events = log.lock();
    (events.len(), events.last().cloned())
  };
  assert_eq!(count, before + 1, "purge 链内不得泄漏任何物理写墓碑事件");
  assert_eq!(
    last,
    Some(RecordedEvent::TtlPurge {
      // 事件域 = 被清记录的物理域（AOF 条目前缀即由此域编码，副本按条目域直设落回原域）；
      // 上报逻辑域 (5, 2) 会让副本本地二次映射到新域，清错的库
      ns: vns,
      db: vdb,
      key: key.to_vec(),
      // 事件携带存储值：put_ttl 裸写内核逐位相等（值域裁决唯命令边界单点，§143）
      expire_at: past,
    }),
    "TtlPurge 必须恰好收到一次且 (物理 ns, 物理 db, key, expire_at) 正确"
  );

  // 抑制标志零残留：purge 之后的普通写照常分发
  session.upsert(b"k:after", b"v2").await?;
  let after_k = session.session_string_key(b"k:after");
  let last = log.lock().last().cloned();
  assert_eq!(
    last,
    Some(RecordedEvent::Write {
      key: after_k.to_vec(),
      val: b"v2".to_vec(),
      tombstone: false,
    }),
    "purge 后普通写事件必须照常触发（标志零残留）"
  );
  OK
}

/// 测试 3：expire_at 过去时间戳（返回 2）与 persist 过期分支（返回 0）同样
/// 经 TtlPurge 事件单条化，到期值取调用方传入/记录中的精确时间戳
#[compio::test]
async fn purge_event_covers_expire_at_past_and_persist_branches() -> Void {
  let (_dir, store) = open_test_store("branches")?;
  let log: EventLog = Arc::new(Mutex::new(Vec::new()));
  assert!(store.set_event_sink(record_sink(Arc::clone(&log))));
  let session = store.new_session()?;
  // 未显式 set_context：会话停在根租户，根域 (0, 0)→(0, 0) 恒等映射，
  // 故根域用例的物理域与逻辑域同值，不与测试 2 的非恒等判据冲突

  // expire_at 过去时间戳分支：立即物理删除，分发该过去时间戳的 TtlPurge 事件
  let past = now_ticks() - TICKS_PER_SECOND * 2;
  session.upsert(b"k:exp", b"v").await?;
  let before = log.lock().len();
  assert_eq!(session.expire_at(b"k:exp", past, TtlOpt::NONE).await?, 2);
  let (count, last) = {
    let events = log.lock();
    (events.len(), events.last().cloned())
  };
  assert_eq!(
    count,
    before + 1,
    "expire_at 过去分支必须零墓碑镜像且恰发一条 TtlPurge"
  );
  assert_eq!(
    last,
    Some(RecordedEvent::TtlPurge {
      ns: 0,
      db: 0,
      key: b"k:exp".to_vec(),
      // expire_at 会话入口恒等直通（§143）：过去判定与事件携带入参原值逐位
      expire_at: past,
    })
  );

  // persist 过期分支：惰性清除后返回 0，分发携带 TTL 记录到期值的 TtlPurge
  let expired = now_ticks() - TICKS_PER_SECOND;
  session.upsert(b"k:persist", b"v").await?;
  session.put_ttl(b"k:persist", expired).await?;
  assert_eq!(
    session.read_raw(&session.ttl_key(b"k:persist")).await?,
    Some(I64Codec::encode(expired).to_vec()),
    "put_ttl 裸写内核不粗化：落盘与输入逐位相等"
  );
  let before = log.lock().len();
  assert_eq!(session.persist(b"k:persist").await?, 0);
  let (count, last) = {
    let events = log.lock();
    (events.len(), events.last().cloned())
  };
  assert_eq!(
    count,
    before + 1,
    "persist 过期分支必须零墓碑镜像且恰发一条 TtlPurge"
  );
  assert_eq!(
    last,
    Some(RecordedEvent::TtlPurge {
      ns: 0,
      db: 0,
      key: b"k:persist".to_vec(),
      expire_at: expired,
    })
  );
  OK
}

/// 截取清退窗口起点之后分发的条目（顺序保持，计数与形态断言用）
fn tail_events(log: &EventLog, before: usize) -> Vec<RecordedEvent> {
  log.lock()[before..].to_vec()
}

/// 断言条目切片恰为指定键集（可重复）的 TtlPurge、零其他镜像（含裸墓碑 Write
/// 与 TtlWrite(None) 折 Persist）——purge 链单条折叠与跨会话零串线的条目流判据
fn assert_only_ttl_purges(
  events: &[RecordedEvent],
  want_expire_at: i64,
  mut want_keys: Vec<Vec<u8>>,
) {
  assert_eq!(
    events.len(),
    want_keys.len(),
    "条目流必须恰为每键一条 TtlPurge，零清退窗镜像泄漏: events={events:?}"
  );
  for e in events {
    match e {
      RecordedEvent::TtlPurge {
        ns,
        db,
        key,
        expire_at,
      } => {
        // 根租户 (0,0) 恒等映射：物理域即逻辑域
        assert_eq!((*ns, *db), (0, 0));
        assert_eq!(*expire_at, want_expire_at, "条目携带被清记录的精确到期值");
        let idx = want_keys
          .iter()
          .position(|k| k == key)
          .unwrap_or_else(|| panic!("条目流混入未知键的 TtlPurge: key={key:?}"));
        want_keys.remove(idx);
      }
      other => panic!("清退窗口条目流混入非折叠镜像: {other:?}"),
    }
  }
  assert!(want_keys.is_empty(), "存在未分发 TtlPurge 的清退键");
}

/// 测试 4：双会话并发清退编排——A/B 各清退不同过期键（两条清退窗在 thread-per-core
/// 调度下交错或串行），条目流与调度次序无关：恰两条 TtlPurge、零裸墓碑镜像。
/// 会话私有窗位使两条清退链构造上互不可达（旧全局抑制槽形态下 A入→B入→A退→B退
/// 交错会让 B 的 drop 把 A 陈旧令牌回填槽内、B 余链墓碑穿破抑制入条目流）
#[compio::test]
async fn dual_session_purge_entries_are_schedule_invariant() -> Void {
  let (_dir, store) = open_test_store("dual_session")?;
  let log: EventLog = Arc::new(Mutex::new(Vec::new()));
  assert!(store.set_event_sink(record_sink(Arc::clone(&log))));

  let k1 = b"k:dual:a".to_vec();
  let k2 = b"k:dual:b".to_vec();
  let past = now_ticks() - TICKS_PER_SECOND;
  {
    // 装配会话先退场：其窗内正常写镜像（Write×2 + TtlWrite×2）不计入清退窗断言
    let setup = store.new_session()?;
    setup.upsert(&k1, b"v1").await?;
    setup.upsert(&k2, b"v2").await?;
    setup.put_ttl(&k1, past).await?;
    setup.put_ttl(&k2, past).await?;
  }

  let before = log.lock().len();
  let a = store.new_session()?;
  let b = store.new_session()?;
  let (ka, kb) = (k1.clone(), k2.clone());
  let ha = spawn(async move { a.check_expired(&ka).await.expect("A 链清退裁决不得失败") });
  let hb = spawn(async move { b.check_expired(&kb).await.expect("B 链清退裁决不得失败") });
  assert!(
    ha.await.resume_unwind().expect("A 链任务正常收口"),
    "A 键必须被物理清退"
  );
  assert!(
    hb.await.resume_unwind().expect("B 链任务正常收口"),
    "B 键必须被物理清退"
  );
  assert_only_ttl_purges(
    &tail_events(&log, before),
    past,
    vec![k1.clone(), k2.clone()],
  );

  // 双链收口后新会话读面终态两键物理亡（数据与旁路记录皆清）
  let d = store.new_session()?;
  assert_eq!(d.read(&k1).await?, None);
  assert_eq!(d.read(&k2).await?, None);
  OK
}

/// 测试 5：清退链退场与会话销毁后的镜像复常——窗位随会话消亡、新会话无继承抑制
/// 态：首条 SET/EXPIRE 镜像条目正常产出（钉死旧形态的悬垂令牌加地址复用错杀面：
/// 槽内残留已毁会话令牌时，复用同地址的新会话用户域写整段落不到 AOF）；且新会话
/// 自身 purge 链仍单条折叠（会话私有标志恰等于「仅本会话级联写被抑制」）
#[compio::test]
async fn new_session_after_purged_session_mirrors_writes() -> Void {
  let (_dir, store) = open_test_store("session_reuse")?;
  let log: EventLog = Arc::new(Mutex::new(Vec::new()));
  assert!(store.set_event_sink(record_sink(Arc::clone(&log))));

  let k_old = b"k:reuse:old";
  {
    let old = store.new_session()?;
    old.upsert(k_old, b"v").await?;
    old.put_ttl(k_old, now_ticks() - TICKS_PER_SECOND).await?;
    assert_eq!(old.read(k_old).await?, None, "过期键读面惰性清退");
    // 本会话窗后退场：save/restore 已复窗位，同会话后续写照常镜像
    let before = log.lock().len();
    old.upsert(b"k:reuse:inold", b"v").await?;
    assert_eq!(
      tail_events(&log, before).len(),
      1,
      "本会话清退窗后的正常写不得被残留窗位抑制"
    );
  }

  // 老会话销毁后重分配的新会话：首条 SET/EXPIRE 镜像必须正常产出
  let fresh = store.new_session()?;
  let before = log.lock().len();
  fresh.upsert(b"k:reuse:new", b"w").await?;
  let live = now_ticks() + TICKS_PER_SECOND;
  fresh.put_ttl(b"k:reuse:new", live).await?;
  let sk = fresh.session_string_key(b"k:reuse:new");
  assert_eq!(
    tail_events(&log, before),
    vec![
      RecordedEvent::Write {
        key: sk.to_vec(),
        val: b"w".to_vec(),
        tombstone: false,
      },
      RecordedEvent::TtlWrite {
        key: b"k:reuse:new".to_vec(),
        expire_at: Some(live),
      },
    ],
    "新会话首条 SET/EXPIRE 镜像必须正常落条目流（零继承抑制态）"
  );

  // 新会话自身清退链仍单条折叠零墓碑泄漏
  let k_p = b"k:reuse:purge";
  fresh.upsert(k_p, b"v").await?;
  let past = now_ticks() - TICKS_PER_SECOND;
  fresh.put_ttl(k_p, past).await?;
  let before = log.lock().len();
  assert_eq!(fresh.read(k_p).await?, None);
  assert_only_ttl_purges(&tail_events(&log, before), past, vec![k_p.to_vec()]);
  OK
}

/// 测试 6：内置 GC 长驻会话与用户会话读面惰性清退并发回归——每过期键恰一条
/// TtlPurge 条目、零墓碑镜像（GC 链与用户链各持各自会话窗位互不污染，无论哪条
/// 链先清退该键，逐键清退经键独占闩收敛为单链闭环），终态两键物理亡
#[compio::test]
async fn gc_sweep_and_lazy_purge_concurrent_single_entry_per_key() -> Void {
  let (_dir, store) = open_test_store("gc_lazy")?;
  let log: EventLog = Arc::new(Mutex::new(Vec::new()));
  assert!(store.set_event_sink(record_sink(Arc::clone(&log))));

  let k_gc = b"k:mix:gc".to_vec();
  let k_lazy = b"k:mix:lazy".to_vec();
  let past = now_ticks() - TICKS_PER_SECOND;
  {
    let setup = store.new_session()?;
    setup.upsert(&k_gc, b"v").await?;
    setup.upsert(&k_lazy, b"v").await?;
    setup.put_ttl(&k_gc, past).await?;
    setup.put_ttl(&k_lazy, past).await?;
  }

  let before = log.lock().len();
  let mgr = GcManager::new(&store);
  let hg = spawn(async move {
    mgr.run_once().await.expect("GC 单轮扫描不得失败");
  });
  let user = store.new_session()?;
  let k_lazy_read = k_lazy.clone();
  let hu = spawn(async move { user.read(&k_lazy_read).await.expect("用户读路径不得失败") });
  hg.await.resume_unwind().expect("GC 任务正常收口");
  let val = hu.await.resume_unwind().expect("用户读任务正常收口");
  assert_eq!(val, None, "过期键读面视同不存在");

  // 两键各恰一条 TtlPurge（GC 面键与读面键无论谁承接清退，条目流计数恒定），
  // 零裸墓碑/裸 Persist 镜像
  assert_only_ttl_purges(
    &tail_events(&log, before),
    past,
    vec![k_gc.clone(), k_lazy.clone()],
  );
  let probe = store.new_session()?;
  assert_eq!(probe.read(&k_gc).await?, None);
  assert_eq!(probe.read(&k_lazy).await?, None);
  OK
}
