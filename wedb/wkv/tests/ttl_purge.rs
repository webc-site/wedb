//! TTL 过期物理清除的单轨事件测试（StoreEvent::TtlPurge 与会话级物理写镜像抑制）
//!
//! 覆盖：
//! 1. 注册 StoreEventSink：purge 链内物理写通知被会话级精确抑制，TtlPurge 事件恰好触发一次且
//!    参数 (ns, db, key, expire_at) 正确，purge 后普通写事件不受影响（抑制标志零残留）；
//! 2. expire_at 过去时间戳与 persist 过期分支同样分发 TtlPurge 事件，携带精确到期值；
//! 3. 未注册 StoreEventSink 时，purge 逻辑安全执行无异常。

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use parking_lot::Mutex;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wedb_test::open_test_store;
use wkv::{StoreEvent, StoreEventSink, TtlOpt};
use wval::TtlCodec;

#[derive(Debug, Clone, PartialEq, Eq)]
enum RecordedEvent {
  Write {
    key: Vec<u8>,
    val: Vec<u8>,
    tombstone: bool,
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
  Arc::new(move |event| match event {
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
  })
}

/// 测试 1：未注册事件处理器时 purge 流程安全执行
#[test]
fn purge_without_event_sink_succeeds() -> Void {
  Runtime::new()?.block_on(async {
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
  })
}

/// 测试 2：注册事件处理器——链内物理写通知被抑制、TtlPurge 恰好一次且参数正确、
/// purge 后普通写事件不受影响（抑制标志零残留）
#[test]
fn purge_with_event_sink_suppresses_writes_and_emits_purge() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_test_store("with_sink")?;
    let log: EventLog = Arc::new(Mutex::new(Vec::new()));
    assert!(store.set_event_sink(record_sink(Arc::clone(&log))));
    let session = store.new_session()?;
    session.set_context(5, 2);

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
        ns: 5,
        db: 2,
        key: key.to_vec(),
        expire_at: past,
      }),
      "TtlPurge 必须恰好收到一次且 (ns, db, key, expire_at) 正确"
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
  })
}

/// 测试 3：expire_at 过去时间戳（返回 2）与 persist 过期分支（返回 0）同样
/// 经 TtlPurge 事件单条化，到期值取调用方传入/记录中的精确时间戳
#[test]
fn purge_event_covers_expire_at_past_and_persist_branches() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_test_store("branches")?;
    let log: EventLog = Arc::new(Mutex::new(Vec::new()));
    assert!(store.set_event_sink(record_sink(Arc::clone(&log))));
    let session = store.new_session()?;

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
        expire_at: past,
      })
    );

    // persist 过期分支：惰性清除后返回 0，分发携带 TTL 记录到期值的 TtlPurge
    let expired = now_ticks() - TICKS_PER_SECOND;
    session.upsert(b"k:persist", b"v").await?;
    session.put_ttl(b"k:persist", expired).await?;
    assert_eq!(
      session.read_raw(&session.ttl_key(b"k:persist")).await?,
      Some(TtlCodec::encode(expired as i64).to_vec())
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
  })
}
