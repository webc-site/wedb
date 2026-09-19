//! ETag 旁路记录单轨事件测试（对标 C# LogRecord 记录尾 ETag 字段随记录一体维护）
//!
//! 覆盖：
//! 1. upsert 与原位改写（in-place）两条写入路径均同栈分发 EtagWrite 事件，携带
//!    线性化后的绝对 etag 值（Some），且分流短路——Write 事件不再出现 KeyTag::Etag 物理键；
//! 2. 普通 String 键操作正常分发 Write 事件；
//! 3. 显式 del_etag 与 delete 级联清退以墓碑形态（None）分发 EtagWrite 事件；
//! 4. 过期 purge 链（EXPIRE 级联）：链内 ETag 通知被会话级抑制、不分发 EtagWrite
//!    （由 TtlPurge 事件承接），旁路记录物理清除。

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use parking_lot::Mutex;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wkv::{StoreEvent, StoreEventSink};
use wtest_base::open_test_store;
use wval::{KeyTag, NamespaceDbCodec};

#[derive(Debug, Clone, PartialEq, Eq)]
enum RecordedEvent {
  Write {
    key: Vec<u8>,
    val: Vec<u8>,
    tombstone: bool,
  },
  EtagWrite {
    ns: u64,
    db: u64,
    key: Vec<u8>,
    etag: Option<i64>,
  },
  TtlPurge {
    ns: u64,
    db: u64,
    key: Vec<u8>,
    expire_at: i64,
  },
}

type EventLog = Arc<Mutex<Vec<RecordedEvent>>>;

fn record_sink(log: EventLog) -> StoreEventSink {
  StoreEventSink::new(log, |log, event| {
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
      StoreEvent::EtagWrite { ns, db, key, etag } => {
        log.lock().push(RecordedEvent::EtagWrite {
          ns,
          db,
          key: key.to_vec(),
          etag,
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

/// 测试 1：upsert 与原位改写双路径分发 EtagWrite + 分流短路（Write 不见 Etag 键）
#[test]
fn etag_port_fires_on_upsert_and_in_place() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_test_store("write")?;
    let log: EventLog = Arc::new(Mutex::new(Vec::new()));
    assert!(store.set_event_sink(record_sink(Arc::clone(&log))));
    let session = store.new_session()?;
    session.set_context(3, 1);

    let key = b"k:etag";
    // 首写：无记录 → upsert 路径
    session.put_etag(key, 6).await?;
    // 次写：可变区命中 → 原位改写路径（零追加零索引 CAS，同栈通知）
    session.put_etag(key, 7).await?;
    assert_eq!(session.etag_of(key).await?, Some(7));

    let entries = log.lock();
    let etags: Vec<_> = entries
      .iter()
      .filter_map(|e| match e {
        RecordedEvent::EtagWrite { key, etag, .. } => Some((key.clone(), *etag)),
        _ => None,
      })
      .collect();
    assert_eq!(etags.len(), 2, "upsert 与原位改写各分发一次 EtagWrite");
    assert_eq!(etags[0], (key.to_vec(), Some(6)));
    assert_eq!(etags[1], (key.to_vec(), Some(7)));

    // 分流短路：ETag 物理键不经通用 Write 事件重复分发
    let etag_writes = entries
      .iter()
      .filter(|e| match e {
        RecordedEvent::Write { key, .. } => {
          NamespaceDbCodec::decode_tagged_key(key).is_ok_and(|(_, _, tag, _)| tag == KeyTag::Etag)
        }
        _ => false,
      })
      .count();
    assert_eq!(etag_writes, 0, "ETag 记录不得作为普通 Write 重复分发");
    OK
  })
}

/// 测试 2：普通 String 键正常作为 Write 事件分发
#[test]
fn string_key_emits_write_event() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_test_store("string")?;
    let log: EventLog = Arc::new(Mutex::new(Vec::new()));
    assert!(store.set_event_sink(record_sink(Arc::clone(&log))));
    let session = store.new_session()?;

    let key = b"k:normal";
    session.upsert(key, b"v").await?;
    let str_k = session.session_string_key(key);
    let events = log.lock();
    let write_hit = events.iter().any(|e| match e {
      RecordedEvent::Write {
        key,
        val,
        tombstone,
      } => key.as_slice() == str_k.as_slice() && val.as_slice() == b"v" && !*tombstone,
      _ => false,
    });
    assert!(write_hit, "普通 String 键必须分发 Write 事件");
    OK
  })
}

/// 测试 3：del_etag 与 delete 级联清退以墓碑形态（None）分发 EtagWrite
#[test]
fn etag_port_fires_tombstone_on_del_and_cascade() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_test_store("tombstone")?;
    let log: EventLog = Arc::new(Mutex::new(Vec::new()));
    assert!(store.set_event_sink(record_sink(Arc::clone(&log))));
    let session = store.new_session()?;
    session.set_context(0, 0);

    // 显式清除
    let k1 = b"k:del";
    session.put_etag(k1, 5).await?;
    session.del_etag(k1).await?;
    assert_eq!(session.etag_of(k1).await?, None);

    // DEL 级联清退（对标 C# 删除记录连同 ETag 字段一并消失）
    let k2 = b"k:cascade";
    session.upsert(k2, b"v").await?;
    session.put_etag(k2, 9).await?;
    assert!(session.delete(k2).await?);

    let entries = log.lock();
    let etags: Vec<_> = entries
      .iter()
      .filter_map(|e| match e {
        RecordedEvent::EtagWrite { key, etag, .. } => Some((key.clone(), *etag)),
        _ => None,
      })
      .collect();
    assert_eq!(etags.len(), 4, "写×2 + 显式删 + 级联删");
    assert_eq!(etags[1], (k1.to_vec(), None), "del_etag 墓碑");
    assert_eq!(etags[3], (k2.to_vec(), None), "级联清退墓碑");
    OK
  })
}

/// 测试 4：过期 purge 链（EXPIRE 级联）——链内 ETag 通知被抑制、不分发
/// EtagWrite（AOF 由 DELIFEXPIM 单条目承接），旁路记录物理清除
#[test]
fn expired_purge_suppresses_etag_port_and_clears_record() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_test_store("purge")?;
    let log: EventLog = Arc::new(Mutex::new(Vec::new()));
    assert!(store.set_event_sink(record_sink(Arc::clone(&log))));
    let session = store.new_session()?;

    let key = b"k:expired";
    session.upsert(key, b"v").await?;
    session.put_etag(key, 4).await?;
    // 直接写入已过期 TTL（免 sleep），读路径惰性清除走 purge_expired 链
    session.put_ttl(key, now_ticks() - TICKS_PER_SECOND).await?;
    let etag_writes = log
      .lock()
      .iter()
      .filter(|e| matches!(e, RecordedEvent::EtagWrite { .. }))
      .count();

    assert_eq!(session.read(key).await?, None, "已过期键读即惰性清除");
    assert_eq!(session.etag_of(key).await?, None, "旁路记录随 purge 清除");
    let after_etag_writes = log
      .lock()
      .iter()
      .filter(|e| matches!(e, RecordedEvent::EtagWrite { .. }))
      .count();
    assert_eq!(
      after_etag_writes, etag_writes,
      "purge 链内 ETag 通知须被抑制（由 TtlPurge 单条目承接）"
    );
    OK
  })
}
