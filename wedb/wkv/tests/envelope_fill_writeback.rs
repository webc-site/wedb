//! 对象信封单次成形写回字节不变回归（票 zcode-r3-perf-envwriteback-copy）
//!
//! 信封值 `[1B 对象标签][payload]` 经 `try_upsert_envelope_sync_fill` 分段
//! 直写源在记录槽位一次成形（原位 / 链内复活 / 复活池 / 尾部追加四臂同源），
//! 消除中间整值 `Vec` 暂存——对标 C# GarnetObjectSerializer.Serialize 直写
//! 记录 value span、无中间堆缓冲再转拷（garnet/libs/server/Objects/Types/
//! GarnetObjectSerializer.cs:104）。断言：
//! 1. 尾部追加臂（初写）、原位覆写臂（等长 / 短值填入松弛）逐字节与
//!    wcol `obj_encode_custom_into` 形态（`[tag][payload]`）一致；
//! 2. 写成功地址 `with_record_value` 借出切片与读路径值一致（AOF 镜像与
//!    记录共享同一已编码字节）；
//! 3. wkv 快写口零镜像事件（信封非墓碑 Write 镜像由 wnode 侧 EnvelopeUpsert
//!    单点承接，本口不投任何事件）。

use std::sync::Arc;

use aok::{OK, Void};
use parking_lot::Mutex;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, StoreEvent, StoreEventSink, WedbStore};
use wval::{KeyTag, NamespaceDbCodec};

/// 全量事件对账日志：按到达序记录事件名与 Write 键（信封快写口预期恒空）
type EventLog = Mutex<Vec<(&'static str, Vec<u8>)>>;

fn event_tap(
  log: &EventLog,
  _ver: i64,
  _aof_session_id: i32,
  event: StoreEvent<'_>,
) -> wkv::Result<()> {
  let name = match event {
    StoreEvent::Write { key, .. } => {
      log.lock().push(("write", key.to_vec()));
      return Ok(());
    }
    StoreEvent::TtlWrite { .. } => "ttl",
    StoreEvent::EtagWrite { .. } => "etag",
    StoreEvent::EnvelopeUpsert { .. } => "envelope",
    StoreEvent::ObjectRmw(_) => "rmw",
    _ => "other",
  };
  log.lock().push((name, Vec::new()));
  Ok(())
}

/// 手工拼接信封期望值（与 wcol `obj_encode_custom_into` 同构：tag 前缀 + payload）
fn expected(tag: u8, payload: &[u8]) -> Vec<u8> {
  let mut v = Vec::with_capacity(payload.len() + 1);
  v.push(tag);
  v.extend_from_slice(payload);
  v
}

#[compio::test]
async fn envelope_fill_writeback_bytes_identical() -> Void {
  let dir = tempdir()?;
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("envfill.db"))?);
  let store = Arc::new(WedbStore::open(config, device)?);
  let log: EventLog = Mutex::new(Vec::new());
  let log = Arc::new(log);
  assert!(store.set_event_sink(StoreEventSink::new(Arc::clone(&log), event_tap)));
  let session = store.new_session()?;
  session.set_context(7, 2);

  let key = b"envfill:key";
  let tag: u8 = 3; // GarnetObjectType::Hash
  // 三轮覆写覆盖尾部追加（初写）→ 原位覆写（短值填入松弛）→ 再追加（值增长）
  let payloads: [&[u8]; 3] = [b"hash-payload-v1", b"v2", b"hash-payload-v3-longer"];
  let mut last_addr = 0;
  for payload in payloads {
    let addr = session.try_upsert_envelope_sync_fill(key, tag, payload)?;
    let addr = addr.expect("纯内存页余量充足，同步快路径必成");
    // 写成功地址借出的切片与读路径逐字节一致（镜像共享字节锚点）
    let borrowed = session
      .with_record_value(addr, |v| v.to_vec())
      .expect("同步成功臂记录恒内存驻留");
    assert_eq!(borrowed, expected(tag, payload));
    let read_back = session
      .read_tag_with(key, KeyTag::ObjectEnvelope, |v| v.to_vec())
      .await?;
    assert_eq!(read_back.as_deref(), Some(borrowed.as_slice()));
    last_addr = addr;
  }
  assert!(last_addr > 0);
  // wkv 快写口零信封镜像事件（镜像由 wnode 侧 EnvelopeUpsert 单点承接）；
  // DbMeta（0x0E）系统元数据写经 raw 原语镜像为既有行为，与本口无关
  let events = log.lock().clone();
  let relevant: Vec<_> = events
    .iter()
    .filter(|(name, key)| {
      !(*name == "write" && NamespaceDbCodec::decode_tag(key) == Some(KeyTag::DbMeta))
    })
    .collect();
  assert!(
    relevant.is_empty(),
    "信封快写口不得投任何用户域存储事件: {relevant:?}"
  );
  OK
}
