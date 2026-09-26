//! GETEX 过去时刻 EXAT/PXAT 的 TTL 语义回归测试
//!
//! 对标 garnet：BasicCommands.cs:NetworkGETEX 末尾 `(tsExpiry.HasValue &&
//! tsExpiry.Value.Ticks > 0) ? ... : 0` 把过去绝对时刻折算为 expiry=0；
//! RMWMethods.cs GETEX 分支 `input.arg1 > 0` 才设置过期，arg1==0 且非
//! PERSIST 时 ipuResult=NotUpdated，既有 TTL 保留不动。
//!
//! 回归背景：修复前 rust 把 target <= now 折算为 PERSIST，主动清除既有
//! TTL，与 C# 相反。

use std::sync::Arc;

use itoa::Buffer;
use tempfile::tempdir;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wdev::SegmentedDevice;
use wkv::{BatchStoreSession, WedbStore};
use wnode::{
  resp::resp_server_session::RespServerSession,
  storage::session::common::ttl_sync::{put_ttl_sync, ttl_of_sync},
};
use wtest_base::test_store_config;

type TestBatch<'a> = BatchStoreSession<'a, SegmentedDevice>;

fn with_test_env(f: impl FnOnce(&mut RespServerSession, &TestBatch)) {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("resp.db")).unwrap());
  // GC 关闭保持 TTL 惰性过期语义
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let mut resp = RespServerSession::default();
  f(&mut resp, &batch);
}

/// 回归：GETEX EXAT/PXAT 过去时刻须保留既有 TTL（对标 NetworkGETEX 经 RMW NotUpdated 的语义）
#[test]
fn getex_past_absolute_time_keeps_existing_ttl() {
  with_test_env(|s, batch| {
    let key = b"getex-past-absolute";
    let mut out = Vec::new();
    s.network_set(&[key, b"Value"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    // EXAT 过去时刻：既有 TTL 必须原样保留
    for option in [&b"EXAT"[..], b"PXAT"] {
      out.clear();
      let old_ttl = now_ticks() + 60 * TICKS_PER_SECOND;
      put_ttl_sync(batch, key, old_ttl).unwrap();
      // EXAT 1000 / PXAT 1000 均为 1970 年，必然在过去
      let alive = s
        .network_getex(&[key, option, b"1000"], batch, &mut out)
        .unwrap();
      assert!(alive);
      assert_eq!(out, b"$5\r\nValue\r\n");
      let kept = ttl_of_sync(batch, key)
        .unwrap()
        .value()
        .unwrap()
        .expect("GETEX 过去绝对时刻不得清除既有 TTL");
      assert!(
        kept > now_ticks() + 59 * TICKS_PER_SECOND,
        "既有 TTL 被改动：{kept}"
      );
    }

    // 对照组：EXAT 未来时刻仍设置新过期
    out.clear();
    let mut buf = Buffer::new();
    let future = buf.format((now_ticks() + 120 * TICKS_PER_SECOND) / 10_000_000 + 1);
    let alive = s
      .network_getex(&[key, b"EXAT", future.as_bytes()], batch, &mut out)
      .unwrap();
    assert!(alive);
    assert_eq!(out, b"$5\r\nValue\r\n");
    let renewed = ttl_of_sync(batch, key).unwrap().value().unwrap().unwrap();
    assert!(renewed > now_ticks() + 119 * TICKS_PER_SECOND);

    // 对照组：显式 PERSIST 清除 TTL
    out.clear();
    let alive = s
      .network_getex(&[key, b"PERSIST"], batch, &mut out)
      .unwrap();
    assert!(alive);
    assert_eq!(out, b"$5\r\nValue\r\n");
    assert_eq!(ttl_of_sync(batch, key).unwrap().value(), Some(None));
  });
}

/// 回归：GETEX 无过期选项时既有 TTL 不动（权威映射在 basic_commands.rs::network_getex）
#[test]
fn getex_without_option_keeps_ttl() {
  with_test_env(|s, batch| {
    let key = b"getex-no-option";
    let mut out = Vec::new();
    s.network_set(&[key, b"Value"], batch, None, &mut out)
      .unwrap();
    put_ttl_sync(batch, key, now_ticks() + 60 * TICKS_PER_SECOND).unwrap();

    out.clear();
    let alive = s.network_getex(&[key], batch, &mut out).unwrap();
    assert!(alive);
    assert_eq!(out, b"$5\r\nValue\r\n");
    let kept = ttl_of_sync(batch, key).unwrap().value().unwrap().unwrap();
    assert!(kept > now_ticks() + 59 * TICKS_PER_SECOND);
  });
}
