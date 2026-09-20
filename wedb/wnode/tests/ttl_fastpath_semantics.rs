//! TTL 同步读快路径语义回归测试
//!
//! 对标 garnet ReadMethods.cs:Reader 内 LogRecordUtils.cs:CheckExpiry 同栈判定
//!（HasExpiration && Expiration < UtcNow.Ticks，严格小于）：
//! - 未过期 TTL 键同步读零拷贝放行（不再全量降级异步慢路径）；
//! - 已过期键读路径快路径直接 NOTFOUND：GET 回 nil、TTL 回 -2、EXISTS 回 0、
//!   SETNX 视键缺失写入成功，物理清理留写路径惰性清退与后台 GC。

use std::sync::Arc;

use tempfile::tempdir;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wdev::SegmentedDevice;
use wkv::{BatchStoreSession, StoreResult, WedbStore};
use wnode::{
  resp::{key_admin_commands::TtlCmd, resp_server_session::RespServerSession},
  storage::session::common::ttl_sync::{
    probe_alive, put_ttl_sync, read_adjudicated_user_sync, ttl_of_sync,
  },
};
use wtest_base::test_store_config;

type TestBatch<'a> = BatchStoreSession<'a, SegmentedDevice>;

fn with_test_env(f: impl FnOnce(&mut RespServerSession, &TestBatch)) {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("resp.db")).unwrap());
  // GC 关闭保持 TTL 惰性过期语义（过期键物理驻留由快路径逻辑过期覆盖）
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let mut resp = RespServerSession::default();
  f(&mut resp, &batch);
}

/// 写入键并赋予已过期的 TTL（put_ttl_sync 落过去时刻 ticks，惰性未清除）
fn set_expired(batch: &TestBatch, key: &[u8], val: &[u8]) {
  batch.try_upsert_sync(key, val).unwrap().unwrap();
  put_ttl_sync(batch, key, now_ticks() - TICKS_PER_SECOND).unwrap();
  // 前置自检：TTL 记录在场且已过期
  let exp = ttl_of_sync(batch, key)
    .unwrap()
    .value()
    .unwrap()
    .expect("TTL 在场");
  assert!(exp < now_ticks(), "前置自检：TTL 必须已过期");
}

/// 读命令族对已过期键的应答口径（对标 C# Reader 内 CheckExpiry 失败即 NOTFOUND）：
/// GET nil / TTL -2 / EXISTS 0；未过期键放行快路径应答不变
#[test]
fn expired_key_read_commands_notfound() {
  with_test_env(|s, batch| {
    // 已过期键
    set_expired(batch, b"fp:dead", b"Value");
    let mut out = Vec::new();
    s.network_get(&[b"fp:dead"], batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n", "GET 已过期键必须回 nil");

    out.clear();
    s.network_ttl(TtlCmd::Ttl, &[b"fp:dead"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":-2\r\n", "TTL 已过期键必须回 -2（键不存在）");

    out.clear();
    s.network_exists(&[b"fp:dead"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n", "EXISTS 已过期键必须回 0");

    // 对照组：未过期 TTL 键放行快路径，应答口径不变
    batch
      .try_upsert_sync(b"fp:live", b"Value")
      .unwrap()
      .unwrap();
    put_ttl_sync(batch, b"fp:live", now_ticks() + 60 * TICKS_PER_SECOND).unwrap();
    out.clear();
    s.network_get(&[b"fp:live"], batch, &mut out).unwrap();
    assert_eq!(out, b"$5\r\nValue\r\n", "未过期 TTL 键必须快路径直读命中");

    out.clear();
    s.network_ttl(TtlCmd::Ttl, &[b"fp:live"], batch, &mut out)
      .unwrap();
    assert!(out.starts_with(b":"), "未过期键 TTL 必须返回正数而非降级");

    out.clear();
    s.network_exists(&[b"fp:live"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
  });
}

// libs/server/Resp/BasicCommands.cs:NetworkSETNX——已过期键视同不存在，
// NX 写入成功且旧 TTL 同步清退（写面自愈闭环），新值存活可读
#[test]
fn setnx_on_expired_key_succeeds_and_clears_ttl() {
  with_test_env(|s, batch| {
    set_expired(batch, b"fp:nx", b"old");
    let mut out = Vec::new();
    s.network_setnx(&[b"fp:nx", b"new"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n", "SETNX 对已过期键必须视缺失写入成功");

    assert_eq!(
      ttl_of_sync(batch, b"fp:nx").unwrap().value(),
      Some(None),
      "覆盖写入必须同步清退旧 TTL 记录"
    );
    out.clear();
    s.network_get(&[b"fp:nx"], batch, &mut out).unwrap();
    assert_eq!(out, b"$3\r\nnew\r\n", "写入后的新值必须存活可读");
  });
}

/// 存储层内核三态：read_adjudicated_user_sync 对过期 String 键直接闭环
/// NOTFOUND（不降级、不误探信封域回 WRONGTYPE）；probe_alive 视同不存在
#[test]
fn adjudicated_probe_expired_key_tri_state() {
  with_test_env(|_s, batch| {
    set_expired(batch, b"fp:tri", b"v");

    // 读裁决：NotFound = 快路径 NOTFOUND（修复前为降级异步）
    let read = read_adjudicated_user_sync(batch, b"fp:tri", |v| v.len()).unwrap();
    assert_eq!(
      read,
      StoreResult::NotFound,
      "过期键必须快路径闭环 NOTFOUND 而非降级异步"
    );

    // 存活探针：Ok(Some(false)) = 视同不存在（修复前为 Ok(None) 降级）
    assert_eq!(
      probe_alive(batch, b"fp:tri").unwrap(),
      Some(false),
      "过期键 probe_alive 必须视同不存在"
    );

    // 对照组：未过期键存活且读命中；缺失键存活探针同 Some(false)
    batch.try_upsert_sync(b"fp:alive", b"v").unwrap().unwrap();
    put_ttl_sync(batch, b"fp:alive", now_ticks() + 60 * TICKS_PER_SECOND).unwrap();
    let live = read_adjudicated_user_sync(batch, b"fp:alive", |v| v.len()).unwrap();
    assert_eq!(live, StoreResult::Success(Ok(1)), "未过期键快路径直读命中");
    assert_eq!(probe_alive(batch, b"fp:alive").unwrap(), Some(true));
    assert_eq!(probe_alive(batch, b"fp:missing").unwrap(), Some(false));
  });
}
