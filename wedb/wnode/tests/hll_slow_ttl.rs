//! HLL 慢路径写回键级 TTL 语义回归测试
//!
//! 修复前：slow_hll_add（HllCold::Present 分支）与 slow_hll_merge 的 dest
//! 写回用 storage.upsert_string（SET 语义：del_ttl + 信封清退），带 TTL 的
//! 冷区键经慢路径 PFADD/PFMERGE 后键级 TTL 被误清，与快路径 store_hll 的
//! try_rmw_sync（保留 TTL）同命令两路漂移。修复后慢路径写回统一
//! storage.rmw_string（RMW 语义），对标 garnet
//! libs/server/Storage/Functions/MainStore/RMWMethods.cs:CopyUpdater 的
//! PFADD/PFMERGE 分支 TryCopyOptionals 保留 Expiration
//!（libs/storage/Tsavorite/cs/src/core/Allocator/LogRecord.cs:TryCopyOptionals）。

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::tempdir;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, StoreSession, WedbStore};
use wnode::{
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  storage::session::common::ttl_sync::put_ttl_sync,
};
use wresp::command::RespCommand;

/// 执行域：api 会话（命令面）+ probe 会话（TTL 异步闭环断言面，
/// 跨会话经共享索引可见）
fn open_env(
  tag: &str,
) -> (
  Runtime,
  GarnetApi,
  Arc<WedbStore<SegmentedDevice>>,
  StoreSession<SegmentedDevice>,
  tempfile::TempDir,
) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let probe = store.new_session().unwrap();
  (Runtime::new().unwrap(), api, store, probe, dir)
}

/// 挂接分派器的会话（降级链路须经 session.garnet_api 挂起 SlowWait）
fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 核心失败场景：带 TTL 的冷区键 PFADD 转慢路径后，写回必须保留键级 TTL
///（修复前 upsert_string 误清为 -1）且基数累加可见
#[test]
fn slow_pfadd_preserves_key_ttl() {
  let (rt, api, store, probe, _dir) = open_env("hll-slow-pfadd-ttl.db");
  let mut s = session_with(&api);

  // 建键 + 键级 TTL 60s
  api.exec(
    &mut s,
    RespCommand::Pfadd,
    &[b"hll", b"e1", b"e2", b"e3", b"e4", b"e5"],
  );
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Expire, &[b"hll", b"60"]);
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();

  // 冷化：数据与 TTL 记录均落盘（PFADD 快路径转降级）
  rt.block_on(store.flush_and_evict_all()).unwrap();

  // 慢路径 PFADD：写回保留键级 TTL（RMW 语义，对标 CopyUpdater PFADD 分支）
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfadd,
    vec![b"hll".to_vec(), b"e6".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b":1\r\n");

  let ttl = rt.block_on(probe.pttl_ms(b"hll")).unwrap();
  assert!(
    ttl > 0,
    "慢路径 PFADD 必须保留键级 TTL（修复前误清为 -1）：{ttl}"
  );

  // 值可见：旧基数 + 新元素（修复前后一致，防回归护栏）
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfcount,
    vec![b"hll".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b":6\r\n");
}

/// 带 TTL 的冷区 dest PFMERGE 慢路径写回保留 dest 键级 TTL
///（对标 CopyUpdater PFMERGE 分支 TryCopyOptionals）
#[test]
fn slow_pfmerge_preserves_dest_ttl() {
  let (rt, api, store, probe, _dir) = open_env("hll-slow-pfmerge-ttl.db");
  let mut s = session_with(&api);

  api.exec(&mut s, RespCommand::Pfadd, &[b"h1", b"x1", b"x2", b"x3"]);
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Pfadd, &[b"h2", b"y1", b"y2"]);
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();

  // dest h1 键级 TTL 60s
  api.exec(&mut s, RespCommand::Expire, &[b"h1", b"60"]);
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();

  rt.block_on(store.flush_and_evict_all()).unwrap();

  // dest 冷区：PFMERGE 转慢路径，写回保留 h1 键级 TTL
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfmerge,
    vec![b"h1".to_vec(), b"h2".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b"+OK\r\n");

  let ttl = rt.block_on(probe.pttl_ms(b"h1")).unwrap();
  assert!(
    ttl > 0,
    "慢路径 PFMERGE 必须保留 dest 键级 TTL（修复前误清为 -1）：{ttl}"
  );

  // 并集基数正确
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfcount,
    vec![b"h1".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b":5\r\n");
}

/// 已过期键慢路径 PFADD 重建：probe_alive 惰性过期物理清除后视同缺失，
/// 新建键 GET（PFCOUNT）可见且无幽灵 TTL 残留（对标 C# CheckExpiry →
/// ExpireAndResume 后转 InitialUpdater，新记录无 Expiration）
#[test]
fn slow_pfadd_on_expired_key_rebuilds_visible_without_ttl() {
  let (rt, api, store, probe, _dir) = open_env("hll-slow-pfadd-expired.db");
  let mut s = session_with(&api);

  api.exec(
    &mut s,
    RespCommand::Pfadd,
    &[b"hll", b"e1", b"e2", b"e3", b"e4", b"e5"],
  );
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();

  // 直写过期 TTL 记录（过去时刻，RESP 面无法自然构造"过期未清"态）
  {
    let batch = probe.enter_batch();
    put_ttl_sync(&batch, b"hll", now_ticks() - TICKS_PER_SECOND).unwrap();
  }

  rt.block_on(store.flush_and_evict_all()).unwrap();

  // 慢路径 PFADD：过期裁决物理清除后按缺失重建（绝不写入即幽灵）
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfadd,
    vec![b"hll".to_vec(), b"e6".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b":1\r\n");

  // 重建键可见：仅本次新元素
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfcount,
    vec![b"hll".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b":1\r\n");

  // 重建键无 TTL 残留
  let ttl = rt.block_on(probe.pttl_ms(b"hll")).unwrap();
  assert_eq!(ttl, -1, "过期重建键不得残留过期 TTL 记录：{ttl}");
}
