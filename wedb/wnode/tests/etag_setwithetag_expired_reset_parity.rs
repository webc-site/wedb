//! SETWITHETAG 过期未回收键 etag 重置的快慢双臂对拍回归（票 zcode-r139c-etag2 案三 P2）
//!
//! 缺陷面：rust ETag 为 KeyTag::Etag 独立旁路记录、自身无 TTL
//! （etag_sync.rs:24-48 纯侧读无存活门）。快臂 `network_setwithetag` 已握
//! `read_user_sync` 含 TTL 裁决的存活判定却只消费 WrongType 位，Hit/Missing
//! （过期键即 Missing）结果弃置，`etag_of_sync` 旁路直取过期未回收键的残留
//! 旧 etag 落 stale+1；慢臂 `read_value_and_etag_async` Missing 臂
//! existing=NO_ETAG 回 1，C# 过期记录 ETag RMW 先行 RemoveETag +
//! ExpireAndResume（RMWMethods.cs:441-446）后按 HandleSetWithEtagInitialUpdate
//! 初写口径 newEtag = NoETag + 1 = 1（RMWMethods.Etags.cs:289-311）——同键同序
//! 在内存热态 / 冷态 / C# 三面 etag 应答分叉，客户端乐观并发基线随分层升降漂移。
//!
//! 修复形态：复用既有存活裁决单点分流——Missing（含过期）→ existing 视同
//! NO_ETAG（勿再探旁路残值，初写 etag=1）；Deferred → `Ok(false)` 沿既有降级
//! 通道交慢臂；WrongType 现状保留；不新增第二存活探针。
//!
//! 全真存储真协议帧无 mock：同环境先热态快臂跑一遍过期重置序列，再经案一同款
//! 16KB×4 页环形压力翻转使同形态键转磁盘候选，慢臂承接后逐字节对拍双臂应答。

use std::{sync::Arc, time::Duration};

use compio::time::sleep;
use tempfile::{TempDir, tempdir};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wtest_base::resp_frame_str;

/// 压力翻转执行域（案一/案二同款小环形日志）：16KB×4 页，回绕后前序记录转磁盘候选
fn env(
  tag: &str,
) -> aok::Result<(
  TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  GarnetApi,
  RespServerSession,
)> {
  let dir = tempdir()?;
  let config = StoreConfig::new(1024, 16 * 1024, 4, 0.5)?;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag))?);
  let store = Arc::new(WedbStore::open(config, device)?);
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session()?));
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  Ok((dir, store, api, s))
}

/// 单命令往返到闭环，返回 (应答字节, 是否挂慢臂)
async fn pump(s: &mut RespServerSession, args: &[&str]) -> (Vec<u8>, bool) {
  let input = resp_frame_str(args);
  s.recv_buffer.clear();
  s.recv_buffer.extend_from_slice(&input);
  s.bytes_read = input.len();
  s.read_head = 0;
  s.end_read_head = 0;
  s.output.clear();
  assert!(s.try_consume_messages().is_some(), "无协议违规");
  let mut wire = Vec::new();
  wnode_test::drive_pending_parks(s, &mut wire, false).await;
  match s.take_slow_wait() {
    Some(slow) => {
      let reply = slow.resolve().await;
      s.resolve_slow_wait_into(&reply, &mut wire);
      (wire, true)
    }
    None => {
      s.take_output_into(&mut wire);
      (wire, false)
    }
  }
}

/// `[etag, value]` 期望帧（RESP2，与 write_etag_val_array 同形）
fn etag_pair(etag: i64, val: &[u8]) -> Vec<u8> {
  let mut out = format!("*2\r\n:{etag}\r\n${}\r\n", val.len()).into_bytes();
  out.extend_from_slice(val);
  out.extend_from_slice(b"\r\n");
  out
}

/// 过期序列：SETWITHETAG 两笔递增到 etag=2 后携 PX 1 即刻过期，回读终态
async fn seed_expired(s: &mut RespServerSession, key: &str) {
  assert_eq!(pump(s, &["SETWITHETAG", key, "v1"]).await.0, b":1\r\n");
  assert_eq!(
    pump(s, &["SETWITHETAG", key, "v2", "PX", "1"]).await.0,
    b":2\r\n"
  );
}

/// 环形页翻转风暴：足量大值填充写回绕 4 页日志，使前序记录转磁盘候选
async fn wrap_log_storm(s: &mut RespServerSession) -> usize {
  let val = "f".repeat(700);
  let mut degraded = 0usize;
  for i in 0..300 {
    let key = format!("filler{i}");
    let (out, parked) = pump(s, &["SET", &key, &val]).await;
    assert_eq!(out, b"+OK\r\n", "风暴填充写 {key} 须闭环");
    degraded += parked as usize;
  }
  assert!(degraded > 0, "测试前提：回绕 4 页容量应至少触发一次降级");
  degraded
}

/// 案三要害：过期未回收键上 SETWITHETAG——快臂（热态直证）回初写 1 而非
/// stale+1=3（修复前旁路直读残值），GETWITHETAG 观测 [1, v3]；再经压力翻转
/// 使同形态键转磁盘候选，慢臂承接与快臂逐字节对拍（C# 口径双臂收敛）
#[compio::test]
async fn expired_setwithetag_resets_etag_and_arms_parity() -> aok::Result<()> {
  let (_dir, _store, _api, mut s) = env("etag2-expired-reset.db")?;

  // 热态（快臂）：etag 已递增到 2 的键过期后再 SETWITHETAG
  seed_expired(&mut s, "eh").await;
  sleep(Duration::from_millis(60)).await;
  let (hot_set, hot_parked) = pump(&mut s, &["SETWITHETAG", "eh", "v3"]).await;
  assert!(
    !hot_parked,
    "热态夹具应快臂闭环（否则本用例不触达快臂存活裁决分流）"
  );
  assert_eq!(
    hot_set, b":1\r\n",
    "快臂过期未回收键须按 C# RemoveETag 初写口径回 1（修复前读旁路残值回 3）"
  );

  // 冷态（慢臂承接）：同形态键在风暴后转磁盘候选，快臂探针 Deferred 降级
  seed_expired(&mut s, "ec").await;
  wrap_log_storm(&mut s).await;
  let (cold_set, cold_parked) = pump(&mut s, &["SETWITHETAG", "ec", "v3"]).await;
  assert!(cold_parked, "风暴后 ec 须挂慢臂承接（否则对拍不触达慢臂）");
  assert_eq!(
    cold_set, b":1\r\n",
    "慢臂 Missing 臂 existing=NoETag 初写口径（既有正确形态，零漂移哨兵）"
  );
  assert_eq!(
    cold_set, hot_set,
    "SETWITHETAG 过期重置应答快慢双臂逐字节全等"
  );

  // GETWITHETAG 观测面对拍：两键终态皆 [1, v3]（跨过期边界 etag 域从头计）
  let (hot_get, _) = pump(&mut s, &["GETWITHETAG", "eh"]).await;
  let (cold_get, _) = pump(&mut s, &["GETWITHETAG", "ec"]).await;
  assert_eq!(hot_get, etag_pair(1, b"v3"), "快臂键回读 [1, v3]");
  assert_eq!(cold_get, hot_get, "GETWITHETAG 回读快慢双臂逐字节全等");
  Ok(())
}
