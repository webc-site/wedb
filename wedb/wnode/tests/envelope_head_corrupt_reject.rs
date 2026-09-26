//! 信封头部探针畸形载荷显式 Corrupt 拒载回归（zcode-r46-wcolser 发现二）
//!
//! 缺陷形：长度族 env_decode_head 对载荷不足头部（计数域 <4B / 水位域类型
//! <12B）的信封 unwrap_or 兜底——count 计 0、水位按 i64::MAX 永快道，HLEN/
//! SCARD/ZCARD/LLEN 静默出假计数零日志，与装载族 env_decode_object 的 Corrupt
//! 错误帧（corrupt_payload_reject 漏斗）同键同损坏态双标准，违背
//! object_payload「畸形载荷显式失败」契约（C# 无头部快路径，截断载荷经
//! BinaryReader 抛 EndOfStreamException 可见失败）。
//!
//! 修复形态：头部探针 None 一律显式 Err(EnvDecodeErr::Corrupt)，复用
//! step_envelope 既有 corrupt_payload_reject 臂，单一 fail-fast 漏斗收敛。
//!
//! 自研回归锁: 长度族信封头 fail-fast 漏斗（深层防御面，损坏信封须先击穿
//! 记录级 CRC，可达性口径与票面声明一致）

use std::sync::Arc;

use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::err_frame;
use wtest_base::{open_test_store, resp_frame as frame};
use wval::{GarnetObjectType, KeyTag};

type TestStore = WedbStore<SegmentedDevice>;

/// 载荷畸形错误帧（与 wnode/resp/objects/object_store_utils.rs 单一常量同文）
fn corrupt_frame() -> Vec<u8> {
  err_frame("ERR Corrupted object payload")
}

fn consumer_on(store: &Arc<TestStore>) -> RespSessionConsumer {
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// 喂一帧并取输出（长度族信封态读同步段直出，无慢路径挂起）
fn feed(c: &mut RespSessionConsumer, args: &[&[u8]]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(&frame(args));
  c.return_recv_scratch(scratch);
  let mut out = Vec::new();
  let _ = c.try_consume_messages_into(&mut out);
  out
}

/// 直塞原始信封物理记录（绕过业务写路径，构造畸形头载荷）
fn seed_envelope(store: &Arc<TestStore>, key: &[u8], raw: &[u8]) {
  let sess = store.new_session().unwrap();
  let batch = sess.enter_batch();
  batch
    .try_upsert_tag_sync(key, KeyTag::ObjectEnvelope, raw)
    .unwrap()
    .unwrap();
}

#[compio::test]
async fn truncated_hash_envelope_rejects_corrupt() {
  let (_dir, store) = open_test_store("env_head_hash_trunc").unwrap();

  // 载荷 2B < 4B 计数域
  seed_envelope(&store, b"h", &[GarnetObjectType::Hash as u8, 1, 2]);
  // 载荷 7B ∈ 4..12B：计数域完整、水位域截断
  seed_envelope(
    &store,
    b"h2",
    &[GarnetObjectType::Hash as u8, 1, 0, 0, 0, 0xff, 0xff, 0xff],
  );

  let mut c = consumer_on(&store);
  assert_eq!(feed(&mut c, &[b"HLEN", b"h"]), corrupt_frame());
  assert_eq!(feed(&mut c, &[b"HLEN", b"h2"]), corrupt_frame());
}

#[compio::test]
async fn truncated_zset_envelope_rejects_corrupt() {
  let (_dir, store) = open_test_store("env_head_zset_trunc").unwrap();

  seed_envelope(&store, b"z", &[GarnetObjectType::SortedSet as u8, 9]);
  seed_envelope(
    &store,
    b"z2",
    &[
      GarnetObjectType::SortedSet as u8,
      2,
      0,
      0,
      0,
      0x01,
      0x02,
      0x03,
    ],
  );

  let mut c = consumer_on(&store);
  assert_eq!(feed(&mut c, &[b"ZCARD", b"z"]), corrupt_frame());
  assert_eq!(feed(&mut c, &[b"ZCARD", b"z2"]), corrupt_frame());
}

/// 完整头部（4B 计数 + 8B 永快道水位）不受修复影响：HLEN 快道直读计数
#[compio::test]
async fn intact_hash_envelope_head_count_still_short_circuits() {
  let (_dir, store) = open_test_store("env_head_intact").unwrap();

  let mut raw = vec![GarnetObjectType::Hash as u8];
  raw.extend_from_slice(&2_u32.to_le_bytes());
  raw.extend_from_slice(&i64::MAX.to_le_bytes());
  seed_envelope(&store, b"h", &raw);

  let mut c = consumer_on(&store);
  assert_eq!(feed(&mut c, &[b"HLEN", b"h"]), b":2\r\n");
}

/// 无水位域类型（List）恒 4B 头：正常头计数应答不受影响，截断头同漏斗拒载
#[compio::test]
async fn list_envelope_head_short_and_truncated() {
  let (_dir, store) = open_test_store("env_head_list").unwrap();

  let mut raw = vec![GarnetObjectType::List as u8];
  raw.extend_from_slice(&5_u32.to_le_bytes());
  seed_envelope(&store, b"l", &raw);
  seed_envelope(&store, b"l2", &[GarnetObjectType::List as u8, 7, 7]);

  let mut c = consumer_on(&store);
  assert_eq!(feed(&mut c, &[b"LLEN", b"l"]), b":5\r\n");
  assert_eq!(feed(&mut c, &[b"LLEN", b"l2"]), corrupt_frame());
}
