//! 副本推流记录帧单源（物理键编码 + 真实 AOF 条目产出 + 完整记录帧）
//!
//! 收口 replica_background_replay / replica_driver_store_generation /
//! replica_recover_clamp_partial_resync / replica_replay_truncate_clamp /
//! replicate_switch_generation 五册逐字同形的记录面：`physical` 物理键编码、
//! `source_entries` 真实 SET 条目产出（临时 GarnetLog 入队 + 扫描取
//! payload，严禁假 mock）、`record_frame` 8B wal 记录头 + 条目的主端推流
//! 帧口径。产出目录名以 `tag` 参数保留各册原值。宿主册直挂（沿用
//! primary_assets 先例）：
//!
//! ```text
//! #[path = "common/replica_entry_frames.rs"]
//! mod replica_entry_frames;
//! use replica_entry_frames::{physical, record_frame, source_entries};
//! ```

use std::sync::Arc;

use waof::{AofEntryType, WalFrameHeader};
use wconf::RuntimeServerOptions;
use wnode::aof::garnet_log::{GarnetLog, RecordShape};
use wnode_test::replay_input_bytes;
use wresp::command::RespCommand;
use wval::{KeyTag, NamespaceDbCodec};

/// 物理键编码（条目 key 统一为 wkv 物理键：[NsVarint][DbVarint][KeyTag][用户键]）
pub fn physical(user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// 单条 AOF 条目产出（临时 GarnetLog 入队 shape 后扫描取首条 payload）
pub fn entry_payload(tag: &str, enqueue: impl FnOnce(&GarnetLog)) -> Vec<u8> {
  let options = RuntimeServerOptions::default();
  let (_dirs, backends) = wnode_test::test_sublogs(tag, 1);
  let log = Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog"));
  enqueue(&log);
  let begin = log.get_begin_address(0);
  let mut records = Vec::new();
  log.scan_single_with(0, begin, log.get_tail_address(0), |r| {
    records.push(r.clone());
    true
  });
  records
    .into_iter()
    .next()
    .map(|r| r.payload)
    .expect("条目产出")
}

/// 真实 AOF SET 条目产出（StoreUpsert 记录，对标 C# ProcessAofRecordInternal
/// 消费的形态；`tag` 为临时 GarnetLog 子日志目录名，各册原值保留）
pub fn source_entries(tag: &str, key: &[u8], value: &[u8]) -> Vec<u8> {
  entry_payload(tag, |log| {
    let input = replay_input_bytes(RespCommand::Set, vec![key.to_vec(), value.to_vec()]);
    let _ = log.enqueue(&RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 0,
      session_id: 1,
      key: &physical(key),
      value,
      input: &input,
      database_id: 0,
    });
  })
}

/// 完整记录帧（8B wal 记录头 + AOF 条目，主端推流帧口径）
pub fn record_frame(entry: &[u8]) -> Vec<u8> {
  let mut frame = WalFrameHeader::for_payload_parts(&[entry])
    .to_bytes()
    .to_vec();
  frame.extend_from_slice(entry);
  frame
}
