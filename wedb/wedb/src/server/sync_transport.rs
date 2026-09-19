//! 迁移停等链与 diskless 快照链共用的流式传输件 (SyncTransport)
//!
//! RangeIndex 与向量集的带外流式发送核心：源端快照/装配 → 帧编码 →
//! 逐块发送（帧发送经闭包注入，两链各自挂停等或直写出口）。二者是共享
//! 传输件而非迁移私件，故独立于 migration 目录、由迁移与复制各引一处。
//! 编排面（sketch 门控、会话取消、停等限时）留在各自的迁移门面文件。
//! 注：RangeIndex 与向量集迁移编排是本仓相对 C# 的扩展面，C# 无同名函数，
//! 本模块不挂 cs 映射锚点。

use std::{result, slice::from_ref};

use gxhash::HashMap as GxHashMap;
use wconn::record::{
  MigrateVectorElement, encode_range_index_stream_payload_into, encode_vector_set_element_payload,
  encode_vector_set_index_payload,
};
use wdev::Device;
use wkv::StoreSession;
use wnode::{
  rangeindex::{RangeIndexManagerMigration, TransmitActivity},
  resp::vector::{
    vector_manager::{INDEX_SIZE_BYTES, VectorManager},
    vector_manager_index::Index,
    vector_manager_locking::registry_user_key,
  },
};

use crate::{
  client::GarnetClient,
  error::{Error, Result},
};

/// 停等单批最大记录条数 (64 条；rust 停等批模型特有条数上限，C# 由流式
/// client 迭代缓冲自然限批无对应物)。迁移驱动与 diskless 快照共用
pub const MAX_MIGRATION_BATCH_COUNT: usize = 64;

/// 源端快照单个 RangeIndex 键并分块流式发送（帧发送经 `send` 注入：迁移
/// 停等链与 diskless 快照链共用；快照/读块失败 `Ok(false)`+活动日志，发送
/// 失败 `Err(E)` 上抛交调用方判 poison）
pub async fn transmit_range_index_stream<D: Device, E>(
  store_session: &StoreSession<D>,
  key: &[u8],
  chunk_size: usize,
  mut send: impl AsyncFnMut(&[u8]) -> result::Result<(), E>,
) -> result::Result<bool, E> {
  let mut transmit_activity = TransmitActivity::start_activity();
  let mut reader =
    match RangeIndexManagerMigration::snapshot_range_index_and_create_reader(store_session, key)
      .await
    {
      Ok(reader) => reader,
      Err(e) => {
        transmit_activity.on_error(&e.to_string());
        transmit_activity.end_and_log_activity(key);
        log::error!(
          "TransmitRangeIndexAsync: error during snapshot for key {}: {e}",
          String::from_utf8_lossy(key)
        );
        return Ok(false);
      }
    };

  transmit_activity.on_snapshot_completed(reader.total_file_bytes() as i64);

  let mut buffer = vec![0u8; chunk_size];
  let mut payload = Vec::with_capacity(4 + 1 + 4 + chunk_size);
  while !reader.is_complete() {
    let payload_len = match reader.read_next_chunk(&mut buffer) {
      Ok(len) => len,
      Err(e) => {
        transmit_activity.on_error(&e.to_string());
        transmit_activity.end_and_log_activity(key);
        log::error!(
          "TransmitRangeIndexAsync: reader error for key {}: {e}",
          String::from_utf8_lossy(key)
        );
        return Ok(false);
      }
    };

    if payload_len == 0 {
      transmit_activity.on_error("Zero-length payload from reader");
      transmit_activity.end_and_log_activity(key);
      log::error!(
        "TransmitRangeIndexAsync: reader returned zero-length payload with a {chunk_size}-byte buffer for key {}",
        String::from_utf8_lossy(key)
      );
      return Ok(false);
    }

    encode_range_index_stream_payload_into(&buffer[..payload_len], &mut payload);
    send(&payload).await?;

    transmit_activity.on_chunk_sent(payload_len);
  }

  transmit_activity.end_and_log_activity(key);
  Ok(true)
}

/// 元素帧单批发送（批满/超限冲刷，对标迭代缓冲批量冲刷形态；帧发送经
/// `send` 注入）
async fn send_elements(
  key: &[u8],
  elements: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>,
  max_chunk: usize,
  mut send: impl AsyncFnMut(&[u8]) -> Result<()>,
) -> Result<()> {
  let mut items: Vec<MigrateVectorElement> = Vec::new();
  let mut batch_bytes = 0usize;

  macro_rules! flush {
    () => {
      if !items.is_empty() {
        let payload = encode_vector_set_element_payload(&items);
        send(&payload).await?;
        items.clear();
      }
    };
  }

  for (element, values, attributes) in elements {
    let item = MigrateVectorElement {
      key: key.to_vec(),
      element,
      values,
      attributes,
    };
    if item.frame_len() > max_chunk {
      // 超限单元素独立成批（帧结构固定四段，不切块；载荷上限内天然可行）。
      // 先冲既有批并复位批字节计数，杜绝陈旧计数提前触发下一轮冲刷
      flush!();
      batch_bytes = 0;
      let payload = encode_vector_set_element_payload(from_ref(&item));
      send(&payload).await?;
      continue;
    }
    if !items.is_empty()
      && (items.len() >= MAX_MIGRATION_BATCH_COUNT || batch_bytes + item.frame_len() > max_chunk)
    {
      flush!();
      batch_bytes = 0;
    }
    batch_bytes += item.frame_len();
    items.push(item);
  }
  flush!();
  Ok(())
}

/// 向量集帧传输核心：预留 → 源→目标重映射 → 逐键索引帧 + 元素批帧（帧发送
/// 经 `send` 注入：迁移停等链与 diskless 快照链共用）。返回 false = 远端
/// 拒绝/装配缺失（判败走 recover）；Err 为停等超时/取消
pub async fn transmit_vector_set_frames(
  client: &GarnetClient,
  vm: &VectorManager,
  vector_set_keys: &[(Vec<u8>, [u8; INDEX_SIZE_BYTES])],
  max_chunk: usize,
  is_cancelled: impl Fn() -> bool,
  mut send: impl AsyncFnMut(&[u8]) -> Result<()>,
) -> Result<bool> {
  // 1. 目标端上下文预留 + 源→目标重映射表（一键一上下文；预留应答序与
  //    键序对齐）
  let reserved = match client
    .reserve_vector_set_contexts_async(vector_set_keys.len())
    .await
  {
    Ok(r) => r,
    Err(e) => {
      log::error!("ReserveDestinationVectorSetsAsync: 预留目标端上下文失败: {e}");
      return Ok(false);
    }
  };
  if reserved.len() != vector_set_keys.len() {
    log::error!(
      "ReserveDestinationVectorSetsAsync: 预留数不符 (need {}, got {})",
      vector_set_keys.len(),
      reserved.len()
    );
    return Ok(false);
  }
  let mut namespace_map: GxHashMap<u64, u64> = GxHashMap::default();
  for ((_, src_index), dst_ctx) in vector_set_keys.iter().zip(&reserved) {
    if let Some(index) = Index::from_bytes(src_index) {
      namespace_map.insert(index.context, *dst_ctx);
    }
  }

  // 2/3. 逐键：索引帧（停等）→ 元素分批帧（停等）。帧口径恒为剥域用户键
  //（C# 无域前缀，目标端按本端会话域重新复合）
  for (rk, src_index) in vector_set_keys {
    if is_cancelled() {
      return Err(Error::OperationCancelled);
    }
    let key = registry_user_key(rk);

    let Some(dst_index) = vm.remap_index_for_migration(src_index, &namespace_map) else {
      log::error!(
        "向量集键 {} 上下文无预留映射，迁移中止",
        String::from_utf8_lossy(key)
      );
      return Ok(false);
    };

    let index_payload = encode_vector_set_index_payload(key, &dst_index);
    if let Err(e) = send(&index_payload).await {
      log::error!(
        "向量集索引帧发送失败 key {}: {e}",
        String::from_utf8_lossy(key)
      );
      return Err(e);
    }

    let elements = vm
      .export_migration_elements(src_index)
      .into_iter()
      .map(|e| (e.element, e.values, e.attributes))
      .collect();
    if let Err(e) = send_elements(key, elements, max_chunk, &mut send).await {
      log::error!(
        "向量集元素帧发送失败 key {}: {e}",
        String::from_utf8_lossy(key)
      );
      return Err(e);
    }
  }
  Ok(true)
}
