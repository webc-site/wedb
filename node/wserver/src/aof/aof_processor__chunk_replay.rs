//! 分块记录重放（对标 libs/server/AOF/AofProcessor.ChunkReplay.cs）
//!
//! [`ChunkedAccumulator`](crate::aof::aof_chunked_record_reader::ChunkedAccumulator)
//! 完成后的直接重放路径：分块记录恒为数据操作（Store/Object/Unified 的
//! upsert/RMW/delete），永非事务标记 / 检查点 / FLUSH / 存储过程 / 向量操作，
//! 故仅与事务缓冲和操作分派交互（无连续记录镜像物化）。

use wdev::Device;

use super::{
  aof_chunked_record_reader::ChunkedAccumulator,
  aof_processor::{AofProcessor, PreparedParameters, ReplayTarget},
};
use crate::aof::aof_entry_type::AofEntryType;

/// libs/server/AOF/AofProcessor.ChunkReplay.cs:ProcessAofRecordInternal
///
/// 事务活动期内入组缓冲；否则按常规分派重放。
pub async fn process_chunked_record<D: Device>(
  processor: &AofProcessor,
  virtual_sublog_idx: usize,
  acc: ChunkedAccumulator,
  as_replica: bool,
  log_address_sequence_number: i64,
  target: &ReplayTarget<'_, '_, D>,
) -> Result<bool, String> {
  let buffered = processor.coordinator().buffer_chunk_operation(
    virtual_sublog_idx,
    acc.session_id,
    super::replaycoordinator::aof_replay_context::ReplayOperation::Chunk(Box::new(acc.clone())),
  );
  if buffered {
    return Ok(false);
  }
  // 非事务路径：先做读一致性时间戳推进（对齐非分块拓扑预处理）
  replay_op_dispatch_chunk(
    processor,
    virtual_sublog_idx,
    &acc,
    as_replica,
    log_address_sequence_number,
    target,
  )
  .await?;
  Ok(false)
}

/// libs/server/AOF/AofProcessor.ChunkReplay.cs:ReplayOpDispatch
///
/// 分块形态的分派：执行读一致性推进后交 [`replay_chunk`]。
pub async fn replay_op_dispatch_chunk<D: Device>(
  processor: &AofProcessor,
  virtual_sublog_idx: usize,
  acc: &ChunkedAccumulator,
  _as_replica: bool,
  log_address_sequence_number: i64,
  target: &ReplayTarget<'_, '_, D>,
) -> Result<(), String> {
  // 分块记录拓扑预处理：分片序列号 / 单物理日志地址推进一致性时间戳
  if (processor.append_only_file().log().size() > 1
    || processor.append_only_file().virtual_sublog_count() > 1)
    && let Some(manager) = processor.read_consistency_manager()
  {
    let sequence_number = if acc.sequence_number != 0 {
      acc.sequence_number
    } else {
      log_address_sequence_number
    };
    manager.update_virtual_sublog_key_sequence_number(
      virtual_sublog_idx,
      acc.key_hash,
      sequence_number,
    );
  }
  if !processor.begin_replay_op(processor.should_skip_record_chunk(
    virtual_sublog_idx,
    acc,
    target.store_version,
  )) {
    return Ok(());
  }
  replay_chunk(processor, virtual_sublog_idx, acc.clone(), target).await
}

/// libs/server/AOF/AofProcessor.ChunkReplay.cs:ReplayOp
///
/// 依累积器操作类型应用数据操作。
pub async fn replay_chunk<D: Device>(
  processor: &AofProcessor,
  _virtual_sublog_idx: usize,
  acc: ChunkedAccumulator,
  target: &ReplayTarget<'_, '_, D>,
) -> Result<(), String> {
  let prepared = PreparedParameters {
    key: acc.key_span().to_vec(),
    key_hash: acc.key_hash,
    payload: build_payload(&acc),
  };
  // 分块记录恒为 v4+ 布局（写端不再下发旧编号）
  processor
    .replay_op(acc.op_type, prepared, false, target)
    .await
}

/// 分块组件 → 负载字节（与 [`AofProcessor`](super::aof_processor::AofProcessor)
/// 的非分块解码面复用：长度前缀 value + input 原文）。
fn build_payload(acc: &ChunkedAccumulator) -> Vec<u8> {
  let mut payload = Vec::new();
  match acc.op_type {
    AofEntryType::StoreUpsert
    | AofEntryType::ObjectStoreUpsert
    | AofEntryType::UnifiedStoreStringUpsert
    | AofEntryType::UnifiedStoreObjectUpsert => {
      payload.extend_from_slice(&(acc.value_span().len() as u32).to_le_bytes());
      payload.extend_from_slice(acc.value_span());
      payload.extend_from_slice(acc.input_span());
    }
    _ => {
      payload.extend_from_slice(acc.input_span());
    }
  }
  payload
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::aof::aof_header::AofChunkHeader;

  #[test]
  fn payload_shape_per_op_type() {
    let chunk_header = AofChunkHeader {
      overflow_key_length: 1,
      overflow_value_length: 2,
      input_length: 0,
      object_id: 1,
      key_hash: 1,
    };
    let mut acc = ChunkedAccumulator::new(AofEntryType::StoreUpsert, &chunk_header);
    assert!(acc.feed(b"k"));
    assert!(acc.feed(b"vv"));
    let payload = build_payload(&acc);
    assert_eq!(payload, vec![2, 0, 0, 0, b'v', b'v']);

    let del_header = AofChunkHeader {
      overflow_key_length: 1,
      overflow_value_length: 0,
      input_length: 0,
      object_id: 2,
      key_hash: 1,
    };
    let mut del = ChunkedAccumulator::new(AofEntryType::StoreDelete, &del_header);
    assert!(del.feed(b"k"));
    assert!(build_payload(&del).is_empty());
  }
}
