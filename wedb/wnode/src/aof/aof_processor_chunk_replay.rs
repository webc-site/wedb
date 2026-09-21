//! 分块记录重放（对标 libs/server/AOF/AofProcessor.ChunkReplay.cs）
//!
//! [ChunkedAccumulator]
//! 完成后的直接重放路径：分块记录恒为数据操作（Store/Object/Unified 的
//! upsert/RMW/delete），永非事务标记 / 检查点 / FLUSH / 存储过程 / 向量操作，
//! 故仅与事务缓冲和操作分派交互（无连续记录镜像物化）。

use waof::AofEntryType;
use wdev::Device;
use wval::KeyTag;

use super::{
  aof_chunked_record_reader::ChunkedAccumulator,
  aof_processor::{AofProcessor, AofReplayError, KeyContextGuard, ReplayTarget},
  aof_processor_object_replay::{object_store_delete, object_store_rmw, object_store_upsert},
  aof_processor_store_ops::{store_delete, store_rmw, store_upsert},
  record_gate,
};

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
) -> Result<bool, AofReplayError> {
  let Some(acc) = processor
    .coordinator()
    .try_buffer_chunk_accumulator(virtual_sublog_idx, acc)
  else {
    return Ok(false);
  };
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
  as_replica: bool,
  log_address_sequence_number: i64,
  target: &ReplayTarget<'_, '_, D>,
) -> Result<(), AofReplayError> {
  // 分块记录拓扑预处理：分片序列号 / 单物理日志地址推进一致性时间戳；
  // 多回放拓扑判定单源取 GarnetAppendOnlyFile::multi_log_enabled（禁就地重写）
  if processor.append_only_file().multi_log_enabled()
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
  if record_gate::should_skip_record_chunk(
    processor.coordinator(),
    virtual_sublog_idx,
    acc,
    as_replica,
    target.store_version,
  ) {
    return Ok(());
  }
  replay_chunk(processor, acc, target).await
}

/// libs/server/AOF/AofProcessor.ChunkReplay.cs:ReplayOp
/// libs/server/AOF/AofProcessor.ChunkReplay.cs:StoreUpsert
/// libs/server/AOF/AofProcessor.ChunkReplay.cs:StoreRMW
/// libs/server/AOF/AofProcessor.ChunkReplay.cs:StoreDelete
/// libs/server/AOF/AofProcessor.ChunkReplay.cs:ObjectStoreUpsert
/// libs/server/AOF/AofProcessor.ChunkReplay.cs:ObjectStoreRMW
/// libs/server/AOF/AofProcessor.ChunkReplay.cs:ObjectStoreDelete
/// libs/server/AOF/AofProcessor.ChunkReplay.cs:UnifiedStoreStringUpsert
/// libs/server/AOF/AofProcessor.ChunkReplay.cs:UnifiedStoreRMW
/// libs/server/AOF/AofProcessor.ChunkReplay.cs:UnifiedStoreObjectUpsert
/// libs/server/AOF/AofProcessor.ChunkReplay.cs:UnifiedStoreDelete
///
/// 依累积器操作类型应用数据操作：C# 十枚 `static void Xxx(ChunkedAccumulator …)`
/// 分派臂（ReplayOp switch）各自约 20 行的 span 切取 + 上下文调用，rust 折叠为
/// match 臂直调共享 helper（aof_processor_store_ops / aof_processor_object_replay），
/// 组件切片由累积器视图（key/value/input span）零拷贝直供，无临时 payload 物化。
pub async fn replay_chunk<D: Device>(
  processor: &AofProcessor,
  acc: &ChunkedAccumulator,
  target: &ReplayTarget<'_, '_, D>,
) -> Result<(), AofReplayError> {
  let guard = KeyContextGuard::enter(target.session, acc.key_span())?;
  let key = guard.user_key;
  let tag = guard.tag;

  match acc.op_type {
    AofEntryType::StoreUpsert => store_upsert(target.session, tag, key, acc.value_span()).await,
    AofEntryType::StoreRMW => store_rmw(processor, target.session, key, acc.input_span()).await,
    AofEntryType::StoreDelete => store_delete(target.session, tag, key).await,
    AofEntryType::ObjectStoreUpsert => {
      let value = acc.object_value_bytes();
      object_store_upsert(target.session, key, &value).await
    }
    AofEntryType::ObjectStoreRMW => {
      object_store_rmw(target.session, tag, key, acc.input_span()).await
    }
    AofEntryType::ObjectStoreDelete => object_store_delete(target.session, key).await,
    AofEntryType::UnifiedStoreStringUpsert => {
      store_upsert(target.session, KeyTag::String, key, acc.value_span()).await
    }
    AofEntryType::UnifiedStoreRMW => {
      store_rmw(processor, target.session, key, acc.input_span()).await
    }
    AofEntryType::UnifiedStoreObjectUpsert => {
      let value = acc.object_value_bytes();
      object_store_upsert(target.session, key, &value).await
    }
    AofEntryType::UnifiedStoreDelete => store_delete(target.session, tag, key).await,
    _ => Err(format!("Unexpected chunked op type: {:?}", acc.op_type).into()),
  }
}

#[cfg(test)]
mod tests {
  use waof::AofChunkHeader;

  use super::*;

  #[test]
  fn accumulator_views_per_op_type() {
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
    assert_eq!(acc.key_span(), b"k");
    assert_eq!(acc.value_span(), b"vv");
    assert!(acc.input_span().is_empty());

    let del_header = AofChunkHeader {
      overflow_key_length: 1,
      overflow_value_length: 0,
      input_length: 0,
      object_id: 2,
      key_hash: 1,
    };
    let mut del = ChunkedAccumulator::new(AofEntryType::StoreDelete, &del_header);
    assert!(del.feed(b"k"));
    assert_eq!(del.key_span(), b"k");
    assert!(del.value_span().is_empty());
  }
}
