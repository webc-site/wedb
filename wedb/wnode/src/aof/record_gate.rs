//! AOF 记录头判读与准入/路由决策门（对标 libs/server/AOF/AofProcessor.cs + AofProcessor.ChunkReplay.cs）
//!
//! 聚焦条目与分块记录的静态/上下文判读决策：
//! - 版本过滤与模糊区缓冲（`should_skip_record` / `should_skip_record_chunk` / `is_old_version_record` / `is_new_version_record`）
//! - 并行重放任务归属与前缀一致上界判定（`can_replay` / `skip_replay`）
//! - 同步操作参数与条目物理键速览（`get_synchronized_operation_params` / `peek_entry_key`）

use waof::{
  self, AofEntryType, AofHeader, AofHeaderType, AofShardedLogTransactionHeader,
  AofSingleLogTransactionHeader, Error,
};
use wbase::store_type::REPLAY_TASK_ACCESS_VECTOR_BYTES;

use super::{
  aof_chunked_record_reader::ChunkedAccumulator,
  garnet_append_only_file::GarnetAppendOnlyFile,
  garnet_log::GarnetLog,
  replaycoordinator::{
    aof_replay_context::ReplayOperation, aof_replay_coordinator::AofReplayCoordinator,
  },
};

/// libs/server/AOF/AofProcessor.cs:IsOldVersionRecord
#[inline]
pub const fn is_old_version_record(header: &AofHeader, store_version: i64) -> bool {
  header.store_version < store_version
}

/// libs/server/AOF/AofProcessor.cs:IsNewVersionRecord
#[inline]
pub const fn is_new_version_record(header: &AofHeader, store_version: i64) -> bool {
  header.store_version > store_version
}

/// libs/server/AOF/AofProcessor.cs:ShouldSkipRecord
///
/// 恢复/复制回放的版本闸：旧检查点代际条目跳过；副本模糊区内的新代条目
/// 入缓冲（C# BufferNewVersionRecord）。
#[inline]
pub fn should_skip_record(
  coordinator: &AofReplayCoordinator,
  sublog_idx: usize,
  entry: &[u8],
  as_replica: bool,
  store_version: i64,
) -> bool {
  let Some(header) = AofHeader::parse(entry) else {
    return true;
  };
  if as_replica && coordinator.context(sublog_idx).in_fuzzy_region() {
    if is_new_version_record(&header, store_version) {
      coordinator.add_fuzzy_region_operation(sublog_idx, ReplayOperation::Record(entry.to_vec()));
      return true;
    }
    return false;
  }
  is_old_version_record(&header, store_version)
}

/// libs/server/AOF/AofProcessor.ChunkReplay.cs:ShouldSkipRecord
/// libs/server/AOF/AofProcessor.ChunkReplay.cs:BufferNewVersionRecord
///
/// 分块形态的 ShouldSkipRecord（C# ChunkReplay 分片同名）；模糊区新代入缓冲
/// 即 C# 局部函数 BufferNewVersionRecord（storeVersion 越当前代即入模糊区
/// 缓冲返回 true）。
#[inline]
pub fn should_skip_record_chunk(
  coordinator: &AofReplayCoordinator,
  sublog_idx: usize,
  acc: &ChunkedAccumulator,
  as_replica: bool,
  store_version: i64,
) -> bool {
  if as_replica && coordinator.context(sublog_idx).in_fuzzy_region() {
    if acc.store_version > store_version {
      coordinator
        .add_fuzzy_region_operation(sublog_idx, ReplayOperation::Chunk(Box::new(acc.clone())));
      return true;
    }
    return false;
  }
  acc.store_version < store_version
}

/// 条目 key 速览（事务组加锁集提取面；不在场返回 None）。
#[inline]
pub fn peek_entry_key(entry: &[u8]) -> Option<&[u8]> {
  let offset = AofHeader::skip_header(entry)?;
  let rest = entry.get(offset..)?;
  let (len_bytes, after_len) = rest.split_first_chunk::<4>()?;
  let len = u32::from_le_bytes(*len_bytes) as usize;
  after_len.get(..len)
}

/// 头内 opType 判读（C# 直接枚举转型；不可解释判别值即损坏条目，显式拒绝）。
#[inline]
fn entry_op_type(header: &AofHeader) -> waof::Result<AofEntryType> {
  AofEntryType::try_from(header.op_type).map_err(|_| Error::UnknownEntryType(header.op_type))
}

/// 回放任务位图准入（libs/common/BitVector.cs:BitVector.IsSet）：
/// 小端位序，第 idx 位落在 `vector[idx / 8]` 的 `1 << (idx % 8)`。
/// 位图定宽即每物理子日志 256 个回放任务上限，越界位恒未置位（该任务不参与）。
#[inline]
const fn bit_is_set(vector: &[u8; REPLAY_TASK_ACCESS_VECTOR_BYTES], idx: usize) -> bool {
  let byte = idx / 8;
  byte < REPLAY_TASK_ACCESS_VECTOR_BYTES && vector[byte] & (1 << (idx % 8)) != 0
}

/// libs/server/AOF/AofProcessor.cs:CanReplay
///
/// 并行回放任务归属判定：返回 (本任务是否处理该条目, 条目序列号)。
/// 头型全集逐支穷尽（[`AofHeaderType::ALL`]，新增头型由编译器强制补支）；
/// 未知头型、头损坏与不可解释的 opType 一律上抛（C# `default` 分支抛
/// GarnetException），由恢复链路中止，绝不静默跳过条目。
#[inline]
pub fn can_replay(
  append_only_file: &GarnetAppendOnlyFile,
  entry: &[u8],
  replay_task_idx: usize,
  entry_address: i64,
) -> waof::Result<(bool, i64)> {
  let header = AofHeader::parse(entry).ok_or(Error::InvalidRecordHeader)?;
  // 先定头型（C# switch 的判别次序）：不可解释的型别直接报错，不走序列号取数
  let header_type = header
    .header_type()
    .ok_or(Error::UnsupportedReplayHeaderType(
      header.flags & AofHeader::AOF_HEADER_TYPE_MASK,
    ))?;
  let log = append_only_file.log();
  // 序列号单点：分片形态取内嵌，其余取条目地址
  let sequence_number =
    AofHeader::sequence_number_of(entry, entry_address).ok_or(Error::InvalidRecordHeader)?;
  match header_type {
    AofHeaderType::BasicHeader | AofHeaderType::BasicChunkHeader => {
      let op_type = entry_op_type(&header)?;
      if !op_type.has_key() {
        return Ok((true, sequence_number));
      }
      let routing = if header.is_chunked() {
        let (_, ch) = AofHeader::get_chunked_header_ref(entry).ok_or(Error::InvalidRecordHeader)?;
        ch.key_hash
      } else {
        let key = peek_entry_key(entry).ok_or(Error::InvalidRecordHeader)?;
        GarnetLog::hash(key)
      };
      Ok((
        replay_task_idx == log.get_replay_task_idx(routing),
        sequence_number,
      ))
    }
    AofHeaderType::ShardedHeader | AofHeaderType::ShardedChunkHeader => {
      let op_type = entry_op_type(&header)?;
      if !op_type.has_key() {
        return Ok((replay_task_idx == 0, sequence_number));
      }
      let key = peek_entry_key(entry).ok_or(Error::InvalidRecordHeader)?;
      Ok((
        replay_task_idx == log.get_replay_task_idx(GarnetLog::hash(key)),
        sequence_number,
      ))
    }
    // 单物理日志 + 多回放：事务头无内嵌序号，按写侧盖入的回放任务位图准入
    AofHeaderType::SingleLogTransactionHeader => {
      let txn = AofSingleLogTransactionHeader::parse(entry).ok_or(Error::InvalidRecordHeader)?;
      Ok((
        bit_is_set(&txn.replay_task_access_vector, replay_task_idx),
        sequence_number,
      ))
    }
    // 多物理日志事务头：序号取内嵌 sharded 段，准入同样按位图
    AofHeaderType::ShardedLogTransactionHeader => {
      let txn = AofShardedLogTransactionHeader::parse(entry).ok_or(Error::InvalidRecordHeader)?;
      Ok((
        bit_is_set(&txn.replay_task_access_vector, replay_task_idx),
        sequence_number,
      ))
    }
  }
}

/// libs/server/AOF/AofProcessor.cs:SkipReplay
///
/// 前缀一致恢复上界判定：条目序列号超过阈值即跳过（单调 ⇒ 后续全跳）。
/// 返回 (是否跳过, 条目序列号)；`until_sequence_number == -1` 全跳。
#[inline]
pub fn skip_replay(
  entry: &[u8],
  until_sequence_number: i64,
  log_address_sequence_number: i64,
) -> Option<(bool, i64)> {
  if until_sequence_number == -1 {
    return Some((true, -1));
  }
  // 序列号单点：分片形态取内嵌（含 ShardedLogTransactionHeader，对齐
  // C# SkipReplay 的 txnHeader.shardedHeader.sequenceNumber 分支），其余取条目地址
  let sequence_number = AofHeader::sequence_number_of(entry, log_address_sequence_number)?;
  Some((sequence_number > until_sequence_number, sequence_number))
}

/// libs/server/AOF/AofProcessor.cs:GetSynchronizedOperationParams
///
/// 提取（序列号, 参与者数）：序列号经 [`AofHeader::sequence_number_of`]
/// 单点（分片形态取内嵌、其余取条目地址）；参与者数事务头形态取头内
/// 值，其余取全量回放任务数（C# BasicHeader 兜底分支）。
#[inline]
pub fn get_synchronized_operation_params(
  replay_task_count: usize,
  entry: &[u8],
  entry_address: i64,
) -> Option<(i64, i16)> {
  let header = AofHeader::parse(entry)?;
  let sequence_number = AofHeader::sequence_number_of(entry, entry_address)?;
  let participant_count = match header.header_type()? {
    AofHeaderType::SingleLogTransactionHeader => {
      AofSingleLogTransactionHeader::parse(entry)?.participant_count
    }
    AofHeaderType::ShardedLogTransactionHeader => {
      AofShardedLogTransactionHeader::parse(entry)?.participant_count
    }
    _ => replay_task_count as i16,
  };
  Some((sequence_number, participant_count))
}
