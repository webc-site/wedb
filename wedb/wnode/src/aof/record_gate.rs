//! AOF 记录头判读与准入/路由决策门（对标 libs/server/AOF/AofProcessor.cs + AofProcessor.ChunkReplay.cs）
//!
//! 聚焦条目与分块记录的静态/上下文判读决策：
//! - 位点/版本双维过滤与模糊区缓冲（`should_skip_record` / `should_skip_record_chunk` / `is_old_version_record` / `is_new_version_record`）
//! - 并行重放任务归属与前缀一致上界判定（`can_replay` / `skip_replay`）
//! - 同步操作参数与条目物理键速览（`get_synchronized_operation_params` / `peek_entry_key`）
//! - 载荷长度前缀段切分单点（`split_len_prefixed`，键段 / 值段共用）

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
/// 恢复/复制回放的双维闸：位点维度（恢复面）+ 版本维度。旧检查点代际条目
/// 跳过；副本模糊区内的新代条目入缓冲（C# BufferNewVersionRecord）。
///
/// 位点闸（恢复面 `!as_replica`）：条目位点严格小于 `aof_floor`（恢复检查点
/// 元数据持久化的 AOF 覆盖边界，见 [`wkv::WedbStore::recovered_aof_floor`]）
/// 即跳过——该条目先于快照发起，效果已物化进快照，重放即重复（对标 C#
/// RecoveredSafeAofAddress 恢复链消费）。正常时序下位点 < covered ⇒ 版本恒旧，
/// 版本闸（末行）已天然覆盖，位点闸为「快照发布后、AOF 截断前异常宕机」时
/// 版本戳异常条目的纵深防御。副本面位点域语义归复制域（游标由复制位点承担），
/// 不启用；分块条目（`should_skip_record_chunk`）重组器不持首帧位点，同样由
/// 版本闸单面承接
#[inline]
pub fn should_skip_record(
  coordinator: &AofReplayCoordinator,
  sublog_idx: usize,
  entry: &[u8],
  as_replica: bool,
  store_version: i64,
  entry_address: i64,
  aof_floor: i64,
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
  if !as_replica && entry_address < aof_floor {
    return true;
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

/// AOF 载荷长度前缀段切分单点：`[4B LE 长度][段][其余]`。
///
/// 载荷形状 `[4B keyLen][key][4B valLen][val][input]` 的键段与值段同形，统一
/// 经此切分（写侧 [`GarnetLog`](super::garnet_log::GarnetLog) 的
/// `enqueue_with_header` 内联拼装为镜像布局，字节布局不变）。越界（剩余字节
/// 不足声明长度）一律返回 `None`，与三处消费方现状口径一致；错误文案由调用
/// 方各自折叠，此处不做统一。
#[inline]
pub fn split_len_prefixed(buf: &[u8]) -> Option<(&[u8], &[u8])> {
  let (len_bytes, after_len) = buf.split_first_chunk::<4>()?;
  let len = u32::from_le_bytes(*len_bytes) as usize;
  after_len.split_at_checked(len)
}

/// 条目 key 速览（事务组加锁集提取面；不在场返回 None）。
#[inline]
pub fn peek_entry_key(entry: &[u8]) -> Option<&[u8]> {
  let offset = AofHeader::skip_header(entry)?;
  Some(split_len_prefixed(entry.get(offset..)?)?.0)
}

/// 带键条目的路由哈希提取单点（对标 C# CanReplay 的
/// BasicHeader/BasicChunkHeader/ShardedHeader/ShardedChunkHeader 四支共用逻辑，
/// 消除分块/非分块判读在 Basic、Sharded 两分支的重复）：
/// - 分块头取内嵌 `chunk.key_hash`。分块条目键分散于各片，首帧布局为
///   `头 + [序号] + 分块头 + 裸键`（裸键无 4B 小端长度前缀），若走
///   [`peek_entry_key`] 会把裸键首 4 字节误读作长度前缀：越界即抛
///   [`Error::InvalidRecordHeader`] 中断恢复，侥幸合法则路由哈希偏离
///   `chunk.key_hash`、把同记录各片派往不同回放任务而无法聚合。
/// - 非分块头取长度前缀裸键本体哈希（与写侧 `enqueue_with_header` 镜像）。
#[inline]
fn entry_routing_hash(entry: &[u8], header: &AofHeader) -> waof::Result<i64> {
  if header.is_chunked() {
    let (_, ch) = AofHeader::get_chunked_header_ref(entry).ok_or(Error::InvalidRecordHeader)?;
    Ok(ch.key_hash)
  } else {
    let key = peek_entry_key(entry).ok_or(Error::InvalidRecordHeader)?;
    Ok(GarnetLog::hash(key))
  }
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
      // 无键条目（事务/检查点/FLUSH/存储过程）全任务处理（可经屏障同步）
      if !op_type.has_key() {
        return Ok((true, sequence_number));
      }
      let routing = entry_routing_hash(entry, &header)?;
      Ok((
        replay_task_idx == log.get_replay_task_idx(routing),
        sequence_number,
      ))
    }
    AofHeaderType::ShardedHeader | AofHeaderType::ShardedChunkHeader => {
      let op_type = entry_op_type(&header)?;
      // 无键条目仅任务 0 处理（C# Sharded 两支）
      if !op_type.has_key() {
        return Ok((replay_task_idx == 0, sequence_number));
      }
      let routing = entry_routing_hash(entry, &header)?;
      Ok((
        replay_task_idx == log.get_replay_task_idx(routing),
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
