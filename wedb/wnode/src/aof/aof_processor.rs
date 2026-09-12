//! AOF 重放处理器（对标 libs/server/AOF/AofProcessor.cs:AofProcessor）
//!
//! 恢复回放与复制回放共用的条目处理内核：按条目头分发（检查点标记 /
//! FLUSH / 存储过程 / 事务 / 数据操作），数据操作经 [`ReplayTarget`] 落入
//! wkv 存储会话。rust 侧重放应用为异步（wkv 面天然异步），拓扑预处理
//! （key 哈希 / 一致性时间戳推进）保持同步快路径。
//!
//! C# 的拓扑特化预处理结构（SingleLogPreprocessKey 等）折叠为
//! `prepare_key`：按拓扑更新一致性时间戳并产出 key/payload 视图。

use std::{
  borrow::Cow,
  future::Future,
  path::Path,
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
  },
};

use waof::{AofAddress, AofEntryType};
use wbase::convert::{
  TICKS_PER_MILLISECOND, TICKS_PER_SECOND, UNIX_EPOCH_TICKS,
  unix_timestamp_in_milliseconds_to_ticks, unix_timestamp_in_seconds_to_ticks,
};
use wdev::Device;
use wkv::WedbStore;

use super::{
  aof_header::{AofHeader, AofHeaderType, AofShardedHeader},
  garnet_append_only_file::GarnetAppendOnlyFile,
  garnet_log::GarnetLog,
  readconsistency::read_consistency_manager::ReadConsistencyManager,
  replaycoordinator::{
    aof_replay_context::{ReplayOperation, TransactionGroup},
    aof_replay_coordinator::AofReplayCoordinator,
  },
};
use crate::{
  objects::{
    hash::hash_object::HashOperation,
    list::list_object::ListOperation,
    object_store_utils::{
      hash_from_blob, hash_to_blob, list_from_blob, list_to_blob, make_object_input, set_from_blob,
      set_to_blob, zset_from_blob, zset_to_blob,
    },
    set::set_object::SetOperation,
    sortedset::sorted_set_object::SortedSetOperation,
    types::object_output::ObjectOutput,
  },
  storage::session::{
    mainstore::advanced_ops::StringRMWOp, objectstore::common::obj_decode,
    storage_session::StorageSession,
  },
  types::{GarnetObjectType, RespCommand},
};

/// 范围索引存储会话抽象接口（解耦具体设备类型，消除 unsafe 裸指针转换）
pub trait RangeIndexSessionFace: Send + Sync {
  /// 创建范围索引
  fn ri_create(
    &self,
    key: &[u8],
    backend: wkv::StorageBackend,
    tuning: wkv::TreeTuning,
  ) -> impl Future<Output = Result<(), wkv::RangeIndexError>>;

  /// 插入 / 更新键值
  fn ri_set(
    &self,
    key: &[u8],
    field: &[u8],
    value: &[u8],
  ) -> impl Future<Output = Result<(), wkv::RangeIndexError>>;

  /// 删除键值
  fn ri_del(
    &self,
    key: &[u8],
    field: &[u8],
  ) -> impl Future<Output = Result<bool, wkv::RangeIndexError>>;

  /// 发布迁移索引文件
  fn ri_publish(
    &self,
    key: &[u8],
    stub_bytes: &[u8],
    temp_path: &Path,
    replace: bool,
  ) -> impl Future<Output = Result<(), wkv::RangeIndexError>>;
}

impl<D: Device> RangeIndexSessionFace for wkv::BatchStoreSession<'_, D> {
  async fn ri_create(
    &self,
    key: &[u8],
    backend: wkv::StorageBackend,
    tuning: wkv::TreeTuning,
  ) -> Result<(), wkv::RangeIndexError> {
    self.range_index_create(key, backend, tuning).await
  }

  async fn ri_set(
    &self,
    key: &[u8],
    field: &[u8],
    value: &[u8],
  ) -> Result<(), wkv::RangeIndexError> {
    self.range_index_set(key, field, value).await
  }

  async fn ri_del(&self, key: &[u8], field: &[u8]) -> Result<bool, wkv::RangeIndexError> {
    self.range_index_del(key, field).await
  }

  async fn ri_publish(
    &self,
    key: &[u8],
    stub_bytes: &[u8],
    temp_path: &Path,
    replace: bool,
  ) -> Result<(), wkv::RangeIndexError> {
    self
      .publish_migrated_range_index(key, stub_bytes, temp_path, replace)
      .await
  }
}

impl<D: Device> RangeIndexSessionFace for wkv::StoreSession<D> {
  async fn ri_create(
    &self,
    key: &[u8],
    backend: wkv::StorageBackend,
    tuning: wkv::TreeTuning,
  ) -> Result<(), wkv::RangeIndexError> {
    self.range_index_create(key, backend, tuning).await
  }

  async fn ri_set(
    &self,
    key: &[u8],
    field: &[u8],
    value: &[u8],
  ) -> Result<(), wkv::RangeIndexError> {
    self.range_index_set(key, field, value).await
  }

  async fn ri_del(&self, key: &[u8], field: &[u8]) -> Result<bool, wkv::RangeIndexError> {
    self.range_index_del(key, field).await
  }

  async fn ri_publish(
    &self,
    key: &[u8],
    stub_bytes: &[u8],
    temp_path: &Path,
    replace: bool,
  ) -> Result<(), wkv::RangeIndexError> {
    self
      .publish_migrated_range_index(key, stub_bytes, temp_path, replace)
      .await
  }
}

use crate::resp::rangeindex::range_index_manager_replication::RangeIndexManagerReplication;

/// 范围索引 AOF 回放处理器具象类型别名（消除虚表开销）
pub type RangeIndexReplayerFace = RangeIndexManagerReplication;

/// AOF 重放域错误（C# GarnetException 回放路径的 rust 形态）。
#[derive(Debug, thiserror::Error)]
pub enum AofReplayError {
  /// 重放语义错误（损坏条目 / 未接线子域 / 存储失败）。
  #[error("AOF replay: {0}")]
  Replay(String),
  /// 存储面错误（wkv 透传）。
  #[error(transparent)]
  Store(#[from] wkv::Error),
}

impl From<String> for AofReplayError {
  fn from(message: String) -> Self {
    Self::Replay(message)
  }
}

impl From<&str> for AofReplayError {
  fn from(message: &str) -> Self {
    Self::Replay(message.to_string())
  }
}

/// 单条回放负载的头部（C# RespInputHeader + StringInput 参数区的组合形态）：
/// `[cmd u16][flags u8][sub_id u8][obj_type u8][pad 3B][arg1 i64][arg2 i64][arg3 i64]
///  [args_count u32][args 原文字节...]`。
///
/// `obj_type` 对标 C# RespInputHeader 的判别联合：C# 对象输入以
/// `{type: GarnetObjectType, subId}`（header byte0/byte1）替代字符串输入的
/// `cmd`；rust 侧布局显式分列（cmd 与 obj_type 独立字段），对象 RMW 条目
/// 恒写 obj_type，字符串条目恒 0（Null）。
pub const REPLAY_INPUT_HEADER_SIZE: usize = 32;

/// 重放输入（StringInput 的反序列化形态）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayInput {
  /// 命令（判别值与 types::RespCommand 一致）。
  pub cmd: RespCommand,
  /// 标志位。
  pub flags: u8,
  /// 对象子操作 id。
  pub sub_id: u8,
  /// 对象类型（GarnetObjectType 判别值；仅对象条目非 0）。
  pub obj_type: u8,
  /// arg1（INCR 族增量 / SETRANGE 偏移等）。
  pub arg1: i64,
  /// arg2。
  pub arg2: i64,
  /// arg3。
  pub arg3: i64,
  /// parseState 参数序列化（APPEND/SETRANGE 数据等）。
  pub args: Vec<Vec<u8>>,
}

/// 零分配条目入队元数据借用体（对标 C# ReplayInput）
#[derive(Debug, Clone, Copy)]
pub struct ReplayInputSlice<'a, T: AsRef<[u8]> = &'a [u8]> {
  /// 命令
  pub cmd: RespCommand,
  /// 标志位
  pub flags: u8,
  /// 对象子操作 id
  pub sub_id: u8,
  /// 对象类型（GarnetObjectType 判别值）
  pub obj_type: u8,
  /// arg1
  pub arg1: i64,
  /// arg2
  pub arg2: i64,
  /// arg3
  pub arg3: i64,
  /// 切片参数集合
  pub args: &'a [T],
}

impl<'a, T: AsRef<[u8]>> ReplayInputSlice<'a, T> {
  #[inline]
  pub const fn new(cmd: RespCommand, args: &'a [T]) -> Self {
    Self {
      cmd,
      flags: 0,
      sub_id: 0,
      obj_type: 0,
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args,
    }
  }

  #[inline]
  pub const fn with_flags(mut self, flags: u8) -> Self {
    self.flags = flags;
    self
  }

  #[inline]
  pub const fn with_sub_id(mut self, sub_id: u8) -> Self {
    self.sub_id = sub_id;
    self
  }

  #[inline]
  pub const fn with_obj_type(mut self, obj_type: u8) -> Self {
    self.obj_type = obj_type;
    self
  }

  #[inline]
  pub const fn with_args_num(mut self, arg1: i64, arg2: i64, arg3: i64) -> Self {
    self.arg1 = arg1;
    self.arg2 = arg2;
    self.arg3 = arg3;
    self
  }
}

impl ReplayInput {
  /// 计算切片序列化字节数
  #[inline]
  pub fn encoded_len_for_slices(args: &[impl AsRef<[u8]>]) -> usize {
    36 + args.iter().map(|a| 4 + a.as_ref().len()).sum::<usize>()
  }

  /// 序列化到缓冲切片，返回已写入切片（若缓冲不足则返回 None）
  pub fn encode_to_slice<'a, T: AsRef<[u8]>>(
    input: &ReplayInputSlice<'_, T>,
    buf: &'a mut [u8],
  ) -> Option<&'a [u8]> {
    let total_len = Self::encoded_len_for_slices(input.args);
    if buf.len() < total_len {
      return None;
    }
    let raw: u16 = input.cmd.into();
    buf[0..2].copy_from_slice(&raw.to_le_bytes());
    buf[2] = input.flags;
    buf[3] = input.sub_id;
    buf[4] = input.obj_type;
    buf[5..8].fill(0);
    buf[8..16].copy_from_slice(&input.arg1.to_le_bytes());
    buf[16..24].copy_from_slice(&input.arg2.to_le_bytes());
    buf[24..32].copy_from_slice(&input.arg3.to_le_bytes());
    buf[32..36].copy_from_slice(&(input.args.len() as u32).to_le_bytes());
    let mut cursor = 36;
    for arg in input.args {
      let slice = arg.as_ref();
      buf[cursor..cursor + 4].copy_from_slice(&(slice.len() as u32).to_le_bytes());
      cursor += 4;
      buf[cursor..cursor + slice.len()].copy_from_slice(slice);
      cursor += slice.len();
    }
    Some(&buf[..total_len])
  }

  /// 统一零分配/低分配写入助手：优先使用 512B 栈缓冲，超大载荷自动回落堆缓冲
  pub fn with_encoded_slices<R, T: AsRef<[u8]>>(
    input: &ReplayInputSlice<'_, T>,
    f: impl FnOnce(&[u8]) -> R,
  ) -> R {
    let total_len = Self::encoded_len_for_slices(input.args);
    if total_len <= 512 {
      let mut stack_buf = [0u8; 512];
      let slice = Self::encode_to_slice(input, &mut stack_buf).expect("stack buffer sufficient");
      f(slice)
    } else {
      let mut heap_buf = vec![0u8; total_len];
      let slice = Self::encode_to_slice(input, &mut heap_buf).expect("heap buffer sufficient");
      f(slice)
    }
  }

  /// 序列化（AOF 写入侧共用编码）。
  pub fn serialize(&self, into: &mut Vec<u8>) {
    let needed = Self::encoded_len_for_slices(&self.args);
    into.reserve(needed);
    let start = into.len();
    into.resize(start + needed, 0);
    let slice_input = ReplayInputSlice {
      cmd: self.cmd,
      flags: self.flags,
      sub_id: self.sub_id,
      obj_type: self.obj_type,
      arg1: self.arg1,
      arg2: self.arg2,
      arg3: self.arg3,
      args: &self.args,
    };
    Self::encode_to_slice(&slice_input, &mut into[start..]);
  }

  /// 反序列化（C# StringInput.DeserializeFrom 的组合形态）。
  pub fn deserialize(bytes: &[u8]) -> Option<Self> {
    if bytes.len() < REPLAY_INPUT_HEADER_SIZE {
      return None;
    }
    let cmd = RespCommand::try_from(u16::from_le_bytes([bytes[0], bytes[1]])).ok()?;
    // 参数区：[count u32][逐参 (len u32 + bytes)]，起点 = 固定头 32
    let mut cursor = REPLAY_INPUT_HEADER_SIZE;
    if cursor + 4 > bytes.len() {
      return None;
    }
    let args_count = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().ok()?) as usize;
    cursor += 4;
    let max_possible_args = (bytes.len() - cursor) / 4;
    if args_count > max_possible_args {
      return None;
    }
    let mut args = Vec::with_capacity(args_count);
    for _ in 0..args_count {
      if cursor + 4 > bytes.len() {
        return None;
      }
      let len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().ok()?) as usize;
      cursor += 4;
      if cursor + len > bytes.len() {
        return None;
      }
      args.push(bytes[cursor..cursor + len].to_vec());
      cursor += len;
    }
    Some(Self {
      cmd,
      flags: bytes[2],
      sub_id: bytes[3],
      obj_type: bytes[4],
      arg1: i64::from_le_bytes(bytes[8..16].try_into().ok()?),
      arg2: i64::from_le_bytes(bytes[16..24].try_into().ok()?),
      arg3: i64::from_le_bytes(bytes[24..32].try_into().ok()?),
      args,
    })
  }
}

/// 预处理产出：key、key 哈希与负载起点（C# PreparedParameters）。
pub struct PreparedParameters<'a> {
  /// key 字节。
  pub key: Cow<'a, [u8]>,
  /// key 哈希（GarnetLog::HASH）。
  pub key_hash: i64,
  /// 负载（key 之后的首个组件起点）。
  pub payload: Cow<'a, [u8]>,
}

/// 重放落点：存储会话 + 存储句柄（FLUSH 面）+ 当前存储版本。
pub struct ReplayTarget<'a, 'b, D: Device> {
  /// 存储会话（读写应用面）。
  pub session: &'b StorageSession<'a, D>,
  /// 存储句柄（FLUSH ALL/DB 应用面）。
  pub store: Arc<WedbStore<D>>,
  /// 当前存储版本（SkipRecord 版本判定；C# storeWrapper.store.CurrentVersion）。
  pub store_version: i64,
}

/// AOF 重放处理器。
pub struct AofProcessor {
  /// 追加日志文件（一致性管理器 / 序列号 / 拓扑入口）。
  append_only_file: Arc<GarnetAppendOnlyFile>,
  /// 回放协调器（事务 / 模糊区 / 栅栏）。
  coordinator: AofReplayCoordinator,
  /// 活跃库 id。
  active_db_id: AtomicI64,
  /// 多物理子日志拓扑。
  using_sharded_log: bool,
  /// 单物理日志 + 多回放任务拓扑。
  using_single_physical_log_multi_replay: bool,
  /// 范围索引重放面（C# activeRangeIndexManager；RangeIndexPreview 关闭 /
  /// 未注入为 None，RI 族条目重放按 C# 同文案失败）。
  range_index: Option<Arc<RangeIndexManagerReplication>>,
}

impl AofProcessor {
  /// libs/server/AOF/AofProcessor.cs:AofProcessor（构造）
  ///
  /// 依拓扑装配预处理路径并初始化回放上下文。
  pub fn new(append_only_file: Arc<GarnetAppendOnlyFile>) -> Self {
    let options_physical = append_only_file.log().size();
    let virtual_count = append_only_file.virtual_sublog_count();
    let coordinator =
      AofReplayCoordinator::new(virtual_count, append_only_file.multi_log_enabled());
    if let Some(manager) = append_only_file.read_consistency_manager() {
      coordinator.set_consistency_manager(manager);
    }
    Self {
      append_only_file,
      coordinator,
      active_db_id: AtomicI64::new(0),
      using_sharded_log: options_physical > 1,
      using_single_physical_log_multi_replay: options_physical == 1 && virtual_count > 1,
      range_index: None,
    }
  }

  /// 注入范围索引重放面（C# 由 storeWrapper.activeRangeIndexManager 装配）。
  pub fn set_range_index_manager(&mut self, manager: Arc<RangeIndexManagerReplication>) {
    self.range_index = Some(manager);
  }

  /// 范围索引重放面句柄。
  pub fn range_index_manager(&self) -> Option<&Arc<RangeIndexManagerReplication>> {
    self.range_index.as_ref()
  }

  /// 回放协调器句柄。
  pub fn coordinator(&self) -> &AofReplayCoordinator {
    &self.coordinator
  }

  /// 追加日志文件句柄。
  pub fn append_only_file(&self) -> &Arc<GarnetAppendOnlyFile> {
    &self.append_only_file
  }

  /// 读取一致性管理器快捷入口。
  pub fn read_consistency_manager(&self) -> Option<Arc<ReadConsistencyManager>> {
    self.append_only_file.read_consistency_manager()
  }

  /// libs/server/AOF/AofProcessor.cs:SwitchActiveDatabaseContext
  pub fn switch_active_database_context(&self, db_id: i64) {
    self.active_db_id.store(db_id, Ordering::Release);
  }

  /// 活跃库 id。
  pub fn active_db_id(&self) -> i64 {
    self.active_db_id.load(Ordering::Acquire)
  }

  /// 拓扑预处理（C# IPreprocessKey.PrepareKey 三实现的折叠）：
  /// 解出 key / 哈希 / 负载并按拓扑推进一致性 key 时间戳（零堆分配与零 Arc 克隆）。
  pub fn prepare_key<'a>(
    &self,
    virtual_sublog_idx: usize,
    entry: &'a [u8],
    log_address_sequence_number: i64,
  ) -> Option<PreparedParameters<'a>> {
    let header = AofHeader::parse(entry)?;
    let (header_size, sequence_number) =
      if header.header_type() == Some(AofHeaderType::ShardedHeader) {
        let sh = AofShardedHeader::parse(entry)?;
        (AofShardedHeader::TOTAL_SIZE, sh.sequence_number)
      } else {
        (AofHeader::TOTAL_SIZE, log_address_sequence_number)
      };
    let payload = &entry[header_size..];
    let key_len = u32::from_le_bytes(*payload.first_chunk::<4>()?) as usize;
    let key = payload.get(4..4 + key_len)?;
    let key_hash = GarnetLog::hash(key);
    let rest = payload.get(4 + key_len..)?;

    // 单物理日志多回放 / 分片拓扑：按序列号推进一致性时间戳（零 Arc 克隆）
    if self.using_sharded_log || self.using_single_physical_log_multi_replay {
      self
        .append_only_file
        .with_read_consistency_manager(|manager| {
          manager.update_virtual_sublog_key_sequence_number(
            virtual_sublog_idx,
            key_hash,
            sequence_number,
          );
        });
    }
    Some(PreparedParameters {
      key: Cow::Borrowed(key),
      key_hash,
      payload: Cow::Borrowed(rest),
    })
  }

  /// 从记录负载解析 value（长度前缀形态）与剩余 input。
  fn split_value_input(payload: &[u8]) -> Option<(&[u8], &[u8])> {
    let len = u32::from_le_bytes(*payload.first_chunk::<4>()?) as usize;
    let value = payload.get(4..4 + len)?;
    Some((value, &payload[4 + len..]))
  }

  /// libs/server/AOF/AofProcessor.cs:GetSynchronizedOperationParams
  ///
  /// 提取（序列号, 参与者数）：单物理日志用条目地址，分片用内嵌序列号。
  pub fn get_synchronized_operation_params(
    &self,
    entry: &[u8],
    entry_address: i64,
  ) -> Option<(i64, i16)> {
    let header = AofHeader::parse(entry)?;
    match header.header_type()? {
      AofHeaderType::BasicHeader | AofHeaderType::BasicChunkHeader => {
        Some((entry_address, self.replay_task_count() as i16))
      }
      AofHeaderType::ShardedHeader | AofHeaderType::ShardedChunkHeader => {
        let sh = AofShardedHeader::parse(entry)?;
        Some((sh.sequence_number, self.replay_task_count() as i16))
      }
      _ => None,
    }
  }

  fn replay_task_count(&self) -> usize {
    self.append_only_file.virtual_sublog_count() / self.append_only_file.log().size().max(1)
  }

  /// libs/server/AOF/AofProcessor.cs:ProcessAofRecordInternal
  ///
  /// 共享回放入口（恢复回放与复制回放同一路径）。返回是否遇到检查点
  /// 起始标记（复制侧据此记录 ReplicationCheckpointStartOffset）。
  pub async fn process_aof_record_internal<D: Device>(
    &self,
    virtual_sublog_idx: usize,
    entry: &[u8],
    as_replica: bool,
    log_address_sequence_number: i64,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<bool, AofReplayError> {
    // 顺序布局：存在进行中分块记录时，本条目必为纯数据续块
    if self
      .coordinator
      .context(virtual_sublog_idx)
      .has_in_progress_chunk()
    {
      let completed = self
        .coordinator
        .context(virtual_sublog_idx)
        .chunked_reader
        .read_chunk(entry);
      if let Some(acc) = completed {
        return super::aof_processor_chunk_replay::process_chunked_record(
          self,
          virtual_sublog_idx,
          acc,
          as_replica,
          log_address_sequence_number,
          target,
        )
        .await;
      }
      return Ok(false);
    }

    let header = AofHeader::parse(entry).ok_or("AOF 条目头损坏")?;
    if header.aof_header_version > AofHeader::MAX_SUPPORTED_AOF_HEADER_VERSION {
      return Err(
        format!(
          "Unsupported AOF header version {}; this build supports up to version {}",
          header.aof_header_version,
          AofHeader::MAX_SUPPORTED_AOF_HEADER_VERSION
        )
        .into(),
      );
    }

    // 分块记录：累积至完成即直接分派（不物化连续镜像）
    if header.is_chunked() {
      let completed = self
        .coordinator
        .context(virtual_sublog_idx)
        .chunked_reader
        .read_chunk(entry);
      if let Some(acc) = completed {
        return super::aof_processor_chunk_replay::process_chunked_record(
          self,
          virtual_sublog_idx,
          acc,
          as_replica,
          log_address_sequence_number,
          target,
        )
        .await;
      }
      return Ok(false);
    }

    // C# 直接以枚举转型分发；未知判别值为损坏条目，显式拒绝
    //（replay_op_dispatch 的 default 分支同文案报错）
    let op_type = AofEntryType::try_from(header.op_type)
      .map_err(|_| format!("Unknown AOF header operation type {}", header.op_type))?;

    // 事务处理：TxnStart/TxnAbort/TxnCommit 及组内操作由协调器消化
    let action = self.coordinator.add_or_replay_transaction_operation(
      virtual_sublog_idx,
      entry,
      log_address_sequence_number,
    );
    match action {
      super::replaycoordinator::aof_replay_coordinator::TxnAction::Handled => return Ok(false),
      super::replaycoordinator::aof_replay_coordinator::TxnAction::Commit { group } => {
        // 取出的整组按序重放（恢复路径免锁；副本路径的事务锁由事务域承载）
        self
          .process_transaction_group_operations(virtual_sublog_idx, &group, as_replica, target)
          .await;
        return Ok(false);
      }
      super::replaycoordinator::aof_replay_coordinator::TxnAction::None => {}
    }

    let mut is_checkpoint_start = false;
    match op_type {
      AofEntryType::CheckpointStartCommit => {
        is_checkpoint_start = true;
        if header.aof_header_version > 1 {
          if self
            .coordinator
            .context(virtual_sublog_idx)
            .in_fuzzy_region()
          {
            self
              .coordinator
              .clear_fuzzy_region_buffer(virtual_sublog_idx);
          }
          self
            .coordinator
            .context(virtual_sublog_idx)
            .set_in_fuzzy_region(true);
        }
        if self.using_sharded_log || self.using_single_physical_log_multi_replay {
          let sequence_number = self
            .get_synchronized_operation_params(entry, log_address_sequence_number)
            .map_or(0, |(seq, _)| seq);
          self
            .append_only_file
            .with_read_consistency_manager(|manager| {
              manager
                .update_virtual_sublog_max_sequence_number(virtual_sublog_idx, sequence_number);
            });
        }
      }
      AofEntryType::CheckpointEndCommit => {
        if header.aof_header_version > 1 {
          if !self
            .coordinator
            .context(virtual_sublog_idx)
            .in_fuzzy_region()
          {
            // 无起始标记的结束标记：忽略（C# LogInformation 分支）
          } else {
            self
              .coordinator
              .context(virtual_sublog_idx)
              .set_in_fuzzy_region(false);
            // 模糊区结束后统一重放缓冲的 (v+1) 条目
            self
              .process_fuzzy_region_operations(virtual_sublog_idx, target)
              .await?;
            self
              .coordinator
              .clear_fuzzy_region_buffer(virtual_sublog_idx);
          }
        }
      }
      AofEntryType::FlushAll => {
        // C# FlushAllDatabases(unsafeTruncateLog)；rust 存储面按库清空
        if !header.unsafe_truncate_log() {
          log::warn!("AOF 日志安全截断跳过或未执行");
        }
        target
          .store
          .flush_all()
          .await
          .map_err(|e| format!("FlushAll replay failed: {e}"))?;
      }
      AofEntryType::FlushDb => {
        // C# FlushDatabase(dbId)；wkv 无按库命名空间，降级全清（缺口见汇报）
        target
          .store
          .flush_all()
          .await
          .map_err(|e| format!("FlushDb replay failed: {e}"))?;
      }
      AofEntryType::StoredProcedure => {
        // 存储过程重放：过程注册表由 custom 域承载（缺口见汇报），跳过执行
      }
      AofEntryType::TxnCommit => {
        // 模糊区事务组重放（FIFO）
        self
          .process_fuzzy_region_transaction_group(virtual_sublog_idx, target)
          .await?;
      }
      _ => {
        self
          .replay_op_dispatch(
            virtual_sublog_idx,
            header,
            entry,
            as_replica,
            log_address_sequence_number,
            target,
          )
          .await?;
      }
    }
    Ok(is_checkpoint_start)
  }

  /// 模糊区操作统一重放（C# ProcessFuzzyRegionOperations 的处理器侧）。
  pub async fn process_fuzzy_region_operations<D: Device>(
    &self,
    sublog_idx: usize,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let operations = self.coordinator.take_fuzzy_region_operations(sublog_idx);
    for op in operations {
      match op {
        ReplayOperation::Record(entry) => {
          let header = AofHeader::parse(&entry).ok_or("模糊区条目头损坏")?;
          self
            .replay_op_dispatch(sublog_idx, header, &entry, true, 0, target)
            .await?;
        }
        ReplayOperation::Chunk(acc) => {
          super::aof_processor_chunk_replay::replay_chunk(self, *acc, target).await?;
        }
      }
    }
    Ok(())
  }

  /// 模糊区事务组重放（C# ProcessFuzzyRegionTransactionGroup）。
  pub async fn process_fuzzy_region_transaction_group<D: Device>(
    &self,
    sublog_idx: usize,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let Some(group) = self.coordinator.dequeue_txn_group(sublog_idx) else {
      return Ok(());
    };
    self
      .process_transaction_group_operations(sublog_idx, &group, true, target)
      .await;
    Ok(())
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:ProcessTransactionGroupOperations
  ///
  /// 顺序重放事务组全部操作（组提交原子性由恢复免锁 / 副本锁集保障）。
  pub async fn process_transaction_group_operations<D: Device>(
    &self,
    sublog_idx: usize,
    group: &TransactionGroup,
    as_replica: bool,
    target: &ReplayTarget<'_, '_, D>,
  ) {
    for op in &group.operations {
      let result = match op {
        ReplayOperation::Record(entry) => match AofHeader::parse(entry) {
          Some(header) => {
            self
              .replay_op_dispatch(
                sublog_idx,
                header,
                entry,
                as_replica,
                group.start_sequence_number,
                target,
              )
              .await
          }
          None => Err("模糊区条目头损坏".to_string().into()),
        },
        ReplayOperation::Chunk(acc) => {
          super::aof_processor_chunk_replay::replay_chunk(self, (**acc).clone(), target).await
        }
      };
      if let Err(e) = result {
        log::warn!("AOF 重放事务组操作失败: {e:?}");
      }
    }
  }

  /// libs/server/AOF/AofProcessor.cs:ReplayOpDispatch
  ///
  /// 按拓扑选择预处理路径并分派到 [`Self::replay_op`]。
  pub async fn replay_op_dispatch<D: Device>(
    &self,
    virtual_sublog_idx: usize,
    header: AofHeader,
    entry: &[u8],
    as_replica: bool,
    log_address_sequence_number: i64,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let op_type = AofEntryType::try_from(header.op_type).map_err(|_| "未知 AOF 操作类型")?;
    let skip = self.should_skip_record(virtual_sublog_idx, entry, as_replica, target.store_version);
    if !self.begin_replay_op(skip) {
      return Ok(());
    }
    let prepared = self
      .prepare_key(virtual_sublog_idx, entry, log_address_sequence_number)
      .ok_or("AOF 条目负载损坏")?;
    let legacy_cmd_format = header.aof_header_version < 4;
    self
      .replay_op(op_type, prepared, legacy_cmd_format, target)
      .await
  }

  /// libs/server/AOF/AofProcessor.cs:BeginReplayOp
  ///
  /// 跳过判定通过后放行（C# 还产出对象输出缓冲；wkv 输出面在存储会话内闭环）。
  pub fn begin_replay_op(&self, skip: bool) -> bool {
    !skip
  }

  /// libs/server/AOF/AofProcessor.cs:ReplayOp
  ///
  /// 数据操作应用：按条目类型分发到主存 / 对象存 / 统一存应用面。
  /// 条目 key 为物理键，经 `KeyContextGuard` 切至条目 `(ns, db)` 域后
  /// 以用户键应用（drop 时恢复重放会话原 context）。
  pub async fn replay_op<'a, D: Device>(
    &self,
    op_type: AofEntryType,
    prepared: PreparedParameters<'a>,
    legacy_cmd_format: bool,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let PreparedParameters {
      key,
      key_hash: _,
      payload,
    } = prepared;
    let guard = KeyContextGuard::enter(target.session, &key)?;
    let key: &[u8] = guard.user_key;
    match op_type {
      AofEntryType::StoreUpsert => {
        let (value, _) = Self::split_value_input(&payload).ok_or("StoreUpsert 负载损坏")?;
        Self::store_upsert(target.session, key, value).await
      }
      AofEntryType::StoreRMW => {
        self
          .store_rmw(target.session, key, &payload, legacy_cmd_format)
          .await
      }
      AofEntryType::StoreDelete => Self::store_delete(target.session, key).await,
      AofEntryType::ObjectStoreUpsert => {
        let (value, _) = Self::split_value_input(&payload).ok_or("ObjectStoreUpsert 负载损坏")?;
        Self::object_store_upsert(target.session, key, value).await
      }
      AofEntryType::ObjectStoreRMW => {
        Self::object_store_rmw(target.session, key, &payload, legacy_cmd_format).await
      }
      AofEntryType::ObjectStoreDelete => Self::object_store_delete(target.session, key).await,
      AofEntryType::UnifiedStoreStringUpsert => {
        let (value, _) =
          Self::split_value_input(&payload).ok_or("UnifiedStoreStringUpsert 负载损坏")?;
        Self::unified_store_string_upsert(target.session, key, value).await
      }
      AofEntryType::UnifiedStoreObjectUpsert => {
        let (value, _) =
          Self::split_value_input(&payload).ok_or("UnifiedStoreObjectUpsert 负载损坏")?;
        Self::object_store_upsert(target.session, key, value).await
      }
      // C# UnifiedStoreRMW / UnifiedStoreDelete：wkv 统一面下与主存 RMW /
      // delete 同形
      AofEntryType::UnifiedStoreRMW => {
        self
          .store_rmw(target.session, key, &payload, legacy_cmd_format)
          .await
      }
      AofEntryType::UnifiedStoreDelete => Self::store_delete(target.session, key).await,
      // libs/server/AOF/AofProcessor.cs:HandleRangeIndexStreamChunk
      //（迁移索引流块重放；未注入 RI 面即按 C# 同文案失败）
      AofEntryType::RangeIndexStreamChunk => {
        let Some(ri) = &self.range_index else {
          return Err(
            "RangeIndexPreview disabled; Replay failed"
              .to_string()
              .into(),
          );
        };
        let input = ReplayInput::deserialize(&payload).ok_or("StreamChunk input 损坏")?;
        ri.handle_range_index_stream_replay(&target.session.batch, key, &input)
          .await
          .map_err(|e| format!("RangeIndexStreamChunk replay failed: {e}").into())
      }
      _ => Err(format!("Unknown AOF header operation type {op_type:?}").into()),
    }
  }

  /// libs/server/AOF/AofProcessor.cs:StoreUpsert
  pub async fn store_upsert<D: Device>(
    session: &StorageSession<'_, D>,
    key: &[u8],
    value: &[u8],
  ) -> Result<(), AofReplayError> {
    // 条件写形态（EX/NX 等经 StoreRMW 路径回放）；upsert 直写
    session
      .upsert_string(key, value)
      .await
      .map_err(AofReplayError::Store)
  }

  /// libs/server/AOF/AofProcessor.cs:StoreRMW
  ///
  /// RMW 命令重放：RI 族交范围索引重放面实际执行（C#
  /// RangeIndexManager.HandleRangeIndex*Replay）；DELIFEXPIM 为 TTL 物理清除
  /// 确定性单条目（C# ExpireAndResume + Expired|Deterministic 标志的回放面）；
  /// 其余按命令族落主存 RMW / 键管理面。
  pub async fn store_rmw<D: Device>(
    &self,
    session: &StorageSession<'_, D>,
    key: &[u8],
    input: &[u8],
    // _legacy_cmd_format: 已彻底移除旧版 v3 命令兼容层映射，直接使用二进制 input.cmd
    _legacy_cmd_format: bool,
  ) -> Result<(), AofReplayError> {
    let input = ReplayInput::deserialize(input).ok_or("StoreRMW input 损坏")?;
    // 向量族 RMW 子分派（VADD/VREM/VSETATTR）须向量域承接（缺口见汇报）
    if matches!(
      input.cmd,
      RespCommand::Vadd | RespCommand::Vrem | RespCommand::Vsetattr
    ) {
      return Err(
        "vector replay requires vector domain; unsupported"
          .to_string()
          .into(),
      );
    }
    // 范围索引族须实际执行（C# AofProcessor.StoreRMW 的 RICREATE/RISET/RIDEL
    // 分派）：context 已由 KeyContextGuard 切至条目域，直接交 RI 复制面
    if matches!(
      input.cmd,
      RespCommand::Ricreate | RespCommand::Riset | RespCommand::Ridel
    ) {
      let Some(ri) = &self.range_index else {
        return Err(
          "RangeIndexPreview disabled; Replay failed"
            .to_string()
            .into(),
        );
      };
      let result = match input.cmd {
        RespCommand::Ricreate => {
          ri.handle_range_index_create_replay(&session.batch, key, &input)
            .await
        }
        RespCommand::Riset => {
          ri.handle_range_index_set_replay(&session.batch, key, &input)
            .await
        }
        RespCommand::Ridel => {
          ri.handle_range_index_del_replay(&session.batch, key, &input)
            .await
        }
        _ => unreachable!("RI 族判别已收敛"),
      };
      return result.map_err(|e| format!("RangeIndex replay failed: {e}").into());
    }
    // TTL 物理清除确定性重放（C# DELIFEXPIM：Expired|Deterministic 标志下
    // CheckExpiry 恒真 → 删除）；主端已判定到期，重放端幂等执行统一 DEL
    //（先清随键 TTL 再清数据，context 已切至条目域）
    if input.cmd == RespCommand::Delifexpim {
      session
        .batch
        .delete(key)
        .await
        .map_err(|e| format!("Delifexpim replay failed: {e}"))?;
      return Ok(());
    }
    let op = match input.cmd {
      RespCommand::Incr | RespCommand::Incrby => StringRMWOp::Incr { delta: input.arg1 },
      RespCommand::Decr | RespCommand::Decrby => StringRMWOp::Incr { delta: -input.arg1 },
      RespCommand::Incrbyfloat => StringRMWOp::IncrFloat {
        delta: f64::from_bits(input.arg1 as u64),
      },
      RespCommand::Append => {
        let data = input.args.first().map_or(&[][..], Vec::as_slice);
        StringRMWOp::Append(data)
      }
      RespCommand::Setrange => {
        let data = input.args.first().map_or(&[][..], Vec::as_slice);
        StringRMWOp::SetRange {
          offset: input.arg2.max(0) as usize,
          data,
        }
      }
      RespCommand::Expire => {
        // 重放边界换算（对标 KeyAdminCommands.cs:423 的 EXPIRE→AddSeconds）：
        // 相对秒 → 相对 ticks
        session
          .expire_in_ticks(key, input.arg1.max(0).saturating_mul(TICKS_PER_SECOND))
          .await
          .map_err(|e| format!("Expire replay failed: {e}"))?;
        return Ok(());
      }
      RespCommand::Pexpire => {
        // 相对毫秒 → 相对 ticks（KeyAdminCommands.cs:424 的 PEXPIRE→AddMilliseconds）
        session
          .expire_in_ticks(key, input.arg1.max(0).saturating_mul(TICKS_PER_MILLISECOND))
          .await
          .map_err(|e| format!("Pexpire replay failed: {e}"))?;
        return Ok(());
      }
      RespCommand::Expireat => {
        // 绝对 Unix 秒 → ticks（KeyAdminCommands.cs:423 的 EXPIREAT）
        session
          .expire_at_ticks(
            key,
            unix_timestamp_in_seconds_to_ticks(
              input
                .arg1
                .clamp(0, (i64::MAX - UNIX_EPOCH_TICKS) / TICKS_PER_SECOND),
            ),
          )
          .await
          .map_err(|e| format!("Expireat replay failed: {e}"))?;
        return Ok(());
      }
      RespCommand::Pexpireat => {
        // 绝对 Unix 毫秒 → ticks（KeyAdminCommands.cs:426 的 PEXPIREAT）
        session
          .expire_at_ticks(
            key,
            unix_timestamp_in_milliseconds_to_ticks(
              input
                .arg1
                .clamp(0, (i64::MAX - UNIX_EPOCH_TICKS) / TICKS_PER_MILLISECOND),
            ),
          )
          .await
          .map_err(|e| format!("Pexpireat replay failed: {e}"))?;
        return Ok(());
      }
      RespCommand::Persist => {
        session
          .persist_key(key)
          .await
          .map_err(|e| format!("Persist replay failed: {e}"))?;
        return Ok(());
      }
      RespCommand::Getdel => {
        session
          .getdel(key)
          .await
          .map_err(|e| format!("Getdel replay failed: {e}"))?;
        return Ok(());
      }
      RespCommand::Setex => {
        let val = input.args.first().map_or(&[][..], Vec::as_slice);
        let ticks = input.arg1.max(0).saturating_mul(TICKS_PER_SECOND);
        session
          .setex(key, val, ticks)
          .await
          .map_err(|e| format!("Setex replay failed: {e}"))?;
        return Ok(());
      }
      RespCommand::Psetex => {
        let val = input.args.first().map_or(&[][..], Vec::as_slice);
        let ticks = input.arg1.max(0).saturating_mul(TICKS_PER_MILLISECOND);
        session
          .setex(key, val, ticks)
          .await
          .map_err(|e| format!("Psetex replay failed: {e}"))?;
        return Ok(());
      }
      _ => {
        // C# 此处对全部命令走完整 RMW 重放（条件 SET 族 SETEXNX/SETEXXX/
        // SETKEEPTTL* 经 MainSessionFunctions 重新评估 NX/XX 条件）。
        // 此处保持告警，避免未知命令静默吞没。
        log::warn!(
          "AOF StoreRMW replay skipped unsupported cmd {:?} (key length {})",
          input.cmd,
          key.len()
        );
        return Ok(());
      }
    };
    session
      .rmw_main_store(key, op)
      .await
      .map_err(|e| format!("StoreRMW replay failed: {e}"))?;
    Ok(())
  }

  /// libs/server/AOF/AofProcessor.cs:StoreDelete
  pub async fn store_delete<D: Device>(
    session: &StorageSession<'_, D>,
    key: &[u8],
  ) -> Result<(), AofReplayError> {
    session
      .delete_string(key)
      .await
      .map_err(|e| format!("StoreDelete replay failed: {e}"))?;
    Ok(())
  }

  /// libs/server/AOF/AofProcessor.cs:ObjectStoreUpsert
  pub async fn object_store_upsert<D: Device>(
    session: &StorageSession<'_, D>,
    key: &[u8],
    value: &[u8],
  ) -> Result<(), AofReplayError> {
    // 对象信封：[tag u8][payload]（C# 值 = GarnetObjectSerializer 位码；
    // wkv 信封以类型标签 + 剥壳载荷存储）
    let Some((&tag, payload)) = value.split_first() else {
      return Err("ObjectStoreUpsert 值缺少类型标签".to_string().into());
    };
    session
      .obj_save(key, tag, payload)
      .await
      .map_err(AofReplayError::Store)
  }

  /// libs/server/AOF/AofProcessor.cs:ObjectStoreRMW
  pub async fn object_store_rmw<D: Device>(
    session: &StorageSession<'_, D>,
    key: &[u8],
    input: &[u8],
    legacy_cmd_format: bool,
  ) -> Result<(), AofReplayError> {
    let mut r_input = ReplayInput::deserialize(input).ok_or("ObjectStoreRMW input 损坏")?;
    if legacy_cmd_format {
      // 旧版对象头把子操作 id 装入 flags 低 5 位（C# RelocateLegacyObjectSubId）
      let (sub_id, flags) = Self::relocate_legacy_object_sub_id(r_input.flags);
      r_input.sub_id = sub_id;
      r_input.flags = flags;
    }

    // 确定对象类型（C# ObjectInput.header.type 显式判别；生产写入端恒写
    // obj_type，命令判别与现存键信封为测试/历史条目 fallback）
    let (obj_type, default_sub_id) = if let Some(
      t @ (GarnetObjectType::SortedSet
      | GarnetObjectType::List
      | GarnetObjectType::Hash
      | GarnetObjectType::Set),
    ) = GarnetObjectType::from_u8(r_input.obj_type)
    {
      (t, r_input.sub_id)
    } else {
      match r_input.cmd {
        RespCommand::Hset => (GarnetObjectType::Hash, HashOperation::Hset as u8),
        RespCommand::Hmset => (GarnetObjectType::Hash, HashOperation::Hmset as u8),
        RespCommand::Hsetnx => (GarnetObjectType::Hash, HashOperation::Hsetnx as u8),
        RespCommand::Hdel => (GarnetObjectType::Hash, HashOperation::Hdel as u8),
        RespCommand::Hincrby => (GarnetObjectType::Hash, HashOperation::Hincrby as u8),
        RespCommand::Hincrbyfloat => (GarnetObjectType::Hash, HashOperation::Hincrbyfloat as u8),
        RespCommand::Hexpire => (GarnetObjectType::Hash, HashOperation::Hexpire as u8),
        RespCommand::Hpersist => (GarnetObjectType::Hash, HashOperation::Hpersist as u8),
        RespCommand::Sadd => (GarnetObjectType::Set, SetOperation::Sadd as u8),
        RespCommand::Srem => (GarnetObjectType::Set, SetOperation::Srem as u8),
        RespCommand::Spop => (GarnetObjectType::Set, SetOperation::Spop as u8),
        RespCommand::Lpush => (GarnetObjectType::List, ListOperation::Lpush as u8),
        RespCommand::Rpush => (GarnetObjectType::List, ListOperation::Rpush as u8),
        RespCommand::Lpop => (GarnetObjectType::List, ListOperation::Lpop as u8),
        RespCommand::Rpop => (GarnetObjectType::List, ListOperation::Rpop as u8),
        RespCommand::Lrem => (GarnetObjectType::List, ListOperation::Lrem as u8),
        RespCommand::Ltrim => (GarnetObjectType::List, ListOperation::Ltrim as u8),
        RespCommand::Lset => (GarnetObjectType::List, ListOperation::Lset as u8),
        RespCommand::Linsert => (GarnetObjectType::List, ListOperation::Linsert as u8),
        RespCommand::Zadd => (GarnetObjectType::SortedSet, SortedSetOperation::Zadd as u8),
        RespCommand::Zrem => (GarnetObjectType::SortedSet, SortedSetOperation::Zrem as u8),
        RespCommand::Zincrby => (
          GarnetObjectType::SortedSet,
          SortedSetOperation::Zincrby as u8,
        ),
        RespCommand::Zpopmin => (
          GarnetObjectType::SortedSet,
          SortedSetOperation::Zpopmin as u8,
        ),
        RespCommand::Zpopmax => (
          GarnetObjectType::SortedSet,
          SortedSetOperation::Zpopmax as u8,
        ),
        _ => {
          let tag_opt = session
            .read_string(key)
            .await
            .ok()
            .flatten()
            .and_then(|v| v.first().copied());
          match tag_opt.and_then(GarnetObjectType::from_u8) {
            Some(
              t @ (GarnetObjectType::Hash
              | GarnetObjectType::Set
              | GarnetObjectType::List
              | GarnetObjectType::SortedSet),
            ) => (t, r_input.sub_id),
            _ => (GarnetObjectType::Hash, r_input.sub_id),
          }
        }
      }
    };

    let sub_id = if r_input.sub_id != 0 {
      r_input.sub_id
    } else {
      default_sub_id
    };
    let arg_refs: Vec<&[u8]> = r_input.args.iter().map(Vec::as_slice).collect();
    let obj_input = make_object_input(
      obj_type,
      sub_id,
      &arg_refs,
      r_input.arg1 as i32,
      r_input.arg2 as i32,
    );
    let mut out = ObjectOutput::new();
    let resp_version = session.resp_protocol_version();

    match obj_type {
      GarnetObjectType::Hash => {
        let raw = session.read_string(key).await?;
        let payload = raw
          .as_deref()
          .and_then(|r| obj_decode(r, GarnetObjectType::Hash as u8));
        let mut obj = payload.map(hash_from_blob).unwrap_or_default();
        obj.operate(&obj_input, &mut out, resp_version);
        if obj.is_empty() {
          session
            .delete_string(key)
            .await
            .map_err(AofReplayError::Store)?;
        } else {
          let blob = hash_to_blob(&obj);
          session
            .obj_save(key, GarnetObjectType::Hash as u8, &blob)
            .await
            .map_err(AofReplayError::Store)?;
        }
      }
      GarnetObjectType::Set => {
        let raw = session.read_string(key).await?;
        let payload = raw
          .as_deref()
          .and_then(|r| obj_decode(r, GarnetObjectType::Set as u8));
        let mut obj = payload.map(set_from_blob).unwrap_or_default();
        obj.operate(&obj_input, &mut out, resp_version);
        if obj.is_empty() {
          session
            .delete_string(key)
            .await
            .map_err(AofReplayError::Store)?;
        } else {
          let blob = set_to_blob(&obj);
          session
            .obj_save(key, GarnetObjectType::Set as u8, &blob)
            .await
            .map_err(AofReplayError::Store)?;
        }
      }
      GarnetObjectType::List => {
        let raw = session.read_string(key).await?;
        let payload = raw
          .as_deref()
          .and_then(|r| obj_decode(r, GarnetObjectType::List as u8));
        let mut obj = payload.map(list_from_blob).unwrap_or_default();
        obj.operate(&obj_input, &mut out, resp_version);
        if obj.is_empty() {
          session
            .delete_string(key)
            .await
            .map_err(AofReplayError::Store)?;
        } else {
          let blob = list_to_blob(&obj);
          session
            .obj_save(key, GarnetObjectType::List as u8, &blob)
            .await
            .map_err(AofReplayError::Store)?;
        }
      }
      GarnetObjectType::SortedSet => {
        let raw = session.read_string(key).await?;
        let payload = raw
          .as_deref()
          .and_then(|r| obj_decode(r, GarnetObjectType::SortedSet as u8));
        let mut obj = payload.map(zset_from_blob).unwrap_or_default();
        obj.operate(&obj_input, &mut out, resp_version);
        if obj.is_empty() {
          session
            .delete_string(key)
            .await
            .map_err(AofReplayError::Store)?;
        } else {
          let blob = zset_to_blob(&obj);
          session
            .obj_save(key, GarnetObjectType::SortedSet as u8, &blob)
            .await
            .map_err(AofReplayError::Store)?;
        }
      }
      _ => {}
    }
    Ok(())
  }

  /// libs/server/AOF/AofProcessor.cs:ObjectStoreDelete
  pub async fn object_store_delete<D: Device>(
    session: &StorageSession<'_, D>,
    key: &[u8],
  ) -> Result<(), AofReplayError> {
    session
      .delete_object_store(key)
      .await
      .map_err(|e| format!("ObjectStoreDelete replay failed: {e}"))?;
    Ok(())
  }

  /// libs/server/AOF/AofProcessor.cs:UnifiedStoreStringUpsert
  pub async fn unified_store_string_upsert<D: Device>(
    session: &StorageSession<'_, D>,
    key: &[u8],
    value: &[u8],
  ) -> Result<(), AofReplayError> {
    // 统一存字符串上 ups 与主存 upsert 同形（wkv 单一面）；RENAME 向量特例
    // 须向量域承接（缺口见汇报）
    Self::store_upsert(session, key, value).await
  }

  /// libs/server/AOF/AofProcessor.cs:RelocateLegacyObjectSubId（纯函数面）
  pub fn relocate_legacy_object_sub_id(flags: u8) -> (u8, u8) {
    const LEGACY_SUB_ID_MASK: u8 = 0x1F;
    (flags & LEGACY_SUB_ID_MASK, flags & !LEGACY_SUB_ID_MASK)
  }

  /// libs/server/AOF/AofProcessor.cs:ShouldSkipRecord
  ///
  /// 恢复/复制回放的版本闸：旧检查点代际条目跳过；副本模糊区内的新代条目
  /// 入缓冲（C# BufferNewVersionRecord）。
  pub fn should_skip_record(
    &self,
    sublog_idx: usize,
    entry: &[u8],
    as_replica: bool,
    store_version: i64,
  ) -> bool {
    let Some(header) = AofHeader::parse(entry) else {
      return true;
    };
    if as_replica && self.coordinator.context(sublog_idx).in_fuzzy_region() {
      if Self::is_new_version_record(&header, store_version) {
        self
          .coordinator
          .add_fuzzy_region_operation(sublog_idx, ReplayOperation::Record(entry.to_vec()));
        return true;
      }
      return false;
    }
    Self::is_old_version_record(&header, store_version)
  }

  /// 分块形态的 ShouldSkipRecord（C# ChunkReplay 分片同名；模糊区新代入缓冲）。
  pub fn should_skip_record_chunk(
    &self,
    sublog_idx: usize,
    acc: &super::aof_chunked_record_reader::ChunkedAccumulator,
    as_replica: bool,
    store_version: i64,
  ) -> bool {
    if as_replica && self.coordinator.context(sublog_idx).in_fuzzy_region() {
      if acc.store_version > store_version {
        self
          .coordinator
          .add_fuzzy_region_operation(sublog_idx, ReplayOperation::Chunk(Box::new(acc.clone())));
        return true;
      }
      return false;
    }
    acc.store_version < store_version
  }

  /// libs/server/AOF/AofProcessor.cs:IsOldVersionRecord
  pub fn is_old_version_record(header: &AofHeader, store_version: i64) -> bool {
    header.store_version < store_version
  }

  /// libs/server/AOF/AofProcessor.cs:IsNewVersionRecord
  pub fn is_new_version_record(header: &AofHeader, store_version: i64) -> bool {
    header.store_version > store_version
  }

  /// libs/server/AOF/AofProcessor.cs:CanReplay
  ///
  /// 并行回放任务归属判定：返回 (本任务是否处理该条目, 条目序列号)。
  /// 顺序回放（单任务）恒 true。
  pub fn can_replay(
    &self,
    entry: &[u8],
    replay_task_idx: usize,
    entry_address: i64,
  ) -> Option<(bool, i64)> {
    let header = AofHeader::parse(entry)?;
    let log = self.append_only_file.log();
    match header.header_type()? {
      AofHeaderType::BasicHeader | AofHeaderType::BasicChunkHeader => {
        let op_type = AofEntryType::try_from(header.op_type).ok()?;
        if !op_type.has_key() {
          return Some((true, entry_address));
        }
        let chunk = header.is_chunked();
        let routing = if chunk {
          let (_, ch) = AofHeader::get_chunked_header_ref(entry)?;
          ch.key_hash
        } else {
          let offset = AofHeader::skip_header(entry)?;
          let len = u32::from_le_bytes(entry[offset..offset + 4].try_into().ok()?) as usize;
          let key = entry.get(offset + 4..offset + 4 + len)?;
          GarnetLog::hash(key)
        };
        Some((
          replay_task_idx == log.get_replay_task_idx(routing),
          entry_address,
        ))
      }
      AofHeaderType::ShardedHeader | AofHeaderType::ShardedChunkHeader => {
        let sh = AofShardedHeader::parse(entry)?;
        let op_type = AofEntryType::try_from(header.op_type).ok()?;
        if !op_type.has_key() {
          return Some((replay_task_idx == 0, sh.sequence_number));
        }
        let offset = AofHeader::skip_header(entry)?;
        let len = u32::from_le_bytes(*entry.get(offset..)?.first_chunk::<4>()?) as usize;
        let key = entry.get(offset + 4..offset + 4 + len)?;
        Some((
          replay_task_idx == log.get_replay_task_idx(GarnetLog::hash(key)),
          sh.sequence_number,
        ))
      }
      _ => None,
    }
  }

  /// libs/server/AOF/AofProcessor.cs:SkipReplay
  ///
  /// 前缀一致恢复上界判定：条目序列号超过阈值即跳过（单调 ⇒ 后续全跳）。
  /// 返回 (是否跳过, 条目序列号)；`until_sequence_number == -1` 全跳。
  pub fn skip_replay(
    &self,
    entry: &[u8],
    until_sequence_number: i64,
    log_address_sequence_number: i64,
  ) -> Option<(bool, i64)> {
    if until_sequence_number == -1 {
      return Some((true, -1));
    }
    let header = AofHeader::parse(entry)?;
    let sequence_number = match header.header_type()? {
      AofHeaderType::BasicHeader | AofHeaderType::BasicChunkHeader => log_address_sequence_number,
      AofHeaderType::ShardedHeader | AofHeaderType::ShardedChunkHeader => {
        AofShardedHeader::parse(entry)?.sequence_number
      }
      _ => log_address_sequence_number,
    };
    Some((sequence_number > until_sequence_number, sequence_number))
  }

  /// 条目 key 速览（事务组加锁集提取面；不在场返回 None）。
  pub fn peek_entry_key(entry: &[u8]) -> Option<&[u8]> {
    let offset = AofHeader::skip_header(entry)?;
    let len = u32::from_le_bytes(*entry.get(offset..)?.first_chunk::<4>()?) as usize;
    entry.get(offset + 4..offset + 4 + len)
  }
}

/// 物理键 context 切换守卫（构造时解出 `(ns, db, 用户键)` 并切会话
/// context，drop 时恢复原值）。
///
/// 条目 key 统一为 wkv 物理键（`[NsVarint][DbVarint][KeyTag][用户键]`，
/// 等价 C# 单库 AOF 的用户键 + databaseId 组合，且跨 ns/db 域无损）；
/// 重放应用面（StorageSession）为用户键接口，经本守卫完成域切换。
struct KeyContextGuard<'a, D: Device> {
  batch: &'a wkv::BatchStoreSession<'a, D>,
  prev: (u64, u64),
  /// 用户键（物理键剥前缀）
  user_key: &'a [u8],
}

impl<'a, D: Device> KeyContextGuard<'a, D> {
  fn enter(session: &'a StorageSession<'_, D>, key: &'a [u8]) -> Result<Self, AofReplayError> {
    let (ns, db, _, user_key) = wkv::NamespaceDbCodec::decode_tagged_key(key)
      .map_err(|e| format!("AOF 条目物理键损坏: {e}"))?;
    let batch = &session.batch;
    let prev = (batch.namespace(), batch.active_db());
    batch.set_context(ns, db);
    Ok(Self {
      batch,
      prev,
      user_key,
    })
  }
}

impl<D: Device> Drop for KeyContextGuard<'_, D> {
  fn drop(&mut self) {
    self.batch.set_context(self.prev.0, self.prev.1);
  }
}

/// 无效地址向量便捷构造（恢复面对齐 C# AofAddress.Create(len, -1)）。
pub fn invalid_aof_address(physical_sublog_count: usize) -> AofAddress {
  AofAddress::create(physical_sublog_count as i32, -1)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn replay_input_parses_integration_bytes() {
    let bytes: Vec<u8> = [
      0x4a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x6b, 0x02, 0x00, 0x00, 0x00,
      0x76, 0x31,
    ]
    .to_vec();
    let parsed = ReplayInput::deserialize(&bytes).expect("集成字节应可解析");
    assert_eq!(parsed.cmd, RespCommand::Set);
    assert_eq!(parsed.args, vec![b"k".to_vec(), b"v1".to_vec()]);
  }

  #[test]
  fn replay_input_roundtrip() {
    let input = ReplayInput {
      cmd: RespCommand::Set,
      flags: 0,
      sub_id: 0,
      obj_type: 0,
      arg1: 32,
      arg2: 0,
      arg3: 0,
      args: vec![b"cnt".to_vec(), b"32".to_vec()],
    };
    let mut bytes = Vec::new();
    input.serialize(&mut bytes);
    // 头 8B 元信息 + 24B 参数区 + 4B 计数 + 逐参长度前缀
    assert_eq!(bytes.len(), 8 + 24 + 4 + (4 + 3) + (4 + 2));
    let parsed = ReplayInput::deserialize(&bytes).expect("应可反序列化");
    assert_eq!(parsed.cmd, RespCommand::Set);
    assert_eq!(parsed.arg1, 32);
    assert_eq!(parsed.args, vec![b"cnt".to_vec(), b"32".to_vec()]);

    // 对象形态：obj_type 显式判别（对标 C# ObjectInput.header.type）
    let object_input = ReplayInput {
      cmd: RespCommand::None,
      flags: 0,
      sub_id: 6,
      obj_type: GarnetObjectType::Hash as u8,
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"f".to_vec(), b"v".to_vec()],
    };
    let mut object_bytes = Vec::new();
    object_input.serialize(&mut object_bytes);
    let parsed = ReplayInput::deserialize(&object_bytes).expect("应可反序列化");
    assert_eq!(parsed.obj_type, GarnetObjectType::Hash as u8);
    assert_eq!(parsed.sub_id, 6);
  }
}
