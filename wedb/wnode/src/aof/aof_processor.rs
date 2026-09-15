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

use parking_lot::RwLock;
use waof::{
  AofHeader, AofHeaderType, AofShardedHeader, AofShardedLogTransactionHeader,
  AofSingleLogTransactionHeader,
};
use wbase::{
  convert::{
    duration_milliseconds_to_ticks, duration_seconds_to_ticks, expire_at_milliseconds_to_ticks,
    expire_at_seconds_to_ticks,
  },
  entry_type::AofEntryType,
};
use wcol::{
  HashObject, ListObject, ObjectInput, ObjectOutput, SetObject, SortedSetObject,
  hash::hash_object::HashOperation,
  list::list_object::ListOperation,
  object_store_utils::{make_object_input, obj_decode},
  set::set_object::SetOperation,
  zset::sorted_set_object::SortedSetOperation,
};
use wdev::Device;
use wkv::WedbStore;
use wresp::RespCommand;
use wval::{GarnetObjectType, KeyTag, NO_ETAG, NamespaceDbCodec};

use super::{
  garnet_append_only_file::GarnetAppendOnlyFile,
  garnet_log::GarnetLog,
  readconsistency::{
    custom_procedure_key_hash_collection::CustomProcedureKeyHashCollection,
    read_consistency_manager::ReadConsistencyManager,
  },
  replaycoordinator::{
    aof_replay_context::{ReplayOperation, TransactionGroup},
    aof_replay_coordinator::{AofReplayCoordinator, BarrierKey},
    stored_proc_replay::{StoredProcRegistryReplayer, stored_proc_args, stored_proc_payload},
  },
};
use crate::{
  resp::rangeindex::range_index_manager_replication::RangeIndexManagerReplication,
  storage::session::{mainstore::advanced_ops::StringRMWOp, storage_session::StorageSession},
};

/// 范围索引存储会话抽象接口（解耦具体设备类型，消除 unsafe 裸指针转换）
pub trait RangeIndexSessionFace: Send + Sync {
  /// 创建范围索引
  fn ri_create(
    &self,
    key: &[u8],
    backend: wbftree::StorageBackendType,
    tuning: wbftree::TreeTuning,
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
    backend: wbftree::StorageBackendType,
    tuning: wbftree::TreeTuning,
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
    backend: wbftree::StorageBackendType,
    tuning: wbftree::TreeTuning,
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
  /// 命令（判别值与 wresp::RespCommand 一致）。
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
  pub const fn with_args_num(mut self, arg1: i64, arg2: i64, arg3: i64) -> Self {
    self.arg1 = arg1;
    self.arg2 = arg2;
    self.arg3 = arg3;
    self
  }
}

impl ReplayInput {
  /// 计算切片序列化字节数（32B 固定头 + 参数序列区）
  #[inline]
  pub fn encoded_len_for_slices(args: &[impl AsRef<[u8]>]) -> usize {
    REPLAY_INPUT_HEADER_SIZE + waof::arg_sequence_len(args)
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
    let args_len = waof::encode_arg_sequence(input.args, &mut buf[REPLAY_INPUT_HEADER_SIZE..]);
    Some(&buf[..REPLAY_INPUT_HEADER_SIZE + args_len])
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

  /// 反序列化（C# StringInput.DeserializeFrom 的组合形态；参数序列区
  /// 经 waof 单点解码）。
  pub fn deserialize(bytes: &[u8]) -> Option<Self> {
    if bytes.len() < REPLAY_INPUT_HEADER_SIZE {
      return None;
    }
    let cmd = RespCommand::try_from(u16::from_le_bytes([bytes[0], bytes[1]])).ok()?;
    // 参数序列区：waof 单点（count 防爆破 + 逐参边界校验）
    let args = waof::decode_arg_sequence(&bytes[REPLAY_INPUT_HEADER_SIZE..])?;
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

impl<'a, 'b, D: Device> ReplayTarget<'a, 'b, D> {
  /// 装配重放落点（版本取自存储当前值，装配点统一经此构造）。
  pub fn new(session: &'b StorageSession<'a, D>, store: &Arc<WedbStore<D>>) -> Self {
    Self {
      session,
      store: Arc::clone(store),
      store_version: store.current_version(),
    }
  }
}

/// libs/server/AOF/AofProcessor.cs:AofProcessor
///
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
  /// 未注入为 None，RI 族条目重放按 C# 同文案失败）
  range_index: Option<Arc<RangeIndexManagerReplication>>,
  /// 存储过程重放执行面（C# 经 replayContext.respServerSession 承接；
  /// rust 回放驱动经注册面装配，未注入为 None）。
  stored_proc_replayer: RwLock<Option<Arc<StoredProcRegistryReplayer>>>,
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
      stored_proc_replayer: RwLock::new(None),
    }
  }

  /// 注入存储过程重放执行面（C# 由 storeWrapper.customCommandManager 经
  /// 回放会话承接；rust 恢复/复制驱动经注册面装配）。
  pub fn set_stored_proc_replayer(&mut self, replayer: Arc<StoredProcRegistryReplayer>) {
    *self.stored_proc_replayer.write() = Some(replayer);
  }

  /// 存储过程重放执行面快照。
  pub fn stored_proc_replayer(&self) -> Option<Arc<StoredProcRegistryReplayer>> {
    self.stored_proc_replayer.read().clone()
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
  /// 提取（序列号, 参与者数）：序列号经 [`AofHeader::sequence_number_of`]
  /// 单点（分片形态取内嵌、其余取条目地址）；参与者数事务头形态取头内
  /// 值，其余取全量回放任务数（C# BasicHeader 兜底分支）。
  pub fn get_synchronized_operation_params(
    &self,
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
      _ => self.replay_task_count() as i16,
    };
    Some((sequence_number, participant_count))
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:ReplayStoredProc
  ///
  /// 存储过程回放包装（单 / 分片日志统一入口）：
  /// - 单物理日志：直接重放（无收集器，对齐 C# `tracker: null`）；
  /// - 多日志拓扑：经同步栅栏（`LeaderBarrierType.CustomStoredProc` 类别，
  ///   键为会话 id + 序列号）裁决，仅领导者执行；领导者以
  ///   [`CustomProcedureKeyHashCollection`] 收集过程触达键哈希，回放后
  ///   推进其读一致性时间戳（顺序差异见该类型文档）。
  pub async fn replay_stored_proc(
    &self,
    virtual_sublog_idx: usize,
    entry: &[u8],
    log_address_sequence_number: i64,
  ) -> Result<(), AofReplayError> {
    let Some(replayer) = self.stored_proc_replayer() else {
      return Err(
        "AOF 存储过程条目回放未装配执行面（custom 注册表未注入）"
          .to_string()
          .into(),
      );
    };
    let header = AofHeader::parse(entry).ok_or("存储过程条目头损坏")?;
    let proc_id = header.procedure_id;
    let session_id = header.session_id;
    let args =
      stored_proc_args::decode(stored_proc_payload(entry)?).ok_or("存储过程输入载荷损坏")?;

    if !self.append_only_file.multi_log_enabled() {
      let mut hashes = Vec::new();
      return replayer.replay(proc_id, session_id, &args, &mut hashes);
    }

    let Some((sequence_number, participant_count)) =
      self.get_synchronized_operation_params(entry, log_address_sequence_number)
    else {
      return Err("存储过程条目缺少同步操作参数".into());
    };

    let barrier_key = BarrierKey::new(session_id, sequence_number);
    if !self.coordinator.get_barrier(barrier_key, participant_count) {
      // 非领导者：等待领导者执行（顺序回放面无阻塞等待，对齐 C# 栅栏等待）
      return Ok(());
    }

    let result = self.run_stored_proc_with_tracker(
      virtual_sublog_idx,
      &replayer,
      proc_id,
      session_id,
      &args,
      sequence_number,
    );
    self.coordinator.try_remove_barrier(barrier_key);
    result
  }

  /// 多日志存储过程重放（C# StoredProcRunnerWrapper + StoredProcRunnerBase
  /// 合流）：收集器登记过程触达键哈希并推进读一致性时间戳。
  fn run_stored_proc_with_tracker(
    &self,
    virtual_sublog_idx: usize,
    replayer: &StoredProcRegistryReplayer,
    proc_id: u8,
    session_id: i32,
    args: &[Vec<u8>],
    sequence_number: i64,
  ) -> Result<(), AofReplayError> {
    let _ = virtual_sublog_idx;
    let mut tracker = CustomProcedureKeyHashCollection::new(&self.append_only_file);
    let mut hashes = Vec::new();
    let result = replayer.replay(proc_id, session_id, args, &mut hashes);
    if result.is_ok() && !hashes.is_empty() {
      tracker.extend(hashes);
      tracker.update_sequence_number(sequence_number);
    }
    result
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
        // libs/server/AOF/AofProcessor.cs:ReplayAOF（case FlushAll →
        // StoreWrapper.FlushAllDatabases(unsafeTruncateLog)）：全部库用户域清空
        if !header.unsafe_truncate_log() {
          log::warn!("AOF 日志安全截断跳过或未执行");
        }
        target.store.flush_all_databases().await?;
      }
      AofEntryType::FlushDb => {
        // libs/server/AOF/AofProcessor.cs:ReplayAOF（case FlushDb →
        // StoreWrapper.FlushDatabase(unsafeTruncateLog, dbId: header.databaseId)）：
        // 仅清 databaseId 指定库，其它库数据不受影响
        if !header.unsafe_truncate_log() {
          log::warn!("AOF 日志安全截断跳过或未执行");
        }
        let ns = target.session.batch.namespace();
        target
          .store
          .flush_database(ns, u64::from(header.database_id))
          .await?;
      }
      AofEntryType::StoredProcedure => {
        // 存储过程重放（C# ReplayStoredProc）：经注册表工厂重建过程实例，
        // 走 wtxn 事务三段式落库
        self
          .replay_stored_proc(virtual_sublog_idx, entry, log_address_sequence_number)
          .await?;
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

  /// libs/server/AOF/AofProcessor.cs:ProcessFuzzyRegionOperations
  ///
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

  /// libs/server/AOF/AofProcessor.cs:ProcessFuzzyRegionTransactionGroup
  ///
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
          .map_err(|e| AofReplayError::from(format!("RangeIndexStreamChunk replay failed: {e}")))
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
    // 向量族 RMW 子分派（VADD/VREM/VSETATTR）交向量域真实重放（对标 C#
    // AofProcessor.StoreRMW 的 VADD/VREM/VSETATTR 分派：经 VectorManager
    // 重建索引/增删元素/改属性；context 已由 KeyContextGuard 切至条目域）
    if matches!(
      input.cmd,
      RespCommand::Vadd | RespCommand::Vrem | RespCommand::Vsetattr
    ) {
      let Some(vm) = self.append_only_file.vector_manager() else {
        return Err(
          "Vector Set (preview) commands are not enabled; Replay failed"
            .to_string()
            .into(),
        );
      };
      let result = match input.cmd {
        RespCommand::Vadd => vm.replay_vector_set_add(key, &input),
        RespCommand::Vrem => vm.replay_vector_set_remove(key, &input),
        RespCommand::Vsetattr => vm.replay_vector_set_set_attribute(key, &input),
        _ => unreachable!("向量族判别已收敛"),
      };
      // 错误文案对齐 RangeIndex 分支的原格式
      return result.map_err(|e| AofReplayError::from(format!("Vector Set replay failed: {e}")));
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
      // 错误文案（"RangeIndex replay failed: …"）对齐原格式
      return result.map_err(|e| AofReplayError::from(format!("RangeIndex replay failed: {e}")));
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
    // ETag 旁路记录确定性直设/清除（wnode ETag 写监听端口入队的两跳等价
    // 条目；对标 C# RMWMethods.Etags 族 etag 内嵌记录随 RMW input 重放
    // 确定性重现——rust 侧 etag 为独立旁路记录，条目 arg1 携带主端线性化
    // 后的绝对 etag 值直设，不重算）：arg1 > NO_ETAG 直设旁路记录；
    // arg1 == NO_ETAG(0) 为清除墓碑（合法 etag 恒 >= 1，0 无歧义），物理
    // 删除旁路记录，杜绝 DEL 级联后盘上残留令恢复复活旧 etag。
    //
    // 原子性边界：C# etag 与值同记录一体；rust 拆「值记录 + etag 旁路
    // 记录」两记录，写入端 apply 序恒为「值 upsert → etag 推进」，AOF 全序
    // 保证值条目先于 etag 条目，恢复同序重放即收敛（检查点基线过滤对两
    // 记录同代生效，不产生跨代错序）
    if input.cmd == RespCommand::Setwithetag {
      if input.arg1 > NO_ETAG {
        session
          .batch
          .put_etag(key, input.arg1)
          .await
          .map_err(|e| format!("Setwithetag replay failed: {e}"))?;
      } else {
        session
          .batch
          .del_etag(key)
          .await
          .map_err(|e| format!("Setwithetag replay failed: {e}"))?;
      }
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
        // 相对秒 → 时长 ticks（饱和乘法单点与命令端同源）
        session
          .expire_in_ticks(key, duration_seconds_to_ticks(input.arg1.max(0)))
          .await
          .map_err(|e| format!("Expire replay failed: {e}"))?;
        return Ok(());
      }
      RespCommand::Pexpire => {
        // 相对毫秒 → 时长 ticks（KeyAdminCommands.cs:424 的 PEXPIRE→AddMilliseconds，
        // 饱和乘法单点与命令端同源）
        session
          .expire_in_ticks(key, duration_milliseconds_to_ticks(input.arg1.max(0)))
          .await
          .map_err(|e| format!("Pexpire replay failed: {e}"))?;
        return Ok(());
      }
      RespCommand::Expireat => {
        // 绝对 Unix 秒 → ticks（KeyAdminCommands.cs:425 的 EXPIREAT，
        // 负夹 0/上界钳制单点与命令端同函数）
        session
          .expire_at_ticks(key, expire_at_seconds_to_ticks(input.arg1))
          .await
          .map_err(|e| format!("Expireat replay failed: {e}"))?;
        return Ok(());
      }
      RespCommand::Pexpireat => {
        // 绝对 Unix 毫秒 → ticks（KeyAdminCommands.cs:426 的 PEXPIREAT，
        // 负夹 0/上界钳制单点与命令端同函数）。
        // 刻意差异声明：C# EXPIRE 族条件（NX/XX/GT/LT）经 ExpirationWithOption
        // word 低 4 位随 AOF 完整携带、副本端重评估（UnifiedStore/PrivateMethods.cs:
        // 108 WriteLogRMW 置 Deterministic 后条件语义仍在）；rust AOF 条目由
        // TtlWrite 镜像产出，仅携带主端线性化后的绝对毫秒、不携带 ExpireOption，
        // 故重放按无条件绝对过期执行（TtlOpt::NONE）——主端写事件已按条件裁决，
        // 副本端丢失条件不影响镜像一致性，但携带面弱于 C# word 编码
        session
          .expire_at_ticks(key, expire_at_milliseconds_to_ticks(input.arg1))
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
        let ticks = duration_seconds_to_ticks(input.arg1.max(0));
        session
          .setex(key, val, ticks)
          .await
          .map_err(|e| format!("Setex replay failed: {e}"))?;
        return Ok(());
      }
      RespCommand::Psetex => {
        let val = input.args.first().map_or(&[][..], Vec::as_slice);
        let ticks = duration_milliseconds_to_ticks(input.arg1.max(0));
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
    // 对象信封：[tag u8][payload]（C# 值 = 对象存序列化器位码；
    // wkv 信封以类型标签 + 剥壳载荷存储，单点见 wcol obj_encode/obj_decode）
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
          // 信封载荷值首字节即内层对象类型标签（对象记录挂 ObjectEnvelope 物理域）
          let tag_opt = session
            .read_tag_with(key, KeyTag::ObjectEnvelope, |v| v.first().copied())
            .await
            .ok()
            .flatten()
            .flatten();
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

    // 信封域读现载荷（记录挂 ObjectEnvelope 物理键），单通道按类型分发
    let raw = session
      .read_tag_with(key, KeyTag::ObjectEnvelope, |r| r.to_vec())
      .await
      .map_err(AofReplayError::Store)?;
    match obj_type {
      GarnetObjectType::Hash => {
        replay_object_channel::<HashObject, D>(
          session,
          key,
          raw,
          &obj_input,
          &mut out,
          resp_version,
        )
        .await
      }
      GarnetObjectType::Set => {
        replay_object_channel::<SetObject, D>(session, key, raw, &obj_input, &mut out, resp_version)
          .await
      }
      GarnetObjectType::List => {
        replay_object_channel::<ListObject, D>(
          session,
          key,
          raw,
          &obj_input,
          &mut out,
          resp_version,
        )
        .await
      }
      GarnetObjectType::SortedSet => {
        replay_object_channel::<SortedSetObject, D>(
          session,
          key,
          raw,
          &obj_input,
          &mut out,
          resp_version,
        )
        .await
      }
      _ => Ok(()),
    }
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
    // 统一存字符串面上 upsert 与主存 upsert 同形（wkv 单一面）。C# 的
    // RENAME 向量特例（arg1 == VectorManager.RecordType → HandleVectorSet-
    // RenameCopy）在本仓不落地：向量索引记录驻留向量域登记表（不入 wkv
    // 值域），RENAME 条目不携带向量语义，副本侧经 VADD 条目重放重建索引
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

  /// libs/server/AOF/AofProcessor.ChunkReplay.cs:ShouldSkipRecord
  ///
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
  #[inline]
  pub const fn is_old_version_record(header: &AofHeader, store_version: i64) -> bool {
    header.store_version < store_version
  }

  /// libs/server/AOF/AofProcessor.cs:IsNewVersionRecord
  #[inline]
  pub const fn is_new_version_record(header: &AofHeader, store_version: i64) -> bool {
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
    // 序列号单点：分片形态取内嵌，其余取条目地址
    let sequence_number = AofHeader::sequence_number_of(entry, entry_address)?;
    match header.header_type()? {
      AofHeaderType::BasicHeader | AofHeaderType::BasicChunkHeader => {
        let op_type = AofEntryType::try_from(header.op_type).ok()?;
        if !op_type.has_key() {
          return Some((true, sequence_number));
        }
        let routing = if header.is_chunked() {
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
          sequence_number,
        ))
      }
      AofHeaderType::ShardedHeader | AofHeaderType::ShardedChunkHeader => {
        let op_type = AofEntryType::try_from(header.op_type).ok()?;
        if !op_type.has_key() {
          return Some((replay_task_idx == 0, sequence_number));
        }
        let offset = AofHeader::skip_header(entry)?;
        let len = u32::from_le_bytes(*entry.get(offset..)?.first_chunk::<4>()?) as usize;
        let key = entry.get(offset + 4..offset + 4 + len)?;
        Some((
          replay_task_idx == log.get_replay_task_idx(GarnetLog::hash(key)),
          sequence_number,
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
    // 序列号单点：分片形态取内嵌（含 ShardedLogTransactionHeader，对齐
    // C# SkipReplay 的 txnHeader.shardedHeader.sequenceNumber 分支），其余取条目地址
    let sequence_number = AofHeader::sequence_number_of(entry, log_address_sequence_number)?;
    Some((sequence_number > until_sequence_number, sequence_number))
  }

  /// 条目 key 速览（事务组加锁集提取面；不在场返回 None）。
  pub fn peek_entry_key(entry: &[u8]) -> Option<&[u8]> {
    let offset = AofHeader::skip_header(entry)?;
    let len = u32::from_le_bytes(*entry.get(offset..)?.first_chunk::<4>()?) as usize;
    entry.get(offset + 4..offset + 4 + len)
  }
}

/// 四对象类型重放单通道约束（本地封闭 trait，仅 Hash/Set/List/SortedSet 实现；
/// 对标 C# AofProcessor.ObjectStoreRMW<TObjectContext> 经 Tsavorite
/// objectContext 的泛型单通道，静态分发无 dyn）。
trait ReplayObject: Default {
  /// 信封内层类型标签（GarnetObjectType 判别值）
  const TAG: u8;

  /// 从信封载荷装载（损坏按空对象，对齐 from_blob 口径）
  fn load(raw: &[u8]) -> Self;

  /// 序列化为信封载荷
  fn dump(&self) -> Vec<u8>;

  /// 空对象判定（删空自愈阈值）
  fn is_empty(&self) -> bool;

  /// RESP 语义操作（委托各对象固有 operate）
  fn apply(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) -> bool;
}

impl ReplayObject for HashObject {
  const TAG: u8 = GarnetObjectType::Hash as u8;

  #[inline]
  fn load(raw: &[u8]) -> Self {
    Self::deserialize_from_slice(raw).unwrap_or_default()
  }

  #[inline]
  fn dump(&self) -> Vec<u8> {
    self.serialize_to_vec()
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.is_empty()
  }

  #[inline]
  fn apply(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) -> bool {
    self.operate(input, output, resp_protocol_version)
  }
}

impl ReplayObject for SetObject {
  const TAG: u8 = GarnetObjectType::Set as u8;

  #[inline]
  fn load(raw: &[u8]) -> Self {
    Self::deserialize_from_slice(raw).unwrap_or_default()
  }

  #[inline]
  fn dump(&self) -> Vec<u8> {
    self.serialize_to_vec()
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.is_empty()
  }

  #[inline]
  fn apply(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) -> bool {
    self.operate(input, output, resp_protocol_version)
  }
}

impl ReplayObject for ListObject {
  const TAG: u8 = GarnetObjectType::List as u8;

  #[inline]
  fn load(raw: &[u8]) -> Self {
    Self::deserialize_from_slice(raw).unwrap_or_default()
  }

  #[inline]
  fn dump(&self) -> Vec<u8> {
    self.serialize_to_vec()
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.is_empty()
  }

  #[inline]
  fn apply(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) -> bool {
    self.operate(input, output, resp_protocol_version)
  }
}

impl ReplayObject for SortedSetObject {
  const TAG: u8 = GarnetObjectType::SortedSet as u8;

  #[inline]
  fn load(raw: &[u8]) -> Self {
    Self::deserialize_from_slice(raw).unwrap_or_default()
  }

  #[inline]
  fn dump(&self) -> Vec<u8> {
    self.serialize_to_vec()
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.is_empty()
  }

  #[inline]
  fn apply(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) -> bool {
    self.operate(input, output, resp_protocol_version)
  }
}

/// 整对象回放泛型单通道（C# AofProcessor.ObjectStoreRMW 经 Tsavorite
/// objectContext 多态的对象应用段，rust 侧以 [`ReplayObject`] 静态分发：
/// 信封域现载荷 → [`ReplayObject::load`] → [`ReplayObject::apply`] →
/// 删空走双域删除自愈，非空回写信封；键缺失按空对象重建，与 ObjectStoreRMW
/// 重放会话 NeedToCreate=true 口径一致）。
async fn replay_object_channel<T: ReplayObject, D: Device>(
  session: &StorageSession<'_, D>,
  key: &[u8],
  raw: Option<Vec<u8>>,
  obj_input: &ObjectInput,
  out: &mut ObjectOutput,
  resp_version: u8,
) -> Result<(), AofReplayError> {
  let mut obj = raw
    .as_deref()
    .and_then(|r| obj_decode(r, T::TAG))
    .map(T::load)
    .unwrap_or_default();
  obj.apply(obj_input, out, resp_version);
  if obj.is_empty() {
    session
      .delete_string(key)
      .await
      .map_err(AofReplayError::Store)?;
  } else {
    let blob = obj.dump();
    session
      .obj_save(key, T::TAG, &blob)
      .await
      .map_err(AofReplayError::Store)?;
  }
  Ok(())
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
    let (ns, db, _, user_key) =
      NamespaceDbCodec::decode_tagged_key(key).map_err(|e| format!("AOF 条目物理键损坏: {e}"))?;
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
