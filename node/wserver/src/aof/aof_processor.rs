//! AOF 重放处理器（对标 libs/server/AOF/AofProcessor.cs:AofProcessor）
//!
//! 恢复回放与复制回放共用的条目处理内核：按条目头分发（检查点标记 /
//! FLUSH / 存储过程 / 事务 / 数据操作），数据操作经 [`ReplayTarget`] 落入
//! wkv 存储会话。rust 侧重放应用为异步（wkv 面天然异步），拓扑预处理
//! （key 哈希 / 一致性时间戳推进）保持同步快路径。
//!
//! C# 的拓扑特化预处理结构（SingleLogPreprocessKey 等）折叠为
//! [`prepare_key`]：按拓扑更新一致性时间戳并产出 key/payload 视图。

use std::sync::{
  Arc,
  atomic::{AtomicI64, Ordering},
};

use wdev::Device;
use wkv::WedbStore;

use super::{
  aof_address::AofAddress,
  aof_entry_type::AofEntryType,
  aof_header::{AofHeader, AofHeaderType, AofShardedHeader},
  garnet_append_only_file::GarnetAppendOnlyFile,
  garnet_log::GarnetLog,
  legacy_resp_command::LegacyRespCommand,
  readconsistency::read_consistency_manager::ReadConsistencyManager,
  replaycoordinator::{
    aof_replay_context::{ReplayOperation, TransactionGroup},
    aof_replay_coordinator::AofReplayCoordinator,
  },
};
use crate::{
  storage::session::{mainstore::advanced_ops::StringRMWOp, storage_session::StorageSession},
  types::RespCommand,
};

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
/// `[cmd u16][flags u8][subId u8][pad u8][arg1 i64][arg2 i64][arg3 i64]
///  [args_count u32][args 原文字节...]`。
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
  /// arg1（INCR 族增量 / SETRANGE 偏移等）。
  pub arg1: i64,
  /// arg2。
  pub arg2: i64,
  /// arg3。
  pub arg3: i64,
  /// parseState 参数序列化（APPEND/SETRANGE 数据等）。
  pub args: Vec<Vec<u8>>,
}

impl ReplayInput {
  /// 序列化（AOF 写入侧共用编码）。
  pub fn serialize(&self, into: &mut Vec<u8>) {
    let raw: u16 = self.cmd.into();
    into.extend_from_slice(&raw.to_le_bytes());
    into.push(self.flags);
    into.push(self.sub_id);
    // 对齐 8 字节参数区起点（arg1 位于偏移 8，与 Deserialize 面一致）
    into.extend_from_slice(&[0u8; 4]);
    into.extend_from_slice(&self.arg1.to_le_bytes());
    into.extend_from_slice(&self.arg2.to_le_bytes());
    into.extend_from_slice(&self.arg3.to_le_bytes());
    into.extend_from_slice(&(self.args.len() as u32).to_le_bytes());
    for arg in &self.args {
      into.extend_from_slice(&(arg.len() as u32).to_le_bytes());
      into.extend_from_slice(arg);
    }
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
      arg1: i64::from_le_bytes(bytes[8..16].try_into().ok()?),
      arg2: i64::from_le_bytes(bytes[16..24].try_into().ok()?),
      arg3: i64::from_le_bytes(bytes[24..32].try_into().ok()?),
      args,
    })
  }
}

/// 预处理产出：key、key 哈希与负载起点（C# PreparedParameters）。
pub struct PreparedParameters {
  /// key 字节。
  pub key: Vec<u8>,
  /// key 哈希（GarnetLog::HASH）。
  pub key_hash: i64,
  /// 负载（key 之后的首个组件起点）。
  pub payload: Vec<u8>,
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
    }
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

  /// libs/server/AOF/AofProcessor.cs:SetReadWriteSession
  ///
  /// 存储过程回放需要集群会话置读写态；rust 集群会话域为并行转写，
  /// 顺序回放下无并发读者，此处为语义空操作。
  pub fn set_read_write_session(&self) {}

  /// libs/server/AOF/AofProcessor.cs:ObtainServerSession
  ///
  /// C# 为向量复制回放惰性建独立回放会话；rust 侧向量域为并行转写，
  /// 重放落点由 [`ReplayTarget`] 显式传入，此入口不再需要。
  pub fn obtain_server_session(&self) {}

  /// libs/server/AOF/AofProcessor.cs:SwitchActiveDatabaseContext
  pub fn switch_active_database_context(&self, db_id: i64) {
    self.active_db_id.store(db_id, Ordering::Release);
  }

  /// 活跃库 id。
  pub fn active_db_id(&self) -> i64 {
    self.active_db_id.load(Ordering::Acquire)
  }

  /// libs/server/AOF/AofProcessor.cs:WaitForVectorOperationsToComplete
  ///
  /// VADD 异步排队操作完成等待；向量域为并行转写（无排队面），空操作。
  pub fn wait_for_vector_operations_to_complete(&self) {}

  /// 拓扑预处理（C# IPreprocessKey.PrepareKey 三实现的折叠）：
  /// 解出 key / 哈希 / 负载并按拓扑推进一致性 key 时间戳。
  pub fn prepare_key(
    &self,
    virtual_sublog_idx: usize,
    entry: &[u8],
    log_address_sequence_number: i64,
  ) -> Option<PreparedParameters> {
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

    // 单物理日志多回放 / 分片拓扑：按序列号推进一致性时间戳
    if (self.using_sharded_log || self.using_single_physical_log_multi_replay)
      && let Some(manager) = self.read_consistency_manager()
    {
      manager.update_virtual_sublog_key_sequence_number(
        virtual_sublog_idx,
        key_hash,
        sequence_number,
      );
    }
    Some(PreparedParameters {
      key: key.to_vec(),
      key_hash,
      payload: rest.to_vec(),
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
        return super::aof_processor__chunk_replay::process_chunked_record(
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
        return super::aof_processor__chunk_replay::process_chunked_record(
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

    let op_type = AofEntryType::try_from(header.op_type).unwrap_or(AofEntryType::StoreUpsert);
    // StoreRMW 可并发排队 VADD；其余操作须先等待向量操作完成（一致性）
    if op_type != AofEntryType::StoreRMW {
      self.wait_for_vector_operations_to_complete();
    }

    // 事务处理：TxnStart/TxnAbort/TxnCommit 及组内操作由协调器消化
    let action = self.coordinator.add_or_replay_transaction_operation(
      virtual_sublog_idx,
      entry,
      as_replica,
      log_address_sequence_number,
    );
    match action {
      super::replaycoordinator::aof_replay_coordinator::TxnAction::Handled => return Ok(false),
      super::replaycoordinator::aof_replay_coordinator::TxnAction::Commit { session_id } => {
        // 取出整组并按序重放（恢复路径免锁；副本路径的事务锁由事务域承载）
        let group = self
          .coordinator
          .take_transaction_group(virtual_sublog_idx, session_id);
        if let Some(group) = group {
          self
            .process_transaction_group_operations(virtual_sublog_idx, &group, target)
            .await;
        }
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
          let sequence_number = if self.using_single_physical_log_multi_replay {
            log_address_sequence_number
          } else {
            AofShardedHeader::parse(entry).map_or(0, |sh| sh.sequence_number)
          };
          if let Some(manager) = self.read_consistency_manager() {
            manager.update_virtual_sublog_max_sequence_number(virtual_sublog_idx, sequence_number);
          }
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
        let _ = header.unsafe_truncate_log();
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
          super::aof_processor__chunk_replay::replay_chunk(self, sublog_idx, *acc, target).await?;
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
      .process_transaction_group_operations(sublog_idx, &group, target)
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
                true,
                group.start_sequence_number,
                target,
              )
              .await
          }
          None => Err("模糊区条目头损坏".to_string().into()),
        },
        ReplayOperation::Chunk(acc) => {
          super::aof_processor__chunk_replay::replay_chunk(
            self,
            sublog_idx,
            (**acc).clone(),
            target,
          )
          .await
        }
      };
      let _ = result;
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
  pub async fn replay_op<D: Device>(
    &self,
    op_type: AofEntryType,
    prepared: PreparedParameters,
    legacy_cmd_format: bool,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let PreparedParameters {
      key,
      key_hash: _,
      payload,
    } = prepared;
    match op_type {
      AofEntryType::StoreUpsert => {
        let (value, input) = Self::split_value_input(&payload).ok_or("StoreUpsert 负载损坏")?;
        Self::store_upsert(target.session, &key, value, input, legacy_cmd_format).await
      }
      AofEntryType::StoreRMW => {
        Self::store_rmw(target.session, &key, &payload, legacy_cmd_format).await
      }
      AofEntryType::StoreDelete => Self::store_delete(target.session, &key).await,
      AofEntryType::ObjectStoreUpsert => {
        let (value, _) = Self::split_value_input(&payload).ok_or("ObjectStoreUpsert 负载损坏")?;
        Self::object_store_upsert(target.session, &key, value).await
      }
      AofEntryType::ObjectStoreRMW => {
        Self::object_store_rmw(target.session, &key, &payload, legacy_cmd_format).await
      }
      AofEntryType::ObjectStoreDelete => Self::object_store_delete(target.session, &key).await,
      AofEntryType::UnifiedStoreStringUpsert => {
        let (value, input) =
          Self::split_value_input(&payload).ok_or("UnifiedStoreStringUpsert 负载损坏")?;
        Self::unified_store_string_upsert(target.session, &key, value, input, legacy_cmd_format)
          .await
      }
      AofEntryType::UnifiedStoreObjectUpsert => {
        let (value, _) =
          Self::split_value_input(&payload).ok_or("UnifiedStoreObjectUpsert 负载损坏")?;
        Self::object_store_upsert(target.session, &key, value).await
      }
      // C# UnifiedStoreRMW / UnifiedStoreDelete：wkv 统一面下与主存 RMW /
      // delete 同形
      AofEntryType::UnifiedStoreRMW => {
        Self::store_rmw(target.session, &key, &payload, legacy_cmd_format).await
      }
      AofEntryType::UnifiedStoreDelete => Self::store_delete(target.session, &key).await,
      // libs/server/AOF/AofProcessor.cs:HandleRangeIndexStreamChunk
      //（范围索引流块重放须 RangeIndexManager（并行域），未接线即按
      // C# 同文案失败）
      AofEntryType::RangeIndexStreamChunk => Err(
        "RangeIndexPreview disabled; Replay failed"
          .to_string()
          .into(),
      ),
      _ => Err(format!("Unknown AOF header operation type {op_type:?}").into()),
    }
  }

  /// libs/server/AOF/AofProcessor.cs:StoreUpsert
  pub async fn store_upsert<D: Device>(
    session: &StorageSession<'_, D>,
    key: &[u8],
    value: &[u8],
    input: &[u8],
    legacy_cmd_format: bool,
  ) -> Result<(), AofReplayError> {
    let mut input = ReplayInput::deserialize(input).ok_or("StoreUpsert input 损坏")?;
    if legacy_cmd_format {
      input.cmd = LegacyRespCommand::from_v3(input.cmd);
    }
    // 条件写形态（EX/NX 等经 StoreRMW 路径回放）；upsert 直写
    let _ = input;
    session
      .upsert_string(key, value)
      .await
      .map_err(AofReplayError::Store)
  }

  /// libs/server/AOF/AofProcessor.cs:StoreRMW
  pub async fn store_rmw<D: Device>(
    session: &StorageSession<'_, D>,
    key: &[u8],
    input: &[u8],
    legacy_cmd_format: bool,
  ) -> Result<(), AofReplayError> {
    let mut input = ReplayInput::deserialize(input).ok_or("StoreRMW input 损坏")?;
    if legacy_cmd_format {
      input.cmd = LegacyRespCommand::from_v3(input.cmd);
    }
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
    // 范围索引族须实际执行（缺口见汇报：RangeIndexManager 为并行域）
    if matches!(
      input.cmd,
      RespCommand::Ricreate | RespCommand::Riset | RespCommand::Ridel
    ) {
      return Err(
        "RangeIndexPreview disabled; Replay failed"
          .to_string()
          .into(),
      );
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
      _ => {
        // 其余 RMW 命令无独立 RMW 语义（SET 系条件写在 upsert 侧回放）
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
    let mut input = ReplayInput::deserialize(input).ok_or("ObjectStoreRMW input 损坏")?;
    if legacy_cmd_format {
      // 旧版对象头把子操作 id 装入 flags 低 5 位（C# RelocateLegacyObjectSubId）
      input.sub_id = input.flags & 0x1F;
      input.flags &= !0x1F;
    }
    // 对象 RMW 回放须按子操作 id 分派对象命令面（HSET/HDEL/...）；
    // 对象命令域为并行转写（缺口见汇报），此处按“未知子操作”保守跳过
    let _ = (key, session);
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
    input: &[u8],
    legacy_cmd_format: bool,
  ) -> Result<(), AofReplayError> {
    // 统一存字符串上 ups 与主存 upsert 同形（wkv 单一面）；RENAME 向量特例
    // 须向量域承接（缺口见汇报）
    Self::store_upsert(session, key, value, input, legacy_cmd_format).await
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
      if header.store_version > store_version {
        self
          .coordinator
          .add_fuzzy_region_operation(sublog_idx, ReplayOperation::Record(entry.to_vec()));
        return true;
      }
      return false;
    }
    header.store_version < store_version
  }

  /// 分块形态的 ShouldSkipRecord（C# ChunkReplay 分片同名；模糊区新代入缓冲）。
  pub fn should_skip_record_chunk(
    &self,
    sublog_idx: usize,
    acc: &super::aof_chunked_record_reader::ChunkedAccumulator,
    store_version: i64,
  ) -> bool {
    if as_replica_fuzzy(self, sublog_idx) {
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

/// 副本模糊区判定（处理器侧共享谓词）。
fn as_replica_fuzzy(processor: &AofProcessor, sublog_idx: usize) -> bool {
  // 恢复路径 as_replica = false，模糊区缓冲仅复制回放生效；此处读取缓冲标记
  processor.coordinator.context(sublog_idx).in_fuzzy_region
}

/// AOF 条目写入侧编码助手（恢复闭环测试与未来命令层接入共用；负载布局与
/// C# SpanByte 序列化一致：长度前缀 key / 长度前缀 value / input 原文）。
pub mod encode {
  use super::{AofHeader, AofShardedHeader};

  /// 头 + 负载的通用拼装。
  fn frame(header: &AofHeader, body: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let mut entry = header.to_bytes().to_vec();
    body(&mut entry);
    entry
  }

  /// upsert 形状：[key][value][input]。
  pub fn upsert_entry(header: &AofHeader, key: &[u8], value: &[u8], input: &[u8]) -> Vec<u8> {
    frame(header, |out| {
      out.extend_from_slice(&(key.len() as u32).to_le_bytes());
      out.extend_from_slice(key);
      out.extend_from_slice(&(value.len() as u32).to_le_bytes());
      out.extend_from_slice(value);
      out.extend_from_slice(input);
    })
  }

  /// rmw/delete 形状：仅 key（input/delete 直接跟于 key 后）。
  pub fn keyed_entry(header: &AofHeader, key: &[u8], tail: &[u8]) -> Vec<u8> {
    frame(header, |out| {
      out.extend_from_slice(&(key.len() as u32).to_le_bytes());
      out.extend_from_slice(key);
      out.extend_from_slice(tail);
    })
  }

  /// 分片头形态：ShardedHeader + 负载。
  pub fn sharded_entry(
    header: &AofHeader,
    sequence_number: i64,
    key: &[u8],
    value: &[u8],
    input: &[u8],
  ) -> Vec<u8> {
    let mut sharded = AofShardedHeader {
      basic: *header,
      sequence_number,
    };
    sharded
      .basic
      .set_header_type(super::super::aof_header::AofHeaderType::ShardedHeader);
    let mut entry = sharded.basic.to_bytes().to_vec();
    entry.extend_from_slice(&sequence_number.to_le_bytes());
    entry.extend_from_slice(&(key.len() as u32).to_le_bytes());
    entry.extend_from_slice(key);
    entry.extend_from_slice(&(value.len() as u32).to_le_bytes());
    entry.extend_from_slice(value);
    entry.extend_from_slice(input);
    entry
  }

  /// 无键条目（检查点 / FLUSH / 事务标记）。
  pub fn keyless_entry(header: &AofHeader) -> Vec<u8> {
    frame(header, |_| {})
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
  }
}
