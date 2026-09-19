//! 范围索引 AOF 复制面（对标 libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs）
//!
//! 迁移索引经 AOF 流复制：源侧把快照文件灌成
//! [`AofEntryType::RangeIndexStreamChunk`] 块序列（RI.SET / RI.DEL 走
//! StoreRMW 直写入队）；副本与崩溃恢复侧按键重组流块，完成后把重组成的
//! BfTree 经 `publish_migrated_range_index` 发布。逐键进行中的重组状态
//! （C# rangeIndexAofStreamReassembly）由本域持有。
//!
//! 同一通道亦承载集合就地升阶（promote）的树数据回放：升阶把整树快照灌入本
//! 流，发布判别类型（GarnetObjectType）随首块 ReplayInput.obj_type 携带，副本
//! 完成时据此重建 MetaValue（避免升阶键被误标为 RangeIndex）。
//!
//! AOF 条目编码：input 段为 [`ReplayInput`] 序列化（C# StringInput 的
//! 反序列化形态，aof 域已定）；流块首尾标志打包在 arg1。

use std::{
  fs, io,
  path::{Path, PathBuf},
  sync::{
    Arc,
    atomic::{AtomicU8, AtomicUsize, Ordering},
  },
};

use parking_lot::Mutex;
use waof::AofEntryType;
use wbase::map::{ConcurrentMap, new_concurrent_map};
use wbftree::{
  DEFAULT_FILE_READ_BUFFER_SIZE, DEFAULT_MIGRATION_CHUNK_SIZE, Error as BfTreeError,
  RANGE_INDEX_STUB_SIZE, RangeIndexChunkedDeserializer,
  RangeIndexChunkedSerializer, RangeIndexManager as Engine, RangeIndexMigrationReader,
  RangeIndexStub, StorageBackendType, TreeTuning,
};
use wdev::Device;
use wkv::{RangeIndexError, StoreSession};
use wresp::command::RespCommand;
use wval::GarnetObjectType;

use crate::{
  aof::{
    AofWriteContext,
    garnet_append_only_file::GarnetAppendOnlyFile,
    replay_input::{ReplayInput, ReplayInputSlice},
  },
  rangeindex::{
    range_index_manager_migration::{PublishMigratedIndexResult, RangeIndexManagerMigration},
    range_index_replication_activities::{ReassemblyActivity, StreamActivity},
  },
};

/// 复制面错误（C# 以 GarnetException 上抛的会话侧失败）
#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
  /// 动态协议文案（损坏条目 / 状态机违例等携带上下文快照的失败）
  #[error("RangeIndex replication: {0}")]
  Msg(String),

  /// 文件 I/O 失败
  #[error(transparent)]
  Io(#[from] io::Error),

  /// BfTree 存根解码与分块流错误
  #[error("RangeIndex BfTree: {0}")]
  BfTree(#[from] BfTreeError),

  /// 存储引擎 RangeIndex 错误（AOF 回放透传）
  #[error(transparent)]
  RangeIndex(#[from] RangeIndexError),

  /// AOF 写入错误
  #[error("RangeIndex AOF: {0}")]
  Aof(#[from] waof::Error),
}

impl From<String> for ReplicationError {
  fn from(message: String) -> Self {
    Self::Msg(message)
  }
}

impl From<&str> for ReplicationError {
  fn from(message: &str) -> Self {
    Self::Msg(message.to_string())
  }
}

type ReplicationResult = Result<(), ReplicationError>;

/// 流式灌入 AOF 的参数包
pub struct RangeIndexStreamArgs<'a> {
  pub key: &'a [u8],
  pub stub: &'a [u8],
  pub file_path: &'a Path,
  pub ctx: AofWriteContext,
  pub chunk_size: usize,
  /// 发布后元记录的集合判别类型（GarnetObjectType 判别值）：随首块 ReplayInput
  /// 携带，副本重组完成时据此重建 MetaValue（用户 RI 迁移为 RangeIndex，集合
  /// 就地升阶回放为原集合类型）
  pub obj_type: u8,
}

/// 流块入队入参结构体（收敛 enqueue_range_index_stream_chunk 入参）
pub struct RangeIndexChunkArgs<'a> {
  pub key: &'a [u8],
  pub obj_type: u8,
  pub chunk: &'a [u8],
  pub is_first: bool,
  pub is_last: bool,
}

/// 流块 arg1 首块标志位
const STREAM_CHUNK_IS_FIRST_FLAG: i64 = 2;
/// 流块 arg1 末块标志位
const STREAM_CHUNK_IS_LAST_FLAG: i64 = 1;

/// 打包流块首尾标志进 arg1
#[inline]
pub(crate) const fn pack_stream_chunk_flags(is_first: bool, is_last: bool) -> i64 {
  ((is_last as i64) * STREAM_CHUNK_IS_LAST_FLAG) | ((is_first as i64) * STREAM_CHUNK_IS_FIRST_FLAG)
}

/// 解包流块 arg1 为 (is_first, is_last)
#[inline]
pub(crate) const fn unpack_stream_chunk_flags(arg1: i64) -> (bool, bool) {
  (
    arg1 & STREAM_CHUNK_IS_FIRST_FLAG != 0,
    arg1 & STREAM_CHUNK_IS_LAST_FLAG != 0,
  )
}

/// 逐键 AOF 流重组状态：重组反序列化器 + 追踪活动 + 首块携带的发布判别类型
/// （C# StreamReassemblyState；字段互斥访问经 parking_lot::Mutex，
/// 发布 await 前必须放锁）
struct StreamReassemblyState {
  deserializer: Mutex<RangeIndexChunkedDeserializer>,
  activity: Mutex<ReassemblyActivity>,
  /// 发布后元记录集合判别（首块 ReplayInput.obj_type 捕获；0 = 未携带，完成时归一为 RangeIndex）
  obj_type: AtomicU8,
}

impl StreamReassemblyState {
  fn new(temp_path: PathBuf) -> Result<Self, ReplicationError> {
    Ok(Self {
      deserializer: Mutex::new(RangeIndexChunkedDeserializer::new(temp_path)?),
      activity: Mutex::new(ReassemblyActivity::start_activity()),
      obj_type: AtomicU8::new(0),
    })
  }
}

/// AOF 复制面（C# partial RangeIndexManager 的 Replication 分片）
pub struct RangeIndexManagerReplication {
  /// 引擎管理器（派生迁移临时文件路径 / 发布用键条带锁）
  engine: Arc<Engine>,
  /// 迁移流 AOF 分块大小（恒为默认 256KB；rust AOF 记录为显式入队，
  /// 无 C# RMW 自动记日志需哨兵抑制的调小演练场景）
  aof_stream_chunk_size: AtomicUsize,
  /// 进行中逐键流重组状态（键字节 → 状态）
  reassembly: ConcurrentMap<Vec<u8>, Arc<StreamReassemblyState>>,
}

impl RangeIndexManagerReplication {
  /// 绑定引擎实例创建复制面（分块大小默认 256KB）
  pub fn new(engine: Arc<Engine>) -> Self {
    Self {
      engine,
      aof_stream_chunk_size: AtomicUsize::new(DEFAULT_MIGRATION_CHUNK_SIZE),
      reassembly: new_concurrent_map(),
    }
  }

  /// 当前 AOF 流分块大小
  #[inline]
  pub fn aof_stream_chunk_size(&self) -> usize {
    self.aof_stream_chunk_size.load(Ordering::Acquire)
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:HandleRangeIndexCreateReplay
  ///
  /// RI.CREATE AOF 回放：从条目内的存根字节提取 BfTree 配置（含原进程的
  /// 陈旧树句柄），以新引擎实例重建（树句柄由引擎写回存根落盘）。键已存在
  /// （检查点恢复后重复 RI.CREATE）按 C# 语义静默接受——重复条目不再重建
  pub async fn handle_range_index_create_replay<D: Device>(
    &self,
    session: &StoreSession<D>,
    key: &[u8],
    input: &ReplayInput,
  ) -> ReplicationResult {
    let Some(stub_bytes) = input.args.first() else {
      return Err(ReplicationError::Msg(format!(
        "Corrupt RI.CREATE AOF entry: no stub argument (expected {RANGE_INDEX_STUB_SIZE} bytes)"
      )));
    };
    if stub_bytes.len() != RANGE_INDEX_STUB_SIZE {
      return Err(ReplicationError::Msg(format!(
        "Corrupt RI.CREATE AOF entry: stub size {}, expected {RANGE_INDEX_STUB_SIZE}",
        stub_bytes.len()
      )));
    }
    let stub = RangeIndexStub::decode(stub_bytes)?;
    let tuning = TreeTuning::from(&stub);
    let backend = StorageBackendType::from_u8(stub.storage_backend);
    // 已存在 = 重复回放（引擎在登记冲突时不会留下半初始化实例，等价 C#
    // 侧"新建后丢弃"）；WrongType = 回放位被非索引记录占用（C# 该处 RMW 置
    // WrongType 动作后静默丢弃新建的 BfTree），其余错误上抛
    match session.range_index_create(key, backend, tuning).await {
      Ok(()) => Ok(()),
      Err(RangeIndexError::AlreadyExists | RangeIndexError::WrongType) => Ok(()),
      Err(e) => Err(e.into()),
    }
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:HandleRangeIndexSetReplay
  ///
  /// RI.SET AOF 回放：读存根取在线树后原生插入。键缺失 / 类型不符静默
  /// 跳过（C# status != OK 即 return）；插入被拒即主从发散，上抛
  pub async fn handle_range_index_set_replay<D: Device>(
    &self,
    session: &StoreSession<D>,
    key: &[u8],
    input: &ReplayInput,
  ) -> ReplicationResult {
    let (Some(field), Some(value)) = (input.args.first(), input.args.get(1)) else {
      return Err(ReplicationError::Msg(
        "Corrupt RI.SET AOF entry: field/value arguments missing".to_string(),
      ));
    };
    match session.range_index_set(key, field, value).await {
      Ok(()) => Ok(()),
      // NOTFOUND / WRONGTYPE：回放面静默跳过（C# 提前 return 口径）
      Err(RangeIndexError::NotFound | RangeIndexError::WrongType) => Ok(()),
      Err(e) => Err(e.into()),
    }
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:HandleRangeIndexDelReplay
  ///
  /// RI.DEL AOF 回放：读存根取在线树后原生删除（口径同
  /// [`Self::handle_range_index_set_replay`]）
  pub async fn handle_range_index_del_replay<D: Device>(
    &self,
    session: &StoreSession<D>,
    key: &[u8],
    input: &ReplayInput,
  ) -> ReplicationResult {
    let Some(field) = input.args.first() else {
      return Err(ReplicationError::Msg(
        "Corrupt RI.DEL AOF entry: field argument missing".to_string(),
      ));
    };
    match session.range_index_del(key, field).await {
      Ok(_) => Ok(()),
      Err(RangeIndexError::NotFound | RangeIndexError::WrongType) => Ok(()),
      Err(e) => Err(e.into()),
    }
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:ReplicateRangeIndexStream
  ///
  /// 源侧：把迁移索引的序列化 BfTree 文件经迁移读取器分块读出，逐块以
  /// RangeIndexStreamChunk 灌入 AOF（首块 / 末块标志经 arg1 携带）。全程
  /// 以 StreamActivity 追踪（成功与失败路径均收口日志）
  pub fn replicate_range_index_stream(
    &self,
    args: RangeIndexStreamArgs<'_>,
    append_only_file: Option<&GarnetAppendOnlyFile>,
  ) -> ReplicationResult {
    let Some(aof) = append_only_file else {
      log::warn!("ReplicateRangeIndexStream called with null appendOnlyFile");
      return Ok(());
    };

    let key = args.key;
    let mut stream_activity = StreamActivity::start_activity(args.chunk_size);
    let run = self.stream_into_aof(aof, args, &mut stream_activity);
    if let Err(e) = &run {
      stream_activity.on_error(&e.to_string());
    }
    stream_activity.end_and_log(key);
    run
  }

  /// 流式灌入 AOF 的执行体（活动追踪在调用方收口）
  fn stream_into_aof(
    &self,
    aof: &GarnetAppendOnlyFile,
    args: RangeIndexStreamArgs<'_>,
    stream_activity: &mut StreamActivity,
  ) -> ReplicationResult {
    let file_len = fs::metadata(args.file_path)?.len();
    stream_activity.on_file_length(file_len as i64);
    let serializer = RangeIndexChunkedSerializer::new(args.key, args.stub, file_len);
    let file = fs::File::open(args.file_path)?;
    let mut reader =
      RangeIndexMigrationReader::new(serializer, file, None, DEFAULT_FILE_READ_BUFFER_SIZE)?;

    let mut dest = vec![0u8; args.chunk_size];
    let mut is_first = true;
    while !reader.is_complete() {
      let written = reader.read_next_chunk(&mut dest)?;
      // !is_complete 下 read_next_chunk 必有进展；0 字节即协议违例
      if written == 0 {
        stream_activity.on_error("ZeroLengthChunkFromReader");
        return Err(ReplicationError::Msg(
          "ReplicateRangeIndexStream: reader returned zero-length chunk while the stream is incomplete".to_string(),
        ));
      }
      self.enqueue_range_index_stream_chunk(
        aof,
        args.ctx,
        RangeIndexChunkArgs {
          key: args.key,
          obj_type: args.obj_type,
          chunk: &dest[..written],
          is_first,
          is_last: reader.is_complete(),
        },
      )?;
      stream_activity.on_chunk_enqueued(written);
      is_first = false;
    }
    Ok(())
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:EnqueueRangeIndexStreamChunk
  ///
  /// 入队单个 RangeIndexStreamChunk 块（首尾标志打包进 ReplayInput.arg1）
  pub fn enqueue_range_index_stream_chunk(
    &self,
    append_only_file: &GarnetAppendOnlyFile,
    ctx: AofWriteContext,
    args: RangeIndexChunkArgs<'_>,
  ) -> waof::Result<i64> {
    let payload = [args.chunk];
    let input = ReplayInputSlice::new_deterministic(RespCommand::None, &payload)
      .with_obj_type(args.obj_type)
      .with_args_num(pack_stream_chunk_flags(args.is_first, args.is_last), 0, 0);
    append_only_file.enqueue_rmw_slices(AofEntryType::RangeIndexStreamChunk, ctx, args.key, &input)
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:HandleRangeIndexStreamReplay
  ///
  /// AOF 回放（副本复制 / 崩溃恢复）单个流块：从 arg1 解出首尾标志后
  /// 交 [`Self::process_stream_chunk`]
  pub async fn handle_range_index_stream_replay<D: Device>(
    &self,
    session: &StoreSession<D>,
    key: &[u8],
    input: &ReplayInput,
  ) -> ReplicationResult {
    let Some(chunk) = input.args.first() else {
      return Err(ReplicationError::Msg(
        "Corrupt RangeIndexStreamChunk AOF entry: chunk argument missing".to_string(),
      ));
    };
    let (is_first, is_last) = unpack_stream_chunk_flags(input.arg1);
    self
      .process_stream_chunk(Some(session), key, input.obj_type, chunk, is_first, is_last)
      .await
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:PendingStreamReassemblyCount
  ///
  /// 进行中的逐键 AOF 范围索引流重组数量
  #[inline]
  pub fn pending_stream_reassembly_count(&self) -> usize {
    self.reassembly.pin().len()
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:ProcessStreamChunk
  ///
  /// 流重组核心步：首块重置同键陈旧状态 → 喂块给逐键反序列化器 → 流完成
  /// 时发布重组成的 BfTree。失败路径（喂块被拒 / 发布失败 / 末块后仍未
  /// 完成）一律清理重组状态后上抛
  pub async fn process_stream_chunk<D: Device>(
    &self,
    session: Option<&StoreSession<D>>,
    key: &[u8],
    obj_type: u8,
    chunk: &[u8],
    is_first: bool,
    is_last: bool,
  ) -> ReplicationResult {
    // 新流首块取代同键未完成的重组（连接串行发送同键流块，无并发窗口）
    if is_first {
      self.remove_and_dispose_stream_reassembly(key, "NewStreamReceived");
    }

    let state = self.get_or_create_reassembly(key)?;
    state.activity.lock().on_chunk_received(chunk.len());
    // 首块捕获发布判别类型（随 ReplayInput.obj_type 携带；同键流块同值）
    if is_first {
      state.obj_type.store(obj_type, Ordering::Relaxed);
    }

    let (accepted, complete, has_error, stub_bytes, temp_path, chunk_err) = {
      let mut deserializer = state.deserializer.lock();
      let res = deserializer.process_chunk(chunk);
      let err = res
        .as_ref()
        .err()
        .map(|e| e.to_string())
        .or_else(|| deserializer.take_error().map(|e| e.to_string()));
      (
        res.unwrap_or(false),
        deserializer.is_complete(),
        deserializer.has_error(),
        deserializer.stub().to_vec(),
        deserializer.temp_path().to_path_buf(),
        err,
      )
    };

    if !accepted || has_error {
      let detail = chunk_err.unwrap_or_else(|| "chunk processing failed".to_string());
      log::error!(
        "HandleRangeIndexStreamReplay: failed to process range index stream chunk for key {}: {detail}",
        String::from_utf8_lossy(key)
      );
      self.remove_and_dispose_stream_reassembly(key, "ChunkProcessingError");
      return Err(ReplicationError::Msg(format!(
        "HandleRangeIndexStreamReplay: failed to process range index stream chunk for key {}: {detail}",
        String::from_utf8_lossy(key)
      )));
    }

    if complete {
      let Some(session) = session else {
        self.remove_and_dispose_stream_reassembly(key, "NoSessionOnComplete");
        return Err(ReplicationError::Msg(
          "process_stream_chunk: complete stream received but no storage session provided"
            .to_string(),
        ));
      };
      // TODO(RangeIndex): 随流携带 replaceOption（C# 同 TODO：迁移流未编码
      // REPLACE 语义，固定 replace=false）
      // 首块携带的发布判别类型：0（旧迁移流未编码）归一为 RangeIndex，
      // 集合升阶流为原集合类型（Hash/Set/List/SortedSet）
      let obj_type = match GarnetObjectType::from_u8(state.obj_type.load(Ordering::Relaxed)) {
        Some(t) if t != GarnetObjectType::Null => t,
        _ => GarnetObjectType::RangeIndex,
      };
      let publish_result = RangeIndexManagerMigration::publish_migrated_index(
        session,
        key,
        &stub_bytes,
        &temp_path,
        false,
        obj_type,
      )
      .await;
      state.activity.lock().on_publish_result(publish_result);
      if publish_result == PublishMigratedIndexResult::Failed {
        log::error!(
          "HandleRangeIndexStreamReplay: PublishMigratedIndex failed during AOF replay for key {}",
          String::from_utf8_lossy(key)
        );
        self.remove_and_dispose_stream_reassembly(key, "PublishFailed");
        return Err(ReplicationError::Msg(format!(
          "HandleRangeIndexStreamReplay: PublishMigratedIndex failed during AOF replay for key {}",
          String::from_utf8_lossy(key)
        )));
      }
      self.remove_and_dispose_stream_reassembly(key, "Complete");
      return Ok(());
    }

    if is_last {
      // 末块标志已置但反序列化器未完成：流畸形 / 截断
      log::error!(
        "HandleRangeIndexStreamReplay: final range index stream chunk flag set but stream is incomplete for key {}",
        String::from_utf8_lossy(key)
      );
      self.remove_and_dispose_stream_reassembly(key, "FinalChunkButDeserializerIncomplete");
      return Err(ReplicationError::Msg(format!(
        "HandleRangeIndexStreamReplay: final range index stream chunk flag set but stream is incomplete for key {}",
        String::from_utf8_lossy(key)
      )));
    }

    Ok(())
  }

  /// 取或建逐键重组状态（缺省临时路径由引擎迁移临时目录派生，原子无竞争）
  fn get_or_create_reassembly(
    &self,
    key: &[u8],
  ) -> Result<Arc<StreamReassemblyState>, ReplicationError> {
    let pin = self.reassembly.pin();
    if let Some(state) = pin.get(key) {
      return Ok(Arc::clone(state));
    }
    let new_state = Arc::new(StreamReassemblyState::new(
      self.engine.derive_temp_migration_path(),
    )?);
    let state = pin.get_or_insert(key.to_vec(), new_state);
    Ok(Arc::clone(state))
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:DisposeIncompleteStreamReassembly
  ///
  /// 释放并丢弃所有进行中的 AOF 流重组状态（管理器注销、异常或停机时调用）。
  /// 逐键回收未完成流重组所占用的临时文件与临时树状态。
  pub fn dispose_incomplete_stream_reassembly(&self) {
    let pin = self.reassembly.pin();
    let keys: Vec<Vec<u8>> = pin.keys().cloned().collect();
    for key in keys {
      log::warn!(
        "DisposeIncompleteStreamReassembly: discarding incomplete range index stream reassembly for key {}",
        String::from_utf8_lossy(&key)
      );
      self.remove_and_dispose_stream_reassembly(&key, "CleanupIncomplete");
    }
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.cs:Dispose
  ///
  /// 释放复制面资源：丢弃未完成的流重组状态并释放底层引擎资源
  pub fn dispose(&self) {
    self.dispose_incomplete_stream_reassembly();
    self.engine.dispose();
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:RemoveAndDisposeStreamReassembly
  ///
  /// 摘除并释放指定键的重组状态（活动以 reason 收口日志、临时文件删除）
  pub fn remove_and_dispose_stream_reassembly(&self, key: &[u8], reason: &str) {
    if let Some(state) = self.reassembly.pin().remove(key) {
      state.activity.lock().end_and_log(key, reason);
      state.deserializer.lock().dispose();
    }
  }
}

impl Drop for RangeIndexManagerReplication {
  fn drop(&mut self) {
    self.dispose_incomplete_stream_reassembly();
  }
}
