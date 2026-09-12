//! 范围索引 AOF 复制面（对标 libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs）
//!
//! 迁移索引经 AOF 流复制：源侧把快照文件灌成
//! [`AofEntryType::RangeIndexStreamChunk`] 块序列（RI.SET / RI.DEL 走
//! StoreRMW 直写入队）；副本与崩溃恢复侧按键重组流块，完成后把重组成的
//! BfTree 经 `publish_migrated_range_index` 发布。逐键进行中的重组状态
//! （C# rangeIndexAofStreamReassembly）由本域持有。
//!
//! AOF 条目编码：input 段为 [`ReplayInput`] 序列化（C# StringInput 的
//! 反序列化形态，aof 域已定）；流块首尾标志打包在 arg1。

use std::{
  fs, io,
  path::{Path, PathBuf},
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
};

use parking_lot::Mutex;
use waof::AofEntryType;
use whasher::{GxPapayaMap, new_papaya_map};
use wkv::{
  RANGE_INDEX_STUB_SIZE, RangeIndexChunkedDeserializer, RangeIndexError,
  RangeIndexManager as Engine, RangeIndexStub, StorageBackend, StorageBackendType, TreeTuning,
};

use crate::{
  aof::{
    RangeIndexSessionFace,
    aof_processor::{ReplayInput, ReplayInputSlice},
    garnet_append_only_file::GarnetAppendOnlyFile,
    garnet_log::RecordShape,
  },
  resp::rangeindex::{
    range_index_chunked_serializer::{
      ChunkStreamError, MIN_CHUNK_SIZE, RangeIndexChunkedSerializer,
    },
    range_index_manager_migration::{
      DEFAULT_MIGRATION_CHUNK_SIZE, PublishMigratedIndexResult, RangeIndexManagerMigration,
    },
    range_index_migration_reader::{DEFAULT_FILE_READ_BUFFER_SIZE, RangeIndexMigrationReader},
    range_index_replication_activities::{ReassemblyActivity, StreamActivity},
  },
  types::{RespCommand, RespInputFlags},
};

/// 复制面错误（C# 以 GarnetException 上抛的会话侧失败）
#[derive(Debug, thiserror::Error)]
#[error("RangeIndex replication: {0}")]
pub struct ReplicationError(pub String);

impl From<String> for ReplicationError {
  fn from(message: String) -> Self {
    Self(message)
  }
}

impl From<&str> for ReplicationError {
  fn from(message: &str) -> Self {
    Self(message.to_string())
  }
}

impl From<io::Error> for ReplicationError {
  fn from(e: io::Error) -> Self {
    Self(e.to_string())
  }
}

impl From<ChunkStreamError> for ReplicationError {
  fn from(e: ChunkStreamError) -> Self {
    Self(e.to_string())
  }
}

type ReplicationResult = Result<(), ReplicationError>;

/// AOF 入队上下文（C# version/sessionId 参数对的域内承接）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AofWriteContext {
  /// 存储版本
  pub version: i64,
  /// 会话 id
  pub session_id: i32,
}

/// 流式灌入 AOF 的参数包
pub struct RangeIndexStreamArgs<'a> {
  pub key: &'a [u8],
  pub stub: &'a [u8],
  pub file_path: &'a Path,
  pub ctx: AofWriteContext,
  pub chunk_size: usize,
}

/// AOF 记入的确定性回放标志（C# RespInputFlags.Deterministic）
#[inline]
fn deterministic_flags() -> u8 {
  RespInputFlags::DETERMINISTIC.bits()
}

/// 放入迁移发布 RMW StringInput.arg1 的哨兵：AOF 流是迁移键的唯一 AOF
/// 事实源，主存储 RMW 跳过自动记 AOF。仅迁移发布路径使用
///
/// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:StreamedPublishLogArg
pub const STREAMED_PUBLISH_LOG_ARG: i64 = i64::MIN;

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

/// 存根字节的 StorageBackend 编码转引擎后端枚举（0=Disk/Std, 1=Memory）
#[inline]
fn storage_backend_from_u8(v: u8) -> StorageBackend {
  if v == StorageBackendType::Memory.to_u8() {
    StorageBackend::Memory
  } else {
    StorageBackend::Std
  }
}

/// 逐键 AOF 流重组状态：重组反序列化器 + 追踪活动
/// （C# StreamReassemblyState；字段互斥访问经 parking_lot::Mutex，
/// 发布 await 前必须放锁）
struct StreamReassemblyState {
  deserializer: Mutex<RangeIndexChunkedDeserializer>,
  activity: Mutex<ReassemblyActivity>,
}

impl StreamReassemblyState {
  fn new(temp_path: PathBuf) -> Result<Self, ReplicationError> {
    Ok(Self {
      deserializer: Mutex::new(
        RangeIndexChunkedDeserializer::new(temp_path)
          .map_err(|e| ReplicationError(e.to_string()))?,
      ),
      activity: Mutex::new(ReassemblyActivity::start_activity()),
    })
  }
}

/// AOF 复制面（C# partial RangeIndexManager 的 Replication 分片）
pub struct RangeIndexManagerReplication {
  /// 引擎管理器（派生迁移临时文件路径 / 发布用键条带锁）
  engine: Arc<Engine>,
  /// 迁移流 AOF 分块大小（测试可调小以演练多块路径）
  aof_stream_chunk_size: AtomicUsize,
  /// 进行中逐键流重组状态（键字节 → 状态）
  reassembly: GxPapayaMap<Vec<u8>, Arc<StreamReassemblyState>>,
}

impl RangeIndexManagerReplication {
  /// 绑定引擎实例创建复制面（分块大小默认 256KB）
  pub fn new(engine: Arc<Engine>) -> Self {
    Self {
      engine,
      aof_stream_chunk_size: AtomicUsize::new(DEFAULT_MIGRATION_CHUNK_SIZE),
      reassembly: new_papaya_map(),
    }
  }

  /// 当前 AOF 流分块大小
  #[inline]
  pub fn aof_stream_chunk_size(&self) -> usize {
    self.aof_stream_chunk_size.load(Ordering::Acquire)
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:SetAofStreamChunkSize
  ///
  /// 设置迁移流 AOF 分块大小；小于最小分块（尾部框尺寸）即拒绝——
  /// 分块装不下尾部框会导致流永远无法完成（C# 抛 ArgumentOutOfRangeException）
  pub fn set_aof_stream_chunk_size(&self, chunk_size: usize) -> ReplicationResult {
    if chunk_size < MIN_CHUNK_SIZE {
      return Err(ReplicationError(format!(
        "Range index AOF stream chunk size must be at least {MIN_CHUNK_SIZE} bytes, got {chunk_size}"
      )));
    }
    self
      .aof_stream_chunk_size
      .store(chunk_size, Ordering::Release);
    Ok(())
  }

  /// 进行中逐键流重组数量
  #[inline]
  pub fn pending_stream_reassembly_count(&self) -> usize {
    self.reassembly.pin().len()
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:ReplicateRangeIndexSet
  ///
  /// 以 RI.SET RMW 形状直写入队 AOF（无合成 RMW 通道；stored_proc_mode 下
  /// 跳过——存储过程整体记日志）。C# RespInputFlags.Deterministic 以
  /// ReplayInput.flags 位承接
  pub fn replicate_range_index_set(
    &self,
    key: &[u8],
    field: &[u8],
    value: &[u8],
    append_only_file: Option<&GarnetAppendOnlyFile>,
    ctx: AofWriteContext,
    stored_proc_mode: bool,
  ) -> i64 {
    let Some(aof) = append_only_file else {
      return 0;
    };
    if stored_proc_mode {
      return 0;
    }
    let args = [field, value];
    let input = ReplayInputSlice {
      cmd: RespCommand::Riset,
      flags: deterministic_flags(),
      sub_id: 0,
      obj_type: 0,
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: &args,
    };
    enqueue_rmw_slices(aof, AofEntryType::StoreRMW, ctx, key, &input)
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:ReplicateRangeIndexDel
  ///
  /// 以 RI.DEL RMW 形状直写入队 AOF（语义同 [`Self::replicate_range_index_set`]）
  pub fn replicate_range_index_del(
    &self,
    key: &[u8],
    field: &[u8],
    append_only_file: Option<&GarnetAppendOnlyFile>,
    ctx: AofWriteContext,
    stored_proc_mode: bool,
  ) -> i64 {
    let Some(aof) = append_only_file else {
      return 0;
    };
    if stored_proc_mode {
      return 0;
    }
    let args = [field];
    let input = ReplayInputSlice {
      cmd: RespCommand::Ridel,
      flags: deterministic_flags(),
      sub_id: 0,
      obj_type: 0,
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: &args,
    };
    enqueue_rmw_slices(aof, AofEntryType::StoreRMW, ctx, key, &input)
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:HandleRangeIndexCreateReplay
  ///
  /// RI.CREATE AOF 回放：从条目内的存根字节提取 BfTree 配置（含原进程的
  /// 陈旧树句柄），以新引擎实例重建（树句柄由引擎写回存根落盘）。键已存在
  /// （检查点恢复后重复 RI.CREATE）按 C# 语义静默接受——重复条目不再重建
  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:HandleRangeIndexCreateReplay
  ///
  /// RI.CREATE AOF 回放：从条目内的存根字节提取 BfTree 配置（含原进程的
  /// 陈旧树句柄），以新引擎实例重建（树句柄由引擎写回存根落盘）。键已存在
  /// （检查点恢复后重复 RI.CREATE）按 C# 语义静默接受——重复条目不再重建
  pub async fn handle_range_index_create_replay(
    &self,
    session: &(impl RangeIndexSessionFace + ?Sized),
    key: &[u8],
    input: &ReplayInput,
  ) -> ReplicationResult {
    let Some(stub_bytes) = input.args.first() else {
      return Err(ReplicationError(format!(
        "Corrupt RI.CREATE AOF entry: no stub argument (expected {RANGE_INDEX_STUB_SIZE} bytes)"
      )));
    };
    if stub_bytes.len() != RANGE_INDEX_STUB_SIZE {
      return Err(ReplicationError(format!(
        "Corrupt RI.CREATE AOF entry: stub size {}, expected {RANGE_INDEX_STUB_SIZE}",
        stub_bytes.len()
      )));
    }
    let stub = RangeIndexStub::decode(stub_bytes)
      .map_err(|e| ReplicationError(format!("Failed to recreate BfTree during AOF replay: {e}")))?;
    let tuning = TreeTuning::from(&stub);
    let backend = storage_backend_from_u8(stub.storage_backend);
    // 已存在 = 重复回放（引擎在登记冲突时不会留下半初始化实例，等价 C#
    // 侧"新建后丢弃"）；其余错误上抛
    match session.ri_create(key, backend, tuning).await {
      Ok(()) => Ok(()),
      Err(RangeIndexError::AlreadyExists) => Ok(()),
      Err(e) => Err(ReplicationError(format!(
        "RI.CREATE AOF replay failed for key: {e}"
      ))),
    }
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:HandleRangeIndexSetReplay
  ///
  /// RI.SET AOF 回放：读存根取在线树后原生插入。键缺失 / 类型不符静默
  /// 跳过（C# status != OK 即 return）；插入被拒即主从发散，上抛
  pub async fn handle_range_index_set_replay(
    &self,
    session: &(impl RangeIndexSessionFace + ?Sized),
    key: &[u8],
    input: &ReplayInput,
  ) -> ReplicationResult {
    let (Some(field), Some(value)) = (input.args.first(), input.args.get(1)) else {
      return Err(ReplicationError(
        "Corrupt RI.SET AOF entry: field/value arguments missing".to_string(),
      ));
    };
    match session.ri_set(key, field, value).await {
      Ok(()) => Ok(()),
      // NOTFOUND / WRONGTYPE：回放面静默跳过（C# 提前 return 口径）
      Err(RangeIndexError::NotFound | RangeIndexError::WrongType) => Ok(()),
      Err(e) => Err(ReplicationError(format!("RI.SET AOF replay failed: {e}"))),
    }
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:HandleRangeIndexDelReplay
  ///
  /// RI.DEL AOF 回放：读存根取在线树后原生删除（口径同
  /// [`Self::handle_range_index_set_replay`]）
  pub async fn handle_range_index_del_replay(
    &self,
    session: &(impl RangeIndexSessionFace + ?Sized),
    key: &[u8],
    input: &ReplayInput,
  ) -> ReplicationResult {
    let Some(field) = input.args.first() else {
      return Err(ReplicationError(
        "Corrupt RI.DEL AOF entry: field argument missing".to_string(),
      ));
    };
    match session.ri_del(key, field).await {
      Ok(_) => Ok(()),
      Err(RangeIndexError::NotFound | RangeIndexError::WrongType) => Ok(()),
      Err(e) => Err(ReplicationError(format!("RI.DEL AOF replay failed: {e}"))),
    }
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:ReplicateRangeIndexStream
  ///
  /// 源侧：把迁移索引的序列化 BfTree 文件经迁移读取器分块读出，逐块以
  /// RangeIndexStreamChunk 灌入 AOF（首块 / 末块标志经 arg1 携带）。全程
  /// 以 StreamActivity 追踪（成功与失败路径均收口日志）
  pub fn replicate_range_index_stream(
    &self,
    key: &[u8],
    stub: &[u8],
    file_path: &Path,
    append_only_file: Option<&GarnetAppendOnlyFile>,
    ctx: AofWriteContext,
    chunk_size: usize,
  ) -> ReplicationResult {
    let Some(aof) = append_only_file else {
      log::warn!("ReplicateRangeIndexStream called with null appendOnlyFile");
      return Ok(());
    };

    let mut stream_activity = StreamActivity::start_activity(chunk_size);
    let run = self.stream_into_aof(
      aof,
      RangeIndexStreamArgs {
        key,
        stub,
        file_path,
        ctx,
        chunk_size,
      },
      &mut stream_activity,
    );
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
        return Err(ReplicationError(
          "ReplicateRangeIndexStream: reader returned zero-length chunk while the stream is incomplete".to_string(),
        ));
      }
      self.enqueue_range_index_stream_chunk(
        aof,
        args.ctx,
        args.key,
        &dest[..written],
        is_first,
        reader.is_complete(),
      );
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
    key: &[u8],
    chunk: &[u8],
    is_first: bool,
    is_last: bool,
  ) -> i64 {
    let args = [chunk];
    let input = ReplayInputSlice {
      cmd: RespCommand::None,
      flags: deterministic_flags(),
      sub_id: 0,
      obj_type: 0,
      arg1: pack_stream_chunk_flags(is_first, is_last),
      arg2: 0,
      arg3: 0,
      args: &args,
    };
    enqueue_rmw_slices(
      append_only_file,
      AofEntryType::RangeIndexStreamChunk,
      ctx,
      key,
      &input,
    )
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:HandleRangeIndexStreamReplay
  ///
  /// AOF 回放（副本复制 / 崩溃恢复）单个流块：从 arg1 解出首尾标志后
  /// 交 [`Self::process_stream_chunk`]
  pub async fn handle_range_index_stream_replay(
    &self,
    session: &(impl RangeIndexSessionFace + ?Sized),
    key: &[u8],
    input: &ReplayInput,
  ) -> ReplicationResult {
    let Some(chunk) = input.args.first() else {
      return Err(ReplicationError(
        "Corrupt RangeIndexStreamChunk AOF entry: chunk argument missing".to_string(),
      ));
    };
    let (is_first, is_last) = unpack_stream_chunk_flags(input.arg1);
    self
      .process_stream_chunk(session, key, chunk, is_first, is_last)
      .await
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:ProcessStreamChunk
  ///
  /// 流重组核心步：首块重置同键陈旧状态 → 喂块给逐键反序列化器 → 流完成
  /// 时发布重组成的 BfTree。失败路径（喂块被拒 / 发布失败 / 末块后仍未
  /// 完成）一律清理重组状态后上抛
  pub async fn process_stream_chunk(
    &self,
    session: &(impl RangeIndexSessionFace + ?Sized),
    key: &[u8],
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
      return Err(ReplicationError(format!(
        "HandleRangeIndexStreamReplay: failed to process range index stream chunk for key {}: {detail}",
        String::from_utf8_lossy(key)
      )));
    }

    if complete {
      // TODO(RangeIndex): 随流携带 replaceOption（C# 同 TODO：迁移流未编码
      // REPLACE 语义，固定 replace=false）
      let publish_result = RangeIndexManagerMigration::publish_migrated_index(
        session,
        key,
        &stub_bytes,
        &temp_path,
        false,
      )
      .await;
      state.activity.lock().on_publish_result(publish_result);
      if publish_result == PublishMigratedIndexResult::Failed {
        log::error!(
          "HandleRangeIndexStreamReplay: PublishMigratedIndex failed during AOF replay for key {}",
          String::from_utf8_lossy(key)
        );
        self.remove_and_dispose_stream_reassembly(key, "PublishFailed");
        return Err(ReplicationError(format!(
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
      return Err(ReplicationError(format!(
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
  /// 丢弃全部进行中的流重组状态（管理器释放路径调用）
  pub fn dispose_incomplete_stream_reassembly(&self) {
    let keys: Vec<Vec<u8>> = {
      let pin = self.reassembly.pin();
      pin.iter().map(|(k, _)| k.clone()).collect()
    };
    for key in keys {
      log::warn!(
        "DisposeIncompleteStreamReassembly: discarding incomplete range index stream reassembly for key {}",
        String::from_utf8_lossy(&key)
      );
      self.remove_and_dispose_stream_reassembly(&key, "CleanupIncomplete");
    }
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

/// StoreRMW / RangeIndexStreamChunk 直写入队公共体（切片零分配）
fn enqueue_rmw_slices<T: AsRef<[u8]>>(
  aof: &GarnetAppendOnlyFile,
  op_type: AofEntryType,
  ctx: AofWriteContext,
  key: &[u8],
  input: &ReplayInputSlice<'_, T>,
) -> i64 {
  ReplayInput::with_encoded_slices(input, |serialized| {
    aof.log().enqueue(&RecordShape {
      op_type,
      version: ctx.version,
      session_id: ctx.session_id,
      key,
      value: b"",
      input: serialized,
      database_id: 0,
    })
  })
}

#[cfg(test)]
mod tests {
  use std::fs;

  use tempfile::TempDir;

  use super::*;
  use crate::{
    aof::{
      aof_header::AofHeader,
      garnet_append_only_file::GarnetAppendOnlyFile,
      garnet_log::{GarnetLog, InMemorySublog, LogRecord},
      sublog::Sublog,
    },
    config::runtime_server_options::RuntimeServerOptions,
  };

  const HEADER_SIZE: usize = AofHeader::TOTAL_SIZE;

  /// 内存 AOF（无盘拓扑；测试断言入队形状与地址推进）
  fn memory_aof() -> Arc<GarnetAppendOnlyFile> {
    let options = RuntimeServerOptions::default();
    Arc::new(GarnetAppendOnlyFile::new(
      Arc::new(GarnetLog::new(
        &options,
        vec![Arc::new(Sublog::Mem(InMemorySublog::new()))],
        None,
      )),
      &options,
      None,
    ))
  }

  fn replication_in(dir: &TempDir) -> RangeIndexManagerReplication {
    RangeIndexManagerReplication::new(Arc::new(
      Engine::new(dir.path().join("ri"), dir.path().join("cpr")).unwrap(),
    ))
  }

  /// 解析记录的 RI 输入段（payload = 头 + SpanByte key + input；
  /// StoreRMW 形状无 value 组件）
  fn parse_input(record: &LogRecord, key_len: usize) -> ReplayInput {
    let input_start = HEADER_SIZE + 4 + key_len;
    ReplayInput::deserialize(&record.payload[input_start..]).expect("ReplayInput roundtrip")
  }

  /// 解析记录的 SpanByte key 段
  fn parse_key(record: &LogRecord) -> &[u8] {
    let len = u32::from_le_bytes(
      record.payload[HEADER_SIZE..HEADER_SIZE + 4]
        .try_into()
        .unwrap(),
    ) as usize;
    &record.payload[HEADER_SIZE + 4..HEADER_SIZE + 4 + len]
  }

  fn parse_header(record: &LogRecord) -> AofHeader {
    AofHeader::parse(&record.payload).expect("header decodable")
  }

  #[test]
  fn chunk_size_boundary_rejects_below_trailer_size() {
    let dir = TempDir::new().unwrap();
    let replication = replication_in(&dir);
    // C# MinChunkSize 边界：< 47 拒绝，47 恰好接受
    assert!(
      replication
        .set_aof_stream_chunk_size(MIN_CHUNK_SIZE - 1)
        .is_err()
    );
    assert!(
      replication
        .set_aof_stream_chunk_size(MIN_CHUNK_SIZE)
        .is_ok()
    );
    assert_eq!(replication.aof_stream_chunk_size(), MIN_CHUNK_SIZE);
    assert!(replication.set_aof_stream_chunk_size(0).is_err());
    assert!(replication.set_aof_stream_chunk_size(256 * 1024).is_ok());
    assert_eq!(replication.aof_stream_chunk_size(), 256 * 1024);
  }

  #[test]
  fn stream_chunk_flags_pack_roundtrip() {
    // C# 位面：IsLast = 1，IsFirst = 2，可叠加
    assert_eq!(pack_stream_chunk_flags(false, false), 0);
    assert_eq!(
      pack_stream_chunk_flags(true, false),
      STREAM_CHUNK_IS_FIRST_FLAG
    );
    assert_eq!(
      pack_stream_chunk_flags(false, true),
      STREAM_CHUNK_IS_LAST_FLAG
    );
    assert_eq!(pack_stream_chunk_flags(true, true), 3);
    for (first, last) in [(false, false), (true, false), (false, true), (true, true)] {
      let (f, l) = unpack_stream_chunk_flags(pack_stream_chunk_flags(first, last));
      assert_eq!((f, l), (first, last));
    }
  }

  #[test]
  fn replicate_set_del_enqueue_shapes() {
    let aof = memory_aof();
    let dir = TempDir::new().unwrap();
    let replication = replication_in(&dir);

    // NULL AOF / 存储过程模式：跳过入队（返回 0）
    let ctx = AofWriteContext {
      version: 7,
      session_id: 3,
    };
    let none_ctx = AofWriteContext {
      version: 1,
      session_id: 1,
    };
    assert_eq!(
      replication.replicate_range_index_set(b"k", b"f", b"v", None, none_ctx, false),
      0
    );
    assert_eq!(
      replication.replicate_range_index_set(b"k", b"f", b"v", Some(&aof), none_ctx, true),
      0
    );
    assert_eq!(
      replication.replicate_range_index_del(b"k", b"f", None, none_ctx, false),
      0
    );

    let a1 =
      replication.replicate_range_index_set(b"k", b"field-1", b"value-1", Some(&aof), ctx, false);
    let a2 = replication.replicate_range_index_del(b"k", b"field-2", Some(&aof), ctx, false);
    assert!(a1 > 0 && a2 > a1, "addresses must advance monotonically");

    // 回读：两条目均 StoreRMW 头 + key 领衔负载 + 确定性标志
    let records = aof.log().scan_single(0, 0, i64::MAX);
    assert_eq!(records.len(), 2);
    for record in &records {
      let header = parse_header(record);
      assert_eq!(header.op_type, AofEntryType::StoreRMW as u8);
      assert_eq!(header.store_version, 7);
      assert_eq!(header.session_id, 3);
      assert_eq!(parse_key(record), b"k");
      let input = parse_input(record, 1);
      assert_eq!(input.flags, deterministic_flags());
      assert!(input.cmd == RespCommand::Riset || input.cmd == RespCommand::Ridel);
    }
    let first = parse_input(&records[0], 1);
    assert_eq!(first.cmd, RespCommand::Riset);
    assert_eq!(first.args, vec![b"field-1".to_vec(), b"value-1".to_vec()]);
    let second = parse_input(&records[1], 1);
    assert_eq!(second.cmd, RespCommand::Ridel);
    assert_eq!(second.args, vec![b"field-2".to_vec()]);
  }

  #[test]
  fn single_chunk_stream_enqueues_with_first_and_last_flags() {
    let aof = memory_aof();
    let dir = TempDir::new().unwrap();
    let file_path = dir.path().join("snapshot.bftree");
    fs::write(&file_path, b"tiny-snapshot").unwrap();

    let replication = replication_in(&dir);
    let ctx = AofWriteContext {
      version: 5,
      session_id: 2,
    };
    replication
      .replicate_range_index_stream(b"key", &[9u8; 35], &file_path, Some(&aof), ctx, 4096)
      .unwrap();

    let records = aof.log().scan_single(0, 0, i64::MAX);
    assert_eq!(records.len(), 1, "small snapshot fits one chunk");
    let header = parse_header(&records[0]);
    assert_eq!(header.op_type, AofEntryType::RangeIndexStreamChunk as u8);
    let parsed = parse_input(&records[0], 3);
    assert_eq!(parsed.cmd, RespCommand::None);
    // 单块流：首块与末块标志同时置位
    assert_eq!(parsed.arg1, pack_stream_chunk_flags(true, true));
    // 块载荷 = keyLen(4)+key(3)+fileLen(8)+file(13)+hash(8)+stubLen(4)+stub(35)
    assert_eq!(
      parsed.args.first().map(Vec::len),
      Some(4 + 3 + 8 + 13 + 8 + 4 + 35)
    );
  }

  #[test]
  fn multi_chunk_stream_marks_only_outer_flags() {
    let aof = memory_aof();
    let dir = TempDir::new().unwrap();
    let file_path = dir.path().join("big.bftree");
    // 40KB 快照、128B 分块 → 严格多块
    fs::write(&file_path, vec![0xABu8; 40 * 1024]).unwrap();

    let replication = replication_in(&dir);
    let ctx = AofWriteContext {
      version: 2,
      session_id: 1,
    };
    replication
      .replicate_range_index_stream(b"k", &[7u8; 35], &file_path, Some(&aof), ctx, 128)
      .unwrap();

    let records = aof.log().scan_single(0, 0, i64::MAX);
    assert!(records.len() > 10, "expected strictly multi-chunk stream");
    for (i, record) in records.iter().enumerate() {
      let (is_first, is_last) = unpack_stream_chunk_flags(parse_input(record, 1).arg1);
      assert_eq!(is_first, i == 0, "only first chunk carries first flag");
      assert_eq!(
        is_last,
        i == records.len() - 1,
        "only last chunk carries last flag"
      );
    }
  }

  #[test]
  fn null_aof_stream_is_warning_noop() {
    let dir = TempDir::new().unwrap();
    let file_path = dir.path().join("s.bftree");
    fs::write(&file_path, b"payload").unwrap();
    let replication = replication_in(&dir);
    assert!(
      replication
        .replicate_range_index_stream(
          b"k",
          &[0u8; 35],
          &file_path,
          None,
          AofWriteContext {
            version: 1,
            session_id: 1
          },
          4096,
        )
        .is_ok()
    );
  }

  #[test]
  fn missing_file_fails_stream() {
    let aof = memory_aof();
    let dir = TempDir::new().unwrap();
    let replication = replication_in(&dir);
    let result = replication.replicate_range_index_stream(
      b"k",
      &[0u8; 35],
      &dir.path().join("absent.bftree"),
      Some(&aof),
      AofWriteContext {
        version: 1,
        session_id: 1,
      },
      4096,
    );
    assert!(result.is_err());
  }

  #[test]
  fn sentinel_and_backend_mapping_match_csharp() {
    // C# long.MinValue 哨兵与后端字节映射
    assert_eq!(STREAMED_PUBLISH_LOG_ARG, i64::MIN);
    assert!(matches!(storage_backend_from_u8(0), StorageBackend::Std));
    assert!(matches!(storage_backend_from_u8(1), StorageBackend::Memory));
  }
}
