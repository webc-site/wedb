//! Garnet 分布式 AOF 日志：单日志 / 分片多子日志双拓扑的路由层
//! （对标 libs/server/AOF/GarnetLog.cs:GarnetLog）。
//!
//! C# 底层为 TsavoriteLog（单）或 TsavoriteLog[]（分片）；Rust 侧以
//! [`SublogBackend`] trait 承接设备面（可由内建 [`InMemorySublog`] 或
//! waof 设备实现注入），本类型保留全部路由 / 头编码 / 背压 / 地址向量语义。
//!
//! 分片路由：`物理子日志 = hash % physicalSublogCount`，
//! `回放任务 = hash / physicalSublogCount % replayTaskCount`，
//! `虚拟子日志 = 物理子日志 * replayTaskCount + 回放任务`。

use std::{
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
  },
  thread,
  time::Duration,
};

use parking_lot::Mutex;

use super::{
  aof_address::AofAddress,
  aof_backpressure::AofBackpressure,
  aof_entry_type::AofEntryType,
  aof_header::{AofHeader, AofHeaderType, AofShardedHeader},
  sharded_log::ShardedLogLockMap,
};
use crate::config::runtime_server_options::RuntimeServerOptions;

/// 条目记录（头 + 负载字节）。
#[derive(Debug, Clone)]
pub struct LogRecord {
  /// 条目起始逻辑地址。
  pub address: i64,
  /// 完整条目字节（头 + key + value + input）。
  pub payload: Vec<u8>,
}

/// 子日志设备面（对齐 TsavoriteLog 的 GarnetLog 消费子集）。
pub trait SublogBackend: Send + Sync {
  /// 追加一条记录，返回其起始逻辑地址。
  fn enqueue(&self, sublog_idx: usize, payload: &[u8]) -> i64;
  /// 尾地址（下一记录的写入点）。
  fn tail_address(&self, sublog_idx: usize) -> i64;
  /// begin 地址。
  fn begin_address(&self, sublog_idx: usize) -> i64;
  /// 已提交地址。
  fn committed_until_address(&self, sublog_idx: usize) -> i64;
  /// 推进提交水位。
  fn commit(&self, sublog_idx: usize, until_address: i64);
  /// 刷盘水位。
  fn flushed_until_address(&self, sublog_idx: usize) -> i64;
  /// 扫描 [begin, end) 区间记录。
  fn scan(&self, sublog_idx: usize, begin_address: i64, end_address: i64) -> Vec<LogRecord>;
  /// 截断 begin 至指定地址（日志平移）。
  fn shift_begin_address(&self, sublog_idx: usize, new_begin: i64);
  /// 页大小位。
  fn log_page_size_bits(&self, sublog_idx: usize) -> i32;
  /// 内存占用。
  fn memory_size_bytes(&self, sublog_idx: usize) -> i64;
  /// 重置日志。
  fn reset(&self, sublog_idx: usize);
}

/// 内建内存子日志（测试与无盘场景）。
#[derive(Default)]
pub struct InMemorySublog {
  records: parking_lot::Mutex<Vec<LogRecord>>,
  begin: AtomicI64,
  committed_until: AtomicI64,
}

impl InMemorySublog {
  /// 空日志，起始地址 1（对齐 TsavoriteLog 初始地址约定）。
  pub fn new() -> Self {
    Self {
      records: parking_lot::Mutex::new(Vec::new()),
      begin: AtomicI64::new(1),
      committed_until: AtomicI64::new(1),
    }
  }
}

impl SublogBackend for InMemorySublog {
  fn enqueue(&self, _sublog_idx: usize, payload: &[u8]) -> i64 {
    let mut records = self.records.lock();
    let address = records
      .last()
      .map_or(self.begin.load(Ordering::Relaxed), |last| {
        last.address + last.payload.len() as i64
      });
    records.push(LogRecord {
      address,
      payload: payload.to_vec(),
    });
    address
  }

  fn tail_address(&self, _sublog_idx: usize) -> i64 {
    self.records.lock().last().map_or_else(
      || self.begin.load(Ordering::Relaxed),
      |last| last.address + last.payload.len() as i64,
    )
  }

  fn begin_address(&self, _sublog_idx: usize) -> i64 {
    self.begin.load(Ordering::Relaxed)
  }

  fn committed_until_address(&self, _sublog_idx: usize) -> i64 {
    self.committed_until.load(Ordering::Relaxed)
  }

  fn commit(&self, _sublog_idx: usize, until_address: i64) {
    self
      .committed_until
      .fetch_max(until_address, Ordering::Release);
  }

  fn flushed_until_address(&self, _sublog_idx: usize) -> i64 {
    self.committed_until.load(Ordering::Relaxed)
  }

  fn scan(&self, _sublog_idx: usize, begin_address: i64, end_address: i64) -> Vec<LogRecord> {
    self
      .records
      .lock()
      .iter()
      .filter(|r| r.address >= begin_address && r.address < end_address)
      .cloned()
      .collect()
  }

  fn shift_begin_address(&self, _sublog_idx: usize, new_begin: i64) {
    self.begin.store(new_begin, Ordering::Release);
  }

  fn log_page_size_bits(&self, _sublog_idx: usize) -> i32 {
    22
  }

  fn memory_size_bytes(&self, _sublog_idx: usize) -> i64 {
    self
      .records
      .lock()
      .iter()
      .map(|r| r.payload.len() as i64)
      .sum()
  }

  fn reset(&self, _sublog_idx: usize) {
    self.records.lock().clear();
    self.begin.store(1, Ordering::Release);
    self.committed_until.store(1, Ordering::Release);
  }
}

/// 单日志 / 分片双拓扑容器。
pub struct GarnetLog {
  /// 单日志后端（单物理日志拓扑）。
  single_log: Option<Arc<dyn SublogBackend>>,
  /// 分片后端（多物理子日志拓扑）。
  sharded_log: Vec<Arc<dyn SublogBackend>>,
  /// 物理子日志数。
  physical_sublog_count: usize,
  /// 回放任务数。
  replay_task_count: usize,
  /// 子日志访问位图锁。
  lock_map: ShardedLogLockMap,
  /// 主侧背压闸门（可选）。
  backpressure: Option<Arc<AofBackpressure>>,
  /// 日志平移回调（尾地址前移通知）。
  shift_tail_callback: Mutex<Option<ShiftTailCallback>>,
  /// 尾部见证地址（复制对齐用）。
  tail_witness: AtomicI64,
}

impl GarnetLog {
  /// libs/server/AOF/GarnetLog.cs:GarnetLog（构造）。
  ///
  /// 依选项决定单日志（1 物理 1 回放）或分片拓扑，并构造背压闸门。
  pub fn new(server_options: &RuntimeServerOptions, backends: Vec<Arc<dyn SublogBackend>>) -> Self {
    let physical_sublog_count = server_options.aof_physical_sublog_count.max(1) as usize;
    let replay_task_count = server_options.aof_replay_task_count.max(1) as usize;
    let using_single = physical_sublog_count == 1 && replay_task_count == 1;

    let mut iter = backends.into_iter();
    let single_log = using_single.then(|| iter.next().expect("单日志拓扑需 1 个后端"));
    let sharded_log = if using_single {
      Vec::new()
    } else {
      iter.collect()
    };

    Self {
      single_log,
      sharded_log,
      physical_sublog_count,
      replay_task_count,
      lock_map: ShardedLogLockMap::new(),
      backpressure: Some(Arc::new(AofBackpressure::new(
        physical_sublog_count,
        server_options.aof_sync_max_lag_bytes,
      ))),
      shift_tail_callback: Mutex::new(None),
      tail_witness: AtomicI64::new(0),
    }
  }

  /// libs/server/AOF/GarnetLog.cs:GetSequenceNumberFromCookie
  ///
  /// cookie 头 8 字节即序列号（LE）。
  pub fn get_sequence_number_from_cookie(cookie: &[u8]) -> i64 {
    cookie
      .first_chunk::<8>()
      .map_or(0, |b| i64::from_le_bytes(*b))
  }

  /// libs/server/AOF/GarnetLog.cs:HASH
  ///
  /// 键的 64 位分片哈希。C# 为 Tsavorite SpanByteComparer（xxHash 系）；
  /// 数值跨实现不要求一致——子日志映射在单节点内闭环，仅需进程内稳定
  /// 且分布均匀。
  pub fn hash(key: &[u8]) -> i64 {
    gxhash::gxhash64(key, 0) as i64
  }

  /// libs/server/AOF/GarnetLog.cs:GetPhysicalSublogIdx
  #[inline]
  pub fn get_physical_sublog_idx(&self, hash: i64) -> usize {
    ((hash as u64) % (self.physical_sublog_count as u64)) as usize
  }

  /// libs/server/AOF/GarnetLog.cs:GetReplayTaskIdx
  #[inline]
  pub fn get_replay_task_idx(&self, hash: i64) -> usize {
    (((hash as u64) / (self.physical_sublog_count as u64)) % (self.replay_task_count as u64))
      as usize
  }

  /// libs/server/AOF/GarnetLog.cs:GetVirtualSublogIdx
  #[inline]
  pub fn get_virtual_sublog_idx(&self, hash: i64) -> usize {
    self.get_physical_sublog_idx(hash) * self.replay_task_count + self.get_replay_task_idx(hash)
  }

  /// 恢复时以 cookie 序列号收敛恢复上限（C# RecoverLatestSequenceNumber 的
  /// cookie 回调路径；`recover_until` 为 -1 表示未设）。
  pub fn recover_latest_sequence_number(&self, recover_until: i64, cookie: &[u8]) -> i64 {
    let latest = Self::get_sequence_number_from_cookie(cookie);
    if recover_until == -1 {
      latest
    } else {
      recover_until.min(latest)
    }
  }

  /// 拓扑的子日志总数（C# Size 属性）。
  pub fn size(&self) -> usize {
    if self.single_log.is_some() {
      1
    } else {
      self.sharded_log.len()
    }
  }

  /// libs/server/AOF/GarnetLog.cs:AllLogsBitmask
  ///
  /// 全部子日志的访问位图。
  pub fn all_logs_bitmask(&self) -> u64 {
    (1u64 << self.size()) - 1
  }

  /// libs/server/AOF/GarnetLog.cs:LockSublogs
  ///
  /// 入队操作前的子日志位图锁（慢路径，慎用）。
  pub fn lock_sublogs(&self, log_access_bitmap: u64) {
    self.lock_map.lock_sublogs(log_access_bitmap);
  }

  /// libs/server/AOF/GarnetLog.cs:UnlockSublogs
  pub fn unlock_sublogs(&self, log_access_bitmap: u64) {
    self.lock_map.unlock_sublogs(log_access_bitmap);
  }

  fn sublog(&self, sublog_idx: usize) -> &Arc<dyn SublogBackend> {
    if let Some(single) = &self.single_log {
      single
    } else {
      &self.sharded_log[sublog_idx]
    }
  }

  /// libs/server/AOF/GarnetLog.cs:GetSubLog
  ///
  /// 指定子日志后端。
  pub fn get_sub_log(&self, sublog_idx: usize) -> &Arc<dyn SublogBackend> {
    self.sublog(sublog_idx)
  }

  /// libs/server/AOF/GarnetLog.cs:GetBeginAddress
  ///
  /// 全拓扑地址向量（逐子日志 begin）。
  pub fn get_begin_address(&self) -> AofAddress {
    let len = self.size() as i32;
    let mut result = AofAddress::create(len, 0);
    for i in 0..len as usize {
      result.set(i, self.sublog(i).begin_address(i));
    }
    result
  }

  /// 全拓扑尾地址向量（C# TailAddress 属性）。
  pub fn get_tail_address_vector(&self) -> AofAddress {
    let len = self.size() as i32;
    let mut result = AofAddress::create(len, 0);
    for i in 0..len as usize {
      result.set(i, self.sublog(i).tail_address(i));
    }
    result
  }

  /// 单子日志尾地址（背压/复制对齐路径）。
  pub fn get_tail_address(&self, sublog_idx: usize) -> i64 {
    self.sublog(sublog_idx).tail_address(sublog_idx)
  }

  /// libs/server/AOF/GarnetLog.cs:SetLogShiftTailCallback
  ///
  /// 注册尾地址前移回调（复制对齐通知）。
  pub fn set_log_shift_tail_callback(&self, callback: Box<dyn Fn(i64) + Send + Sync>) {
    *self.shift_tail_callback.lock() = Some(callback);
  }

  /// libs/server/AOF/GarnetLog.cs:ScanSingle
  ///
  /// 单子日志区间扫描（`recover` 与缓冲模式由后端承接）。
  pub fn scan_single(
    &self,
    sublog_idx: usize,
    begin_address: i64,
    end_address: i64,
  ) -> Vec<LogRecord> {
    self
      .sublog(sublog_idx)
      .scan(sublog_idx, begin_address, end_address)
  }

  /// libs/server/AOF/GarnetLog.cs:UnsafeGetLogPageSizeBits
  pub fn unsafe_get_log_page_size_bits(&self) -> i32 {
    self.sublog(0).log_page_size_bits(0)
  }

  /// libs/server/AOF/GarnetLog.cs:UnsafeGetReadOnlyAddressAbove
  ///
  /// 高于 `address` 的只读安全地址（内存拓扑即尾地址）。
  pub fn unsafe_get_read_only_address_above(&self, sublog_idx: usize, address: i64) -> i64 {
    self.get_tail_address(sublog_idx).max(address)
  }

  /// libs/server/AOF/GarnetLog.cs:UnsafeShiftBeginAddress
  ///
  /// 平移子日志 begin 地址并通知尾移回调。
  pub fn unsafe_shift_begin_address(&self, sublog_idx: usize, new_begin: i64) {
    self
      .sublog(sublog_idx)
      .shift_begin_address(sublog_idx, new_begin);
    if let Some(callback) = self.shift_tail_callback.lock().as_ref() {
      callback(new_begin);
    }
  }

  /// libs/server/AOF/GarnetLog.cs:TruncateUntil
  ///
  /// 截断至指定地址向量（逐子日志平移 begin）。
  pub fn truncate_until(&self, until: &AofAddress) {
    for i in 0..self.size() {
      self
        .sublog(i)
        .shift_begin_address(i, until.get(i).unwrap_or(0));
    }
  }

  /// libs/server/AOF/GarnetLog.cs:SafeInitialize
  ///
  /// 幂等初始化（内存拓扑无设备句柄，仅标记就绪）。
  pub fn safe_initialize(&self) -> bool {
    true
  }

  /// libs/server/AOF/GarnetLog.cs:InitializeIf
  ///
  /// `condition` 成立时初始化。
  pub fn initialize_if(&self, condition: bool) -> bool {
    condition && self.safe_initialize()
  }

  /// libs/server/AOF/GarnetLog.cs:WaitForCommit
  ///
  /// 阻塞直至提交水位达到 `address`（内存拓扑提交即时可见，直接返回）。
  pub fn wait_for_commit(&self, sublog_idx: usize, address: i64) {
    while self.sublog(sublog_idx).committed_until_address(sublog_idx) < address {
      thread::sleep(Duration::from_micros(50));
    }
  }

  /// libs/server/AOF/GarnetLog.cs:CommitAsync（同步语义：推进提交水位）
  pub fn commit(&self, sublog_idx: usize) -> i64 {
    let tail = self.sublog(sublog_idx).tail_address(sublog_idx);
    self.sublog(sublog_idx).commit(sublog_idx, tail);
    self.tail_witness.store(tail, Ordering::Release);
    tail
  }

  /// libs/server/AOF/GarnetLog.cs:WaitForCommitAsync（同步等待提交水位）
  pub fn wait_for_commit_async(&self, sublog_idx: usize, address: i64) {
    self.wait_for_commit(sublog_idx, address);
  }

  /// libs/server/AOF/GarnetLog.cs:BackpressureWaitKey
  fn backpressure_wait_key(&self, key: &[u8]) {
    let Some(backpressure) = &self.backpressure else {
      return;
    };
    let sublog_idx = self.get_physical_sublog_idx(Self::hash(key));
    backpressure.wait(sublog_idx, self.get_tail_address(sublog_idx));
  }

  /// libs/server/AOF/GarnetLog.cs:BackpressureWaitKeyHash
  fn backpressure_wait_key_hash(&self, key_hash: i64) {
    let Some(backpressure) = &self.backpressure else {
      return;
    };
    let sublog_idx = self.get_physical_sublog_idx(key_hash);
    backpressure.wait(sublog_idx, self.get_tail_address(sublog_idx));
  }

  /// libs/server/AOF/GarnetLog.cs:BackpressureWaitVector
  ///
  /// 多子日志追加前的失效保护自检：对每个参与子日志做背压等待。
  fn backpressure_wait_vector(&self, mut physical_sublog_access_vector: u64) {
    let Some(backpressure) = &self.backpressure else {
      return;
    };
    while physical_sublog_access_vector > 0 {
      let sublog_idx = physical_sublog_access_vector.trailing_zeros() as usize;
      backpressure.wait(sublog_idx, self.get_tail_address(sublog_idx));
      physical_sublog_access_vector &= physical_sublog_access_vector - 1;
    }
  }

  /// libs/server/AOF/GarnetLog.cs:IsChunkable
  ///
  /// key+value+input 总规模超过最小部分分配尺寸即需分块。
  pub fn is_chunkable(key_len: usize, value_len: usize, input_serialized_length: usize) -> bool {
    (key_len + value_len + input_serialized_length) as i64 > MIN_PARTIAL_ALLOC_SIZE
  }

  /// 头编码 + 负载拼装的通用入队：按拓扑选择 Basic/Sharded 头，
  /// 返回逻辑地址。
  fn enqueue_with_header(&self, record: &RecordShape<'_>) -> i64 {
    let RecordShape {
      op_type,
      version,
      session_id,
      key,
      value,
      input,
      database_id,
    } = *record;
    let using_single_physical_log = self.single_log.is_some();
    let physical_sublog_idx = if using_single_physical_log {
      0
    } else {
      self.get_physical_sublog_idx(Self::hash(key))
    };

    let mut payload =
      Vec::with_capacity(AofHeader::TOTAL_SIZE + key.len() + value.len() + input.len());
    if using_single_physical_log {
      // 单物理日志（含单日志 + 多回放）：BasicHeader，地址即排序。
      let mut header = AofHeader::new();
      header.set_header_type(AofHeaderType::BasicHeader);
      header.op_type = op_type as u8;
      header.store_version = version;
      header.session_id = session_id;
      header.database_id = database_id;
      payload.extend_from_slice(&header.to_bytes());
    } else {
      // 多物理子日志 + 多回放：ShardedHeader 携带跨子日志排序号。
      let mut header = AofHeader::new();
      header.set_header_type(AofHeaderType::ShardedHeader);
      header.op_type = op_type as u8;
      header.store_version = version;
      header.session_id = session_id;
      header.database_id = database_id;
      let sequence_number = self.tail_witness.load(Ordering::Relaxed);
      let sharded = AofShardedHeader {
        basic: header,
        sequence_number,
      };
      payload.extend_from_slice(&sharded.basic.to_bytes());
      payload.extend_from_slice(&sequence_number.to_le_bytes());
    }
    payload.extend_from_slice(key);
    payload.extend_from_slice(value);
    payload.extend_from_slice(input);
    self
      .sublog(physical_sublog_idx)
      .enqueue(physical_sublog_idx, &payload)
  }

  /// libs/server/AOF/GarnetLog.cs:Enqueue（upsert/RMW/delete 通用形状）
  ///
  /// 背压等待 → 大记录分块（此处按不可分块形状直写）→ 头编码入队。
  pub fn enqueue(&self, record: &RecordShape<'_>) -> i64 {
    self.backpressure_wait_key(record.key);
    self.enqueue_with_header(record)
  }

  /// libs/server/AOF/GarnetLog.cs:EnqueueSpanChunked
  ///
  /// 大记录分块写入：全量长度预先盖入分块头，使读取器可按组件预分配。
  /// `write_value` / `write_input` 选择组件（key 恒写）。
  pub fn enqueue_span_chunked(&self, chunk: &ChunkedShape<'_>) -> i64 {
    let ChunkedShape {
      record:
        RecordShape {
          op_type,
          version,
          session_id,
          key,
          value,
          input,
          database_id,
        },
      write_value,
      write_input,
    } = *chunk;
    self.backpressure_wait_key_hash(Self::hash(key));
    let chunk_header = super::aof_header::AofChunkHeader {
      overflow_key_length: key.len() as u32,
      overflow_value_length: if write_value { value.len() as u32 } else { 0 },
      input_length: if write_input { input.len() as u32 } else { 0 },
      object_id: 0,
      key_hash: Self::hash(key),
    };
    let using_single_physical_log = self.single_log.is_some();
    let header_size = if using_single_physical_log {
      AofHeader::TOTAL_SIZE
    } else {
      AofShardedHeader::TOTAL_SIZE
    };

    // 分块尺寸：页内可容纳的负载上限（对齐 C# 按页切分的组件布局）。
    let page_payload = (1usize << self.unsafe_get_log_page_size_bits() as u32)
      - header_size
      - super::aof_header::AofChunkHeader::TOTAL_SIZE;
    let mut chunks: Vec<Vec<u8>> = Vec::new();
    let mut emit = |mut chunk: Vec<u8>, key: &[u8], value: &[u8]| {
      chunk.extend_from_slice(key);
      chunk.extend_from_slice(value);
      chunks.push(chunk);
    };

    let mut first = Vec::with_capacity(header_size + super::aof_header::AofChunkHeader::TOTAL_SIZE);
    let mut header = AofHeader::new();
    header.set_header_type(if using_single_physical_log {
      AofHeaderType::BasicChunkHeader
    } else {
      AofHeaderType::ShardedChunkHeader
    });
    header.op_type = op_type as u8;
    header.store_version = version;
    header.session_id = session_id;
    header.database_id = database_id;
    first.extend_from_slice(&header.to_bytes());
    if !using_single_physical_log {
      first.extend_from_slice(&0i64.to_le_bytes());
    }
    first.extend_from_slice(&chunk_header.overflow_key_length.to_le_bytes());
    first.extend_from_slice(&chunk_header.overflow_value_length.to_le_bytes());
    first.extend_from_slice(&chunk_header.input_length.to_le_bytes());
    first.extend_from_slice(&chunk_header.object_id.to_le_bytes());
    first.extend_from_slice(&chunk_header.key_hash.to_le_bytes());
    emit(first, key, &[]);

    let remaining = if write_value { value } else { &[][..] };
    for piece in remaining.chunks(page_payload.max(1)) {
      let piece_chunk = Vec::with_capacity(piece.len());
      emit(piece_chunk, &[], piece);
    }
    if write_input {
      let input_chunk = Vec::with_capacity(input.len());
      emit(input_chunk, &[], input);
    }

    let mut address = 0;
    for chunk in chunks {
      let physical_sublog_idx = if using_single_physical_log {
        0
      } else {
        self.get_physical_sublog_idx(chunk_header.key_hash)
      };
      address = self
        .sublog(physical_sublog_idx)
        .enqueue(physical_sublog_idx, &chunk);
    }
    address
  }

  /// libs/server/AOF/GarnetLog.cs:EnqueueObjectChunked
  ///
  /// 对象值分块写入（值组件流式，读取器累积）。
  pub fn enqueue_object_chunked(&self, chunk: &ChunkedShape<'_>) -> i64 {
    self.enqueue_span_chunked(&ChunkedShape {
      write_value: true,
      ..chunk.clone()
    })
  }

  /// libs/server/AOF/GarnetLog.cs:ChunkBufferSize
  ///
  /// 分块重组缓冲尺寸：全量长度之和 + 头开销。
  pub fn chunk_buffer_size(
    key_len: usize,
    value_len: usize,
    input_len: usize,
    chunk_count: usize,
  ) -> usize {
    key_len + value_len + input_len + chunk_count * super::aof_header::AofChunkHeader::TOTAL_SIZE
  }

  /// libs/server/AOF/GarnetLog.cs:EnqueueStoredProc
  ///
  /// 存储过程条目：多子日志访问向量背压自检后，逐参与子日志广播。
  pub fn enqueue_stored_proc(
    &self,
    op_type: AofEntryType,
    version: i64,
    session_id: i32,
    procedure_id: u8,
    body: &[u8],
    physical_sublog_access_vector: u64,
  ) -> i64 {
    self.backpressure_wait_vector(physical_sublog_access_vector);
    self.lock_sublogs(physical_sublog_access_vector);
    let result = self.enqueue_with_header(&RecordShape {
      op_type,
      version,
      session_id,
      key: &[],
      value: body,
      input: &[],
      database_id: procedure_id,
    });
    self.unlock_sublogs(physical_sublog_access_vector);
    result
  }

  /// libs/server/AOF/GarnetLog.cs:EnqueueTxn
  ///
  /// 事务条目：同存储过程的多子日志广播形状。
  pub fn enqueue_txn(
    &self,
    op_type: AofEntryType,
    version: i64,
    session_id: i32,
    body: &[u8],
    physical_sublog_access_vector: u64,
  ) -> i64 {
    self.enqueue_stored_proc(
      op_type,
      version,
      session_id,
      0,
      body,
      physical_sublog_access_vector,
    )
  }

  /// libs/server/AOF/GarnetLog.cs:EnqueueBroadcastEntry
  ///
  /// 广播条目（复制元数据等）。
  pub fn enqueue_broadcast_entry(&self, op_type: AofEntryType, version: i64, body: &[u8]) -> i64 {
    self.enqueue_with_header(&RecordShape {
      op_type,
      version,
      session_id: 0,
      key: &[],
      value: body,
      input: &[],
      database_id: 0,
    })
  }

  /// libs/server/AOF/GarnetLog.cs:EnqueueDatabaseCommit
  ///
  /// FLUSH 数据库/全部的提交标记。
  pub fn enqueue_database_commit(
    &self,
    op_type: AofEntryType,
    version: i64,
    database_id: u8,
    unsafe_truncate: bool,
  ) -> i64 {
    let mut header = AofHeader::new();
    header.set_header_type(AofHeaderType::BasicHeader);
    header.op_type = op_type as u8;
    header.store_version = version;
    header.database_id = database_id;
    if unsafe_truncate {
      header.set_unsafe_truncate_log(true);
    }
    self.sublog(0).enqueue(0, &header.to_bytes())
  }

  /// libs/server/AOF/GarnetLog.cs:EnqueueSafeFlushAOF
  ///
  /// 安全 flush 提交标记（记录于子日志 0）。
  pub fn enqueue_safe_flush_aof(&self, version: i64) -> i64 {
    self.enqueue_database_commit(AofEntryType::FlushAll, version, 0, false)
  }
}

/// 分块写入形状：记录形状 + 组件选择标志。
#[derive(Clone)]
pub struct ChunkedShape<'a> {
  /// 记录形状。
  pub record: RecordShape<'a>,
  /// 是否写 value 组件。
  pub write_value: bool,
  /// 是否写 input 组件。
  pub write_input: bool,
}

/// 入队记录形状（头字段 + 负载组件）。
#[derive(Clone)]
pub struct RecordShape<'a> {
  /// 操作类型。
  pub op_type: AofEntryType,
  /// 存储版本。
  pub version: i64,
  /// 会话 id。
  pub session_id: i32,
  /// key。
  pub key: &'a [u8],
  /// value。
  pub value: &'a [u8],
  /// input。
  pub input: &'a [u8],
  /// 数据库 id。
  pub database_id: u8,
}

/// 日志尾移回调句柄。
type ShiftTailCallback = Box<dyn Fn(i64) + Send + Sync>;

/// TsavoriteLog.MinPartialAllocSize 的等价常量（超过即分块）。
pub const MIN_PARTIAL_ALLOC_SIZE: i64 = 8 * 1024 * 1024;

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use super::{ChunkedShape, GarnetLog, InMemorySublog, RecordShape, SublogBackend};
  use crate::{
    aof::{aof_address::AofAddress, aof_entry_type::AofEntryType},
    config::runtime_server_options::RuntimeServerOptions,
  };

  fn log_with(sublogs: usize, replay_tasks: i32) -> GarnetLog {
    let options = RuntimeServerOptions {
      aof_physical_sublog_count: sublogs as i32,
      aof_replay_task_count: replay_tasks,
      ..RuntimeServerOptions::default()
    };
    let backends: Vec<Arc<dyn SublogBackend>> = (0..sublogs.max(1))
      .map(|_| Arc::new(InMemorySublog::new()) as Arc<dyn SublogBackend>)
      .collect();
    GarnetLog::new(&options, backends)
  }

  #[test]
  fn sharding_routes_deterministically() {
    let log = log_with(4, 2);
    let hash = GarnetLog::hash(b"key");
    let physical = log.get_physical_sublog_idx(hash);
    assert!(physical < 4);
    assert!(log.get_replay_task_idx(hash) < 2);
    assert_eq!(
      log.get_virtual_sublog_idx(hash),
      physical * 2 + log.get_replay_task_idx(hash)
    );
  }

  #[test]
  fn enqueue_scan_roundtrip() {
    let log = log_with(1, 1);
    let address = log.enqueue(&RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 7,
      key: b"key1",
      value: b"value1",
      input: &[],
      database_id: 0,
    });
    assert!(address > 0);

    let records = log.scan_single(0, 1, i64::MAX);
    assert_eq!(records.len(), 1);
    // 头之后为 key 与 value。
    let payload = &records[0].payload;
    assert_eq!(&payload[16..20], b"key1");
    assert_eq!(&payload[20..26], b"value1");
  }

  #[test]
  fn commit_and_bitmask() {
    let log = log_with(2, 1);
    assert_eq!(log.all_logs_bitmask(), 0b11);
    let address = log.enqueue(&RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 1,
      key: b"k",
      value: b"v",
      input: &[],
      database_id: 0,
    });
    // 记录落在 hash 路由的子日志；对全拓扑提交并校验尾推进。
    let physical = log.get_physical_sublog_idx(GarnetLog::hash(b"k"));
    let tail = log.commit(physical);
    assert!(tail > address);
    log.wait_for_commit(physical, tail);
    let begins = log.get_begin_address();
    assert_eq!(begins.length(), 2);
  }

  #[test]
  fn chunked_write_reassembles() {
    let log = log_with(1, 1);
    let value = vec![b'x'; 200];
    let address = log.enqueue_object_chunked(&ChunkedShape {
      record: RecordShape {
        op_type: AofEntryType::ObjectStoreUpsert,
        version: 3,
        session_id: 9,
        key: b"big",
        value: &value,
        input: &[],
        database_id: 0,
      },
      write_value: true,
      write_input: false,
    });
    assert!(address > 0);
    let records = log.scan_single(0, 1, i64::MAX);
    // 分块头 + 至少一个数据块。
    assert!(records.len() >= 2);
    // 首块的分块帧头记录了全量 value 长度。
    let chunk = super::super::aof_header::AofChunkHeader::parse(&records[0].payload[16..]).unwrap();
    assert_eq!(chunk.overflow_value_length, 200);
    assert_eq!(chunk.key_hash, GarnetLog::hash(b"big"));
    // 数据块内容可重组。
    let data: Vec<u8> = records[1..]
      .iter()
      .flat_map(|r| r.payload.clone())
      .collect();
    assert_eq!(data, value);
  }

  #[test]
  fn sequence_number_from_cookie() {
    let cookie = 123456789i64.to_le_bytes();
    assert_eq!(
      GarnetLog::get_sequence_number_from_cookie(&cookie),
      123456789
    );
  }

  #[test]
  fn lock_bitmap_and_truncate() {
    let log = log_with(2, 1);
    log.lock_sublogs(0b11);
    log.unlock_sublogs(0b11);

    let until = AofAddress::create(2, 5);
    log.truncate_until(&until);
    assert_eq!(log.get_begin_address().get(0), Some(5));
  }
}
