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
  future::Future,
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
  },
  thread,
};

use parking_lot::Mutex;
use waof::{AofAddress, AofEntryType};

use super::{
  aof_backpressure::AofBackpressure,
  aof_header::{
    AofHeader, AofHeaderType, AofShardedHeader, AofShardedLogTransactionHeader,
    AofSingleLogTransactionHeader, REPLAY_TASK_ACCESS_VECTOR_BYTES,
  },
  sequence_number_generator::SequenceNumberGenerator,
  sharded_log::ShardedLog,
  single_log::SingleLog,
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
  fn enqueue(&self, payload: &[u8]) -> i64;
  /// 尾地址（下一记录的写入点）。
  fn tail_address(&self) -> i64;
  /// begin 地址。
  fn begin_address(&self) -> i64;
  /// 已提交地址。
  fn committed_until_address(&self) -> i64;
  /// 推进提交水位（cookie 随提交记录，恢复期经 recovered_cookie 读回）。
  fn commit(&self, until_address: i64, cookie: i64);
  /// 恢复出的最后一次提交 cookie（无提交记录 / 未持久化即 None）。
  fn recovered_cookie(&self) -> Option<i64>;
  /// 刷盘水位。
  fn flushed_until_address(&self) -> i64;
  /// 扫描 [begin, end) 区间记录（内存拓扑同步快路径；磁盘拓扑恢复链路
  /// 须用 [`Self::scan_async`]，跨环形窗口的历史磁盘段不在此接口覆盖面）。
  fn scan(&self, begin_address: i64, end_address: i64) -> Vec<LogRecord>;
  /// 全量扫描（跨内存环形窗口与历史磁盘段；对标 C# TsavoriteLog.Scan 的
  /// 设备面恢复扫描）。默认回落同步 [`Self::scan`]（内存后端等价）。
  fn scan_async(
    &self,
    begin_address: i64,
    end_address: i64,
  ) -> impl Future<Output = Vec<LogRecord>> + '_ {
    async move { self.scan(begin_address, end_address) }
  }
  /// 截断 begin 至指定地址（日志平移）。
  fn shift_begin_address(&self, new_begin: i64);
  /// 物理截断（位点平移 + 设备段文件回收；对标 C# TsavoriteLog.TruncateUntil
  /// 的设备删除面）。默认回落同步位点平移（内存后端等价）。
  fn truncate_until_async(&self, new_begin: i64) -> impl Future<Output = ()> + '_ {
    async move { self.shift_begin_address(new_begin) }
  }
  /// 物理刷盘提交（对标 C# TsavoriteLog.CommitAsync 的持久化面）。默认回落
  /// 同步位点提交（内存后端等价）。
  fn commit_flush_async(&self, cookie: i64) -> impl Future<Output = ()> + '_ {
    let tail = self.tail_address();
    async move { self.commit(tail, cookie) }
  }
  /// 设备面恢复（对标 C# TsavoriteLog.RecoverAsync：扫描磁盘段定位尾位点）。
  /// 默认空操作（内存后端无设备态）。
  fn recover_async(&self) -> impl Future<Output = ()> + '_ {
    async {}
  }
  /// 页大小位。
  fn log_page_size_bits(&self) -> i32;
  /// 内存占用。
  fn memory_size_bytes(&self) -> i64;
  /// 重置日志。
  fn reset(&self);
  /// 安全初始化子日志位点（对标 C# TsavoriteLog.SafeInitialize / Initialize）。
  fn safe_initialize(&self, begin_address: i64, committed_until_address: i64, last_commit_num: i64);
  /// 异步等待提交落盘至指定地址（0 表示等待当前尾地址；对标 C# TsavoriteLog.WaitForCommitAsync）。
  fn wait_for_commit_async(&self, until_address: i64) -> impl Future<Output = ()> + '_ {
    let target = if until_address == 0 {
      self.tail_address()
    } else {
      until_address
    };
    async move {
      while self.committed_until_address() < target {
        thread::yield_now();
      }
    }
  }
}

/// 内建内存子日志（测试与无盘场景）。
#[derive(Default)]
pub struct InMemorySublog {
  records: parking_lot::Mutex<Vec<LogRecord>>,
  begin: AtomicI64,
  committed_until: AtomicI64,
  /// 最后提交 cookie（i64::MIN 哨兵 = 无提交记录）。
  cookie: AtomicI64,
}

/// 无提交 cookie 哨兵。
pub(crate) const NO_COOKIE: i64 = i64::MIN;

impl InMemorySublog {
  /// 空日志，起始地址 1（对齐 TsavoriteLog 初始地址约定）。
  pub fn new() -> Self {
    Self {
      records: parking_lot::Mutex::new(Vec::new()),
      begin: AtomicI64::new(1),
      committed_until: AtomicI64::new(1),
      cookie: AtomicI64::new(NO_COOKIE),
    }
  }
}

impl SublogBackend for InMemorySublog {
  fn enqueue(&self, payload: &[u8]) -> i64 {
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

  fn tail_address(&self) -> i64 {
    self.records.lock().last().map_or_else(
      || {
        self
          .committed_until
          .load(Ordering::Relaxed)
          .max(self.begin.load(Ordering::Relaxed))
      },
      |last| last.address + last.payload.len() as i64,
    )
  }

  fn begin_address(&self) -> i64 {
    self.begin.load(Ordering::Relaxed)
  }

  fn committed_until_address(&self) -> i64 {
    self.committed_until.load(Ordering::Relaxed)
  }

  fn commit(&self, until_address: i64, cookie: i64) {
    self
      .committed_until
      .fetch_max(until_address, Ordering::Release);
    self.cookie.store(cookie, Ordering::Release);
  }

  fn recovered_cookie(&self) -> Option<i64> {
    let cookie = self.cookie.load(Ordering::Acquire);
    (cookie != NO_COOKIE).then_some(cookie)
  }

  fn flushed_until_address(&self) -> i64 {
    self.committed_until.load(Ordering::Relaxed)
  }

  fn scan(&self, begin_address: i64, end_address: i64) -> Vec<LogRecord> {
    let records = self.records.lock();
    let start_idx = records.partition_point(|r| r.address < begin_address);
    records[start_idx..]
      .iter()
      .take_while(|r| r.address < end_address)
      .cloned()
      .collect()
  }

  fn shift_begin_address(&self, new_begin: i64) {
    self.begin.store(new_begin, Ordering::Release);
  }

  fn log_page_size_bits(&self) -> i32 {
    22
  }

  fn memory_size_bytes(&self) -> i64 {
    self
      .records
      .lock()
      .iter()
      .map(|r| r.payload.len() as i64)
      .sum()
  }

  fn reset(&self) {
    self.records.lock().clear();
    self.begin.store(1, Ordering::Release);
    self.committed_until.store(1, Ordering::Release);
    self.cookie.store(NO_COOKIE, Ordering::Release);
  }

  fn safe_initialize(
    &self,
    begin_address: i64,
    committed_until_address: i64,
    last_commit_num: i64,
  ) {
    let mut records = self.records.lock();
    records.retain(|r| r.address >= begin_address && r.address < committed_until_address);
    self.begin.store(begin_address, Ordering::Release);
    self
      .committed_until
      .store(committed_until_address, Ordering::Release);
    if last_commit_num > 0 {
      self.cookie.store(last_commit_num, Ordering::Release);
    }
  }
}

/// 单日志 / 分片双拓扑容器（对标 libs/server/AOF/GarnetLog.cs:GarnetLog）。
pub struct GarnetLog {
  /// 单物理日志包装（AofPhysicalSublogCount == 1 拓扑）。
  single_log: Option<SingleLog>,
  /// 分片物理日志集合（AofPhysicalSublogCount > 1 拓扑）。
  sharded_log: Option<ShardedLog>,
  /// 物理子日志数。
  physical_sublog_count: usize,
  /// 回放任务数。
  replay_task_count: usize,
  /// 单日志拓扑标志（1 物理 × 1 回放）。
  using_single_log: bool,
  /// 单物理日志拓扑标志（1 物理子日志）。
  using_single_physical_log: bool,
  /// 主侧背压闸门（可选）。
  backpressure: Option<Arc<AofBackpressure>>,
  /// 日志平移回调（尾地址前移通知）。
  shift_tail_callback: Mutex<Option<ShiftTailCallback>>,
  /// 分片模式序列号生成器（C# appendOnlyFile.seqNumGen 共享引用）。
  seq_num_gen: Option<Arc<SequenceNumberGenerator>>,
  /// AofAutoCommit（C# 派生属性 CommitFrequencyMs == 0）。
  auto_commit: bool,
}

impl GarnetLog {
  /// libs/server/AOF/GarnetLog.cs:GarnetLog（构造）。
  ///
  /// 依选项决定单日志（1 物理 1 回放）或分片拓扑，并构造背压闸门。
  /// `seq_num_gen` 仅多物理日志模式传入（C# 由 appendOnlyFile 构造并共享）。
  pub fn new(
    server_options: &RuntimeServerOptions,
    backends: Vec<Arc<super::sublog::Sublog>>,
    seq_num_gen: Option<Arc<SequenceNumberGenerator>>,
  ) -> Self {
    let physical_sublog_count = server_options.aof_physical_sublog_count.max(1) as usize;
    let replay_task_count = server_options.aof_replay_task_count.max(1) as usize;
    let using_single_log = physical_sublog_count == 1 && replay_task_count == 1;
    let using_single_physical_log = physical_sublog_count == 1;

    let (single_log, sharded_log) = if using_single_physical_log {
      let single = backends
        .into_iter()
        .next()
        .expect("单物理日志拓扑需至少 1 个后端");
      (Some(SingleLog::new(single)), None)
    } else {
      (None, Some(ShardedLog::new(backends)))
    };

    Self {
      single_log,
      sharded_log,
      physical_sublog_count,
      replay_task_count,
      using_single_log,
      using_single_physical_log,
      backpressure: Some(Arc::new(AofBackpressure::new(
        physical_sublog_count,
        server_options.aof_sync_max_lag_bytes,
      ))),
      shift_tail_callback: Mutex::new(None),
      seq_num_gen: if physical_sublog_count > 1 {
        seq_num_gen
      } else {
        None
      },
      auto_commit: server_options.commit_frequency_ms == 0,
    }
  }

  /// 单日志拓扑（1 物理 × 1 回放）。
  #[inline]
  pub fn using_single_log(&self) -> bool {
    self.using_single_log
  }

  /// 单物理日志拓扑（含单日志 + 单物理多回放）。
  #[inline]
  pub fn using_single_physical_log(&self) -> bool {
    self.using_single_physical_log
  }

  /// 分片序列号取号（C# appendOnlyFile.seqNumGen.GetSequenceNumber）。
  fn next_sequence_number(&self) -> i64 {
    self
      .seq_num_gen
      .as_ref()
      .map_or(0, |g| g.get_sequence_number())
  }

  /// 背压闸门句柄（C# 经 appendOnlyFile.backpressure 共享）。
  #[inline]
  pub fn backpressure(&self) -> Option<&Arc<AofBackpressure>> {
    self.backpressure.as_ref()
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
  /// 键的 64 位分片哈希。
  #[inline]
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

  /// 恢复时以各子日志提交 cookie 收敛恢复上限（C# RecoverLatestSequenceNumber）。
  pub fn recover_latest_sequence_number(&self) -> Option<i64> {
    if self.using_single_physical_log {
      return Some(-1);
    }
    let sharded = self.sharded_log.as_ref()?;
    let mut recover_until = None;
    for sublog in &sharded.sublog {
      let cookie = sublog.recovered_cookie()?;
      let latest = Self::get_sequence_number_from_cookie(&cookie.to_le_bytes());
      recover_until = Some(recover_until.map_or(latest, |min: i64| min.min(latest)));
    }
    recover_until
  }

  /// 拓扑的物理子日志总数（C# Size 属性）。
  #[inline]
  pub fn size(&self) -> usize {
    if self.using_single_physical_log {
      1
    } else {
      self.sharded_log.as_ref().map_or(1, |s| s.len())
    }
  }

  /// 回放任务数（C# ReplayTaskCount）。
  #[inline]
  pub fn replay_task_count(&self) -> usize {
    self.replay_task_count
  }

  /// 日志头尺寸（C# HeaderSize）。
  #[inline]
  pub fn header_size(&self) -> i64 {
    if let Some(s) = &self.single_log {
      s.header_size()
    } else {
      self.sharded_log.as_ref().unwrap().header_size()
    }
  }

  /// libs/server/AOF/GarnetLog.cs:AllLogsBitmask
  ///
  /// 全部物理子日志的访问位图。
  #[inline]
  pub fn all_logs_bitmask(&self) -> u64 {
    (1u64 << self.size()) - 1
  }

  /// libs/server/AOF/GarnetLog.cs:LockSublogs
  ///
  /// 入队操作前的子日志位图锁（慢路径，慎用）。
  #[inline]
  pub fn lock_sublogs(&self, log_access_bitmap: u64) {
    if let Some(sharded) = &self.sharded_log {
      sharded.lock_sublogs(log_access_bitmap);
    }
  }

  /// libs/server/AOF/GarnetLog.cs:UnlockSublogs
  #[inline]
  pub fn unlock_sublogs(&self, log_access_bitmap: u64) {
    if let Some(sharded) = &self.sharded_log {
      sharded.unlock_sublogs(log_access_bitmap);
    }
  }

  /// libs/server/AOF/GarnetLog.cs:GetSubLog
  ///
  /// 指定子日志后端。
  #[inline]
  pub fn get_sub_log(&self, sublog_idx: usize) -> &Arc<super::sublog::Sublog> {
    if let Some(single) = &self.single_log {
      debug_assert_eq!(sublog_idx, 0);
      single.log()
    } else {
      self.sharded_log.as_ref().unwrap().get_sub_log(sublog_idx)
    }
  }

  /// libs/server/AOF/GarnetLog.cs:GetBeginAddress
  #[inline]
  pub fn get_begin_address(&self) -> AofAddress {
    self.begin_address()
  }

  /// libs/server/AOF/GarnetLog.cs:BeginAddress
  pub fn begin_address(&self) -> AofAddress {
    if let Some(single) = &self.single_log {
      single.begin_address()
    } else {
      self.sharded_log.as_ref().unwrap().begin_address()
    }
  }

  /// libs/server/AOF/GarnetLog.cs:TailAddress
  pub fn tail_address(&self) -> AofAddress {
    if let Some(single) = &self.single_log {
      single.tail_address()
    } else {
      self.sharded_log.as_ref().unwrap().tail_address()
    }
  }

  /// libs/server/AOF/GarnetLog.cs:CommittedUntilAddress
  pub fn committed_until_address(&self) -> AofAddress {
    if let Some(single) = &self.single_log {
      single.committed_until_address()
    } else {
      self.sharded_log.as_ref().unwrap().committed_until_address()
    }
  }

  /// libs/server/AOF/GarnetLog.cs:CommittedBeginAddress
  pub fn committed_begin_address(&self) -> AofAddress {
    if let Some(single) = &self.single_log {
      single.committed_begin_address()
    } else {
      self.sharded_log.as_ref().unwrap().committed_begin_address()
    }
  }

  /// libs/server/AOF/GarnetLog.cs:FlushedUntilAddress
  pub fn flushed_until_address(&self) -> AofAddress {
    if let Some(single) = &self.single_log {
      single.flushed_until_address()
    } else {
      self.sharded_log.as_ref().unwrap().flushed_until_address()
    }
  }

  /// libs/server/AOF/GarnetLog.cs:MaxMemorySizeBytes
  pub fn max_memory_size_bytes(&self) -> AofAddress {
    if let Some(single) = &self.single_log {
      single.max_memory_size_bytes()
    } else {
      self.sharded_log.as_ref().unwrap().max_memory_size_bytes()
    }
  }

  /// libs/server/AOF/GarnetLog.cs:MemorySizeBytes
  pub fn memory_size_bytes(&self) -> AofAddress {
    if let Some(single) = &self.single_log {
      single.memory_size_bytes()
    } else {
      self.sharded_log.as_ref().unwrap().memory_size_bytes()
    }
  }

  /// 单子日志尾地址（背压/复制对齐路径）。
  #[inline]
  pub fn get_tail_address(&self, sublog_idx: usize) -> i64 {
    if let Some(single) = &self.single_log {
      debug_assert_eq!(sublog_idx, 0);
      single.log().tail_address()
    } else {
      self
        .sharded_log
        .as_ref()
        .unwrap()
        .get_tail_address(sublog_idx)
    }
  }

  /// 单子日志 begin 地址。
  #[inline]
  pub fn get_sublog_begin_address(&self, sublog_idx: usize) -> i64 {
    if let Some(single) = &self.single_log {
      debug_assert_eq!(sublog_idx, 0);
      single.log().begin_address()
    } else {
      self
        .sharded_log
        .as_ref()
        .unwrap()
        .get_begin_address(sublog_idx)
    }
  }

  /// libs/server/AOF/GarnetLog.cs:SetLogShiftTailCallback
  ///
  /// 注册尾地址前移回调（复制对齐通知）。
  pub fn set_log_shift_tail_callback(&self, callback: ShiftTailCallback) {
    *self.shift_tail_callback.lock() = Some(callback);
  }

  /// libs/server/AOF/GarnetLog.cs:ScanSingle
  ///
  /// 单子日志区间扫描。
  #[inline]
  pub fn scan_single(
    &self,
    sublog_idx: usize,
    begin_address: i64,
    end_address: i64,
  ) -> Vec<LogRecord> {
    self
      .get_sub_log(sublog_idx)
      .scan(begin_address, end_address)
  }

  /// ScanSingle 的全量设备面形态（跨环形窗口与历史磁盘段；恢复链路权威入口）。
  #[inline]
  pub async fn scan_single_async(
    &self,
    sublog_idx: usize,
    begin_address: i64,
    end_address: i64,
  ) -> Vec<LogRecord> {
    self
      .get_sub_log(sublog_idx)
      .scan_async(begin_address, end_address)
      .await
  }

  /// 设备面恢复（C# RecoverAsync 路由：逐物理子日志扫描磁盘段定位位点）。
  pub async fn recover_async(&self) {
    if let Some(single) = &self.single_log {
      single.recover_async().await;
    } else if let Some(sharded) = &self.sharded_log {
      sharded.recover_async().await;
    }
  }

  /// 重置日志（C# Reset 路由：逐物理子日志回退内存位点；FLUSHDB/FLUSHALL 出口）。
  pub fn reset(&self) {
    if let Some(single) = &self.single_log {
      single.reset();
    } else if let Some(sharded) = &self.sharded_log {
      sharded.reset();
    }
  }

  /// libs/server/AOF/GarnetLog.cs:CommitAsync
  ///
  /// 物理刷盘提交全部物理子日志（逐子日志刷盘推进，分片拓扑分发全局单调 cookie 序列号）。
  pub async fn commit_async(&self) {
    if let Some(single) = &self.single_log {
      single.log().commit_flush_async(NO_COOKIE).await;
      return;
    }
    let cookie = self.next_sequence_number();
    if let Some(sharded) = &self.sharded_log {
      for sublog in &sharded.sublog {
        sublog.commit_flush_async(cookie).await;
      }
    }
  }

  /// 物理刷盘提交（转发至 commit_async）。
  #[inline]
  pub async fn commit_flush_async(&self) {
    self.commit_async().await;
  }

  /// libs/server/AOF/GarnetLog.cs:UnsafeGetLogPageSizeBits
  #[inline]
  pub fn unsafe_get_log_page_size_bits(&self) -> i32 {
    self.get_sub_log(0).log_page_size_bits()
  }

  /// libs/server/AOF/GarnetLog.cs:UnsafeGetReadOnlyAddressAbove
  ///
  /// 高于 `address` 的只读安全地址（内存拓扑即尾地址）。
  #[inline]
  pub fn unsafe_get_read_only_address_above(&self, sublog_idx: usize, address: i64) -> i64 {
    self.get_tail_address(sublog_idx).max(address)
  }

  /// libs/server/AOF/GarnetLog.cs:UnsafeShiftBeginAddress
  ///
  /// 平移子日志 begin 地址并通知尾移回调。
  pub fn unsafe_shift_begin_address(&self, sublog_idx: usize, new_begin: i64) {
    self.get_sub_log(sublog_idx).shift_begin_address(new_begin);
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
        .get_sub_log(i)
        .shift_begin_address(until.get(i).unwrap_or(0));
    }
  }

  /// TruncateUntil 的物理形态（位点平移 + 设备段回收 + 尾移回调；对标 C#
  /// TsavoriteLog.TruncateUntil 的设备删除面，checkpoint / SafeTruncateAOF 出口）。
  pub async fn truncate_until_async(&self, until: &AofAddress) {
    for i in 0..self.size() {
      self
        .get_sub_log(i)
        .truncate_until_async(until.get(i).unwrap_or(0))
        .await;
    }
    if let Some(callback) = self.shift_tail_callback.lock().as_ref() {
      callback(until.get(0).unwrap_or(0));
    }
  }

  /// libs/server/AOF/GarnetLog.cs:SafeInitialize
  ///
  /// 安全初始化指定物理子日志位点。
  pub fn safe_initialize(
    &self,
    sublog_idx: usize,
    begin_address: i64,
    committed_until_address: i64,
    last_commit_num: i64,
  ) {
    if let Some(single) = &self.single_log {
      debug_assert_eq!(sublog_idx, 0);
      single.safe_initialize(begin_address, committed_until_address, last_commit_num);
    } else {
      self.sharded_log.as_ref().unwrap().safe_initialize(
        sublog_idx,
        begin_address,
        committed_until_address,
        last_commit_num,
      );
    }
  }

  /// libs/server/AOF/GarnetLog.cs:Initialize
  pub fn initialize(
    &self,
    begin_address: &AofAddress,
    committed_until_address: &AofAddress,
    last_commit_num: i64,
  ) {
    if let Some(single) = &self.single_log {
      single.initialize(
        begin_address[0],
        committed_until_address[0],
        last_commit_num,
      );
    } else {
      self.sharded_log.as_ref().unwrap().initialize(
        begin_address,
        committed_until_address,
        last_commit_num,
      );
    }
  }

  /// libs/server/AOF/GarnetLog.cs:InitializeIf
  ///
  /// 条件初始化：当子日志尾位点落后于恢复出的安全 AOF 地址时，将位点推进至安全位点。
  pub fn initialize_if(&self, recovered_safe_aof_address: &AofAddress) {
    if let Some(single) = &self.single_log {
      let tail = self.get_tail_address(0);
      let safe_addr = recovered_safe_aof_address.get(0).unwrap_or(0);
      if tail < safe_addr {
        single.safe_initialize(tail, safe_addr, 0);
      }
    } else if let Some(sharded) = &self.sharded_log {
      for i in 0..sharded.len() {
        let tail = self.get_tail_address(i);
        let safe_addr = recovered_safe_aof_address.get(i).unwrap_or(0);
        if tail < safe_addr {
          sharded.get_sub_log(i).safe_initialize(tail, safe_addr, 0);
        }
      }
    }
  }

  /// libs/server/AOF/GarnetLog.cs:WaitForCommit
  ///
  /// 阻塞直至提交水位达到 `address`（0 表示等待当前尾地址）。
  pub fn wait_for_commit(&self, sublog_idx: usize, address: i64) {
    let target = if address == 0 {
      self.get_tail_address(sublog_idx)
    } else {
      address
    };
    while self.get_sub_log(sublog_idx).committed_until_address() < target {
      thread::yield_now();
    }
  }

  /// libs/server/AOF/GarnetLog.cs:Commit
  ///
  /// 提交全部物理子日志；分片拓扑以同一 cookie（序列号）随各子日志提交，
  /// 恢复期经 [`Self::recover_latest_sequence_number`] 收敛恢复上界。
  pub fn commit(&self) {
    if let Some(single) = &self.single_log {
      let tail = single.log().tail_address();
      single.log().commit(tail, NO_COOKIE);
      return;
    }
    let cookie = self.next_sequence_number();
    if let Some(sharded) = &self.sharded_log {
      for sublog in &sharded.sublog {
        let tail = sublog.tail_address();
        sublog.commit(tail, cookie);
      }
    }
  }

  /// libs/server/AOF/GarnetLog.cs:WaitForCommitAsync
  ///
  /// 异步等待全部物理子日志提交落盘（0 表示等待当前尾地址；对标 C# GarnetLog.WaitForCommitAsync）。
  pub async fn wait_for_commit_async(&self, until_address: i64) {
    if let Some(single) = &self.single_log {
      single.log().wait_for_commit_async(until_address).await;
      return;
    }
    if let Some(sharded) = &self.sharded_log {
      for sublog in &sharded.sublog {
        sublog.wait_for_commit_async(until_address).await;
      }
    }
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
    let header_size = if self.using_single_physical_log() {
      AofHeader::TOTAL_SIZE
    } else {
      AofShardedHeader::TOTAL_SIZE
    };
    let value_len_prefix = if op_type.has_chunk_value() {
      KEY_LEN_PREFIX_SIZE
    } else {
      0
    };
    let mut payload = Vec::with_capacity(
      header_size + KEY_LEN_PREFIX_SIZE + key.len() + value_len_prefix + value.len() + input.len(),
    );
    let physical_sublog_idx;
    if self.using_single_physical_log() {
      physical_sublog_idx = 0;
      let mut header = AofHeader::new();
      header.set_header_type(AofHeaderType::BasicHeader);
      header.op_type = op_type as u8;
      header.store_version = version;
      header.session_id = session_id;
      header.database_id = database_id;
      payload.extend_from_slice(&header.to_bytes());
    } else {
      physical_sublog_idx = self.get_physical_sublog_idx(Self::hash(key));
      let mut header = AofHeader::new();
      header.set_header_type(AofHeaderType::ShardedHeader);
      header.op_type = op_type as u8;
      header.store_version = version;
      header.session_id = session_id;
      header.database_id = database_id;
      let sharded = AofShardedHeader {
        basic: header,
        sequence_number: self.next_sequence_number(),
      };
      payload.extend_from_slice(&sharded.to_bytes());
    }
    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
    payload.extend_from_slice(key);
    if op_type.has_chunk_value() {
      payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
      payload.extend_from_slice(value);
    }
    payload.extend_from_slice(input);
    let address = self.get_sub_log(physical_sublog_idx).enqueue(&payload);
    if self.auto_commit {
      self.commit();
    }
    address
  }

  /// libs/server/AOF/GarnetLog.cs:Enqueue（upsert/RMW/delete 通用形状）
  ///
  /// 背压等待 → 大记录自动分块（组件选择按 op 类型）→ 头编码入队。
  pub fn enqueue(&self, record: &RecordShape<'_>) -> i64 {
    self.backpressure_wait_key(record.key);
    if Self::is_chunkable(record.key.len(), record.value.len(), record.input.len()) {
      return self.enqueue_span_chunked(&ChunkedShape {
        record: record.clone(),
        write_value: record.op_type.has_chunk_value(),
        write_input: record.op_type.has_chunk_input(),
      });
    }
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
    let using_single_physical_log = self.using_single_physical_log();
    let header_size = if using_single_physical_log {
      AofHeader::TOTAL_SIZE
    } else {
      AofShardedHeader::TOTAL_SIZE
    };

    let page_payload = (1usize << self.unsafe_get_log_page_size_bits() as u32)
      - header_size
      - super::aof_header::AofChunkHeader::TOTAL_SIZE;
    let mut chunks: Vec<Vec<u8>> = Vec::new();
    let mut emit = |mut chunk: Vec<u8>, key: &[u8], value: &[u8]| {
      chunk.extend_from_slice(key);
      chunk.extend_from_slice(value);
      chunks.push(chunk);
    };

    let mut first =
      Vec::with_capacity(header_size + super::aof_header::AofChunkHeader::TOTAL_SIZE + key.len());
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
      let sequence_number = self.next_sequence_number();
      first.extend_from_slice(&sequence_number.to_le_bytes());
    }
    first.extend_from_slice(&chunk_header.to_bytes());
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
      address = self.get_sub_log(physical_sublog_idx).enqueue(&chunk);
    }
    if self.auto_commit {
      self.commit();
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
  /// 存储过程条目：单日志 BasicHeader / 单物理多回放轻量事务头 /
  /// 分片事务头逐参与子日志广播（位图逐子日志盖入，参与者计数随行）。
  pub fn enqueue_stored_proc(
    &self,
    op_type: AofEntryType,
    version: i64,
    session_id: i32,
    procedure_id: u8,
    body: &[u8],
    access: &SublogAccess<'_>,
  ) -> i64 {
    if self.using_single_physical_log() {
      if let Some(gate) = &self.backpressure {
        gate.wait(0, self.get_tail_address(0));
      }
    } else {
      self.backpressure_wait_vector(access.physical_vector);
    }

    let address = if self.using_single_log() {
      let mut header = AofHeader::new();
      header.set_header_type(AofHeaderType::BasicHeader);
      header.op_type = op_type as u8;
      header.procedure_id = procedure_id;
      header.store_version = version;
      header.session_id = session_id;
      let header_bytes = header.to_bytes();
      if body.is_empty() {
        self.get_sub_log(0).enqueue(&header_bytes)
      } else {
        let mut payload = Vec::with_capacity(header_bytes.len() + body.len());
        payload.extend_from_slice(&header_bytes);
        payload.extend_from_slice(body);
        self.get_sub_log(0).enqueue(&payload)
      }
    } else if self.using_single_physical_log() {
      let mut header = AofHeader::new();
      header.set_header_type(AofHeaderType::SingleLogTransactionHeader);
      header.op_type = op_type as u8;
      header.procedure_id = procedure_id;
      header.store_version = version;
      header.session_id = session_id;
      let txn_header = AofSingleLogTransactionHeader {
        basic: header,
        participant_count: access.participant_count as i16,
        replay_task_access_vector: access
          .virtual_vectors
          .first()
          .copied()
          .unwrap_or([0; REPLAY_TASK_ACCESS_VECTOR_BYTES]),
      };
      let txn_bytes = txn_header.to_bytes();
      if body.is_empty() {
        self.get_sub_log(0).enqueue(&txn_bytes)
      } else {
        let mut payload = Vec::with_capacity(txn_bytes.len() + body.len());
        payload.extend_from_slice(&txn_bytes);
        payload.extend_from_slice(body);
        self.get_sub_log(0).enqueue(&payload)
      }
    } else {
      let mut header = AofHeader::new();
      header.set_header_type(AofHeaderType::ShardedLogTransactionHeader);
      header.op_type = op_type as u8;
      header.procedure_id = procedure_id;
      header.store_version = version;
      header.session_id = session_id;
      let txn_header = AofShardedLogTransactionHeader {
        sharded: AofShardedHeader {
          basic: header,
          sequence_number: self.next_sequence_number(),
        },
        participant_count: access.participant_count as i16,
        replay_task_access_vector: [0; REPLAY_TASK_ACCESS_VECTOR_BYTES],
      };
      self.lock_sublogs(access.physical_vector);
      let mut address = 0;
      let mut vector = access.physical_vector;
      while vector > 0 {
        let sublog_idx = vector.trailing_zeros() as usize;
        vector &= vector - 1;
        let mut txn_header = txn_header;
        txn_header.replay_task_access_vector = access
          .virtual_vectors
          .get(sublog_idx)
          .copied()
          .unwrap_or([0; REPLAY_TASK_ACCESS_VECTOR_BYTES]);
        let txn_bytes = txn_header.to_bytes();
        address = if body.is_empty() {
          self.get_sub_log(sublog_idx).enqueue(&txn_bytes)
        } else {
          let mut payload = Vec::with_capacity(txn_bytes.len() + body.len());
          payload.extend_from_slice(&txn_bytes);
          payload.extend_from_slice(body);
          self.get_sub_log(sublog_idx).enqueue(&payload)
        };
      }
      self.unlock_sublogs(access.physical_vector);
      address
    };

    if self.auto_commit {
      self.commit();
    }
    address
  }

  /// libs/server/AOF/GarnetLog.cs:EnqueueTxn
  ///
  /// 事务标记条目：与存储过程同形状（无过程 id、空体）。
  #[inline]
  pub fn enqueue_txn(
    &self,
    op_type: AofEntryType,
    version: i64,
    session_id: i32,
    access: &SublogAccess<'_>,
  ) -> i64 {
    self.enqueue_stored_proc(op_type, version, session_id, 0, &[], access)
  }

  /// libs/server/AOF/GarnetLog.cs:EnqueueBroadcastEntry
  ///
  /// 全子日志广播条目：须对所有回放任务可见的标记（数据库提交、
  /// safe-flush、checkpoint），位图全置位、参与者 = 虚拟子日志总数。
  fn enqueue_broadcast_entry(&self, basic_header: AofHeader) -> i64 {
    if self.using_single_physical_log() {
      if let Some(gate) = &self.backpressure {
        gate.wait(0, self.get_tail_address(0));
      }
    } else {
      self.backpressure_wait_vector(self.all_logs_bitmask());
    }

    if self.using_single_log() {
      let payload = basic_header.to_bytes();
      return self.get_sub_log(0).enqueue(&payload);
    }
    if self.using_single_physical_log() {
      let mut basic = basic_header;
      basic.set_header_type(AofHeaderType::SingleLogTransactionHeader);
      let txn_header = AofSingleLogTransactionHeader {
        basic,
        participant_count: (self.physical_sublog_count * self.replay_task_count) as i16,
        replay_task_access_vector: [0xFF; REPLAY_TASK_ACCESS_VECTOR_BYTES],
      };
      let payload = txn_header.to_bytes();
      return self.get_sub_log(0).enqueue(&payload);
    }
    let mut basic = basic_header;
    basic.set_header_type(AofHeaderType::ShardedLogTransactionHeader);
    let txn_header = AofShardedLogTransactionHeader {
      sharded: AofShardedHeader {
        basic,
        sequence_number: self.next_sequence_number(),
      },
      participant_count: (self.physical_sublog_count * self.replay_task_count) as i16,
      replay_task_access_vector: [0xFF; REPLAY_TASK_ACCESS_VECTOR_BYTES],
    };
    let physical_sublog_access_vector = self.all_logs_bitmask();
    self.lock_sublogs(physical_sublog_access_vector);
    let mut address = 0;
    let mut vector = physical_sublog_access_vector;
    let payload = txn_header.to_bytes();
    while vector > 0 {
      let sublog_idx = vector.trailing_zeros() as usize;
      vector &= vector - 1;
      address = self.get_sub_log(sublog_idx).enqueue(&payload);
    }
    self.unlock_sublogs(physical_sublog_access_vector);
    if self.auto_commit {
      self.commit();
    }
    address
  }

  /// libs/server/AOF/GarnetLog.cs:EnqueueDatabaseCommit
  ///
  /// FLUSH 数据库/全部的提交标记（广播，sessionID = -1）。
  pub fn enqueue_database_commit(&self, op_type: AofEntryType, version: i64) -> i64 {
    let mut header = AofHeader::new();
    header.set_header_type(AofHeaderType::BasicHeader);
    header.op_type = op_type as u8;
    header.store_version = version;
    header.session_id = -1;
    self.enqueue_broadcast_entry(header)
  }

  /// libs/server/AOF/GarnetLog.cs:EnqueueSafeFlushAOF
  ///
  /// 安全 flush 提交标记（广播，storeVersion = 0、sessionID = -1）。
  pub fn enqueue_safe_flush_aof(
    &self,
    op_type: AofEntryType,
    unsafe_truncate_log: bool,
    db_id: u8,
  ) -> i64 {
    let mut header = AofHeader::new();
    header.set_header_type(AofHeaderType::BasicHeader);
    header.op_type = op_type as u8;
    header.store_version = 0;
    header.session_id = -1;
    header.database_id = db_id;
    if unsafe_truncate_log {
      header.set_unsafe_truncate_log(true);
    }
    self.enqueue_broadcast_entry(header)
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

use wtxn::SublogAccess;

impl wtxn::TxnAofLog for GarnetLog {
  #[inline]
  fn size(&self) -> usize {
    self.size()
  }

  #[inline]
  fn replay_task_count(&self) -> usize {
    self.replay_task_count()
  }

  #[inline]
  fn get_physical_sublog_idx(&self, key_hash: i64) -> usize {
    self.get_physical_sublog_idx(key_hash)
  }

  #[inline]
  fn get_replay_task_idx(&self, key_hash: i64) -> usize {
    self.get_replay_task_idx(key_hash)
  }

  #[inline]
  fn enqueue_txn(
    &self,
    op_type: waof::AofEntryType,
    txn_version: i64,
    session_id: i32,
    access: &SublogAccess<'_>,
  ) {
    self.enqueue_txn(op_type, txn_version, session_id, access);
  }

  #[inline]
  fn enqueue_stored_proc(
    &self,
    op_type: waof::AofEntryType,
    txn_version: i64,
    session_id: i32,
    proc_id: u8,
    payload: &[u8],
    access: &SublogAccess<'_>,
  ) {
    self.enqueue_stored_proc(op_type, txn_version, session_id, proc_id, payload, access);
  }
}

/// 日志尾移回调句柄。
type ShiftTailCallback = fn(i64);

/// SpanByte 长度前缀字节数（C# SpanByte 头 4B 长度）。
pub const KEY_LEN_PREFIX_SIZE: usize = 4;

/// TsavoriteLog.MinPartialAllocSize 的等价常量（超过即分块；C# = 1 << 20）。
pub const MIN_PARTIAL_ALLOC_SIZE: i64 = 1 << 20;

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use waof::{AofAddress, AofEntryType};

  use super::*;
  use crate::{
    aof::{SequenceNumberGenerator, sublog::Sublog},
    config::runtime_server_options::RuntimeServerOptions,
  };

  fn log_with(sublogs: usize, replay_tasks: i32) -> GarnetLog {
    let options = RuntimeServerOptions {
      aof_physical_sublog_count: sublogs as i32,
      aof_replay_task_count: replay_tasks,
      ..RuntimeServerOptions::default()
    };
    let backends: Vec<Arc<Sublog>> = (0..sublogs.max(1))
      .map(|_| Arc::new(Sublog::Mem(InMemorySublog::new())))
      .collect();
    let seq_num_gen = (sublogs > 1).then(|| Arc::new(SequenceNumberGenerator::new(0)));
    GarnetLog::new(&options, backends, seq_num_gen)
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
    let payload = &records[0].payload;
    assert_eq!(&payload[16..20], &4u32.to_le_bytes());
    assert_eq!(&payload[20..24], b"key1");
    assert_eq!(&payload[24..28], &6u32.to_le_bytes());
    assert_eq!(&payload[28..34], b"value1");
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
    let physical = log.get_physical_sublog_idx(GarnetLog::hash(b"k"));
    log.commit();
    let tail = log.get_tail_address(physical);
    assert!(tail > address);
    log.wait_for_commit(physical, tail);
    let begins = log.get_begin_address();
    assert_eq!(begins.length(), 2);
  }

  #[test]
  fn broadcast_writes_all_sublogs_with_txn_headers() {
    let log = log_with(2, 1);
    log.enqueue_database_commit(AofEntryType::FlushAll, 7);
    for i in 0..2 {
      let records = log.scan_single(i, 1, i64::MAX);
      assert_eq!(records.len(), 1, "子日志 {i} 须有广播条目");
      let header = super::AofHeader::parse(&records[0].payload).unwrap();
      assert_eq!(header.session_id, -1);
      assert_eq!(
        header.header_type(),
        Some(super::AofHeaderType::ShardedLogTransactionHeader)
      );
    }
  }

  #[test]
  fn recover_until_converges_from_commit_cookies() {
    let log = log_with(2, 1);
    assert_eq!(log.recover_latest_sequence_number(), None);
    log.enqueue(&RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 1,
      key: b"k",
      value: b"v",
      input: &[],
      database_id: 0,
    });
    log.commit();
    assert!(log.recover_latest_sequence_number().is_some());
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
    assert!(records.len() >= 2);
    let chunk = super::super::aof_header::AofChunkHeader::parse(&records[0].payload[16..]).unwrap();
    assert_eq!(chunk.overflow_value_length, 200);
    assert_eq!(chunk.key_hash, GarnetLog::hash(b"big"));
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

  #[test]
  fn test_garnet_log_advanced_methods() {
    let log = log_with(2, 1);
    let sz = GarnetLog::chunk_buffer_size(10, 20, 0, 1);
    assert!(sz > 0);

    let safe_addr = AofAddress::create(2, 10);
    log.initialize_if(&safe_addr);
    assert!(log.enqueue_safe_flush_aof(AofEntryType::CheckpointStartCommit, false, 100) >= 0);

    // 回调预留地址入参
    log.set_log_shift_tail_callback(|_addr| {});
    let addr = log.unsafe_get_read_only_address_above(0, 100);
    assert!(addr >= 100);
    log.unsafe_shift_begin_address(0, 100);
  }
}
