//! 地址向量 / 哈希索引换算 / Unsafe / 扫描面
//! （对标 libs/server/AOF/GarnetLog.cs 的 HASH、Get*Idx、地址属性段、
//! Unsafe* 段、Scan/ScanSingle 段、TruncateUntil 段）。

use waof::{AofAddress, WalRecord, WalScanIterator};
use wdev::SegmentedDevice;

use super::GarnetLog;
use crate::aof::{sharded_log::ShardedLog, single_log::SingleLog};

impl GarnetLog {
  /// libs/server/AOF/GarnetLog.cs:GetSequenceNumberFromCookie
  ///
  /// cookie 头 8 字节即序列号（LE）。
  pub fn get_sequence_number_from_cookie(cookie: &[u8]) -> i64 {
    cookie
      .first_chunk::<8>()
      .map_or(0, |b| i64::from_le_bytes(*b))
  }

  /// 在 garnet 中的相对路径:libs/server/AOF/GarnetLog.cs:HASH
  ///
  /// 键的 64 位分片哈希（aof 域语义点：内部转引 whasher 单点原语
  /// `fast_hash_i64`，与 wkv 一致读 / wtxn 事务锁序跨层同键同哈希）。
  #[inline]
  pub fn hash(key: &[u8]) -> i64 {
    whasher::fast_hash_i64(key)
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
    let mut recover_until = None;
    for sublog in &self.sharded().sublog {
      let cookie = sublog.recovered_cookie()?;
      let latest = Self::get_sequence_number_from_cookie(&cookie.to_le_bytes());
      recover_until = Some(recover_until.map_or(latest, |min: i64| min.min(latest)));
    }
    recover_until
  }

  /// libs/server/AOF/GarnetLog.cs:AllLogsBitmask
  ///
  /// 全部物理子日志的访问位图。
  #[inline]
  pub fn all_logs_bitmask(&self) -> u64 {
    (1u64 << self.size()) - 1
  }

  /// libs/server/AOF/GarnetLog.cs:BeginAddress
  pub fn begin_address(&self) -> AofAddress {
    self.route(SingleLog::begin_address, ShardedLog::begin_address)
  }

  /// libs/server/AOF/GarnetLog.cs:TailAddress
  pub fn tail_address(&self) -> AofAddress {
    self.route(SingleLog::tail_address, ShardedLog::tail_address)
  }

  /// libs/server/AOF/GarnetLog.cs:CommittedUntilAddress
  pub fn committed_until_address(&self) -> AofAddress {
    self.route(
      SingleLog::committed_until_address,
      ShardedLog::committed_until_address,
    )
  }

  /// libs/server/AOF/GarnetLog.cs:CommittedBeginAddress
  ///
  /// 已提交 begin 地址（commit 记录快照，非实时 begin；TsavoriteLog.cs:120）。
  pub fn committed_begin_address(&self) -> AofAddress {
    self.route(
      SingleLog::committed_begin_address,
      ShardedLog::committed_begin_address,
    )
  }

  /// libs/server/AOF/GarnetLog.cs:FlushedUntilAddress
  pub fn flushed_until_address(&self) -> AofAddress {
    self.route(
      SingleLog::flushed_until_address,
      ShardedLog::flushed_until_address,
    )
  }

  /// libs/server/AOF/GarnetLog.cs:MaxMemorySizeBytes
  pub fn max_memory_size_bytes(&self) -> AofAddress {
    self.route(
      SingleLog::max_memory_size_bytes,
      ShardedLog::max_memory_size_bytes,
    )
  }

  /// libs/server/AOF/GarnetLog.cs:MemorySizeBytes
  pub fn memory_size_bytes(&self) -> AofAddress {
    self.route(SingleLog::memory_size_bytes, ShardedLog::memory_size_bytes)
  }

  /// libs/server/AOF/GarnetLog.cs:GetTailAddress
  ///
  /// 单子日志尾地址（背压/复制对齐路径）。
  #[inline]
  pub fn get_tail_address(&self, sublog_idx: usize) -> i64 {
    self.get_sub_log(sublog_idx).tail_address()
  }

  /// libs/server/AOF/GarnetLog.cs:GetBeginAddress
  ///
  /// 单子日志 begin 地址。
  #[inline]
  pub fn get_begin_address(&self, sublog_idx: usize) -> i64 {
    self.get_sub_log(sublog_idx).begin_address()
  }

  /// libs/server/AOF/GarnetLog.cs:ScanSingle
  ///
  /// 单子日志区间闭包扫描（零全量 Vec 分配）。
  #[inline]
  pub fn scan_single_with(
    &self,
    sublog_idx: usize,
    begin_address: i64,
    end_address: i64,
    f: impl FnMut(&WalRecord) -> bool,
  ) {
    self
      .get_sub_log(sublog_idx)
      .scan_with(begin_address, end_address, f);
  }

  /// ScanSingle 的全量设备面流式闭包扫描（跨环形窗口与历史磁盘段；恢复链路权威入口）。
  #[inline]
  pub async fn scan_single_async_with<'a, E, F, Fut>(
    &'a self,
    sublog_idx: usize,
    begin_address: i64,
    end_address: i64,
    f: F,
  ) -> Result<(), E>
  where
    F: FnMut(WalRecord) -> Fut + 'a,
    Fut: Future<Output = Result<bool, E>> + 'a,
    E: 'a,
  {
    self
      .get_sub_log(sublog_idx)
      .scan_async_with(begin_address, end_address, f)
      .await
  }

  /// ScanSingle 的全量设备面迭代器直取（零闭包形态；副本重放热路径扁平
  /// 直驱；闭包形态见 [`Self::scan_single_async_with`]）。
  #[inline]
  pub fn scan_single_iter(
    &self,
    sublog_idx: usize,
    begin_address: i64,
    end_address: i64,
  ) -> WalScanIterator<SegmentedDevice> {
    self
      .get_sub_log(sublog_idx)
      .scan_iter(begin_address, end_address)
  }

  /// libs/server/AOF/GarnetLog.cs:UnsafeGetLogPageSizeBits
  #[inline]
  pub fn unsafe_get_log_page_size_bits(&self) -> i32 {
    self.get_sub_log(0).log_page_size_bits()
  }

  /// libs/server/AOF/GarnetLog.cs:UnsafeShiftBeginAddress（truncateLog: true 形态）
  ///
  /// 全 AOF 域唯一的单子日志物理回收真身：平移子日志 begin 并即时删段。
  /// 页界/段界钳制与 `min(committed_until)` 一致性全部交由 [`WaofSublog::truncate_until_async`]
  /// → `WalLog::truncate` 的 `until` 入参承担（设备面单点，AOF 域不另算页界）；
  /// C# 的 `snapToPageStart` / `truncateLog` 两开关在 rust 单一口径下无存活空间
  /// ——截断恒为物理、页对齐恒由设备负责，故不引入永假形参。
  pub async fn unsafe_shift_begin_address(&self, sublog_idx: usize, new_begin: i64) {
    self
      .get_sub_log(sublog_idx)
      .truncate_until_async(new_begin)
      .await;
  }

  /// libs/server/AOF/GarnetLog.cs:TruncateUntil（收敛到唯一物理回收真身的向量形态）
  ///
  /// 逐子日志转调 [`GarnetLog::unsafe_shift_begin_address`]：全仓唯一的 AOF 段物理
  /// 回收链（begin 前移 + `min(committed)` 钳制 + 设备删段）；截断后 `aof_size` /
  /// `total_size`（tail-begin）读数与磁盘段数一致，`checkpoint_if_aof_exceeds` 体积闸成立。
  pub async fn truncate_until_async(&self, until: &AofAddress) {
    for i in 0..self.size() {
      self
        .unsafe_shift_begin_address(i, until.get(i).unwrap_or(0))
        .await;
    }
  }
}

/// 物理子日志 × 回放任务 → 虚拟子日志下标（aof 域共享内核换算，单点公式）。
///
/// 公式真源为 GarnetAppendOnlyFile.cs 的 GetVirtualSublogIdx（第 63 行，
/// `replayTaskCount` 取 `serverOptions.AofReplayTaskCount`）；C#
/// ReadConsistencyManager 无自有方法，经注入的 appendOnlyFile 同名调用——
/// rust RCM 不持日志句柄，改经本自由函数同源换算。哈希形态见
/// [`GarnetLog::get_virtual_sublog_idx`]。
#[inline]
pub fn virtual_sublog_idx(sublog_idx: usize, replay_idx: usize, replay_task_count: usize) -> usize {
  sublog_idx * replay_task_count + replay_idx
}
