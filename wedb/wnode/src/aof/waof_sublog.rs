//! waof 块设备子日志后端适配器
//! （承接 TsavoriteLog / waof::WalLog 与 GarnetLog::SublogBackend 的接线）。

use std::{
  ops::Deref,
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
  },
  thread,
};

use compio::runtime::Runtime;
use waof::WalLog;
use wconf::RuntimeServerOptions;
use wdev::Device;

use super::garnet_log::{GarnetLog, LogRecord, NO_COOKIE, SublogBackend};

/// 单物理日志域装配工厂（C# StoreWrapper 构造期 appendOnlyFile 单点装配的
/// rust 形态：TsavoriteLog 设备 ← GarnetLog 路由 ← GarnetAppendOnlyFile 门面）。
///
/// 唯一权威路由：数据记录（Enqueue*）、提交标记（EnqueueDatabaseCommit/
/// EnqueueBroadcastEntry）与 checkpoint 截断/刷盘/恢复全部经同一物理
/// [`WalLog`]（对标 C# `db.AppendOnlyFile.Log` 与
/// `storeWrapper.appendOnlyFile.Log` 为同一日志实例）。
pub fn single_log_aof(
  wal: Arc<WalLog<wdev::SegmentedDevice>>,
  options: &RuntimeServerOptions,
) -> Arc<super::garnet_append_only_file::GarnetAppendOnlyFile> {
  let backend = Arc::new(super::sublog::Sublog::Waof(WaofSublog::new(wal)));
  Arc::new(super::garnet_append_only_file::GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(options, vec![backend], None)),
    options,
    None,
  ))
}

/// 基于 `waof::WalLog` 的物理子日志设备后端。
pub struct WaofSublog<D: Device> {
  wal: Arc<WalLog<D>>,
  /// 最后提交 cookie（i64::MIN 哨兵 = 无提交记录）。
  ///
  /// WalLog 无 commit 元数据持久化区（刻意架构差异，见 WalLog::recover 文档），
  /// cookie 仅进程内可见；跨重启的恢复上界由调用方按扫描终点界定。
  cookie: AtomicI64,
  /// 已提交 begin 快照（C# TsavoriteLog.CommittedBeginAddress：commit 记录
  /// 写出的 begin 值；初值 1 = FirstValidAddress，进程内恢复链由
  /// safe_initialize 以 begin 参数恢复）。
  committed_begin: AtomicI64,
}

impl<D: Device> Deref for WaofSublog<D> {
  type Target = WalLog<D>;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.wal
  }
}

impl<D: Device> WaofSublog<D> {
  /// 创建基于 waof 的子日志后端
  pub fn new(wal: Arc<WalLog<D>>) -> Self {
    Self {
      wal,
      cookie: AtomicI64::new(NO_COOKIE),
      committed_begin: AtomicI64::new(1),
    }
  }
}

impl<D: Device> SublogBackend for WaofSublog<D> {
  fn enqueue(&self, payload: &[u8]) -> i64 {
    match self.wal.enqueue(payload) {
      Ok(addr) => addr as i64,
      Err(err) => {
        log::error!("WaofSublog 日志入队失败: {err:?}");
        -1
      }
    }
  }

  /// 分部件直通零整包拼接（WalLog::enqueue_parts 产出页与 enqueue 逐字节一致）
  fn enqueue_parts(&self, parts: &[&[u8]]) -> i64 {
    match self.wal.enqueue_parts(parts) {
      Ok(addr) => addr as i64,
      Err(err) => {
        log::error!("WaofSublog 日志入队失败: {err:?}");
        -1
      }
    }
  }

  fn tail_address(&self) -> i64 {
    self.wal.tail_address() as i64
  }

  fn begin_address(&self) -> i64 {
    self.wal.begin_address() as i64
  }

  fn committed_until_address(&self) -> i64 {
    self.wal.committed_until_address() as i64
  }

  /// 提交并触发刷盘：
  ///
  /// 底层 wal.commit / wal.commit_to 内部已具备 Leader/Follower 级联 Group Commit 流水线
  /// （CommitPipelineState），自动合并并发提交并由单一 Leader 串行刷盘，外层无需多余的三态 CAS。
  fn commit(&self, until_address: i64, cookie: i64) {
    self.cookie.store(cookie, Ordering::Release);
    // commit 记录写出 begin 快照（C# TsavoriteLog.WriteCommitMetadata：
    // info.BeginAddress = BeginAddress，TsavoriteLog.cs:2696）
    self
      .committed_begin
      .store(self.wal.begin_address() as i64, Ordering::Release);
    let wal = Arc::clone(&self.wal);
    let run_flush = move |wal: Arc<WalLog<D>>| async move {
      let res = if until_address <= 0 {
        wal.commit().await
      } else {
        wal.commit_to(until_address as u64).await
      };
      if let Err(err) = res {
        log::error!("WaofSublog 物理刷盘失败: {err:?}");
      }
    };

    if let Some(rt) = Runtime::try_current() {
      rt.spawn(run_flush(wal)).detach();
    } else {
      thread::spawn(move || {
        if let Ok(rt) = Runtime::new() {
          rt.block_on(run_flush(wal));
        }
      });
    }
  }

  fn recovered_cookie(&self) -> Option<i64> {
    let cookie = self.cookie.load(Ordering::Acquire);
    (cookie != NO_COOKIE).then_some(cookie)
  }

  fn flushed_until_address(&self) -> i64 {
    self.wal.flushed_until_address() as i64
  }

  fn scan(&self, begin_address: i64, end_address: i64) -> Vec<LogRecord> {
    let start = begin_address.max(0) as u64;
    let end = end_address.max(0) as u64;
    let cap = self.wal.ring_buffer.capacity() as u64;
    let safe_tail = self.wal.safe_tail_address();
    let mem_base = self.wal.tail_address();
    let scan_end = end.min(safe_tail);

    let mut records = Vec::new();
    let mut cur = start.max(self.wal.begin_address());

    // 环形缓冲覆盖区间直读（零 I/O 快路径）
    if cur >= mem_base.saturating_sub(cap) {
      while cur + (waof::RECORD_HEADER_LEN as u64) <= scan_end {
        let hdr = self.wal.ring_buffer.read_header(cur);
        let entry_len = hdr.payload_len();
        if hdr.is_zero() || entry_len > self.wal.config().buffer_size {
          break;
        }
        let next_addr = cur + (waof::RECORD_HEADER_LEN as u64) + (entry_len as u64);
        if next_addr > scan_end {
          break;
        }
        let payload = self
          .wal
          .ring_buffer
          .read_vec(cur + waof::RECORD_HEADER_LEN as u64, entry_len);
        if hdr.verify(&payload).is_ok() {
          records.push(LogRecord {
            address: cur as i64,
            payload,
          });
        } else {
          break;
        }
        cur = next_addr;
      }
    } else {
      log::warn!(
        "WaofSublog::scan 请求地址 {cur} 已超出环形缓冲区容量，同步接口只覆盖内存窗口，恢复链路须用 scan_async"
      );
    }
    records
  }

  /// 全量扫描：`WalScanIterator` 透明跨内存环形窗口与历史磁盘段
  ///（对标 C# TsavoriteLog.Scan 的设备面恢复扫描），恢复链路权威入口。
  async fn scan_async(&self, begin_address: i64, end_address: i64) -> Vec<LogRecord> {
    let start = begin_address.max(0) as u64;
    let end = end_address.max(0) as u64;
    let begin = self.wal.begin_address();
    let mut iter = self.wal.scan(start.max(begin), end);
    let mut records = Vec::new();
    loop {
      match iter.next().await {
        Ok(Some(rec)) => records.push(LogRecord {
          address: rec.address as i64,
          payload: rec.payload,
        }),
        Ok(None) => break,
        // 缺数据优于错数据：扫描 IO 异常显式报告并终止（已收集区间可用）
        Err(err) => {
          log::error!(
            "WaofSublog::scan_async 磁盘段读取失败 @ {addr:#x}: {err:?}",
            addr = iter.current_address()
          );
          break;
        }
      }
    }
    if iter.overwritten_skips() > 0 {
      log::error!(
        "WaofSublog::scan_async 扫描中 {skipped} 条未提交记录被环形覆写，区间数据不完整",
        skipped = iter.overwritten_skips()
      );
    }
    records
  }

  /// 物理截断：位点平移 + 设备段回收（WalLog::truncate 持提交锁串行化）。
  async fn truncate_until_async(&self, new_begin: i64) {
    let until = new_begin.max(0) as u64;
    if let Err(err) = self.wal.truncate(until).await {
      log::error!("WaofSublog 物理截断失败 (until={until}): {err:?}");
    }
  }

  /// 物理刷盘提交：环形缓冲 → 设备（WalLog::commit 持提交锁刷盘 + 位点推进）。
  async fn commit_flush_async(&self, cookie: i64) {
    self.cookie.store(cookie, Ordering::Release);
    // commit 记录写出 begin 快照（C# TsavoriteLog.WriteCommitMetadata：
    // info.BeginAddress = BeginAddress，TsavoriteLog.cs:2696）
    self
      .committed_begin
      .store(self.wal.begin_address() as i64, Ordering::Release);
    if let Err(err) = self.wal.commit().await {
      log::error!("WaofSublog 物理刷盘失败: {err:?}");
    }
  }

  /// 设备面恢复：扫描磁盘段定位尾位点并预载环形窗口（WalLog::recover）。
  async fn recover_async(&self) {
    if let Err(err) = self.wal.recover().await {
      log::error!("WaofSublog 设备面恢复失败: {err:?}");
    }
  }

  fn shift_begin_address(&self, new_begin: i64) {
    self
      .wal
      .begin_address
      .fetch_max(new_begin.max(0) as u64, Ordering::SeqCst);
  }

  fn log_page_size_bits(&self) -> i32 {
    self.wal.config().buffer_size.trailing_zeros() as i32
  }

  /// 容量上限：环形缓冲窗口字节数（C# TsavoriteLog.MaxMemorySizeBytes 的
  /// 磁盘形态 = MaxAllocatedPageCount * PageSize，waof 定长窗口即总容量）。
  fn max_memory_size_bytes(&self) -> i64 {
    self.wal.config().buffer_size as i64
  }

  /// 当前占用：环形窗口有效字节数 = tail - begin（C# TsavoriteLog.MemorySizeBytes
  /// "Actual memory used by log" 的 waof 承载形态；截断后随 begin 前移收缩）。
  fn memory_size_bytes(&self) -> i64 {
    (self
      .wal
      .tail_address()
      .saturating_sub(self.wal.begin_address())) as i64
  }

  fn committed_begin_address(&self) -> i64 {
    self.committed_begin.load(Ordering::Acquire)
  }

  /// 重置日志：cookie 复位 + 转发权威 [`WalLog::reset`]
  /// （设备面复位——持提交锁原子复位位点 + sync_data 串行化，C# 映射
  /// 单点在 WalLog::reset；原手抄四原子无锁复位，与在途 commit_to
  /// 并发会撕裂位点，已删）。
  async fn reset_async(&self) {
    self.cookie.store(NO_COOKIE, Ordering::Release);
    // CommittedBeginAddress 归 FirstValidAddress（C# TsavoriteLog.Reset，
    // TsavoriteLog.cs:244-246）
    self.committed_begin.store(1, Ordering::Release);
    if let Err(err) = self.wal.reset().await {
      log::error!("WaofSublog 日志重置失败: {err:?}");
    }
  }

  fn safe_initialize(
    &self,
    begin_address: i64,
    committed_until_address: i64,
    last_commit_num: i64,
  ) {
    if last_commit_num > 0 {
      self.cookie.store(last_commit_num, Ordering::Release);
    } else {
      self.cookie.store(NO_COOKIE, Ordering::Release);
    }
    // 恢复自 commit 记录（C# TsavoriteLog.Initialize：
    // CommittedBeginAddress = beginAddress，TsavoriteLog.cs:528/:596）
    self
      .committed_begin
      .store(begin_address.max(0), Ordering::Release);
    self.wal.safe_initialize(
      begin_address.max(0) as u64,
      committed_until_address.max(0) as u64,
    );
  }

  async fn wait_for_commit_async(&self, until_address: i64) {
    let target = until_address.max(0) as u64;
    if let Err(err) = self.wal.wait_for_commit(target).await {
      log::error!("WaofSublog 等待提交落盘失败: {err:?}");
    }
  }
}
