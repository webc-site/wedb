//! waof 块设备物理子日志（唯一具体子日志类型，对标 C# TsavoriteLog 与
//! StoreWrapper.CommitTaskAsync 常驻提交任务的合体面）。

use std::{
  ops::Deref,
  sync::{
    Arc, OnceLock,
    atomic::{AtomicI64, AtomicU64, Ordering},
  },
  thread,
};

use compio::runtime::Runtime;
use crossfire::{AsyncRx, MAsyncTx, mpsc};
use event_listener::Event;
use waof::{NO_COOKIE, WalLog, WalRecord, WalScanIterator};
use wconf::RuntimeServerOptions;
use wdev::{Device, SegmentedDevice};

use super::garnet_log::GarnetLog;

/// 生产子日志具体类型：设备恒为 [`wdev::SegmentedDevice`]（单测走轻量
/// 真实段文件，同类型），SingleLog / ShardedLog / GarnetLog 全链持有此类型
pub type AofSublog = WaofSublog<SegmentedDevice>;

/// 单物理日志域装配工厂（C# StoreWrapper 构造期 appendOnlyFile 单点装配的
/// rust 形态：TsavoriteLog 设备 ← GarnetLog 路由 ← GarnetAppendOnlyFile 门面）。
///
/// 唯一权威路由：数据记录（Enqueue*）、提交标记（EnqueueDatabaseCommit/
/// EnqueueBroadcastEntry）与 checkpoint 截断/刷盘/恢复全部经同一物理
/// [`WalLog`]（对标 C# `db.AppendOnlyFile.Log` 与
/// `storeWrapper.appendOnlyFile.Log` 为同一日志实例）。
///
/// 尺寸口径：三旋钮（aof-memory / aof-page-size / aof-segment-size）的组合体检
/// 在子日志路由装配处单点执行（[`GarnetLog::new`] → [`super::AofSettings`]），
/// 体检投影出的物理尺寸由物理日志构造口经 [`super::AofSettings::wal_config`]
/// 装载——装配口收到的是已定容的 [`WalLog`]，故本工厂不改物理窗口；boot 侧把
/// 体检前移到设备分配之前与 C# 同序（禁第二套体检）。
pub fn single_log_aof(
  wal: Arc<WalLog<wdev::SegmentedDevice>>,
  options: &RuntimeServerOptions,
) -> crate::Result<Arc<super::garnet_append_only_file::GarnetAppendOnlyFile>> {
  let backend: Arc<AofSublog> = Arc::new(WaofSublog::new(wal));
  Ok(Arc::new(
    super::garnet_append_only_file::GarnetAppendOnlyFile::new(
      Arc::new(GarnetLog::new(options, vec![backend], None)?),
      options,
      None,
    ),
  ))
}

/// 基于 `waof::WalLog` 的物理子日志设备后端。
pub struct WaofSublog<D: Device> {
  wal: Arc<WalLog<D>>,
  /// 最后提交 cookie（[`NO_COOKIE`] 哨兵 = 无提交记录；进程内视图，
  /// 跨重启自 commit 元数据帧回填）。
  cookie: AtomicI64,
  /// 已提交 begin 快照（C# TsavoriteLog.CommittedBeginAddress：commit 记录
  /// 写出的 begin 值；初值 1 = FirstValidAddress，进程内恢复链由
  /// safe_initialize 以 begin 参数恢复）。
  committed_begin: AtomicI64,
  /// 常驻提交协程的折叠信号发送端（容量 1，满即折叠——并发提交合并为
  /// 一次刷盘，对标 libs/server/StoreWrapper.cs:CommitTaskAsync 的
  /// 常驻提交任务与 ongoing commit 合并语义）。首次提交装配。
  commit_wake: OnceLock<MAsyncTx<mpsc::Array<()>>>,
  /// 提交请求目标地址（fetch_max 折叠并发提交请求；常驻协程排空信号后
  /// 一次 commit_to 覆盖全部积压）
  commit_request: Arc<AtomicU64>,
  /// 刷盘推进完成通知（等待提交落盘的同步等待者单点事件源；对标
  /// C# TsavoriteLog 内部 CommitEvent 的等待面）
  flush_event: Arc<Event>,
  /// 物理刷盘失败累计（commit_flush_async 错误留痕的可见性计数：error 日志
  /// 之外可观测的运维面，仅计数不推断——失败后数据是否落盘以恢复面实测为准）
  flush_failures: AtomicU64,
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
    // committed_begin 初值 = 设备首地址（C# TsavoriteLog.CommittedBeginAddress
    // 构造归 FirstValidAddress，TsavoriteLog.cs:244）
    let begin = wal.begin_address() as i64;
    Self {
      wal,
      cookie: AtomicI64::new(NO_COOKIE),
      committed_begin: AtomicI64::new(begin),
      commit_wake: OnceLock::new(),
      commit_request: Arc::new(AtomicU64::new(0)),
      flush_event: Arc::new(Event::new()),
      flush_failures: AtomicU64::new(0),
    }
  }

  /// 物理刷盘失败累计（运维可见性计数，见字段文档）
  pub fn flush_failures(&self) -> u64 {
    self.flush_failures.load(Ordering::Relaxed)
  }

  /// 刷盘推进事件源（同步 wait_for_commit 的挂起点；先注册 listener 再复查
  /// 提交水位，无丢唤醒）
  pub fn flush_event(&self) -> &Event {
    &self.flush_event
  }

  /// 装配常驻提交协程（一次性）：容量 1 折叠信号驱动，排空信号后以
  /// 积压的最大请求位点一次 `commit_to`（GroupCommitPipeline 内继续
  /// 合并并发提交）。无运行时上下文（测试）时以独立线程 + 独立运行时
  /// 承载，一次性成本。
  fn ensure_committer(&self) {
    let (tx, rx): (_, AsyncRx<mpsc::Array<()>>) = mpsc::bounded_async(1);
    if self.commit_wake.set(tx).is_err() {
      return;
    }
    if let Some(rt) = Runtime::try_current() {
      let wal = Arc::clone(&self.wal);
      let request = Arc::clone(&self.commit_request);
      let flush_event = Arc::clone(&self.flush_event);
      rt.spawn(committer_loop(wal, request, rx, flush_event))
        .detach();
    } else {
      // 无运行时上下文（测试装配）：刷盘步进刻意不加 Send 上界（compio
      // thread-per-core），future 不可跨线程迁移，协程须在独立线程内建的
      // 运行时上原地 block_on 驱动。通道即关停信号：WaofSublog 析构 →
      // commit_wake 释放 → rx.recv 出错返回 → 协程与线程一并退出
      //（与运行期分支同口径，不设关停标志字段）
      let wal = Arc::clone(&self.wal);
      let request = Arc::clone(&self.commit_request);
      let flush_event = Arc::clone(&self.flush_event);
      thread::spawn(move || {
        if let Ok(rt) = Runtime::new() {
          rt.block_on(committer_loop(wal, request, rx, flush_event));
        }
      });
    }
  }
}

/// 常驻提交协程主循环：容量 1 折叠信号驱动，排空信号后以积压最大请求
/// 位点一次 commit_to（内部 GroupCommitPipeline 继续合并并发提交）
async fn committer_loop<D: Device>(
  wal: Arc<WalLog<D>>,
  request: Arc<AtomicU64>,
  rx: AsyncRx<mpsc::Array<()>>,
  flush_event: Arc<Event>,
) {
  loop {
    if rx.recv().await.is_err() {
      return;
    }
    // 排空折叠信号：积压的多次提交由单次 commit_to 覆盖
    while rx.try_recv().is_ok() {}
    let target = request.load(Ordering::Acquire);
    if let Err(err) = wal.commit_to(target).await {
      log::error!("WaofSublog 常驻提交协程刷盘失败: {err:?}");
    }
    // 成败均通知：等待者醒来复查提交水位（失败时避免永久沉睡）
    flush_event.notify(usize::MAX);
  }
}

impl<D: Device> WaofSublog<D> {
  /// 失败显式上抛（BufferFull/RecordTooLarge），由调用链终端拒绝命令——
  /// 杜绝「主存写入成功而 AOF 缺条目」的静默丢数据与主从发散
  pub fn enqueue(&self, payload: &[u8]) -> waof::Result<i64> {
    self.wal.enqueue(payload).map(|addr| addr as i64)
  }

  /// 分部件直通零整包拼接（WalLog::enqueue_parts 产出页与 enqueue 逐字节一致）
  pub fn enqueue_parts(&self, parts: &[&[u8]]) -> waof::Result<i64> {
    self.wal.enqueue_parts(parts).map(|addr| addr as i64)
  }

  /// 多帧原子落盘直通（WalLog::enqueue_frames 单次预留连续地址，帧间不插花）
  pub fn enqueue_frames(&self, frames: &[&[&[u8]]]) -> waof::Result<i64> {
    self.wal.enqueue_frames(frames).map(|addr| addr as i64)
  }

  pub fn tail_address(&self) -> i64 {
    self.wal.tail_address() as i64
  }

  pub fn begin_address(&self) -> i64 {
    self.wal.begin_address() as i64
  }

  pub fn committed_until_address(&self) -> i64 {
    self.wal.committed_until_address() as i64
  }

  /// 提交并触发刷盘：cookie/帧元数据同步落账后以容量 1 折叠信号唤醒
  /// 常驻提交协程（杜绝每次提交 spawn 刷盘任务的任务创建开销；并发提交
  /// 由折叠信号 + commit_to 内 GroupCommitPipeline 双层合并）
  pub fn commit(&self, until_address: i64, cookie: i64) {
    self.cookie.store(cookie, Ordering::Release);
    // commit 记录写出 begin 快照（C# TsavoriteLog.WriteCommitMetadata：
    // info.BeginAddress = BeginAddress，TsavoriteLog.cs:2696）
    self
      .committed_begin
      .store(self.wal.begin_address() as i64, Ordering::Release);
    // commit 元数据帧随批携带 cookie（WaofSublog 层仅写不透明槽位）
    self.wal.set_pending_cookie(cookie);

    let target = if until_address <= 0 {
      self.wal.safe_tail_address()
    } else {
      until_address as u64
    };
    self.commit_request.fetch_max(target, Ordering::AcqRel);
    // 已装配直发折叠信号；首调装配后补发（热路径零 channel 构造）
    if self.commit_wake.get().is_some() {
      if let Some(tx) = self.commit_wake.get() {
        let _ = tx.try_send(());
      }
    } else {
      self.ensure_committer();
      if let Some(tx) = self.commit_wake.get() {
        let _ = tx.try_send(());
      }
    }
  }

  pub fn recovered_cookie(&self) -> Option<i64> {
    let cookie = self.cookie.load(Ordering::Acquire);
    (cookie != NO_COOKIE).then_some(cookie)
  }

  pub fn flushed_until_address(&self) -> i64 {
    self.wal.flushed_until_address() as i64
  }

  /// 内存窗口同步扫描：帧解析单点在 [`WalLog::scan_memory_records`]（零二次堆分配）
  pub fn scan_with(&self, begin_address: i64, end_address: i64, f: impl FnMut(&WalRecord) -> bool) {
    let start = begin_address.max(0) as u64;
    let end = end_address.max(0) as u64;
    if !self.wal.scan_memory_records(start, end, f) {
      log::warn!(
        "WaofSublog::scan_with 请求地址 {start} 已超出环形缓冲区容量，同步接口只覆盖内存窗口，恢复链路须用 scan_async_with"
      );
    }
  }

  /// 全量流式扫描：`WalScanIterator` 透明跨内存环形窗口与历史磁盘段
  ///（对标 C# TsavoriteLog.Scan 与 BulkConsumeAll 的设备面恢复扫描），恢复链路权威入口。
  pub async fn scan_async_with<'a, E, F, Fut>(
    &'a self,
    begin_address: i64,
    end_address: i64,
    mut f: F,
  ) -> Result<(), E>
  where
    F: FnMut(WalRecord) -> Fut + 'a,
    Fut: Future<Output = Result<bool, E>> + 'a,
    E: 'a,
  {
    let start = begin_address.max(0) as u64;
    let end = end_address.max(0) as u64;
    let begin = self.wal.begin_address();
    let mut iter = self.wal.scan(start.max(begin), end);
    loop {
      match iter.next().await {
        Ok(Some(rec)) => {
          if !f(rec).await? {
            break;
          }
        }
        Ok(None) => break,
        // 缺数据优于错数据：扫描 IO 异常显式报告并终止（已收集区间可用）
        Err(err) => {
          log::error!(
            "WaofSublog::scan_async_with 磁盘段读取失败 @ {addr:#x}: {err:?}",
            addr = iter.current_address()
          );
          break;
        }
      }
    }
    if iter.overwritten_skips() > 0 {
      log::error!(
        "WaofSublog::scan_async_with 扫描中 {skipped} 条未提交记录被环形覆写，区间数据不完整",
        skipped = iter.overwritten_skips()
      );
    }
    Ok(())
  }

  /// 全量流式扫描迭代器直取（[`Self::scan_async_with`] 的零闭包形态）：
  /// 副本重放热路径直驱——逐记录应用由调用方扁平循环直 await 承担，消
  /// 逐记录 async 块包装与跨闭包记账的原子操作（消费侧的对位是 C# 重放
  /// 驱动页内扁平循环，非本口）。begin 钳制由 [`WalLog::scan`] 内部单点承担。
  ///
  /// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:ScanSingle
  pub fn scan_iter(&self, begin_address: i64, end_address: i64) -> WalScanIterator<D> {
    self
      .wal
      .scan(begin_address.max(0) as u64, end_address.max(0) as u64)
  }

  /// 物理截断：位点平移 + 设备段回收（WalLog::truncate 持提交锁串行化）。
  pub async fn truncate_until_async(&self, new_begin: i64) {
    let until = new_begin.max(0) as u64;
    if let Err(err) = self.wal.truncate(until).await {
      log::error!("WaofSublog 物理截断失败 (until={until}): {err:?}");
    }
  }

  /// 物理刷盘提交：环形缓冲 → 设备（await 形态，检查点等须刷盘完成语义
  /// 的路径走此入口；commit 元数据帧随批尾写出）。
  pub async fn commit_flush_async(&self, cookie: i64) {
    self.cookie.store(cookie, Ordering::Release);
    // commit 记录写出 begin 快照（C# TsavoriteLog.WriteCommitMetadata：
    // info.BeginAddress = BeginAddress，TsavoriteLog.cs:2696）
    self
      .committed_begin
      .store(self.wal.begin_address() as i64, Ordering::Release);
    self.wal.set_pending_cookie(cookie);
    if let Err(err) = self.wal.commit().await {
      let total = self.flush_failures.fetch_add(1, Ordering::Relaxed) + 1;
      log::error!("WaofSublog 物理刷盘失败(累计 {total}): {err:?}");
    }
    self.flush_event.notify(usize::MAX);
  }

  /// 设备面恢复：扫描磁盘段定位尾位点并预载环形窗口（WalLog::recover），
  /// 并把 commit 元数据帧收敛出的 cookie/begin 快照回填进程内视图
  ///（C# TsavoriteLog.Initialize 自提交记录回填 CommittedBeginAddress 与
  /// RecoveredCookie 的对应面）。
  pub async fn recover_async(&self) {
    if let Err(err) = self.wal.recover().await {
      log::error!("WaofSublog 设备面恢复失败: {err:?}");
      return;
    }
    let cookie = self.wal.recovered_cookie();
    if cookie != NO_COOKIE {
      self.cookie.store(cookie, Ordering::Release);
    }
    self.committed_begin.store(
      self.wal.recovered_committed_begin() as i64,
      Ordering::Release,
    );
  }

  /// 日志页位（物理日志设置的页容量取位；AOF 分块写入的片上界由此而来）
  ///
  /// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:UnsafeGetLogPageSizeBits
  pub fn log_page_size_bits(&self) -> i32 {
    self.wal.config().page_size.trailing_zeros() as i32
  }

  /// 容量上限：环形缓冲窗口字节数（C# TsavoriteLog.MaxMemorySizeBytes 的
  /// 磁盘形态 = MaxAllocatedPageCount * PageSize，waof 定长窗口即总容量；
  /// aof-memory 旋钮经 AofSettings 投影进物理日志设置后此值即配置生效值）。
  pub fn max_memory_size_bytes(&self) -> i64 {
    self.wal.config().buffer_size as i64
  }

  /// 当前占用：环形窗口有效字节数 = tail - begin（C# TsavoriteLog.MemorySizeBytes
  /// "Actual memory used by log" 的 waof 承载形态；截断后随 begin 前移收缩）。
  pub fn memory_size_bytes(&self) -> i64 {
    (self
      .wal
      .tail_address()
      .saturating_sub(self.wal.begin_address())) as i64
  }

  pub fn committed_begin_address(&self) -> i64 {
    self.committed_begin.load(Ordering::Acquire)
  }

  /// 重置日志：cookie 复位 + 转发权威 [`WalLog::reset`]
  /// （设备面复位——持提交锁原子复位位点 + sync_data 串行化，C# 映射
  /// 单点在 WalLog::reset；原手抄四原子无锁复位，与在途 commit_to
  /// 并发会撕裂位点，已删）。
  pub async fn reset_async(&self) {
    self.cookie.store(NO_COOKIE, Ordering::Release);
    if let Err(err) = self.wal.reset().await {
      log::error!("WaofSublog 日志重置失败: {err:?}");
      return;
    }
    // CommittedBeginAddress 归 FirstValidAddress（C# TsavoriteLog.Reset，
    // TsavoriteLog.cs:244-246）——取设备复位后的首地址，绝不取复位前快照
    self
      .committed_begin
      .store(self.wal.begin_address() as i64, Ordering::Release);
  }

  pub fn safe_initialize(
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

  pub async fn wait_for_commit_async(&self, until_address: i64) {
    let target = until_address.max(0) as u64;
    if let Err(err) = self.wal.wait_for_commit(target).await {
      log::error!("WaofSublog 等待提交落盘失败: {err:?}");
    }
  }
}

#[cfg(test)]
mod tests {
  use std::{
    thread,
    time::{Duration, Instant},
  };

  use crate::aof::test_support::test_sublog;

  /// 无运行时装配的回退提交线程：commit 后刷盘推进至尾（回退支在独立
  /// 线程内建运行时上 block_on 驱动常驻提交协程；析构后通道关闭、
  /// 协程返回、线程随之退出，杜绝永久挂起的 OS 线程泄漏）
  #[test]
  fn fallback_committer_flushes_without_runtime() {
    let (_dir, sublog) = test_sublog("waof_sublog_fallback");
    let addr = sublog.enqueue(b"fallback_record").unwrap();
    sublog.commit(addr, 0);
    // 回退线程异步刷盘：限时轮询等待 flushed 推进至提交位点
    let deadline = Instant::now() + Duration::from_secs(5);
    while sublog.flushed_until_address() <= addr {
      assert!(Instant::now() < deadline, "回退提交线程未在限时内推进刷盘");
      thread::sleep(Duration::from_millis(5));
    }
  }
}
