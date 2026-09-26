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
use event_listener::{Event, Listener};
use waof::{CommitMeta, Error, NO_COOKIE, WalLog, WalRecord, WalScanIterator};
use wbase::{
  backoff::{Backoff, BackoffStage},
  supervise::supervise_task,
};
use wconf::RuntimeServerOptions;
use wdev::{Device, SegmentedDevice};

use super::garnet_log::GarnetLog;
use crate::primary_tasks::PrimaryTasks;

/// 监督快照里的任务名（wbase::supervise 归组键，INFO bg_task_health 可见）
const WAOF_COMMITTER_TASK: &str = "waof_committer";

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
/// 选项口径：`options` 为调用方实参投影的完整 [`RuntimeServerOptions`]（C#
/// GarnetAppendOnlyFile 持完整 GarnetServerOptions 同位），全量透传门面——
/// 背压预算、复制读超时、漂移阈值、子日志/回放拓扑皆自此取值，禁调用方以
/// 缺省组合窄化投影。三尺寸旋钮（aof-memory / aof-page-size / aof-segment-size）
/// 的组合体检唯一在装配口 [`super::AofSettings::from_options`]（先于设备分配，
/// 与 C# GetAofSettings 同序），体检投影出的物理尺寸已由物理日志构造口经
/// [`super::AofSettings::wal_config`] 装载——本工厂收到的是已定容的
/// [`WalLog`]，故不改物理窗口、不设第二道体检（[`GarnetLog::new`] 与 C#
/// 构造子同位零校验）。
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
  /// 之外可观测的运维面，仅计数不推断——失败后数据是否落盘以恢复面实测为准）。
  /// `Arc` 共享给常驻提交协程：协程 `commit_to` 失败同样在此累计，令入队背压
  /// 循环据「非零绝对判定」视作磁盘致命故障并终止（对标 C# cannedException
  /// 一次性置位后永久重抛；提交线程 runtime 创建失败 / panic 退出臂同此累计）
  flush_failures: Arc<AtomicU64>,
  /// 提交落盘角色闸的角色位槽（服务装配期经 [`Self::attach_primary_tasks`]
  /// 注入，与门面/数据库管理器同一 Arc<PrimaryTasks>，勿造第二角色状态源；
  /// `Arc` 共享给常驻提交协程。None = 裸构造形态恒视为主角色）
  primary_tasks: Arc<OnceLock<Arc<PrimaryTasks>>>,
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
      flush_failures: Arc::new(AtomicU64::new(0)),
      primary_tasks: Arc::new(OnceLock::new()),
    }
  }

  /// 刷盘推进事件源（同步 wait_for_commit 的挂起点；先注册 listener 再复查
  /// 提交水位，无丢唤醒）
  pub fn flush_event(&self) -> &Event {
    &self.flush_event
  }

  /// 刷盘失败累计（INFO PERSISTENCE 段 `aof_flush_failures` 行尾项真源；
  /// 非零即常驻提交驱动发生过致命故障，运维可告警）
  pub fn flush_failures(&self) -> u64 {
    self.flush_failures.load(Ordering::Relaxed)
  }

  /// 注入角色状态源（装配期一次，与门面/数据库管理器同一 Arc<PrimaryTasks>；
  /// 重复注入以先到为准）
  pub fn attach_primary_tasks(&self, tasks: Arc<PrimaryTasks>) {
    let _ = self.primary_tasks.set(tasks);
  }

  /// 当前是否副本角色（角色位缺省裸构造 = 主角色）
  #[inline]
  fn is_replica(&self) -> bool {
    self
      .primary_tasks
      .get()
      .is_some_and(|tasks| tasks.is_replica())
  }

  /// 装配常驻提交协程（一次性）：容量 1 折叠信号驱动，排空信号后经
  /// [`drive_commit`] 角色闸以积压的最大请求位点一次刷盘（主端组提交
  /// 合并仍在 GroupCommitPipeline 内）。
  ///
  /// 恒在独立 OS 线程 + 独立 [`Runtime`] 上原地 `block_on` 驱动，绝不复用调用方
  /// 当前运行时（对标 C# `CommitTaskAsync` 常驻后台任务独立于入队线程这一拓扑）。
  /// 理由：入队写路径为同步执行，缓冲满载时 [`Self::enqueue_with_backpressure`]
  /// 以 `flush_event` 阻塞挂起等待刷盘推进——compio 为 thread-per-core 单线程
  /// 运行时，若提交协程与挂起的入队者共处同一 reactor，阻塞入队线程即饿死提交
  /// 协程，水位永不推进，形成同核死锁。独立线程承载刷盘，令背压挂起仅冻结入队者
  /// 本线程、刷盘照常推进并被 `flush_event.notify` 唤醒。
  ///
  /// 通道即关停信号：WaofSublog 析构 → commit_wake 释放 → rx.recv 出错返回 →
  /// 协程与线程一并退出（不设关停标志字段）。
  fn ensure_committer(&self) {
    let (tx, rx): (_, AsyncRx<mpsc::Array<()>>) = mpsc::bounded_async(1);
    if self.commit_wake.set(tx).is_err() {
      return;
    }
    // 刷盘步进不加 Send 上界（compio thread-per-core），future 不可跨线程迁移，
    // 故协程须在独立线程内建的运行时上原地 block_on 驱动
    let wal = Arc::clone(&self.wal);
    let request = Arc::clone(&self.commit_request);
    let flush_event = Arc::clone(&self.flush_event);
    let flush_failures = Arc::clone(&self.flush_failures);
    let primary_tasks = Arc::clone(&self.primary_tasks);
    thread::spawn(move || match Runtime::new() {
      Ok(rt) => {
        // committer_loop 经 wbase supervise_task 顶层监督（单点一次成型）：
        // panic 落 log::error + 监督快照计数后线程退出——等待者按下方失败
        // 信号（绝对判定）收敛，绝不无限挂起
        let _ = rt.block_on(supervise_task(
          WAOF_COMMITTER_TASK,
          committer_loop(wal, request, rx, flush_event, flush_failures, primary_tasks),
        ));
      }
      Err(e) => {
        // 失败臂显式化（对标 C# cannedException：提交线程死亡必转化为全部
        // 等待者的可见错误）：log::error + flush_failures 计数 +
        // flush_event 全量通知（复用 committer_loop 失败通知既有三件套，
        // 零新机制）——runtime 创建失败（线程/句柄耗尽等资源枯竭场景）
        // 零痕退出即背压等待者永挂、写入管线死锁
        log::error!("WaofSublog 常驻提交线程运行时创建失败，刷盘驱动未拉起: {e}");
        flush_failures.fetch_add(1, Ordering::Relaxed);
        flush_event.notify(usize::MAX);
      }
    });
  }
}

/// 提交落盘驱动角色闸（副本一切提交落盘只准走 flush-only 内核——本地 commit
/// 元数据帧仅主端可写，副本 AOF 须为主端流严格镜像，机制约束见
/// [`WalLog::commit_flush_only`]；C# 对偶：CommitTaskAsync 副本分支跳过提交）。
///
/// 单点闸住全部经提交协程与异步提交入口的刷盘驱动：帧写入唯一发生在
/// [`WalLog::commit_to`] 的组提交步内，副本臂改道 commit_flush_only 即绝帧。
async fn drive_commit<D: Device>(
  wal: &WalLog<D>,
  roles: &OnceLock<Arc<PrimaryTasks>>,
  target: u64,
) -> waof::Result<u64> {
  if roles.get().is_some_and(|tasks| tasks.is_replica()) {
    wal.commit_flush_only().await
  } else {
    wal.commit_to(target).await
  }
}

/// 常驻提交协程主循环：容量 1 折叠信号驱动，排空信号后以积压最大请求
/// 位点经 [`drive_commit`] 角色闸一次刷盘覆盖（组提交合并语义仍在 commit_to
/// 的 GroupCommitPipeline 内）。
///
/// 刷盘失败与 [`WaofSublog::settle_flush`] 同口径计入 `flush_failures`：该计数是
/// 入队背压循环对标 C# `cannedException` 的致命故障信号，协程与同步刷盘入口须
/// 共用同一累计源，杜绝两驱动面失败计数不一致使背压等待误判为可恢复而无限挂起
async fn committer_loop<D: Device>(
  wal: Arc<WalLog<D>>,
  request: Arc<AtomicU64>,
  rx: AsyncRx<mpsc::Array<()>>,
  flush_event: Arc<Event>,
  flush_failures: Arc<AtomicU64>,
  primary_tasks: Arc<OnceLock<Arc<PrimaryTasks>>>,
) {
  loop {
    if rx.recv().await.is_err() {
      return;
    }
    // 排空折叠信号：积压的多次提交由单次刷盘覆盖
    while rx.try_recv().is_ok() {}
    let target = request.load(Ordering::Acquire);
    if let Err(err) = drive_commit(&wal, &primary_tasks, target).await {
      flush_failures.fetch_add(1, Ordering::Relaxed);
      log::error!("WaofSublog 常驻提交协程刷盘失败: {err:?}");
    }
    // 成败均通知：等待者醒来复查提交水位（失败时避免永久沉睡）
    flush_event.notify(usize::MAX);
  }
}

impl<D: Device> WaofSublog<D> {
  /// 数据记录入队：对标 C# TsavoriteLog.AllocateBlock 的背压重试语义——
  /// 环形缓冲区瞬时写满时绝不向存储层上抛瞬时 BufferFull，而是挂起等待常驻提交
  /// 协程刷盘腾窗后重试，杜绝「主存已写而 AOF 漏记」的静默主从发散。
  /// 仅记录过大（[`Error::RecordTooLarge`]）等确定性错误一次性上抛，磁盘致命故障
  /// 经 [`Error::FlushFailed`] 显式上抛（详见 [`Self::enqueue_with_backpressure`]）
  pub fn enqueue(&self, payload: &[u8]) -> waof::Result<i64> {
    self.enqueue_with_backpressure(|| self.wal.enqueue(payload).map(|addr| addr as i64))
  }

  /// 分部件直通零整包拼接（WalLog::enqueue_parts 产出页与 enqueue 逐字节一致），
  /// 背压语义同 [`Self::enqueue`]
  pub fn enqueue_parts(&self, parts: &[&[u8]]) -> waof::Result<i64> {
    self.enqueue_with_backpressure(|| self.wal.enqueue_parts(parts).map(|addr| addr as i64))
  }

  /// 多帧原子落盘直通（WalLog::enqueue_frames 单次预留连续地址，帧间不插花），
  /// 背压语义同 [`Self::enqueue`]（整批帧一次重试，不丢不重）
  pub fn enqueue_frames(&self, frames: &[&[&[u8]]]) -> waof::Result<i64> {
    self.enqueue_with_backpressure(|| self.wal.enqueue_frames(frames).map(|addr| addr as i64))
  }

  /// 入队唯一背压内核：包裹一次物理日志预占写入，环形缓冲瞬时满即请求刷盘腾窗、
  /// 阶梯退避挂起等待刷盘推进事件、循环重试，直至写入成功或抵达终态。
  ///
  /// 快路径零事件面开销：首轮裸尝试预占、成功即返，不触碰任何事件对象（对标 C#
  /// `AllocateBlock` 的 `TryAllocateRetryNow` 单次 CAS 预占即返，事件等待严格
  /// 限于缓冲写满后的慢重试路径），兑现数据面热路径零额外堆分配承诺。
  ///
  /// 单点覆盖 enqueue / enqueue_parts / enqueue_frames 三入口（全链路仅此一套
  /// 背压机制，无并行旁路）。对标 libs/storage/Tsavorite/cs/src/core/TsavoriteLog/
  /// TsavoriteLog.cs:AllocateBlock：
  /// - `TryAllocateRetryNow` 失败（rust `Error::BufferFull`）→ C# 清在途槽位
  ///   （rust `reserve_address` 已在失败路径复位槽位，故挂起期不占槽、不阻塞
  ///   刷盘协程计算 SafeTail）→ `flushEvent.Wait()` → 重试；
  /// - `cannedException` 非空 → 抛出终止（rust 以 `flush_failures` 非零绝对
  ///   判定识别刷盘致命故障，返回 [`Error::FlushFailed`]，杜绝坏盘无限挂起；
  ///   对标 C# cannedException 一次性置位后永久重抛的绝对位语义——曾有
  ///   「进入时基线后增量」的收敛近似，但提交驱动死亡先于入队时（如 runtime
  ///   创建失败臂）任何等待者基线都已含旧失败，增量判定永不可见，故改绝对
  ///   判定：宁可显式错误，绝不静默挂起）；
  /// - 记录过大 C# 由前置 `ValidateAllocatedLength` 在进入 AllocateBlock 前抛出，
  ///   rust 对应 `check_record_len` 的 [`Error::RecordTooLarge`]，此处直接透传不重试。
  ///
  /// 挂起等待的监听器注册收敛在 Sleep 慢路径（register-recheck：先注册再复查
  /// 预占，notify 必命中在册监听器，无丢唤醒），与 `GarnetLog::wait_for_commit`、
  /// `AofBackpressure::wait_slow` 同一 `wbase::backoff` 阶梯：Spin/Yield 短自旋
  /// 消化瞬时毛刺，Sleep 无锁深挂起令位交还常驻提交协程。
  fn enqueue_with_backpressure<R>(
    &self,
    mut op: impl FnMut() -> waof::Result<R>,
  ) -> waof::Result<R> {
    let mut backoff = Backoff::new();
    loop {
      match op() {
        // 环形缓冲瞬时满：非终态，腾窗后循环重试（绝不在此上抛瞬时 BufferFull）
        Err(Error::BufferFull { .. }) => {}
        // 成功，或 RecordTooLarge/设备/校验等任何非背压可解错误：一次性透传
        // ——快路径首轮即达此支：全程零监听器注册、零堆分配、零事件链表原子对
        other => return other,
      }
      // 致命 I/O 防御：常驻提交驱动已发生过刷盘失败（含提交线程 runtime 创建
      // 失败 / committer panic 退出臂）→ 磁盘故障，跳出重试上抛，绝不陷入坏盘
      // 无限挂起（对标 C# `if (cannedException != null) throw`）
      if self.flush_failures.load(Ordering::Relaxed) > 0 {
        return Err(Error::FlushFailed);
      }
      // 阶梯退避：Spin/Yield 自旋/让核消化瞬时毛刺；Sleep 才武装事件挂起
      match backoff.stage() {
        BackoffStage::Spin | BackoffStage::Yield => {
          // 请求刷盘腾窗：折叠唤醒常驻提交协程把环形缓冲已提交数据落设备并推进
          // flushed_until 水位（对标 C# 依赖后台 CommitTask 推进 flushEvent）
          self.request_flush();
          backoff.snooze();
        }
        BackoffStage::Sleep => {
          // 先注册刷盘推进事件监听，再请求刷盘与复查预占：本轮及后续提交协程的
          // notify 必命中在册 listener，杜绝「注册前完成刷盘」丢唤醒后永久沉睡
          let listener = self.flush_event.listen();
          self.request_flush();
          match op() {
            // 并发刷盘已腾窗：复查直接命中，注册即弃，不挂起
            Err(Error::BufferFull { .. }) => {}
            other => return other,
          }
          // 注册后复查致命失败：注册前完成的刷盘故障，其失败累计此刻已可见，
          // 立即上抛而非带着已知故障沉睡
          if self.flush_failures.load(Ordering::Relaxed) > 0 {
            return Err(Error::FlushFailed);
          }
          // 无锁深挂起至 flush_event 唤醒（成败均 notify），醒来回顶部重试预占
          listener.wait();
        }
      }
    }
  }

  /// 请求一次刷盘腾窗：以当前安全尾为折叠目标唤醒常驻提交协程，不改写任何提交
  /// 边界元数据（cookie/committed_begin/pending_cookie 保持不动）。与 [`Self::commit`]
  /// 共用同一常驻提交协程与折叠信号通道（单机制），仅省去入队侧不该触碰的 cookie
  /// 记账——供 [`Self::enqueue_with_backpressure`] 在缓冲满载时主动加速刷盘腾窗
  fn request_flush(&self) {
    self.kick_committer(self.wal.safe_tail_address());
  }

  /// 提交并触发刷盘：cookie/帧元数据同步落账后以容量 1 折叠信号唤醒
  /// 常驻提交协程（杜绝每次提交 spawn 刷盘任务的任务创建开销；并发提交
  /// 由折叠信号 + commit_to 内 GroupCommitPipeline 双层合并）
  pub fn commit(&self, until_address: i64, cookie: i64) {
    // 副本角色闸：无本地提交边界，cookie/pending 帧槽簿记一律不落，
    // 仅请求刷盘腾位（对标 C# CommitTaskAsync 副本分支跳过提交记录写出）
    if self.is_replica() {
      self.request_flush();
      return;
    }
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
    self.kick_committer(target);
  }

  /// 折叠提交请求并唤醒常驻提交协程（commit / request_flush 共尾单点）：
  /// fetch_max 合并并发目标，已装配直发折叠信号，首调装配后补发（热路径零
  /// channel 构造）
  fn kick_committer(&self, target: u64) {
    self.commit_request.fetch_max(target, Ordering::AcqRel);
    if let Some(tx) = self.commit_wake.get() {
      let _ = tx.try_send(());
    } else {
      self.ensure_committer();
      if let Some(tx) = self.commit_wake.get() {
        let _ = tx.try_send(());
      }
    }
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
    let end = if end_address < 0 {
      self.wal.tail_address()
    } else {
      end_address as u64
    };
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
    let end = if end_address < 0 {
      self.wal.tail_address()
    } else {
      end_address as u64
    };
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
  ///
  /// 副本角色闸：改道 [`Self::commit_flush_only_async`] 纯刷盘——本入口的
  /// 帧写出经 commit_to 组提交步，副本严禁触达（闸点唯一，见
  /// [`drive_commit`] 机制约束；C# 对偶：副本 Dispose/Commit 链无 CommitAsync）
  pub async fn commit_flush_async(&self, cookie: i64) {
    if self.is_replica() {
      self.commit_flush_only_async().await;
      return;
    }
    self.cookie.store(cookie, Ordering::Release);
    // commit 记录写出 begin 快照（C# TsavoriteLog.WriteCommitMetadata：
    // info.BeginAddress = BeginAddress，TsavoriteLog.cs:2696）
    self
      .committed_begin
      .store(self.wal.begin_address() as i64, Ordering::Release);
    self.wal.set_pending_cookie(cookie);
    let res = self.wal.commit().await;
    let _ = self.settle_flush(res, "物理刷盘");
  }

  /// 副本角色落盘：已入队记录刷设备并推进提交水位，不写本地 commit 元数据
  /// 帧（副本 AOF 为主端流严格镜像的约束见 [`WalLog::commit_flush_only`]）。
  /// cookie/committed_begin 不动——副本无本地提交边界，恢复面随主端转发帧
  /// 收敛（[`Self::recover_async`]）
  pub async fn commit_flush_only_async(&self) {
    let res = self.wal.commit_flush_only().await;
    let _ = self.settle_flush(res, "副本落盘");
  }

  /// 副本回放提交元数据对齐：物理 WAL 提交边界对齐主端帧值后，同步本地
  /// cookie 与 committed_begin 标量并放行提交等待者。对标 C#
  /// TsavoriteLog.cs 的 UnsafeCommitMetadataOnly 经 ReplicaReplayDriver.cs:
  /// ConsumeDirect 的 payloadLength < 0 分支调用；本地标量回填对标
  /// UpdateCommittedState（CommittedBeginAddress 直赋 +
  /// persistedCommitNum 单调——[`NO_COOKIE`] = i64::MIN 经 fetch_max 恒
  /// 不回退真实序列号）。
  ///
  /// 副本角色闸的反向补全：副本一切本地提交面（commit / commit_flush_async /
  /// 周期提交任务）均不落提交边界，本口是副本提交边界唯一推进源——与
  /// [`drive_commit`] 的「帧写入唯一在主端」闸合璧成完整复制提交协议
  pub async fn unsafe_commit_metadata_only(
    &self,
    meta: CommitMeta,
    until_address: i64,
  ) -> waof::Result<()> {
    let res = self
      .wal
      .unsafe_commit_metadata_only(meta, until_address.max(0) as u64)
      .await;
    self.cookie.fetch_max(meta.cookie, Ordering::AcqRel);
    // 提交记录写出的 begin 快照（C# UpdateCommittedState：CommittedBeginAddress
    // = recoveryInfo.BeginAddress，TsavoriteLog.cs:2737）
    self
      .committed_begin
      .store(meta.begin as i64, Ordering::Release);
    self.settle_flush(res, "副本提交对齐")
  }

  /// 提交入口共尾：失败累计计数 + 告警，随即无条件唤醒等刷盘挂起者（失败
  /// 亦 notify，防等待者沉睡至超时；全部提交入口样板单点化）。结果原样回传，
  /// 供需要上抛的调用方（副本提交元数据对齐）沿用同一失败记账
  fn settle_flush<T>(&self, result: waof::Result<T>, label: &str) -> waof::Result<T> {
    if let Err(err) = &result {
      let total = self.flush_failures.fetch_add(1, Ordering::Relaxed) + 1;
      log::error!("WaofSublog {label}失败(累计 {total}): {err:?}");
    }
    self.flush_event.notify(usize::MAX);
    result
  }

  /// 设备面恢复：扫描磁盘段定位尾位点并预载环形窗口（WalLog::recover），
  /// 并把 commit 元数据帧收敛出的 cookie/begin 快照回填进程内视图
  ///（C# TsavoriteLog.Initialize 自提交记录回填 CommittedBeginAddress 与
  /// RecoveredCookie 的对应面；C# TsavoriteLog.cs:623 RecoverAsync 沿
  /// ValueTask 透明上抛——致命 I/O 绝不吞错。C# 消费端续行门控
  /// FailOnRecoveryError 生效默认关（吞错续行）；rust 侧该旗标零代码消费，
  /// 本错沿 `?` 恒上抛拒启（刻意收紧，见 deviations.md §122）。
  pub async fn recover_async(&self) -> waof::Result<()> {
    self.wal.recover().await?;
    let cookie = self.wal.recovered_cookie();
    if cookie != NO_COOKIE {
      self.cookie.store(cookie, Ordering::Release);
    }
    self.committed_begin.store(
      self.wal.recovered_committed_begin() as i64,
      Ordering::Release,
    );
    Ok(())
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

  /// 异步等待提交落盘至 `until_address`（C# TsavoriteLog.cs:WaitForCommitAsync
  /// :1866-1879 的等待面：CommitTask 故障即沿 await 重抛 CommitFailureException
  /// ——TsavoriteLog.cs:2786-2788 cannedException + TrySetException。失败上浮，
  /// 不吞错：吞错即 wait 档客户端拿到 +OK 而写未落盘，已确认写丢失）
  ///
  /// 副本角色闸：等待者自身就是刷盘驱动（与主端 [`WalLog::wait_for_commit`]
  /// 经 commit_to 驱动同构），但驱动只准走 flush-only 内核——commit_to 的
  /// 组提交步会写本地 commit 帧，副本触达即破主端流镜像
  pub async fn wait_for_commit_async(&self, until_address: i64) -> waof::Result<()> {
    let target = until_address.max(0) as u64;
    if self.is_replica() {
      loop {
        // 先注册推进事件再驱动/复查，无丢唤醒；失败沿 await 上浮（主端同口径）
        let listener = self.flush_event.listen();
        if self.wal.commit_flush_only().await? >= target {
          return Ok(());
        }
        listener.await;
      }
    }
    self.wal.wait_for_commit(target).await.map(|_| ())
  }
}

#[cfg(test)]
mod tests {
  use std::{
    sync::{
      Arc,
      atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
  };

  use crate::aof::test_support::test_sublog;

  /// 快路径零事件面锚定：首轮裸尝试预占时刷盘推进事件上不得有任何在册监听器
  /// （旧实现循环首行无条件 `listen()`，预占时刻 `total_listeners()` 恒可观测到 1；
  /// 重排后为 0——监听器注册与对应的堆分配、事件链表原子对被完全收敛进 Sleep 慢路径）
  #[test]
  fn fast_path_enqueue_registers_no_flush_listener() {
    let (_dir, sublog) = test_sublog("waof_sublog_fastpath_listener");
    let listeners_at_attempt = Arc::new(AtomicUsize::new(usize::MAX));
    let probe = Arc::clone(&listeners_at_attempt);
    // 单测线程独占事件源：预占执行瞬间的在册监听器数即该次尝试的武装状态，无并发噪声
    let addr = sublog
      .enqueue_with_backpressure(|| {
        probe.store(sublog.flush_event().total_listeners(), Ordering::Relaxed);
        sublog.enqueue(b"fastpath_record")
      })
      .expect("空载快路径入队不应失败");
    assert_eq!(
      listeners_at_attempt.load(Ordering::Relaxed),
      0,
      "快路径预占执行时刷盘事件出现在册监听器（注册先于尝试回潮）"
    );
    // 记录真实写入：尾位点随预占推进
    assert!(
      sublog.tail_address() > addr,
      "快路径入队未推进尾位点: {addr}"
    );
    // 内核返回后监听器无残留
    assert_eq!(sublog.flush_event().total_listeners(), 0);
  }

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
