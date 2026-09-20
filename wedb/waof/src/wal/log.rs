use std::{
  future::ready,
  ops::Deref,
  sync::{
    Arc, OnceLock,
    atomic::{AtomicI64, AtomicU64, Ordering, fence},
  },
};

use async_lock::Mutex as AsyncLockMutex;
use crossfire::{MAsyncTx, mpsc::Array};
use wbase::group_commit::GroupCommitPipeline;
use wdev::Device;

use super::{
  commit,
  config::WalConfig,
  header::RECORD_HEADER_LEN,
  iterator::WalScanIterator,
  record::WalRecord,
  ring_buffer::{MemFrame, RingBuffer},
};
use crate::error::Result;

/// 复制流推流唤醒信号发送端类型（容量 1 有界通道的发送端；
/// 满即折叠去重——多帧写入只留一个未处理信号）
pub type ReplicationWakeTx = MAsyncTx<Array<()>>;

/// 恢复/扫描共用的滑动窗口分块大小
pub(crate) const RECOVER_CHUNK_SIZE: usize = 64 * 1024;

/// 日志起始位点派生单点：段编号域 × 段大小
///
/// 非分段设备（wdev SegmentedDevice::single_file，segment_size 为 None）的段编号域
/// 恒为 0，故派生结果为 0 * 0 = 0，正是单文件日志以文件内偏移为逻辑地址的正确起始
/// 位点，不存在「塌到 0」的信息丢失；分段设备段大小必为 Some
///
/// 对标 C# TsavoriteLog.Reset 的口径：单点先取
/// `beginAddress = allocator.GetFirstValidLogicalAddressOnPage(0)`，起始地址唯一
/// 取自设备派生，构造/reset/恢复三路不得各算一遍段换算（Reset 锚点在其本体文档）
#[inline]
pub(crate) fn log_begin_address<D: Device>(device: &D) -> u64 {
  let start_seg = device.start_segment() as u64;
  let seg_size = device.segment_size().unwrap_or(0);
  start_seg * seg_size
}

/// WAL 引擎共享内部状态
pub struct WalLogInner<D: Device> {
  /// 起始有效逻辑地址（该地址之前的段已被物理截断）
  pub begin_address: AtomicU64,
  /// 当前末尾逻辑地址（下一个待分配写入的地址）
  pub tail_address: AtomicU64,
  /// 已刷入底层设备的逻辑地址
  pub flushed_until_address: AtomicU64,
  /// 已成功提交并持久化的逻辑地址
  pub committed_until_address: AtomicU64,
  /// 内存环形写缓冲区
  pub ring_buffer: RingBuffer,
  /// 底层块存储设备句柄
  pub device: Arc<D>,
  /// WAL 配置参数
  pub config: WalConfig,
  /// 并发写入在途追踪槽位
  pub inflight_slots: Box<[AtomicU64]>,
  /// 提交刷盘互斥锁
  pub commit_lock: async_lock::Mutex<()>,
  /// 提交落盘流水线（支持并发合并 Group Commit）
  pub commit_pipeline: GroupCommitPipeline,
  /// 复制流推流唤醒信号发送端（宿主经 [`WalLog::set_replication_wake`] 注入；
  /// 帧数据不流经信号——推流端从环形缓冲按地址拉取，`safe_tail_address()`
  /// 即安全可读面）
  pub replication_wake: OnceLock<ReplicationWakeTx>,
  /// 待随批写出的提交 cookie（宿主经 [`WalLog::set_pending_cookie`] 更新，
  /// Leader 写 commit 元数据帧时采样；[`commit::NO_COOKIE`] = 无序列号）
  pub pending_cookie: AtomicI64,
  /// 已写出的最后 commit 元数据帧尾地址（帧游标：级联循环防重复写帧；
  /// truncate/reset 时同步收敛）
  pub last_commit_frame: AtomicU64,
  /// 恢复收敛出的最后一次提交 cookie（[`commit::NO_COOKIE`] = 无 commit 帧）
  pub recovered_cookie: AtomicI64,
  /// 恢复收敛出的最后 commit 帧的 begin 快照
  pub recovered_committed_begin: AtomicU64,
  /// 恢复保守截尾的损坏帧地址（0 = 本次恢复无截断，观测面见
  /// [`WalLog::recover_truncation`]）
  pub recover_truncated_at: AtomicU64,
  /// 恢复保守截尾丢弃的字节数（损坏点所在段残余 + 其后各段整段）
  pub recover_dropped_bytes: AtomicU64,
}

impl<D: Device> WalLogInner<D> {
  /// 计算当前安全可读/可刷盘的尾部逻辑地址（所有小于该地址的并发写入均已落盘到环形内存）
  #[inline]
  pub fn safe_tail_address(&self) -> u64 {
    let tail = self.tail_address.load(Ordering::Acquire);
    fence(Ordering::SeqCst);
    self
      .inflight_slots
      .iter()
      .fold(tail, |min, slot| min.min(slot.load(Ordering::Acquire)))
  }
}

/// WAL 引擎（对标 C# TsavoriteLog）
///
/// 持久性边界：commit 时随批尾写 commit 元数据帧（begin + cookie，对标
/// TsavoriteLog.cs:TryEnqueueCommitRecord，见 [`super::commit`]），`recover`
/// 扫至最后 commit 帧收敛提交上界，帧后的未提交记录不被动转正。设备上无任何
/// commit 帧（旧形态日志或截断后）时回退保守兼容语义：以最后一条完整记录为
/// 已提交。要求更宽提交边界的调用方（如一致性协议日志）仍可以扫描终点自行
/// 界定
pub struct WalLog<D: Device> {
  pub(crate) inner: Arc<WalLogInner<D>>,
}

impl<D: Device> Clone for WalLog<D> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      inner: Arc::clone(&self.inner),
    }
  }
}

impl<D: Device> Deref for WalLog<D> {
  type Target = WalLogInner<D>;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.inner
  }
}

impl<D: Device> WalLog<D> {
  /// 创建新的 WAL 日志实例
  pub fn new(device: Arc<D>, config: WalConfig) -> Result<Self> {
    // 环形缓冲区对齐口径取设备扇区大小（单一真源，见 WalConfig 文档）
    let sector_size = device.sector_size();
    let ring_buffer = RingBuffer::new(config.buffer_size, sector_size)?;
    let slot_count = config.inflight_slots.max(1);
    let slots = (0..slot_count)
      .map(|_| AtomicU64::new(u64::MAX))
      .collect::<Box<[_]>>();

    // 日志首地址取段边界（派生单点见 [`log_begin_address`]）。刻意差异：C# Garnet
    // AOF 的 kFirstValidAofAddress = 64 源自 TsavoriteLog 设备头区（commit 元数据
    // 写设备头，复制恢复以 64 作 "副本非空"哨兵）；本实现 commit 元数据为随批尾帧
    // （见模块文档），无头区，空日志判据统一为 begin == tail，磁盘格式差异见 header 模块
    let begin_addr = log_begin_address(&*device);

    Ok(Self {
      inner: Arc::new(WalLogInner {
        begin_address: AtomicU64::new(begin_addr),
        tail_address: AtomicU64::new(begin_addr),
        flushed_until_address: AtomicU64::new(begin_addr),
        committed_until_address: AtomicU64::new(begin_addr),
        ring_buffer,
        device,
        config,
        inflight_slots: slots,
        commit_lock: AsyncLockMutex::new(()),
        commit_pipeline: GroupCommitPipeline::new(),
        replication_wake: OnceLock::new(),
        pending_cookie: AtomicI64::new(commit::NO_COOKIE),
        last_commit_frame: AtomicU64::new(begin_addr),
        recovered_cookie: AtomicI64::new(commit::NO_COOKIE),
        recovered_committed_begin: AtomicU64::new(begin_addr),
        recover_truncated_at: AtomicU64::new(0),
        recover_dropped_bytes: AtomicU64::new(0),
      }),
    })
  }

  /// 注册复制流推流唤醒信号发送端（须在产生任何写入之前调用；一次注入，
  /// 重复注入返回 false）
  ///
  /// 唤醒语义：每条记录入队完成后 `try_send(())` 一次（容量 1，满即折叠），
  /// 推流端被唤醒后从 [`Self::safe_tail_address`] 按地址序拉取新帧——推流序
  /// 与 AOF 地址序原子一致（同一环形缓冲的线性化序），并发写入下从侧应用序
  /// 与主侧重启重放序永不发散
  pub fn set_replication_wake(&self, tx: ReplicationWakeTx) -> bool {
    self.replication_wake.set(tx).is_ok()
  }

  /// 打开或恢复已有 WAL 日志实例，自动扫描磁盘段文件恢复有效位点
  pub async fn open(device: Arc<D>, config: WalConfig) -> Result<Self> {
    let log = Self::new(device, config)?;
    log.recover().await?;
    Ok(log)
  }

  /// 在途槽位填充单点：位点重置族（safe_initialize / reset / recover）收尾一律
  /// 把在途槽位填回 u64::MAX（safe_tail_address 下界折叠的中性元，语义即「无在途
  /// 写入」）；构造期由字段初始化以同一哨兵落位，不重复本填充
  #[inline]
  pub(crate) fn reset_inflight_slots(&self) {
    for slot in self.inflight_slots.iter() {
      slot.store(u64::MAX, Ordering::Release);
    }
  }

  /// 安全初始化或重设 WAL 日志有效地址范围（对标 C# TsavoriteLog.SafeInitialize / Initialize）
  ///
  /// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:SafeInitialize
  pub fn safe_initialize(&self, begin_address: u64, committed_until_address: u64) {
    let end = committed_until_address.max(begin_address);
    self.begin_address.store(begin_address, Ordering::Release);
    self.tail_address.store(end, Ordering::Release);
    self.flushed_until_address.store(end, Ordering::Release);
    self.committed_until_address.store(end, Ordering::Release);
    self.reset_inflight_slots();
  }

  /// 推进起始有效地址，并调用底层设备物理截断清理旧段文件
  ///
  /// 持有提交锁与 commit 互斥：防止物理删段与在途刷盘并发，段文件被删除后又被幽灵重建
  pub async fn truncate(&self, until_address: u64) -> Result<()> {
    let guard = self.commit_lock.lock().await;
    let committed = self.committed_until_address.load(Ordering::Acquire);
    // 安全上界钳制（只截已获持久承诺的字节）是 waof 侧语义参数，先于内核完成
    let safe_until = until_address.min(committed);
    // 截断编排内核单源下沉（Device::truncate_begin_until，对标 AllocatorBase.cs:
    // ShiftBeginAddress 的「先推进 begin → 物理截断」次序）：waof 无纪元排空
    // 屏障，注入 ready 空屏障；本调用与全部刷盘在 commit_lock 内串行，无可观测
    // 交错差异。帧游标钳至截断点（被物理删除段内的 commit 帧失效，后续 commit
    // 重写新帧）在设备删段返回后完成
    self
      .device
      .truncate_begin_until(&self.begin_address, safe_until, ready(()))
      .await?;
    self
      .last_commit_frame
      .fetch_min(safe_until, Ordering::AcqRel);
    drop(guard);
    Ok(())
  }

  /// 创建指定范围的 WAL 记录扫描迭代器
  pub fn scan(&self, from: u64, to: u64) -> WalScanIterator<D> {
    let begin = self.begin_address.load(Ordering::Acquire);
    let start_addr = from.max(begin);
    WalScanIterator::new(Arc::clone(&self.inner), start_addr, to)
  }

  /// 内存窗口同步记录扫描（直构 WalRecord，零二次堆分配）
  ///
  /// 单帧解码（读头 → 守卫 → 推进 → 校验）单源化于
  /// [`RingBuffer::decode_frame`]（对标 C# TsavoriteLog 的 Scan 单趟解码），
  /// 本入口只保留回调 break 终止语义：伪头/越上界/CRC 失败均视为记录链终止
  pub fn scan_memory_records(
    &self,
    from: u64,
    to: u64,
    mut f: impl FnMut(&WalRecord) -> bool,
  ) -> bool {
    let cap = self.ring_buffer.capacity() as u64;
    let mem_base = self.tail_address();
    let start = from.max(self.begin_address());
    if start < mem_base.saturating_sub(cap) {
      return false;
    }
    let scan_end = to.min(self.safe_tail_address());
    let mut cur = start;
    while cur + (RECORD_HEADER_LEN as u64) <= scan_end {
      match self.ring_buffer.decode_frame(cur, scan_end) {
        MemFrame::Valid {
          header,
          payload,
          next_addr,
        } => {
          let rec = WalRecord {
            address: cur,
            next_address: next_addr,
            header,
            payload,
          };
          if !f(&rec) {
            break;
          }
          cur = next_addr;
        }
        // 链终止（本入口的 break 终止语义）
        MemFrame::CrcMismatch(_) | MemFrame::Invalid => break,
      }
    }
    true
  }

  /// 获取日志当前有效数据总大小（tail_address - begin_address）
  #[inline]
  pub fn total_size(&self) -> u64 {
    self.tail_address().saturating_sub(self.begin_address())
  }

  /// 重置 WAL 日志至初始空状态
  ///
  /// # 危险
  ///
  /// reset 后磁盘历史数据仍在：reset 仅回退内存位点，不物理清零磁盘。若 reset 后
  /// 未将新日志刷满 [begin, align_up(tail)) 前缀即发生崩溃，恢复扫描可能越过新尾部
  /// 复活旧记录（与 C# TsavoriteLog.Reset 后未打检查点即崩溃的恢复语义一致）。
  /// 调用方要么 reset 后立即 truncate 物理清理，要么接受该复活窗口。
  ///
  /// WARNING: 须在日志静默（无并发读写）后调用，对标 libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:Reset。
  pub async fn reset(&self) -> Result<()> {
    log::warn!(
      "WAL reset：磁盘历史数据未清零，崩溃后恢复可能复活旧记录（begin={begin:#x}）",
      begin = self.begin_address.load(Ordering::Acquire),
    );
    let guard = self.commit_lock.lock().await;
    // 起始位点与构造/恢复同取派生单点（见 [`log_begin_address`]）
    let begin_addr = log_begin_address(&*self.device);

    // 本路只重置「当前会话位点集」：recovered_cookie / recovered_committed_begin
    // 刻意保留（那是「本进程曾恢复过」的观测事实，reset 不得伪造历史），
    // 与恢复路径的写序差异登记见 recover.rs 的提交上界收敛注释
    self.begin_address.store(begin_addr, Ordering::Release);
    self.tail_address.store(begin_addr, Ordering::Release);
    self
      .flushed_until_address
      .store(begin_addr, Ordering::Release);
    self
      .committed_until_address
      .store(begin_addr, Ordering::Release);
    self.last_commit_frame.store(begin_addr, Ordering::Release);
    self
      .pending_cookie
      .store(commit::NO_COOKIE, Ordering::Release);

    self.reset_inflight_slots();
    // 与 commit 一致采用 fdatasync 快速刷盘
    self.device.sync_data().await?;
    drop(guard);
    Ok(())
  }

  /// 扫描当前所有已提交的记录
  #[inline]
  pub fn scan_committed(&self) -> WalScanIterator<D> {
    self.scan(
      self.begin_address.load(Ordering::Acquire),
      self.committed_until_address.load(Ordering::Acquire),
    )
  }

  /// 获取起始有效地址
  #[inline]
  pub fn begin_address(&self) -> u64 {
    self.begin_address.load(Ordering::Acquire)
  }

  /// 获取当前尾部逻辑地址
  #[inline]
  pub fn tail_address(&self) -> u64 {
    self.tail_address.load(Ordering::Acquire)
  }

  /// 获取已刷盘的逻辑地址
  #[inline]
  pub fn flushed_until_address(&self) -> u64 {
    self.flushed_until_address.load(Ordering::Acquire)
  }

  /// 获取已提交的逻辑地址
  #[inline]
  pub fn committed_until_address(&self) -> u64 {
    self.committed_until_address.load(Ordering::Acquire)
  }

  /// 获取底层设备引用
  #[inline]
  pub fn device(&self) -> &Arc<D> {
    &self.device
  }

  /// 更新待随批写出的提交 cookie（宿主提交入口调用；[`commit::NO_COOKIE`] = 无序列号）
  #[inline]
  pub fn set_pending_cookie(&self, cookie: i64) {
    self.pending_cookie.store(cookie, Ordering::Release);
  }

  /// 恢复收敛出的最后一次提交 cookie（[`commit::NO_COOKIE`] = 无 commit 帧）
  #[inline]
  pub fn recovered_cookie(&self) -> i64 {
    self.recovered_cookie.load(Ordering::Acquire)
  }

  /// 恢复收敛出的最后 commit 帧的 begin 快照
  #[inline]
  pub fn recovered_committed_begin(&self) -> u64 {
    self.recovered_committed_begin.load(Ordering::Acquire)
  }

  /// 恢复保守截尾观测（Some((损坏帧地址, 丢弃字节数)) = 本次恢复发生截尾；
  /// 供上层按配置选择拒绝恢复，观测面见 [`Self::note_recover_truncation`]）
  #[inline]
  pub fn recover_truncation(&self) -> Option<(u64, u64)> {
    let at = self.recover_truncated_at.load(Ordering::Acquire);
    (at != 0).then(|| (at, self.recover_dropped_bytes.load(Ordering::Acquire)))
  }

  /// 获取配置引用
  #[inline]
  pub fn config(&self) -> &WalConfig {
    &self.config
  }
}
