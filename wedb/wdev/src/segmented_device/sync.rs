//! 全局刷盘同步与脏段登记（sync 持久化契约的唯一落点）
//!
//! 对位 C# LocalStorageDevice.cs 的句柄表刷新语义（句柄进程级共享、任意线程可
//! sync）。Rust 侧句柄永不过线程（`compio::fs::File` 实测 `!Send`，链路 `File →
//! AsyncFd → Attacher → SharedFd → Rc<Inner<File>>`），无法像 C# 那样遍历共享表，
//! 故契约改由**设备级脏段集**承接：写入前登记段号，sync 只刷登记在册的段，并在
//! 调用线程按需补开句柄后提交 fsync/fdatasync。
//!
//! 脏段集是生产机制而非调试守护：debug 与 release 同一形态，杜绝"两套世界"下
//! 测试验证过的覆盖口径在生产里失效；同时消掉旧实现"按 start/end 水位全区间补开
//! 再全量 fsync"的随数据集放大的刷盘成本（每笔提交多刷 N−1 个无脏页段）。

use std::{io, rc::Rc};

use compio::fs::File;
use futures_util::future::join_all;
use parking_lot::Mutex;
use wbase::map::HashMap;

use super::SegmentedDevice;
use crate::error::{Error, Result};

/// 脏段登记项
///
/// 项在册即表示"该段存在已被登记、尚未被成功 sync 背书的写入"。
pub(super) struct DirtyEntry {
  /// 写前登记的累计笔数（含在途）。sync 快照记录其值，刷盘成功后仅当 `issued`
  /// 未推进——即快照后无新登记——才出集，避免把快照之后才发生的写入误判为
  /// "已被本次 fsync 覆盖"而永久漏刷
  issued: u64,
  /// 在途写入笔数：登记时加一、写入返回时由 [`WriteGuard`] 的 `Drop` 减一。
  /// 非零期间项不得出集（免责删除与刷盘确认都跳过它），故 `Drop` 必然命中本项，
  /// 计数不会漂移到他处的同段新项
  pending: u32,
}

/// 设备级脏段集（跨线程共享：任一线程的登记、任一线程的 sync 均见全集）
pub(super) type DirtySet = Mutex<HashMap<u32, DirtyEntry>>;

/// 在途写入登记守卫：`Drop` 时销记对应段的在途计数
///
/// 由 [`SegmentedDevice::mark_write`] 产生，调用方持有至写入返回；提前返回
/// （校验失败、打开失败、短写报错）同样销记，登记项则留在册——失败写入可能已
/// 把部分字节送入页缓存，仍须 fsync 背书
pub(super) struct WriteGuard<'a> {
  device: &'a SegmentedDevice,
  segment_id: u32,
}

impl Drop for WriteGuard<'_> {
  #[inline]
  fn drop(&mut self) {
    self.device.finish_write(self.segment_id);
  }
}

/// 按 sync 强度口径对单个段文件提交刷盘（`sync` 与 `sync_data` 的唯一系统调用点）
///
/// 执行位置取**调用线程同步直调**而非 compio 异步原语：compio 在 macOS（无
/// io_uring）走 poll driver，文件级 `Sync` op 判为 `Decision::Blocking` 派发到
/// AsyncifyPool 工作线程，完成后经 channel + waker 两次跨线程唤醒收割——探针
/// 实测该环回税约 5μs/次（无脏页 no-op sync 6.3μs 对比本体），逐笔提交的
/// individual_writes 段放大为可感知的吞吐损失。fsync(2)/fdatasync(2) 按 inode
/// 全量生效、无线程亲和（本模块 [`SegmentedDevice::sync`] 文档已论证），调用
/// 线程直调同一 fd 与池线程执行等价；worker 阻塞等待的正是物理介质本身，与
/// 原形态 worker 在 poll 中等待池线程完成同构，不引入新的阻塞语义。
///
/// 签名保留 async 以便多段臂经 `join_all` 统一下发；同线程形态下多段为串行
/// 落盘，持久语义（全部成功才返回）不变。
#[inline]
async fn sync_one(file: Rc<File>, datasync: bool) -> io::Result<()> {
  // unix（含 macOS）：调用线程同步直调 fsync(2)/fdatasync(2)——compio poll
  // driver 把文件级 Sync 判 Blocking 派发池线程的环回税已实证（~5μs/次），
  // fsync 按 inode 全量生效无线程亲和，直调与池线程执行等价。系统调用映射
  // 逐分支对齐 compio-driver build.rs 的 datasync cfg（datasync: all(unix,
  // not(apple))）：apple 平台 sync_data 与 sync 同走 fsync(2)。
  // windows：compio 走 IOCP 完成端口（非 poll driver 的池环回形态），保留
  // compio 异步原语原形态（sync_all/sync_data），平台原生强度语义不变。
  #[cfg(unix)]
  {
    use std::os::fd::AsRawFd;
    let fd = file.as_raw_fd();
    #[cfg(target_os = "macos")]
    let r = unsafe { libc::fsync(fd) };
    #[cfg(not(target_os = "macos"))]
    let r = if datasync {
      unsafe { libc::fdatasync(fd) }
    } else {
      unsafe { libc::fsync(fd) }
    };
    let _ = datasync;
    if r == 0 {
      Ok(())
    } else {
      Err(io::Error::last_os_error())
    }
  }
  #[cfg(windows)]
  {
    let _ = datasync;
    file.sync_all().await
  }
}

impl SegmentedDevice {
  /// 写入前把目标段登记为脏段，返回在途守卫
  ///
  /// 登记必须先于 `write_at`：若改为写入成功后登记，则"字节已落页缓存、脏段集
  /// 尚无记录"的窗口里发生的 sync 快照看不到本次写入，返回 Ok 即成假持久化。
  /// 写前登记的代价仅是一次 uncontended 加锁，相对 fsync 可忽略
  #[inline]
  pub(super) fn mark_write(&self, segment_id: u32) -> WriteGuard<'_> {
    let mut set = self.dirty.lock();
    let entry = set.entry(segment_id).or_insert(DirtyEntry {
      issued: 0,
      pending: 0,
    });
    entry.issued += 1;
    entry.pending += 1;
    WriteGuard {
      device: self,
      segment_id,
    }
  }

  /// 销记在途写入（仅由 [`WriteGuard::drop`] 调用）
  ///
  /// 本项在守卫存活期间不可能出集（出集两处均要求 `pending == 0`），故正常路径
  /// 必命中；`if let` 而非 `unwrap`：出集口径若被他处改动，最坏是计数停在途、
  /// 该段多刷一次，绝不 panic 于写入收尾
  #[inline]
  fn finish_write(&self, segment_id: u32) {
    let mut set = self.dirty.lock();
    if let Some(entry) = set.get_mut(&segment_id) {
      entry.pending -= 1;
    }
  }

  /// 免责出集：段文件被显式删除或截断回收后，其数据不再需要 fsync 背书
  ///
  /// 在途写入（`pending > 0`）的项保留在册：此刻出集会令守卫的销记落到后续重建
  /// 的同段新项上（计数漂移），留着则由下一次 sync 以 `SegmentNotFound` 自然出集
  pub(super) fn discard_dirty(&self, mut expired: impl FnMut(&u32) -> bool) {
    self.dirty.lock().retain(|sid, entry| {
      if entry.pending != 0 {
        return true;
      }
      !expired(sid)
    });
  }

  /// 刷盘成功后按快照出集：仅当该项自快照后无新登记且无在途写入时移除
  fn confirm_synced(&self, segment_id: u32, issued: u64) {
    let mut set = self.dirty.lock();
    let quiet = set
      .get(&segment_id)
      .is_some_and(|entry| entry.pending == 0 && entry.issued == issued);
    if quiet {
      set.remove(&segment_id);
    }
  }

  /// 当前在册脏段编号（升序）
  ///
  /// `#[doc(hidden)]` 且仅 debug 构建：脏段集本体是生产机制，本 getter 无生产
  /// 调用方，唯一消费者是 `tests/device/sync_contract.rs` 与
  /// `tests/device/truncate.rs` 的契约测试——它们以"写入后、sync 前"的在册集合
  /// 等值断言锁定每条写路径都完成登记（漏登记的写路径会在此现红）。升序口径由
  /// 测试的 `vec![0, 1]` 形态断言锁定。
  #[cfg(debug_assertions)]
  #[doc(hidden)]
  pub fn debug_dirty_segments(&self) -> Vec<u32> {
    let mut segs: Vec<u32> = self.dirty.lock().keys().copied().collect();
    segs.sort_unstable();
    segs
  }

  /// 刷盘同步设备上全部在册脏段（全量落盘，包含数据与元数据 fsync）
  ///
  /// 语义对齐 libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs:LocalStorageDevice：句柄表进程级共享，任意线程的 sync 覆盖设备上
  /// 全部线程已完成的写入。rust 侧句柄不过线程，覆盖面改由脏段集定义：本方法刷
  /// **调用发起前已登记的全部段**，与"哪些段被写过"严格同集，既无遗漏也无过量。
  /// sync 返回即保证：调用发起前已在任意线程完成的全部写入持久化，release 构建
  /// 不存在"线程 A 写入、线程 B sync 漏刷他线程句柄"的静默丢失窗口。
  ///
  /// # 跨线程可行性（Thread-Per-Core 无句柄迁移）
  ///
  /// - compio `File` 未启用 `sync` feature 时内含 `Rc`（实测 `!Send`），句柄无法
  ///   跨线程共享；脏段集则是 `Mutex` 托管的设备级状态，任一线程的登记对全线程
  ///   可见，故"全局覆盖"不再依赖句柄表遍历；
  /// - 他线程写入过的段由调用线程就地补开（`create=false`，打开动作发生且仅发生
  ///   在调用线程自身的驱动上）；本线程已持有句柄时命中 TLS 表，零系统调用；
  /// - fd 属进程级 files_struct，fsync 按 inode 全量生效、无线程亲和——调用线程对
  ///   同一 inode 补开刷新，与持有原始写入句柄的他线程自行刷新落盘等价；
  ///   O_DIRECT fd 的 fsync/fdatasync 同样无线程亲和要求。
  ///
  /// # 排序契约（与 POSIX fsync 同口径）
  ///
  /// 快照发生在 sync 起点：快照前已**完成**的写入由本次 fsync 覆盖；快照时仍在途
  /// 的写入（`pending > 0`）其字节未必落入本次 fsync，故本段不出集，由下一次
  /// sync 背书；快照后的新写入同理。调用方（whlog flush → wkv flush_all /
  /// evict_pages_for）均为同线程"写后即 sync"，天然满足。
  ///
  /// # 持久化承诺（含目录项）
  ///
  /// 段文件由设备层创建时已同步 fsync 父目录，因此本方法返回后"新段写入 + sync
  /// 即持久"的承诺同时覆盖段文件数据与新建段的目录项，断电崩溃后新段保证可见；
  /// WAL/HLog 等调用方无需自行刷盘目录。
  pub async fn sync(&self) -> Result<()> {
    self.sync_internal(false).await
  }

  /// 异步刷盘仅同步文件数据（fdatasync），尽量避免同步 inode 元数据（时间戳等）
  ///
  /// 覆盖面与 [`SegmentedDevice::sync`] 完全相同（同为脏段集在册段），仅系统调用
  /// 降级为 fdatasync；新建段的目录项持久化口径亦同：段创建时设备层已一次性
  /// fsync 父目录，本方法的持久化承诺同样覆盖新段可见性。
  pub async fn sync_data(&self) -> Result<()> {
    self.sync_internal(true).await
  }

  /// 全局 sync 统一实现：快照在册脏段 → 补开句柄 → 并发 fsync → 条件出集
  ///
  /// 锁内只做快照与出集判定，全部 I/O 在锁外；补开或刷盘失败均记首个错误后
  /// 继续处理其余段（尽力刷完），失败段保留在册由下一次 sync 补刷，方法末尾
  /// 透明上抛——绝不"返回 Ok 却漏刷"。
  async fn sync_internal(&self, datasync: bool) -> Result<()> {
    // 1. 在册脏段快照（含 issued 计数，供刷盘后判定能否出集）
    let snapshot: Vec<(u32, u64)> = self
      .dirty
      .lock()
      .iter()
      .map(|(&sid, entry)| (sid, entry.issued))
      .collect();
    if snapshot.is_empty() {
      return Ok(());
    }

    // 2. 逐段取句柄：本线程 TLS 命中即零 syscall，他线程写入过的段就地补开。
    //    仅打开磁盘上已存在的段（create=false）：在册脏段的段文件必已随写入创建，
    //    取不到即已被并发删段/截断回收（数据随 inode 消失，免责出集）；从未写入的
    //    空洞段根本不在册，绝不会被本路径触碰，故 sync 无幽灵创建风险（对齐
    //    recover 的幽灵段防御）。真实 I/O 故障上抛而非静默跳过，杜绝假持久化
    let mut targets: Vec<(u32, Rc<File>, u64)> = Vec::with_capacity(snapshot.len());
    let mut first_err: Option<Error> = None;
    for (sid, issued) in snapshot {
      match self.get_or_open_file(sid, false).await {
        Ok(file) => targets.push((sid, file, issued)),
        Err(Error::SegmentNotFound(_)) => self.discard_dirty(|&dirty| dirty == sid),
        Err(e) => {
          first_err.get_or_insert(e);
        }
      }
    }

    // 3. 单段高频场景（WAL 顺序追加写尾段）：就地刷盘，免除 join 的堆分配与装箱
    if let [(sid, file, issued)] = targets.as_slice() {
      if let Err(e) = sync_one(Rc::clone(file), datasync).await {
        first_err.get_or_insert(Error::from(e));
      } else {
        self.confirm_synced(*sid, *issued);
      }
      return first_err.map_or(Ok(()), Err);
    }

    // 4. 多段场景：逐段同步落盘（fsync 按 inode 串行生效，跨线程并发派发对同一
    //    调用线程无收益）；join_all 不短路，保证每段都尽力完成，避免中途取消导致
    //    后续段脏页滞留
    let results = join_all(targets.iter().map(|(_, file, _)| {
      let file = Rc::clone(file);
      async move { sync_one(file, datasync).await }
    }))
    .await;

    for ((sid, _, issued), res) in targets.iter().zip(results) {
      if let Err(e) = res {
        first_err.get_or_insert(Error::from(e));
      } else {
        self.confirm_synced(*sid, *issued);
      }
    }

    first_err.map_or(Ok(()), Err)
  }
}
