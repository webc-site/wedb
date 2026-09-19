//! 全局刷盘同步与 sync 持久化契约守护
//!
//! 对位 C# LocalStorageDevice.cs 的句柄表全量刷新语义（句柄进程级共享、任意线程
//! 可 sync）：本域收集本线程 TLS 在表句柄并按需补齐在册脏段后逐个 fsync/fdatasync；
//! debug 构建另持前 128 段的"已写入待 sync"位图守护。

use std::{collections::hash_map::Entry, rc::Rc, sync::atomic::Ordering};

use compio::fs::File;
use futures_util::future::join_all;
use gxhash::HashMap;

use super::{SegmentedDevice, handle::LOCAL_FILES};
use crate::error::{Error, Result};

/// sync 持久化契约守护跟踪的段数上限（仅 debug 构建参与编译，release 零成本）
///
/// 守护目标为测试与 WAL 场景的契约回归，128 段（2 个 AtomicU64）已充分覆盖；
/// 超出窗口的写入段不跟踪、不校验
#[cfg(debug_assertions)]
pub(super) const SYNC_GUARD_SEGMENTS: usize = 128;

impl SegmentedDevice {
  /// 刷盘同步设备上全部在表段文件句柄（全量落盘，包含数据与元数据 fsync）
  ///
  /// 语义对齐 libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs:LocalStorageDevice：句柄表进程级共享，任意线程的 sync 覆盖设备上
  /// 全部线程已打开的句柄（遍历全集并按段号去重——fsync 按 inode 全量生效，同段多个
  /// fd 仅需一次 fsync，调用线程不再重复刷其他线程已刷过的同一 inode）。sync 返回
  /// 即保证：调用发起前已在任意线程完成的全部写入持久化，release 构建不存在
  /// "线程 A 写入、线程 B sync 漏刷他线程句柄"的静默丢失窗口。
  ///
  /// # 跨线程可行性（Thread-Per-Core 无句柄迁移）
  ///
  /// - compio `File` 未启用 `sync` feature 时内含 `Rc`（!Send），句柄无法跨线程共享；
  ///   本实现据此设计为"句柄永不过线程"：sync 只收集调用线程 TLS 内的本地句柄，
  ///   他线程写入过的段由调用线程按文件存在性就地补开（打开动作发生且仅发生在
  ///   调用线程自身的驱动上），随后对本线程句柄提交 fsync/fdatasync；
  /// - fd 属进程级 files_struct，fsync 按 inode 全量生效、无线程亲和——调用线程对
  ///   同一 inode 补开刷新，与持有原始写入句柄的他线程自行刷新落盘等价；
  ///   O_DIRECT fd 的 fsync/fdatasync 同样无线程亲和要求。
  ///
  /// # 排序契约（与 POSIX fsync 同口径）
  ///
  /// 仅覆盖"sync 开始收集句柄前已在任意线程完成"的写入；收集后完成的并发写入由
  /// 下一次 sync 背书。调用方（whlog flush → wkv flush_all / evict_pages_for）均为
  /// 同线程"写后即 sync"，天然满足。
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
  /// 语义与 [`SegmentedDevice::sync`] 相同（全局覆盖、按段去重），仅系统调用降级为
  /// fdatasync；新建段的目录项持久化口径亦同：段创建时设备层已一次性 fsync 父目录，
  /// 本方法的持久化承诺同样覆盖新段可见性。
  pub async fn sync_data(&self) -> Result<()> {
    self.sync_internal(true).await
  }

  /// 全局 sync 统一实现：收集当前 CPU 核心本地在表句柄并按需补齐在册脏段后逐个 fsync
  async fn sync_internal(&self, datasync: bool) -> Result<()> {
    let min_seg = self.start_segment.load(Ordering::Relaxed);
    let max_seg = self.end_segment.load(Ordering::Relaxed);
    // 守护基线：先取"发起前在册"的脏段快照（这些写入必须被本次 sync 覆盖），
    // 快照后并发完成的新写入由下一次 sync 背书（排序契约同 POSIX fsync）
    #[cfg(debug_assertions)]
    let pending = self.debug_dirty_segments();

    // 1. 获取当前 CPU 核心本地已打开的段句柄并主动释放已被截断的陈旧句柄
    let mut files: HashMap<u32, Rc<File>> = LOCAL_FILES.with(|map| {
      let mut map = map.borrow_mut();
      map.retain(|&(dev_id, sid), _| dev_id != self.device_id || sid >= min_seg);
      map
        .iter()
        .filter(|&(&(dev_id, sid), _)| dev_id == self.device_id && sid >= min_seg)
        .map(|(&(_, sid), f)| (sid, Rc::clone(f)))
        .collect()
    });

    // 2. 若存在本核心未打开但全局在册已写入的段（如他核心写入后跨线程 sync），按需补开。
    //    仅打开磁盘上已存在的段文件（create=false）：从未写入的空洞段无脏页无须刷盘，
    //    绝不因 sync 被幽灵创建（对齐 recover 的幽灵段防御）；在册写入段打开失败属
    //    真实 I/O 故障，静默跳过会造成"返回 Ok 却漏刷"的假持久化，必须上抛
    if max_seg >= min_seg as i32 {
      let max_u32 = max_seg as u32;
      for sid in min_seg..=max_u32 {
        if let Entry::Vacant(e) = files.entry(sid) {
          match self.get_or_open_file(sid, false).await {
            Ok(file) => {
              e.insert(file);
            }
            // 空洞段（从未写入）或已并发截断的段（截断推进 start_segment 背书免责）
            Err(Error::SegmentNotFound(_)) => {}
            Err(e) => return Err(e),
          }
        }
      }
    }

    // 快速路径 1：无段需刷盘
    if files.is_empty() {
      #[cfg(debug_assertions)]
      self.debug_verify_synced(&pending, &files);
      return Ok(());
    }

    // 快速路径 2：单段高频场景（WAL 顺序追加写尾段），免除 join 堆内存分配
    if files.len() == 1
      && let Some(file) = files.values().next()
    {
      let res = if datasync {
        file.sync_data().await
      } else {
        file.sync_all().await
      };
      res.map_err(Error::from)?;
      #[cfg(debug_assertions)]
      self.debug_verify_synced(&pending, &files);
      return Ok(());
    }

    // 多段并发场景：在 compio 驱动下并发下发所有段的 sync，由操作系统内核与 NVMe 硬件并发刷新脏段
    // 采用 join_all 确保所有段均尽最大努力完成下发与落盘，避免短路取消导致后续段脏页滞留
    let results = join_all(files.values().map(|file| {
      let file = Rc::clone(file);
      async move {
        if datasync {
          file.sync_data().await
        } else {
          file.sync_all().await
        }
      }
    }))
    .await;

    let mut first_err = None;
    for res in results {
      if let Err(e) = res
        && first_err.is_none()
      {
        first_err = Some(Error::from(e));
      }
    }

    if let Some(err) = first_err {
      return Err(err);
    }

    // 仅在全部段成功刷盘后，才清位并断言契约（失败时保留在册脏段状态）
    #[cfg(debug_assertions)]
    self.debug_verify_synced(&pending, &files);

    Ok(())
  }

  /// 置位全局写入段位图（sync 持久化契约守护，仅 debug 构建参与编译）
  ///
  /// 超出守护窗口（前 128 段）的段不跟踪：守护目标为测试与 WAL 场景的契约回归，
  /// 位图窗口取 2 个 AtomicU64 编译期定长，避免动态扩容开销
  #[cfg(debug_assertions)]
  pub(super) fn debug_mark_dirty(&self, segment_id: u32) {
    let seg = segment_id as usize;
    if seg < SYNC_GUARD_SEGMENTS {
      self.dirty_segs[seg / 64].fetch_or(1 << (seg % 64), Ordering::Relaxed);
    }
  }

  /// 校验全局 sync 覆盖发起前在册的全部写入段并清位（sync 持久化契约守护，仅 debug 构建）
  ///
  /// `pending` 为 sync 发起时的在册脏段快照（位图置位发生在写入成功之后，故快照段
  /// 的段文件必已存在于磁盘，sync 补开循环必然能按文件存在性打开覆盖）。
  /// 违约判定：快照段既不在本次全局 fsync 覆盖集合（在表句柄按段
  /// 去重全集）中，也未获截断背书（段号 < `start_segment` 为已删除段）——脏页将
  /// 无人 fsync，断言失败暴露"句柄被异常路径移除"的契约破坏。快照后的并发新写入
  /// 不在本快照内，由下一次 sync 背书；免责段一并清位防陈旧位滞留
  #[cfg(debug_assertions)]
  fn debug_verify_synced(&self, pending: &[u32], synced: &HashMap<u32, Rc<File>>) {
    let start_seg = u64::from(self.start_segment.load(Ordering::Relaxed));
    for &seg in pending {
      let bit = !(1 << (seg as usize % 64));
      let covered = synced.contains_key(&seg) || u64::from(seg) < start_seg;
      assert!(
        covered,
        "sync 契约违约：段 {seg} 在册有写入，但本次全局 sync 未覆盖其句柄且段未被截断背书，脏页无人 fsync"
      );
      self.dirty_segs[seg as usize / 64].fetch_and(bit, Ordering::Relaxed);
    }
  }

  /// 清除全局位图中指定段的在册位（sync 持久化契约守护，仅 debug 构建参与编译）
  ///
  /// 供 `remove_segment` 调用：段被显式删除后其数据语义上已废弃，无须 fsync 背书
  #[cfg(debug_assertions)]
  pub(super) fn debug_clear_segment(&self, segment_id: u32) {
    let seg = segment_id as usize;
    if seg < SYNC_GUARD_SEGMENTS {
      self.dirty_segs[seg / 64].fetch_and(!(1 << (seg % 64)), Ordering::Relaxed);
    }
  }

  /// 读取契约守护仍在册的待 sync 段列表（仅 debug 构建编译；供测试与调用方观测
  /// "写入后、sync 前"的未覆盖窗口，sync 覆盖、显式删段或截断后即清空）
  #[cfg(debug_assertions)]
  pub fn debug_dirty_segments(&self) -> Vec<u32> {
    (0..SYNC_GUARD_SEGMENTS)
      .filter(|&seg| self.dirty_segs[seg / 64].load(Ordering::Relaxed) & (1 << (seg % 64)) != 0)
      .map(|seg| seg as u32)
      .collect()
  }
}
