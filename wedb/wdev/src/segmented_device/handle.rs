//! 段文件路径命名与 Thread-Per-Core 句柄表
//!
//! 对位 C# 设备族的句柄池与段名域基类（AsyncPool 与 StorageDeviceBase）：句柄按
//! `(device_id, segment_id)` 在本线程 TLS 内独占持有 `Rc<File>`，永不过线程；
//! Direct I/O 探测定型协议与新建段的目录项持久化同样落在本域的
//! `open_file` / `get_or_open_file`。
//!
//! 失效广播（对位 C# 进程级共享表的"全局失效"语义）：C# `LocalStorageDevice` 以
//! `SafeConcurrentDictionary<int, SafeFileHandle>` 托管句柄，`RemoveSegment`/`Reset`
//! 直接 `TryRemove` + `Dispose` 即对全线程生效（LocalStorageDevice.cs:37、:354-358、
//! :168-176）。Rust 侧 `compio::fs::File` 实测为 `!Send`（`File → AsyncFd → Attacher
//! → SharedFd → Rc<Inner<File>>`，未启用 `compio-driver/sync` feature），句柄无法
//! 迁移或被他线程释放，故等价语义改由**戳广播**承载：设备侧 [`SegmentedDevice::stamp`]
//! 发布失效戳，各线程在命中路径比戳、失配即 [`SegmentedDevice::reconcile`] 就地驱逐
//! 本线程陈旧句柄（`Rc` 归零当场 `close`）。写侧仅一次原子自增，读侧零跨核写。

use std::{
  cell::RefCell,
  ffi::OsString,
  fs::{create_dir_all, metadata},
  io::ErrorKind,
  path::{Path, PathBuf},
  rc::Rc,
  sync::atomic::Ordering,
};

use compio::fs::{File, OpenOptions, remove_file};
use wbase::{
  base32::{BASE32_LEN_U64, encode_u64},
  map::HashMap,
};

use super::SegmentedDevice;
use crate::error::{Error, Result};

/// 设备句柄失效戳：`(起始有效段号, 整表失效世代)`
///
/// 跨线程只读（设备侧发布，本结构无写者，仅在句柄入表/对账时记录观测值）：
///
/// - `start`：`start_segment` 即天然代际——常规回收（`truncate_until_segment` 与
///   容量逐出）单调推进它，各线程据此**精确**驱逐 `sid < start` 的陈旧句柄，
///   存活句柄零误伤、零重开；
/// - `epoch`：不推进 `start_segment` 的整表失效（`reset` 对标 C# 共享表全量
///   `TryRemove`、`remove_segment` 显式删段、Direct I/O 定型驱逐）单调自增，
///   令全线程在下次访问时弃表。
///
/// 不变量：本线程表内属于同一设备的句柄共享同一戳（等于该线程最近一次对账时
/// 观测到的设备戳），故命中路径只需比对查询键自身的戳，即可判定该设备的整表
/// 是否需要重清扫——无需任何额外的跨核扫描或线程注册表。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct HandleStamp {
  /// 观测时的起始有效段号
  start: u32,
  /// 观测时的整表失效世代
  epoch: u64,
}

/// 本线程句柄表项：`(入表时观测到的设备失效戳, 句柄)`
type Cached = (HandleStamp, Rc<File>);

thread_local! {
  /// Thread-Per-Core 本地段文件句柄缓存：
  /// 按 `(device_id, segment_id)` 隔离，单线程独占拥有 `Rc<File>`，
  /// 彻底杜绝全局并发 Map、跨核 cache-line 争用与原子引用计数开销
  pub(super) static LOCAL_FILES: RefCell<HashMap<(u64, u32), Cached>> =
    RefCell::new(HashMap::default());
  /// 本线程已对账到哪个设备戳（设备数级小表，仅慢路径查询）：
  /// 命中路径靠表项自带的戳比对判定，无须查此表；本表只用于让慢路径的
  /// O(表长) 清扫在状态未变时零成本跳过
  static LOCAL_STAMPS: RefCell<HashMap<u64, HandleStamp>> = RefCell::new(HashMap::default());
}

impl SegmentedDevice {
  /// 取设备当前句柄失效戳
  ///
  /// `start_segment` 沿用读写两侧一贯的 `SeqCst`（截断单调性与访问防御判据），
  /// 世代位仅需 `Relaxed`（单字段自增，与段号之间无须建立跨原子的全序）。
  #[inline]
  pub(super) fn stamp(&self) -> HandleStamp {
    HandleStamp {
      start: self.start_segment.load(Ordering::SeqCst),
      epoch: self.handle_epoch.load(Ordering::Relaxed),
    }
  }

  /// 广播整表失效：世代单调自增，全线程在下次句柄访问时对账弃表
  ///
  /// 对位 C# 从进程级共享表 `TryRemove` + `Dispose`（全线程即时生效）。本调用只
  /// 做一次原子加一，且不落在常规 I/O 快路径上（仅 `reset`/`remove_segment`/
  /// Direct I/O 定型），故热路径仍保持 0 跨核写。
  #[inline]
  pub(super) fn broadcast_invalid(&self) {
    self.handle_epoch.fetch_add(1, Ordering::Release);
  }

  /// 本线程句柄表与设备当前失效戳对账（幂等，跨线程失效广播的落地单点）
  ///
  /// 状态未变时一次小表查询即返回；状态已变则单次遍历完成三件事——判定存活、
  /// 驱逐失效项（`Rc` 归零当场关闭 fd、释放已解除链接 inode 占用的磁盘空间）、
  /// 把存活项观测戳对齐到当前值（维持"同设备同戳"不变量，同一失效只触发一次清扫）。
  pub(super) fn reconcile(&self, cur: HandleStamp) {
    let dev = self.device_id;
    let reconciled = LOCAL_STAMPS.with(|s| s.borrow().get(&dev).is_some_and(|&s| s == cur));
    if reconciled {
      return;
    }
    LOCAL_STAMPS.with(|s| {
      s.borrow_mut().insert(dev, cur);
    });
    LOCAL_FILES.with(|m| {
      m.borrow_mut().retain(|&(d, sid), cached| {
        if d != dev {
          return true;
        }
        // 截断线之前的段文件已被删除、被整表失效世代弃用的句柄不得再承接 I/O
        let live = sid >= cur.start && cached.0.epoch == cur.epoch;
        if live {
          cached.0 = cur;
        }
        live
      });
    });
  }

  /// 本线程在表的段号集合（升序，仅供测试观测）
  ///
  /// 对标 C# 侧对 `logHandles` 直接断言的测试口径：TLS 表项即 fd 的唯一持有者，
  /// 表内无陈旧项即 fd 已当场关闭、空间已回收。
  #[cfg(debug_assertions)]
  pub fn debug_local_segments(&self) -> Vec<u32> {
    let dev = self.device_id;
    let mut segs: Vec<u32> = LOCAL_FILES.with(|m| {
      m.borrow()
        .keys()
        .filter_map(|k| (k.0 == dev).then_some(k.1))
        .collect()
    });
    segs.sort_unstable();
    segs
  }

  /// 本线程彻底遗忘本设备的句柄与对账记录（仅供设备析构调用，不做跨线程广播）
  ///
  /// 设备生命周期终结后，本线程在表句柄就是该设备在本线程的最后 fd 持有者：不
  /// 主动释放即占用到线程退出为止（长生命周期工作线程 + 反复建销设备的场景），
  /// Windows 下 `delete_on_close` 更会因在册句柄未关而删不掉段文件。他线程的句柄
  /// 无从代为释放（`File` 实测 `!Send`），随其下次访问的对账或线程退出回收
  pub(super) fn forget_local_handles(&self) {
    let dev = self.device_id;
    LOCAL_FILES.with(|m| {
      m.borrow_mut().retain(|&(d, _), _| d != dev);
    });
    LOCAL_STAMPS.with(|s| {
      s.borrow_mut().remove(&dev);
    });
  }
}

/// fsync 父目录持久化新建文件的目录项
///
/// 委托公共原语 [`crate::sync_dir`]。POSIX 语义下新建段文件后须 fsync 父目录，
/// 失败降级为 warn 日志不阻断写入；非 Unix 平台无目录 fsync 原语，直接跳过。
fn sync_dir(parent: &Path) {
  #[cfg(unix)]
  if let Err(e) = crate::sync_dir(parent) {
    log::warn!(
      "新建段文件后 fsync 父目录 {} 失败: {e}，崩溃后新段可能不可见",
      parent.display()
    );
  }
  #[cfg(not(unix))]
  let _ = parent;
}

impl SegmentedDevice {
  /// 获取父目录路径（base_path 无父目录分量时以当前工作目录 "." 兜底）
  #[inline]
  pub(super) fn parent_dir(&self) -> &Path {
    match self.base_path.parent() {
      Some(p) if !p.as_os_str().is_empty() => p,
      _ => Path::new("."),
    }
  }

  /// 获取指定段编号对应的实际文件路径
  pub fn segment_path(&self, segment_id: u32) -> PathBuf {
    match self.segment_size {
      // OsString 拼接保证非 UTF-8 路径的字节精确性（对标 libs/storage/Tsavorite/cs/src/core/Device/StorageDeviceBase.cs:GetSegmentFilename）；
      // 段号编码为 13 字符定长小写 Base32（转写规范偏离，优先于 C# 十进制 1:1）：
      // 定长保证文件名字典序与段号数值序严格一致，杜绝十进制 ".10" < ".2" 的字典序倒挂
      Some(_) => {
        let seg_str = encode_u64(u64::from(segment_id));
        let base = self.base_path.as_os_str();
        let mut path = OsString::with_capacity(base.len() + 1 + BASE32_LEN_U64);
        path.push(base);
        path.push(".");
        path.push(seg_str);
        PathBuf::from(path)
      }
      None => self.base_path.clone(),
    }
  }

  /// 构造文件打开选项
  ///
  /// `create` 决定可写打开是否携带 O_CREAT：读写路径 true（对标 C# `OpenOrCreate`），
  /// sync 补开路径 false（仅打开已存在段——元数据预检后、open 前段文件被并发删除
  /// （`remove_segment`/外部删除）时以 NotFound 失败而非竞态窗口重建幽灵段）
  #[inline]
  fn open_options(read_only: bool, create: bool) -> OpenOptions {
    let mut opts = OpenOptions::new();
    opts.read(true);
    if read_only {
      opts.write(false).create(false);
    } else {
      opts.write(true).create(create);
    }
    opts
  }

  async fn try_preallocate(file: &File, path: &Path, preallocate: Option<u64>) {
    if let Some(sz) = preallocate
      && let Err(e) = file.set_len(sz).await
    {
      // 预分配仅为性能提示（写路径按需扩展文件），失败降级不致命，但须可观测
      log::warn!("段文件 {} 预分配至 {sz} 字节失败: {e}", path.display());
    }
  }

  /// 异步打开文件句柄（Linux O_DIRECT、只读保护与预分配）
  ///
  /// Direct I/O 启用策略对齐 C# 设备族：Linux 原生设备（libaio/io_uring）默认 O_DIRECT，
  /// 其余平台对齐 Managed 设备采用缓冲 I/O（享受 OS 页缓存与预读）；
  /// 只读打开同样适用 Direct（对标 C# disableFileBuffering 与 readOnly 可自由组合，
  /// 读路径绕过页缓存污染）。
  ///
  /// 探测定型（与 C# 的差异论证）：C# NativeDevice 以 O_DIRECT 打开失败即异常上抛，
  /// 无回退；早期 Rust 实现的"任意时刻失败即全局翻转"存在竞争隐患（并发写入途中
  /// 翻转标志并驱逐全部线程句柄）。本实现收窄为首次真实打开时探测一次支持性：
  /// 不支持类错误一次性定型为缓冲 I/O（此后 write 路径无运行中翻转），定型后
  /// Direct 打开失败直接上抛，语义对齐 C# 的快速失败。
  async fn open_file(
    &self,
    path: &Path,
    read_only: bool,
    preallocate: Option<u64>,
    create: bool,
  ) -> Result<File> {
    if !read_only
      && let Some(parent) = path.parent()
      && !parent.as_os_str().is_empty()
    {
      let _ = create_dir_all(parent);
    }

    #[cfg(target_os = "linux")]
    if self.direct_io.load(Ordering::Relaxed) {
      let mut opts = Self::open_options(read_only, create);
      opts.custom_flags(libc::O_DIRECT);
      match opts.open(path).await {
        Ok(file) => {
          self.direct_io_probed.store(true, Ordering::Relaxed);
          log::debug!("成功以 Direct I/O (O_DIRECT) 打开文件: {}", path.display());
          if !read_only {
            Self::try_preallocate(&file, path, preallocate).await;
          }
          return Ok(file);
        }
        Err(e) if matches!(e.kind(), ErrorKind::InvalidInput | ErrorKind::Unsupported) => {
          // 未定型时才允许降级：仅对文件系统/内核不支持类错误定型，其余错误原样
          // 上抛避免掩盖真实故障；定型时广播整表失效，驱逐**全部线程**已打开的
          // O_DIRECT 句柄，防止陈旧句柄收到未对齐 I/O (EINVAL)。此后 direct_io 恒为
          // false，写入路径不再回退；本线程的弃表由调用方慢路径末端的对账完成
          if !self.direct_io_probed.swap(true, Ordering::Relaxed) {
            self.direct_io.store(false, Ordering::Relaxed);
            self.broadcast_invalid();
            log::error!(
              "Direct I/O 探测失败（{}: {e}），设备一次性定型为常规缓存 I/O，此后 Direct 打开失败将直接上抛",
              path.display()
            );
          } else if !self.direct_io.load(Ordering::Acquire) {
            // 并发首探竞态败方：胜方已将设备定型为常规缓存 I/O，落入下方
            // 缓冲打开重试，避免吃到本应一次性定型即可避免的假错误
            log::debug!(
              "并发 Direct I/O 探测竞态败方（{e}），按定型后的常规缓存 I/O 打开: {}",
              path.display()
            );
          } else {
            // Direct 形态（探测成功定型）下的打开失败：设计路径直接上抛，不回退。
            // 与胜方 swap→store 两指令窗口并发的败方可能在此误上抛一次伪失败
            //（无句柄副作用，下次重试即恢复；消除需合并双原子为状态字，收益不抵复杂度）
            return Err(Error::from(e));
          }
        }
        Err(e) => return Err(Error::from(e)),
      }
    }

    let file = Self::open_options(read_only, create).open(path).await?;
    log::debug!("成功打开文件: {}", path.display());

    if !read_only {
      Self::try_preallocate(&file, path, preallocate).await;
    }

    Ok(file)
  }

  /// 获取或异步打开指定段的句柄（当前 CPU 核心本地缓存优先，0 跨核争用）
  ///
  /// `create` 为 true 时段文件缺失即物理新建（对标 C# `GetOrAddHandle` 的
  /// OpenOrCreate 语义，读写路径专用）；为 false 时仅打开磁盘上已存在的段文件，
  /// 缺失段返回 [`Error::SegmentNotFound`]（sync 补开路径专用，杜绝从未写入的
  /// 空洞段因 sync 被幽灵创建，对齐 `recover` 的幽灵段防御）。
  ///
  /// 命中路径只做一次 TLS 查表 + 一次失效戳整数比对，即他线程的截断/整表失效
  /// 广播在此被感知并就地驱逐陈旧句柄；0 跨核写、0 锁、0 原子 RMW。
  pub(super) async fn get_or_open_file(&self, segment_id: u32, create: bool) -> Result<Rc<File>> {
    // 0. 取设备当前失效戳，并做防御校验：已被截断的段严禁访问（与 Garnet begin_segment_ 语义一致）
    let cur = self.stamp();
    if segment_id < cur.start {
      return Err(Error::SegmentNotFound(segment_id));
    }

    let key = (self.device_id, segment_id);

    // 1. 快速路径：Thread-local 极速命中（L1 CPU 本地缓存，0 原子、0 锁、0 争用）；
    //    戳一致即整表在册（"同设备同戳"不变量），戳落后说明他线程已广播失效
    if let Some(file) = LOCAL_FILES.with(|m| {
      m.borrow()
        .get(&key)
        .filter(|(obs, _)| *obs == cur)
        .map(|(_, f)| Rc::clone(f))
    }) {
      return Ok(file);
    }

    // 2. 戳落后或未命中：先与本线程表对账（就地关闭已截断段与整表失效的句柄），
    //    再复核存活句柄——存活项无须重开，直接命中
    self.reconcile(cur);
    if let Some(file) = LOCAL_FILES.with(|m| m.borrow().get(&key).map(|(_, f)| Rc::clone(f))) {
      return Ok(file);
    }

    // 3. 缓存确实无此项：当前线程驱动下异步打开文件
    let path = self.segment_path(segment_id);
    // 写模式下先探测段文件是否缺失：open(create) 将物理新建段文件，成功后须
    // fsync 父目录持久化目录项（探测与打开之间被并发抢先建文件仅多刷一次目录，无害）
    let is_new_segment = if create {
      !self.read_only && metadata(&path).is_err_and(|e| e.kind() == ErrorKind::NotFound)
    } else {
      // 仅打开已存在的段：他线程写入过的段必然已创建文件；缺失段无须为其刷盘
      match metadata(&path) {
        Ok(_) => false,
        Err(e) if e.kind() == ErrorKind::NotFound => {
          return Err(Error::SegmentNotFound(segment_id));
        }
        Err(e) => return Err(e.into()),
      }
    };
    let prealloc = if self.preallocate && !self.read_only {
      self.segment_size
    } else {
      None
    };
    let file = match self
      .open_file(&path, self.read_only, prealloc, create)
      .await
    {
      Ok(f) => f,
      // 补开路径（create=false）的删除竞态兜底：元数据预检后、open 前段文件被并发
      // 删除，不带 O_CREAT 的 open 以 NotFound 失败，映射为 SegmentNotFound——
      // 与预检同口径，杜绝竞态窗口物理重建幽灵段文件
      Err(Error::Io(e)) if !create && e.kind() == ErrorKind::NotFound => {
        return Err(Error::SegmentNotFound(segment_id));
      }
      Err(e) => return Err(e),
    };
    if is_new_segment {
      sync_dir(self.parent_dir());
    }

    // 4. 复核打开期间是否被并发截断，若是则清理并拒绝
    let cur = self.stamp();
    if segment_id < cur.start {
      if !self.read_only {
        let _ = remove_file(&path).await;
      }
      return Err(Error::SegmentNotFound(segment_id));
    }

    // 5. 入表前先按新戳对账（打开期间他线程的截断/整表失效在此关闭），并以该戳
    //    登记；此后若再有并发截断，本项戳号落后，下一次访问的对账即驱逐它
    let rc = Rc::new(file);
    self.reconcile(cur);
    LOCAL_FILES.with(|m| {
      m.borrow_mut().insert(key, (cur, Rc::clone(&rc)));
    });

    Ok(rc)
  }

  /// 重置设备句柄缓存：关闭并遗忘**全线程**在本设备上打开的文件句柄
  ///
  /// 对位 `garnet/libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs:168-176`
  /// `Reset` 对进程级共享表整段 `TryRemove` + `Dispose`（全线程即时生效）：本方法
  /// 先广播整表失效世代，再就地对账关闭本线程句柄，他线程在下次句柄访问时同样
  /// 关闭并弃表（与 C# 的唯一残余差异是关闭时点推迟到该线程下次访问，见模块注记）
  pub fn reset(&self) {
    self.broadcast_invalid();
    self.reconcile(self.stamp());
    #[cfg(debug_assertions)]
    {
      // 遗忘句柄后 fsync 职责移交给后续任意线程的重新打开（fsync 按 inode 全量生效，
      // 关闭 fd 不丢内核脏页），在册写入位图随之失效，全部清除避免守护误报
      for word in &self.dirty_segs {
        word.store(0, Ordering::Relaxed);
      }
    }
  }
}
