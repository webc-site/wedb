//! 段文件路径命名与 Thread-Per-Core 句柄表
//!
//! 对位 C# 设备族的句柄池与段名域基类（AsyncPool 与 StorageDeviceBase）：句柄按
//! `(device_id, segment_id)` 在本线程 TLS 内独占持有 `Rc<File>`，永不过线程；
//! Direct I/O 探测定型协议与新建段的目录项持久化同样落在本域的
//! `open_file` / `get_or_open_file`。

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
use gxhash::HashMap;
use wbase::base32::{BASE32_LEN_U64, encode_u64};

use super::SegmentedDevice;
use crate::error::{Error, Result};

thread_local! {
  /// Thread-Per-Core 本地段文件句柄缓存：
  /// 按 `(device_id, segment_id)` 隔离，单线程独占拥有 `Rc<File>`，
  /// 彻底杜绝全局并发 Map、跨核 cache-line 争用与原子引用计数开销
  pub(super) static LOCAL_FILES: RefCell<HashMap<(u64, u32), Rc<File>>> =
    RefCell::new(HashMap::default());
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
          // 上抛避免掩盖真实故障；定型时驱逐全部已打开的 O_DIRECT 句柄，防止陈旧
          // 句柄收到未对齐 I/O (EINVAL)。此后 direct_io 恒为 false，写入路径不再回退
          if !self.direct_io_probed.swap(true, Ordering::Relaxed) {
            self.direct_io.store(false, Ordering::Relaxed);
            LOCAL_FILES.with(|m| {
              m.borrow_mut()
                .retain(|&(dev_id, _), _| dev_id != self.device_id);
            });
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
  pub(super) async fn get_or_open_file(&self, segment_id: u32, create: bool) -> Result<Rc<File>> {
    // 0. 防御校验：已被截断的段严禁访问（与 Garnet begin_segment_ 语义一致）
    if segment_id < self.start_segment.load(Ordering::SeqCst) {
      return Err(Error::SegmentNotFound(segment_id));
    }

    let key = (self.device_id, segment_id);

    // 1. 快速路径：Thread-local 极速命中（L1 CPU 本地缓存，0 原子、0 锁、0 争用）
    if let Some(file) = LOCAL_FILES.with(|m| m.borrow().get(&key).cloned()) {
      return Ok(file);
    }

    // 2. 缓存未命中：当前线程驱动下异步打开文件
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

    // 3. 复核打开期间是否被并发截断，若是则清理并拒绝
    let truncated = segment_id < self.start_segment.load(Ordering::SeqCst);
    if truncated {
      if !self.read_only {
        let _ = remove_file(&path).await;
      }
      return Err(Error::SegmentNotFound(segment_id));
    }

    let rc = Rc::new(file);
    LOCAL_FILES.with(|m| {
      m.borrow_mut().insert(key, Rc::clone(&rc));
    });

    Ok(rc)
  }

  /// 重置设备句柄缓存（关闭并遗忘当前线程打开的文件句柄，与 C# IDevice.Reset 语义一致）
  pub fn reset(&self) {
    LOCAL_FILES.with(|m| {
      m.borrow_mut()
        .retain(|&(dev_id, _), _| dev_id != self.device_id);
    });
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
