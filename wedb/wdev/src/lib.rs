//! 块存储设备层：段文件设备 ([`SegmentedDevice`])、Direct I/O 与设备抽象 ([`Device`])。
//!
//! ## 持久化契约（wedb_wal / wedb_hlog 等调用方依赖）
//!
//! - [`Device::sync`] / [`Device::sync_data`]：全局持久化屏障，语义对齐 C#
//!   `LocalStorageDevice`（句柄表进程级共享、任意线程可 sync）——任一线程调用即
//!   在册段落盘，release 构建无静默丢失窗口。在 Thread-Per-Core 架构下，句柄由
//!   各线程 Thread-Local (`LOCAL_FILES`) 本地持有 (`Rc<File>`)，零跨核争用；
//!   排序契约与 POSIX fsync 同口径：仅覆盖 sync 发起前已完成的写入。
//! - 句柄生命周期：段文件句柄由各线程 Thread-Local 持有，随该线程的截断驱逐、
//!   `reset` 显式清理或线程退出而回收（对标 C# `Dispose` 关闭进程级共享句柄表；
//!   长生命周期工作线程弃用设备前应调用 [`Device::reset`] 释放本线程句柄，
//!   fd 占用上界为 线程数 × 在册段数）。
//! - 目录项持久化：段文件由设备层创建时会同步 fsync 其父目录（Unix 平台
//!   open(dir) + fsync 一次性成本；Windows 平台不支持，见 segmented_device
//!   模块内说明），因此 sync 返回后"新段写入 + sync 即持久"的承诺同时覆盖
//!   段数据与新建段的目录项，调用方无需自行刷盘目录。
//! - 新建段后的父目录 fsync 执行次数可通过 `SegmentedDevice::dir_sync_count` 计数器观测。
//! - 契约守护（仅 debug 构建参与编译）：`SegmentedDevice` 内部全局位图跟踪
//!   前 128 段的"已写入待 sync"状态，sync 末尾校验全部在册写入被本次 fsync 覆盖
//!   （截断或显式删段免责），违约断言失败；`SegmentedDevice::debug_dirty_segments`
//!   可观测当前未覆盖窗口。
//! - Direct I/O（Linux）：首个段文件打开时探测一次支持性并定型，定型后打开失败
//!   直接上抛，写入路径不存在运行中回退（详见 `SegmentedDevice` 的 `direct_io` 文档）。

#![cfg_attr(docsrs, feature(doc_cfg))]

mod chunk;
mod device;
mod error;
mod null;
mod segmented_device;
mod sys;

#[cfg(unix)]
use std::fs::File;
use std::{io, path::Path};

pub use device::Device;
pub use error::{Error, Result};
pub use null::NullDevice;
pub use segmented_device::SegmentedDevice;
pub use sys::{MAX_SEGMENT_SIZE, detect_cpu_cores, detect_system_memory};
// 例外 re-export（偏离"禁止 pub use 第三方"约定）：whlog 测试经 `wdev::BufferPool`
// 引用池类型；设备与池总是成对出现，随设备层一并导出属稳定契约，非冗余别名
pub use wbase::BufferPool;

/// fsync 目录项以确保文件创建/重命名等目录元数据变更掉电持久。
///
/// POSIX 语义下新建或重命名文件仅保证其数据与 inode 持久，不保证父目录项
/// 在断电崩溃后可见，须额外 fsync 父目录。
/// Windows 平台无等价机制（`FlushFileBuffers` 对目录句柄拒绝访问，NTFS 目录项
/// 随卷元数据自动持久），非 Unix 平台为空操作直接返回 `Ok(())`。
#[cfg(unix)]
#[inline]
pub fn sync_dir(dir: &Path) -> io::Result<()> {
  File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
#[inline]
pub fn sync_dir(_dir: &Path) -> io::Result<()> {
  // 非 Unix 系统目录无须单独 fsync，保留 _dir 形参以对齐跨平台统一函数签名
  Ok(())
}
