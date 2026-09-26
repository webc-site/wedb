//! 块存储设备层：段文件设备 ([`SegmentedDevice`])、Direct I/O 与设备抽象 ([`Device`])。
//!
//! ## 持久化契约（wedb_wal / wedb_hlog 等调用方依赖）
//!
//! - [`Device::sync`] / [`Device::sync_data`]：全局持久化屏障，语义对齐 C#
//!   `LocalStorageDevice`（句柄表进程级共享、任意线程可 sync）——任一线程调用即
//!   在册段落盘，release 构建无静默丢失窗口。在 Thread-Per-Core 架构下，句柄由
//!   各线程 Thread-Local (`LOCAL_FILES`) 本地持有 (`Rc<File>`)，零跨核争用；
//!   排序契约与 POSIX fsync 同口径：仅覆盖 sync 发起前已完成的写入。
//! - 句柄生命周期：段文件句柄由各线程 Thread-Local 持有，失效语义对位 C# 进程级
//!   共享表的**全局失效**——截断/容量逐出推进 `start_segment`（天然代际）、
//!   `reset`/`remove_segment`/Direct I/O 定型自增 `handle_epoch`，任一广播对**全部
//!   线程**生效：各线程在下次句柄访问时驱逐陈旧项，`Rc<File>` 归零当场关闭 fd、
//!   已删段的磁盘空间随即回收（不再依赖该线程自身再跑一次截断，fd 不再单调增长）。
//!   与 C# 的残余差异仅两处，均由 `compio::fs::File` 实测 `!Send`（内含 `Rc`，
//!   句柄不可跨线程迁移或代释放）导出：① 关闭发生在被通知线程的下次访问，而非
//!   广播线程当场；② 广播前已短路的在途 I/O 由 compio `SharedFd` 计数延命。
//!   设备析构时本线程在表句柄当场关闭。fd 占用上界为 线程数 × 在册（未截断）段数。
//! - 目录项持久化：段文件由设备层创建时会同步 fsync 其父目录（Unix 平台
//!   open(dir) + fsync 一次性成本；Windows 平台不支持，见 segmented_device
//!   模块内说明），因此 sync 返回后"新段写入 + sync 即持久"的承诺同时覆盖
//!   段数据与新建段的目录项，调用方无需自行刷盘目录。
//! - 设备旋钮（容量上限/预分配/只读/关闭即删）只有 [`DeviceParams`] 构造注入一条
//!   通道，对标 C# 设备构造形参，运行期无可变入口。
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
mod segmented_device;
mod sys;

#[cfg(unix)]
use std::fs::File;
use std::{io, path::Path};

pub use device::Device;
pub use error::{Error, FlushError, Result};
pub use segmented_device::{DeviceParams, SegmentedDevice};
pub use sys::{MAX_SEGMENT_SIZE, detect_cpu_cores, detect_system_memory};

/// fsync 目录项以确保文件创建/重命名等目录元数据变更掉电持久。
///
/// POSIX 语义下新建或重命名文件仅保证其数据与 inode 持久，不保证父目录项
/// 在断电崩溃后可见，须额外 fsync 父目录。
/// Windows 平台无等价机制（`FlushFileBuffers` 对目录句柄拒绝访问，NTFS 目录项
/// 随卷元数据自动持久），非 Unix 平台为空操作直接返回 `Ok(())`。
///
/// # 持久化发布双屏障口径（全仓唯一定义处）
///
/// 「一个文件发布掉电持久」= 数据屏障 + 目录屏障，缺一不可：
/// 1. **数据屏障**：文件内容 `sync_all`，由写方负责（设备层 [`Device::sync`]、
///    引擎 VFS flush、迁移接收端反序列化器等）；
/// 2. **目录屏障**：新建文件或 `rename` 换入后调用本函数 fsync 父目录，令
///    目录项变更掉电可见。
///
/// 仅做 1 不做 2，断电后文件可能整体消失（发布半途而废，调用方已确认的操作
/// 复活回旧状态）；仅做 2 不做 1，文件可见但内容回退到旧页。调用方
/// （wcpr 检查点 `sync_dir_tree`、wbftree 迁移发布、本 crate 段文件创建）
/// 统一引用本口径，不得各自内联 `open(dir) + fsync`。
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
