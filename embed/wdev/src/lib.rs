//! 块存储设备层：段文件设备 ([`SegmentedDevice`])、Direct I/O 与设备抽象 ([`Device`])。
//!
//! ## 持久化契约（wedb_wal / wedb_hlog 等调用方依赖）
//!
//! - [`Device::sync`] / [`Device::sync_data`]：全局持久化屏障，语义对齐 C#
//!   `LocalStorageDevice`（句柄表进程级共享、任意线程可 sync）——任一线程调用即
//!   遍历设备上全部在表句柄（按段号去重后逐 inode fsync/fdatasync），覆盖所有线程
//!   在 sync 发起前完成的写入，release 构建无静默丢失窗口。跨线程 fsync 的可行性
//!   由 `compio-driver/sync` feature 保证（SharedFd 为 `Arc`，fd 无线程亲和）。
//!   排序契约与 POSIX fsync 同口径：仅覆盖 sync 发起前已完成的写入。
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

pub use device::{Device, StorageDevice};
pub use error::{Error, Result};
pub use null::NullDevice;
pub use segmented_device::SegmentedDevice;
pub use sys::{
  FALLBACK_CPU_CORES, FALLBACK_SYSTEM_MEMORY_BYTES, MAX_SEGMENT_SIZE, detect_cpu_cores,
  detect_system_memory,
};
// 例外 re-export（偏离"禁止 pub use 第三方"约定）：whlog 测试经 `wdev::BufferPool`
// 引用池类型；设备与池总是成对出现，随设备层一并导出属稳定契约，非冗余别名
pub use wram::BufferPool;
