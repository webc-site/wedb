//! whlog：混合日志（Hybrid Log）分配器与页缓冲
//!
//! 对标 C# Tsavorite `AllocatorBase`/`IDeltaLog` 系列的日志分配、循环页缓冲与
//! 刷盘流水线，提供逻辑地址分配、页装载/驱逐与扫描迭代。
//!
//! 注意：本 crate 是**存储引擎混合日志**，与 whyperlog（HyperLogLog 基数估计
//! 概率数据结构，对标 libs/server/Resp/HyperLogLog/HyperLogLog.cs）职责完全不同，
//! 二者仅命名相近，严禁混淆。
//!
//! 与 waof `WalLog` 的分工边界（双日志分层，非重复实现）：waof wal/ 是**物理 WAL**
//! （先写日志、按事务提交序定水位，对标 C# Tsavorite TsavoriteLog 的 CommitAsync
//! 提交流水），本 crate 是**混合日志**（存储引擎主日志，页缓冲 + 纪元驱逐，对标
//! C# AllocatorBase 的 AsyncFlushPagesForSnapshot 页刷盘流水）；二者在 C# 同样是
//! 两套同形流水线。相同的环形页缓冲刷盘/截断内核单点在 `wdev::Device`
//! （`flush_range_aligned` 与 `truncate_begin_until`），两侧均经此落盘，不再各自下沉。
#![cfg_attr(docsrs, feature(doc_cfg))]

mod address;
mod buffer;
mod config;
mod error;
mod flush;
mod hlog;
mod output;
mod scan;
mod walk;

pub use address::{AddressManager, AddressSnapshot};
pub use buffer::CircularPageBuffer;
pub use config::{
  DEFAULT_INITIAL_ADDRESS, DEFAULT_MUTABLE_FRACTION, DEFAULT_NUM_PAGES, DEFAULT_PAGE_SIZE,
  DEFAULT_SERVER_PAGE_SIZE, HybridLogConfig, SECTOR_ALIGNMENT, ro_lag_num_from_fraction,
};
pub use error::{Error, Result};
pub use flush::{PageFlushRange, PendingFlushList};
pub use hlog::{HybridLog, RevivifyArgs, VERSION_MASK, VERSION_SHIFT_OPEN_BIT};
pub use output::RecordOutput;
pub use scan::ScanIterator;
pub use walk::for_each_record_in_page;

#[cfg(debug_assertions)]
pub use crate::hlog::{EncodeStall, VersionReadStall};
