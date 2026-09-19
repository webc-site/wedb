//! whlog：混合日志（Hybrid Log）分配器与页缓冲
//!
//! 对标 C# Tsavorite `AllocatorBase`/`IDeltaLog` 系列的日志分配、循环页缓冲与
//! 刷盘流水线，提供逻辑地址分配、页装载/驱逐与扫描迭代。
//!
//! 注意：本 crate 是**存储引擎混合日志**，与 whyperlog（HyperLogLog 基数估计
//! 概率数据结构，对标 libs/server/Resp/HyperLogLog/HyperLogLog.cs）职责完全不同，
//! 二者仅命名相近，严禁混淆。
#![cfg_attr(docsrs, feature(doc_cfg))]

mod address;
mod buffer;
mod config;
mod error;
mod flush;
mod hlog;
mod output;
mod scan;

pub use address::{AddressManager, AddressSnapshot};
pub use buffer::CircularPageBuffer;
pub use config::{
  DEFAULT_INITIAL_ADDRESS, DEFAULT_MUTABLE_FRACTION, DEFAULT_NUM_PAGES, DEFAULT_PAGE_SIZE,
  DEFAULT_SERVER_PAGE_SIZE, HybridLogConfig, SECTOR_ALIGNMENT, ro_lag_num_from_fraction,
};
pub use error::{Error, Result};
pub use flush::{PageFlushRange, PendingFlushList};
pub use hlog::HybridLog;
pub use output::RecordOutput;
pub use scan::ScanIterator;
