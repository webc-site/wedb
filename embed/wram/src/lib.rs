//! 原生内存层：直接虚拟内存与分配追踪 (对标 C# Tsavorite `core/Native/`)
//!
//! - [`DirectVirtualMemory`] / [`DirectVmBlock`] ← `Native/DirectVirtualMemory.cs`：
//!   mmap/VirtualAlloc 按需置零大块映射，供哈希索引表 (windex) 等长生命周期单例使用
//! - [`NativeMemoryTracker`] ← `Native/NativeMemoryTracker.cs`：条带化无锁内存记账
//!
//! # 兼容门面
//!
//! 扇区对齐缓冲池 ([`BufferPool`]) 与 [`AlignedBuf`] 的本体已下沉至 wbase
//! (对标 C# `core/Utilities` 位于依赖图最底层、被 Device/Allocator/TsavoriteLog
//! 平行引用的拓扑；原先 wdev(Device)→wram(Allocator) 的反向依赖已纠正)。
//! 下列再导出仅为存量调用方 (embed 外部 node/waof) 的兼容门面，语义与 C#
//! `AllocatorBase` → `Utilities/BufferPool` 的引用方向一致，新代码请直接用 wbase。

#![cfg_attr(docsrs, feature(doc_cfg))]

mod align;
mod direct_vm;
mod tracker;

pub use align::{
  DEFAULT_SECTOR_SIZE, MIN_SECTOR_SIZE, SectorRange, SectorRangeError, align_down, align_up,
  checked_align_up, is_aligned, is_valid_sector_size,
};
pub use direct_vm::{DirectVirtualMemory, DirectVmBlock, system_page_size};
pub use tracker::NativeMemoryTracker;
/// 兼容门面：内存/对齐错误与缓冲池族原语本体在 wbase (见模块级文档)
pub use wbase::{
  AlignedBuf, BufferPool, CLASS_CAPACITIES_SECTORS, DEFAULT_LARGE_BUDGET_BYTES,
  DEFAULT_SMALL_BUDGET_BYTES, DEPOT_STRIPE_CAP, Error, LARGE_TIER_MIN_BYTES, MAX_LOCAL_PER_CLASS,
  MAX_POOLED_SECTORS, NUM_CLASSES, PoolStats, Result, class_capacity_bytes, class_capacity_sectors,
  class_of_sectors, current_thread_id,
};
