//! 原生内存层：直接虚拟内存与分配追踪 (对标 C# Tsavorite `core/Native/`)
//!
//! - [`DirectVirtualMemory`] / [`DirectVmBlock`] ← `Native/DirectVirtualMemory.cs`：
//!   mmap/VirtualAlloc 按需置零大块映射，供哈希索引表等长生命周期单例使用
//! - [`NativeMemoryTracker`] ← `Native/NativeMemoryTracker.cs`：条带化无锁内存记账
//!
//! 扇区对齐缓冲池与扇区对齐数学的本体在 wbase（对标 C# `core/Utilities`
//! 位于依赖图最底层、被 Device/Allocator/TsavoriteLog 平行引用的拓扑），
//! 需要时直接依赖 wbase 的 `pool` / `align` feature。
//!
//! 预留层现状：当前 wkv 索引走 [`crate::HashIndex`]（进程内原生内存表），
//! 本层 mmap 直接虚拟内存为后续大规模索引预留，现仅测试引用
//! （windex/tests/ram），无生产调用方。

mod direct_vm;
mod tracker;

pub use direct_vm::{DirectVirtualMemory, DirectVmBlock, system_page_size};
pub use tracker::NativeMemoryTracker;
