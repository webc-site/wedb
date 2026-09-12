[English](#en) | [中文](#zh)

---

<a name="en"></a>

# wdev : Block Storage Device Layer

- [Introduction](#introduction)
- [Module Layout](#module-layout)
- [Core API](#core-api)
- [Design Notes](#design-notes)
- [Test Coverage](#test-coverage)

## Introduction

wdev provides the segmented-file device (`SegmentedDevice`), Direct I/O, and the device abstraction (`Device`) — the persistence foundation for upper log engines such as whlog / waof.

It is built on the compio async runtime: io_uring on Linux, IOCP on Windows, kqueue on macOS. Files are organized in segments; a segment size must be a power of two and at least sector_size, with `MAX_SEGMENT_SIZE = 2^62`. Segment files are named `<base>.<segment-id>`, where the id is a fixed-width 13-char lowercase Base32 (a transpile-spec deviation from the C# decimal ids), so lexicographic file-name order always equals numeric segment order.

## Module Layout

- `device`: device trait `Device` defining sector size, segment size, Direct I/O, and read/write interfaces
- `segmented_device`: segmented-file device; handles held Thread-Local per (device id, segment id) as `Rc<File>` (zero cross-core contention; papaya is compiled in only for the Windows deferred-deletion queue)
- `chunk`: sector/segment slicing, cross-segment and single-segment I/O boundary iteration with alignment checks
- `null`: `NullDevice`, instant fake-success I/O with zero physical I/O
- `sys`: dependency-free cross-platform hardware probing (CPU cores, system memory), falling back to `FALLBACK_CPU_CORES = 4` and `FALLBACK_SYSTEM_MEMORY_BYTES = 4 GiB`
- `error`: error types (alignment / out-of-bounds / missing-segment validation errors)

## Core API

- `Device`: abstraction; `write_aligned` / `read_aligned` require offset / len to be multiples of sector_size with aligned buffers, while `read_range` has no alignment requirement (exact logical-range reads in buffered-I/O mode)
- `SegmentedDevice`: segmented-file device; `dir_sync_count()` observes parent-directory fsyncs
- `NullDevice`: empty device for tests and benchmarks
- `detect_cpu_cores()` / `detect_system_memory()`: hardware probing
- Re-exports `wbase::BufferPool` for building aligned buffers

## Design Notes

- Persistence contract: `sync` / `sync_data` are global barriers aligned with the C# `LocalStorageDevice` shared handle table — any thread's sync covers writes completed by all threads before the call (foreign-written segments are re-opened in place by the syncing thread; handles never cross threads). A debug-only `dirty_segs` guard bitmap (device-global, first 128 segments) verifies the contract; zero cost in release
- Handle lifetime: handles live in thread-local storage until truncated away, `reset`, or thread exit — call `Device::reset` on long-lived workers before dropping a device (fd upper bound: threads × live segments)
- Directory durability: creating a segment fsyncs the parent directory (Unix), so "write new segment + sync" covers both data and directory entry; not supported on Windows
- Direct I/O probing on Linux is settled at first segment open; later failures propagate — no runtime fallback
- Alignment: offset / len must be multiples of sector_size; segment_size must be a power of two and ≥ sector_size

## Test Coverage

tests/device/ covers: alignment and invalid parameters, cross-segment round_trip, boundary and overflow defense, sync durability and cross-thread sync contracts (ghost-segment defense, remove/truncate immunity), directory fsync lifecycle, segment recovery and mismatch detection, fixed-width Base32 segment-name ordering (lexicographic = numeric), truncate and reset, capacity eviction (segmented and single-file bounded), 32/64-way concurrency and cold-open races, multi-OS-thread shared runtime, null device.


---

<a name="zh"></a>

# wdev : 块存储设备层

- [项目介绍](#项目介绍)
- [模块组成](#模块组成)
- [核心 API](#核心-api)
- [设计要点](#设计要点)
- [测试覆盖](#测试覆盖)

## 项目介绍

wdev 提供段文件设备（`SegmentedDevice`）、Direct I/O 与设备抽象（`Device`），是 whlog / waof 等上层日志引擎的持久化底座。

设计基于 compio 异步运行时：Linux 走 io_uring，Windows 走 IOCP，macOS 走 kqueue。设备以段（segment）为单位组织文件，段大小为 2 的幂且不小于 sector_size；单段上限 `MAX_SEGMENT_SIZE = 2^62`。段文件命名为 `<base>.<段号>`，段号为 13 字符定长小写 Base32（转写规范偏离 C# 十进制），文件名字典序与段号数值序严格一致。

## 模块组成

- `device`：设备抽象 trait `Device`，定义扇区尺寸、段尺寸、Direct I/O 与读写接口
- `segmented_device`：段文件设备实现，句柄按（设备编号， 段号）Thread-Local 持有 `Rc<File>`（零跨核争用；papaya 仅 Windows 延迟删除队列参与编译）
- `chunk`：扇区与分段切片计算，跨段 / 单段 I/O 边界切片迭代与对齐校验
- `null`：`NullDevice` 空设备，I/O 即时假成功、零物理 I/O
- `sys`：跨平台零依赖硬件探测（CPU 核数、系统内存），探测失败回退 `FALLBACK_CPU_CORES = 4`、`FALLBACK_SYSTEM_MEMORY_BYTES = 4 GiB`
- `error`：错误类型（对齐 / 越界 / 段不存在等 I/O 参数校验错误族）

## 核心 API

- `Device`：设备抽象；`write_aligned` / `read_aligned` 要求 offset / len 为 sector_size 整数倍且缓冲区地址对齐，`read_range` 便捷读取无对齐要求（缓冲 I/O 模式按逻辑范围精确直读）
- `SegmentedDevice`：段文件设备；`dir_sync_count()` 计数器可观测父目录 fsync 次数
- `NullDevice`：测试与基准用空设备
- `detect_cpu_cores()` / `detect_system_memory()`：硬件探测
- 另导出 `wbase::BufferPool` 供调用方直接构建对齐缓冲

## 设计要点

- 持久化契约：`sync` / `sync_data` 为全局屏障，语义对齐 C# `LocalStorageDevice` 的进程级共享句柄表——任一线程调用即覆盖调用发起前全部线程已完成的写入（他线程写入段由调用线程就地补开刷新，句柄永不过线程）；debug 构建以设备级 `dirty_segs` 守护位图（前 128 段）校验契约，release 零成本
- 句柄生命周期：句柄 Thread-Local 持有，随该线程截断驱逐、`reset` 显式清理或线程退出回收；长生命周期工作线程弃用设备前应调用 `Device::reset`（fd 占用上界：线程数 × 在册段数）
- 目录项持久化：新建段时同步 fsync 父目录（Unix），"新段写入 + sync 即持久"同时覆盖段数据与目录项；Windows 平台不支持
- Direct I/O 定型：Linux 首个段打开时探测定型，定型后失败直接上抛，运行中不回退
- 对齐要求：offset / len 须为 sector_size 整数倍，segment_size 须为 2 的幂且 ≥ sector_size

## 测试覆盖

tests/device/ 覆盖：对齐与非法参数、跨段读写 round_trip、边界与溢出防御、sync 持久化与跨线程 sync 契约（幽灵段防御、删段/截断免责）、目录 fsync 生命周期、段恢复与不匹配检测、定长 Base32 段名字典序保序、截断与 reset、容量逐出（分段与单文件有界）、32/64 并发与冷打开竞态、多 OS 线程共享运行时、null 设备。

