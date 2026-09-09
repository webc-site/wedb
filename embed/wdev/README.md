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

It is built on the compio async runtime: io_uring on Linux, IOCP on Windows, kqueue on macOS. Files are organized in segments; a segment size must be a power of two and at least sector_size, with `MAX_SEGMENT_SIZE = 2^62`.

## Module Layout

- `device`: device trait `Device` (`StorageDevice` alias) defining sector size, segment size, Direct I/O, and read/write interfaces
- `segmented_device`: segmented-file device; handles cached per (thread id, segment id) in `FileMap` (papaya lock-free map)
- `chunk`: sector/segment slicing, cross-segment and single-segment I/O boundary iteration with alignment checks
- `null`: `NullDevice`, instant fake-success I/O with zero physical I/O
- `sys`: dependency-free cross-platform hardware probing (CPU cores, system memory), falling back to `FALLBACK_CPU_CORES = 4` and `FALLBACK_SYSTEM_MEMORY_BYTES = 4 GiB`
- `error`: error types (alignment / out-of-bounds / missing-segment validation errors)

## Core API

- `Device` / `StorageDevice`: abstraction; `write_aligned` / `read_aligned` require offset / len to be multiples of sector_size with aligned buffers, while `read_range` has no alignment requirement (exact logical-range reads in buffered-I/O mode)
- `SegmentedDevice`: segmented-file device; `dir_syncs` counter observes parent-directory fsyncs
- `NullDevice`: empty device for tests and benchmarks
- `SegmentChunk` / `SegmentChunks`: I/O slice descriptor and iterator
- `detect_cpu_cores()` / `detect_system_memory()`: hardware probing
- Re-exports `wram::BufferPool` for building aligned buffers

## Design Notes

- Persistence contract: `sync` / `sync_data` fsync / fdatasync only the segment handles cached by the calling thread; writes and sync on one device must stay on the same thread (thread-per-core), enforced by the `dirty_segs` guard in debug builds (a per-thread bitmap window covering only the first 128 segments; zero cost in release)
- Directory durability: creating a segment fsyncs the parent directory (Unix), so "write new segment + sync" covers both data and directory entry; not supported on Windows
- Direct I/O probing on Linux is settled at first segment open; later failures propagate — no runtime fallback
- Alignment: offset / len must be multiples of sector_size; segment_size must be a power of two and ≥ sector_size

## Test Coverage

tests/device/ covers: alignment and invalid parameters, cross-segment round_trip, boundary and overflow defense, sync durability and directory fsync lifecycle, segment recovery and mismatch detection, truncate and reset, capacity eviction, 32/64-way concurrency and cold-open races, null device.


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

设计基于 compio 异步运行时：Linux 走 io_uring，Windows 走 IOCP，macOS 走 kqueue。设备以段（segment）为单位组织文件，段大小为 2 的幂且不小于 sector_size；单段上限 `MAX_SEGMENT_SIZE = 2^62`。

## 模块组成

- `device`：设备抽象 trait `Device`（`StorageDevice` 为别名），定义扇区尺寸、段尺寸、Direct I/O 与读写接口
- `segmented_device`：段文件设备实现，句柄按（线程 ID，段号）缓存于 `FileMap`（papaya 无锁字典）
- `chunk`：扇区与分段切片计算，跨段 / 单段 I/O 边界切片迭代与对齐校验
- `null`：`NullDevice` 空设备，I/O 即时假成功、零物理 I/O
- `sys`：跨平台零依赖硬件探测（CPU 核数、系统内存），探测失败回退 `FALLBACK_CPU_CORES = 4`、`FALLBACK_SYSTEM_MEMORY_BYTES = 4 GiB`
- `error`：错误类型（对齐 / 越界 / 段不存在等 I/O 参数校验错误族）

## 核心 API

- `Device` / `StorageDevice`：设备抽象；`write_aligned` / `read_aligned` 要求 offset / len 为 sector_size 整数倍且缓冲区地址对齐，`read_range` 便捷读取无对齐要求（缓冲 I/O 模式按逻辑范围精确直读）
- `SegmentedDevice`：段文件设备；`dir_syncs` 计数器可观测父目录 fsync 次数
- `NullDevice`：测试与基准用空设备
- `SegmentChunk` / `SegmentChunks`：I/O 切片描述与迭代器
- `detect_cpu_cores()` / `detect_system_memory()`：硬件探测
- 另导出 `wram::BufferPool` 供调用方直接构建对齐缓冲

## 设计要点

- 持久化契约：`sync` / `sync_data` 仅对调用线程已缓存的段句柄执行 fsync / fdatasync；同一设备的写入与 sync 必须在同一线程（thread-per-core 亲和），debug 构建以 `dirty_segs` 守护校验（按线程键控的位图窗口，仅跟踪前 128 段，release 零成本）
- 目录项持久化：新建段时同步 fsync 父目录（Unix），"新段写入 + sync 即持久"同时覆盖段数据与目录项；Windows 平台不支持
- Direct I/O 定型：Linux 首个段打开时探测定型，定型后失败直接上抛，运行中不回退
- 对齐要求：offset / len 须为 sector_size 整数倍，segment_size 须为 2 的幂且 ≥ sector_size

## 测试覆盖

tests/device/ 覆盖：对齐与非法参数、跨段读写 round_trip、边界与溢出防御、sync 持久化与目录 fsync 生命周期、段恢复与不匹配检测、截断与 reset、容量逐出、32/64 并发与冷打开竞态、null 设备。

