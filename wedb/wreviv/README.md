[English](#en) | [中文](#zh)

---

<a name="en"></a>

# wreviv : Free-Slot Revivification Pool

- [Introduction](#introduction)
- [Module Layout](#module-layout)
- [Core API](#core-api)
- [Design Notes](#design-notes)
- [Test Coverage](#test-coverage)

## Introduction

wreviv provides revivification of free memory record slots: slots freed by deletes / updates in the HybridLog are cached in size-classed bins so later writes reuse them in place instead of advancing the log tail monotonically. It mirrors the Garnet Tsavorite `Revivification/` directory.

Deliberate differences from the C# implementation (one implementation per feature):

- No segments: a single flat slot array with a round-robin write cursor; Best-Fit quality comes from full-bin scans via `best_fit_scan_limit` (defaults to `BEST_FIT_SCAN_ALL`, clamped if configured)
- No CheckEmptyWorker background thread: an atomic `active_count` drives the empty-bin fast path directly
- No oversize bins: inline size caps at 65535B; relocating larger records belongs to upper layers
- No RevivificationManager facade: parameters such as `min_address` are passed in by upper layers
- Single-byte fill precision: pairs with wrecord FillerWords / FillerRem slack fill, matching at byte granularity instead of 8B alignment

## Module Layout

- `record`: `FreeRecord`, 64-bit slot metadata (48-bit address + 16-bit size) atomically packed
- `bin`: `FreeRecordBin`, fixed-capacity bin with First-Fit / Best-Fit atomic take
- `pool`: `FreeRecordPool`, multi-size tiered bin pool with cross-bin lookup and statistics
- `error`: error types

## Core API

- `FreeRecord` (`repr(transparent)` AtomicU64): pack / unpack / set / peek / try_purge_below; `SetStatus` (InsertedEmpty / ReplacedExpired / Occupied; `Occupied` uniformly denotes a failed insert: slot occupied by a valid record, bin full, or invalid parameters rejected defensively)
- `FreeRecordBin`: put / take_best_fit / clear; `USE_FIRST_FIT = 0`, `BEST_FIT_SCAN_ALL = usize::MAX`
- `FreeRecordPool`: put / take / take_allocation / clear / stats / reset_stats
- `RevivAllocation{address, actual_size, required_size, filler_bytes}`, `RevivStats` (put / take / hit / drop counts and hit_rate)
- `DEFAULT_BIN_SIZES = [16, 32, 64, ..., 65535]` (13 classes), `DEFAULT_BIN_CAPACITY = 256`

## Design Notes

- Concurrency model: pool structures are fully atomic and lock-free for concurrent access; in-place record rewrite follows the compio per-core single-thread premise of "single writer + hlog page write lock + epoch protection"; the C# TrySeal CAS protocol is intentionally not ported
- Pack preconditions: size ≤ 65535 with debug asserts against silent truncation; addresses beyond 48 bits truncate by definition
- Address semantics match the aligned-address mask of `windex::HashBucketEntry`

## Test Coverage

Covers: pool lifecycle with slack fill, slot-pack state machine; First / Best-Fit allocation sequences, min_address boundaries, same-size tie determinism, max_bins limits; single-slot contention and multi-thread stress, active_count invariants; capacity overflow, bulk purge_below, expired-slot replacement, CAS ABA defense, scan-limit clamping, parameter boundary defense, non-monotonic min_address safety.


---

<a name="zh"></a>

# wreviv : 空闲槽位复活回收池

- [项目介绍](#项目介绍)
- [模块组成](#模块组成)
- [核心 API](#核心-api)
- [设计要点](#设计要点)
- [测试覆盖](#测试覆盖)

## 项目介绍

wreviv 提供内存记录槽位的复活（revivification）回收：把 HybridLog 中因删除 / 更新腾出的空闲槽位按尺寸分桶缓存，后续写入优先原位复用，避免日志尾单调推进。对标 Garnet Tsavorite `Revivification/` 目录。

与 C# 实现的刻意差异（同一功能只保留一种实现）：

- 省略 segment 机制：单一扁平槽位数组 + 轮询写游标，Best-Fit 质量由 `best_fit_scan_limit` 全桶扫描保证（默认 `BEST_FIT_SCAN_ALL`，可钳位）
- 省略 CheckEmptyWorker 后台线程：以原子 `active_count` 直接驱动空桶快速路径
- 省略 oversize 分桶：16 位内联尺寸上限 65535B，超限记录的腾挪属上层职责
- 省略 RevivificationManager 门面：`min_address` 等参数由上层传入
- 单字节填充精度：配合 wrecord 的 FillerWords / FillerRem 松弛填充，按字节粒度匹配而非 8B 对齐

## 模块组成

- `record`：`FreeRecord`，64 位槽位元信息（48 位地址 + 16 位尺寸）原子打包
- `bin`：`FreeRecordBin` 定长分桶，First-Fit / Best-Fit 原子取出
- `pool`：`FreeRecordPool` 多尺寸分级分桶池与跨桶检索、统计
- `error`：错误类型

## 核心 API

- `FreeRecord`（`repr(transparent)` AtomicU64）：pack / unpack / set / peek / try_purge_below；`SetStatus`（InsertedEmpty / ReplacedExpired / Occupied；Occupied 统一表示写入失败：槽位被有效记录占用、分桶已满或参数非法被防御性拒绝）
- `FreeRecordBin`：put / take_best_fit / clear；`USE_FIRST_FIT = 0`、`BEST_FIT_SCAN_ALL = usize::MAX`
- `FreeRecordPool`：put / take / take_allocation / clear / stats / reset_stats
- `RevivAllocation{address, actual_size, required_size, filler_bytes}`、`RevivStats`（put / take / hit / drop 四计数与 hit_rate）
- `DEFAULT_BIN_SIZES = [16, 32, 64, ..., 65535]`（13 级）、`DEFAULT_BIN_CAPACITY = 256`

## 设计要点

- 并发模型：池结构全原子无锁，可多线程并发存取；记录本体的原位复活改写遵循 compio 每核单线程的"单写者 + hlog 页写锁 + epoch 保护"前提，C# 的 TrySeal CAS 协议未移植
- 打包前置校验：size ≤ 65535，debug 断言禁止静默截断；地址超 48 位按定义性截断
- 地址语义与 `windex::HashBucketEntry` 的对齐地址掩码一致

## 测试覆盖

覆盖：池生命周期与松弛填充、槽位打包状态机；First / Best-Fit 分配序列、min_address 边界、同尺寸平局确定性、max_bins 限制；单槽位竞争与多线程压测、active_count 不变量；容量溢出、批量 purge_below、过期槽位替换、CAS ABA 防御、扫描上限钳位、参数边界防御、非单调 min_address 安全。

