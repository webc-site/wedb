[English](#en) | [中文](#zh)

---

<a name="en"></a>

# wbase : Common Foundation Primitives and Constants

- [Core Positioning](#core-positioning)
- [Modules & Features (On-Demand, No Full Feature)](#modules--features-on-demand-no-full-feature)
- [Core API & Primitives](#core-api--primitives)
- [Design Principles & Parity](#design-principles--parity)

- [Core Positioning](#core-positioning)
- [Modules & Features (On-Demand, No Full Feature)](#modules-features-on-demand-no-full-feature)
- [Core API & Primitives](#core-api-primitives)
  - [1. `addr` (48-bit Log Address System)](#1-addr-48-bit-log-address-system)
  - [2. `align` (Memory & Sector Alignment)](#2-align-memory-sector-alignment)
  - [3. `backoff` (3-Stage Adaptive Backoff)](#3-backoff-3-stage-adaptive-backoff)
  - [4. `thread` (High-Throughput Thread ID)](#4-thread-high-throughput-thread-id)
- [Design Principles & Parity](#design-principles-parity)

## Core Positioning

`wbase` is the Layer-0 foundational primitives library for the WeDB storage engine. It extracts cross-crate constants and fundamental state machines, eliminating circular and reverse dependencies to ensure a strictly acyclic DAG across the workspace.

All features are provided as **fine-grained optional Cargo features**, with **no `full` feature**, allowing callers to opt into exactly what they need with zero overhead.

## Modules & Features (On-Demand, No Full Feature)

| Feature | Path | Responsibility | Dependencies |
| :--- | :--- | :--- | :--- |
| `addr` | `wbase::addr` | 48-bit address masks, ReadCache bit, `LogAddress` strongly typed wrapper | None (pure bitwise ops) |
| `align` | `wbase::align` | 64B cacheline, 512B/4096B sector alignment checks and safe calculations | None (pure bitwise ops) |
| `backoff` | `wbase::backoff` | 3-stage adaptive backoff state machine (spin → yield → sleep) | None (supports reactor yield) |
| `thread` | `wbase::thread` | High-throughput TLS monotonic thread identifier `current_thread_id()` | None (TLS register reads) |

## Core API & Primitives

### 1. `addr` (48-bit Log Address System)
Strict parity with C# Tsavorite / Garnet `LogAddress`:
- Constants: `ADDRESS_BITS = 48`, `ADDRESS_MASK = 0x0000_FFFF_FFFF_FFFF` (up to 256TB address space).
- ReadCache Bit: `READ_CACHE_BIT = 1 << 47`, `ABSOLUTE_ADDRESS_MASK = ADDRESS_MASK & !READ_CACHE_BIT`.
- Predicates & conversions: `is_valid`, `is_read_cache`, `to_absolute`, `with_read_cache`.
- Wrapper: `LogAddress` transparent struct with compact layout and display formatting.

### 2. `align` (Memory & Sector Alignment)
- Cacheline: `CACHELINE_BYTES = 64`, `is_cacheline_aligned`, `align_to_cacheline`.
- Sector calculations: `MIN_SECTOR_SIZE = 512`, `DEFAULT_SECTOR_SIZE = 4096`.
- Safe math: `checked_align_up` (returns `None` on overflow), `align_up` (saturating), `align_down`, `is_aligned`.

### 3. `backoff` (3-Stage Adaptive Backoff)
Designed for multi-core high-concurrency and compio thread-per-core reactor models:
- Stage 1 (< 32 spins): `spin_loop()` CPU instruction pause.
- Stage 2 (32..1024 rounds): `yield_now()` OS time-slice yield.
- Stage 3 (>= 1024 rounds): `sleep(50µs)` synchronous sleep or async non-blocking sleep via `BackoffStage::Sleep`.

### 4. `thread` (High-Throughput Thread ID)
- `current_thread_id() -> u64`: Global atomic increment on first visit (starting from 1), cached in TLS for register-level access (< 1ns, 0 locks, 0 atomic ops).
- Single source of truth for thread IDs, eliminating bus contention and ABA hazards.

## Design Principles & Parity

1. **Zero-Cost Abstraction**: All physical masks fold at compile-time with zero runtime penalty.
2. **compio Friendly**: Backoff state machines cleanly expose stages, preventing synchronous sleep calls from freezing reactor worker threads.
3. **Orthogonal Decoupling**: Upstream crates (`wrecord`, `windex`, `wreviv`, `wram`, `wepoch`) import only their required features.


---

<a name="zh"></a>

# wbase : 存储底座通用基础原语与常量库

- [核心定位](#核心定位)
- [模块划分与特性（按需启用，无 full）](#模块划分与特性按需启用无-full)
- [核心 API 与原语](#核心-api-与原语)
- [设计原则与对标](#设计原则与对标)

- [核心定位](#核心定位)
- [模块划分与特性（按需启用，无 full）](#模块划分与特性按需启用无-full)
- [核心 API 与原语](#核心-api-与原语)
  - [1. `addr`（48 位日志地址体系）](#1-addr48-位日志地址体系)
  - [2. `align`（内存与扇区对齐）](#2-align内存与扇区对齐)
  - [3. `backoff`（三阶自适应退避）](#3-backoff三阶自适应退避)
  - [4. `thread`（高吞吐线程 ID）](#4-thread高吞吐线程-id)
- [设计原则与对标](#设计原则与对标)

## 核心定位

`wbase` 是 WeDB 存储引擎的最底层（L0 级）公共基础原语库。抽离跨 crate 共享的核心常量与基础状态机，消除循环依赖与反向依赖，保障整个 workspace 拓扑单向无环。

所有功能模块均作为**细粒度可选特性（feature）**提供，**杜绝全量 `full` 特性**，使用方按需引入，零多余依赖与开销。

## 模块划分与特性（按需启用，无 full）

| 特性名 | 模块路径 | 职责说明 | 外部依赖 |
| :--- | :--- | :--- | :--- |
| `addr` | `wbase::addr` | 48 位逻辑/物理地址掩码、ReadCache 标记、`LogAddress` 强类型封装 | 无（纯标准库位运算） |
| `align` | `wbase::align` | 64B 缓存行、512B/4096B 扇区对齐校验与防溢出计算 | 无（纯标准库位运算） |
| `backoff` | `wbase::backoff` | 三阶自适应退避状态机（自旋 → yield 让核 → 微秒休眠） | 无（支持异步 reactor 让渡） |
| `thread` | `wbase::thread` | 高吞吐 TLS 全局唯一单调递增线程标识 `current_thread_id()` | 无（TLS 寄存器级访问） |

## 核心 API 与原语

### 1. `addr`（48 位日志地址体系）
严格对标 C# Tsavorite / Garnet `LogAddress`：
- 常量：`ADDRESS_BITS = 48`，`ADDRESS_MASK = 0x0000_FFFF_FFFF_FFFF`（最大 256TB 寻址空间）。
- 读缓存标记：`READ_CACHE_BIT = 1 << 47`，`ABSOLUTE_ADDRESS_MASK = ADDRESS_MASK & !READ_CACHE_BIT`。
- 判据与转换：`is_valid`、`is_read_cache`、`to_absolute`、`with_read_cache`。
- 封装：`LogAddress` 结构体，提供紧凑无锁与格式化支持。

### 2. `align`（内存与扇区对齐）
- 缓存行：`CACHELINE_BYTES = 64`，`is_cacheline_aligned`，`align_to_cacheline`。
- 扇区计算：`MIN_SECTOR_SIZE = 512`，`DEFAULT_SECTOR_SIZE = 4096`。
- 安全计算：`checked_align_up`（极值溢出返回 `None`）、`align_up`（溢出安全饱和）、`align_down`、`is_aligned`。

### 3. `backoff`（三阶自适应退避）
适配多核高并发与 compio thread-per-core 反应器模型：
- 第一阶段（< 32 轮）：`spin_loop()` 极短指令等待。
- 第二阶段（32..1024 轮）：`yield_now()` 操作系统级让渡时间片。
- 第三阶段（>= 1024 轮）：`sleep(50µs)` 同步微睡，或供异步调用方感知 `BackoffStage::Sleep` 后执行非阻塞 `compio::time::sleep().await`。

### 4. `thread`（高吞吐线程 ID）
- `current_thread_id() -> u64`：首访全局原子计数器单调自增（1 起步），后续由线程本地存储（TLS）纯寄存器读取（< 1ns，0 锁，0 原子操作）。
- 统一全局线程标识，天然免疫 ABA，消除模块各自维护 ID 造成的总线争用。

## 设计原则与对标

1. **零成本抽象与单一真源**：所有物理掩码在编译期直接折叠，不产生运行时损耗。
2. **compio 友好**：退避状态机显式暴露阶段，彻底消除异步上下文误调同步阻塞 sleep 冻结 reactor 的隐患。
3. **架构正交解耦**：上层模块如 `wrecord`、`windex`、`wreviv`、`wram`、`wepoch` 按需引入特定特性，彻底消除平级相互穿透。

