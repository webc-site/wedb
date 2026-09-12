# wbase : Common Foundation Primitives and Constants

- [Core Positioning](#core-positioning)
- [Modules & Features (On-Demand, No Full Feature)](#modules--features-on-demand-no-full-feature)
- [Core API & Primitives](#core-api--primitives)
- [Design Principles & Parity](#design-principles--parity)

## Core Positioning

`wbase` is the Layer-0 foundational primitives library for the WeDB storage engine. It extracts cross-crate constants and fundamental state machines, eliminating circular and reverse dependencies to ensure a strictly acyclic DAG across the workspace.

All features are provided as **fine-grained optional Cargo features**, with **no `full` feature**, allowing callers to opt into exactly what they need with zero overhead.

## Modules & Features (On-Demand, No Full Feature)

| Feature   | Path             | Responsibility                                                           | Dependencies                  |
| :-------- | :--------------- | :----------------------------------------------------------------------- | :---------------------------- |
| `addr`    | `wbase::addr`    | 48-bit address masks, ReadCache bit, `LogAddress` strongly typed wrapper | None (pure bitwise ops)       |
| `align`   | `wbase::align`   | 64B cacheline, 512B/4096B sector alignment checks and safe calculations  | None (pure bitwise ops)       |
| `backoff` | `wbase::backoff` | 3-stage adaptive backoff state machine (spin → yield → sleep)            | None (supports reactor yield) |
| `thread`  | `wbase::thread`  | High-throughput TLS monotonic thread identifier `current_thread_id()`    | None (TLS register reads)     |

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
