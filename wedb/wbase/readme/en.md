# wbase : Common Foundation Primitives and Constants

- [Core Positioning](#core-positioning)
- [Modules & Features (On-Demand, No Full Feature)](#modules--features-on-demand-no-full-feature)
- [Core API & Primitives](#core-api--primitives)
- [Design Principles & Parity](#design-principles--parity)

## Core Positioning

`wbase` is the Layer-0 foundational primitives library for the WeDB storage engine. It extracts cross-crate constants and fundamental state machines, eliminating circular and reverse dependencies to ensure a strictly acyclic DAG across the workspace.

All features are provided as **fine-grained optional Cargo features**, with **no `full` feature**, allowing callers to opt into exactly what they need with zero overhead.

## Modules & Features (On-Demand, No Full Feature)

Modules map one-to-one onto features at `wbase::<module>`; `ascii` / `heap` / `keyfmt` / `ns_prefix` compile unconditionally. The 32 modules below mirror `src/lib.rs` `pub mod`; responsibilities are taken from each module's `//!` header.

| Feature | Path | Responsibility | Dependencies |
| :--- | :--- | :--- | :--- |
| (unconditional) | `wbase::ascii` | ASCII normalization and case-folding primitives (parity with C# `ASCIIEncoding.GetString`) | None (pure std) |
| `addr` | `wbase::addr` | 48-bit address masks, ReadCache bit, `LogAddress` strongly typed wrapper | None (pure bitwise ops) |
| `align` | `wbase::align` | 64B cacheline, 512B/4096B sector alignment checks and safe calculations | None (pure bitwise ops) |
| `backoff` | `wbase::backoff` | 3-stage adaptive backoff state machine (spin → yield → sleep) | None (supports reactor yield) |
| `base32` | `wbase::base32` | Zero-alloc, order-preserving lowercase Base32 (RFC 4648 Base32hex) codec for snapshot and flush file names | None (pure std) |
| `buf` | `wbase::buf` | Stack-first / heap-fallback byte buffer (`StackHeapBuf` const-generic) | None (pure std) |
| `cfg` | `wbase::cfg` | Cross-crate base config: shared criteria for consumers that must not depend on each other (compaction tier, logical db bounds) | None (pure std) |
| `convert` | `wbase::convert` | Data primitive conversions (parity with `libs/common/ConvertUtils.cs`, time via coarsetime) | cascades `time` |
| `crc` | `wbase::crc` | High-throughput CRC32 checksum (hardware-accelerated); single point for WAL, checkpoint sealing, segment integrity | `crc32fast` |
| `crc64` | `wbase::crc64` | Table-driven CRC64, bit-exact with Garnet `Crc64.cs` | None (pure std table) |
| `endpoint` | `wbase::endpoint` | Single source for socket endpoint classification: Unix domain socket paths vs typed loopback | None (pure std) |
| `error` | `wbase::error` | Common error type definitions | `thiserror` |
| `future` | `wbase::future` | Async coroutine-cooperation primitives and Future helpers (`block_on` pure park driver) | None (pure std) |
| `glob` | `wbase::glob` | Allocation-free Glob matching (non-recursive greedy FSM, O(N) typical, Redis-aligned) | None (pure std) |
| `group_commit` | `wbase::group_commit` | Group Commit pipeline skeleton: negotiation / follower registration / leader cascading loop | `parking_lot`, `thiserror`, `crossfire` |
| `hash` | `wbase::hash` | Bit-exact hashing library (faithful port of Garnet `HashUtils.cs`) | None (pure std) |
| (unconditional) | `wbase::heap` | Collection heap-memory accounting constants: the single named basis for `heap_memory_size` arithmetic | None (pure std) |
| `hash_slot` | `wbase::hash_slot` | Cluster slot kernel: `Slot = Mixer(namespace, active_db)` | `whasher` |
| `hex` | `wbase::hex` | Hex encoding/decoding micro utilities, single definition point | `fastrand` |
| (unconditional) | `wbase::keyfmt` | Log-side key preview: unified truncation for key printing in error arms | None (pure std) |
| `map` | `wbase::map` | Concurrent map and set (`ConcurrentMap` / `ConcurrentSet`; the `set` feature enables the same module) | `papaya`, `gxhash`, `fastrand` |
| `num` | `wbase::num` | Strict number grammar parsing and conversion (parity with `NumUtils.cs`) | None (pure std) |
| (unconditional) | `wbase::ns_prefix` | Session-scope isolation prefix codec (multi-tenant ns + db prefix, wedb-specific) | None (pure std) |
| `pool` | `wbase::pool` | Sector-aligned buffer pools: Origin-Return three-tier cache, `AlignedBuf`, network `LimitedFixedBufferPool` | `parking_lot`, `compio-buf`, `gxhash`, `crossfire` |
| `primed` | `wbase::primed` | TLS append-read buffer priming contract and memoized zeroed buffers | `compio-buf` |
| `simd` | `wbase::simd` | SIMD vectorized slice comparison (key lookup, version-chain tracing, dedup) | `fearless_simd` |
| `striped` | `wbase::striped` | Key-hash striped read/write locks (128B `CachePadded` slots) | `parking_lot`, cascades `align` |
| `thread` | `wbase::thread` | High-throughput TLS monotonic thread identifier `current_thread_id()` | None (TLS register reads) |
| `time` | `wbase::time` | Timestamp utilities (coarsetime / VDSO, `now_ms` / `now_ticks`) | `coarsetime` |
| `store_type` | `wbase::store_type` | Storage-plane classification enums (parity with `StoreType.cs:StoreType`) | `num_enum`, `strum` |
| `supervise` | `wbase::supervise` | Single point for background-task panic supervision (one `catch_unwind` wrapper workspace-wide) | `log`, `parking_lot` |
| `varint` | `wbase::varint` | Order-preserving varint codec (OPPV, for ordered composite keys) | None (pure std) |

## Core API & Primitives

### 1. `addr` (48-bit Log Address System)

Strict parity with C# Tsavorite / Garnet `LogAddress`:

- Constants: `ADDRESS_BITS = 48`, `ADDRESS_MASK = 0x0000_FFFF_FFFF_FFFF` (up to 256TB address space).
- ReadCache Bit: `READ_CACHE_BIT = 1 << 47`, `ABSOLUTE_ADDRESS_MASK = ADDRESS_MASK & !READ_CACHE_BIT`.
- Predicates & conversions: `is_valid`, `is_read_cache`, `to_absolute`, `with_read_cache`.
- Wrapper: `LogAddress` transparent struct with compact layout and display formatting.

### 2. `align` (Memory & Sector Alignment)

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
3. **Orthogonal Decoupling**: Upstream crates (`wrecord`, `windex`, `wreviv`, `wepoch`) import only their required features.
