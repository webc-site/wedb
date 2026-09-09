# wram : Zero-lock sector-aligned memory for async storage

## Overview

wram delivers sector-aligned memory infrastructure, mirroring the industrial buffer pool design of Microsoft Garnet / Tsavorite:

- `BufferPool`: sector-aligned buffer pool. Three-tier cache ladder (thread-local stack → lock-free cross-thread inbox → global striped depot), 28 size classes (512B..16MB at 512B sectors), isolated small/large byte budgets, bypass allocation for oversize requests.
- `AlignedBuf`: the sector-aligned buffer itself. RAII drop returns it to the pool automatically; implements compio_buf `IoBuf` / `IoBufMut` / `SetLen`.
- `DirectVirtualMemory`: OS direct virtual memory allocator. Demand-zero mmap / VirtualAlloc mappings with Linux transparent huge page hints.
- `NativeMemoryTracker`: striped lock-free native memory counter for memory telemetry.
- Alignment math: const fn alignment primitives and `SectorRange` logical/physical translation.

Target workloads: async storage engines, file I/O paths built on compio, large page caches and index residency.

## Usage

### Pool rent and return

```rust
use wram::BufferPool;

// Default sector 4096, small budget 32MiB / large budget 128MiB
let pool = BufferPool::new(wram::DEFAULT_SECTOR_SIZE)?;

// Rent: bytes rounded up to sector granularity, content zeroed
let mut buf = pool.get(4096)?;
buf.set_len(100)?;

// Drop returns the buffer: same thread pushes to the local stack with
// zero atomics; foreign threads push to the owner inbox with one CAS
drop(buf);

// Read-destination scenario: rent without clear-on-return; the buffer is
// marked dirty on return and lazily cleared on next rent
let mut dst = pool.get_with_policy(8192, false)?;

// Copy-initialize from a slice
let payload = pool.get_from_slice(b"payload")?;
assert_eq!(&payload[..], b"payload");
```

### Capacity guarantee and cross-thread return

```rust
use wram::BufferPool;

let pool = BufferPool::new(4096)?;
let mut buf = pool.get(4096)?;

// In-place reuse when capacity suffices; otherwise the old buffer is
// returned and a larger one is rented automatically
pool.ensure_size(&mut buf, 65536)?;
assert!(buf.capacity() >= 65536);

// Dropping on a worker thread routes the buffer back to the owner inbox
let handle = std::thread::spawn(move || drop(buf));
handle.join().expect("worker thread succeeded");

// Owner rents again: bulk-claims the inbox and hits the same allocation
let again = pool.get(65536)?;
```

### Standalone aligned buffer and sector math

```rust
use wram::{AlignedBuf, DEFAULT_SECTOR_SIZE, SectorRange};

// Standalone (non-pooled): fully zeroed, pointer sector-aligned
let mut buf = AlignedBuf::zeroed(4096, DEFAULT_SECTOR_SIZE)?;
assert!(buf.is_ptr_aligned());

// Translate a logical read into a physical sector range:
// read 100 bytes at offset 4196
let range = SectorRange::calculate(4196, 100, DEFAULT_SECTOR_SIZE)?;
assert_eq!(range.aligned_offset, 4096); // aligned physical start
assert_eq!(range.aligned_len, 4096); // aligned physical length
assert_eq!(range.internal_offset, 100); // logical offset inside the sector

// Extract the logical slice from the aligned buffer
let user_data = &buf[range.sub_range(100)];
```

### Direct virtual memory

```rust
use wram::{DirectVirtualMemory, NativeMemoryTracker};

// 8MB demand-zero mapping aligned to 4096; Linux >= 2MB gets THP hints
let mut block = DirectVirtualMemory::allocate(8 << 20, 4096)?;
assert!(block.as_aligned_slice(8 << 20).iter().all(|&b| b == 0));

// The global tracker reflects reserved bytes
let reserved = NativeMemoryTracker::bytes();

// RAII drop unmaps and decrements the tracker; explicit free is idempotent
wram::DirectVirtualMemory::free(&mut block);
```

### Budget isolation

```rust
use wram::BufferPool;

// Explicit small/large budgets, strictly isolated: large-buffer churn
// can never starve the small-buffer quota
let pool = BufferPool::with_budgets(512, 2 << 20, 6 << 20)?;

assert_eq!(pool.small_budget_bytes(), 2 << 20);
assert_eq!(pool.large_budget_bytes(), 6 << 20);

// Close: reclaims the calling thread's cache and the global depot; quota returns to zero
pool.free();
assert_eq!(pool.reserved_bytes(), 0);
```

## Features

- **Three-tier cache ladder, zero-lock hot path**: L1 thread-local stack rents and returns with no locks and no atomics; L2 cross-thread MPSC lock-free inbox, claimed by the owner with a single atomic swap (no ABA); L3 global 8-way striped depot carries overflow, large-class sharing and thread-exit reclamation.
- **RAII origin-return routing**: same-thread returns land on the local stack at zero cost; foreign returns publish through intrusive list nodes with one CAS onto the owner inbox; once the owner exits, the inbox is sealed and late returns reroute to the global depot — permits never strand.
- **Clear-on-return policy**: cleared return by default; `get_with_policy(bytes, false)` skips clearing, marks the buffer dirty and defers clearing to the next renter, removing the memory-bandwidth bottleneck on read paths.
- **Dual-layer byte budgets_*: small (≤256KB classes) and large (>256KB classes) quotas are strongly isolated with `AtomicI64` CAS accounting; exhaustion degrades to non-pooled direct allocation without blocking callers.
- **28-class capacity ladder**: 2 exact + 4 linear + 22 geometric classes (two per doubling, worst-case waste 1.5x), driven by a compile-time constant table; oversize requests bypass with exact sizing.
- **Direct virtual memory**: demand-zero mappings, Linux `MADV_HUGEPAGE` hints, global byte tracking over 64 cache-line-padded stripes.
- **compio ecosystem**: implements `IoBuf` / `IoBufMut` / `SetLen`, ready as the buffer type for compio async file I/O.

## Design

### Pool rent and return, end to end

```mermaid
graph TD
  G[get_with_policy] --> Z{zero-byte request}
  Z -->|yes| E[empty buffer]
  Z -->|no| R{over MAX_POOLED_SECTORS (16MB at 512B sectors)}
  R -->|yes| B[bypass exact alloc, never pooled]
  R -->|no| C{small class}
  C -->|yes| L1[L1 local stack pop]
  L1 -->|hit| U[reuse, lazily clear if dirty]
  L1 -->|miss| L2[L2 single-swap inbox claim]
  L2 -->|non-empty| U
  L2 -->|empty| L3[L3 depot work stealing]
  L3 -->|hit| U
  L3 -->|empty| N[reserve budget, system alloc]
  C -->|no| L3
  U --> BUF[AlignedBuf]
  N --> BUF
  B --> BUF
  BUF -->|drop| P{clear policy}
  P -->|clear| FZ[zero the buffer]
  P -->|skip| DTY[mark dirty]
  FZ --> RT{return routing}
  DTY --> RT
  RT -->|pool closed| REL[release permit and memory]
  RT -->|large class| DEP[global striped depot]
  RT -->|small class, owner thread| TLS[TLS local stack]
  RT -->|small class, foreign thread| INB[CAS push to owner inbox]
  INB -->|sealed| DEP
  DEP -->|depot full| REL
```

### Key mechanisms

**Rent path**: the request is rounded up to sectors and `class_of_sectors` selects a class from the compile-time ladder. Small classes probe L1 → L2 → L3 in order; nodes claimed beyond the local cap spill back to L3; on total miss, a permit is reserved from the matching budget tier before the system allocation. Large classes skip thread-local tiers entirely and share through the global depot, preventing multi-thread residency from inflating the working set.

**Return path**: `AlignedBuf` drop triggers the RAII return. The buffer is cleared or marked dirty per policy, then routed by class. Budget permits travel with the buffer across local stack, inbox, depot and in-flight states; a permit is released only when the buffer dies permanently, keeping the ledger exact.

**Lifecycle**: thread-local entries hold weak references to the pool. On thread exit, TLS RAII seals the inbox and disperses leftover buffers to the depot stripes captured at entry creation, releasing permits inline. Pool close atomically flips the closed flag under each stripe lock, so late pushes fail and release inline — the quota is guaranteed to return to zero after close.

**Deliberate divergences from the C# original**: thread-local retention is capped per class in slots (C# uses a per-thread byte cap with fair refill); `free()` reclaims only the calling thread's cache and the depot eagerly, while other threads' caches are reclaimed when those threads exit (C# uses finalizers for best-effort eager reclaim); zero-byte requests return an empty buffer (C# issues a one-sector buffer).

## Tech Stack

| Component         | Role                                                       |
| ----------------- | ---------------------------------------------------------- |
| Rust 2024 edition | let-chains, modern iterators, const fn evaluation          |
| compio-buf        | `IoBuf` / `IoBufMut` / `SetLen` zero-copy I/O traits       |
| libc              | mmap / munmap / madvise / sysconf and Windows VirtualAlloc |
| parking_lot       | global depot stripe locks                                  |
| thiserror         | error definition and transparent forwarding                |
| log               | structured logging                                         |

Test stack: `cargo-nextest`, `aok`, `ctor`, `log_init`.

## Directory Layout

```text
wram/
├── src/
│   ├── lib.rs            # public export aggregation
│   ├── align.rs          # alignment primitives and SectorRange
│   ├── aligned_buf.rs    # AlignedBuf buffer type
│   ├── direct_vm.rs      # DirectVirtualMemory / DirectVmBlock
│   ├── error.rs          # Error / Result
│   ├── tracker.rs        # NativeMemoryTracker
│   └── pool/
│       ├── mod.rs        # BufferPool and the size-class ladder
│       ├── tls.rs        # thread-local L1 stack and lifecycle
│       ├── inbox.rs      # cross-thread MPSC lock-free inbox
│       ├── depot.rs      # global striped depot
│       └── budget.rs     # dual-layer byte budgets
└── tests/
    ├── main.rs           # single test entry with log init
    └── suite/            # ports of the C# test suites
        ├── align.rs
        ├── aligned_buf.rs
        ├── direct_vm.rs
        ├── pool_ladder.rs
        ├── pool_get_return.rs
        ├── pool_cross_thread.rs
        ├── pool_budget.rs
        └── pool_stress.rs
```

## API

### Constants

| Constant                     | Value         | Meaning                                         |
| ---------------------------- | ------------- | ----------------------------------------------- |
| `DEFAULT_SECTOR_SIZE`        | 4096          | default sector size                             |
| `MIN_SECTOR_SIZE`            | 512           | minimum legal sector size                       |
| `NUM_CLASSES`                | 28            | total size classes                              |
| `CLASS_CAPACITIES_SECTORS`   | `[usize; 28]` | compile-time per-class capacity table           |
| `MAX_POOLED_SECTORS`         | 32768         | maximum poolable sectors (16MB at 512B sectors) |
| `LARGE_TIER_MIN_BYTES`       | 262144        | small/large budget tier threshold               |
| `DEFAULT_SMALL_BUDGET_BYTES` | 32MiB         | default small budget                            |
| `DEFAULT_LARGE_BUDGET_BYTES` | 128MiB        | default large budget                            |
| `MAX_LOCAL_PER_CLASS`        | 64            | per-class thread-local cache slots              |
| `DEPOT_STRIPE_CAP`           | 8             | per-stripe depot capacity                       |

### Alignment functions

- `is_aligned(val, align) -> bool`: alignment check via power-of-two bit math or modulo fallback.
- `align_down(val, align) -> u64`: round down.
- `align_up(val, align) -> u64`: round up; saturates to the largest aligned multiple on overflow.
- `checked_align_up(val, align) -> Option<u64>`: round up with overflow detection.

### `SectorRange`

Translation result from a logical offset/length to a physical sector range.

```rust
pub struct SectorRange {
  pub aligned_offset: u64,    // aligned physical start offset
  pub aligned_len: usize,     // aligned physical length in bytes
  pub internal_offset: usize, // logical offset inside the first sector
}
```

- `calculate(offset, len, sector_size) -> Result<Self>`: translate; invalid sector size or overflow fails.
- `sector_count(&self, sector_size) -> usize`: number of sectors spanned.
- `sub_range(&self, len) -> Range<usize>`: logical data range inside the aligned buffer.

### `AlignedBuf`

Sector-aligned buffer. `Deref` / `DerefMut` / `AsRef<[u8]>` / `Borrow<[u8]>` expose the byte slice; implements `Clone` (deep copy, clones never enter the pool), `PartialEq`, `Debug`; implements compio `IoBuf` / `IoBufMut` / `SetLen`; `Send` / `Sync`.

- Construction: `new(cap, align)` (zeroed, len 0), `zeroed(cap, align)` (zeroed, len = cap), `from_slice(data, align)` (copy-initialized), `with_sector_size(cap)`, `zeroed_with_sector_size(cap)`; alignment must be a power of two ≥ 512.
- Length: `len` / `is_empty` / `set_len(usize) -> Result<()>` / `clear` / `unsafe set_len_unchecked`.
- Views: `as_slice` / `as_mut_slice` (logical length), `as_allocated_slice` / `as_allocated_slice_mut` (full capacity).
- Policy: `clear_on_return` / `set_clear_on_return(bool)` query and switch; `required_len` reports the effective request length.
- Pointers: `as_buf_ptr` / `as_mut_buf_ptr` / `is_ptr_aligned` / `is_aligned_to(align)`.
- Metadata: `capacity` / `align`.

### `BufferPool`

Sector-aligned buffer pool.

- Construction: `new(sector_size) -> Result<Arc<Self>>` (default budgets), `with_budgets(sector_size, small, large) -> Result<Arc<Self>>`; sector must be a power of two ≥ 512, budgets must be non-negative, and the largest class capacity must fit i64 budget accounting.
- Rent:
  - `get(required_bytes) -> Result<AlignedBuf>`: default clear-on-return.
  - `get_with_policy(required_bytes, clear_on_return) -> Result<AlignedBuf>`: explicit policy.
  - `get_from_slice(slice) -> Result<AlignedBuf>`: copy-initialized rent.
  - `ensure_size(&mut AlignedBuf, size) -> Result<()>`: in-place reuse when capacity suffices (syncing the required length); re-rent otherwise.
- Observation: `reserved_bytes` / `small_reserved_bytes` / `large_reserved_bytes` / `small_budget_bytes` / `large_budget_bytes` / `cached_len(cls)` / `sector_size` / `is_closed` / `stats() -> PoolStats`.
- `stats() -> PoolStats`: snapshot with `reserved_bytes` / `small_reserved_bytes` / `large_reserved_bytes`, budget-exhaustion direct-alloc counters `direct_alloc_count` / `direct_alloc_bytes` (steady growth means the budget is too small), and oversize/closed bypass counters `bypass_alloc_count` / `bypass_alloc_bytes`.
- Close: `free()` is idempotent; it drains the calling thread's cache and the depot, after which rents become non-pooled direct allocations and in-flight returns release immediately.

### `DirectVirtualMemory` and `DirectVmBlock`

- `DirectVirtualMemory::allocate(size, alignment) -> Result<DirectVmBlock>`: demand-zero mapping; alignment must be a power of two; Linux requests ≥ 2MB promote alignment and hint transparent huge pages.
- `DirectVirtualMemory::free(&mut DirectVmBlock)`: explicit release, idempotent.
- `unsafe DirectVirtualMemory::clear(ptr, len)`: zero a raw pointer range.
- `DirectVmBlock` exposes its fields: `base_ptr` (mapping base), `aligned_ptr` (aligned usable address), `reserved_length` (reserved bytes); `empty()` / `is_empty()` / `as_aligned_slice` / `as_aligned_mut_slice` / `slice(range)` / `slice_mut(range)`.
- `system_page_size() -> usize`: physical page size, cached process-wide.

### `NativeMemoryTracker`

- `bytes() -> usize`: total native direct virtual memory reserved (lock-free sum over 64 stripes).

### Errors

`Result<T> = std::result::Result<T, Error>`; `Error` is a thiserror enum: `InvalidAlignment`, `InvalidSize`, `InvalidBudget`, `SetLenExceeded`, `AllocFailed`, `DirectVmAllocFailed`, `Overflow`, and transparent `Layout` forwarding.

### Utility functions

- `class_of_sectors(sectors) -> Option<usize>`: sector count to class mapping; `None` beyond the poolable cap.
- `class_capacity_sectors(cls) -> usize`: class capacity in sectors (const, saturating on out-of-range).
- `class_capacity_bytes(cls, sector_size) -> usize`: class capacity in bytes (const).
- `current_thread_id() -> u64`: process-wide increasing thread ID.
