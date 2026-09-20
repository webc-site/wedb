# wreviv : Free-Slot Revivification Pool

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
