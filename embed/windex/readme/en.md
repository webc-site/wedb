# windex : Lock-Free Hash Index

## Introduction

windex provides a Garnet Tsavorite-style 64-byte cacheline-aligned lock-free concurrent hash index: hash buckets, compact entries, an overflow bucket pool and a fixed-capacity index table.

Addressing invariant: capacity is always a power of two, `mask == buckets.len() - 1`, bound for life at construction.

## Module Layout

- `bucket`: `HashBucket`, strictly one 64B cacheline per bucket (`[AtomicU64; 8]`); slots 0..6 hold data entries, slot 7 stores a 1-based overflow-bucket index in the low 48 bits, a 15-bit shared-lock reader count next, and the exclusive-lock bit on top
- `entry`: `HashBucketEntry(u64)` compact entry: 48-bit address (up to 256TB; bit47 doubles as the read_cache flag, overlapping the top address bit — when set only the low 47 bits are a valid address), 15-bit hash tag, tentative bit63 for two-phase insertion
- `overflow_pool`: overflow bucket pool with two-level chunked allocation (1024 buckets per chunk), atomic 1-based numbering; allocate first recycles buckets returned by free via a lock-free free list (Treiber stack with a 32-bit ABA tag), falling back to counter allocation only when empty
- `table`: `HashIndex` fixed-capacity table + `HashBuckets` (DirectVirtualMemory demand-zero mapping, ≥2MB auto alignment with MADV_HUGEPAGE) + multi-key locking and L1 prefetch

## Core API

- `HashBucket` / `BucketSharedGuard` / `BucketExclusiveGuard`; `ENTRIES_PER_BUCKET = 8`, `DATA_ENTRIES = 7`, `OVERFLOW_INDEX = 7`
- `HashBucketEntry`: address / tag / tentative bit packing with CAS updates
- `OverflowPool`: allocate / free / get / has_free / allocated_count (`CHUNK_SIZE = 1024`, `MAX_CHUNKS = 4096`)
- `HashIndex`: insert / find_tag / lookup; deduplicating insert find_tag_or_insert / find_or_create_tag(\_with_min_addr) (dead-slot reclamation); batch prefetch find_tag_batch / lookup_candidates_batch (12-entry sliding window, 64-key chunks); update_address / delete (RCU CAS / atomic zeroing); multi-key locking acquire_keys_lock_exclusive / acquire_hash_locks
- `HashBuckets` / `HashEntryInfo` (pinned CAS / try_elide) / `CandidateAddresses` / `MultiBucketGuard` / `prefetch_read_l1`; latch promotion/demotion `BucketSharedGuard::try_promote` / `BucketExclusiveGuard::downgrade`

## Design Notes

- Spin read-write latch embedded in slot 7: 15-bit shared count (up to 32767 readers) + 1 exclusive bit; a single attempt spins up to 128 rounds (exclusive: up to 1024 reader-drain rounds), using spin_loop for the first 32 rounds then yield_now, returning false on failure — a drain timeout rolls back the exclusive bit. Multi-key batch locking has its own three-stage backoff: exponential spin with jitter, then yield (1024 retries), then sleep (100µs→1ms cap, 16384 retries) before failing with `LockTimeout`
- Single-CAS lock-free insertion: an empty slot goes 0 → complete entry atomically with no partially-visible window; same-tag dedup is delegated to callers choosing the newest candidate (a merged equivalent of C#'s two-phase tentative FindOrCreateTag protocol, with the tentative bit retained for transient visibility decisions in batch probing)
- RCU lock-free CAS updates + atomic zeroing deletes; `MAX_CHAIN_STEPS = 1 << 20` single-pointer counting for chain-cycle defense
- `MultiBucketGuard` inlines 16 entries on the stack, unlocks in reverse on Drop satisfying 2PL, with ordered acquisition preventing deadlock

## Test Coverage

tests/index/ covers: cacheline alignment and bit-packing boundaries, tag-mask defense, shared/exclusive latch lifecycle and reader draining, lock upgrade/downgrade, multi-bucket deadlock-free ordering, full-contention stress, cross-chunk concurrent allocation, 1024+ deep overflow chains and cycle detection, concurrent RCU updates, find_tag probing, mixed insert/lookup workloads.
