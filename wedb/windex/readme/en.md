# windex : Lock-Free Hash Index

## Introduction

windex provides a Garnet Tsavorite-style 64-byte cacheline-aligned lock-free concurrent hash index: hash buckets, compact entries, an overflow bucket pool and a fixed-capacity index table.

Addressing invariant: capacity is always a power of two, `mask == buckets.len() - 1`, bound for life at construction.

## Module Layout

- `bucket`: `HashBucket`, strictly one 64B cacheline per bucket (`[AtomicU64; 8]`); slots 0..6 hold data entries, slot 7 stores a 1-based overflow-bucket index in the low 48 bits, a 15-bit shared-lock reader count next, and the exclusive-lock bit on top
- `entry`: `HashBucketEntry(u64)` compact entry: 48-bit address (up to 256TB; bit47 doubles as the read_cache flag, overlapping the top address bit — when set only the low 47 bits are a valid address), 15-bit hash tag, tentative bit63 for two-phase insertion
- `overflow_pool`: overflow bucket pool with two-level chunked allocation (1024 buckets per chunk), atomic 1-based numbering; allocate first recycles buckets returned by free via a lock-free free list (Treiber stack with a 32-bit ABA tag), falling back to counter allocation only when empty
- `table`: `HashIndex` fixed-capacity table + `HashBuckets` (DirectVirtualMemory demand-zero mapping, ≥2MB auto alignment with MADV_HUGEPAGE) + single-key bucket latch and L1 prefetch

## Core API

- `HashBucket` / `BucketSharedGuard` / `BucketExclusiveGuard`; `ENTRIES_PER_BUCKET = 8`, `DATA_ENTRIES = 7`, `OVERFLOW_INDEX = 7`
- `HashBucketEntry`: address / tag / tentative bit packing with CAS updates
- `OverflowPool`: allocate / free / get / has_free / allocated_count (`CHUNK_SIZE = 1024`, `MAX_CHUNKS = 4096`)
- `HashIndex`: insert / find_tag / lookup; deduplicating insert find_tag_or_insert / find_or_create_tag(\_with_min_addr) (dead-slot reclamation); batch read two-level prefetch kernel prefetch_batch_probes (12-key window, PrefetchProbe hash + first address from one source); update_address / delete (RCU CAS / atomic zeroing); single-key exclusive latch try_lock_key_exclusive
- `HashBuckets` / `HashEntryInfo` (pinned CAS / try_elide) / `CandidateAddresses` / `KeyLatch` / `prefetch_read_l1`; latch promotion/demotion `BucketSharedGuard::try_promote` / `BucketExclusiveGuard::downgrade`

## Design Notes

- Spin read-write latch embedded in slot 7: 15-bit shared count (up to 32767 readers) + 1 exclusive bit; a single attempt spins up to 128 rounds (exclusive: up to 1024 reader-drain rounds), using spin_loop for the first 32 rounds then yield_now, returning false on failure — a drain timeout rolls back the exclusive bit. Key-level read-modify-write windows take the latch with a single attempt via `HashIndex::try_lock_key_exclusive` (mirroring C# `TryEphemeralXLock`, which just returns a status); acquisition failure is turned into a retry by the caller — the index layer has no multi-key batch orchestration, no reverse rollback and no self-imposed timeout
- Single-CAS lock-free insertion: an empty slot goes 0 → complete entry atomically with no partially-visible window; same-tag dedup is delegated to callers choosing the newest candidate (a merged equivalent of C#'s two-phase tentative FindOrCreateTag protocol, with the tentative bit retained for transient visibility decisions in batch probing)
- RCU lock-free CAS updates + atomic zeroing deletes; `MAX_CHAIN_STEPS = 1 << 20` single-pointer counting for chain-cycle defense
- `KeyLatch` is the key-addressed alias of `BucketExclusiveGuard` (same bucket latch, same Drop release, zero duplicated guard code); the index layer exports no multi-key batch guard at all — multi-key two-phase locking belongs exclusively to `wtxn::TxnKeyEntry`

## Test Coverage

tests/index/ covers: cacheline alignment and bit-packing boundaries, tag-mask defense, shared/exclusive latch lifecycle and reader draining, lock upgrade/downgrade, single-key latch contention exclusion, full-contention stress, cross-chunk concurrent allocation, 1024+ deep overflow chains and cycle detection, concurrent RCU updates, find_tag probing, mixed insert/lookup workloads.
