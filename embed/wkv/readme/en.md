# wkv : Top-Level Hybrid Storage Engine

## Introduction

wkv is the top-level single-node hybrid storage engine: it integrates the windex hash index + whlog HybridLog + wepoch epoch protection + wbftree/RangeIndex, with built-in TTL, GC, read cache, compaction scheduling, and CPR checkpoint integration.

## Module Layout

- `store`: the `WedbStore` top-level engine
- `session`: `StoreSession` / `BatchStoreSession` sessions and physical key encoding
- `config`: adaptive configuration (`StoreConfig`)
- `ttl`: key-level TTL probing and clearing
- `gc`: background expiry scanning + compaction scheduling (`GcManager`)
- `read_cache`: DRAM-only read cache log
- `compact`: adaptation layer over wcompact
- `checkpoint`: RangeIndex / shared BfTree CPR snapshots and recovery
- `range_index`: secondary index operations and RESP frame encoding
- `error`: error types

## Core API

- `WedbStore`: open / open_shared / from_components / from_components_with_bftree / new_session / start_gc / gc_handle / shift_read_only_address / flush_all / flush_and_evict_all / truncate / expired_key_deletion_scan / raise_key_id_floor
- `StoreConfig`: four constructors auto / auto_with_budget / minimal / new (Default = minimal); builder chain with_max_sessions / with_revivification / with_read_cache(\_pages)  / with_bftree_path / with_range_index_dir / with_gc; `DEFAULT_INDEX_SIZE = 65536`, `DEFAULT_MEMORY_PERCENT = 25`, budget floor 256MB, ceiling 32GB
- `StoreSession`: upsert / read / delete, three-state try_upsert_sync (success returns the record address — a new address for tail appends, the original address for in-place updates / in-chain revival / revivification-pool reuse; page-flip returns the page id to evict; u64::MAX means TTL clearing must fall back to the async path), try_modify_in_place, enter_batch, physical key encoding (session_string_key / meta_key / hash_sub_key / chunk_key)
- `BatchStoreSession`: batch processing with a single epoch protection; `*_unprotected` zero-atomic-overhead fast paths
- `GcManager` / `GcHandle` / `GcStatsSnapshot`; `ReadCache` (append / with_record / skip_read_cache)
- `TtlOpt` (NX / XX / GT / LT), `TtlProbe` (Pass / Due / Deferred)
- `encode_ri_set` / `encode_ri_del` / `encode_ri_create` (RESP Bulk String frames for WAL pre-write and replication streams)
- Also re-exports public types from wbftree / wcompact / wcpr / wrecord

## Design Notes

- Fixed-capacity contract: deliberately bound to a fixed-capacity flat HashIndex opened at startup, no online resize; foreground find_or_create_tag + try_cas slot handles always reference the same live table, eliminating "CAS landing in a retired table during resize" lost writes
- GC: each cycle fixes a `[begin, scan_end)` snapshot (tail pinned to prevent continuous appends from starving old TTLs); per round limited to max_scan_records and max_batch_deletes; candidates re-verify latest TTL then uniformly clear collection metadata / sub-keys / TTL records; compaction triggers when `read_only - begin > max_segments × segment_size`; the compactor is directly integrated from wcompact and `GcManager` holds only a Weak; compaction advances begin via shift_begin_address, after which whlog calls device truncate_until_address to physically delete reclaimed disk segment files (automatic on segmented devices; nothing to reclaim in single-file unbounded mode); an explicit truncate only forces an additional pass; disabled by default
- Read cache: bit 47 marks READ_CACHE_BIT with the low 48 bits as the absolute address; at checkpoint time index entries are walked via skip_read_cache to write back true main-log addresses
- TTL probing tri-state: Pass (no TTL / not due), Due (expired, physically cleared after batch-read closure), Deferred (disk candidates downgraded to async handling); purge_expired never triggers a second clear when the record is not found — no recursion
- Compact collection thresholds: hash ≤512 entries with values ≤64B, set / zset ≤128 entries (set member values ≤64B, zset members ≤64B), total compact encoding per record capped at 4096B (`MAX_COMPACT_TOTAL_BYTES`), beyond which records convert to flattened / BfTree encodings

## Test Coverage

tests/ covers: basic reads/writes, in-place overwrite, RCU version chains, tombstone delete and revival, multi-session concurrency, page-turn eviction cold reads, RMW and shared-BfTree open modes; store suites crud / flush_evict / defense / reviv / collision_chain; compact suites (basic / lazy / concurrency-collision / spanbyte / multi-round); checkpoint suites (recovery / edge / manager / index_checkpoint / fault_defense); gc and config defaults.
