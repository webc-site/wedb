# wkv : Top-Level Hybrid Storage Engine

## Introduction

wkv is the top-level single-node hybrid storage engine: it integrates the windex hash index + whlog HybridLog + wepoch epoch protection + wbftree/RangeIndex, with built-in TTL, GC, read cache, compaction scheduling, and CPR checkpoint integration.

## Module Layout

- `store`: the `WedbStore` top-level engine (address shifting, online resize, keyspace stats, checkpoint shortcuts)
- `session`: `StoreSession` / `BatchStoreSession` sessions and physical key encoding
- `config`: adaptive configuration (`StoreConfig` / `GcConfig`)
- `ttl`: key-level TTL tri-state gate (`TtlGate`) and purging
- `gc`: background expiry scanning + compaction scheduling (`GcManager`)
- `read_cache`: DRAM-only read cache log
- `compact`: adaptation layer over wcompact
- `range_index`: BfTree secondary-index operations, 35B stubs, tiered promote / demote and chunked migration
- `ri`: the `RiTreeOps` extension trait (ri_* surface over `BfTreeService`, crate-internal)
- `etag`: session read/write domain for key-level ETag sidecar records
- `vdb`: virtual-database routing and database metadata (`DbMetaRecord` codec, dead-id ledger, swap-time tree reclamation)
- `error`: error types

## Core API

- `WedbStore`: open / open_shared / from_components / new_session / grow_index / start_gc / stop_gc / update_gc_config / gc_config / gc_stats / gc_running / shift_read_only_address / flush_all / flush_and_evict_all / truncate / expired_key_deletion_scan / raise_key_id_floor / create_checkpoint / recover
- `StoreConfig`: four constructors auto / auto_with_budget / minimal / new (Default = minimal); builder chain with_max_sessions / with_revivification / with_revivifiable_fraction / with_read_cache(\_pages) / with_copy_reads_to_tail / with_range_index_dir / with_tree_cache_budget; exported constants `MIN_INDEX_SIZE = 65536`, `MAX_INDEX_SIZE = 16_777_216`, `DEFAULT_GC_MAX_SEGMENTS = 32`, `DEFAULT_GC_MAX_BATCH_DELETES = 256`, `DEFAULT_DB_GC_RECLAIM_DELAY_SECS = 86_400`, `MIN_ADAPTIVE_BUDGET_BYTES` (auto() derives the budget from 25% of host memory by default, internal bounds 256MB–32GB)
- `StoreSession`: upsert / read / delete, three-state try_upsert_sync (success returns the record address — a new address for tail appends, the original address for in-place updates / in-chain revival / revivification-pool reuse; page-flip returns the page id to evict; u64::MAX means TTL clearing must fall back to the async path), enter_batch, physical key encoding (session_tag_key / session_string_key / session_meta_key / vector_key)
- `BatchStoreSession`: batch processing with a single epoch protection; `*_unprotected` zero-atomic-overhead fast paths
- Built-in GC single facade `GcManager` (the GC handle and stats snapshot are internal types; read stats via `gc_stats()`); `ReadCache` (append / with_record / skip_read_cache)
- `TtlOpt` (NX / XX / GT / LT), `TtlGate` tri-state gate (Pass / Due / Degrade)
- Session-level range-index operations: range_index_create / range_index_set / range_index_set_batch / range_index_get / range_index_get_with / range_index_del / range_index_scan_stream / range_index_range_stream / range_index_exists / range_index_count / range_index_config / range_index_metrics
- Parameter-type single-facade re-exports: `wcompact::CompactionType`, `wcpr::CheckpointType`, `whlog::VERSION_MASK`

## Design Notes

- Index concurrency and online resize: `HashIndex` combines `ArcSwap` with the `SplitIndex` online-resize state machine; during a resize-migration window the foreground claims chunked split migration on demand per hash via CAS, and reads / writes progress lock-free through `active_index`
- GC: each cycle fixes a `[begin, scan_end)` snapshot (tail pinned to prevent continuous appends from starving old TTLs); per round limited to max_scan_records and max_batch_deletes; candidates re-verify latest TTL then uniformly clear collection metadata / sub-keys / TTL records; compaction triggers when `read_only - begin > max_segments × segment_size`; the compactor is directly integrated from wcompact and `GcManager` holds only a Weak; compaction advances begin via shift_begin_address, after which whlog calls device truncate_until_address to physically delete reclaimed disk segment files (automatic on segmented devices; nothing to reclaim in single-file unbounded mode); an explicit truncate only forces an additional pass; disabled by default
- Read cache: bit 47 marks READ_CACHE_BIT with the low 48 bits as the absolute address; at checkpoint time index entries are walked via skip_read_cache to write back true main-log addresses
- TTL gate tri-state: Pass (no TTL / not due), Due (expired, fast path closes as NOTFOUND), Degrade (a disk candidate exists for the TTL record, downgraded to async adjudication); the purge chain never triggers a second clear when the record is not found — no recursion
- Adaptive collection tiering: small collections live in in-memory envelopes (`KeyTag::ObjectEnvelope` record + bitcode payload); on promotion they move to a dedicated BfTree tiered tree with only a 35B stub left in the main log; the promote / demote criteria have a single source in `wcol` — promote via `should_promote` (count ≥ 65,536 or heap ≥ 4MB), demote via `should_demote` (count ≤ 32,768 and heap ≤ 2MB), with a hysteresis dead-band between the high and low watermarks to prevent flapping

## Test Coverage

tests/ covers: basic reads/writes, in-place overwrite, RCU version chains, tombstone delete and revival, multi-session concurrency, page-turn eviction cold reads, RMW and shared-BfTree open modes; store suites crud / flush_evict / defense / reviv / collision_chain; compact suites (basic / lazy / concurrency-collision / spanbyte / multi-round); checkpoint suites (recovery / edge / manager / index_checkpoint / fault_defense); gc and config defaults.
