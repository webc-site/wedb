# wcompact : Log Compaction

## Introduction

wcompact provides the HybridLog online compactor: it scans the read-only region for dead records, copies live records to the log tail with atomic index CAS replacement, then advances the begin address to reclaim cold segments. It decouples from concrete engines via the `CompactStore` / `CompactSession` trait abstractions.

## Module Layout

- `compactor`: compaction core and strategies (`LogCompactor`, `CompactionType`, `CompactionStats`)
- `host`: host abstraction traits (`CompactSession`, `CompactStore`)
- `error`: error types

## Core API

- `LogCompactor`: new / with_cas_retries / compact / compact_with_filter / compact_lazy(max_seek_bytes)
- `CompactionType`: Lookup (per-record index probe of the live value) / Scan (single-pass candidate hash table for batch dedup, O(unique keys) space, value bodies read back only when alive)
- `CompactionStats`: scanned_records / live_copied / superseded / dead_dropped / retained / bytes_freed / new_begin_address (exact conservation: scanned_records = live_copied + superseded + dead_dropped + retained)
- `CompactSession`: enter_epoch / append_record / read_ttl_expiry; `TTL_VALUE_LEN = 8`
- `CompactStore`: hlog / index / read-only and begin addresses / shift_begin_address / read-cache detection and skipping / revivification pool injection and other host capability ports

## Design Notes

- Flow: scan (delegates to the whlog ScanIterator with per-page prefetch) → liveness decision → conditional_copy_to_tail append + atomic index CAS → shift_begin_address segment reclamation
- CAS retries cap at 8; on exhaustion with a still-live record the truncation point falls back for the next round — records are never dropped incorrectly
- TTL-aware three rules: expired TTL records die; unexpired TTL records whose host key is absent from the index (orphan TTL) die; data records with attached expired TTL die
- Additional pass: replay collection-metadata watermarks during compaction; is_stale_subkey drops deleted collections / stale-version history sub-keys; at finalize, key_id_versions dead entries whose death address falls entirely within the compacted range and remain dead in memory are reclaimed
- compact_lazy bounds the per-round scan seek budget via max_seek_bytes over [begin, min(read_only, begin + max_seek_bytes)) with a fixed Lookup strategy (returns empty stats immediately when there is nothing to compact or the budget is 0), fitting incremental background compaction

## Test Coverage

Inline tests in compactor.rs cover only: CompactionStats defaults / is_empty and MetaDeathScope death registration. The Lookup / Scan strategies, three TTL rules, CAS contention fallback, statistics accounting, and lazy budget constraints are covered by the wkv/tests/compact integration suites (basic / lazy_compaction / concurrency_and_collision / spanbyte_compaction / more_log_compaction).
