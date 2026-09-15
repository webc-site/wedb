# wbftree : Bf-Tree Range Index Service

## Introduction

wbftree provides the Rust service layer for the Bf-Tree ordered storage engine and the RangeIndex manager: single-tree lifecycle (`BfTreeService`), multi-tree registry (`RangeIndexManager`), the 35-byte fixed stub in the main log, and the chunked migration stream protocol. The underlying engine is the `bf-tree` crate.

## Module Layout

- `service`: `BfTreeService` single-tree lifecycle with point read / write / delete / scan / CPR snapshot / recovery; `WriteBarrierGuard`
- `manager`: `RangeIndexManager` multi-tree registry, lazy recovery, pre-stage, flush / checkpoint / truncate / replication enumeration, migration temp-path derivation; `RangeIndexLocks` key-hashed striped locks
- `chunk`: `RangeIndexChunkedSerializer` / `RangeIndexChunkedDeserializer` / `RangeIndexMigrationReader` chunked migration stream state machines
- `stub`: `RangeIndexStub`, the 35-byte fixed stub in the main store log, with in-place modification helpers
- `types`: `BfTreeConfig`, `TreeTuning`, `StorageBackendType` (Disk / Memory), read/insert/delete result codes, `ScanRecord`
- `error`: error types (Io / InvalidArgument / InvalidConfig / IndexExists / Snapshot / Recovery / Disposed / Timeout (30s drain limit) / Corrupted)

## Core API

- `BfTreeService`: create / open / point read-write-delete / scan / cpr_snapshot / recovery; `write_barrier` returns a counting RAII guard
- `RangeIndexManager`: get_or_open_tree (lazy recovery), create / dispose / flush / checkpoint / truncate, replication enumeration and migration temp-path derivation, pre_stage_and_register_pending, recover_all_trees_from_checkpoint / recover_all_trees_from_dir
- `RangeIndexStub`: tree_handle 8B + cache_size 8B + min/max_record_size / max_key_len / leaf_page_size 4B×4 + backend / flags / serialization_phase 1B each, 35B total (`RANGE_INDEX_STUB_SIZE = 35`)
- Migration stream format: `[4B keyLen][key][8B fileBytes][file][8B checksum][4B stubLen][stub]`; `MIN_CHUNK_SIZE = 47`, `DEFAULT_MIGRATION_CHUNK_SIZE = 256KiB`
- Constants: `NUM_LOCK_STRIPES = 128`, `INDEX_SIZE_BYTES = 35`

## Design Notes

- thread-per-core contract: fully synchronous API, no runtime dependency; point read / write paths take zero striped locks (the engine's leaf latches ensure concurrency) with `Arc<BfTreeService>` shared across threads; only lifecycle changes (create / lazy recovery / unregister / delete) take `RangeIndexLocks` striped write locks; online references live in a papaya lock-free map — reader-pinned snapshots never block writers; tree deletion is deferred until `Arc` refcount reaches zero
- Snapshot write barrier: a counting barrier plus an in-flight writer count, both AtomicUsize paired via SeqCst store-buffering (Dekker); the counting barrier nests, writers block until the outermost guard drops, briefly backing off along a spin → yield → micro-sleep ladder — the tree stays write-quiescent and snapshots tear-free; draining beyond 30s raises `Error::Timeout`; holding the guard forbids await / same-thread I/O events
- Key semantics: keys are binary-safe zero-copy `&[u8]` throughout; the 128-bit key id derives from gxhash128 with a dedicated seed domain (digest domain isolated from user data), and the file-name prefix is its 26-char Base32 encoding
- Lazy recovery: get_or_open_tree first copies the flush snapshot (bare name preferred, else the highest address) onto the data file and restores via CPR snapshot when the magic (`BF-TREE-V0-BEGIN`) matches, otherwise rebuilds / reopens from the stub; on_flush copies data files of cold trees and sets the flushed bit
- Leaf page sizing: `max_record_size` ≤2KB takes 4096; otherwise 2.5× capped at 32768, rounded up to a power of two

## Test Coverage

tests/ covers: record capacity boundaries, disk reopen and CPR snapshot recovery, multi-reader multi-writer concurrency, nested barriers and tear-free snapshots; lock stripe count / alignment / contention, stub encoding and slice helpers, leaf_page_size derivation, manager lifecycle / checkpoint / truncate / replication enumeration / duplicate-create defense / strict flush filename parsing; chunked serialization roundtrip, cross-chunk boundaries and empty chunks, error-state termination, checksum corruption, streaming reads; interop lifecycle and disposal, zero-allocation point read/write/delete contract, scan counts / end keys / field selection / ordering, snapshot recovery roundtrip and corrupted-snapshot errors.
