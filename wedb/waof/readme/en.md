# waof : AOF Write-Ahead Log

## Introduction

waof provides a WAL engine (`WalLog`) built on a ring memory write buffer plus segmented block devices; persistence goes through the `Device` abstraction from wdev, with wram supplying aligned memory and buffer pools. `AofLog` / `AofRecord` etc. are aliases of the `Wal*` types.

The record header is only 8B (`entry_len: u32` + `crc32: u32`, little-endian); empty payloads use the `EMPTY_PAYLOAD_CRC = 0xFFFF_FFFF` sentinel so committed empty record headers are never all-zero, distinguishing them from torn crash tails.

## Module Layout

- `config`: `WalConfig` (buffer_size default 16MiB, inflight_slots default 256); sector alignment is not configurable — `Device::sector_size()` is the single source of truth
- `header` / `record`: `RecordHeader`, `WalRecord` (address / next_address / header / payload, Derefs to [u8])
- `log`: `WalLog` / `WalLogInner` core engine holding begin / tail / flushed_until / committed_until atomic positions
- `disk_window`: `DiskWindow` shared sliding pre-read window for recovery and scan (internal)
- `iterator`: `WalScanIterator` sliding-window chunked reads, transparent across memory and disk segments
- `ring_buffer`: `RingBuffer` in-memory ring write buffer; power-of-two capacities take the bitmask fast path

## Core API

- `WalLog<D>`: open (open-and-recover), enqueue (lock-free CAS address reservation with in-flight slot registration), commit (batch flush up to safe_tail), enqueue_raw (write a pre-formatted frame verbatim — replica-faithful persistence, cf. C# UnsafeTryEnqueueRaw), enqueue_and_wait_for_commit (enqueue then await durability at the record end address), wait_for_commit, scan / scan_all / scan_committed, total_size, recover, truncate, reset
- `WalScanIterator<D>`: transparent memory/disk scanning
- `WalConfig`, `WalRecord`, `RecordHeader` (`RECORD_HEADER_LEN = 8`), `RingBuffer`
- Aliases: `AofConfig` / `AofLog<D>` / `AofLogInner<D>` / `AofRecord` / `AofScanIterator<D>`

## Design Notes

- Flush semantics: enqueue is lock-free; commit holds the commit lock and flushes [flushed, safe_tail) where safe_tail = min(tail, all in-flight slots); wait_for_commit is a fast fence — lock-free return when already committed, otherwise double-checked try_lock cooperative flush or event listening to avoid thundering herds
- Overwrite semantics: the ring overwrites unflushed data. When the in-memory copy of an already-flushed record is clobbered, scans fall back to the authoritative disk data and continue; when an unflushed record is evicted by ring overwrite, it is counted in `overwritten_skips` and the scan terminates early with `Ok(None)` — that range has no authoritative disk copy and cannot be recovered; a non-zero count means a "completed" scan actually ended early due to overwrite
- Recovery: requires a quiet log; EOF / torn header / checksum failure / all-zero fill all conservatively truncate to the last complete record, other I/O errors propagate; no checkpoint dependency — the CRC record chain self-synchronizes to locate the tail; after truncate, if the segment start lands mid-payload of a torn cross-segment record, recovery first re-synchronizes byte-by-byte via frame_sync and advances begin_address
- truncate advances begin and physically deletes segments, mutually excluded with commit against ghost segments; reset only rewinds in-memory positions and has a stale-record revival window (documented)
- RingBuffer role: pre-commit memory residency — enqueue makes zero syscalls, commit flushes sequentially in batches
- Commit boundary note: there is no per-commit metadata record; recovery treats the last complete record as committed. Records enqueued but not yet committed may be revived after a crash if a concurrent commit's flush already covered them — callers requiring an exact commit durability boundary must handle this (see `WalLog` docs)

## Test Coverage

tests/ covers: RecordHeader encoding and corruption robustness, RingBuffer large-address reads/writes; end-to-end smoke (write-scan-commit-truncate-restart-append); full buffer, payload limits, raw-frame fidelity and replica address-parity replay, bounded concurrent growth, fast-commit concurrent waiting, short-write protection; sub-range scans, uncommitted memory, behind-begin jumps, physical truncation stop, slow-reader eviction with disk fallback, memory overwrite disk fallback, large records, disk prefetch boundaries; multi-stage recovery, torn tail, empty-record durability, all-zero fill non-revival, mid-log corruption conservative stop, cross-segment frame_sync, massive record counts; truncate with file deletion, exact segment boundaries, periodic truncation, reset reuse.
