# wdev : Block Storage Device Layer

## Introduction

wdev provides the segmented-file device (`SegmentedDevice`), Direct I/O, and the device abstraction (`Device`) — the persistence foundation for upper log engines such as whlog / waof.

It is built on the compio async runtime: io_uring on Linux, IOCP on Windows, kqueue on macOS. Files are organized in segments; a segment size must be a power of two and at least sector_size, with `MAX_SEGMENT_SIZE = 2^62`. Segment files are named `<base>.<segment-id>`, where the id is a fixed-width 13-char lowercase Base32 (a transpile-spec deviation from the C# decimal ids), so lexicographic file-name order always equals numeric segment order.

## Module Layout

- `device`: device trait `Device` defining sector size, segment size, Direct I/O, and read/write interfaces
- `segmented_device`: segmented-file device; handles held Thread-Local per (device id, segment id) as `Rc<File>` (zero cross-core contention; papaya is compiled in only for the Windows deferred-deletion queue)
- `chunk`: sector/segment slicing, cross-segment and single-segment I/O boundary iteration with alignment checks
- `null`: `NullDevice`, instant fake-success I/O with zero physical I/O
- `sys`: dependency-free cross-platform hardware probing (CPU cores, system memory), falling back to `FALLBACK_CPU_CORES = 4` and `FALLBACK_SYSTEM_MEMORY_BYTES = 4 GiB`
- `error`: error types (alignment / out-of-bounds / missing-segment validation errors)

## Core API

- `Device`: abstraction; `write_aligned` / `read_aligned` require offset / len to be multiples of sector_size with aligned buffers, while `read_range` has no alignment requirement (exact logical-range reads in buffered-I/O mode)
- `SegmentedDevice`: segmented-file device; `dir_sync_count()` observes parent-directory fsyncs
- `NullDevice`: empty device for tests and benchmarks
- `detect_cpu_cores()` / `detect_system_memory()`: hardware probing
- Re-exports `wbase::BufferPool` for building aligned buffers

## Design Notes

- Persistence contract: `sync` / `sync_data` are global barriers aligned with the C# `LocalStorageDevice` shared handle table — any thread's sync covers writes completed by all threads before the call (foreign-written segments are re-opened in place by the syncing thread; handles never cross threads). A debug-only `dirty_segs` guard bitmap (device-global, first 128 segments) verifies the contract; zero cost in release
- Handle lifetime: handles live in thread-local storage until truncated away, `reset`, or thread exit — call `Device::reset` on long-lived workers before dropping a device (fd upper bound: threads × live segments)
- Directory durability: creating a segment fsyncs the parent directory (Unix), so "write new segment + sync" covers both data and directory entry; not supported on Windows
- Direct I/O probing on Linux is settled at first segment open; later failures propagate — no runtime fallback
- Alignment: offset / len must be multiples of sector_size; segment_size must be a power of two and ≥ sector_size

## Test Coverage

tests/device/ covers: alignment and invalid parameters, cross-segment round_trip, boundary and overflow defense, sync durability and cross-thread sync contracts (ghost-segment defense, remove/truncate immunity), directory fsync lifecycle, segment recovery and mismatch detection, fixed-width Base32 segment-name ordering (lexicographic = numeric), truncate and reset, capacity eviction (segmented and single-file bounded), 32/64-way concurrency and cold-open races, multi-OS-thread shared runtime, null device.
