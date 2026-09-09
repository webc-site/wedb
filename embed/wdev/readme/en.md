# wdev : Block Storage Device Layer

## Introduction

wdev provides the segmented-file device (`SegmentedDevice`), Direct I/O, and the device abstraction (`Device`) — the persistence foundation for upper log engines such as whlog / waof.

It is built on the compio async runtime: io_uring on Linux, IOCP on Windows, kqueue on macOS. Files are organized in segments; a segment size must be a power of two and at least sector_size, with `MAX_SEGMENT_SIZE = 2^62`.

## Module Layout

- `device`: device trait `Device` (`StorageDevice` alias) defining sector size, segment size, Direct I/O, and read/write interfaces
- `segmented_device`: segmented-file device; handles cached per (thread id, segment id) in `FileMap` (papaya lock-free map)
- `chunk`: sector/segment slicing, cross-segment and single-segment I/O boundary iteration with alignment checks
- `null`: `NullDevice`, instant fake-success I/O with zero physical I/O
- `sys`: dependency-free cross-platform hardware probing (CPU cores, system memory), falling back to `FALLBACK_CPU_CORES = 4` and `FALLBACK_SYSTEM_MEMORY_BYTES = 4 GiB`
- `error`: error types (alignment / out-of-bounds / missing-segment validation errors)

## Core API

- `Device` / `StorageDevice`: abstraction; `write_aligned` / `read_aligned` require offset / len to be multiples of sector_size with aligned buffers, while `read_range` has no alignment requirement (exact logical-range reads in buffered-I/O mode)
- `SegmentedDevice`: segmented-file device; `dir_syncs` counter observes parent-directory fsyncs
- `NullDevice`: empty device for tests and benchmarks
- `SegmentChunk` / `SegmentChunks`: I/O slice descriptor and iterator
- `detect_cpu_cores()` / `detect_system_memory()`: hardware probing
- Re-exports `wram::BufferPool` for building aligned buffers

## Design Notes

- Persistence contract: `sync` / `sync_data` fsync / fdatasync only the segment handles cached by the calling thread; writes and sync on one device must stay on the same thread (thread-per-core), enforced by the `dirty_segs` guard in debug builds (a per-thread bitmap window covering only the first 128 segments; zero cost in release)
- Directory durability: creating a segment fsyncs the parent directory (Unix), so "write new segment + sync" covers both data and directory entry; not supported on Windows
- Direct I/O probing on Linux is settled at first segment open; later failures propagate — no runtime fallback
- Alignment: offset / len must be multiples of sector_size; segment_size must be a power of two and ≥ sector_size

## Test Coverage

tests/device/ covers: alignment and invalid parameters, cross-segment round_trip, boundary and overflow defense, sync durability and directory fsync lifecycle, segment recovery and mismatch detection, truncate and reset, capacity eviction, 32/64-way concurrency and cold-open races, null device.
