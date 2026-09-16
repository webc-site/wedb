# whyperlog : HyperLogLog

- [Introduction](#introduction)
- [Relationship with whlog](#relationship-with-whlog)
- [Core API](#core-api)
- [Design Notes](#design-notes)
- [Test Coverage](#test-coverage)

## Introduction

whyperlog provides a Garnet HyperLogLog sketch: a probabilistic cardinality estimator backing the PFADD / PFCOUNT / PFMERGE command family, transcribed 1:1 from `libs/server/Resp/HyperLogLog/HyperLogLog.cs`.

The sketch operates on caller-owned byte blobs with two encodings:

- Dense: a 16-byte header (HYLL magic + encoding + cached cardinality) followed by 16384 packed 6-bit registers (12304 bytes total)
- Sparse: the same 16-byte header plus a 2-byte RLE length and an RLE opcode stream (zero-range `1xxx xxxx` / non-zero `0vvv vvvv`), densifying beyond the 4KB cap

Cardinality estimation uses the new algorithms from ["New cardinality estimation algorithms for HyperLogLog sketches"](https://arxiv.org/abs/1702.01284) (register histogram + tau/sigma correction), with MurmurHash2x64A element hashing.

## Relationship with whlog

whyperlog and whlog are orthogonal domains that merely look alike by name:

- whyperlog = HyperLogLog **data structure**: fixed-layout sketches stored as object payloads; no IO, no addresses, no concurrency. C# counterpart: `HyperLogLog.cs`.
- whlog = HybridLog **log allocator**: a 64-bit address space over the device with page buffering, flush and region shifting. C# counterpart: Tsavorite's `TsavoriteLog`.

They never merge: one is a Redis object encoding, the other is storage engine infrastructure.

## Core API

- `HyperLogLog`: new (pbit = 14) / with_pbit, init / init_sparse / init_dense, update (in-place or needs-space), copy_update, update_grow / merge_grow / can_grow_in_place (growth planning), count (cached cardinality with invalidation), merge / try_merge, sparse_to_dense, sparse_to_sparse_copy / sparse_to_dense_copy / copy_update_merge, is_valid_hyll / is_valid_hll_length / is_valid_sparse_stream, compare_sparse_to_dense, dump_raw_bytes / dump_regs (DEBUG dump as String)
- `HllDtype` (Sparse / Dense), `murmur_hash_2_x64_a`
- Layout constants: `SPARSE_SIZE_MAX_CAP` (4096), `SPARSE_MEMORY_SECTOR_SIZE` (128)

## Design Notes

- Rust operates on `&mut [u8]` slices instead of C# raw pointers; slice/payload equivalence is the caller's duty (the sketch itself holds no storage)
- The 6-bit registers are packed LSB-first across byte boundaries; the last register reads past-the-end as 0 instead of C#'s harmless out-of-bounds read
- Cached cardinality lives in the header as an i64 (negative = invalidated); every register mutation invalidates it
- Sparse zero-range split writes at most 3 opcodes with a one-byte-shifted suffix move, mirroring C# `UpdateSparseReg`
- DEBUG export methods (C# Console.WriteLine) return `String` instead

## Test Coverage

tests cover: layout constants vs C#, sparse/dense init layouts, PFADD semantics with repeat insert no-op, count accuracy windows (sparse 500 / dense 10k), sparse-to-dense upgrade with register-level equality, zero-range splits at head/mid/tail boundaries, reg_idx/clz bounds, sparse and sparse-into-dense merges, cardinality cache invalidation, growth planning incl. empty-source underflow defense, copy paths, MurmurHash invariants, malformed blob detection, DEBUG dump helpers, custom pbit.
