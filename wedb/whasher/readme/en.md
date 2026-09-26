# whasher : Hardware-accelerated hashing and streaming checksums

- [Overview](#overview)
- [Usage](#usage)
  - [Installation and target requirements](#installation-and-target-requirements)
  - [Key hashing](#key-hashing)
  - [Streaming checksums](#streaming-checksums)
- [Features](#features)
- [Design](#design)
  - [Input-domain routing](#input-domain-routing)
  - [Streaming path](#streaming-path)
  - [Seed domains](#seed-domains)
  - [Compatibility boundaries](#compatibility-boundaries)
- [Technology stack](#technology-stack)
- [Directory structure](#directory-structure)
- [API reference](#api-reference)
  - [Hash functions](#hash-functions)
  - [Scrambling primitives](#scrambling-primitives)
  - [`StreamHasher`](#streamhasher)
- [Validation](#validation)

## Overview

whasher provides hardware-accelerated byte hashing (64-bit and 128-bit, with explicit seeds), chunk-invariant streaming checksums, and bijective `u64` scramblers, all on the AES-vectorized `gxhash` backend.

It is a hashing library only. Hashed and concurrent collections live in `wbase::map`, the Redis wire-compatible MurmurHash2 lives in `wbase::hash`, and the workspace's single source of truth for library-level slot routing lives in `wbase::hash_slot` (`slot_of`), which consumes this crate's `GOLDEN_RATIO_64` and the bijective `mix13` finalizer; this crate deliberately exposes none of them.

These are non-cryptographic hashes. Do not use them for passwords, signatures, authentication, or tamper-proof integrity checks. Wider output does not guarantee collision freedom.

## Usage

### Installation and target requirements

```sh
cargo add whasher
```

Use Rust 2024 with toolchain support for `slice::as_chunks` (stabilized in Rust 1.88). The current backend requires AES and SSE2 on x86_64, or AES and NEON on aarch64. It has no portable software fallback for unsupported targets.

The repository workspace enables `target-feature=+aes`. Outside this workspace, configure the required target features explicitly, or build for the local processor:

```sh
RUSTFLAGS="-C target-cpu=native" cargo build
```

The processor must support the required instructions. Native builds may not run on deployment machines with different processor capabilities; select flags for the deployment target when distributing binaries.

### Key hashing

```rust
use whasher::{GOLDEN_RATIO_64, fast_hash, fast_hash_i64, fast_hash_with_seed, hash128, mix13, splitmix64};

fn main() {
  let data = b"hello world";
  assert_eq!(fast_hash(data), fast_hash_with_seed(data, 0));
  assert_eq!(fast_hash_i64(data), fast_hash(data) as i64);
  assert_eq!(hash128(data, 7, 9), hash128(data, 7, 9));

  assert_eq!(splitmix64(42), mix13(42u64.wrapping_add(GOLDEN_RATIO_64)));
}
```

`fast_hash` is the single source for generic byte-string key hashing (index tags, lock striping, migration sketches); `fast_hash_with_seed` carries the seeded domain used for slot-domain separation; `hash128` derives strong 128-bit key IDs from two seeds.

### Streaming checksums

Chunk boundaries do not affect the checksum of the concatenated bytes. The example crosses the internal 64-byte stripe boundary, as the streaming tests do.

```rust
use whasher::{StreamHasher, compute_checksum, compute_checksum_with_seed};

fn main() {
  let data = b"The quick brown fox jumps over the lazy dog. A fast streaming hash with buffer.";
  let mut hasher = StreamHasher::new();
  for chunk in data.chunks(7) {
    hasher.write(chunk);
  }
  let checksum = hasher.finish();
  assert_eq!(checksum, compute_checksum(data));
  assert_eq!(hasher.finish(), checksum);

  hasher.write(b"tail");
  let full = [data.as_slice(), b"tail"].concat();
  assert_eq!(hasher.finish(), compute_checksum(&full));

  let mut seeded = StreamHasher::with_seed(42);
  seeded.write(data);
  assert_eq!(seeded.finish(), compute_checksum_with_seed(data, 42));
  seeded.reset();
  assert_eq!(seeded.finish(), compute_checksum_with_seed(b"", 42));
}
```

`compute_checksum(data)` matches `StreamHasher::new()` followed by `write(data)` and `finish()`. It is not an alias for `fast_hash(data)`: the streaming family and the one-shot key-hash family are independent algorithm domains, and derived values must not be interchanged across domains.

## Features

- AES-backed 64-bit and 128-bit byte hashing, with default or explicit seeds.
- Chunk-invariant streaming checksums with non-destructive finalization and seed-preserving reset.
- Allocation-free streaming state: 128-byte size, 64-byte alignment, and a 64-byte residual buffer.
- Compile-time initialization for the default streaming seed and independent folding lanes for instruction-level parallelism.
- Structural-collision-safe routing for inputs of 32 KiB and above (see [Design](#design)).
- Bijective `u64` scramblers (`mix13`, `splitmix64`, `mix_thread_id`) for striping and sharding.

No optional crate features are currently defined; the default feature set is empty.

## Design

All public interfaces and internal streaming logic reside in `src/lib.rs`. The implementation separates responsibilities through functions and types rather than submodules.

### Input-domain routing

gxhash 3.5.0 folds large inputs by XOR-accumulating 128-byte groups with a per-byte, mod-256 group counter, and mixes the length as `u32` only. At and above 32 KiB this admits constructible whole-state collision families (same-phase group swaps, 32 KiB block permutations, 2^32 length wrap-around). The private `ONE_SHOT_MAX` threshold therefore routes every `fast_hash`, `fast_hash_with_seed`, and `hash128` call of 32 KiB or more into the position-sensitive streaming folding family, which biases the seed with a reserved domain constant and mixes the full-width `u64` length. Inputs below the threshold keep the single-shot hardware path (roughly 0.31 ns for small keys). `tests/main.rs` locks the collision families and the boundary from both sides.

### Streaming path

1. `new` or `with_seed` initializes 4 folding lanes. The zero-seed state is precomputed at compile time; other seeds are mixed with lane-specific salts through the bijective `mix13`.
2. `write` fills any buffered remainder, folds complete 64-byte stripes, and retains the trailing bytes. Stripe number modulo 4 selects the lane, independent of write boundaries.
3. Aligned groups of 4 stripes update independent lanes. Complete input stripes are read directly without copying into the residual buffer.
4. `finish` merges the lane states in order, then hashes the remainder followed by the total byte count encoded as little-endian `u64`. It leaves the state unchanged.
5. `reset` restores the original seed state and clears counters without zeroing the residual buffer.

`compute_checksum*` constructs this state, writes the full input, and finalizes it. Processing is linear in input length with constant auxiliary memory.

### Seed domains

`fast_hash` (fixed seed 0), `fast_hash_with_seed` (explicit seeds, e.g. namespace-derived sketch seeds), and the streaming checksum family form independent algorithm domains: the same `(input, seed)` never yields the same value across domains. The large-input key-hash path biases the streaming seed with the reserved `FAST_HASH_DOMAIN` constant so the routed domains stay separated as well. Derived values must not be mixed across domains.

### Compatibility boundaries

Fixed-seed byte hashing is repeatable for the same algorithm configuration. Persisted or transmitted hashes should record the backend version, seed, and algorithm choice; do not assume compatibility across backend or streaming implementation changes.

Generic `Hash` input is not guaranteed portable across platforms or compiler versions. Encode persistent keys explicitly before byte hashing. Seed pairs in `hash128` are combined nonlinearly (`mix13`, then XOR with a 32-bit rotation), so structured seed pairs cannot collide algebraically; only birthday-bound collisions inherent to the 128-to-64-bit reduction remain. The APIs do not reproduce MurmurHash or XxHash output from C# implementations; Redis wire-compatible MurmurHash2 lives in the workspace `wbase` crate instead.

## Technology stack

| Component                 | Role                                                                  |
| ------------------------- | --------------------------------------------------------------------- |
| Rust 2024, `core::hash`   | Hash trait, compile-time state and layout checks                       |
| `gxhash` 3.5.0            | AES/SIMD byte-hashing backend for one-shot and streaming paths         |
| `aok`, `ctor`, `log_init` | Test result ergonomics and logging initialization                      |
| Cargo Nextest             | Integration-test runner used by `test.sh`                              |
| Bun and `mdt`             | Generate `README.md` from `README.mdt` and bilingual source documents  |

Dependency versions above are the requirements declared in the package manifest, not exact version pins.

## Directory structure

Paths are relative to the package root.

```text
whasher/
  src/
    lib.rs
  tests/
    main.rs
  readme/
    en.md
    zh.md
  AGENTS.md
  Cargo.toml
  README.mdt
  README.md
  test.sh
```

`tests/main.rs` is the primitive lock set: compile-time versus derived seed-0 lane identity, streaming chunk invariance against one-shot checksums, the `ONE_SHOT_MAX` structural-collision families with routing-boundary pins, and seed-domain separation across the key-hash and checksum families. `README.mdt` includes both language sources; edit those sources and regenerate the combined README.

## API reference

All interfaces below are available at the `whasher` crate root. Arguments named `bytes` or `data` borrow input slices; hashing functions return values directly, not `Result`. Empty slices are valid input.

### Hash functions

| Signature                                                   | Behavior                                                                                           |
| ----------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `fast_hash(bytes: &[u8]) -> u64`                            | 64-bit key hash with fixed seed 0; ≥ 32 KiB routes to the streaming domain.                        |
| `fast_hash_i64(bytes: &[u8]) -> i64`                        | Two's-complement `i64` view of `fast_hash`; the workspace's single such reinterpretation point.     |
| `fast_hash_with_seed(bytes: &[u8], seed: u64) -> u64`       | Seeded key hash for slot-domain separation; same input-domain routing.                              |
| `hash128(bytes: &[u8], seed_a: u64, seed_b: u64) -> u128`   | 128-bit key ID; seeds combined nonlinearly via `mix13`; ≥ 32 KiB uses two independent streaming checksums. |
| `compute_checksum(data: &[u8]) -> u64`                      | Full-input streaming checksum with seed 0.                                                         |
| `compute_checksum_with_seed(data: &[u8], seed: u64) -> u64` | Full-input streaming checksum with an explicit seed.                                               |

Explicit `u64` seeds are interpreted by the backend as `i64` bit patterns; the high bit is preserved.

### Scrambling primitives

| Export                              | Behavior                                                                    |
| ----------------------------------- | ------------------------------------------------------------------------------ |
| `GOLDEN_RATIO_64`                   | The 64-bit golden-ratio constant (2^64 / φ).                                 |
| `mix13(z: u64) -> u64`              | Stafford Variant 13 bijective finalizer; branch-free and table-free.         |
| `splitmix64(z: u64) -> u64`         | SplitMix64 transform: `mix13(z.wrapping_add(GOLDEN_RATIO_64))`.              |
| `mix_thread_id(tid: u64) -> usize`  | Scatters a thread ID or sequence number into an unbiased stripe slot index.  |

### `StreamHasher`

Streaming checksum state with private fields. Implements `Clone`, `Debug`, `Default`, and `core::hash::Hasher`. Clones preserve the current state and can then be advanced or reset independently. Inherent `write` and `finish` methods do not require importing the trait.

| Method                               | Behavior                                                                                 |
| ------------------------------------ | -------------------------------------------------------------------------------------------- |
| `const new() -> Self`                | Creates empty state with seed 0; also used by `Default`.                                   |
| `const with_seed(seed: u64) -> Self` | Creates empty state with the supplied seed.                                                |
| `write(&mut self, bytes: &[u8])`     | Appends bytes; empty input leaves the state unchanged.                                     |
| `finish(&self) -> u64`               | Returns the checksum without resetting; further writes remain valid.                       |
| `reset(&mut self)`                   | Restores empty state with the construction seed; does not securely erase buffered bytes.   |

Chunk invariance concerns `write(&[u8])` calls over the same concatenated bytes. It does not make arbitrary `Hash` implementations a portable encoding. The internal counters use wrapping `u64` arithmetic.

## Validation

Run from the package directory within the repository workspace:

```sh
./test.sh
bun x mdt
```

The test script invokes `cargo nextest run --all-features --no-capture` and requires Cargo Nextest. The document generator requires Bun. The package inherits workspace lint settings.
