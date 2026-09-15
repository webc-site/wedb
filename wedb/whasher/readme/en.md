# whasher : Hardware-accelerated hashing and collections

- [Overview](#overview)
- [Usage](#usage)
  - [Installation and target requirements](#installation-and-target-requirements)
  - [Hashing and standard collections](#hashing-and-standard-collections)
  - [Streaming checksums](#streaming-checksums)
  - [Concurrent collections](#concurrent-collections)
- [Features](#features)
- [Design](#design)
  - [Direct hashing](#direct-hashing)
  - [Streaming path](#streaming-path)
  - [Collection path](#collection-path)
  - [Compatibility boundaries](#compatibility-boundaries)
- [Technology stack](#technology-stack)
- [Directory structure](#directory-structure)
- [API reference](#api-reference)
  - [Hash functions](#hash-functions)
  - [Scrambling primitives](#scrambling-primitives)
  - [`StreamHasher`](#streamhasher)
  - [Collection types and re-exports](#collection-types-and-re-exports)
  - [Collection constructors](#collection-constructors)
- [Validation](#validation)

## Overview

whasher provides byte and integer hashing, chunk-invariant streaming checksums, and hash collections backed by `gxhash`. It supports 64-bit and 128-bit output and explicit seeds.

Standard maps and sets use `GxBuildHasher`. The concurrent map combines the same builder with `papaya`, keeping hash selection consistent across collection types.

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

### Hashing and standard collections

This example follows the hashing and collection tests.

```rust
use whasher::{
  GOLDEN_RATIO_64, HashSet, fast_hash, fast_hash_u64, fast_hash_with_seed, hash128, hash_set_with_capacity,
  mix13, new_hash_map, splitmix64,
};

fn main() {
  let data = b"hello world";
  assert_eq!(fast_hash(data), fast_hash_with_seed(data, 0));
  assert_eq!(fast_hash_u64(42), fast_hash(&42u64.to_le_bytes()));
  assert_eq!(hash128(data, 7, 9), hash128(data, 7, 9));

  assert_eq!(splitmix64(42), mix13(42u64.wrapping_add(GOLDEN_RATIO_64)));

  let mut map = new_hash_map();
  map.insert("key", 100);
  assert_eq!(map.get("key"), Some(&100));

  let mut set = hash_set_with_capacity(8);
  set.insert("alpha");
  assert!(set.contains("alpha"));
}
```

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

`compute_checksum(data)` matches `StreamHasher::new()` followed by `write(data)` and `finish()`. It is not an alias for `fast_hash(data)`: the streaming family and the one-shot family are independent algorithm domains, and derived values must not be interchanged across domains.

### Concurrent collections

This detached-thread example adapts the concurrent map test. Ownership crosses threads through `Arc`, as in the test suite.

```rust
use std::{sync::Arc, thread};

use whasher::{GxPapayaMap, new_papaya_map};

fn main() {
  let map: Arc<GxPapayaMap<u64, u64>> = Arc::new(new_papaya_map());
  thread::scope(|scope| {
    for t in 0..4u64 {
      let map = Arc::clone(&map);
      scope.spawn(move || {
        let map_pin = map.pin();
        for i in 0..1000u64 {
          let key = t * 1000 + i;
          map_pin.insert(key, key * 2);
        }
      });
    }
  });

  assert_eq!(map.len(), 4000);
  let map_pin = map.pin();
  assert_eq!(map_pin.get(&42), Some(&84));
}
```

Keep pinned access handles alive while using borrowed entries. Drop them promptly after access so they do not unnecessarily delay memory reclamation. Concurrency and reclamation behavior come from `papaya`; not every operation is guaranteed to be lock-free.

## Features

- AES-backed 64-bit and 128-bit byte hashing, with default or explicit seeds.
- Little-endian integer hashing and bijective `u64` scramblers for striping and sharding.
- Chunk-invariant streaming checksums with non-destructive finalization and seed-preserving reset.
- Allocation-free streaming state: 128-byte size, 64-byte alignment, and a 64-byte residual buffer.
- Compile-time initialization for the default streaming seed and independent folding lanes for instruction-level parallelism.
- Standard maps and sets plus a lock-free concurrent map, all on one hardware-accelerated builder.

No optional crate features are currently defined; the default feature set is empty.

## Design

All public interfaces and internal streaming logic reside in `src/lib.rs`. The implementation separates responsibilities through functions and types rather than submodules.

### Direct hashing

`fast_hash` and `fast_hash_with_seed` delegate to `gxhash::gxhash64`; `hash128` delegates to `gxhash::gxhash128`. `fast_hash_u64` first converts the integer to little-endian bytes. `hash128` folds each seed through the bijective `mix13` mixer, then XORs with a 32-bit rotation, so structured seed pairs cannot collide algebraically; only birthday-bound random collisions inherent to the 128-to-64 bit reduction remain.

### Streaming path

1. `new` or `with_seed` initializes 4 folding lanes. The zero-seed state is precomputed; other seeds are mixed with lane-specific salts.
2. `write` fills any buffered remainder, folds complete 64-byte stripes, and retains the trailing bytes. Stripe number modulo 4 selects the lane, independent of write boundaries.
3. Aligned groups of 4 stripes update independent lanes. Complete input stripes are read directly without copying into the residual buffer.
4. `finish` merges the lane states in order, then hashes the remainder followed by the total byte count encoded as little-endian `u64`. It leaves the state unchanged.
5. `reset` restores the original seed state and clears counters without zeroing the residual buffer.

`compute_checksum*` constructs this state, writes the full input, and finalizes it. Processing is linear in input length with constant auxiliary memory.

### Collection path

Standard collection constructors install `GxBuildHasher::default()`. The concurrent map constructor configures the `papaya` builder with the same hasher. The backend defaults randomize collection hashing unless the dependency's `deterministic` feature is enabled through Cargo feature unification.

### Compatibility boundaries

Fixed-seed byte hashing is repeatable for the same algorithm configuration. Persisted or transmitted hashes should record the backend version, seed, and algorithm choice; do not assume compatibility across backend or streaming implementation changes.

Generic `Hash` input is not guaranteed portable across platforms or compiler versions. Encode persistent keys explicitly before byte hashing. Seed pairs in `hash128` collide only at the birthday bound of the 128-to-64 bit reduction, without algebraic structure. The APIs do not reproduce MurmurHash or XxHash output from C# implementations; Redis wire-compatible MurmurHash2 lives in the workspace `wbase` crate instead.

## Technology stack

| Component                                   | Role                                                                     |
| ------------------------------------------- | ------------------------------------------------------------------------ |
| Rust 2024, `core::hash`, `std::collections` | Hash traits, collection interfaces, compile-time state and layout checks |
| `gxhash` 3.5.0                              | AES/SIMD byte hashing, hashers, and standard collection aliases          |
| `papaya` 0.2.5                              | Lock-free concurrent map with guarded access                             |
| `aok`, `ctor`, `log_init`                   | Test results and logging initialization                                  |
| Cargo Nextest                               | Integration-test runner used by `test.sh`                                |
| Bun and `mdt`                               | Generate `README.md` from `README.mdt` and bilingual source documents    |

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

`tests/main.rs` covers collection operations, concurrent insertion, hash distribution, seeded hashing and seed-domain separation, streaming boundaries, reset, cloning, and long inputs. `README.mdt` includes both language sources; edit those sources and regenerate the combined README.

## API reference

All interfaces below are available at the `whasher` crate root. Arguments named `bytes` or `data` borrow input slices; hashing functions return values directly, not `Result`.

### Hash functions

| Signature                                                   | Behavior                                                           |
| ----------------------------------------------------------- | ------------------------------------------------------------------ |
| `fast_hash(bytes: &[u8]) -> u64`                            | Direct 64-bit byte hash with seed 0.                               |
| `fast_hash_u64(val: u64) -> u64`                            | Equivalent to `fast_hash(&val.to_le_bytes())`.                     |
| `fast_hash_with_seed(bytes: &[u8], seed: u64) -> u64`       | Direct 64-bit byte hash with an explicit seed.                     |
| `hash128(bytes: &[u8], seed_a: u64, seed_b: u64) -> u128`   | Mixes both seeds through `mix13`, then XOR with a 32-bit rotation. |
| `compute_checksum(data: &[u8]) -> u64`                      | Full-input streaming checksum with seed 0.                         |
| `compute_checksum_with_seed(data: &[u8], seed: u64) -> u64` | Full-input streaming checksum with an explicit seed.               |

Explicit `u64` seeds are interpreted by the backend as `i64` bit patterns; the high bit is preserved. Empty slices are valid input.

### Scrambling primitives

| Export                            | Behavior                                                                     |
| --------------------------------- | ---------------------------------------------------------------------------- |
| `GOLDEN_RATIO_64`                 | The 64-bit golden-ratio constant (2^64 / φ).                                 |
| `mix13(z: u64) -> u64`            | Stafford Variant 13 bijective finalizer; branch-free and table-free.         |
| `splitmix64(z: u64) -> u64`       | SplitMix64 transform: `mix13(z.wrapping_add(GOLDEN_RATIO_64))`.              |
| `mix_thread_id(tid: u64) -> usize`| Scatters a thread ID or sequence number into an unbiased stripe slot index.  |

### `StreamHasher`

Streaming checksum state with private fields. Implements `Clone`, `Debug`, `Default`, and `core::hash::Hasher`. Clones preserve the current state and can then be advanced or reset independently. Inherent `write` and `finish` methods do not require importing the trait.

| Method                               | Behavior                                                                                 |
| ------------------------------------ | ---------------------------------------------------------------------------------------- |
| `const new() -> Self`                | Creates empty state with seed 0; also used by `Default`.                                 |
| `const with_seed(seed: u64) -> Self` | Creates empty state with the supplied seed.                                              |
| `write(&mut self, bytes: &[u8])`     | Appends bytes; empty input leaves the state unchanged.                                   |
| `finish(&self) -> u64`               | Returns the checksum without resetting; further writes remain valid.                     |
| `reset(&mut self)`                   | Restores empty state with the construction seed; does not securely erase buffered bytes. |

Chunk invariance concerns `write(&[u8])` calls over the same concatenated bytes. It does not make arbitrary `Hash` implementations a portable encoding. The internal counters use wrapping `u64` arithmetic.

### Collection types and re-exports

| Export              | Definition or purpose                                                                                        |
| ------------------- | ------------------------------------------------------------------------------------------------------------- |
| `HashMap<K, V>`     | `std::collections::HashMap<K, V, GxBuildHasher>`, re-exported from `gxhash`.                                  |
| `HashSet<T>`        | `std::collections::HashSet<T, GxBuildHasher>`, re-exported from `gxhash`.                                     |
| `GxBuildHasher`     | Backend `BuildHasher`; supports `default()` and `with_seed(i64)`. Use the default for randomized collection hashing. |
| `HashMapExt`        | Trait supplying `new() -> Self` and `with_capacity(usize) -> Self` for the map alias when imported.           |
| `HashSetExt`        | Trait supplying `new() -> Self` and `with_capacity(usize) -> Self` for the set alias when imported.           |
| `GxPapayaMap<K, V>` | `papaya::HashMap<K, V, GxBuildHasher>`; use `pin()` for guarded operations.                                   |
| `papaya`            | Re-exported dependency, accessible as `whasher::papaya`. Its own types retain their upstream defaults.        |

### Collection constructors

| Signature                                            | Behavior                                             |
| ---------------------------------------------------- | ---------------------------------------------------- |
| `new_hash_map<K, V>() -> HashMap<K, V>`              | Creates an empty standard map.                       |
| `hash_set_with_capacity<T>(capacity: usize) -> HashSet<T>` | Creates an empty standard set with initial capacity. |
| `new_papaya_map<K, V>() -> GxPapayaMap<K, V>`        | Creates an empty concurrent map.                     |

Constructors impose no key trait bounds. Insertion and lookup require the underlying collections' `Hash` and `Eq` bounds; cross-thread sharing also requires the applicable `Send` and `Sync` bounds. Capacity is an initial allocation hint, not a size limit.

## Validation

Run from the package directory within the repository workspace:

```sh
./test.sh
bun x mdt
```

The test script invokes `cargo nextest run --all-features --no-capture` and requires Cargo Nextest. The document generator requires Bun. The package inherits workspace lint settings.
