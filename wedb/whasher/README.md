[English](#en) | [中文](#zh)

---

<a name="en"></a>

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


---

<a name="zh"></a>

# whasher : 硬件加速哈希与集合

- [项目介绍](#项目介绍)
- [使用演示](#使用演示)
  - [安装与目标要求](#安装与目标要求)
  - [哈希与普通集合](#哈希与普通集合)
  - [流式校验和](#流式校验和)
  - [并发集合](#并发集合)
- [特性介绍](#特性介绍)
- [设计思路](#设计思路)
  - [直接哈希](#直接哈希)
  - [流式路径](#流式路径)
  - [集合路径](#集合路径)
  - [兼容性边界](#兼容性边界)
- [技术堆栈](#技术堆栈)
- [目录结构](#目录结构)
- [API 说明](#api-说明)
  - [哈希函数](#哈希函数)
  - [打散原语](#打散原语)
  - [StreamHasher](#streamhasher)
  - [集合类型与重导出](#集合类型与重导出)
  - [集合构造函数](#集合构造函数)
- [验证](#验证)

- [项目介绍](#项目介绍)
- [使用演示](#使用演示)
  - [安装与目标要求](#安装与目标要求)
  - [哈希与普通集合](#哈希与普通集合)
  - [流式校验和](#流式校验和)
  - [并发集合](#并发集合)
- [特性介绍](#特性介绍)
- [设计思路](#设计思路)
  - [直接哈希](#直接哈希)
  - [流式路径](#流式路径)
  - [集合路径](#集合路径)
  - [兼容性边界](#兼容性边界)
- [技术堆栈](#技术堆栈)
- [目录结构](#目录结构)
- [API 说明](#api-说明)
  - [哈希函数](#哈希函数)
  - [打散原语](#打散原语)
  - [StreamHasher](#streamhasher)
  - [集合类型与重导出](#集合类型与重导出)
  - [集合构造函数](#集合构造函数)
- [验证](#验证)

## 项目介绍

whasher 提供基于 gxhash 的字节与整数哈希、分块恒等的流式校验和与哈希集合。支持 64 位与 128 位输出、显式种子。

普通映射与集合搭载 `GxBuildHasher`。并发映射在同一构建器之上组合 papaya，使各集合类型的哈希选择保持一致。

以上均为非加密哈希。不可用于口令、签名、身份认证或防篡改完整性校验。更宽的输出不保证无碰撞。

## 使用演示

### 安装与目标要求

```sh
cargo add whasher
```

使用 Rust 2024，工具链需支持 `slice::as_chunks`（Rust 1.88 起稳定）。当前后端在 x86_64 需要 AES 与 SSE2，在 aarch64 需要 AES 与 NEON，对不支持的目标没有可移植软件回退。

仓库工作区已启用 `target-feature=+aes`。工作区之外需显式配置所需 target-feature，或面向本机处理器构建：

```sh
RUSTFLAGS="-C target-cpu=native" cargo build
```

处理器必须支持所需指令集。面向本机的构建可能无法在指令集不同的部署机器上运行；分发二进制时请按部署目标选择参数。

### 哈希与普通集合

本示例对应哈希与集合测试。

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

### 流式校验和

分块边界不影响拼接后字节的校验和。示例跨越内部 64 字节条带边界，与流式测试一致。

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

`compute_checksum(data)` 等价于 `StreamHasher::new()` 后依次 `write(data)` 与 `finish()`，并非 `fast_hash(data)` 的别名：流式校验和族与单次键哈希族是两套独立算法域，派生值不得跨域互换。

### 并发集合

本脱离线程示例改写自并发映射测试。所有权经 `Arc` 跨线程共享，与测试套件一致。

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

借用条目期间应保持 pin 访问句柄存活，访问后及时释放，以免不必要地延迟内存回收。并发与回收行为来自 papaya，并非所有操作都保证无锁。

## 特性介绍

- 基于 AES 的 64 位与 128 位字节哈希，支持默认或显式种子。
- 小端整数哈希与用于分段/分片的双射 `u64` 打散原语。
- 分块恒等的流式校验和，非破坏性收尾，保留种子的复位。
- 零堆分配的流式状态：128 字节体积、64 字节对齐、64 字节残留缓冲。
- 默认流式种子编译期初始化，独立折叠链提升指令级并行。
- 普通映射与集合加无锁并发映射，统一搭载硬件加速构建器。

当前未定义可选 crate 特性，默认特性集为空。

## 设计思路

全部公开接口与内部流式逻辑集中在 `src/lib.rs`，以函数与类型而非子模块划分职责。

### 直接哈希

`fast_hash` 与 `fast_hash_with_seed` 委托给 `gxhash::gxhash64`；`hash128` 委托给 `gxhash::gxhash128`。`fast_hash_u64` 先将整数转为小端字节。`hash128` 先将两个种子各自经双射 `mix13` 打散，再异或错位合并，结构化种子对无法产生代数碰撞，仅剩 128→64 位固有的生日界随机碰撞。

### 流式路径

1. `new` 或 `with_seed` 初始化 4 条折叠链。零种子状态编译期预计算；其他种子与各链盐值混合。
2. `write` 补满缓冲残留，折叠完整 64 字节条带，并保留尾部字节。条带号对 4 取模决定链归属，与写入边界无关。
3. 对齐的 4 条带组更新独立折叠链。完整输入条带直接读取，不拷贝进残留缓冲。
4. `finish` 依序汇合各链状态，再对残留字节与按小端编码的累计字节数做末端混合，不改动内部状态。
5. `reset` 恢复构造时种子对应的初态并清零计数器，不清零残留缓冲。

`compute_checksum*` 构造该状态、写入全部输入并收尾。处理耗时随输入线性增长，辅助内存恒定。

### 集合路径

普通集合构造函数安装 `GxBuildHasher::default()`。并发映射构造函数以同一构建器配置 papaya。后端默认随机化集合哈希，除非通过 Cargo 特性统一启用依赖的 `deterministic` 特性。

### 兼容性边界

固定种子的字节哈希在相同算法配置下可复现。落盘或传输的哈希应记录后端版本、种子与算法选择；不要假设跨后端或流式实现变更仍兼容。

泛型 `Hash` 输入不保证跨平台或跨编译器版本可移植。持久化键请先显式编码再做字节哈希。`hash128` 的种子对仅在 128→64 位归约固有的生日界上碰撞，无代数结构。以上接口不重现 C# 实现的 MurmurHash 或 XxHash 输出；Redis 协议逐位兼容的 MurmurHash2 由工作区 `wbase` crate 提供。

## 技术堆栈

| 组件                                        | 作用                                       |
| ------------------------------------------- | ------------------------------------------ |
| Rust 2024、`core::hash`、`std::collections` | 哈希 trait、集合接口、编译期状态与布局校验 |
| gxhash 3.5.0                                | AES/SIMD 字节哈希、哈希器与普通集合别名    |
| papaya 0.2.5                                | 无锁并发映射，守卫式访问                   |
| aok、ctor、log_init                         | 测试结果与日志初始化                       |
| Cargo Nextest                               | `test.sh` 使用的集成测试运行器             |
| Bun 与 mdt                                  | 由 README.mdt 与双语源文档生成 README.md   |

上述依赖版本为清单中声明的版本要求，并非精确锁定版本。

## 目录结构

路径相对包根目录。

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

`tests/main.rs` 覆盖集合操作、并发插入、哈希分布、带种子哈希与种子域分离、流式边界、复位、克隆与超长输入。`README.mdt` 包含两语言源文档；请编辑源文档后重新生成合并的 README。

## API 说明

以下接口均位于 `whasher` crate 根。参数 `bytes` 或 `data` 借用输入切片；哈希函数直接返回数值，不返回 `Result`。

### 哈希函数

| 签名                                                        | 行为                                   |
| ----------------------------------------------------------- | -------------------------------------- |
| `fast_hash(bytes: &[u8]) -> u64`                            | 种子为 0 的直接 64 位字节哈希。        |
| `fast_hash_u64(val: u64) -> u64`                            | 等价于 `fast_hash(&val.to_le_bytes())`。 |
| `fast_hash_with_seed(bytes: &[u8], seed: u64) -> u64`       | 显式种子的直接 64 位字节哈希。         |
| `hash128(bytes: &[u8], seed_a: u64, seed_b: u64) -> u128`   | 双种子经 `mix13` 打散后异或错位合并。  |
| `compute_checksum(data: &[u8]) -> u64`                      | 种子为 0 的整段输入流式校验和。        |
| `compute_checksum_with_seed(data: &[u8], seed: u64) -> u64` | 显式种子的整段输入流式校验和。         |

显式 `u64` 种子由后端按 `i64` 位型解释，最高位保留。空切片为合法输入。

### 打散原语

| 导出                              | 行为                                                   |
| --------------------------------- | ------------------------------------------------------ |
| `GOLDEN_RATIO_64`                 | 64 位黄金分割常数（2^64 / φ）。                        |
| `mix13(z: u64) -> u64`            | Stafford Variant 13 双射终末变换，无分支、无查表。     |
| `splitmix64(z: u64) -> u64`       | SplitMix64 变换：`mix13(z.wrapping_add(GOLDEN_RATIO_64))`。 |
| `mix_thread_id(tid: u64) -> usize`| 将线程 ID 或顺序序号打散为无偏条带槽位索引。           |

### StreamHasher

流式校验和状态，字段私有。实现 `Clone`、`Debug`、`Default` 与 `core::hash::Hasher`。克隆保留当前状态，之后可独立推进或复位。固有方法 `write` 与 `finish` 无需导入 trait。

| 方法                                 | 行为                                       |
| ------------------------------------ | ------------------------------------------ |
| `const new() -> Self`                | 创建种子为 0 的空状态，`Default` 亦采用。  |
| `const with_seed(seed: u64) -> Self` | 创建指定种子的空状态。                     |
| `write(&mut self, bytes: &[u8])`     | 追加字节；空输入不改动状态。               |
| `finish(&self) -> u64`               | 返回校验和而不复位，之后仍可继续写入。     |
| `reset(&mut self)`                   | 恢复构造种子对应的空状态；不擦除缓冲字节。 |

分块恒等针对相同拼接字节上的 `write(&[u8])` 调用，并不使任意 `Hash` 实现成为可移植编码。内部计数采用 `u64` 环绕算术。

### 集合类型与重导出

| 导出                | 定义或用途                                                                     |
| ------------------- | ------------------------------------------------------------------------------- |
| `HashMap<K, V>`     | `std::collections::HashMap<K, V, GxBuildHasher>`，自 gxhash 重导出。            |
| `HashSet<T>`        | `std::collections::HashSet<T, GxBuildHasher>`，自 gxhash 重导出。               |
| `GxBuildHasher`     | 后端 `BuildHasher`，支持 `default()` 与 `with_seed(i64)`。默认实例用于随机化集合哈希。 |
| `HashMapExt`        | 导入后为映射别名提供 `new()` 与 `with_capacity(usize)`。                        |
| `HashSetExt`        | 导入后为集合别名提供 `new()` 与 `with_capacity(usize)`。                        |
| `GxPapayaMap<K, V>` | `papaya::HashMap<K, V, GxBuildHasher>`，经 `pin()` 守卫式操作。                 |
| `papaya`            | 重导出的依赖，以 `whasher::papaya` 访问，其自身类型保留上游默认。               |

### 集合构造函数

| 签名                                                       | 行为                         |
| ---------------------------------------------------------- | ---------------------------- |
| `new_hash_map<K, V>() -> HashMap<K, V>`                    | 创建空普通映射。             |
| `hash_set_with_capacity<T>(capacity: usize) -> HashSet<T>` | 创建带初始容量的空普通集合。 |
| `new_papaya_map<K, V>() -> GxPapayaMap<K, V>`              | 创建空并发映射。             |

构造函数不施加键 trait 约束。插入与查找要求底层集合的 `Hash` 与 `Eq` 约束；跨线程共享还需满足相应的 `Send` 与 `Sync` 约束。容量是初始分配提示，并非大小上限。

## 验证

在仓库工作区内于包目录执行：

```sh
./test.sh
bun x mdt
```

测试脚本调用 `cargo nextest run --all-features --no-capture`，需要 Cargo Nextest。文档生成需要 Bun。包继承工作区 lint 设置。

