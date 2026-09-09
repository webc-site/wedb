# whasher : 硬件加速哈希与集合

- [项目介绍](#项目介绍)
- [使用演示](#使用演示)
  - [安装与目标要求](#安装与目标要求)
  - [哈希与普通集合](#哈希与普通集合)
  - [流式校验和](#流式校验和)
  - [并发集合](#并发集合)
- [特性介绍](#特性介绍)
- [设计思路](#设计思路)
  - [直接哈希与泛型哈希](#直接哈希与泛型哈希)
  - [流式路径](#流式路径)
  - [集合路径](#集合路径)
  - [兼容性边界](#兼容性边界)
- [技术堆栈](#技术堆栈)
- [目录结构](#目录结构)
- [API 说明](#api-说明)
  - [哈希函数](#哈希函数)
  - [StreamHasher](#streamhasher)
  - [集合类型与重导出](#集合类型与重导出)
  - [集合构造函数](#集合构造函数)
- [验证](#验证)

## 项目介绍

whasher 提供基于 gxhash 的字节与整数哈希、流式校验和与哈希集合。支持 64 位与 128 位输出、显式种子，以及实现 Rust `Hash` trait 的泛型值。

普通映射与集合搭载 `GxBuildHasher`。并发映射与集合在同一构建器之上组合 papaya，使各集合类型的哈希选择保持一致。

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
  Entry, fast_hash, fast_hash_u64, fast_hash_with_seed, fast_hash128,
  hash_value, hash_value_with_seed, hash128, hash128_with_seed, mix13,
  new_hash_map, new_hash_set,
};

fn main() {
  let data = b"hello world";
  assert_eq!(fast_hash(data), fast_hash_with_seed(data, 0));
  assert_eq!(fast_hash_u64(42), fast_hash(&42u64.to_le_bytes()));
  assert_eq!(fast_hash128(data), hash128_with_seed(data, 0));
  assert_eq!(hash128(data, 7, 9), hash128_with_seed(data, (mix13(7) ^ mix13(9).rotate_left(32)) as u64));
  assert_eq!(hash_value(&(1u64, 2u64)), hash_value_with_seed(&(1u64, 2u64), 0));

  let mut map = new_hash_map();
  map.insert("key", 100);
  if let Entry::Occupied(mut entry) = map.entry("key") {
    *entry.get_mut() += 50;
  }
  assert_eq!(map.get("key"), Some(&150));

  let mut set = new_hash_set();
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
  assert_eq!(hasher.total_bytes_written(), data.len() as u64);

  hasher.write(b"tail");
  let full = [data.as_slice(), b"tail"].concat();
  assert_eq!(hasher.finish(), compute_checksum(&full));

  let mut seeded = StreamHasher::with_seed(42);
  seeded.write(data);
  assert_eq!(seeded.finish(), compute_checksum_with_seed(data, 42));
  seeded.reset();
  assert!(seeded.is_empty());
  assert_eq!(seeded.finish(), compute_checksum_with_seed(b"", 42));
}
```

`compute_checksum(data)` 等价于 `StreamHasher::new()` 后依次 `write(data)` 与 `finish()`，并非 `fast_hash(data)` 的别名。需要字节分块恒等时应使用 `StreamHasher`，而非重导出的 `GxHasher`。

### 并发集合

本作用域线程示例改写自并发映射与集合测试。作用域线程借用集合；脱离作用域的所有权可改用 `Arc`，如测试套件所示。

```rust
use std::thread;

use whasher::{papaya_map_with_capacity, papaya_set_with_capacity};

fn main() {
  let map = papaya_map_with_capacity::<u64, u64>(4000);
  let set = papaya_set_with_capacity::<u64>(4000);
  thread::scope(|scope| {
    for t in 0..4u64 {
      let map = &map;
      let set = &set;
      scope.spawn(move || {
        let map_pin = map.pin();
        let set_pin = set.pin();
        for i in 0..1000u64 {
          let key = t * 1000 + i;
          map_pin.insert(key, key * 2);
          set_pin.insert(key);
        }
      });
    }
  });

  let map_pin = map.pin();
  let set_pin = set.pin();
  assert_eq!(map.len(), 4000);
  assert_eq!(set.len(), 4000);
  assert_eq!(map_pin.get(&42), Some(&84));
  assert!(set_pin.contains(&42));
}
```

借用条目期间应保持 pin 访问句柄存活，访问后及时释放，以免不必要地延迟内存回收。并发与回收行为来自 papaya，并非所有操作都保证无锁。

## 特性介绍

- 基于 AES 的 64 位与 128 位字节哈希，支持默认或显式种子。
- 小端整数哈希与泛型 `Hash` 输入支持。
- 分块恒等的流式校验和，非破坏性收尾，保留种子的复位。
- 零堆分配的流式状态：128 字节体积、64 字节对齐、64 字节残留缓冲。
- 默认流式种子编译期初始化，独立折叠链提升指令级并行。
- 普通与并发映射、集合，均支持初始容量构造。

当前未定义可选 crate 特性，默认特性集为空。

## 设计思路

全部公开接口与内部流式逻辑集中在 `src/lib.rs`，以函数与类型而非子模块划分职责。

### 直接哈希与泛型哈希

`fast_hash*` 与 `hash128*` 委托给 `gxhash::gxhash64` 或 `gxhash::gxhash128`。`fast_hash_u64` 先将整数转为小端字节。`hash128` 先将两个种子各自经双射 `mix13` 打散，再异或错位合并，结构化种子对无法产生代数碰撞，仅剩 128→64 位固有的生日界随机碰撞。

`hash_value*` 创建带种子的 `GxHasher`，经 `Hash::hash` 写入后调用 `Hasher::finish`。该路径遵循类型的 `Hash` 实现，而非规范的字节序列化。

### 流式路径

1. `new` 或 `with_seed` 初始化 4 条折叠链。零种子状态编译期预计算；其他种子与各链盐值混合。
2. `write` 补满缓冲残留，折叠完整 64 字节条带，并保留尾部字节。条带号对 4 取模决定链归属，与写入边界无关。
3. 对齐的 4 条带组更新独立折叠链。完整输入条带直接读取，不拷贝进残留缓冲。
4. `finish` 依序汇合各链状态，再对残留字节与按小端编码的累计字节数做末端混合，不改动内部状态。
5. `reset` 恢复构造时种子对应的初态并清零计数器，不清零残留缓冲。

`compute_checksum*` 构造该状态、写入全部输入并收尾。处理耗时随输入线性增长，辅助内存恒定。

### 集合路径

普通集合构造函数安装 `GxBuildHasher::default()`。并发构造函数以同一构建器配置 papaya，可指定初始容量。后端默认随机化集合哈希，除非通过 Cargo 特性统一启用依赖的 `deterministic` 特性。

### 兼容性边界

固定种子的字节哈希在相同算法配置下可复现。落盘或传输的哈希应记录后端版本、种子与算法选择；不要假设跨后端或流式实现变更仍兼容。

泛型 `Hash` 输入不保证跨平台或跨编译器版本可移植。持久化键请先显式编码再做字节哈希。`hash128` 的种子对仅在 128→64 位归约固有的生日界上碰撞，无代数结构。以上接口不重现 C# 实现的 MurmurHash 或 XxHash 输出。

## 技术堆栈

| 组件                                        | 作用                                       |
| ------------------------------------------- | ------------------------------------------ |
| Rust 2024、`core::hash`、`std::collections` | 哈希 trait、集合接口、编译期状态与布局校验 |
| gxhash 3.5.0                                | AES/SIMD 字节哈希、哈希器与普通集合别名    |
| papaya 0.2.5                                | 并发映射与集合，守卫式访问                 |
| aok、ctor、log、log_init                    | 测试结果与日志初始化                       |
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

`tests/main.rs` 覆盖集合操作、并发插入、哈希分布、带种子哈希、流式边界、复位、克隆与超长输入。`README.mdt` 包含两语言源文档；请编辑源文档后重新生成合并的 README。

## API 说明

以下接口均位于 `whasher` crate 根。参数 `bytes` 或 `data` 借用输入切片；哈希函数直接返回数值，不返回 `Result`。

### 哈希函数

| 签名                                                                  | 行为                                     |
| --------------------------------------------------------------------- | ---------------------------------------- |
| `fast_hash(bytes: &[u8]) -> u64`                                      | 种子为 0 的直接 64 位字节哈希。          |
| `fast_hash_u64(val: u64) -> u64`                                      | 等价于 `fast_hash(&val.to_le_bytes())`。 |
| `fast_hash_with_seed(bytes: &[u8], seed: u64) -> u64`                 | 显式种子的直接 64 位字节哈希。           |
| `fast_hash128(bytes: &[u8]) -> u128`                                  | 种子为 0 的直接 128 位字节哈希。         |
| `hash128(bytes: &[u8], seed_a: u64, seed_b: u64) -> u128`             | 双种子经 `mix13` 打散后异或错位合并。   |
| `hash128_with_seed(bytes: &[u8], seed: u64) -> u128`                  | 显式种子的直接 128 位字节哈希。          |
| `hash_value<T: Hash + ?Sized>(value: &T) -> u64`                      | 经 `GxHasher::with_seed(0)` 哈希该值。   |
| `hash_value_with_seed<T: Hash + ?Sized>(value: &T, seed: u64) -> u64` | 显式种子的泛型哈希，支持不定长输入。     |
| `compute_checksum(data: &[u8]) -> u64`                                | 种子为 0 的整段输入流式校验和。          |
| `compute_checksum_with_seed(data: &[u8], seed: u64) -> u64`           | 显式种子的整段输入流式校验和。           |

显式 `u64` 种子由后端按 `i64` 位型解释，最高位保留。空切片为合法输入。

### StreamHasher

流式校验和状态，字段私有。实现 `Clone`、`Debug`、`Default` 与 `core::hash::Hasher`。克隆保留当前状态，之后可独立推进或复位。固有方法 `write` 与 `finish` 无需导入 trait。

| 方法                                      | 行为                                       |
| ----------------------------------------- | ------------------------------------------ |
| `const new() -> Self`                     | 创建种子为 0 的空状态，`Default` 亦采用。  |
| `const with_seed(seed: u64) -> Self`      | 创建指定种子的空状态。                     |
| `write(&mut self, bytes: &[u8])`          | 追加字节；空输入不改动状态。               |
| `finish(&self) -> u64`                    | 返回校验和而不复位，之后仍可继续写入。     |
| `reset(&mut self)`                        | 恢复构造种子对应的空状态；不擦除缓冲字节。 |
| `const total_bytes_written(&self) -> u64` | 返回累计字节数，采用 `u64` 环绕加法。      |
| `const is_empty(&self) -> bool`           | 判断字节计数是否为零。                     |

分块恒等针对相同拼接字节上的 `write(&[u8])` 调用，并不使任意 `Hash` 实现成为可移植编码。字节计数按 2^64 取模环绕，不宜用于统计更长的生命周期总量。

### 集合类型与重导出

| 导出                 | 定义或用途                                                                                                              |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `HashMap<K, V>`      | `std::collections::HashMap<K, V, GxBuildHasher>`，自 gxhash 重导出。                                                    |
| `HashSet<T>`         | `std::collections::HashSet<T, GxBuildHasher>`，自 gxhash 重导出。                                                       |
| `Entry<'a, K, V>`    | 标准映射条目枚举，含 `Occupied` 与 `Vacant` 变体。                                                                      |
| `GxHasher`           | 后端 `Hasher`，除 trait 方法外支持 `with_seed(i64)` 与 `finish_u128(&self) -> u128`。非 `StreamHasher` 的分块恒等替代。 |
| `GxBuildHasher`      | 后端 `BuildHasher`，支持 `default()` 与 `with_seed(i64)`。默认实例用于随机化集合哈希。                                  |
| `HashMapExt`         | 导入后为映射别名提供 `new()` 与 `with_capacity(usize)`。                                                                |
| `HashSetExt`         | 导入后为集合别名提供 `new()` 与 `with_capacity(usize)`。                                                                |
| `GxPapayaMap<K, V>`  | `papaya::HashMap<K, V, GxBuildHasher>`，经 `pin()` 守卫式操作。                                                         |
| `GxPapayaSet<T>`     | `papaya::HashSet<T, GxBuildHasher>`，经 `pin()` 守卫式操作。                                                            |
| `papaya`             | 重导出的依赖，以 `whasher::papaya` 访问，其自身类型保留上游默认。                                                       |

### 集合构造函数

| 签名                                                                   | 行为                         |
| ---------------------------------------------------------------------- | ---------------------------- |
| `new_hash_map<K, V>() -> HashMap<K, V>`                                | 创建空普通映射。             |
| `hash_map_with_capacity<K, V>(capacity: usize) -> HashMap<K, V>`       | 创建带初始容量的空普通映射。 |
| `new_hash_set<T>() -> HashSet<T>`                                      | 创建空普通集合。             |
| `hash_set_with_capacity<T>(capacity: usize) -> HashSet<T>`             | 创建带初始容量的空普通集合。 |
| `new_papaya_map<K, V>() -> GxPapayaMap<K, V>`                          | 创建空并发映射。             |
| `papaya_map_with_capacity<K, V>(capacity: usize) -> GxPapayaMap<K, V>` | 创建带初始容量的空并发映射。 |
| `new_papaya_set<T>() -> GxPapayaSet<T>`                                | 创建空并发集合。             |
| `papaya_set_with_capacity<T>(capacity: usize) -> GxPapayaSet<T>`       | 创建带初始容量的空并发集合。 |

构造函数不施加键 trait 约束。插入与查找要求底层集合的 `Hash` 与 `Eq` 约束；跨线程共享还需满足相应的 `Send` 与 `Sync` 约束。容量是初始分配提示，并非大小上限。

## 验证

在仓库工作区内于包目录执行：

```sh
./test.sh
bun x mdt
```

测试脚本调用 `cargo nextest run --all-features --no-capture`，需要 Cargo Nextest。文档生成需要 Bun。包继承工作区 lint 设置。
