# whasher : 硬件加速哈希与流式校验和

- [项目介绍](#项目介绍)
- [使用演示](#使用演示)
  - [安装与目标要求](#安装与目标要求)
  - [键哈希](#键哈希)
  - [流式校验和](#流式校验和)
- [特性介绍](#特性介绍)
- [设计思路](#设计思路)
  - [输入域路由](#输入域路由)
  - [流式路径](#流式路径)
  - [种子域](#种子域)
  - [兼容性边界](#兼容性边界)
- [技术堆栈](#技术堆栈)
- [目录结构](#目录结构)
- [API 说明](#api-说明)
  - [哈希函数](#哈希函数)
  - [打散原语](#打散原语)
  - [StreamHasher](#streamhasher)
- [验证](#验证)

## 项目介绍

whasher 提供硬件加速的字节哈希（64 位与 128 位、支持显式种子）、分块恒等的流式校验和与 `u64` 双射打散原语，全部构建于 AES 向量加速的 `gxhash` 后端之上。

本库只做哈希。并发与哈希集合位于 `wbase::map`，Redis 协议逐位兼容的 MurmurHash2 位于 `wbase::hash`，全仓库级定槽的唯一真值源位于 `wbase::hash_slot`（`slot_of`），其混合内核消费本 crate 的 `GOLDEN_RATIO_64` 与双射 `mix13` 终末变换；以上三者本 crate 均刻意不暴露。

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

### 键哈希

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

`fast_hash` 是全仓通用字节串键哈希的单源（索引 tag、锁分段、迁移 Sketch）；`fast_hash_with_seed` 承载用于槽位域分离的带种子域；`hash128` 由双种子派生强抗碰撞的 128 位键 ID。

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

## 特性介绍

- 基于 AES 的 64 位与 128 位字节哈希，支持默认或显式种子。
- 分块恒等的流式校验和，非破坏性收尾，保留种子的复位。
- 零堆分配的流式状态：128 字节体积、64 字节对齐、64 字节残留缓冲。
- 默认流式种子编译期初始化，独立折叠链提升指令级并行。
- ≥ 32 KiB 输入的结构性碰撞安全路由（见[设计思路](#设计思路)）。
- 用于分段/分片的双射 `u64` 打散原语（`mix13`、`splitmix64`、`mix_thread_id`）。

当前未定义可选 crate 特性，默认特性集为空。

## 设计思路

全部公开接口与内部流式逻辑集中在 `src/lib.rs`，以函数与类型而非子模块划分职责。

### 输入域路由

gxhash 3.5.0 对大输入以 128 字节组 XOR 累加折叠、组计数器逐字节 mod 256 回绕，且长度仅以 `u32` 混入。≥ 32 KiB 时由此存在可构造的全态碰撞族（同相位组交换、32 KiB 整块置换、2^32 长度回绕）。私有的 `ONE_SHOT_MAX` 阈值因此把 ≥ 32 KiB 的 `fast_hash`、`fast_hash_with_seed`、`hash128` 调用全部路由到位置敏感的流式折叠族：种子经保留域常量偏置，全宽 `u64` 长度参与混合。低于阈值的输入保持单次硬件直算路径（小键约 0.31 ns）。`tests/main.rs` 从边界两侧锁定各碰撞族与路由归属。

### 流式路径

1. `new` 或 `with_seed` 初始化 4 条折叠链。零种子状态编译期预计算；其他种子与各链专属盐经双射 `mix13` 混合。
2. `write` 补满缓冲残留，折叠完整 64 字节条带，并保留尾部字节。条带号对 4 取模决定链归属，与写入边界无关。
3. 对齐的 4 条带组更新独立折叠链。完整输入条带直接读取，不拷贝进残留缓冲。
4. `finish` 依序汇合各链状态，再对残留字节与按小端编码的累计字节数做末端混合，不改动内部状态。
5. `reset` 恢复构造时种子对应的初态并清零计数器，不清零残留缓冲。

`compute_checksum*` 构造该状态、写入全部输入并收尾。处理耗时随输入线性增长，辅助内存恒定。

### 种子域

`fast_hash`（固定种子 0）、`fast_hash_with_seed`（显式种子，如命名空间派生的 Sketch 种子）与流式校验和族互为独立算法域：同一 `(输入, 种子)` 跨域必不同值。大输入键哈希路径以保留常量 `FAST_HASH_DOMAIN` 偏置流式种子，使路由后的两域同样保持分离。派生值不得跨域混用。

### 兼容性边界

固定种子的字节哈希在相同算法配置下可复现。落盘或传输的哈希应记录后端版本、种子与算法选择；不要假设跨后端或流式实现变更仍兼容。

泛型 `Hash` 输入不保证跨平台或跨编译器版本可移植。持久化键请先显式编码再做字节哈希。`hash128` 的种子对经非线性合并（`mix13` 后异或错位），结构化种子对无法产生代数碰撞，仅剩 128→64 位归约固有的生日界碰撞。以上接口不重现 C# 实现的 MurmurHash 或 XxHash 输出；Redis 协议逐位兼容的 MurmurHash2 由工作区 `wbase` crate 提供。

## 技术堆栈

| 组件                             | 作用                                                     |
| -------------------------------- | -------------------------------------------------------- |
| Rust 2024、`core::hash`          | 哈希 trait、编译期状态与布局校验                          |
| `gxhash` 3.5.0                   | 单次与流式两条路径共用的 AES/SIMD 字节哈希后端             |
| `aok`、`ctor`、`log_init`        | 测试结果断言与日志初始化                                   |
| Cargo Nextest                    | `test.sh` 使用的集成测试运行器                             |
| Bun 与 mdt                       | 由 README.mdt 与双语源文档生成 README.md                   |

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

`tests/main.rs` 是原语锁定集：编译期与运行时派生的种子 0 链初态恒等、流式分块一致性与单次校验和恒等、`ONE_SHOT_MAX` 结构性碰撞族与路由边界两侧钉死、键哈希族与校验和族之间的种子域分离。`README.mdt` 包含两语言源文档；请编辑源文档后重新生成合并的 README。

## API 说明

以下接口均位于 `whasher` crate 根。参数 `bytes` 或 `data` 借用输入切片；哈希函数直接返回数值，不返回 `Result`。空切片为合法输入。

### 哈希函数

| 签名                                                        | 行为                                                                                       |
| ----------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `fast_hash(bytes: &[u8]) -> u64`                            | 固定种子 0 的 64 位键哈希；≥ 32 KiB 路由到流式域。                                         |
| `fast_hash_i64(bytes: &[u8]) -> i64`                        | `fast_hash` 的补码 `i64` 位面视角；全仓唯一的此类重解释点。                                |
| `fast_hash_with_seed(bytes: &[u8], seed: u64) -> u64`       | 用于槽位域分离的带种子键哈希；输入域路由同上。                                              |
| `hash128(bytes: &[u8], seed_a: u64, seed_b: u64) -> u128`   | 128 位键 ID；双种子经 `mix13` 非线性合并；≥ 32 KiB 走双独立流式校验和。                     |
| `compute_checksum(data: &[u8]) -> u64`                      | 种子为 0 的整段输入流式校验和。                                                             |
| `compute_checksum_with_seed(data: &[u8], seed: u64) -> u64` | 显式种子的整段输入流式校验和。                                                              |

显式 `u64` 种子由后端按 `i64` 位型解释，最高位保留。

### 打散原语

| 导出                               | 行为                                                                         |
| ---------------------------------- | ------------------------------------------------------------------------------ |
| `GOLDEN_RATIO_64`                  | 64 位黄金分割常数（2^64 / φ）。                                              |
| `mix13(z: u64) -> u64`             | Stafford Variant 13 双射终末变换，无分支、无查表。                           |
| `splitmix64(z: u64) -> u64`        | SplitMix64 变换：`mix13(z.wrapping_add(GOLDEN_RATIO_64))`。                  |
| `mix_thread_id(tid: u64) -> usize` | 将线程 ID 或顺序序号打散为无偏条带槽位索引。                                 |

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

## 验证

在仓库工作区内于包目录执行：

```sh
./test.sh
bun x mdt
```

测试脚本调用 `cargo nextest run --all-features --no-capture`，需要 Cargo Nextest。文档生成需要 Bun。包继承工作区 lint 设置。
