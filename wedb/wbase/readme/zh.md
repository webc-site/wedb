# wbase : 存储底座通用基础原语与常量库

- [核心定位](#核心定位)
- [模块划分与特性（按需启用，无 full）](#模块划分与特性按需启用无-full)
- [核心 API 与原语](#核心-api-与原语)
- [设计原则与对标](#设计原则与对标)

## 核心定位

`wbase` 是 WeDB 存储引擎的最底层（L0 级）公共基础原语库。抽离跨 crate 共享的核心常量与基础状态机，消除循环依赖与反向依赖，保障整个 workspace 拓扑单向无环。

所有功能模块均作为**细粒度可选特性（feature）**提供，**杜绝全量 `full` 特性**，使用方按需引入，零多余依赖与开销。

## 模块划分与特性（按需启用，无 full）

模块即特性，路径即 `wbase::<module>`；`ascii` / `heap` / `keyfmt` / `ns_prefix` 四者无条件编译。下表 32 模块对照 `src/lib.rs` 的 `pub mod`，职责取自各模块 `//!` 头。

| 特性名 | 模块路径 | 职责说明 | 外部依赖 |
| :--- | :--- | :--- | :--- |
| （无条件） | `wbase::ascii` | ASCII 规范化与折叠原语（对标 C# `ASCIIEncoding.GetString`） | 无（纯标准库） |
| `addr` | `wbase::addr` | 48 位逻辑/物理地址掩码、ReadCache 标记、`LogAddress` 强类型封装 | 无（纯标准库位运算） |
| `align` | `wbase::align` | 64B 缓存行、512B/4096B 扇区对齐校验与防溢出计算 | 无（纯标准库位运算） |
| `backoff` | `wbase::backoff` | 三阶自适应退避状态机（自旋 → yield 让核 → 微秒休眠） | 无（支持异步 reactor 让渡） |
| `base32` | `wbase::base32` | 零堆分配、保序小写 Base32（RFC 4648 Base32hex）编解码，面向快照与刷盘文件名 | 无（纯标准库） |
| `buf` | `wbase::buf` | 栈优先 / 堆回退双态字节缓冲（`StackHeapBuf` 常量泛型） | 无（纯标准库） |
| `cfg` | `wbase::cfg` | 跨 crate 基座配置：互不应依赖的消费方共用的判据（紧缩档位、逻辑库界限） | 无（纯标准库） |
| `convert` | `wbase::convert` | 数据原语换算（对标 `libs/common/ConvertUtils.cs`，时间面统一 coarsetime） | 级联 `time` |
| `crc` | `wbase::crc` | 高吞吐 CRC32 校验和（硬件指令加速），WAL / 检查点封签 / 段完整性单点 | `crc32fast` |
| `crc64` | `wbase::crc64` | CRC64 查表实现，逐位兼容 Garnet `Crc64.cs` | 无（纯标准库查表） |
| `endpoint` | `wbase::endpoint` | 套接字端点判定单源：Unix 域套接字路径形态与 typed 回环判定 | 无（纯标准库） |
| `error` | `wbase::error` | 公共错误类型定义 | `thiserror` |
| `future` | `wbase::future` | 异步协程协作原语与 Future 辅助（`block_on` 纯 park 驱动器） | 无（纯标准库） |
| `glob` | `wbase::glob` | 无分配 Glob 通配符匹配（非递归贪心 FSM，O(N) 典型复杂度，对齐 Redis 规范） | 无（纯标准库） |
| `group_commit` | `wbase::group_commit` | Group Commit 公共流水线骨架：协商 / Follower 登记 / Leader 级联循环 | `parking_lot`、`thiserror`、`crossfire` |
| `hash` | `wbase::hash` | 逐位兼容哈希算法库（精确移植 Garnet `HashUtils.cs`） | 无（纯标准库） |
| （无条件） | `wbase::heap` | 集合对象堆内存记账常量：`heap_memory_size` 加减运算唯一具名口径 | 无（纯标准库） |
| `hash_slot` | `wbase::hash_slot` | 集群槽位内核：`Slot = Mixer(namespace, active_db)` | `whasher` |
| `hex` | `wbase::hex` | 十六进制编解码微工具单点 | `fastrand` |
| （无条件） | `wbase::keyfmt` | 日志面键预览单点：错误臂键打印统一截断 | 无（纯标准库） |
| `map` | `wbase::map` | 并发字典与集合（`ConcurrentMap` / `ConcurrentSet`，`set` 特性与 `map` 共启本模块） | `papaya`、`gxhash`、`fastrand` |
| `num` | `wbase::num` | 严格数字语法解析与转换（对标 `NumUtils.cs`） | 无（纯标准库） |
| （无条件） | `wbase::ns_prefix` | 会话域隔离前缀编解码（多租户 ns + db 前缀，wedb 自有架构） | 无（纯标准库） |
| `pool` | `wbase::pool` | 扇区对齐缓冲池：Origin-Return 三级缓存、`AlignedBuf`、网络 `LimitedFixedBufferPool` | `parking_lot`、`compio-buf`、`gxhash`、`crossfire` |
| `primed` | `wbase::primed` | TLS 追加读缓冲空闲段初始化契约与记忆化清零缓冲 | `compio-buf` |
| `simd` | `wbase::simd` | SIMD 硬件向量化切片比对（键查找、版本链追溯、去重） | `fearless_simd` |
| `striped` | `wbase::striped` | 键哈希分段 / 条带读写锁（128B `CachePadded` 槽位） | `parking_lot`，级联 `align` |
| `thread` | `wbase::thread` | 高吞吐 TLS 全局唯一单调递增线程标识 `current_thread_id()` | 无（TLS 寄存器级访问） |
| `time` | `wbase::time` | 时间戳工具（coarsetime / VDSO，`now_ms` / `now_ticks`） | `coarsetime` |
| `store_type` | `wbase::store_type` | 存储面分类枚举（对标 `StoreType.cs:StoreType`） | `num_enum`、`strum` |
| `supervise` | `wbase::supervise` | 后台任务 panic 监督单点（`catch_unwind` 全仓一处封装） | `log`、`parking_lot` |
| `varint` | `wbase::varint` | 保序变长整数编解码原语（OPPV，面向有序复合键） | 无（纯标准库） |

## 核心 API 与原语

### 1. `addr`（48 位日志地址体系）

严格对标 C# Tsavorite / Garnet `LogAddress`：

- 常量：`ADDRESS_BITS = 48`，`ADDRESS_MASK = 0x0000_FFFF_FFFF_FFFF`（最大 256TB 寻址空间）。
- 读缓存标记：`READ_CACHE_BIT = 1 << 47`，`ABSOLUTE_ADDRESS_MASK = ADDRESS_MASK & !READ_CACHE_BIT`。
- 判据与转换：`is_valid`、`is_read_cache`、`to_absolute`、`with_read_cache`。
- 封装：`LogAddress` 结构体，提供紧凑无锁与格式化支持。

### 2. `align`（内存与扇区对齐）

- 扇区计算：`MIN_SECTOR_SIZE = 512`，`DEFAULT_SECTOR_SIZE = 4096`。
- 安全计算：`checked_align_up`（极值溢出返回 `None`）、`align_up`（溢出安全饱和）、`align_down`、`is_aligned`。

### 3. `backoff`（三阶自适应退避）

适配多核高并发与 compio thread-per-core 反应器模型：

- 第一阶段（< 32 轮）：`spin_loop()` 极短指令等待。
- 第二阶段（32..1024 轮）：`yield_now()` 操作系统级让渡时间片。
- 第三阶段（>= 1024 轮）：`sleep(50µs)` 同步微睡，或供异步调用方感知 `BackoffStage::Sleep` 后执行非阻塞 `compio::time::sleep().await`。

### 4. `thread`（高吞吐线程 ID）

- `current_thread_id() -> u64`：首访全局原子计数器单调自增（1 起步），后续由线程本地存储（TLS）纯寄存器读取（< 1ns，0 锁，0 原子操作）。
- 统一全局线程标识，天然免疫 ABA，消除模块各自维护 ID 造成的总线争用。

## 设计原则与对标

1. **零成本抽象与单一真源**：所有物理掩码在编译期直接折叠，不产生运行时损耗。
2. **compio 友好**：退避状态机显式暴露阶段，彻底消除异步上下文误调同步阻塞 sleep 冻结 reactor 的隐患。
3. **架构正交解耦**：上层模块如 `wrecord`、`windex`、`wreviv`、`wepoch` 按需引入特定特性，彻底消除平级相互穿透。
