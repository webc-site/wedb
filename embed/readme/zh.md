# WeDB Base : 以 Rust 全栈重写微软 Garnet 存储引擎

WeDB Base 是 [WeDB](https://github.com/webc-site/wedb) 的存储引擎底座。以 Rust 重写微软 [Garnet](https://github.com/microsoft/garnet) 的 C# 存储核心——Tsavorite 混合日志、无锁哈希索引、CPR 检查点、槽位复活、日志紧缩——以及 BfTree 范围索引，拆分为十七个职责单一的 crate，运行于 `compio` 异步运行时（Linux io_uring、Windows IOCP、macOS kqueue）。

- [功能介绍](#功能介绍)
- [使用演示](#使用演示)
- [特性介绍](#特性介绍)
- [设计思路](#设计思路)
- [技术堆栈](#技术堆栈)
- [目录结构](#目录结构)
- [API 说明](#api-说明)
  - [wkv —— 顶层引擎](#wkv-顶层引擎)
  - [wbase —— L0 原语](#wbase-l0-原语)
  - [wutil —— 公共工具与缓冲池](#wutil-公共工具与缓冲池)
  - [wram —— 直接虚拟内存](#wram-直接虚拟内存)
  - [whasher —— 哈希与并发字典](#whasher-哈希与并发字典)
  - [wepoch —— 纪元保护](#wepoch-纪元保护)
  - [wdev —— 异步设备](#wdev-异步设备)
  - [wrecord —— 记录格式](#wrecord-记录格式)
  - [wval —— 值层](#wval-值层)
  - [windex —— 无锁哈希索引](#windex-无锁哈希索引)
  - [whlog —— 混合日志分配器](#whlog-混合日志分配器)
  - [wreviv —— 空闲槽位回收](#wreviv-空闲槽位回收)
  - [wbftree —— BfTree 范围索引](#wbftree-bftree-范围索引)
  - [wcompact —— 日志紧缩](#wcompact-日志紧缩)
  - [wcpr —— CPR 检查点](#wcpr-cpr-检查点)
  - [wsync —— Tsavorite 并发原语](#wsync-tsavorite-并发原语)
  - [wobject —— 记录式对象层](#wobject-记录式对象层)

## 功能介绍

工作区分层交付整套存储栈。底层 `wbase` 提供缓存行安全原语：48 位日志寻址、扇区对齐运算、自适应退避、TLS 线程标识。`wram` 管理直接虚拟内存与原生内存追踪，`wutil` 承载扇区对齐缓冲池（对标 Tsavorite `core/Utilities` 底层位）与 libs/common 工具。`whasher` 封装 AES 加速 GxHash、四链并行流式校验和与 Papaya 无锁并发字典。`wepoch` 提供纪元保护，支撑安全内存回收。`wdev` 基于 `compio` 抽象异步块设备。`wsync` 承载 Tsavorite 并发原语（读优化锁、单写多读锁、旋转门 / 领袖屏障、计数事件）。

核心层对标 Tsavorite。`wrecord` 定义 16 字节记录头与零拷贝记录视图。`windex` 实现 64 字节对齐的无锁哈希索引，含溢出桶池与桶级并发守卫。`whlog` 实现混合日志分配器与三区滑动窗口（可变 / 只读 / 磁盘）。`wreviv` 回收已删记录槽位。`wval` 叠加 Redis 值层：多租户命名空间编码、集合元数据、hash / set / zset 紧凑编解码。`wobject` 提供记录式对象层——基于并发索引的内存 Hash / Set / List / SortedSet 对象，供服务端层消费。

服务层编排核心模块。`wcpr` 驱动 CPR 检查点。`wcompact` 紧缩只读日志段并物理截断回收段文件。`wbftree` 管理基于 BfTree 的有序范围索引。`wkv` 把上述能力聚合为 `WedbStore` 单机引擎，提供存储会话、记录级 TTL、后台 GC、读缓存、检查点恢复与范围索引操作。

## 使用演示

在分段文件设备上打开存储，经会话执行 CRUD。测试用 `aok::Void` 表达错误；生产代码直接映射 `wkv::Result`。

```rust
use std::sync::Arc;

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};

let rt = Runtime::new()?;
rt.block_on(async {
  // 自动探测宿主机推导配置，或显式指定：
  // 索引桶数、页大小、页数、可变区占比
  let config = StoreConfig::new(2048, 64 * 1024, 16, 0.5)?;
  let device = Arc::new(SegmentedDevice::single_file("demo.db")?);
  let store = Arc::new(WedbStore::open(config, device)?);

  let session = store.new_session()?;

  // 写入、读取、删除
  let addr = session.upsert(b"user:1001", b"alice").await?;
  assert!(addr > 0);
  assert_eq!(session.read(b"user:1001").await?, Some(b"alice".to_vec()));
  assert!(session.delete(b"user:1001").await?);

  // 内存可变区刷盘
  store.flush_all().await?;

  Ok::<(), wkv::Error>(())
})?;
```

记录级 TTL 对齐 Redis `EXPIREAT` / `PERSIST` 返回码语义：-2 键不存在、0 条件不满足、1 设置成功、2 已过期并立即物理删除。过期键读取时惰性消失；后台 GC 提前物理清除。

```rust
use wbase::time::now_ms;
use wkv::TtlOpt;

assert_eq!(session.expire_at(b"session:42", now_ms() + 60_000, TtlOpt::NONE).await?, 1);
assert_eq!(session.ttl_of(b"session:42").await?, Some(now_ms() + 60_000));
assert_eq!(session.persist(b"session:42").await?, 1);
```

经 CPR 做检查点与崩溃恢复。`FoldOver` 恢复后只读地址精确对齐尾地址，全部历史记录封印只读。恢复容量完全由持久化 `StoreMeta` 决定，绝不静默缩表。

```rust
use std::sync::Arc;

use compio::runtime::Runtime;
use wcpr::CheckpointType;
use wdev::SegmentedDevice;
use wkv::{CheckpointManager, StoreConfig, WedbStore};

let rt = Runtime::new()?;
rt.block_on(async {
  let ckpt_dir = std::path::Path::new("checkpoints");

  // 1. 写入数据、创建 FoldOver 检查点、随后释放存储实例
  let token;
  {
    let device = Arc::new(SegmentedDevice::single_file("demo.db")?);
    let store = Arc::new(WedbStore::open(StoreConfig::default(), device)?);
    let session = store.new_session()?;
    for i in 0..1000 {
      let k = format!("user_click:{i:05}");
      let v = format!("click_count_{i}");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }
    let meta = CheckpointManager::new()
      .create_checkpoint(&store, ckpt_dir, CheckpointType::FoldOver)
      .await?;
    token = meta.token;
  } // 此处模拟进程退出

  // 2. 在全新引擎上恢复并校验
  let device = Arc::new(SegmentedDevice::single_file("demo.db")?);
  let recovered = Arc::new(CheckpointManager::recover(ckpt_dir, token, device).await?);
  assert_eq!(recovered.entry_count(), 1000);

  Ok::<(), wkv::Error>(())
})?;
```

`open_shared` 在 `config.gc.enabled` 时自动拉起后台 GC，运行期免重启热调参，亦可手动驱动单轮。

```rust
store.start_gc();
store.update_gc_config(|gc| {
  gc.scan_interval_ms = 60_000;
  gc.compaction_interval_ms = 300_000;
});
let stats: wkv::GcStatsSnapshot = store.gc_handle().unwrap().stats();
```

紧缩把存活记录前移，物理删除回收后的段文件。

```rust
use wcompact::{CompactionType, LogCompactor};

let stats = LogCompactor::new(Arc::clone(&store))
  .compact(2 * segment_size, CompactionType::Scan)
  .await?;
assert_eq!(store.begin_address(), stats.new_begin_address);
```

范围索引服务大规模有序集合的有序扫描与闭区间范围查询，数据落 BfTree，主日志内仅存 35 字节定长桩。

```rust
use wbftree::{ScanReturnField, StorageBackend, TreeTuning};

session
  .range_index_create(b"leaderboard", StorageBackend::Std, TreeTuning::default())
  .await?;
session.range_index_set(b"leaderboard", b"score:alice", b"9800").await?;
let records = session
  .range_index_scan(b"leaderboard", b"", 10, ScanReturnField::KeyAndValue)
  .await?;
```

## 特性介绍

- 混合日志内存磁盘二象性：可变尾区在内存中服务读与原位更新；封印页经扇区对齐 I/O 管道刷盘。
- 无锁哈希索引：定容 64 字节桶、溢出桶池、共享 / 独占桶守卫下的 CAS 槽位更新，热路径无 rehash。
- CPR 检查点：`FoldOver` 封印历史只读；`Snapshot` 恢复时重建可变区，无需独立快照文件。Token 大小序即版本序，墙钟回拨不破坏单调性。
- 槽位复活：已删槽位按尺寸分桶回收（First-Fit / Best-Fit），原位复用，抑制分配与日志增长。
- BfTree 范围索引：多树注册表、惰性恢复、分块迁移协议、与主日志共享写屏障。
- Redis 语义 TTL：`expire_at` / `persist` 返回码对齐 Redis 7.4，支持 NX / XX / GT / LT 条件，读路径惰性过期加定时物理清除。
- 零拷贝纪律：`RecordRef` / `RecordMut` 视图、`fast_key_eq` SIMD 键比较、栈上键缓冲，热路径免分配。
- 崩溃一致设备：`SegmentedDevice` 强制父目录 fsync，掉电后文件创建不丢失。

## 设计思路

模块按严格单向依赖分层，每层只导入所需依赖。

```mermaid
graph TD
  subgraph engine[引擎层]
    wkv[wkv WedbStore]
  end
  subgraph services[服务层]
    wcpr[wcpr CPR 检查点]
    wcompact[wcompact 日志紧缩]
    wbftree[wbftree BfTree 范围索引]
  end
  subgraph core[核心层]
    whlog[whlog 混合日志]
    windex[windex 哈希索引]
    wreviv[wreviv 空闲槽位池]
    wval[wval 值编解码]
    wobject[wobject 记录式对象层]
  end
  subgraph foundation[基础层]
    wrecord[wrecord 记录格式]
    wdev[wdev 异步设备]
    wepoch[wepoch 纪元保护]
    wsync[wsync 并发原语]
    whasher[whasher 哈希与并发字典]
    wutil[wutil 缓冲池与工具]
    wram[wram 直接虚拟内存]
    wbase[wbase L0 原语]
  end

  wkv --> wcpr
  wkv --> wcompact
  wkv --> wbftree
  wkv --> whlog
  wkv --> windex
  wkv --> wreviv
  wkv --> wval
  wobject --> whasher
  wcompact --> whlog
  wcompact --> windex
  wcompact --> wval
  wcompact --> wrecord
  wcpr --> whlog
  wcpr --> windex
  wbftree --> whasher
  whlog --> wrecord
  whlog --> wdev
  whlog --> wepoch
  windex --> whasher
  windex --> wram
  wval --> wrecord
  wdev --> wutil
  whlog --> wutil
  wram --> wutil
  wutil --> wbase
  wepoch --> whasher
  wram --> wbase
  wrecord --> wbase
```

写入路径从会话到磁盘：定位哈希标签，在混合日志可变区占位（启用时优先复活空闲槽位），页进入环形缓冲暂存，封印页在纪元保护下刷入设备。GC 与检查点作为旁路通道复用同一设备。

```mermaid
graph TD
  upsert[session.upsert] --> tag[HashIndex 定位标签]
  tag -->|命中可变区| inplace[原位修改或 RCU 追加]
  tag -->|未命中| reviv[FreeRecordPool 认领空闲槽位]
  reviv --> alloc[HybridLog 追加]
  inplace --> staged
  alloc --> staged[CircularPageBuffer 暂存页]
  staged -->|刷盘| dev[SegmentedDevice 扇区写]
  dev --> disk[(磁盘)]
  epoch[LightEpoch] -. 保护 .-> tag
  epoch -. 保护 .-> staged
  gc[GcManager] -->|过期扫描| compactor[LogCompactor]
  compactor -->|拷贝存活记录| dev
  ckpt[CheckpointManager] -->|CPR 快照| dev
```

## 技术堆栈

- 运行时：`compio`——Linux io_uring、Windows IOCP、macOS kqueue。
- 哈希：`gxhash` AES 加速后端；`crc32fast` 校验和。
- 并发：`papaya` 无锁字典、`parking_lot` 条带锁。
- 序列化：`bitcode` 二进制编解码、`sonic-rs` 检查点元数据、`itoa` / `zmij` 数字格式化。
- 有序索引：`bf-tree` 块级 BfTree。
- 错误与枚举：`thiserror`、`strum`。
- 可观测：`log` 门面，测试配 `log_init`。

## 目录结构

```text
embed/
  wbase/     L0 原语：寻址、对齐、退避、变长整型、glob、TLS 线程标识
  wutil/     扇区对齐缓冲池（Origin-Return）、对齐原语再导出、libs/common 工具
  wram/      直接虚拟内存、原生内存追踪（缓冲池兼容再导出自 wutil）
  whasher/   GxHash 后端、流式校验和、Papaya 并发字典
  wepoch/    LightEpoch 纪元保护与条目表
  wdev/      compio Device trait、SegmentedDevice、NullDevice、fsync 契约
  wsync/     Tsavorite 并发原语：ReadOptimizedLock、SingleWriterMultiReaderLock、屏障、信号量
  wrecord/   16B 记录头、零拷贝视图、分块框架、SIMD 键比较
  wval/      命名空间与会话键编码、集合元数据、紧凑编解码、glob、TTL
  windex/    无锁哈希索引、溢出桶池、桶守卫
  whlog/     HybridLog 分配器、地址管理、页缓冲、扫描迭代器
  wreviv/    空闲记录池、按尺寸分桶复活
  wbftree/   BfTreeService、RangeIndexManager、分块迁移、35B 桩
  wcompact/  LogCompactor、紧缩会话 trait、紧缩统计
  wcpr/      CPR 检查点状态机、索引检查点读写、元数据格式
  wkv/       WedbStore、会话、TTL、GC、读缓存、恢复编排
  wobject/   记录式对象层：内存 Hash / Set / List / SortedSet 对象
  example/   工作区模板与测试脚手架（不发布）
  sh/        开发与发布脚本
  test.sh    全特性 cargo nextest 入口
```

## API 说明

### wkv —— 顶层引擎

- `WedbStore<D: Device>`——聚合哈希索引、混合日志、纪元、设备、BfTree、范围索引、复活池与读缓存的引擎。核心方法：
  - `WedbStore::open(config, device)` / `open_shared`——构建引擎；`open_shared` 在 `config.gc.enabled` 时幂等拉起 GC。索引容量在生命周期内定容。
  - `new_session()`——注册参与者并返回 `StoreSession<D>`。
  - `flush_all()` / `flush_and_evict_all()`——可变页刷盘，可选逐出内存。
  - `create_checkpoint(dir, CheckpointType)`、`create_checkpoint_with_token(dir, type, token)`、`WedbStore::recover(dir, token, device)`、`recover_latest(dir, device)`——免建管理器的检查点快捷入口。
  - `compact(until_address, CompactionType)`、`compact_with_filter`、`compact_lazy(max_seek_bytes)`、`compactor()`——引擎内置紧缩入口。
  - `start_gc()`、`update_gc_config(f)`、`gc_config()`、`gc_handle()`——后台 GC 生命周期与热更新；驱动循环每轮重读配置。
  - `set_write_listener` / `set_range_listener`——以端口注入 AOF / 复制适配器。
  - 地址观测：`tail_address`、`read_only_address`、`head_address`、`begin_address`、`shift_read_only_address`、`shift_head_address`、`shift_begin_address`、`truncate`。
  - `keyspace_stats()`——经池化扫描会话统计存活 / 过期键。
  - `scan_range_callback(start, end, on_record)`——主日志有序范围扫描。
  - `entry_count()`、`hlog()`、`bftree()`、`expired_key_deletion_scan`。
- `StoreConfig`——索引桶数、页大小、页数、可变区占比、最大会话数、BfTree 路径、范围索引目录、复活 / 读缓存开关、`GcConfig`。构造器：`auto()`、`auto_with_budget(bytes)`、`new(...)`、`minimal()`、`recommended_index_size(expected_keys)`；建造器 `with_max_sessions`、`with_revivification`、`with_read_cache`、`with_read_cache_pages`、`with_bftree_path`、`with_range_index_dir`、`with_gc`。
- `StoreSession<D>`——会话级操作：
  - `upsert` / `read` / `delete` / `contains_key` / `read_batch_with`——带标签用户键 CRUD；`upsert_raw` / `read_raw` / `read_raw_with` / `delete_raw` / `contains_key_raw` / `read_record(addr)` 直接操作物理键。
  - `try_read_in_memory`、`try_modify_in_place`、`try_modify_with_slack`、`try_upsert_sync`、`try_read_sync`、`try_read_batch_in_memory`——跳过刷盘等待的快路径；`*_unprotected` 变体服务批处理上下文。
  - `expire_at(key, ms, TtlOpt)` / `persist(key)` / `ttl_of(key)`——Redis 语义记录级 TTL；`hexpire_at`、`hpersist`、`collect_expired_hash_fields`——哈希字段级 TTL。
  - `set_context(ns, db)`、`set_namespace`、`set_active_db`——多租户路由；`session_prefix()` 输出 19 字节零分配前缀。
  - `set_copy_reads_to_tail`、`set_record_elision`——对标 Garnet 的冷读提升与记录脱钩开关。
  - `enter_batch()`——`BatchStoreSession` 将写入聚合到同一纪元窗口。
  - `load_meta`、`save_meta`、`load_collection_raw_read`、`save_compact_meta`、`append_hash_field(s)_batch`、`append_set_member(s)_batch`——集合元数据与分块存储。
  - `range_index_create(key, StorageBackend, TreeTuning)` / `set` / `get` / `del` / `scan(key, start, count, ScanReturnField)` / `scan_stream` / `range(key, start, end, field)` / `range_stream` / `exists` / `config` / `metrics` / `rename_range_index`——BfTree 范围索引操作。
- `CheckpointManager`——`create_checkpoint(store, dir, CheckpointType)`、`create_checkpoint_with_token`、`recover(dir, token, device) -> WedbStore`、`recover_latest`、`recover_store`、`list_checkpoints`、`find_latest_checkpoint`、`purge_checkpoint(dir, token)`、`purge_all_checkpoints`；另有 `take_cpr_snapshots` / `recover_cpr_snapshots`、`take_shared_bftree_snapshot` / `recover_shared_bftree`。
- GC 面：`GcManager`（`new`、`spawn`、`run_once`、`drive`）、`GcHandle`（`stats`、`stop`、`run_once`）、`GcStatsSnapshot`、`GcConfig`、`RunGuard`。
- 成员 crate 再导出：`LogCompactor`、`CompactSession`、`CompactStore`、`CompactionStats`、`CompactionType`、`CheckpointMeta`、`CheckpointType`、`CprRecover`、`CprStore`、`StoreMeta`、`BfTreeService`、`RangeIndexManager`、`RangeIndexStub`、`RangeIndexError`、`ScanRecord`、`ScanReturnField`、`StorageBackend`、`StorageBackendType`、`TreeTuning`、`StorageEncoding`、`TaggedKeyBuf`、`ReadCache`、`TtlOpt`、`TtlProbe`、`WriteListenerFn`、`RangeIndexListenerFn`。

### wbase —— L0 原语

按特性启用的模块，无 `full` 特性：`addr`（48 位 `LogAddress` 掩码）、`align`（64B 缓存行 / 扇区运算）、`backoff`（自适应重试状态机）、`base32`、`buf`、`crc`（`crc32fast`）、`float`（保序 f64 位模式）、`glob`、`simd`、`striped`（锁条带）、`thread`（TLS 线程标识）、`time`（`coarsetime` 助手、`now_ms`）、`varint`（OPPV 变长整型）。

### wutil —— 公共工具与缓冲池

- `BufferPool`——分级 Direct I/O 缓冲池（对标 Tsavorite `core/Utilities/BufferPool.OriginReturn.cs`），含容量分级、线程本地仓与 `PoolStats`；分级运算经 `class_of_sectors`、`class_capacity_bytes`、`NUM_CLASSES`。
- `AlignedBuf`——扇区对齐缓冲区（对标 `SectorAlignedMemory`），RAII 归还入池。
- `ascii` / `num` / `convert` / `crc64` / `hash` / `hash_slot`——garnet `libs/common` 工具面。

### wram —— 直接虚拟内存

- `DirectVirtualMemory`、`DirectVmBlock`、`system_page_size()`（对标 `core/Native/DirectVirtualMemory.cs`）。
- `NativeMemoryTracker`；对齐助手 `align_up` / `align_down` / `checked_align_up` / `is_aligned` / `SectorRange` 兼容再导出。
- `BufferPool` / `AlignedBuf` 自 wutil 兼容再导出（Allocator→Utilities 方向，与 C# 一致）。


- `BufferPool`——分级 Direct I/O 缓冲池，含容量分级、线程本地仓与 `PoolStats`；分级运算经 `class_of_sectors`、`class_capacity_bytes`、`NUM_CLASSES`。
- `AlignedBuf`、`DirectVirtualMemory`、`DirectVmBlock`、`system_page_size()`。
- `NativeMemoryTracker`；对齐助手 `align_up` / `align_down` / `checked_align_up` / `is_aligned` / `SectorRange`。

### whasher —— 哈希与并发字典

- `fast_hash(_with_seed)`、`fast_hash_u64`、`fast_hash128`、`hash128(_with_seed)`、`hash_value(_with_seed)`——GxHash 后端。
- `StreamHasher`——四链并行折叠，流式校验和 `write` / `finish` / `reset` / `total_bytes_written`。
- `compute_checksum(_with_seed)`——CRC 校验和；`mix13` / `splitmix64` / `mix_thread_id`——位混合器。
- 再导出 `gxhash` 的 `HashMap` / `HashSet`，以及 `GxPapayaMap` / `GxPapayaSet` 无锁并发集合与构造函数 `new_papaya_map`、`papaya_map_with_capacity`、`new_papaya_set`。

### wepoch —— 纪元保护

- `LightEpoch`——`register()`、`suspend` / `resume`、`protect_and_drain()`、`protected_scope()`、`bump_epoch` / `bump_current_epoch_action`、`drain()`、`safe_to_reclaim_epoch()`、`allocate_user_word()`。
- `Participant`、`EpochGuard`、`ProtectedScope`、`EpochEntry`、`MAX_USER_WORDS`。

### wdev —— 异步设备

- `Device` / `StorageDevice` trait——异步读 / 写 / 刷与段生命周期。
- `SegmentedDevice`——可增长分段文件（`single_file` 与 `segmented` 构造器）、`SegmentChunk` / `SegmentChunks`、`FileMap`。
- `NullDevice`——基准测试用丢弃设备。
- `sys::detect_system_memory` / `detect_cpu_cores`、`MAX_SEGMENT_SIZE`；再导出 `wbase::BufferPool`（Utilities 层原语）。

### wrecord —— 记录格式

- `RecordHeader` 常量——`HEADER_SIZE`（16B）、`SEALED_BIT`、`TOMBSTONE_BIT`、`READ_CACHE_BIT`、`MODIFIED_BIT`、`IN_NEW_VERSION_BIT`、`ADDRESS_MASK`、`MAX_FILLER_BYTES`。
- `RecordRef` / `RecordMut`——日志内存上的零拷贝读 / 写视图。
- `record_size`、`checked_record_size`、`encode_to_slice`、`try_encode_to_vec`——把记录编码进日志槽位。
- `ChunkCodec` / `ChunkIter`——长度前缀分块框架；`fast_key_eq`——SIMD 键比较。

### wval —— 值层

- `KeyTag`、`CollectionType`（`Hash` / `Set` / `ZSet` / `RangeIndex` 等）、`StorageEncoding`——带标签键方案。
- `NamespaceDbCodec`、`SessionPrefixBuf`、`TaggedKeyBuf`、`SubKeyCodec` / `SubKeyRef`、`DecodedSubKey`——基于 OPPV 变长整型的多租户命名空间、会话前缀与子键编码，栈缓冲常量 `STACK_KEY_CAP`、`MAX_SESSION_PREFIX_LEN`。
- `MetaValue` / `CompactMetaValue`——集合元数据；`CompactHash` / `CompactSet` / `CompactZSet` 编解码器与迭代器；zset 子键经 `ZScoreKeyRef` / `ZMemberKeyRef` 与保序 f64 编解码。
- `glob_match(_nocase)(_opt)`——Redis 风格 glob 匹配；`TtlCodec`——字段级 TTL 值；`sample_distinct_indices`——无重复抽样；`RecordValueExt` / `RecordValueMutExt`——把记录视图桥接回值层解析。

### windex —— 无锁哈希索引

- `HashIndex`——定容 64B 桶表；`find_tag` / `find_tag_by_hash`，经 `HashEntryInfo` 做 CAS 槽位更新，`acquire_keys_lock_exclusive` 串行化读改写窗口。
- `HashBucket`（`ENTRIES_PER_BUCKET`、`DATA_ENTRIES`、`OVERFLOW_INDEX`）、`HashBucketEntry`、`CandidateAddresses`（内联候选地址表，`retain` / `sort_descending`）。
- `OverflowPool`、`MultiBucketGuard`、`BucketExclusiveGuard` / `BucketSharedGuard`、`prefetch_read_l1`。

### whlog —— 混合日志分配器

- `HybridLog<D>`——`append`、`try_update_in_place`、`try_modify_record_in_place`、`try_modify_record_with_slack`、`try_revivify_in_chain`、区域推进（`shift_read_only_address`、`shift_head_address`、`shift_begin_address`）、`read_record` / `read_disk_record`、`flush_page(s_range)` / `flush_all` / `sync`、`iterate_version_chain`、`with_memory_record`、扫描与 `recover`。
- `HybridLogConfig`——页大小、页数、可变区占比默认值与 `ro_lag_num_from_fraction`。
- `AddressManager` / `AddressSnapshot`——逻辑 / 物理地址换算；`CircularPageBuffer`——暂存页环形缓冲；`PendingFlushList` / `PageFlushRange`——刷盘记账；`ScanIterator`、`RecordOutput`。

### wreviv —— 空闲槽位回收

- `FreeRecordPool`——按尺寸分桶（`DEFAULT_BIN_SIZES`、`DEFAULT_BIN_CAPACITY`）、`RevivAllocation`、`RevivStats`。
- `FreeRecordBin`、`FreeRecord`、`SetStatus`、`USE_FIRST_FIT`、`BEST_FIT_SCAN_ALL`。

### wbftree —— BfTree 范围索引

- `BfTreeService`——`new(BfTreeConfig)`、`open_disk(path, cb_min_record_size)` / `open_memory(...)`、`insert`、`read` / `read_into`、`delete`、`scan_with_count(_callback)`、`scan_with_end_key(_callback)`、`scan_all(_callback)`、`write_barrier()`、`cpr_snapshot(path)`、`recover_in_place(snapshot, work)`，结果类型 `BfTreeInsertResult` / `BfTreeReadResult` / `BfTreeDeleteResult`。
- `RangeIndexManager`——以 `key_id_of(key)` 为键的多树注册表、惰性恢复、检查点认领 / 释放、`RangeIndexLocks` 条带锁。
- `RangeIndexStub`（`RANGE_INDEX_STUB_SIZE` = 35B）、`RangeIndexChunkedSerializer` / `RangeIndexChunkedDeserializer` / `RangeIndexMigrationReader`、`compute_checksum(_with_seed)`。

### wcompact —— 日志紧缩

- `LogCompactor<S: CompactStore>`——`new(store)`、`with_cas_retries`、`compact(until_address, CompactionType)`、`compact_lazy(max_seek_bytes)`、`compact_with_filter`；紧缩后物理截断回收段文件。
- `CompactStore` / `CompactSession`——把紧缩器接入在役引擎的宿主 trait；`CompactionType`、`CompactionStats`。

### wcpr —— CPR 检查点

- `CprStore` / `CprRecover`——参与检查点 / 恢复的存储 trait 契约。
- `RecoveredCheckpoint<D>`——恢复出的 `CheckpointMeta` 与重建的 `HashIndex`、`HybridLog`、`LightEpoch`。
- `write_index_checkpoint` / `read_index_checkpoint_truncated`、`take_index_checkpoint`、`IndexCkptHeader`、`next_token`。
- `CheckpointManager`——`wkv` 包装之下的设备级管理器；`CheckpointMeta`、`CheckpointType`（`FoldOver` / `Snapshot`）、`StoreMeta`、`HlogMeta`、`IndexMeta`、文件命名助手。

### wsync —— Tsavorite 并发原语

- `ReadOptimizedLock` / `LockToken` / `LockType`、`SingleWriterMultiReaderLock`——对标 Tsavorite 的读写纪律与自旋重试环。
- `DoubleTurnstileBarrier`、`LeaderBarrier`、`CountingEventSlim`、`Semaphore`——屏障与信号原语。
- `ActiveWorkerMonitor`、`CooperativeDisposeGuard`——协作式停机记账。

### wobject —— 记录式对象层

- `HashObject` / `SetObject` / `ListObject` / `SortedSetObject`——基于无锁 `whasher::GxPapayaMap` / `GxPapayaSet` 索引的内存对象实现。
- `HashOperation`、`SetOperation`、`ListOperation`、`SortedSetOperation` 枚举、`SortedSetEntry`（分值/成员全序）、`OperationDirection`。
- bitcode `serialize` / `deserialize` 检查点载荷编解码。

