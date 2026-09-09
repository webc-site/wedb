[English](#en) | [中文](#zh)

---

<a name="en"></a>

# WeDB Base Module Mapping Reference

This repository (`wedb_base`) provides the foundational high-performance storage engine for WeDB. It is a full-stack Rust rewrite based on the `compio` asynchronous runtime (Linux io_uring / Windows IOCP / macOS kqueue) of Microsoft [Microsoft Garnet](https://github.com/microsoft/garnet)'s C# storage engine (Tsavorite / FASTER) and BfTree.

---

- [1. Core Storage Engine Foundation](#1-core-storage-engine-foundation)

## 1. Core Storage Engine Foundation

- **`wbase`**
  - Responsibility: common foundational primitives and constants library (Layer-0 single source of truth) — 48-bit address masks & `LogAddress` (`addr`), 64B cacheline & sector alignment safe math (`align`), 3-stage adaptive backoff state machine (`backoff`), and high-throughput TLS thread identifier (`thread`); on-demand features only, no `full` feature
  - Corresponding Garnet Files:
    - [`LogAddress.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Common/LogAddress.cs)
    - [`Utility.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Utilities/Utility.cs)

- **`wram`**
  - Responsibility: 512B/4096B sector-aligned memory management, tiered Direct I/O buffer pool (`BufferPool`), direct virtual memory (`DirectVirtualMemory`), and native memory tracking
  - Corresponding Garnet Files:
    - [`BufferPool.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool.cs)
    - [`DirectVirtualMemory.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Native/DirectVirtualMemory.cs)
    - [`NativeMemoryTracker.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Native/NativeMemoryTracker.cs)

- **`whasher`**
  - Responsibility: AES hardware-accelerated GxHash backend (single-shot / seeded / 128-bit), streaming checksums (`StreamHasher`), and lock-free Papaya concurrent map/set
  - Corresponding Garnet Files:
    - [`HashUtils.cs`](https://github.com/microsoft/garnet/blob/main/libs/common/HashUtils.cs)
    - [`Utility.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Utilities/Utility.cs)

- **`wepoch`**
  - Responsibility: LightEpoch epoch protection, 64B cacheline-aligned lock-free entry table, and safe memory reclamation (SMR)
  - Corresponding Garnet Files:
    - [`LightEpoch.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs)
    - [`LightEpoch.EntryTable.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.EntryTable.cs)

- **`wdev`**
  - Responsibility: `compio`-powered asynchronous block device abstraction, segmented file device (`SegmentedDevice`), null device, and directory-entry persistence contract (parent-dir fsync)
  - Corresponding Garnet Files:
    - [`IDevice.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Device/IDevice.cs)
    - [`LocalStorageDevice.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs)
    - [`NullDevice.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Device/NullDevice.cs)

- **`wrecord`**
  - Responsibility: pure record-format layer — fixed 16B record headers (sealed / tombstone / filler bits), variable-length KV layout, zero-copy record views (`RecordRef` / `RecordMut`), chunk length-prefix framing, and SIMD key comparison; no value-layer semantics (whlog / windex depend only on this layer, mirroring Tsavorite core importing zero Garnet types)
  - Corresponding Garnet Files:
    - [`RecordInfo.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs)
    - [`IRecordTriggers.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/StoreFunctions/IRecordTriggers.cs)
    - [`ChunkedObjectSerializer.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Allocator/ObjectSerialization/ChunkedObjectSerializer.cs)

- **`wval`**
  - Responsibility: Redis value-layer encoding built on `wrecord` — multi-tenant namespace & session key encoding (OPPV varint), collection metadata (`MetaValue` / `SubKey`), compact hash / set / zset codecs, flattened zset subkey codec with order-preserving f64 scores, glob matching, distinct sampling, and `RecordValueExt` extension traits bridging record views back to value parsing; strict single-direction dependency `wval -> wrecord`, mirroring the Tsavorite core vs. Garnet `libs/server` value-object layering
  - Corresponding Garnet Files:
    - [`RecordNamespace.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Allocator/RecordNamespace.cs)
    - [`GlobUtils.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/GlobUtils.cs)
    - [`GarnetObjectBase.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/Objects/Types/GarnetObjectBase.cs)
    - [`GarnetObjectSerializer.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/Objects/Types/GarnetObjectSerializer.cs)

- **`windex`**
  - Responsibility: Tsavorite lock-free hash index, 64B cacheline-aligned hash buckets, overflow bucket pool, lock-free per-bucket concurrency guards (`BucketExclusiveGuard` / `BucketSharedGuard`), and index snapshot metadata
  - Corresponding Garnet Files:
    - [`HashBucket.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs)
    - [`HashBucketEntry.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucketEntry.cs)
    - [`OverflowPool.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Utilities/OverflowPool.cs)

- **`whlog`**
  - Responsibility: Tsavorite HybridLog allocator, circular staged page buffer, three-region sliding state machine (Mutable / ReadOnly / OnDisk), pending flush list, and scan iterator
  - Corresponding Garnet Files:
    - [`TsavoriteLogAllocator.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Allocator/TsavoriteLogAllocator.cs)
    - [`TsavoriteLogAllocatorImpl.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Allocator/TsavoriteLogAllocatorImpl.cs)
    - [`PendingFlushList.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Allocator/PendingFlushList.cs)
    - [`MallocFixedPageSize.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Allocator/MallocFixedPageSize.cs)

- **`wreviv`**
  - Responsibility: HybridLog bucketed free-slot revivification (First-Fit / Best-Fit), multi-size free-record bins, and success/failure statistics
  - Corresponding Garnet Files:
    - [`FreeRecordPool.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/FreeRecordPool.cs)
    - [`RevivificationManager.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationManager.cs)
    - [`RevivificationStats.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationStats.cs)

- **`wbftree`**
  - Responsibility: Bf-Tree range index service (`BfTreeService`), `RangeIndexManager` (multi-tree registry, lazy recovery, checkpoint / truncate / replication), chunked migration protocol, 35-byte `RangeIndexStub` in the main log, and cache-line striped `RangeIndexLocks`
  - Corresponding Garnet Files:
    - [`BfTreeService.cs`](https://github.com/microsoft/garnet/blob/main/libs/native/bftree-garnet/BfTreeService.cs)
    - [`RangeIndexManager.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/Resp/RangeIndex/RangeIndexManager.cs)
    - [`RangeIndexManager.Index.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs)
    - [`RangeIndexChunkedSerializer.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs)

- **`wcompact`**
  - Responsibility: HybridLog read-only segment compaction and garbage collection, with TTL-aware compaction session host
  - Corresponding Garnet Files:
    - [`TsavoriteCompaction.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs)
    - [`CompactionType.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Compaction/CompactionType.cs)
    - [`ICompactionFunctions.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Compaction/ICompactionFunctions.cs)

- **`wcpr`**
  - Responsibility: Concurrent Prefix Recovery (CPR) checkpoint state machine (VersionShift → Flush → CheckpointCompleted), index checkpoint read/write, and checkpoint metadata & integrity formats
  - Corresponding Garnet Files:
    - [`StateMachineDriver.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/StateMachineDriver.cs)
    - [`StateTransitions.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/StateTransitions.cs)
    - [`HybridLogCheckpointSM.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/HybridLogCheckpointSM.cs)
    - [`IndexCheckpointSM.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/IndexCheckpointSM.cs)
    - [`DeviceLogCommitCheckpointManager.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/CheckpointManagement/DeviceLogCommitCheckpointManager.cs)

- **`wkv`**
  - Responsibility: top-level single-node storage engine (`WedbStore`), store sessions (single-key & batch), record-level TTL, segment GC & compaction scheduling, read cache, CPR snapshot / BfTree recovery orchestration, Range Index lifecycle coordination, and RESP frame encoding for Range Index ops (WAL write-ahead and replication streams)
  - Corresponding Garnet Files:
    - [`Tsavorite.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs)
    - [`ClientSession.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/ClientSession/ClientSession.cs)
    - [`GarnetCheckpointManager.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/GarnetCheckpointManager.cs)
    - [`StoreWrapper.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/StoreWrapper.cs)
    - [`ReadCache.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs)

---

<a name="zh"></a>

# WeDB Base 模块映射对照表

本仓库（`wedb_base`）为 WeDB 高性能存储引擎底座，采用 Rust 与 `compio` 异步运行时（Linux io_uring / Windows IOCP / macOS kqueue）改写自微软 [Microsoft Garnet](https://github.com/microsoft/garnet) 的 C# 核心存储引擎（Tsavorite / FASTER）与 BfTree。

---

- [1. 核心存储引擎底座](#1-核心存储引擎底座)

## 1. 核心存储引擎底座

- **`wbase`**
  - 职责：通用基础原语与公共常量库（L0 级单一真源），提供 48 位逻辑/物理地址掩码与 `LogAddress` (`addr`)、64B 缓存行与扇区对齐常数与安全运算 (`align`)、多阶自适应退避状态机 (`backoff`)、高吞吐 TLS 全局唯一线程标识 (`thread`)；所有模块按需启用特性，无 `full` 特性
  - 对应 Garnet 的文件：
    - [`LogAddress.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Common/LogAddress.cs)
    - [`Utility.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Utilities/Utility.cs)

- **`wram`**
  - 职责：512B/4096B 扇区对齐内存管理、分级 Direct I/O 缓冲池 (`BufferPool`)、直接虚拟内存 (`DirectVirtualMemory`) 与原生内存追踪
  - 对应 Garnet 的文件：
    - [`BufferPool.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool.cs)
    - [`DirectVirtualMemory.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Native/DirectVirtualMemory.cs)
    - [`NativeMemoryTracker.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Native/NativeMemoryTracker.cs)

- **`whasher`**
  - 职责：AES 硬件加速 GxHash 后端（单次 / 带种子 / 128 位）、流式校验和 (`StreamHasher`) 与 Papaya 无锁并发字典/集合
  - 对应 Garnet 的文件：
    - [`HashUtils.cs`](https://github.com/microsoft/garnet/blob/main/libs/common/HashUtils.cs)
    - [`Utility.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Utilities/Utility.cs)

- **`wepoch`**
  - 职责：LightEpoch 纪元保护、64B Cacheline 对齐无锁 Entry 表与安全内存回收机制 (SMR)
  - 对应 Garnet 的文件：
    - [`LightEpoch.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs)
    - [`LightEpoch.EntryTable.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.EntryTable.cs)

- **`wdev`**
  - 职责：基于 `compio` 的异步块存储设备抽象、分段文件设备 (`SegmentedDevice`)、空设备与目录项持久化契约（父目录 fsync）
  - 对应 Garnet 的文件：
    - [`IDevice.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Device/IDevice.cs)
    - [`LocalStorageDevice.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs)
    - [`NullDevice.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Device/NullDevice.cs)

- **`wrecord`**
  - 职责：纯记录格式层——定长 16B 记录头（sealed / tombstone / filler 位）、变长键值布局、零拷贝记录视图 (`RecordRef` / `RecordMut`)、分块长度前缀框架与 SIMD 键比较；不感知任何值层语义（whlog / windex 仅依赖本层，对标 Tsavorite core 零 Garnet 类型导入）
  - 对应 Garnet 的文件：
    - [`RecordInfo.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs)
    - [`IRecordTriggers.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/StoreFunctions/IRecordTriggers.cs)
    - [`ChunkedObjectSerializer.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Allocator/ObjectSerialization/ChunkedObjectSerializer.cs)

- **`wval`**
  - 职责：构建于 `wrecord` 之上的 Redis 值层编码——多租户命名空间与会话键编码（OPPV 变长整型）、集合元数据 (`MetaValue` / `SubKey`)、hash/set/zset 紧凑编解码、保序 f64 分值的打平 zset 子键编解码、glob 匹配、无重复抽样，以及把记录视图桥接回值层解析的 `RecordValueExt` 扩展 trait；依赖严格单向 `wval -> wrecord`，对标 Tsavorite core 与 Garnet `libs/server` 值对象层的分层关系
  - 对应 Garnet 的文件：
    - [`RecordNamespace.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Allocator/RecordNamespace.cs)
    - [`GlobUtils.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/GlobUtils.cs)
    - [`GarnetObjectBase.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/Objects/Types/GarnetObjectBase.cs)
    - [`GarnetObjectSerializer.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/Objects/Types/GarnetObjectSerializer.cs)

- **`windex`**
  - 职责：Tsavorite 无锁哈希索引、64B 缓存行对齐哈希桶、溢出桶池、桶级无锁并发守卫 (`BucketExclusiveGuard` / `BucketSharedGuard`) 与索引快照元数据
  - 对应 Garnet 的文件：
    - [`HashBucket.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs)
    - [`HashBucketEntry.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucketEntry.cs)
    - [`OverflowPool.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Utilities/OverflowPool.cs)

- **`whlog`**
  - 职责：Tsavorite HybridLog 分配器、环形暂存页缓冲、三区滑动状态机 (Mutable/ReadOnly/OnDisk)、待刷盘列表与扫描迭代器
  - 对应 Garnet 的文件：
    - [`TsavoriteLogAllocator.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Allocator/TsavoriteLogAllocator.cs)
    - [`TsavoriteLogAllocatorImpl.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Allocator/TsavoriteLogAllocatorImpl.cs)
    - [`PendingFlushList.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Allocator/PendingFlushList.cs)
    - [`MallocFixedPageSize.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Allocator/MallocFixedPageSize.cs)

- **`wreviv`**
  - 职责：HybridLog 空闲槽位分桶复活回收（First-Fit / Best-Fit）、多尺寸空闲记录桶与成败统计
  - 对应 Garnet 的文件：
    - [`FreeRecordPool.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/FreeRecordPool.cs)
    - [`RevivificationManager.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationManager.cs)
    - [`RevivificationStats.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationStats.cs)

- **`wbftree`**
  - 职责：Bf-Tree 有序范围索引服务 (`BfTreeService`)、`RangeIndexManager`（多树注册表、惰性恢复、检查点/截断/复制枚举）、分块迁移流协议、主日志中 35 字节定长 `RangeIndexStub` 与缓存行条带化 `RangeIndexLocks`
  - 对应 Garnet 的文件：
    - [`BfTreeService.cs`](https://github.com/microsoft/garnet/blob/main/libs/native/bftree-garnet/BfTreeService.cs)
    - [`RangeIndexManager.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/Resp/RangeIndex/RangeIndexManager.cs)
    - [`RangeIndexManager.Index.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs)
    - [`RangeIndexChunkedSerializer.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs)

- **`wcompact`**
  - 职责：HybridLog 只读段整理与日志压缩 (Log Compaction)，含 TTL 感知的压缩会话宿主
  - 对应 Garnet 的文件：
    - [`TsavoriteCompaction.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs)
    - [`CompactionType.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Compaction/CompactionType.cs)
    - [`ICompactionFunctions.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Compaction/ICompactionFunctions.cs)

- **`wcpr`**
  - 职责：CPR 检查点机制 (Concurrent Prefix Recovery: VersionShift → Flush → CheckpointCompleted)、索引检查点读写与检查点元数据/完整性格式
  - 对应 Garnet 的文件：
    - [`StateMachineDriver.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/StateMachineDriver.cs)
    - [`StateTransitions.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/StateTransitions.cs)
    - [`HybridLogCheckpointSM.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/HybridLogCheckpointSM.cs)
    - [`IndexCheckpointSM.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/IndexCheckpointSM.cs)
    - [`DeviceLogCommitCheckpointManager.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/CheckpointManagement/DeviceLogCommitCheckpointManager.cs)

- **`wkv`**
  - 职责：顶层单机存储引擎 (`WedbStore`)、存储会话（单键与批量）、记录级 TTL、段 GC 与压缩调度、读缓存、CPR 快照/BfTree 恢复编排、范围索引生命周期协同中枢与 RangeIndex 操作的 RESP 帧编码（WAL 预写与复制流）
  - 对应 Garnet 的文件：
    - [`Tsavorite.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs)
    - [`ClientSession.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/ClientSession/ClientSession.cs)
    - [`GarnetCheckpointManager.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/GarnetCheckpointManager.cs)
    - [`StoreWrapper.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/StoreWrapper.cs)
    - [`ReadCache.cs`](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs)
