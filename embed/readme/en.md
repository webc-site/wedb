# WeDB Base Module Mapping Reference

This repository (`wedb_base`) provides the foundational high-performance storage engine for WeDB. It is a full-stack Rust rewrite based on the `compio` asynchronous runtime (Linux io_uring / Windows IOCP / macOS kqueue) of Microsoft [Microsoft Garnet](https://github.com/microsoft/garnet)'s C# storage engine (Tsavorite / FASTER) and BfTree.

---

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
    - [`RangeIndexManager.Locking.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs)
    - [`RangeIndexManager.Migration.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs)
    - [`RangeIndexManager.Replication.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs)
    - [`RangeIndexChunkedSerializer.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs)
    - [`RangeIndexChunkedDeserializer.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs)

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
    - [`RangeIndexOps.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/Storage/Session/MainStore/RangeIndexOps.cs)
    - [`GarnetRecordTriggers.cs`](https://github.com/microsoft/garnet/blob/main/libs/server/Storage/Functions/GarnetRecordTriggers.cs)
