[English](#en) | [中文](#zh)

---

<a name="en"></a>

# wbftree : Bf-Tree Range Index Service

- [Introduction](#introduction)
- [Module Layout](#module-layout)
- [Core API](#core-api)
- [Design Notes](#design-notes)
- [Test Coverage](#test-coverage)

## Introduction

wbftree provides the Rust service layer for the Bf-Tree ordered storage engine and the RangeIndex manager: single-tree lifecycle (`BfTreeService`), multi-tree registry (`RangeIndexManager`), the 35-byte fixed stub in the main log, and the chunked migration stream protocol. The underlying engine is the `bf-tree` crate.

## Module Layout

- `service`: `BfTreeService` single-tree lifecycle with point read / write / delete / scan / CPR snapshot / recovery; `WriteBarrierGuard`
- `manager`: `RangeIndexManager` multi-tree registry, lazy recovery, pre-stage, flush / checkpoint / truncate / replication enumeration, migration temp-path derivation; `RangeIndexLocks` key-hashed striped locks
- `chunk`: `RangeIndexChunkedSerializer` / `RangeIndexChunkedDeserializer` / `RangeIndexMigrationReader` chunked migration stream state machines
- `stub`: `RangeIndexStub`, the 35-byte fixed stub in the main store log, with in-place modification helpers
- `types`: `BfTreeConfig`, `TreeTuning`, `StorageBackendType` (Disk / Memory), read/insert/delete result codes, `ScanRecord`
- `error`: error types (Io / InvalidArgument / InvalidConfig / IndexExists / Snapshot / Recovery / Disposed / Timeout (30s drain limit) / ScanAborted / Corrupted)

## Core API

- `BfTreeService`: create / open / point read-write-delete / scan / cpr_snapshot / recovery; `write_barrier` returns a counting RAII guard
- `RangeIndexManager`: get_or_open_tree (lazy recovery), create / dispose / flush / checkpoint / truncate, replication enumeration and migration temp-path derivation, pre_stage_and_register_pending, recover_all_trees_from_checkpoint / recover_all_trees_from_dir
- `RangeIndexStub`: tree_handle 8B + cache_size 8B + min/max_record_size / max_key_len / leaf_page_size 4B×4 + backend / flags / serialization_phase 1B each, 35B total (`RANGE_INDEX_STUB_SIZE = 35`)
- Migration stream format: `[4B keyLen][key][8B fileBytes][file][8B checksum][4B stubLen][stub]`; `MIN_CHUNK_SIZE = 47`, `DEFAULT_MIGRATION_CHUNK_SIZE = 256KiB`
- Constants: `NUM_LOCK_STRIPES = 128`, `INDEX_SIZE_BYTES = 35`

## Design Notes

- thread-per-core contract: fully synchronous API, no runtime dependency; point read / write paths take zero striped locks (the engine's leaf latches ensure concurrency) with `Arc<BfTreeService>` shared across threads; only lifecycle changes (create / lazy recovery / unregister / delete) take `RangeIndexLocks` striped write locks; online references live in a papaya lock-free map — reader-pinned snapshots never block writers; tree deletion is deferred until `Arc` refcount reaches zero
- Snapshot write barrier: a counting barrier plus an in-flight writer count, both AtomicUsize paired via SeqCst store-buffering (Dekker); the counting barrier nests, writers block until the outermost guard drops, briefly backing off along a spin → yield → micro-sleep ladder — the tree stays write-quiescent and snapshots tear-free; draining beyond 30s raises `Error::Timeout`; holding the guard forbids await / same-thread I/O events
- Key semantics: keys are binary-safe zero-copy `&[u8]` throughout; the 128-bit key id derives from gxhash128 with a dedicated seed domain (digest domain isolated from user data), and the file-name prefix is its 26-char Base32 encoding
- Lazy recovery: get_or_open_tree first copies the flush snapshot (bare name preferred, else the highest address) onto the data file and restores via CPR snapshot when the magic (`BF-TREE-V0-BEGIN`) matches, otherwise rebuilds / reopens from the stub; on_flush copies data files of cold trees and sets the flushed bit
- Leaf page sizing: `max_record_size` ≤2KB takes 4096; otherwise 2.5× capped at 32768, rounded up to a power of two

## Test Coverage

tests/ covers: record capacity boundaries, disk reopen and CPR snapshot recovery, multi-reader multi-writer concurrency, nested barriers and tear-free snapshots; lock stripe count / alignment / contention, stub encoding and slice helpers, leaf_page_size derivation, manager lifecycle / checkpoint / truncate / replication enumeration / duplicate-create defense / strict flush filename parsing; chunked serialization roundtrip, cross-chunk boundaries and empty chunks, error-state termination, checksum corruption, streaming reads; interop lifecycle and disposal, zero-allocation point read/write/delete contract, scan counts / end keys / field selection / ordering, snapshot recovery roundtrip and corrupted-snapshot errors.


---

<a name="zh"></a>

# wbftree : Bf-Tree 范围索引服务

- [项目介绍](#项目介绍)
- [模块组成](#模块组成)
- [核心 API](#核心-api)
- [设计要点](#设计要点)
- [测试覆盖](#测试覆盖)

## 项目介绍

wbftree 提供块级有序存储引擎 Bf-Tree 的 Rust 服务层与 RangeIndex 管理器：单树生命周期（`BfTreeService`）、多树注册表（`RangeIndexManager`）、主日志中的 35 字节定长存根、迁移分块流协议。底层引擎由 `bf-tree` crate 承担。

## 模块组成

- `service`：`BfTreeService` 单树生命周期与点读 / 写入 / 删除 / 扫描 / CPR 快照 / 恢复；`WriteBarrierGuard`
- `manager`：`RangeIndexManager` 多树注册表、惰性恢复、Pre-Stage、刷盘 / 检查点 / 截断 / 复制枚举、迁移临时路径派生；`RangeIndexLocks` 键哈希条带锁
- `chunk`：`RangeIndexChunkedSerializer` / `RangeIndexChunkedDeserializer` / `RangeIndexMigrationReader` 迁移分块流状态机
- `stub`：`RangeIndexStub` 主存储日志中 35 字节定长存根及原位修改助手
- `types`：`BfTreeConfig`、`TreeTuning`、`StorageBackendType`（Disk / Memory）、读写删结果状态码、`ScanRecord`
- `error`：错误类型（Io / InvalidArgument / InvalidConfig / IndexExists / Snapshot / Recovery / Disposed / Timeout（屏障排空 30s 上限）/ ScanAborted / Corrupted）

## 核心 API

- `BfTreeService`：创建 / 打开 / 点读写删 / 扫描 / cpr_snapshot / 恢复；`write_barrier` 返回计数式 RAII 守卫
- `RangeIndexManager`：get_or_open_tree（惰性恢复）、create / dispose / flush / checkpoint / truncate、复制枚举与迁移临时路径派生、pre_stage_and_register_pending、recover_all_trees_from_checkpoint / recover_all_trees_from_dir
- `RangeIndexStub`：tree_handle 8B + cache_size 8B + min/max_record_size / max_key_len / leaf_page_size 4B×4 + backend / flags / serialization_phase 各 1B，共 35B（`RANGE_INDEX_STUB_SIZE = 35`）
- 迁移流格式：`[4B keyLen][key][8B fileBytes][file][8B checksum][4B stubLen][stub]`；`MIN_CHUNK_SIZE = 47`、`DEFAULT_MIGRATION_CHUNK_SIZE = 256KiB`
- 常量：`NUM_LOCK_STRIPES = 128`、`INDEX_SIZE_BYTES = 35`

## 设计要点

- thread-per-core 契约：全同步 API、无运行时依赖；点读 / 写路径零条带锁（引擎内部叶子闩锁保并发），跨线程共享 `Arc<BfTreeService>`；仅生命周期变更（创建 / 惰性恢复 / 注销 / 删除）取 `RangeIndexLocks` 条带写锁；在线引用为 papaya 无锁字典，读侧 pin 快照与写侧互不阻塞；删除树延迟到 `Arc` 引用归零
- 快照写屏障：「屏障计数 + 在途写者计数」双 AtomicUsize，SeqCst store-buffering（Dekker）配对；屏障计数式、可嵌套，写者阻塞至最外层守卫丢弃，按自旋 → yield → 微睡阶梯短暂退避，树对写静稳、快照无撕裂；排空超 30s 以 `Error::Timeout` 显式上抛；持守卫窗口内严禁 await / 同线程 I/O 事件
- 键语义：键全程 `&[u8]` 二进制安全零拷贝；128 位键 ID 由 gxhash128 派生（专用种子域，摘要域与用户数据域隔离），文件名前缀即该 ID 的 32 字符十六进制编码
- 惰性恢复：get_or_open_tree 先把刷盘快照（裸名优先，否则取最大地址）复制为数据文件，数据文件带 CPR 魔数（`BF-TREE-V0-BEGIN`）则走快照恢复，否则按存根重建 / 重开树；on_flush 冷树复制数据文件并置 flushed 位
- 叶页尺寸推导：`max_record_size` ≤2KB 时取 4096，否则按 2.5 倍封顶 32768 后向上取 2 的幂

## 测试覆盖

tests/ 覆盖：记录容量边界、磁盘重开与 CPR 快照恢复、多读多写并发、屏障嵌套与快照无撕裂；锁条带数 / 对齐 / 竞争、stub 编解码与 slice 助手、leaf_page_size 推导、manager 生命周期 / 检查点 / 截断 / 复制枚举 / 重复创建防护 / flush 文件名严格解析；分块序列化 round_trip、跨块边界与空块、错误态终结、checksum 损坏、流式读取；interop 生命周期与销毁、点读写删零分配契约、扫描计数 / 端键 / 字段选择 / 排序、快照恢复往返与损坏快照报错。

