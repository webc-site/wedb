[English](#en) | [中文](#zh)

---

<a name="en"></a>

# waof : AOF Write-Ahead Log

## Introduction

waof provides a WAL engine (`WalLog`) built on a ring memory write buffer plus segmented block devices; persistence goes through the `Device` abstraction from wdev, with `wbase::pool` supplying aligned memory and buffer pools (`AlignedBuf` / `BufferPool`). The crate splits into the `wal/` physical layer (TsavoriteLog counterpart) and the `aof/` semantic layer (GarnetAppendOnlyFile counterpart); the external API is re-exported at the crate root.

Each committed batch ends with an in-log commit metadata frame (`CommitMeta`, payload 24B / full frame 32B = 8B `WalFrameHeader` + 24B); recovery scans to the last commit frame to settle the commit boundary and physically erases residual records beyond it.

The record frame header is only 8B (`entry_len: u32` + `crc32: u32`, little-endian); empty payloads use the `EMPTY_PAYLOAD_CRC = 0xFFFF_FFFF` sentinel so committed empty record headers are never all-zero, distinguishing them from torn crash tails.

## Module Layout

- `config`: `WalConfig` (buffer_size default 16MiB, inflight_slots default 256); sector alignment is not configurable — `Device::sector_size()` is the single source of truth
- `header` / `record`: `WalFrameHeader`, `WalRecord` (address / next_address / header / payload, Derefs to [u8])
- `log`: `WalLog` / `WalLogInner` core engine holding begin / tail / flushed_until / committed_until atomic positions
- `disk_window`: `DiskWindow` shared sliding pre-read window for recovery and scan (internal)
- `iterator`: `WalScanIterator` sliding-window chunked reads, transparent across memory and disk segments
- `ring_buffer`: `RingBuffer` in-memory ring write buffer; power-of-two capacities take the bitmask fast path

## Core API

- `WalLog<D>`: open (open-and-recover), enqueue / enqueue_parts / enqueue_raw / enqueue_frames (lock-free CAS address reservation with in-flight slot registration; enqueue_raw writes a pre-formatted frame verbatim — replica-faithful persistence, cf. C# UnsafeTryEnqueueRaw), commit / commit_to / commit_flush_only / unsafe_commit_metadata_only (batch flush through the `wbase::GroupCommitPipeline`), wait_for_commit, scan / scan_memory_records, total_size, recover (via open), truncate, reset
- `WalScanIterator<D>`: transparent memory/disk scanning
- `WalConfig`, `WalRecord`, `WalFrame`, `WalFrameHeader` (`RECORD_HEADER_LEN = 8`), `CommitMeta`, `RingBuffer`, `SequenceNumberGenerator`
- `aof/` semantic layer: `AofAddress`, `AofEntryType`, `AofHeader` family (`AofHeader` / `AofShardedHeader` / `AofChunkHeader` / transaction headers)

## Design Notes

- Flush semantics: enqueue is lock-free; commit drains [flushed, target) through `wbase::GroupCommitPipeline` — the first caller becomes the leader and cascades physical flush + device sync, concurrent followers wait on the pipeline and return once the shared watermark covers their target (same kernel as the wkv store flush step); wait_for_commit is the fence over the same watermark — lock-free return when already committed, otherwise wait on the pipeline to avoid thundering herds
- Overwrite semantics: the ring overwrites unflushed data. When the in-memory copy of an already-flushed record is clobbered, scans fall back to the authoritative disk data and continue; when an unflushed record is evicted by ring overwrite, it is counted in `overwritten_skips` and the scan terminates early with `Ok(None)` — that range has no authoritative disk copy and cannot be recovered; a non-zero count means a "completed" scan actually ended early due to overwrite
- Recovery: requires a quiet log; EOF / torn header / checksum failure / all-zero fill all conservatively truncate to the last complete record, other I/O errors propagate; no checkpoint dependency — the CRC record chain self-synchronizes to locate the tail; after truncate, if the segment start lands mid-payload of a torn cross-segment record, recovery first re-synchronizes byte-by-byte via frame_sync and advances begin_address
- truncate advances begin and physically deletes segments, mutually excluded with commit against ghost segments; reset only rewinds in-memory positions and has a stale-record revival window (documented)
- RingBuffer role: pre-commit memory residency — enqueue makes zero syscalls, commit flushes sequentially in batches
- Commit boundary note: every committed batch is closed by an in-log commit metadata frame (`CommitMeta`, payload 24B / full frame 32B = 8B `WalFrameHeader` + 24B); recovery converges the commit boundary to the last commit frame and physically erases records beyond it, so the last commit frame — not the last complete record — is the durability boundary

## Test Coverage

tests/ covers: WalFrameHeader encoding and corruption robustness, RingBuffer large-address reads/writes; end-to-end smoke (write-scan-commit-truncate-restart-append); full buffer, payload limits, raw-frame fidelity and replica address-parity replay, bounded concurrent growth, fast-commit concurrent waiting, short-write protection; sub-range scans, uncommitted memory, behind-begin jumps, physical truncation stop, slow-reader eviction with disk fallback, memory overwrite disk fallback, large records, disk prefetch boundaries; multi-stage recovery, torn tail, empty-record durability, all-zero fill non-revival, mid-log corruption conservative stop, cross-segment frame_sync, massive record counts; truncate with file deletion, exact segment boundaries, periodic truncation, reset reuse.

---

<a name="zh"></a>

# waof : AOF 预写日志

## 项目介绍

waof 提供基于环形内存写缓冲 + 分段块设备的 WAL 预写日志引擎（`WalLog`）；落盘经 wdev 的 `Device` 设备抽象完成，`wbase::pool` 提供对齐内存与缓冲池（`AlignedBuf` / `BufferPool`）。crate 内分 `wal/` 物理层（TsavoriteLog 对标）与 `aof/` 语义层（GarnetAppendOnlyFile 对标），外部 API 在 crate 根 re-export。

每个已提交批以随批尾的 in-log commit 元数据帧（`CommitMeta`，负载 24B / 整帧 32B = 8B `WalFrameHeader` + 24B）收口；恢复扫至最后 commit 帧收敛提交边界，并物理擦除其后的残余记录。

记录帧头仅 8B（`entry_len: u32` + `crc32: u32`，小端）；空负载以 `EMPTY_PAYLOAD_CRC = 0xFFFF_FFFF` 哨兵保证已提交空记录头不全零，可与崩溃残缺尾区分。

## 模块组成

- `config`：`WalConfig`（buffer_size 默认 16MiB、inflight_slots 默认 256）；扇区对齐不进配置，以 `Device::sector_size()` 为单一真源
- `header` / `record`：`WalFrameHeader`、`WalRecord`（address / next_address / header / payload，Deref 到 [u8]）
- `log`：`WalLog` / `WalLogInner` 核心引擎，持 begin / tail / flushed_until / committed_until 四个原子位点
- `disk_window`：`DiskWindow` 恢复与扫描共用的磁盘滑动预读窗（内部）
- `iterator`：`WalScanIterator` 滑动窗口分块读，透明跨内存与磁盘段
- `ring_buffer`：`RingBuffer` 内存环形写缓冲，容量为 2 的幂时走位掩码快路径

## 核心 API

- `WalLog<D>`：open（打开并自动 recover）、enqueue / enqueue_parts / enqueue_raw / enqueue_frames（无锁 CAS 预占地址并注册在途槽位；enqueue_raw 原样写入完整记录帧——复制从节点保真落盘，对标 C# UnsafeTryEnqueueRaw）、commit / commit_to / commit_flush_only / unsafe_commit_metadata_only（批量刷盘经 `wbase::GroupCommitPipeline`）、wait_for_commit、scan / scan_memory_records、total_size、recover（经 open）、truncate、reset
- `WalScanIterator<D>`：跨内存 / 磁盘透明扫描
- `WalConfig`、`WalRecord`、`WalFrame`、`WalFrameHeader`（`RECORD_HEADER_LEN = 8`）、`CommitMeta`、`RingBuffer`、`SequenceNumberGenerator`
- `aof/` 语义层：`AofAddress`、`AofEntryType`、`AofHeader` 族（`AofHeader` / `AofShardedHeader` / `AofChunkHeader` / 事务头）

## 设计要点

- 刷盘语义：enqueue 无锁；commit 经 `wbase::GroupCommitPipeline` 排空 [flushed, target)——首到者成为 Leader 级联物理刷盘 + 设备 sync，并发跟随者在流水线上等待，共享水位覆盖各自目标即返回（与 wkv store 刷盘步共用同一内核）；wait_for_commit 是同一水位上的栅栏——已提交无锁返回，否则在流水线上等待，避免惊群
- 覆写语义：环形缓冲区会覆写未刷盘数据。已落盘记录的内存副本被覆写时，扫描回退磁盘权威数据继续；未落盘记录被环形覆写挤出内存窗时计入 `overwritten_skips` 并以 `Ok(None)` 提前终止——该区间磁盘无权威副本、无法回读恢复，计数非零即代表"扫完"实为中途覆写丢失
- 恢复：要求日志静默；EOF / 残缺头 / 校验和失败 / 全零填充一律保守截断到最后完整记录，其他 I/O 错误显式上抛；无检查点依赖，靠 CRC 记录链自同步定位尾部；truncate 后段首落在跨段残缺负载中部时，恢复先以 frame_sync 逐字节探测重同步并前移 begin_address
- truncate 推进 begin 并物理删段，与 commit 互斥防幽灵段；reset 仅回退内存位点不请磁盘，存在旧记录复活窗口（注释明示）
- RingBuffer 定位：提交前的内存驻留区，enqueue 零系统调用，commit 批量顺序落盘
- 提交边界：每个已提交批以随批尾的 in-log commit 元数据帧（`CommitMeta`，负载 24B / 整帧 32B）收口；恢复把提交边界收敛到最后一个 commit 帧并物理擦除其后记录——持久性边界是最后 commit 帧，而非最后一条完整记录

## 测试覆盖

tests/ 覆盖：WalFrameHeader 编解码与破坏鲁棒性、RingBuffer 大地址读写；端到端冒烟（写-扫-提交-截断-重启-追加）；满缓冲、payload 限制、raw 帧保真与从节点重放地址一致、并发有界增长、快速提交并发等待、短写防护；子区间扫描、未提交内存、落后 begin 跳转、物理截断停止、慢读者逐出回退磁盘、内存覆写回退磁盘、大记录、磁盘预取边界；多阶段恢复、残缺尾、空记录持久性、全零填充不复活、中段损坏保守停止、跨段残缺 frame_sync、海量记录；truncate 与文件删除、精确段边界、周期截断、reset 复用。
