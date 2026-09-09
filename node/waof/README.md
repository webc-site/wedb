[English](#en) | [中文](#zh)

---

<a name="en"></a>

# waof : AOF Write-Ahead Log

- [Introduction](#introduction)
- [Module Layout](#module-layout)
- [Core API](#core-api)
- [Design Notes](#design-notes)
- [Test Coverage](#test-coverage)

## Introduction

waof provides a WAL engine (`WalLog`) built on a ring memory write buffer plus segmented block devices; persistence goes through the `Device` abstraction from wdev, with wram supplying aligned memory and buffer pools. `AofLog` / `AofRecord` etc. are aliases of the `Wal*` types.

The record header is only 8B (`entry_len: u32` + `crc32: u32`, little-endian); empty payloads use the `EMPTY_PAYLOAD_CRC = 0xFFFF_FFFF` sentinel so committed empty record headers are never all-zero, distinguishing them from torn crash tails.

## Module Layout

- `config`: `WalConfig` (buffer_size default 16MiB, inflight_slots default 256); sector alignment is not configurable — `Device::sector_size()` is the single source of truth
- `header` / `record`: `RecordHeader`, `WalRecord` (address / next_address / header / payload, Derefs to [u8])
- `log`: `WalLog` / `WalLogInner` core engine holding begin / tail / flushed_until / committed_until atomic positions
- `disk_window`: `DiskWindow` shared sliding pre-read window for recovery and scan (internal)
- `iterator`: `WalScanIterator` sliding-window chunked reads, transparent across memory and disk segments
- `ring_buffer`: `RingBuffer` in-memory ring write buffer; power-of-two capacities take the bitmask fast path

## Core API

- `WalLog<D>`: open (open-and-recover), enqueue (lock-free CAS address reservation with in-flight slot registration), commit (batch flush up to safe_tail), enqueue_raw (write a pre-formatted frame verbatim — replica-faithful persistence, cf. C# UnsafeTryEnqueueRaw), enqueue_and_wait_for_commit (enqueue then await durability at the record end address), wait_for_commit, scan / scan_all / scan_committed, total_size, recover, truncate, reset
- `WalScanIterator<D>`: transparent memory/disk scanning
- `WalConfig`, `WalRecord`, `RecordHeader` (`RECORD_HEADER_LEN = 8`), `RingBuffer`
- Aliases: `AofConfig` / `AofLog<D>` / `AofLogInner<D>` / `AofRecord` / `AofScanIterator<D>`

## Design Notes

- Flush semantics: enqueue is lock-free; commit holds the commit lock and flushes [flushed, safe_tail) where safe_tail = min(tail, all in-flight slots); wait_for_commit is a fast fence — lock-free return when already committed, otherwise double-checked try_lock cooperative flush or event listening to avoid thundering herds
- Overwrite semantics: the ring overwrites unflushed data. When the in-memory copy of an already-flushed record is clobbered, scans fall back to the authoritative disk data and continue; when an unflushed record is evicted by ring overwrite, it is counted in `overwritten_skips` and the scan terminates early with `Ok(None)` — that range has no authoritative disk copy and cannot be recovered; a non-zero count means a "completed" scan actually ended early due to overwrite
- Recovery: requires a quiet log; EOF / torn header / checksum failure / all-zero fill all conservatively truncate to the last complete record, other I/O errors propagate; no checkpoint dependency — the CRC record chain self-synchronizes to locate the tail; after truncate, if the segment start lands mid-payload of a torn cross-segment record, recovery first re-synchronizes byte-by-byte via frame_sync and advances begin_address
- truncate advances begin and physically deletes segments, mutually excluded with commit against ghost segments; reset only rewinds in-memory positions and has a stale-record revival window (documented)
- RingBuffer role: pre-commit memory residency — enqueue makes zero syscalls, commit flushes sequentially in batches
- Commit boundary note: there is no per-commit metadata record; recovery treats the last complete record as committed. Records enqueued but not yet committed may be revived after a crash if a concurrent commit's flush already covered them — callers requiring an exact commit durability boundary must handle this (see `WalLog` docs)

## Test Coverage

tests/ covers: RecordHeader encoding and corruption robustness, RingBuffer large-address reads/writes; end-to-end smoke (write-scan-commit-truncate-restart-append); full buffer, payload limits, raw-frame fidelity and replica address-parity replay, bounded concurrent growth, fast-commit concurrent waiting, short-write protection; sub-range scans, uncommitted memory, behind-begin jumps, physical truncation stop, slow-reader eviction with disk fallback, memory overwrite disk fallback, large records, disk prefetch boundaries; multi-stage recovery, torn tail, empty-record durability, all-zero fill non-revival, mid-log corruption conservative stop, cross-segment frame_sync, massive record counts; truncate with file deletion, exact segment boundaries, periodic truncation, reset reuse.

---

<a name="zh"></a>

# waof : AOF 预写日志

- [项目介绍](#项目介绍)
- [模块组成](#模块组成)
- [核心 API](#核心-api)
- [设计要点](#设计要点)
- [测试覆盖](#测试覆盖)

## 项目介绍

waof 提供基于环形内存写缓冲 + 分段块设备的 WAL 预写日志引擎（`WalLog`）；落盘经 wdev 的 `Device` 设备抽象完成，wram 提供对齐内存与缓冲池（`AlignedBuf` / `BufferPool`）。`AofLog` / `AofRecord` 等为 `Wal*` 类型的别名。

记录头仅 8B（`entry_len: u32` + `crc32: u32`，小端）；空负载以 `EMPTY_PAYLOAD_CRC = 0xFFFF_FFFF` 哨兵保证已提交空记录头不全零，可与崩溃残缺尾区分。

## 模块组成

- `config`：`WalConfig`（buffer_size 默认 16MiB、inflight_slots 默认 256）；扇区对齐不进配置，以 `Device::sector_size()` 为单一真源
- `header` / `record`：`RecordHeader`、`WalRecord`（address / next_address / header / payload，Deref 到 [u8]）
- `log`：`WalLog` / `WalLogInner` 核心引擎，持 begin / tail / flushed_until / committed_until 四个原子位点
- `disk_window`：`DiskWindow` 恢复与扫描共用的磁盘滑动预读窗（内部）
- `iterator`：`WalScanIterator` 滑动窗口分块读，透明跨内存与磁盘段
- `ring_buffer`：`RingBuffer` 内存环形写缓冲，容量为 2 的幂时走位掩码快路径

## 核心 API

- `WalLog<D>`：open（打开并自动 recover）、enqueue（无锁 CAS 预占地址并注册在途槽位）、commit（批量刷盘至 safe_tail）、enqueue_raw（原样写入完整记录帧——复制从节点保真落盘，对标 C# UnsafeTryEnqueueRaw）、enqueue_and_wait_for_commit（写入并等待提交持久化，目标地址为记录末端）、wait_for_commit、scan / scan_all / scan_committed、total_size、recover、truncate、reset
- `WalScanIterator<D>`：跨内存 / 磁盘透明扫描
- `WalConfig`、`WalRecord`、`RecordHeader`（`RECORD_HEADER_LEN = 8`）、`RingBuffer`
- 别名：`AofConfig` / `AofLog<D>` / `AofLogInner<D>` / `AofRecord` / `AofScanIterator<D>`

## 设计要点

- 刷盘语义：enqueue 无锁；commit 持提交锁把 [flushed, safe_tail) 刷盘，safe_tail = tail 与全部在途槽位最小值；wait_for_commit 高速栅栏——已提交无锁返回，否则双重检查 + try_lock 协同提交或监听广播，避免惊群
- 覆写语义：环形缓冲区会覆写未刷盘数据。已落盘记录的内存副本被覆写时，扫描回退磁盘权威数据继续；未落盘记录被环形覆写挤出内存窗时计入 `overwritten_skips` 并以 `Ok(None)` 提前终止——该区间磁盘无权威副本、无法回读恢复，计数非零即代表"扫完"实为中途覆写丢失
- 恢复：要求日志静默；EOF / 残缺头 / 校验和失败 / 全零填充一律保守截断到最后完整记录，其他 I/O 错误显式上抛；无检查点依赖，靠 CRC 记录链自同步定位尾部；truncate 后段首落在跨段残缺负载中部时，恢复先以 frame_sync 逐字节探测重同步并前移 begin_address
- truncate 推进 begin 并物理删段，与 commit 互斥防幽灵段；reset 仅回退内存位点不请磁盘，存在旧记录复活窗口（注释明示）
- RingBuffer 定位：提交前的内存驻留区，enqueue 零系统调用，commit 批量顺序落盘
- 提交边界：无独立 commit 元数据记录，恢复以最后一条完整记录为已提交。已 enqueue 未 commit 的记录若被并发 commit 的刷盘区间覆盖，崩溃后会被视为已提交复活——要求精确提交持久性边界的调用方须自行处理（见 `WalLog` 文档）

## 测试覆盖

tests/ 覆盖：RecordHeader 编解码与破坏鲁棒性、RingBuffer 大地址读写；端到端冒烟（写-扫-提交-截断-重启-追加）；满缓冲、payload 限制、raw 帧保真与从节点重放地址一致、并发有界增长、快速提交并发等待、短写防护；子区间扫描、未提交内存、落后 begin 跳转、物理截断停止、慢读者逐出回退磁盘、内存覆写回退磁盘、大记录、磁盘预取边界；多阶段恢复、残缺尾、空记录持久性、全零填充不复活、中段损坏保守停止、跨段残缺 frame_sync、海量记录；truncate 与文件删除、精确段边界、周期截断、reset 复用。
