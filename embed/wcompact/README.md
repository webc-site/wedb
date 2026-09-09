[English](#en) | [中文](#zh)

---

<a name="en"></a>

# wcompact : Log Compaction

- [Introduction](#introduction)
- [Module Layout](#module-layout)
- [Core API](#core-api)
- [Design Notes](#design-notes)
- [Test Coverage](#test-coverage)

## Introduction

wcompact provides the HybridLog online compactor: it scans the read-only region for dead records, copies live records to the log tail with atomic index CAS replacement, then advances the begin address to reclaim cold segments. It decouples from concrete engines via the `CompactStore` / `CompactSession` trait abstractions.

## Module Layout

- `compactor`: compaction core and strategies (`LogCompactor`, `CompactionType`, `CompactionStats`)
- `host`: host abstraction traits (`CompactSession`, `CompactStore`)
- `error`: error types

## Core API

- `LogCompactor`: new / with_cas_retries / compact / compact_with_filter / compact_lazy(max_seek_bytes)
- `CompactionType`: Lookup (per-record index probe of the live value) / Scan (single-pass candidate hash table for batch dedup, O(unique keys) space, value bodies read back only when alive)
- `CompactionStats`: scanned_records / live_copied / superseded / dead_dropped / retained / bytes_freed / new_begin_address (exact conservation: scanned_records = live_copied + superseded + dead_dropped + retained)
- `CompactSession`: enter_epoch / append_record / read_ttl_expiry; `TTL_VALUE_LEN = 8`
- `CompactStore`: hlog / index / read-only and begin addresses / shift_begin_address / read-cache detection and skipping / revivification pool injection and other host capability ports

## Design Notes

- Flow: scan (delegates to the whlog ScanIterator with per-page prefetch) → liveness decision → conditional_copy_to_tail append + atomic index CAS → shift_begin_address segment reclamation
- CAS retries cap at 8; on exhaustion with a still-live record the truncation point falls back for the next round — records are never dropped incorrectly
- TTL-aware three rules: expired TTL records die; unexpired TTL records whose host key is absent from the index (orphan TTL) die; data records with attached expired TTL die
- Additional pass: replay collection-metadata watermarks during compaction; is_stale_subkey drops deleted collections / stale-version history sub-keys; at finalize, key_id_versions dead entries whose death address falls entirely within the compacted range and remain dead in memory are reclaimed
- compact_lazy bounds the per-round scan seek budget via max_seek_bytes over [begin, min(read_only, begin + max_seek_bytes)) with a fixed Lookup strategy (returns empty stats immediately when there is nothing to compact or the budget is 0), fitting incremental background compaction

## Test Coverage

Inline tests in compactor.rs cover only: CompactionStats defaults / is_empty and MetaDeathScope death registration. The Lookup / Scan strategies, three TTL rules, CAS contention fallback, statistics accounting, and lazy budget constraints are covered by the wkv/tests/compact integration suites (basic / lazy_compaction / concurrency_and_collision / spanbyte_compaction / more_log_compaction).

---

<a name="zh"></a>

# wcompact : 日志紧缩

- [项目介绍](#项目介绍)
- [模块组成](#模块组成)
- [核心 API](#核心-api)
- [设计要点](#设计要点)
- [测试覆盖](#测试覆盖)

## 项目介绍

wcompact 提供 HybridLog 在线紧缩器：扫描只读区判死活，把活记录拷贝到日志尾、原索引 CAS 替换，再推进 begin 地址回收冷段。经 `CompactStore` / `CompactSession` trait 抽象与宿主引擎解耦，不绑定具体引擎实现。

## 模块组成

- `compactor`：紧缩核心与策略（`LogCompactor`、`CompactionType`、`CompactionStats`）
- `host`：宿主抽象 trait（`CompactSession`、`CompactStore`）
- `error`：错误类型

## 核心 API

- `LogCompactor`：new / with_cas_retries / compact / compact_with_filter / compact_lazy(max_seek_bytes)
- `CompactionType`：Lookup（逐条索引探查现值）/ Scan（单趟候选哈希表批量去重，空间 O(唯一键)，值体仅存活时回读一次）
- `CompactionStats`：scanned_records / live_copied / superseded / dead_dropped / retained / bytes_freed / new_begin_address（精确守恒：scanned_records = live_copied + superseded + dead_dropped + retained）
- `CompactSession`：enter_epoch / append_record / read_ttl_expiry；`TTL_VALUE_LEN = 8`
- `CompactStore`：hlog / index / 只读与 begin 地址 / shift_begin_address / read_cache 判定与跳过 / 复活池注入等宿主能力端口

## 设计要点

- 流程：scan（委托 whlog ScanIterator 按页预取）→ 判死 → conditional_copy_to_tail 尾部追加 + CAS 原子替换索引 → shift_begin_address 回收段
- CAS 重试上限 8 次，耗尽且记录仍存活则回退截断点留给下轮，绝不误删
- TTL 感知三规则：TTL 记录自身到期判死；未到期但宿主主键不在索引（孤儿 TTL）判死；数据记录附带 TTL 且已到期判死
- 附加方案：紧缩途中回放集合元数据水位，is_stale_subkey 淘汰已删集合 / 旧版本历史子键；收尾回收死亡地址已完整落入紧缩区间且内存态仍判死的 key_id_versions 死条目
- compact_lazy 以 max_seek_bytes 约束单轮扫描跳跃预算（区间 [begin, min(read_only, begin + max_seek_bytes))，固定 Lookup 策略；无可紧缩区间或预算为 0 时空统计直返），适配渐进式后台紧缩

## 测试覆盖

compactor.rs 内联测试仅覆盖：CompactionStats 默认值 / is_empty 与 MetaDeathScope 死亡登记。Lookup / Scan 两策略、TTL 三规则、CAS 竞争回退、统计口径与 lazy 预算约束由 wkv/tests/compact 集成套件（basic / lazy_compaction / concurrency_and_collision / spanbyte_compaction / more_log_compaction）覆盖。
