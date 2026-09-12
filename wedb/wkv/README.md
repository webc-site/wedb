[English](#en) | [中文](#zh)

---

<a name="en"></a>

# wkv : Top-Level Hybrid Storage Engine

- [Introduction](#introduction)
- [Module Layout](#module-layout)
- [Core API](#core-api)
- [Design Notes](#design-notes)
- [Test Coverage](#test-coverage)

## Introduction

wkv is the top-level single-node hybrid storage engine: it integrates the windex hash index + whlog HybridLog + wepoch epoch protection + wbftree/RangeIndex, with built-in TTL, GC, read cache, compaction scheduling, and CPR checkpoint integration.

## Module Layout

- `store`: the `WedbStore` top-level engine
- `session`: `StoreSession` / `BatchStoreSession` sessions and physical key encoding
- `config`: adaptive configuration (`StoreConfig`)
- `ttl`: key-level TTL probing and clearing
- `gc`: background expiry scanning + compaction scheduling (`GcManager`)
- `read_cache`: DRAM-only read cache log
- `compact`: adaptation layer over wcompact
- `checkpoint`: RangeIndex CPR snapshots and recovery
- `range_index`: secondary index operations and RESP frame encoding
- `error`: error types

## Core API

- `WedbStore`: open / open_shared / from_components / new_session / start_gc / gc_handle / shift_read_only_address / flush_all / flush_and_evict_all / truncate / expired_key_deletion_scan / raise_key_id_floor
- `StoreConfig`: four constructors auto / auto_with_budget / minimal / new (Default = minimal); builder chain with_max_sessions / with_revivification / with_read_cache(\_pages) / with_range_index_dir / with_gc; `DEFAULT_INDEX_SIZE = 65536`, `DEFAULT_MEMORY_PERCENT = 25`, budget floor 256MB, ceiling 32GB
- `StoreSession`: upsert / read / delete, three-state try_upsert_sync (success returns the record address — a new address for tail appends, the original address for in-place updates / in-chain revival / revivification-pool reuse; page-flip returns the page id to evict; u64::MAX means TTL clearing must fall back to the async path), try_modify_in_place, enter_batch, physical key encoding (session_string_key / meta_key / hash_sub_key / chunk_key)
- `BatchStoreSession`: batch processing with a single epoch protection; `*_unprotected` zero-atomic-overhead fast paths
- `GcManager` / `GcHandle` / `GcStatsSnapshot`; `ReadCache` (append / with_record / skip_read_cache)
- `TtlOpt` (NX / XX / GT / LT), `TtlProbe` (Pass / Due / Deferred)
- `encode_ri_set` / `encode_ri_del` / `encode_ri_create` (RESP Bulk String frames for WAL pre-write and replication streams)
- Also re-exports public types from wbftree / wcompact / wcpr / wrecord

## Design Notes

- Fixed-capacity contract: deliberately bound to a fixed-capacity flat HashIndex opened at startup, no online resize; foreground find_or_create_tag + try_cas slot handles always reference the same live table, eliminating "CAS landing in a retired table during resize" lost writes
- GC: each cycle fixes a `[begin, scan_end)` snapshot (tail pinned to prevent continuous appends from starving old TTLs); per round limited to max_scan_records and max_batch_deletes; candidates re-verify latest TTL then uniformly clear collection metadata / sub-keys / TTL records; compaction triggers when `read_only - begin > max_segments × segment_size`; the compactor is directly integrated from wcompact and `GcManager` holds only a Weak; compaction advances begin via shift_begin_address, after which whlog calls device truncate_until_address to physically delete reclaimed disk segment files (automatic on segmented devices; nothing to reclaim in single-file unbounded mode); an explicit truncate only forces an additional pass; disabled by default
- Read cache: bit 47 marks READ_CACHE_BIT with the low 48 bits as the absolute address; at checkpoint time index entries are walked via skip_read_cache to write back true main-log addresses
- TTL probing tri-state: Pass (no TTL / not due), Due (expired, physically cleared after batch-read closure), Deferred (disk candidates downgraded to async handling); purge_expired never triggers a second clear when the record is not found — no recursion
- Compact collection thresholds: hash ≤512 entries with values ≤64B, set / zset ≤128 entries (set member values ≤64B, zset members ≤64B), total compact encoding per record capped at 4096B (`MAX_COMPACT_TOTAL_BYTES`), beyond which records convert to flattened / BfTree encodings

## Test Coverage

tests/ covers: basic reads/writes, in-place overwrite, RCU version chains, tombstone delete and revival, multi-session concurrency, page-turn eviction cold reads, RMW and shared-BfTree open modes; store suites crud / flush_evict / defense / reviv / collision_chain; compact suites (basic / lazy / concurrency-collision / spanbyte / multi-round); checkpoint suites (recovery / edge / manager / index_checkpoint / fault_defense); gc and config defaults.


---

<a name="zh"></a>

# wkv : 顶层混合存储引擎

- [项目介绍](#项目介绍)
- [模块组成](#模块组成)
- [核心 API](#核心-api)
- [设计要点](#设计要点)
- [测试覆盖](#测试覆盖)

## 项目介绍

wkv 是顶层单机混合存储引擎：整合 windex 哈希索引 + whlog HybridLog + wepoch 纪元保护 + wbftree/RangeIndex，并内置 TTL、GC、读缓存、紧缩调度与 CPR 检查点集成。

## 模块组成

- `store`：`WedbStore` 顶层引擎
- `session`：`StoreSession` / `BatchStoreSession` 会话与物理键编码
- `config`：自适应配置（`StoreConfig`）
- `ttl`：key 级 TTL 探测与清除
- `gc`：后台过期扫描 + 紧缩调度（`GcManager`）
- `read_cache`：纯内存只读缓存日志
- `compact`：wcompact 适配层
- `checkpoint`：RangeIndex CPR 快照与恢复
- `range_index`：二级索引操作与 RESP 帧编码
- `error`：错误类型

## 核心 API

- `WedbStore`：open / open_shared / from_components / new_session / start_gc / gc_handle / shift_read_only_address / flush_all / flush_and_evict_all / truncate / expired_key_deletion_scan / raise_key_id_floor
- `StoreConfig`：四个构造函数 auto / auto_with_budget / minimal / new（Default = minimal）；builder 链 with_max_sessions / with_revivification / with_read_cache(\_pages) / with_range_index_dir / with_gc；`DEFAULT_INDEX_SIZE = 65536`、`DEFAULT_MEMORY_PERCENT = 25`、预算下限 256MB、上限 32GB
- `StoreSession`：upsert / read / delete、try_upsert_sync 三态（成功返回记录地址——尾部追加为新地址，原位更新 / 链内复活 / 复活池复用为原地址；环形缓冲翻转返回待驱逐页号；u64::MAX 表示 TTL 清除需降级异步路径）、try_modify_in_place、enter_batch、物理键编码（session_string_key / meta_key / hash_sub_key / chunk_key）
- `BatchStoreSession`：批处理单次纪元保护，`*_unprotected` 零原子开销快路径
- `GcManager` / `GcHandle` / `GcStatsSnapshot`；`ReadCache`（append / with_record / skip_read_cache）
- `TtlOpt`（NX / XX / GT / LT）、`TtlProbe`（Pass / Due / Deferred）
- `encode_ri_set` / `encode_ri_del` / `encode_ri_create`（RESP Bulk String 帧，用于 WAL 预写与复制流）
- 另转导 wbftree / wcompact / wcpr / wrecord 的公开类型

## 设计要点

- 定容契约：刻意绑定打开时定容的扁平 HashIndex，无在线扩容；前台 find_or_create_tag + try_cas 槽位句柄恒指向同一活跃表，不存在"扩容窗口 CAS 落入退役表"丢写
- GC：每周期固定 `[begin, scan_end)` 快照（尾地址固定防持续追加饿死旧 TTL）；单轮限 max_scan_records 条与 max_batch_deletes 键，候选经最新 TTL 双检后统一清集合元数据 / 子键 / TTL；紧缩触发 `read_only - begin > max_segments × segment_size`；紧缩器直接集成 wcompact，`GcManager` 仅持 Weak；紧缩经 shift_begin_address 推进 begin 地址，whlog 在推进后直接调用设备 truncate_until_address 物理删除已回收的磁盘段文件（分段设备自动回收，单文件无界模式无段可删），显式 truncate 仅作强制补截断；默认 enabled=false
- 读缓存：地址第 47 位为 READ_CACHE_BIT 打标，低 48 位为绝对地址；checkpoint 时索引条目经 skip_read_cache 顺链回写主日志真实地址
- TTL 探测三态：Pass（无 TTL / 未到期）、Due（已到期，批量读闭环后物理清除）、Deferred（磁盘候选降级异步处理）；purge_expired 探测不到记录不触发二次清除，无递归
- 紧凑集合阈值：hash ≤512 条目且值 ≤64B、set / zset ≤128 条目（set 成员值 ≤64B、zset 成员 ≤64B），单记录紧凑编码总量上限 4096B（`MAX_COMPACT_TOTAL_BYTES`），超出转打平 / BfTree 编码

## 测试覆盖

tests/ 覆盖：基本读写、原位覆写、RCU 版本链、墓碑删除与复活、多会话并发、换页驱逐冷读、RMW 与共享 BfTree 打开形态；store 的 crud / flush_evict / defense / reviv / collision_chain；compact 套件（basic / lazy / 并发碰撞 / spanbyte / 多轮紧缩）；checkpoint 套件（recovery / edge / manager / index_checkpoint / fault_defense）；gc 与配置默认值。

