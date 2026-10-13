# wkv : 顶层混合存储引擎

## 项目介绍

wkv 是顶层单机混合存储引擎：整合 windex 哈希索引 + whlog HybridLog + wepoch 纪元保护 + wbftree/RangeIndex，并内置 TTL、GC、读缓存、紧缩调度与 CPR 检查点集成。

## 模块组成

- `store`：`WedbStore` 顶层引擎（地址推进、在线扩容、键空间统计、检查点快捷入口）
- `session`：`StoreSession` / `BatchStoreSession` 会话与物理键编码
- `config`：自适应配置（`StoreConfig` / `GcConfig`）
- `ttl`：key 级 TTL 三态判定门（`TtlGate`）与清退
- `gc`：后台过期扫描 + 紧缩调度（`GcManager`）
- `read_cache`：纯内存只读缓存日志
- `compact`：wcompact 紧缩适配层
- `range_index`：BfTree 二级索引操作、35B 存根、分层升 / 降阶与分块迁移
- `ri`：`RiTreeOps` 扩展 trait（`BfTreeService` 的 ri_* 语义面，crate 内部）
- `etag`：key 级 ETag 旁路记录的会话读写域
- `vdb`：虚拟库路由与库元数据（`DbMetaRecord` 编解码、死亡号账本、换号树回收）
- `error`：错误类型

## 核心 API

- `WedbStore`：open / open_shared / from_components / new_session / grow_index / start_gc / stop_gc / update_gc_config / gc_config / gc_stats / gc_running / shift_read_only_address / flush_all / flush_and_evict_all / truncate / expired_key_deletion_scan / raise_key_id_floor / create_checkpoint / recover
- `StoreConfig`：构造器 auto / auto_with_budget / minimal / new（Default = minimal）；builder 链 with_max_sessions / with_revivification / with_revivifiable_fraction / with_read_cache(\_pages) / with_copy_reads_to_tail / with_range_index_dir / with_tree_cache_budget；导出常量 `MIN_INDEX_SIZE = 65536`、`MAX_INDEX_SIZE = 16_777_216`、`DEFAULT_GC_MAX_SEGMENTS = 32`、`DEFAULT_GC_MAX_BATCH_DELETES = 256`、`DEFAULT_DB_GC_RECLAIM_DELAY_SECS = 86_400`、`MIN_ADAPTIVE_BUDGET_BYTES`（auto() 默认按宿主内存 25% 推导预算，内部界 256MB–32GB）
- `StoreSession`：upsert / read / delete、try_upsert_sync 三态（成功返回记录地址——尾部追加为新地址，原位更新 / 链内复活 / 复活池复用为原地址；环形缓冲翻转返回待驱逐页号；u64::MAX 表示 TTL 清除需降级异步路径）、enter_batch、物理键编码（session_tag_key / session_string_key / session_meta_key / vector_key）
- `BatchStoreSession`：批处理单次纪元保护，`*_unprotected` 零原子开销快路径
- 内置 GC 单门面 `GcManager`（GC 句柄与统计快照为内部类型，统计经 `gc_stats()` 直读）；`ReadCache`（append / with_record / skip_read_cache）
- `TtlOpt`（NX / XX / GT / LT）、`TtlGate` 三态门（Pass / Due / Degrade）
- 会话级范围索引操作：range_index_create / range_index_set / range_index_set_batch / range_index_get / range_index_get_with / range_index_del / range_index_scan_stream / range_index_range_stream / range_index_exists / range_index_count / range_index_config / range_index_metrics
- 另转导参数型单门面：`wcompact::CompactionType`、`wcpr::CheckpointType`、`whlog::VERSION_MASK`

## 设计要点

- 索引并发与动态扩容：HashIndex 结合 ArcSwap 与 SplitIndex 在线扩容状态机，前台在扩容迁移期按需按哈希 CAS 抢占分块分裂迁移，读写操作通过 active_index 无锁安全推进
- GC：每周期固定 `[begin, scan_end)` 快照（尾地址固定防持续追加饿死旧 TTL）；单轮限 max_scan_records 条与 max_batch_deletes 键，候选经最新 TTL 双检后统一清集合元数据 / 子键 / TTL；紧缩触发 `read_only - begin > max_segments × segment_size`；紧缩器直接集成 wcompact，`GcManager` 仅持 Weak；紧缩经 shift_begin_address 推进 begin 地址，whlog 在推进后直接调用设备 truncate_until_address 物理删除已回收的磁盘段文件（分段设备自动回收，单文件无界模式无段可删），显式 truncate 仅作强制补截断；默认 enabled=false
- 读缓存：地址第 47 位为 READ_CACHE_BIT 打标，低 48 位为绝对地址；checkpoint 时索引条目经 skip_read_cache 顺链回写主日志真实地址
- TTL 门三态：Pass（无 TTL / 未到期）、Due（已到期，快路径按 NOTFOUND 闭环）、Degrade（TTL 记录存在磁盘候选，降级异步裁决）；清退链探测不到记录不触发二次清除，无递归
- 集合自适应分层：小集合驻内存信封（`KeyTag::ObjectEnvelope` 记录 + bitcode 载荷），升阶后转 BfTree 独立分层树、主日志仅存 35B 存根；升 / 降阶判据单源在 `wcol`——升阶 `should_promote`（条目数 ≥ 65,536 或 heap ≥ 4MB），降阶 `should_demote`（条目数 ≤ 32,768 且 heap ≤ 2MB），高低水位之间为迟滞死区防抖动

## 测试覆盖

tests/ 覆盖：基本读写、原位覆写、RCU 版本链、墓碑删除与复活、多会话并发、换页驱逐冷读、RMW 与共享 BfTree 打开形态；store 的 crud / flush_evict / defense / reviv / collision_chain；compact 套件（basic / lazy / 并发碰撞 / spanbyte / 多轮紧缩）；checkpoint 套件（recovery / edge / manager / index_checkpoint / fault_defense）；gc 与配置默认值。
