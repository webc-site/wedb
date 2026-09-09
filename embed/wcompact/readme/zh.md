# wcompact : 日志紧缩

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
