R4 门禁红七枚归因修复：aof 回放系 + garnet_log 广播 + ttl_purge

来源：主代理门禁 R4（干净 detached 树 /tmp/gate-r4，基线 dev a954680，2026-09-19T14:38）。
R3 全绿基线为 22548fc（2924 tests），区间 22548fc..a954680 合入后出现 8 失败，其中
scan_parse_quirks_match_csharp 已由主代理定性为 scan-type 语义修正的过期断言并在
44455b4 修正（按 C# SequenceEqual 双形态，混合大小写走未知臂空回）。剩余 7 枚待归因：

1. wnode::aof_flush_replay test_flush_entry_payload_u64_domain
2. wnode::aof_flush_replay test_flush_ns_replica_replay
3. wnode::aof_flush_replay test_flush_db_replica_replays_entry_domains_without_local_remap
   （panic 于 aof_flush_replay.rs:279「FlushDb 条目须把载荷旧域 (3, 7) 投进本地 GC 死亡账本」）
4. wnode::aof_replay_domain full_replay_nonzero_domain_lands_in_entry_domain
5. wnode::aof_stored_proc_replay flush_db_entry_replays_targeted_database
6. wnode::garnet_log broadcast_writes_all_sublogs_with_txn_headers
7. wnode::service ttl_purge_single_deterministic_entry

嫌疑区间（按域对齐）：6dc1cb6 fix FlushDb/FlushNs 回放本地取号分叉（DbMeta 镜像承接
主从映射继承，并发会话合入 2b6a4d0，票归档 9a23ca4）——1~5 全在其 FlushDb/域路由射程；
6 另疑 aof-header 拆分（656e2c4，事务头线格式）与 replica-wire（aa9cef6）；7 疑 gc 域。
逐枚归因用 `git log 22548fc..dev -- <失败测试文件与被测源文件>` 加针对性 checkout 复跑，
必要时 bisect 到提交。

修法二选一，按 C# 语义终裁：若并发修复行为正确，则更新过期测试断言（逐条给出 C#
行实：FlushDb/FlushNs 在 C# Replica 回放下是否入本地死亡账本、广播事务头线格式、
ttl_purge 取号）；若行为破坏 C# 语义，则修复实现并保留测试。只留一套机制。
门禁：私有 target 跑 wnode 全域 nextest + workspace check，回报主代理全量复绿数字。
