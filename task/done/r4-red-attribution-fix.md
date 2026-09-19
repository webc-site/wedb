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

---
主代理代办收口（棒撞 150 顶，回报残句「Agent execution completed」）：
- 载荷 b96aeab（四测试文件断言随 6dc1cb6 DbMeta 镜像契约迁移，+332/−59）→ 三回合 dev 后由主代理 FF 入 dev。
- 代办前置门禁（分支树私有 target /tmp/ct-r4）：wnode 全量 nextest 1092 例中本票 7 红所属五套件全绿；回合 dev dd9d15f 后五套件复跑 35/35 绿。
- 全量暴露 4 枚 dev 新红（range_index_wrongtype_gate/ri_key_rename_not_wrongtyped、resp_commandstats_session/commandstats_calls_failed_rejected_end_to_end、resp_pubsub/pub_sub_mode_resp2_whitelist_commands、tiered_field_ttl/tiered_hash_expire_sets_and_reads_back）——已在 dev 基线 detached 树复现同红，与本棒载荷无关，另立归因票 next/r5-red-attribution-four-failures.md。
- 归因结论沿用票面：7 红根因 6dc1cb6 契约迁移成立，按 C# 行实更新断言路线（修法分支一）。
