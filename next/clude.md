# clude 待办

2. [P2] wnode 四个巨型文件拆分
   位置：wedb/wnode/src/resp/resp_server_session.rs:1（3993 行）、wedb/wnode/src/aof/aof_processor.rs:1（1915 行）、wedb/wnode/src/resp/basic_commands.rs:1（1915 行）、wedb/wnode/src/aof/garnet_log.rs:1（1736 行）
   对标：garnet/libs/server/Resp/RespServerSession.cs（C# 用 partial class 拆分为多文件）、RespServerSessionSlotVerify.cs、RespServerSessionOutput.cs
   问题：四个文件持续膨胀（3311→3993、1781→1915、1825→1915），storage/session 仍触 wresp（wedb/wnode/src/storage/session/mainstore/main_store_ops.rs:10），下沉前置未解
   改法：aof + storage/session + resp/rangeindex 连体拆（约 17k 行），拆后 facade 保留聚合

5. [P2] key_spec 双模型缺单一转换点
   位置：wedb/wresp/src/key_spec.rs:16（KeySpecificationFlags / BeginSearchMethod / FindKeysMethod / RespCommandKeySpecification 完整版）、wedb/wnode/src/key_spec.rs:8（SimpleRespKeySpec 族简化版，basic_commands.rs / cluster_session.rs / session_parse_state_extensions.rs 消费）
   对标：garnet/libs/server/Resp/RespCommandKeySpecification.cs（完整版）、garnet/libs/server/Resp/RespCommandInfoSimplifiedStructs.cs（简化版）
   问题：两套键规格模型并行，无 From/单一转换点
   改法：建立单一转换点或统一为一套

6. [P2] wcol ObjectInput 包装开销
   位置：wedb/wcol/src/resp/input.rs:91（ObjectInput）、wedb/wcol/src/types/i_garnet_object.rs:18（operate(&ObjectInput, ...)）
   对标：garnet/libs/server/InputHeader.cs（ObjectInput 定义）、garnet/libs/server/Objects/Types/IGarnetObject.cs:Operate
   问题：对象层传参需先包装 ObjectInput，&[&[u8]] 参数须经转换
   改法：operate 直收 &[&[u8]]（消包装）→ ArgSlice offset 化（消 unsafe Send/Sync 裸指针契约）

7. [P2] 五处 src 内集成测试外移 tests/
   位置：wedb/wnode/src/resp/resp_server_session.rs:2725、wedb/wnode/src/txn_resp_commands.rs:493（MockTxnSession:503）、wedb/wpubsub/src/session_commands.rs:506（MockSession:511）、wedb/wcpr/src/manager.rs:967、wedb/wcompact/src/compactor/mod.rs:304
   对标：SKILL.md L98（集成测试放 tests/）
   问题：完整 RESP 流、端到端事务流、PubSub 流、checkpoint 与 compaction 组件级测试留在 src
   改法：移至各 crate tests/ 目录

8. [P3] 手写自旋退避阶梯统一至 wbase::backoff
   位置：wedb/wrecord/src/header.rs:384、wedb/wnode/src/aof/garnet_log.rs:820-828、wedb/wnode/src/aof/sharded_log.rs:58、wedb/wkv/src/read_cache.rs:204（wedb/waof/src/log.rs:517 已用 wbase::backoff，可作样板）
   对标：SKILL.md L18（锁用 parking_lot）— 统一退避同理
   问题：spin→yield 阶梯多处手写，wbase 已有统一实现但采用面不足
   改法：全仓统一使用 wbase::backoff::Backoff

9. [P3] 巨型 match 命令分派表驱动
   位置：wedb/wnode/src/resp/resp_server_session.rs:1012（process_basic_commands）、:1076（process_array_commands）、:1248（process_other_commands）
   对标：garnet/libs/server/Resp/RespServerSession.cs（C# switch + partial class 分散管理）、garnet/libs/server/Resp/Parser/RespCommandHashLookup.cs（hash 快查表）
   问题：数千行 match 样板分派，RespAclCategories（wedb/wnode/src/resp/resp_commands_info.rs:174）未用于路由
   改法：表驱动或数据驱动命令分派

11. [P3] service.rs get_session 注入链收敛
    位置：wedb/wnode/src/service.rs:966-989（attach_transaction_components / set_item_broker / set_runtime_config / set_custom_command_manager / attach_acl / attach_pubsub 逐个注入）
    对标：SKILL.md L78（重复散落面收敛）
    问题：会话依赖注入调用分散，可收敛
    改法：会话依赖打包为结构体一次注入

14. [P3] 高价值测试残项：T7 N>2 并发轮次、T9 真提交周期臂
    位置：wedb/wnode/src/aof/readconsistency/replay_align_barrier.rs:358（现有 4 个单元测试，无 N>2 并发轮次）、wedb/wnode/src/service.rs:462（aof_commit_ms 接线在位，缺真提交 + 周期臂测试）
    对标：garnet/libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs（T7）、test/standalone/AofUpsertStoreCkptRecoverTestAsync（T8 参照）
    问题：T1-T8 已有对应测试落地，唯此两项未覆盖
    改法：补 T7 并发轮次测试与 T9 真提交 + aof_commit_ms 周期臂测试

15. [P3] flush_evict 对抗并发测试根治
    位置：wedb/wkv/tests/store/flush_evict.rs:136（test_adversarial_heavy_concurrency_with_eviction）
    问题：12 线程调度级对抗测试偶发 flaky；.config/nextest.toml 已移除 retries 兜底，仅剩 slow-timeout
    改法：根治竞态或加确定性同步

16. [P3] whlog append_scan 偶发失败
    位置：wedb/whlog/src/hlog/append_scan.rs test_scan_inflight_zero_header_respin
    问题：全量并发跑偶发 FAIL，单跑 -p whlog 稳定过；对并发调度或跨测试干扰敏感
    改法：根治竞态或加确定性同步（处理路径同条 15）
