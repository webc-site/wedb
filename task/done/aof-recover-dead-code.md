# aof-recover-dead-code

来源：next/glm.md 条目 54/55（已删）、next/design.md 条目 12（已删）、条目 5 AOF 行（已删）。

## 核实结论（全仓 src+tests grep）

1. recover 并行化残骸
   - recover_replay_task.rs 整文件死（replay_page 仅被 recover_replay_task_async 调，后者零外部调用）→ 删文件并清 mod 声明
   - recover_log_driver.rs 混活：new/run 被 aof_recover.rs（AofRecover::recover_replay_driver/single_log_recover，主链 service.rs:440、garnet_append_only_file.rs:440 在用）
   - 死面：consume、throttle、replayed_record_count 访问器、create_and_run_intra_page_parallel_replay_tasks、PageReplaySlice、physical_sublog_idx 访问器（删 task 文件后零调用）
   - replayed_record_count 字段改 run() 局部计数（顺序驱动无并发写）
2. aof_replay_coordinator.rs：BarrierKey::equals、clear_session_txn、take_transaction_group、process_synchronized_operation 零调用（get_barrier/try_remove_barrier 为 aof_processor.rs:649/662 生产面，保留）
3. read_consistency_manager.rs:292 update_key_sequence_numbers（批量版）零调用；单数版/by_hash 版为生产面保留
4. replica_read_session_context.rs 两项 = cached_len、key_hash_cache_len（发明访问器，外部零调用；waiter() 有生产调用 manager:342 不删；expand/shrink_key_hash_cache 有 C# 对标+自测，不动）
5. aof_backpressure.rs:224 set_log 零调用（set_weak_log 为装配面 garnet_append_only_file.rs:91 在用）
6. aof_processor.rs：invalid_aof_address 自由函数版（方法版在 garnet_append_only_file.rs:180 生产在用）、with_sub_id、with_obj_type 零调用（sub_id/obj_type 字段为编码布局保留，service.rs:247 字面量直写）
7. wnode 依赖 wcpr：src 仅 garnet_api.rs:369/661/669 注释提及，检查点实际经 wkv（checkpoint.rs:381 wcpr::CprStore）通道 → cargo remove wcpr -p wnode，注释措辞改为 wkv 检查点通道

## 改动

- 删 wedb/wnode/src/aof/recover/recover_replay_task.rs + mod.rs 声明
- 删 recover_log_driver.rs 死面（保留 struct/new/run，计数局部化）
- 删 aof_replay_coordinator.rs 四符号
- 删上述 3-6 各符号
- js/check/ignore/server.yml 登记：RecoverLogDriver.cs 的 Throttle/Consume/ReplayedRecordCount/CreateAndRunIntraPageParallelReplayTasks、RecoverReplayTask.cs 的 RecoverReplayTaskAsync/ReplayPage、AofReplayCoordinator.cs 的 Equals/ClearSessionTxn/ProcessSynchronizedOperation、AofBackpressure.cs 的 SetLog（理由：Rust 已决策顺序恢复/单通道，无需此面）
- cargo remove wcpr -p wnode + garnet_api.rs 注释清理

## 验证

- bun ./js/check.js：0 缺失 0 重复
- ./clippy.sh：0 警告
- ./test.sh：全过

## 结果

- 删除明细（11 文件，+46/-275）：
  - recover_replay_task.rs 整文件删（recover_replay_task_async、replay_page），mod.rs 清声明
  - recover_log_driver.rs 删 consume/throttle/replayed_record_count 访问器/create_and_run_intra_page_parallel_replay_tasks/PageReplaySlice/physical_sublog_idx 访问器；计数局部化；保留 new/run 活面
  - aof_replay_coordinator.rs 删 BarrierKey::equals、clear_session_txn、take_transaction_group、process_synchronized_operation
  - read_consistency_manager.rs 删 update_key_sequence_numbers（批量版）
  - replica_read_session_context.rs 删 cached_len、key_hash_cache_len（两项即此；waiter() 有生产调用未删，测试改直接读私有字段）
  - aof_backpressure.rs 删 set_log + 连带死变体 LogTailHandle::GarnetStrong
  - aof_processor.rs 删 invalid_aof_address 自由函数版、with_sub_id、with_obj_type
- wcpr：cargo remove wcpr -p wnode 成功；garnet_api.rs:369/661/669 注释措辞改为 wkv 检查点通道
- ignore 登记 server.yml 4 条：RecoverLogDriver.cs（Consume/Throttle/ReplayedRecordCount/CreateAndRunIntraPageParallelReplayTasks）、RecoverReplayTask.cs（RecoverReplayTaskAsync/ReplayPage）、AofReplayCoordinator.cs（Equals/ClearSessionTxn/ProcessSynchronizedOperation）、AofBackpressure.cs（SetLog）
- design.md 条目 5 处理 AOF 行后剩 3 行（resp 命令面、会话/输出面、RangeIndex 复制状态面）
- 验证：bun ./js/check.js 0 缺失 0 重复；./clippy.sh 0 警告；./test.sh 2039 passed + 1 skipped + regress 2 passed（whlog::append_scan::test_scan_inflight_zero_header_respin 首轮 flaky，单独重跑与全量重跑均过）
- 异常留档：任务期间主仓库 .git 被并发代理重建（worktree 元数据丢失），已在 4b74625 上重建 worktree 并重放改动后重新三验证；共享 /tmp/rust 缓存被并发 worktree 踩踏产生幻影编译错误（E0460/E0432），用独立 CARGO_TARGET_DIR 隔离验证

