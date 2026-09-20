任务名称: failover-candidate-select

问题描述
1. 主端 failover 候选副本抢占短路与假死:
   wedb/wedb/src/server/failover/primary_failover_session.rs 的 wait_for_first_replica_sync_async 中，offset_rx.recv() 仅等待首条应答，若首个返回的副本位点未追平，直接返回 None，导致 failover 假死。需在超时窗口内循环等待首个追平的合格副本。
2. FailoverManager 主端故障转移状态丢失:
   wedb/wedb/src/server/failover/failover_manager.rs:try_start_primary_failover 启动前未设置 BeginFailover，协程结束未回写 FailoverCompleted/FailoverAborted。
3. FailoverManager::reset 解锁与通知缺失:
   reset 流程需重置 failover_task_lock 并触发 event.notify。

实现规划
1. 优化 wait_for_first_replica_sync_async 循环接收直到追平或超时。
2. 完善 FailoverManager 状态更新与 reset 解锁。
3. 补充相应单测。
4. 运行 cargo check 确保编译通过。
5. 审查优化代码。
