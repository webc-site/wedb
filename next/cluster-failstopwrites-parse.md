任务名称: cluster-failstopwrites-parse

问题描述
1. CLUSTER FAILSTOPWRITES 非法参数静默触发从节点复位:
   wedb/wedb/src/server/cluster_session/failover.rs 的 network_cluster_fail_stop_writes 中，hex_u128(args[0]) 在非空非法时返回 None 误当成空参执行复位。需区分空参与非空非法参数。
2. FAILSTOPWRITES 同步自旋造成 Reactor 调度饥饿:
   避免在 compio 反应器线程上直接同步忙等，转换为异步驱动或调度友好让步。

实现规划
1. 校验 args[0]，非空非法 hex 时返回语法错误。
2. 调度推进优化。
3. 补充对应单测。
4. 运行 cargo check 确保编译通过。
5. 审查优化代码。
