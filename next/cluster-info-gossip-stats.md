任务名称: cluster-info-gossip-stats

问题描述
wedb/wedb/src/server/cluster_manager.rs:get_info 中，cluster_state 恒定硬编码为 ok，未反映 fail 槽位；且 Gossip 统计指标恒输出 0。
需根据 slot_state_counts 中 fail 状态动态输出 cluster_state，并接入 GossipManager 的真实统计指标。

实现规划
1. 动态判断槽位 fail 状态并反映到 cluster_state。
2. 对接 GossipManager 统计计数。
3. 补充对应单测。
4. 运行 cargo check 确保编译通过。
5. 审查优化代码。
