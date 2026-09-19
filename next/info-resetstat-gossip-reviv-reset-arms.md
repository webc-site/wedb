INFO RESETSTAT 的 gossip 与 reviv 两条复位臂未接线：监视器消费端只承接 C# 六件事里的前四件

来源：next/glm.my.md 第 11 轮条。取证基线：主仓 /Users/z/git/db/wedb 分支 dev，
行号按符号在当下代码复核（初检 HEAD a7402c4，位点在后续 HEAD 复测未漂移；
仅 cluster_provider 的默认空实现判定需修正，见末段）。

结论

C# 的 INFO RESETSTAT 复位臂在同一个 CleanupGlobalStats 里做六件事：瞬时指标清零、连接计数清零、
全局与历史会话指标清零、逐活跃会话指标清零、clusterProvider.ResetGossipStats()、
storeWrapper.ResetRevivificationStats()。rust 的置位链是通的，消费链只落了前四件，
后两臂在 rust 全仓没有任何生产调用点：gossip 侧真实现已在集群层写好但零消费者，
reviv 侧连实现都是空操作且注释与存储侧事实相反。用户可见后果是 INFO RESETSTAT 之后
gossip 计数不归零（与 C# 与 Redis 口径不一致），并且复活池统计永远无法复位。

现状

1. 置位面在位：/Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session.rs:1745
   monitor.set_info_reset_flag(section)，对位 garnet/libs/server/Metrics/Info/InfoCommand.cs:59-65。
2. 消费面缺两臂：/Users/z/git/db/wedb/wedb/wmetric/src/garnet_server_monitor.rs:324
   cleanup_global_stats 的 Stats 分支只复位瞬时吞吐、连接计数、全局与历史会话指标，
   再回调 reset_active_sessions；:87 MonitorIterationInputs 的字段面只有 servers 加四个复位闭包
   （reset_all_session_latency / reset_active_sessions / reset_active_command_stats /
   reset_session_latency），没有 gossip 与 reviv 的挂点。
3. 装配面同样只有四件：/Users/z/git/db/wedb/wedb/wnode/src/servers/consumer_registry.rs:418
   monitor_iteration_inputs（连接计数与会话指标在 reset_active_sessions 闭包内一并处理）；
   采样任务在 /Users/z/git/db/wedb/wedb/wnode/src/server.rs:817 start_server_monitor 装配，
   该函数与它的调用点 server.rs:283 处 cluster_provider 句柄与 session_provider 均在作用域内
   （server.rs:242 取 cluster_provider、server.rs:256 取 session_provider），补两个入参即可，
   不需要新的全局单例。
4. gossip 导出已通、复位没人调：INFO 段取数走
   /Users/z/git/db/wedb/wedb/wnode/src/resp/info_provider.rs:157 gossip_stats
   （:162 转调 cluster_provider.get_gossip_stats），
   集群层真实现 /Users/z/git/db/wedb/wedb/wedb/src/server/cluster_provider.rs:1651
   reset_gossip_stats（体内 gm.stats().reset()，对位 garnet/libs/cluster/Server/ClusterProvider.cs:142）。
   全仓 grep reset_gossip_stats 只命中三处定义与转发：wnode/src/cluster_provider.rs:125（trait 默认实现）、
   :253（Arc 转发 impl）、上面那条真实现，零调用点。计数源
   /Users/z/git/db/wedb/wedb/wedb/src/server/gossip/gossip_stats.rs:147 reset 本身可用。
5. reviv 侧是空实现加失真注释：/Users/z/git/db/wedb/wedb/wnode/src/database/single_database_manager.rs:145
   reset_revivification_stats 函数体只有一句注释「wkv 无复活化统计面（wkv index 内部化），空操作」，
   trait 声明 i_database_manager.rs:101、impl 转发 single_database_manager.rs:443，同样零调用。
   注释与事实相反：/Users/z/git/db/wedb/wedb/wkv/src/store/mod.rs:97 持
   pub reviv_pool: Arc<wreviv::FreeRecordPool>，池的四个计数在
   /Users/z/git/db/wedb/wedb/wreviv/src/pool.rs:106-112（put/take/hit/drop），
   读改写链路真实在记账（wkv/src/session/raw/mod.rs:132/:153 take 与 put、
   wkv/src/session/raw/write/inplace.rs:37/:297/:484、wkv/src/compact.rs:164、
   wkv/src/session/raw/write/copy_to_tail.rs:142），快照出口 pool.rs:317 stats()。
   池层也没有计数复位入口：pool.rs:344 clear 只逐桶 clear，不动四计数。

C# 参考

garnet/libs/server/Metrics/GarnetServerMonitor.cs:184-213 CleanupGlobalStats
（:209 storeWrapper.clusterProvider?.ResetGossipStats()、:211 storeWrapper.ResetRevivificationStats()）；
复位终点 garnet/libs/server/Databases/SingleDatabaseManager.cs:301 →
garnet/libs/storage/Tsavorite/cs/src/core/ClientSession/ManageClientSessions.cs:91 ResetRevivificationStats
（合并会话统计后清零全局 revivificationStats）；集群终点
garnet/libs/cluster/Server/ClusterProvider.cs:142 ResetGossipStats（gossipStats.Reset()）。

修法

一、wreviv 补计数复位：FreeRecordPool 增 reset_stats（四计数 store(0)，对标
RevivificationStats.Reset），与既有 clear 区分职责（clear 清槽、reset_stats 清账），
经 WedbStore 暴露一个 reviv 复位薄入口（store 已 pub reviv_pool，wnode 侧
SingleDatabaseManager 可达 self.db.store）。
二、single_database_manager.rs:145 由空实现改为真正下发复位，并删掉「wkv 无复活化统计面」
的失真注释（保留 C# 锚点注释）。
三、MonitorIterationInputs 增两条复位闭包（或按 C# 的 provider 语义直接注入
cluster_provider 与 database_manager 句柄），wnode 装配点 consumer_registry.rs:418 与
server.rs:817 补挂 reset_gossip_stats 与 reset_revivification_stats，使 STATS 分支
与 C# 的六件事一一对应。闭包形态与现有四个一致，保持监视器 crate 不反向依赖 wnode。
四、原报「wnode/src/cluster_provider.rs:125 的 trait 默认空实现顺带成为死面，可删」的判断
不采纳：该默认实现是 C# storeWrapper.clusterProvider 为 null（单机无集群形态）的 rust 对位，
接线后它正是单机下的空操作落点，删掉反而要为每个消费者各写一次 Option 判空。

优先级

功能缺口（用户可见的 INFO 语义与 C# 不一致：RESETSTAT 后 gossip 计数不清零），
连带一处死面（零调用的真实现）与一处失真注释。不涉及多套架构，改动面小，可独立成棒。

边界

next/info-store-snapshot-channel.md 只管 INFO 存储域段的导出通道（SessionInfoSource::databases
恒空集），其射程是「取数与填段」，不含 RESETSTAT 的消费臂；本单只补复位挂点与池层复位原语，
不动 STOREREVIV 段的导出（全仓 grep STOREREVIV 零命中，该段本身尚未落地）。gossip 段导出面
在代码侧已通（info_provider.rs:157 与 wedb/src/server/cluster_provider.rs:1608 实现在位），
本单不重开导出；他档所称该并档工作的归档件 task/done/info-cluster-segments-provider-homing.md
在当下 task/done/ 内不存在，本单不以它作为「已完成」凭据。

并发双花登记

同一题面另有一份同名的单问题派发档 next/info-resetstat-gossip-reviv-reset-arms.md
（拆分自本单同一来源档，面更全：含 gossip 与 reviv 两臂、注释失真、默认空实现三点，
与本单裁决一致），两份取一实施即可。特别提示编排方：这两份文档文件名完全相同、
仅目录不同，若按「git mv next/<slug>.md task/ing/<slug>.md」的认领流程会撞同名目标，
须先删派发摘要或改走本细化单，切勿把两档都派给不同 worktree 各改一次监视器装配面。

验收

1. INFO RESETSTAT 后 INFO STATS 段的 gossip 计数（以及 STOREREVIV 一旦导出）回落为 0，
   未 RESETSTAT 时不受采样轮影响。
2. 全仓 grep reset_gossip_stats / reset_revivification_stats 各至少一处生产调用点。
3. wreviv 的 clear 与 reset_stats 职责不混（现有 wreviv/tests/main.rs 的 stats 断言保持通过）。
4. cargo check --workspace --all-targets 零告警，禁写 allow。

盘点补记（qw13.invA info-resetstat-gossip-reviv-reset-arms）：dev e75716e 复核原样：reset_gossip_stats 仍只有 wnode/src/cluster_provider.rs:271 trait 转发与 wedb/src/server/cluster_provider.rs 真实现，零生产调用；single_database_manager.rs:166-167 reset_revivification_stats 仍是空体 + 「wkv 无复活化统计面」注释（与 wkv/src/store 持 reviv_pool 的事实相反，reviv 旋钮现已接 wconf/service 装配链，注释失真加剧）；wreviv/src 无 reset_stats。三面接线修法不变。
