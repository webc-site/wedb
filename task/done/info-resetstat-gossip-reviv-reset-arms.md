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

## 判词（棒：info-resetstat-arms，树 /tmp/fork/info-resetstat-arms，CARGO_TARGET_DIR=/tmp/ct-isr）

结论：票面五段现状与盘点补记逐条复核为真（未落地、C# 形态无反证），按修法一~四实施完毕，
内容提交 51b0a9d，经三次树内 merge dev（75f0ba8 并入 cd44779、f74e7df 并入 523b234、
5fb1621 并入 c0ef376）后由主仓 git merge --ff-only 快进至 dev（tip 5fb1621，HEAD 已含）。
行号按当下代码重取（票面 a7402c4/e75716e 期行号已漂移，位点同函数）：
resp_server_session.rs:1745 现 :1769；single_database_manager.rs:145 现 :166（复核时）；
cluster_provider.rs:125/:253 现 :135/:270；consumer_registry.rs:418 现 :469；
server.rs:817 现 :857；info_provider.rs:157 现 :173；pool.rs:106-112 现 :107-112。
gossip_stats.rs 真实现随 cluster_provider.rs 拆分为 server/cluster_provider/traits.rs，现 :513。

落地（file:line 为主仓 dev 现刻）

1. 修法一 池层复位原语 + 存储薄入口：wreviv 无此口，现 wedb/wreviv/src/pool.rs:373
   FreeRecordPool::reset_stats（四计数 store(0, Relaxed)，锚 RevivificationStats.cs:Reset），
   :396 将 clear 文档改注「只清槽不动账目，计数复位见 reset_stats」；
   wedb/wkv/src/store/stats.rs:286 WedbStore::reset_revivification_stats 转调
   :287 reviv_pool.reset_stats()，并注明 C#「先并活跃会话账」在 rust 无对应物
   （复活账唯一源即池四计数）。
2. 修法二 真下发：wedb/wnode/src/database/single_database_manager.rs:170-171 函数体
   改为 self.db.store.reset_revivification_stats()，删除「wkv 无复活化统计面（wkv index
   内部化），空操作」失真注释，保留 C# 锚 SingleDatabaseManager.cs:ResetRevivificationStats；
   trait 声明 :101（i_database_manager.rs）与转发 :481 零改动。
3. 修法三 监视器两臂 + 装配补挂：
   wedb/wmetric/src/garnet_server_monitor.rs:87-110 MonitorIterationInputs 增 F5/F6 两泛型参
   （默认 fn()）与字段 :107 reset_gossip_stats / :109 reset_revivification_stats；
   :336 cleanup_global_stats 增两形参，STATS 分支在 reset_active_sessions() 之后、
   清标志之前按 C# :209/:211 同序调用 :367 / :368；:427 monitor_iteration 与
   :484 main_monitor_task_async 泛型面同步扩至 F5/F6。本 crate 不持集群/存储句柄，零反向依赖。
   wedb/wnode/src/servers/consumer_registry.rs:472 monitor_iteration_inputs 增两形参
   （:474/:475）并落入结构体（:525/:526）——全仓 MonitorIterationInputs 字面量唯一构造点
   （grep 仅命中此处），未起第二套挂点、未写兼容层。
   wedb/wnode/src/traits.rs:162 SessionProviderFace::reset_revivification_stats 默认空操作
   （对位 StoreWrapper.cs:ResetRevivificationStats），:1715-1716（service.rs）
   StorageSessionProvider 下发 database_manager；
   wedb/wnode/src/server.rs:869 start_server_monitor 增 cluster_provider: C 与
   session_provider: Arc<P> 两入参，采样任务每轮重建两臂闭包各持一份句柄克隆：
   :909 gossip.reset_gossip_stats()、:910 reviv.reset_revivification_stats()；
   调用点 :262-270 补两实参（cluster_provider.clone() 与 :258 先行取走的
   Arc::clone(&session_provider)），未引入全局单例。
4. 修法四：wedb/wnode/src/cluster_provider.rs:135（trait 默认空实现）与 :270（Arc 转发）
   零改动，接线后即单机形态（C# clusterProvider 为 null）的下臂落点。

验收四条逐条取证

1. gossip 计数回落 0 且非复位轮不受影响：新增 wedb/wedb/tests/info_resetstat_arms.rs:75
   test_resetstat_arms_zero_gossip_stats（真 wedb::ClusterProvider + 真 GarnetServerMonitor +
   真 ConsumerRegistry，经 INFO 取数面 get_gossip_stats 断言 meet_requests_recv /
   gossip_bytes_send 在 RESETSTAT 轮为 "0"，前后共五轮对照）；
   wedb/wnode/tests/server_monitor_tests.rs:307 断两臂触达次数与时机（0 → 1 → 1）；
   wedb/wnode/tests/database_manager.rs:263 断 reviv 账目真落库（四计数归零 + 不清槽 +
   trait 面同效）。STOREREVIV 段导出仍未落地（本单射程外，票内边界已载）。
2. 生产调用点：reset_gossip_stats 现 server.rs:909（监视器 STATS 分支另于
   garnet_server_monitor.rs:367 调回调）；reset_revivification_stats 现 server.rs:910 →
   service.rs:1716 → single_database_manager.rs:171 → store/stats.rs:287 → pool.rs:373。
3. clear 与 reset_stats 职责分立：新增 wedb/wreviv/tests/main.rs:69
   reset_stats_zeroes_counters_and_clear_keeps_them（clear 后账目原样、reset_stats 后槽位
   仍可复活）；既有 smoke_pool_lifecycle_and_slack_allocation 的 stats/clear 断言零改动，
   wreviv 全套 25/25 通过。
4. 门禁：树内 cargo check --workspace --all-targets exit=0 且零告警（未新增 allow，
   diff 内 grep allow( 零命中）；定向 nextest：wreviv 25/25、wconf + wnode
   （server_monitor_tests / database_manager / client_commands_tests / resp_info）22/22、
   wedb（info_resetstat_arms + gossip_manager）10/10、合并 wkv 改动后 wkv 225/225 全绿。
   树内三次 merge dev（75f0ba8 并入 cd44779 wkv 读内核收敛、f74e7df 并入 523b234 归档批次、
   5fb1621 并入 c0ef376 wconf 注释锚批次）后两次复跑上述门禁全绿。

顺带处置：cleanup_global_stats 文档里同一 GarnetServerMonitor.cs:CleanupGlobalStats 锚
被重复粘了两遍（存量误粘），本次改写该 doc 时合并为一枚，不构成第二挂点。
未做：不动 STOREREVIV 导出、不动 js/check 语料（ResetRevivificationStats 现仍列
server.yml:877 与 storage.yml:748/:799 忽略册，待主代理门禁回写）。
