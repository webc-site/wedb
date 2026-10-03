甄别结论：通过（甄别席 J3，2026-09-27，定级 P3——换代残留根休眠窗坐实，条件触发）。C# 四处换代亲验全命中：GarnetAppendOnlyFile.cs:77（构造）/:118（换代内核）、AofRecover.cs:31（恢复起点）、ReplicaDiskbasedSync.cs:50 与 ReplicaDisklessSync.cs:44（票面审核已订正 :50→:44）。rust 侧 garnet_append_only_file.rs:101 确为生产唯一换代点（:570 系 #[cfg(test)] mod tests 内，:450 起）、:332 内核同构、replay_database_aof（:378 起）AofProcessor::new 前无换代、:446 仅 finally 抬号；残留根 virtual_sublog_replay_state.rs:205-217 双 fetch_max 与消费闸 read_consistency_manager.rs:332-369 亲读成立；喂入面 aof_processor.rs/aof_processor_chunk_replay.rs 皆门下接线在位。b02 重叠面现码核查：replica_sync_session.rs/replication_manager.rs 零 read_consistency/key_sequence_manager 引用，reader-pin 与本票换代面正交，判据不灭失。休眠窗坐实（boot.rs:108-112 硬拒 !=1、node_options.rs 投影零命中、boot 注释自陈扇出另立棒）。派沙箱席 c01b。

审核结论：通过（P3）

逐锚亲验属实：assembly.rs replicate_sync_async 三步（:307 try_add_replica → :314 纪元等待 → :316 attach）间确无换代调用；garnet_append_only_file.rs:101 为生产码唯一换代点（:570 系测试模块）、:332-351 换代内核与 C# GarnetAppendOnlyFile.cs:118 同构（版本+1/旧栅栏 disable）、:378-448 仅 :446 抬号无换代（C# AofRecover.cs:31 入口换代 + :37 finally 抬号成对，rust 缺前半）；virtual_sublog_replay_state.rs:206/:215 fetch_max 单调残留根、read_consistency_manager.rs:347-362 闸向亲读成立——残留高前沿使等待瞬时放行（假新鲜陈旧读）、残留高 mssn 对低前沿使等待目标不可达（持续假超时），两形机制均自洽。喂入面（aof_processor.rs:274-285、aof_processor_chunk_replay.rs:63-76）与消费面（service.rs:2101-2108 建会话挂 ReadSessionState）均生产接线在位、multi_log_enabled 一门同源。

温挂窗现实性核验：现势构建休眠——两拓扑旋钮（aof_physical_sublog_count/aof_replay_task_count）缺省恒 1，runtime_server_options() 投影（wconf/src/node_options.rs:1425-1504）零赋值、无 CLI 旋钮、boot.rs:108-112 对 physical != 1 硬拒，multi_log_enabled 当前不可达，缺陷无现势运行面危害。但机制全数在位且点亮轨道已排：boot.rs:100-107 自陈多子日志扇出「另立棒」，deviations.md §158 第 7 项将读一致性消费链记为在役闭环、多日志在线重放面已有登记在案的取舍条目，且无任何裁决封存 multi_log 恒关——boot.rs 装配门本身即「不可达仍防未来接通漂移」的同类先例。契约完备性缺陷、条件触发、休眠窗，P3 定级恰当。

锚点订正：ReplicaDisklessSync.cs 实为 :44（票面 :50），同语句同位（TryAddReplica 与纪元等待之间），不碍判定；余锚全数命中。

查重：task/ 全树无同题票，deviations.md 无覆盖本缺陷的既有裁决（§158 仅登记漂移双旋钮，不及拓扑旋钮与换代面）。

CLUSTER REPLICATE 挂接入口缺一致性管理器换代对位：跨主温挂后旧代际草图/前沿经 fetch_max 残留，副本一致读闸被污染——假新鲜陈旧读与持续假 ConsistentReadTimeout（C# 挂接点 CreateOrUpdateKeySequenceManager 整缺）

问题分析：
1 Garnet 契约对齐：C# 在四处换代一致性管理器——构造（GarnetAppendOnlyFile.cs:77）、每次恢复起点（AofRecover.cs:31 CreateOrUpdateKeySequenceManager）、两种副本挂接入口（ReplicaDiskbasedSync.cs:50 与 ReplicaDisklessSync.cs:50，位于 TryAddReplica 与纪元等待之间）；换代语义（GarnetAppendOnlyFile.cs:118）新管理器草图归零/前沿归零/版本 = 前代+1，旧管理器栅栏 disable，读者会话经版本变更即重置 mssn 与缓存前沿。
2 工程现状确证：rust 生产码唯一换代点在构造期（wedb/wnode/src/aof/garnet_append_only_file.rs:101）；replicate_sync_async（wedb/wedb/src/server/replication/assembly.rs:286-336）三步「:307 try_add_replica_async → :314 纪元推进等待 → attach 发起」之间无换代调用——C# 挂接入口那行整缺；replay_database_aof（garnet_append_only_file.rs:378-448）仅 finally 位 reset_sequence_number_generator（:446）无换代（AofRecover.cs:31 成对缺；冷启动由构造期 :101 等价，温启二次挂接/二次回放不等价）。副本回放热路径持续喂草图/前沿：aof_processor.rs:279-282 与 aof_processor_chunk_replay.rs:71-74 逐条 update_virtual_sublog_key_sequence_number，virtual_sublog_replay_state.rs:205-217 fetch_max 使旧代际高值永驻 sketch/frontier/cached_sublog_max。读闸消费面在位：read_consistency_manager.rs:332-369 verify_key_freshness。
3 逻辑危害确证：multi_log_enabled 拓扑温启重挂窗——副本先挂主 A 回放累积 A 代际序列号（时间基准可为大值）→ 脱离 → CLUSTER REPLICATE 挂主 B（新晋升生成器起点低于 A 峰值）→ 不换代则 A 代际高值残留：verify_key_freshness 见 mssn < cached 高值即假新鲜跳过等待，B 流尚未重放到位的键被陈旧读（前缀一致破坏）；post-read mssn 被旧值顶高后 B 流前沿长期追不上，跨子日志读持续假 ConsistentReadTimeout。C# 同输入在挂接点整体换代无残留。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/replication/assembly.rs:replicate_sync_async（:286-336 挂接无换代）
wedb/wnode/src/aof/garnet_append_only_file.rs:create_or_update_key_sequence_manager（:332-351，唯一调用 :101）、replay_database_aof（:378-448）
wedb/wnode/src/aof/readconsistency/virtual_sublog_replay_state.rs:fetch_max 残留根（:205-217）
wedb/wnode/src/aof/readconsistency/read_consistency_manager.rs:verify_key_freshness（:332-369）

对应 c# 文件与函数：
garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:50 与 ReplicaDisklessSync.cs:50（挂接换代行）
garnet/libs/server/AOF/Recover/AofRecover.cs:31（恢复起点换代）
garnet/libs/server/AOF/GarnetAppendOnlyFile.cs:118 CreateOrUpdateKeySequenceManager

精炼执行方案：
1 replicate_sync_async 在 try_add_replica 臂后、纪元等待前补 create_or_update_key_sequence_manager()（仅 multi_log_enabled 内生效自带门，单点复用既有换代内核）；replay_database_aof 起点对位补同一调用；冷启重复换代无害（版本 +1 即设计）
2 锁测：2 物理子日志拓扑双节点挂接轮换——挂 A 积累高序列号草图后挂 B，断言管理器版本递增、sketch/frontier 归零、副本一致读不被旧代际值闸死

审核裁定执行方案（审核席整理，供 task/fix.md 直接消费）：
1 挂接点：assembly.rs replicate_sync_async 的 :310（try_add_replica 臂尾）与 :314（bump_and_wait_for_epoch_transition_async）之间补 aof 换代调用，对位 ReplicaDiskbasedSync.cs:50 / ReplicaDisklessSync.cs:44；aof 句柄经 provider 单源取，不另开获取面
2 回放起点：garnet_append_only_file.rs replay_database_aof 在重放分派前（:403 AofProcessor::new 之前）补 create_or_update_key_sequence_manager()，与 :446 reset_sequence_number_generator 收口成对，对位 AofRecover.cs:31/:37；冷启时构造期 :101 已建 v1、此处换代出 v2，恢复先于端点 accept（boot.rs:89）故无在途读者会话，无害成立
3 会话捕获残面（不阻断，随票登记交 fix 席裁决）：rust ReadSessionState 持构造期捕获的 Arc（replica_read_session_context.rs:201），C# 系逐操作现取 appendOnlyFile.readConsistencyManager（ReplicaReadSessionContext.cs:167/:186/:205/:242）——换代前建的老会话感知不到换代、闸面残留至重连。两处换代落地后新会话全保护（挂接先于读者流量的主流程即闭合）；老会话残面或随本票改逐操作现取求全对位、或登记缓议，由 fix 席按最小改动拍板
4 锁测：多日志拓扑（1 物理 × 2 回放或 2 物理子日志）不重启轮换挂接——挂 A 回放积累高序列号草图/前沿 → 直接 CLUSTER REPLICATE 挂 B，断言管理器版本递增、sketch/frontier 归零、跨子日志一致读既不被旧代际高值瞬时放行也不假超时；replay_database_aof 起点换代沿同夹具补断言
5 验收：./test.sh 全绿；clippy 零警告

收口记录（收票席 R4 批次，2026-09-28）：合入 ca94f060（验货 098bfca4）。收口形态=replicate_sync_async 于 TryAddReplica 臂后纪元等待前补 CreateOrUpdateKeySequenceManager 换代（provider.try_aof 单源取柄，对位 C# ReplicaDiskbasedSync.cs:50/Diskless:44）+ replay_database_aof 分派前换代与 finally 抬号成对（AofRecover.cs:31/:37），管理器口对标 C# public 收口三生产换代点，MultiLogEnabled 内部门控单日志恒 no-op。锁测 wnode::aof_recover_reset_seq_num_gen 跨代残留归零 + wedb::replicate_switch_generation 三臂（版本递增/假超时/假新鲜），回装全红实测。缓议备案：换代前已建老读者会话持旧管理器 Arc 残面至重连闭合（C# 逐操作现取），随后续票裁决不登册。

（补记：首次归档 mv 因他席在途合并暂存冲突撤回，重放合入 c986b22e 后闭合）
