终态：已合入 dev（2026-09-27）。a9ed7ea ShardAnchor 锚表随订阅者走,SSUBSCRIBE slot_of 单源登锚,SETSLOT/SETSLOTSRANGE/relinquish 三臂收口钩,回滚臂不挂;sunsubscribe 帧回推;双宿主真 socket 锁测

甄别结论：通过 | 定级 P3 | 2026-09-27 zcode fixloop 甄别席（双侧锚现码亲验，零撞票）
执行提示：slot_of 单源+经 sink 邮箱守会话单写者

审核结论：通过（P3）
定级理由：自有域漏项而非 C# 契约分叉（C# 无分片域，对齐 yardstick 为 §6 声明的 Redis 标准语义）；触发依赖运维面重分片（SETSLOT/迁移）控制面操作，非数据面常态路径；无数据丢失、无 panic、无账本失真，客户端重订可自愈；危害为静默饿死加投递集按入口分裂，属集群模式功能正确性缺口，不入 P2。
复核席反证排查补记（独立重验，结论与首审一致）：间接触发通道全量闭环——SETSLOT 两臂（slot_mgmt.rs:366 network_cluster_set_slot / :436 network_cluster_set_slots_range，落 cluster_manager_slot_state.rs try_prepare_* 与 try_reset_slot_state）、CLUSTER RESET 慢路径（basic.rs:132 cluster_reset_slow → try_reset）、FAILOVER 面（failover/）、REPLICAOF 面（replica_of.rs）均零订阅触碰；危害链双臂复核成立：收端本地臂 publish_shard_now（basic.rs:667）直投本节点分片表，转发臂取本节点所属 shard 成员（cluster_manager.rs:362 get_node_ids_for_shard），故订阅在 A、槽权移 B 后 B 入口投递不到、A 入口本地臂仍喂到。Redis 外证坐实：pubsubShardUnsubscribeAllChannelsInSlot 在 redis/src/pubsub.c 定义、cluster.c 槽权移交路径调用，对槽内分片频道全体退订并推 sunsubscribe（github redis/redis unstable 可查），§6 对齐声明下本票成立。查重复核：§51/§65（deviations.md:757/:888）钉集群 PUBLISH 先发后订投递窗，§6 三表分离、§7 SUNSUBSCRIBE 补齐，均不覆盖订阅生命周期对槽事件面，五池查重净。
（首审记录：rust 锚点亲验：subscribe_broker.rs:186 裸隔离键、session_commands.rs:130-133 仅集群会话门、slot_mgmt/migrate 全域零订阅清理钩；C# 验伪：garnet 全库无 SSUBSCRIBE 对位机制，属 rust 自建面按板块 4.2 状态闭环收口，§6 只登记三表隔离不豁免迁移收口；五池查重净。订正已并入：auth.rs 锚改 :355、方案补 sink 邮箱投递纪律守会话单写者）

分片订阅无槽位归属绑定：SSUBSCRIBE 订阅表无槽锚、槽迁移/SETLOT 全路径零订阅清理钩，槽迁走后原节点订阅者悬挂饿死无 sunsubscribe 推送（rust 自有分片域内状态悬挂漏项，Redis 标准在 SETLOT 有 pubsubShardUnsubscribeAllChannelsInSlot 收口）

问题分析：
1 Garnet 契约对齐：C# 无分片域（garnet/libs/server/PubSub/SubscribeBroker.cs 仅 subscriptions+patternSubscriptions 两表，SSUBSCRIBE 复用普通频道表），无对位函数；§6 已把分片域登记为 rust 自有设计并声明「向 Redis 标准语义」对齐——Redis 标准在 SETLOT 丢槽路径有 sharded 订阅清理与 sunsubscribe 推送（redis src/pubsub.c pubsubShardUnsubscribeAllChannelsInSlot 挂 setslot），本面缺该收口，属自有域内状态悬挂漏项而非 C# 契约分叉。
2 工程现状确证：分片订阅表按裸隔离键挂无槽位维度（wedb/wpubsub/src/subscribe_broker.rs:186 shard_subscriptions: ChannelSubscriptions<S>）；SSUBSCRIBE 门只有集群会话门无槽归属校验（wedb/wpubsub/src/session_commands.rs:130-133）；SPUBLISH 转发目标取「本节点所属 shard 成员」现取配置（wedb/wedb/src/server/cluster_manager.rs:356-362 conf.get_node_ids_for_shard()）；槽迁移/SETLOT 全路径零订阅清理钩——slot_mgmt.rs 与 migrate_driver 全域 grep pubsub/shard/subscribe 零命中，remove_subscription 唯二调用点为会话 dispose（core.rs:768）与 AUTH 换租防御（auth.rs:355），均与槽位事件无关。
3 逻辑危害确证：客户端在 A 节点 SSUBSCRIBE ch（当时 A 持槽）→ 槽迁移 A→B → 此后 SPUBLISH 仅按当前拓扑转发至 B 侧 → A 的订阅者永收不到消息：无 sunsubscribe 推送、无报错、订阅帧 ack 后永久悬挂；且同一通道投递集随发布者入口节点而异（basic.rs:667 本地臂使非 owner 入口仍可喂到 A 的悬挂订阅者），投递语义按入口分裂。与乙轴 CLUSTER PUBLISH 转发面（已在册）不重叠——本票钉在 SSUBSCRIBE 订阅生命周期侧（broker 表无槽锚加槽事件零钩）。

涉及代码：
rust 文件与函数：
wedb/wpubsub/src/subscribe_broker.rs:shard_subscriptions（:186 无槽锚）
wedb/wpubsub/src/session_commands.rs:SSUBSCRIBE 门（:130-133）
wedb/wedb/src/server/cluster_manager.rs:SPUBLISH 转发目标现取（:356-362）
wedb/wedb/src/server/cluster_session/slot_mgmt.rs 与 wedb/wedb/src/server/migration/migrate_driver/:槽事件零订阅钩（全域 grep 零命中）

对应 c# 文件与函数：
N.A.（C# SubscribeBroker.cs 无分片域；rust 自有域，Redis pubsubShardUnsubscribeAllChannelsInSlot 为语义对齐源）

精炼执行方案：
1 槽位事件接订阅收口钩：SETLOT/迁移完成臂对失槽节点按 pubsubShardUnsubscribeAllChannelsInSlot 语义清 shard_subscriptions 内该槽通道并向下行会话推 sunsubscribe 帧（复用既有移除单点 remove_subscription 的会话通知通道，不新增第二套投递机制）；投递纪律：强制清理不得在 broker/迁移线程直改会话 num_active_channels（违会话单写者），应经既有 sink 邮箱推 sunsubscribe 帧、由会话 drain 臂本地递减；注意 :302-325 实测已摘键不再递减，空参 UNSUBSCRIBE 臂遍历表重算形不成立
2 锁测：A 节点 SSUBSCRIBE → 槽迁至 B → 断言 A 侧订阅被清且客户端收 sunsubscribe 帧；迁移回滚臂订阅不误清

审核裁定执行方案（复核席整理，修正原方案两处，供 task/fix.md 直接消费）：
0 前置裁定：原方案「清 shard_subscriptions 内该槽通道」缺选择判据——本仓废除键级定槽，slot 单一真值源为 wbase/src/hash_slot.rs:62 slot_of(namespace, db)，:20-21 明令禁止从键内容推导槽位；严禁为选「该槽通道」引入频道名 CRC16 取槽（触红线「引入双机制」）；槽锚只能取自仓库自有单一定槽源。备选的「声明入口分片域即语义、登记偏差不修」不取：§6 已声明向 Redis 标准对齐，静默悬挂无收口违板块 4.2 状态闭环
1 订阅挂锚：SSUBSCRIBE 成功臂对每条新增分片订阅记录槽锚 slot_of(会话 namespace, 会话现役库)，锚挂订阅者记录（不挂频道外层键——broker 频道键只折叠 ns 无 db 维度，同频道不同现役库的订阅者锚可异，锚必须随订阅者走）；取槽只经 slot_of 单点，不新增第二套取槽路径
2 槽事件收口钩：SETSLOT Node 权移臂（cluster_manager_slot_state.rs:208 try_prepare_slot_for_ownership_change，源侧失槽即此臂）与迁移完成臂，对本节点失出的槽位收集命中锚的分片订阅者，清表并向下行会话推 sunsubscribe 帧：经既有 sink 邮箱单通道投递（帧形复用 session_commands.rs write_unsubscribe_frame：sunsubscribe+裸名+剩余计数），严禁 broker/迁移线程直改会话 num_active_channels（守会话单写者，由会话 drain 臂本地递减）；同频道未命中锚的订阅者不误清
3 锁测（夹具现成：wedb/wedb/tests/cluster_migration.rs、cluster_pubsub_peer_shutdown.rs）：A 节点会话 SSUBSCRIBE（锚槽 S）→ SETSLOT S NODE→B → 断言 A 侧订阅被清且客户端收 sunsubscribe 帧；锚槽未动的订阅者不受影响（部分清理正确性）；SETSLOT STABLE 与迁移回滚臂零清理副作用
