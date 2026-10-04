终态注记（2026-09-30 收口）：合入哈希 6bac34b（merge --no-ff fix-shard-notify），实现 e9c3f2c。收口形态：PubSubMailbox::shard_forced_unsubscribe 臂独走 try_publish_control 有界等待重试兜底（自旋+yield 让出、逐邮箱预算 NOTIFY_WAIT_ROUNDS=0x100、成功入列即回充、入列即止），超界 warn 留痕放弃并递增 dropped_shard_notify 可观测计数；悬挂定性按复核席约束收窄为连接同 ns 生存期内（AUTH 换租/会话释放整体清退收口）；resp3 自推窗臂有界必终止、严禁无限等待。保序不变（邮箱单队列，通知恒先于其后到站数据帧出列）；drain 臂递减与收旗逻辑零改动；数据面 §14 满水位丢尾策略一字不动，deviations.md §14 澄记分账。测试：subscriber.rs 4 新单测（保序/腾位入列/有界放弃计数/零额度不饿死）+ tests/shard_notify_full_mailbox.rs 两全链路（真实邮箱与 broker），subscribe_broker 既有槽迁出测试与 mailbox_watermark/session_commands_unit/cluster_mgmt_epoch_drain_warn 回归全绿。

甄别结论:通过(P2,2026-09-30 现码复核,双侧锚亲验成立,五池无重复,deviations 无同面在册裁决)

审核结论：通过（2026-09-30 甲轮45-G，P2 级）。分片槽迁出退订通知 ShardUnsubscribe 与普通数据帧共用满即丢弃邮箱，满水位丢失通知令会话 is_subscription_session 永久悬挂事实确证，导致客户端被白名单门永久阻断。执行席遵照：控制通知入列改短程有界等待重试（自旋+yield 让出，有界次数），超界 warn 留痕，消除永久死状态；数据面满水位丢尾策略一字不动。

复核席注记（2026-09-30 独立复核同向通过，危害链七环逐点实证；自旋死锁核验：发布端三处调用与消费端恒跨任务，自旋仅持 papaya epoch pin 无锁序反转）：三点执行约束——一、悬挂定性收窄为「连接同 ns 生存期内」（AUTH 换租 materialize_authenticated_handle auth.rs:353-359 与 dispose core.rs:848-850 可整体清零，非绝对永久，票面「仅能断开重建」略欠精确）；二、有界自旋的「超界放弃」臂为自推窗唯一兜底（RESP3 订阅会话不过白名单门 core.rs:1079，自身发 SETSLOT 命中自身满邮箱时同任务无人在排空），严禁实现成无限等待；三、目标会话深陷慢命令横跨自旋预算时仍会放弃丢帧，warn+可观测计数是最终兜底，测试用例须覆盖放弃臂断言可观测计数。近邻已毕票 no-slot-anchor-hang-on-migration（a9ed7ea）钉钩缺失面，本票系其推帧臂残留缺口，面不重叠。

原票面：
槽迁出强制退订通知与数据帧共邮箱同丢弃策略,满水位丢通知令会话订阅态永久悬挂

问题分析：
1. Garnet 契约对齐：分片域为 rust 自有面(Redis pubsub.c pubsubShardUnsubscribeAllChannelsInSlot 对位,C# SubscribeBroker 无 shard 表);C# 广播线程对订阅会话同步直写发送器,满经 GarnetTcpNetworkSender throttle.Wait 阻塞发布端,推送零丢弃,不存在「服务端主动状态通知被丢」的形态。会话级整体清退面(AUTH 换租 materialize_authenticated_handle、dispose 的 remove_subscription)就地摘订阅清零计数并排空邮箱,无悬挂;本票只涉槽事件钩的邮箱通知臂。
2. 工程现状确证：shard_slot_migrated_out(wedb/wpubsub/src/subscribe_broker.rs)摘锚清表后经 sink.shard_forced_unsubscribe 入会话邮箱,该控制通知与普通数据帧共用 PubSubMailbox 同一有界队列、同一满即拒收丢尾策略(subscriber.rs enqueue 吞 try_publish 的 false,失败臂仅递增 dropped 且不 notify);会话侧 drain_pubsub_frames(wedb/wpubsub/src/session_commands.rs)唯一凭 ShardUnsubscribe 帧递减 num_active_channels 并收口 is_subscription_session。通知帧被丢后 broker 侧订阅与锚均已摘除,显式 SUNSUBSCRIBE 走 channel_sub_remove 返回 false 不减计数,空参全退枚举不到该频道,计数悬挂无任何自愈路径;重订再退也只能在同值上加减,漂移量永久保留。
3. 逻辑危害确证：慢订阅者邮箱打满(积压达 DEFAULT_MAILBOX_CAPACITY)恰逢其锚定槽迁出,通知帧被丢,会话 num_active_channels 永久多计一,is_subscription_session 恒真: RESP2 客户端被订阅白名单门(wnode/src/resp/parser/resp_command.rs is_allowed_in_subscription_mode,core.rs 订阅门臂)永久拦宠除订阅族/PING/QUIT 外全部命令,连接半死仅能断开重建;CLIENT TYPE 恒报 pubsub;后续 SUBSCRIBE/退订应答的活跃计数整体漂移。状态机落入悬挂死状态,违审查核心维度 4.2 状态单向闭环;危害与甲轮33 已扫的换租摘订阅面无关(该面整体清退无此窗),亦不在 §6/§7/§14 任一在册判据文本内。

涉及代码：
rust 文件与函数：
wedb/wpubsub/src/subscribe_broker.rs: shard_slot_migrated_out、shard_unsubscribe
wedb/wpubsub/src/subscriber.rs: PubSubMailbox::try_publish、PubSubSink for PubSubMailbox(shard_forced_unsubscribe 臂)
wedb/wpubsub/src/session_commands.rs: PubSubSessionCommands::drain_pubsub_frames(ShardUnsubscribe 臂)、unsubscribe_family
wedb/wnode/src/resp/parser/resp_command.rs: is_allowed_in_subscription_mode
wedb/wnode/src/resp/resp_server_session/core.rs: 订阅模式门臂(is_subscription_session 判定处)

对应 c# 文件与函数：
garnet/libs/server/PubSub/SubscribeBroker.cs: SubscribeBroker.Broadcast(直写发送器零丢弃对位)
garnet/libs/common/Networking/GarnetTcpNetworkSender.cs: 发送节流背压面(throttle.Wait)

精炼执行方案：
1. ShardUnsubscribe 通知入列单点改有界等待: shard_slot_migrated_out 推帧臂对满拒收做短程重试(自旋+yield 让出,有界次数),入列即止;超界 warn 留痕放弃并递增可观测计数——控制面低频路径,有界等待不成新死锁源,数据面满水位丢尾策略(§14)一字不动
2. 保序不变: 通知仍走邮箱单队列,恒先于其后到站数据帧出列,drain 臂帧序与计数递减逻辑零改动
3. 测试验证: wpubsub 测试域增满水位窗用例,邮箱打满后调 shard_slot_migrated_out,断言通知最终入列(或超界放弃可观测),排空邮箱见 ShardUnsubscribe 帧、活跃计数归零、订阅旗收口;subscribe_broker.rs 既有 shard_slot_migrated_out 测试不回退
