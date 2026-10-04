终态注记: 已合入 main（commit: 3ed830d）。收口形态：PubSubSink 四方法返回类型改为 bool，PubSubMailbox enqueue/enqueue_control 透传 bool；channel_sub_broadcast 仅在 deliver 返回 true 时累加计数；broadcast 模式订阅与 broadcast_shard 实达计数收敛；订正 tests/mailbox_watermark.rs 满水位回执断言为 0，dropped() 计数不变。

甄别结论:通过(P3,2026-09-30 现码复核,双侧锚亲验成立,五池无重复,deviations 无同面在册裁决)

审核结论：通过（2026-09-30 甲轮45-F，P3 级）。channel_sub_broadcast 内部 deliver 闭包忽略 PubSubMailbox::try_publish 返回的 bool，满水位拒收丢弃仍无条件累加 count 导致回执数虚高事实确证。执行席遵照：deliver 闭包透传 try_publish 的 bool，仅入队成功才累加 count；§14 邮箱丢弃机制与 dropped 计数原样保留，仅收口回执回显契约。

复核席注记（2026-09-30 独立复核同向通过，C# 零丢弃对位 SubscribeBroker.cs:88-89/:110-111 + GarnetTcpNetworkSender.cs:316-321 throttle.Wait 均核实）：三点执行补充——一、bool 吞点在 PubSubSink trait 边界而非 deliver 闭包本体（subscriber.rs:59-68 三方法返回单元、enqueue :168-175 吞 bool），最小执行面含 trait 三方法签名改 bool + 两实现方（PubSubMailbox 四臂与 Arc<T> 三臂，全仓唯二实现）透传 + channel_sub_broadcast/broadcast 两臂/broadcast_shard，勿只改闭包；二、wpubsub/tests/mailbox_watermark.rs:107-110 既有钉测逐帧断言满位窗 publish_now == 1，修复后该窗回执为 0 断言必红，须随修复同步订正（:103 容量内、:116 排空后、:117 单调计数断言不动）；三、集群收端 basic.rs:655 network_cluster_publish 丢返回值且 C# 同无应答，无需改动。

原票面：
PUBLISH/SPUBLISH 通知数把满水位拒收帧计为已通知,回执契约虚高

问题分析：
1. Garnet 契约对齐：C# SubscribeBroker.Broadcast(garnet/libs/server/PubSub/SubscribeBroker.cs)对每订阅者调 session.Publish/PatternPublish 同步直写其网络发送器,发送器在途超门限经 GarnetTcpNetworkSender 的 throttle.Wait 阻塞发布线程传导背压,推送零丢弃,numSubscribers 逐订阅者累加;NetworkPUBLISH(garnet/libs/server/Resp/PubSubCommands.cs)回写该数。C# 正常路径回执数恒等于实达订阅者数,与 Redis「返回接收消息的客户端数量」契约一致。
2. 工程现状确证：rust 侧 channel_sub_broadcast(wedb/wpubsub/src/subscribe_broker.rs)对每订阅者执行 deliver 闭包,内部 PubSubMailbox::try_publish(wedb/wpubsub/src/subscriber.rs)返回 bool 表入队成败,该 bool 被丢弃,count 无条件加一;broadcast 的模式臂与会话侧 network_publish(wedb/wpubsub/src/session_commands.rs)以该数回 :N。满水位拒收丢尾帧系 doc/zh/deviations.md §14 在册修复性分叉,但 §14 只登记水位封顶与 dropped 计数承接,未裁决 PUBLISH 回执口径;测试 wpubsub/tests/mailbox_watermark.rs 的 drop_counter_tracks_rejected_frames_monotonically 只钉「publish_now 返回匹配订阅者数、拒收计入 dropped」的邮箱面,回执契约面无任何票在册。broadcast_shard/publish_shard_now 分片臂同形。
3. 逻辑危害确证：慢订阅者邮箱(默认 DEFAULT_MAILBOX_CAPACITY=1024)打满后,后续发布对该订阅者的帧被丢,PUBLISH/SPUBLISH 回执仍计入该订阅者,发布方依据回执判定送达而实际丢帧;丢弃量仅经会话 ClientView 发布轨的 dropped 计数暴露,回执这一协议可见契约面失真。非满水位窗两口径无差,唯慢订阅者积压窗发散,恰是 §14 机制生效窗,运维无法从回执发现投递缺口。

涉及代码：
rust 文件与函数：
wedb/wpubsub/src/subscribe_broker.rs: channel_sub_broadcast、broadcast、broadcast_shard、publish_now、publish_shard_now
wedb/wpubsub/src/subscriber.rs: PubSubMailbox::try_publish、PubSubSink for PubSubMailbox
wedb/wpubsub/src/session_commands.rs: PubSubSessionCommands::network_publish

对应 c# 文件与函数：
garnet/libs/server/PubSub/SubscribeBroker.cs: SubscribeBroker.Broadcast、SubscribeBroker.PublishNow
garnet/libs/server/Resp/PubSubCommands.cs: RespServerSession.NetworkPUBLISH
garnet/libs/common/Networking/GarnetTcpNetworkSender.cs: 发送节流背压面(throttle.Wait,零丢弃对位)

精炼执行方案：
1. channel_sub_broadcast 的 deliver 闭包改返回 bool(透传 try_publish 成败),仅入队成功才 count 加一;broadcast 模式臂与 broadcast_shard 同改,回执口径收敛为实达数
2. §14 丢弃机制本体不动: 满水位拒收、丢尾不丢头、dropped 单调计数、ClientView 发布轨全部保持,仅回执不再吞 bool
3. 测试验证: wpubsub/tests/mailbox_watermark.rs 增满水位窗断言,打满后 publish_now 回执数等于成功入列数(容量内前缀)而非订阅者数,排空后回执恢复;mailbox_watermark.rs 既有零阻塞与水位断言不回退
