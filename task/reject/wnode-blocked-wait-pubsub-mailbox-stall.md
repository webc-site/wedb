审核结论：拒绝。反证确凿（C# 行为亲验）：议题核心前提"C# 同场景推送照常送达"失实。C# 订阅会话消费入口 RespServerSession.cs:492 EnterAndGetResponseObject 持每会话发送器 spinLock，:575 finally 才 Exit；BLPOP 的 AsyncUtils.BlockingWait（ListCommands.cs:283-284，码内注释明言 Must block as we're on the network thread）挂起期间该锁一直被持有；发布线程 Broadcast 直调订阅会话 Publish（PubSubCommands.cs:31/:66）同入口 EnterAndGetResponseObject，且 GarnetTcpNetworkSender.cs:120-124 为阻塞式 SpinLock.Enter——即 C# 在 BLPOP 挂起窗口对推送同样零投递，仅以「阻塞发布端、无损」区别于 rust「有界邮箱满位拒收丢帧」。该差异正是 doc/zh/deviations.md §14（:101-114，:113-114 明载「C# 阻塞发布端无损，Rust 丢新帧保发布端进度」）已登记的机制替换面，挂起窗口即极端慢订阅者积压形态，不构成未登记分叉。rust 侧现状锚（drive.rs 各臂无邮箱监听、try_publish 满丢尾、1024 有界、TLS 读写锁单次 poll 粒度）审核均属实，惟立项前提「契约对齐」不成立。若欲即时投递属「优于原型」改良，须按审查流程另立偏离登记并裁 §14 边界后在 task/issue/ 重新提案，不得以现案按提案实施——那将使 probe_race 产出 C# 无对位的行为面。

阻塞/慢/脚本挂起期 pubsub 邮箱排空停摆

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 推送投递不经订阅会话的网络线程：SubscribeBroker.Broadcast（libs/server/PubSub/SubscribeBroker.cs:76-117）在发布方/AOF 消费线程直调订阅会话的 Publish/PatternPublish（libs/server/Resp/PubSubCommands.cs:21-55/:58-93），后者直写订阅者 networkSender 并即时 Send。订阅会话网络线程即使在 BLPOP 的 AsyncUtils.BlockingWait 中挂起（不持发送器锁），推送帧照常送达对端。订阅态命令门仅作用于 RESP2（RespServerSession.cs:657 isSubscriptionSession && respProtocolVersion == 2），RESP3 会话可同时持有活跃订阅与阻塞命令，交互场景协议可达。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 推送投递仅两个活跃窗口：消费轮头部顺带排空（drive.rs:208 drain_pubsub_into）与读段双路等待的 Push 臂（drive.rs:493-575 wait_read_or_push + 排空直写）。而阻塞挂起臂（drive.rs:301-319）、慢路径挂起臂（drive.rs:321-350）、脚本续跑臂（drive.rs:275-285）经 probe_race（drive.rs:681-737）只做「终止广播 × 执行体 × 对端活性探测读」三路竞速，无邮箱监听臂；ACL 停车两臂（drive.rs:237-268）同样不排空。挂起期间到达邮箱的推送帧滞留，邮箱为 1024 有界（wpubsub/src/subscriber.rs try_publish 满即丢尾帧 + dropped 计数，§14 登记机制本体）。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
RESP3 客户端 SUBSCRIBE 后执行 BLPOP key 0（无限等待）期间：推送零投递直至 BLPOP 解除（可能永不解除），高频通道下邮箱 1024 满位后新帧永久丢弃（dropped 计数仅可观测不可恢复）；EVAL 内 BLPOP 的脚本挂起窗口与冷键慢路径挂起窗口同病。C# 同场景推送照常送达，属消息投递丢失/停摆的真实行为分叉，非既定改良（§14 登记的是邮箱有界拒收机制对 C# 阻塞背压的替换，未覆盖本挂起窗口停摆面）。

涉及代码：
rust 文件与函数：
wedb/wnode/src/net/handler/drive.rs:drive_loop（阻塞臂 :301-319/慢臂 :321-350/脚本臂 :275-285/ACL 停车臂 :237-268/读段推送臂 :493-575）
wedb/wnode/src/net/handler/drive.rs:probe_race（三路竞速无邮箱臂，:681-737）
wedb/wnode/src/net/handler/push.rs:wait_read_or_push（双路等待既有机制，复用源）
wedb/wpubsub/src/subscriber.rs:PubSubMailbox（try_publish 满拒丢尾 + listen）

对应 c# 文件与函数：
libs/server/PubSub/SubscribeBroker.cs:Broadcast
libs/server/Resp/PubSubCommands.cs:Publish/PatternPublish
libs/server/Resp/RespServerSession.cs:ProcessMessages（:657 订阅门仅 RESP2）

精炼执行方案：
1. probe_race 增邮箱监听竞速臂：会话 pubsub_mailbox() 在场时，将 mailbox.listen()（与 push.rs wait_read_or_push 同一 listener 双检机制）并入 select 四路竞速；Push 胜出按读段推送臂既有写出规则排空邮箱直写（resp_pooled 排空 → wait_for_aof_blocking → throttle → write_all_shared → 字节镜像 add_net_bytes），写毕续等执行体
2. 探测读 future 在途时推送写出与读段推送臂同拓扑（TCP/Unix 全双工合法；TLS 读句柄挂起即释 BiLock，写句柄可推进），无第二写出机制
3. 测试验证点：集成测试 RESP3 会话 SUBSCRIBE 后 BLPOP 空键 timeout=0 挂起，另连客户端 PUBLISH，断言挂起会话在 BLPOP 未解除期间收到 message 推送帧且 mailbox dropped 归零；对照回归 BLPOP 解除后应答与推送帧流水线序不乱
