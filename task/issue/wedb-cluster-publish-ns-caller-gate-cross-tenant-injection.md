CLUSTER PUBLISH / CLUSTER SPUBLISH 收令帧无调用方身份门，租户会话可直写任意 ns 隔离键向他租户订阅者注入伪造消息

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# RespClusterBasicCommands.cs:NetworkClusterPublish（:509-530）收端只校验参数个数与 broker 在场，即以 parseState 第 0 参原样调用 subscribeBroker.Publish。C# 无 namespace，频道名即用户可见名，任何会话直发 CLUSTER PUBLISH 只等价于一次普通 PUBLISH，无越权面。
   wedb 自研面改变了该帧的语义：发送侧 wpubsub session_commands.rs:network_publish 先以 ChannelNsPrefix 把频道折叠成隔离键（形如 "7:orders"），cluster_publish 原样把隔离键作为 CLUSTER PUBLISH 第 0 参转发，收端以该键直入本地 broker（session_commands.rs:cluster_publish 文档注明「收端以该键直入本地 broker，租户分区随键跨节点贯通」）。因此第 0 参已是内部隔离键，CLUSTER PUBLISH 实质是 doc/zh/db.md 3.5 所述「多租户集群总线收令帧」，应仅节点间连接或 namespace == 0 会话可发起。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   wedb/wnode/src/resp/admin_commands.rs:network_process_cluster_command 是 CLUSTER 命令面唯一入层，身份门只覆盖 cmd == RespCommand::ClusterFlushallNs（namespace != 0 且 remote_node_id 为空即 NOPERM），ClusterPublish / ClusterSpublish 无任何门，直落 process_cluster_commands。
   wedb/wedb/src/server/cluster_session/basic.rs:network_cluster_publish 对 args[0] 不做任何 ns 归属校验，直接 broker.publish_now(args[0], args[1]) 或 publish_shard_now。
   ACL 层也拦不住：wedb/wresp/RespCommandsInfo.json 中 CLUSTER|PUBLISH 与 CLUSTER|SPUBLISH 分类为 Admin, PubSub, Slow, Garnet；wedb/wresp/src/catalog/mod.rs:Catalog::build 把子命令条目按自身分类位登入 cat_members，wacl user.rs:apply_category 经 commands_for_category 展开，故仅授予 +@pubsub 的租户用户（发布订阅客户端的常规授权）即获得 CLUSTER|PUBLISH 执行位。
   复现路径：集群形态下 ns=5 租户用户（+@pubsub）认证后发送 CLUSTER PUBLISH 7:orders evil，本节点 ns=7 订阅 orders 的会话收到 message orders evil（drain_pubsub_frames 按 "7:" 剥离成功，帧形与真实发布完全一致），ns=7 的 PSUBSCRIBE 模式订阅者同样命中；CLUSTER SPUBLISH 7:orders evil 对 ns=7 分片订阅者同理。
3. 逻辑危害确证
   跨租户消息注入：攻击租户无需任何伪造凭据即可向任意租户的通道、模式、分片订阅者投递任意内容，受害方无法区分真伪，破坏 doc/zh/db.md 多租户强逻辑隔离与 channel_ns.rs 声明的「消息域同口径隔离」不变量。逐节点重复发送即可覆盖全集群。该帧无应答写出，攻击无回显痕迹。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/admin_commands.rs:network_process_cluster_command
wedb/wedb/src/server/cluster_session/basic.rs:network_cluster_publish
wedb/wpubsub/src/session_commands.rs:PubSubSessionCommands::network_publish
wedb/wpubsub/src/session_commands.rs:PubSubSessionCommands::cluster_publish
wedb/wedb/src/server/cluster_session/mod.rs:cluster_publish

对应 c# 文件与函数：
garnet/libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterPublish
garnet/libs/server/Resp/PubSubCommands.cs:NetworkPUBLISH

精炼执行方案：
1. network_process_cluster_command 现有 FLUSHALL_NS 身份门判据扩为命令集合 matches!(cmd, ClusterFlushallNs | ClusterPublish | ClusterSpublish)，复用同一 namespace != 0 且 remote_node_id().is_none() 判据与 RESP_ERR_NOPERM 文案，不建第二套门。发送侧 try_cluster_publish_async 走 gossip NodeConnection，节点间连接正常放行。
2. 门的 remote_node_id 可被 CLUSTER GOSSIP WITHMEET 自封的弱点由 task/todo/wedb-cluster-flushall-ns-caller-gate-gossip-forgery.md 统一收口，本票不另设身份判定，随该票修复自动生效。
3. 测试验证点：在 wedb/wedb/tests/cluster_flushall_ns_caller_gate.rs 同册补用例，ns=5 +@pubsub 会话发 CLUSTER PUBLISH 7:ch x 与 CLUSTER SPUBLISH 7:ch x 断言 NOPERM 且 ns=7 订阅者邮箱零投递；ns0 会话与 gossip 建链会话发帧断言正常投递；普通 PUBLISH 跨节点转发链路回归不变。
