终态：合入 bfafa335（核心提交 09c6408/87e9087），network_process_cluster_command 调用方身份门判据扩为 matches!(ClusterFlushallNs | ClusterPublish | ClusterSpublish) 单机制复用（namespace != 0 且 remote_node_id 空即 NOPERM 判据与文案不动），basic.rs 不设第二处门，db.md 3.5 示例补列 PUBLISH/SPUBLISH，tests/cluster_publish_ns_caller_gate.rs 三面闭环（ns5 +@pubsub 攻击帧 NOPERM + ns7 三类订阅者零投递 / ns0 超管放行剥前缀帧形 / 双节点 gossip 转发链路回归）。

审核结论：通过

判定要点：
1. 攻击链亲验成立。分派：dispatch.rs:217-231 is_cluster_sub_command 区间整体进 admin_commands.rs:network_process_cluster_command，租户会话同路径；门 :338-344 仅判 cmd == ClusterFlushallNs，ClusterPublish/ClusterSpublish 直落 cluster_session/mod.rs:254 → basic.rs:659-677 network_cluster_publish，args[0] 原样 publish_now / publish_shard_now，无 ns 归属校验。
2. 其他层门禁逐一排除：
   集群槽校验：consume.rs:264 can_serve_slot 首判 is_data_command，CLUSTER 子命令（command.rs:494 ClusterAddslots..=ClusterSync 区间）不在数据区间，直接 Serve，不因 7:orders 槽归属回 MOVED。
   IsInternal：RespCommandsInfo.json:864-871 / :981-988 两条目 IsInternal=true，但该位仅影响 COMMAND 输出（resp_command_docs.rs:528），ACL 目录 catalog/mod.rs:308 project_entries 明文「全量收录含 IsInternal」，cat_members 按 AclCategories（Admin, PubSub, Slow, Garnet）登入，+@pubsub 即得执行位。
   非集群模式：cluster_session 为 None 回 CLUSTER DISABLED（admin_commands.rs:324-327），攻击面限集群形态，票面已限定「集群形态」，无误。
3. 发送侧确证第 0 参为隔离键：wpubsub/src/session_commands.rs:409-418 ChannelNsPrefix::isolate 后本地 publish 并 cluster_publish(&isolated)，收端以隔离键直入 broker，CLUSTER PUBLISH 在 wedb 中实质为多租户集群总线收令帧，落 doc/zh/db.md 3.5（:310-312）门禁语义范围。
4. C# 对位亲验：garnet/libs/cluster/Session/RespClusterBasicCommands.cs:509-530 NetworkClusterPublish 仅校验参数个数与 broker，C# 无 ns，等价普通 PUBLISH，无越权面；本缺陷为 wedb 自研 ns 折叠引入的新语义缺口，非移植分歧，不属臆测。
5. 与 task/todo/wedb-cluster-flushall-ns-caller-gate-gossip-forgery.md 关系：同一门、不同维度，不合并。该票修 remote_node_id 置位可信度（门判据真值），本票扩门覆盖命令集合（门适用面）；两票改动点不相交，本票复用该门判据即单机制。另：攻击者仅 +@pubsub 时无 CLUSTER|GOSSIP 执行位（Admin+Dangerous），无法走 WITHMEET 自封旁路，本票扩门对主攻击形态立即有效，不依赖该票先落地。
6. 非重复：task/done、task/reject 无 CLUSTER PUBLISH/SPUBLISH 身份门同判据票。
7. 判定标准 1-6：缺陷经代码确证（非臆测）；方案无假桩、复用既有门不建双机制、不改回上游设计、无 allow；本文纯文本。

优化执行方案（覆盖原票面执行方案）：
1. 唯一改动点 wedb/wnode/src/resp/admin_commands.rs:network_process_cluster_command 现有门：cmd == RespCommand::ClusterFlushallNs 改为 matches!(cmd, RespCommand::ClusterFlushallNs | RespCommand::ClusterPublish | RespCommand::ClusterSpublish)，namespace != 0 且 remote_node_id().is_none() 判据、RESP_ERR_NOPERM 文案不动；门上注释同步改述为「多租户集群总线收令帧（FLUSHALL_NS 清库扇出、PUBLISH/SPUBLISH 隔离键转发）」。
2. basic.rs:network_cluster_publish 不加 ns 校验（避免第二处门）；doc/zh/db.md 3.5 示例补列 CLUSTER PUBLISH / SPUBLISH，契约与代码同口径。
3. 闭环测试（同册 wedb/wedb/tests/cluster_flushall_ns_caller_gate.rs）：
   (a) ns=5 仅 +@pubsub 用户发 CLUSTER PUBLISH 7:ch x 与 CLUSTER SPUBLISH 7:ch x，断言回 NOPERM，且 ns=7 的 SUBSCRIBE ch / PSUBSCRIBE c* / SSUBSCRIBE ch 订阅者在超时窗口内零消息；
   (b) ns0 会话发同帧，断言 ns=7 订阅者收到 message ch x（剥前缀帧形）；
   (c) 双节点集群 ns=7 会话在节点 A PUBLISH ch x，节点 B 的 ns=7 订阅者收到，回归节点间转发链路不被门误拦（若入站 gossip 连接认证身份非 ns0，此用例即验证 remote_node_id 放行臂）。
4. 执行后跑 ./test.sh。

以下为原票面。

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
