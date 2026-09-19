pubsub 通道 namespace 隔离（承接 next/qcode.db.md 条 28，MED）

【拒绝：已实现】票据描述的"现状"在当前 dev 已不存在，全部方向点已落地（历史经 squash 进
3c4f74a4 init，2026-09-19 12:56）。甄别基线：dev HEAD a75287ec，cargo check -p wpubsub -p wnode 绿。
- 隔离键编解码：wedb/wpubsub/src/channel_ns.rs（ChannelNsPrefix isolate/strip）。定界选型为
  十进制数字 + `:`（非票据设想的 [NsVarint] 二进制）：模式订阅走 glob_match，二进制 ns 字节
  可能落入 glob 元字符（* ? [ \）污染匹配；十进制+定界符为 glob 字面安全字节，且规避
  ns4"2x" 与 ns42"x" 的数字前导歧义，比票据方案更严谨。
- 会话命令全接入：session_commands.rs 的 SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE 入 broker 前 isolate，
  UNSUBSCRIBE/PUNSUBSCRIBE/SUNSUBSCRIBE 定向臂 isolate、空参全量臂 strip 过滤本 ns，
  PUBLISH/SPUBLISH 广播前 isolate；应答帧/推送帧（drain_pubsub_frames）一律 strip 还原裸通道名。
- 查询面同域过滤：PUBSUB CHANNELS 走 broker.write_channels(caller_prefix)（单点实现
  write_channel_array：前缀归属判定 + 用户 glob 作用于剥离后裸名，两趟计数写出零临时分配）；
  NUMPAT 走 num_pattern_subscriptions(caller_prefix)；NUMSUB 以隔离键点查，计数天然按租户独立。
- broker 表结构未改（隔离纯靠键前缀，符合票据方向）；集群面不改协议：发端 cluster_publish
  透传隔离键，收端 network_cluster_publish（wedb/wedb/src/server/cluster_session/basic.rs:609）
  原样入本地 broker，租户分区随键跨节点贯通。
- 宿主接线：trait PubSubSessionCommands::namespace() 默认 0 承接非多租户宿主，
  RespServerSession::namespace()（resp_server_session.rs:2961）返回认证绑定的会话 ns
  （:930 AUTH/HELLO 赋值），与存储域物理键同口径。
- 验收已固化：wedb/wpubsub/tests/namespace_isolation.rs（313 行 8 用例），覆盖两 ns 同名通道
  PUBLISH 互不可见、计数独立、同 ns 互通、glob 歧义对抗（ns4/ns42）、退订本域过滤、
  shard 族隔离、集群转发携带隔离键。与票据验收口径一致。

现状
- 订阅与发布全链用裸 channel 字节，无 ns 维度：
  - wedb/wpubsub/src/session_commands.rs:181 network_subscribe → :204-212 裸 channel 入 broker
    （broker.subscribe(channel, …) / shard_subscribe(channel, …)）；
  - 同文件 psubscribe（session_commands.rs:224 network_psubscribe）/ publish
    （session_commands.rs:406 network_publish）臂同样裸 channel 广播；
  - wedb/wpubsub/src/subscribe_broker.rs:34 ChannelSubscriptions 键为裸字节，:220 subscribe、
    :424 publish_now；
  - 装配透传 wedb/wnode/src/resp/resp_server_session.rs:1376（args 原样）；
  - 集群广播 wedb/wnode/src/cluster_session.rs:179 cluster_publish 跨节点同样裸 channel。
- ns A 用户 PUBLISH news，ns B 用户 SUBSCRIBE news 直接收到；PUBSUB CHANNELS/NUMSUB 跨 ns 互见，
  计数混合。
- wedb 自有多租户架构（SKILL.md:31-36：认证 <ns>#用户名、物理键 [NsVarint]+[DbVarint] 刚性隔离、
  ACL 随 ns 绑定）在 pubsub 域整体缺席，存储域隔离与消息域隔离口径分叉。

C# 参考
- 无对位（C# Garnet 无 namespace 概念；PubSubCommands.cs / SubscribeBroker.cs 本就裸 key）。
  本条为 wedb 自有架构（SKILL 规定）的打通缺口，非 1:1 转写面。

优先级
功能缺口（多租户隔离在消息域未打通，跨租户订阅互通为隔离/泄漏缺陷）。

方向
- 会话侧前缀承载：复用 session 域 session_prefix 外提机制，在 SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE
  入 broker 前与 PUBLISH/SPUBLISH 广播前把 ns 拼入 channel 键（格式对齐物理键 [NsVarint] 前缀或
  独立定界）。
- PUBSUB CHANNELS/NUMSUB 查询同域过滤、应答剥离前缀还原用户视角。
- broker 表结构与集群广播不改（隔离靠键前缀达成）。
- 验收：两 ns 各自 SUBSCRIBE 同名 channel，PUBLISH 互不可见且计数独立。
