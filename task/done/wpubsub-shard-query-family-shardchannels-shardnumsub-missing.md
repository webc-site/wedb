终态注记：合入 2693324，分片域查询族 PUBSUB SHARDCHANNELS/SHARDNUMSUB 全链收口——RespCommand 追加 371/372 入册双 JSON 目录，会话两查询臂复用 write_channel_array 单机制按 ns 前缀过滤回裸名、SHARDNUMSUB 按隔离键查分片表与 num_subscriptions 同构，跨租户不可见与三表分离契约回归测试钉死

甄别结论：通过（2026-09-29 主控甄别，定级 P3——PUBSUB_SUBTABLE command_table.rs:339-343 仅三行无 SHARDCHANNELS/SHARDNUMSUB，numsub 经 broker.num_subscriptions 只查普通表对分片通道恒 0；C# 全库零命中系 rust 补全面，SUNSUBSCRIBE §7 同口径先例（list_all_shard_subscriptions 既有骨架）。修复：复用 write_channel_array 单机制，跨租户可见性断言+三表分离契约回归）

审核结论：通过（2026-09-29 甲轮35-B，P3 级）。PUBSUB_SUBTABLE 仅三行、全仓零命中整族缺席、NUMSUB 对分片通道恒 0、C# 无对位物、§6/§7 自有域+SUNSUBSCRIBE rust 补全先例——「自有域漏项非契约分叉」定性正确。无修正意见。

原票面：
分片域 PUBSUB SHARDCHANNELS/SHARDNUMSUB 查询族缺失：rust 自有分片订阅表无任何对外观测查询入口

问题分析：
1 Garnet 契约对齐：C# 无分片域（garnet/libs/server/Resp/PubSubCommands.cs 仅 NetworkPUBSUB_CHANNELS/NetworkPUBSUB_NUMPAT/NetworkPUBSUB_NUMSUB 三个查询，garnet 全库 grep SHARDNUMSUB 与 SHARDCHANNELS 零命中），无对位函数；doc/zh/deviations.md §6 已把分片域登记为 rust 自有设计并声明向 Redis 标准语义对齐，§7 按同一口径补齐 SUNSUBSCRIBE——Redis 7.0 标准 PUBSUB 查询族含 SHARDCHANNELS [pattern] 与 SHARDNUMSUB channel...（redis/src/pubsub.c pubsubCommandShardChannels / pubsubCommandShardNumSub），本仓 rust 自有分片域内该查询族整族缺席。
2 工程现状确证：命令子命令表仅三行（wedb/wnode/src/resp/parser/command_table.rs PUBSUB_SUBTABLE 仅 CHANNELS/NUMPAT/NUMSUB），会话分派臂（wedb/wnode/src/resp/resp_server_session/pubsub.rs process_pubsub_command）与 wpubsub 查询面（wedb/wpubsub/src/session_commands.rs network_pubsub_channels/numsub/numpat）均无分片出口；broker 侧 list_all_shard_subscriptions 仅被空参 SUNSUBSCRIBE 内部消费，无命令面对外出口。后果：SSUBSCRIBE 订阅者只能盲订，PUBSUB NUMSUB 对分片通道恒回 0（num_subscriptions 只查普通订阅表），分片订阅清单与计数不可查。
3 逻辑危害确证：非数据正确性缺陷，属自有域可观测性漏项——运维无法查询分片频道清单与分片订阅数，SPUBLISH 投递空转与订阅悬挂（甲轮8 收口面）均无旁路核实手段；与乙轮已消费的既有命令应答面无涉（本票只钉缺失命令对，不触既有命令行为）。同谱先例：甲轮8 即按「自有域漏项而非 C# 契约分叉」定 P3 收口。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/parser/command_table.rs:PUBSUB_SUBTABLE
wedb/wnode/src/resp/resp_server_session/pubsub.rs:process_pubsub_command
wedb/wpubsub/src/session_commands.rs:network_pubsub_channels、network_pubsub_numsub
wedb/wpubsub/src/subscribe_broker.rs:list_all_shard_subscriptions、write_channel_array、num_subscriptions

对应 c# 文件与函数：
N.A.（garnet/libs/server/Resp/PubSubCommands.cs 无分片域查询；Redis redis/src/pubsub.c pubsubCommandShardChannels / pubsubCommandShardNumSub 为语义对齐源）

精炼执行方案：
1 wresp RespCommand 增 PubsubShardchannels/PubsubShardnumsub 变体，command_table.rs PUBSUB_SUBTABLE 补 ("SHARDCHANNELS", ..) 与 ("SHARDNUMSUB", ..) 两行
2 session_commands 增 network_pubsub_shardchannels/shardnumsub 两臂：复用 write_channel_array 既有骨架以 shard_subscriptions 入参按 ns 前缀过滤并回写裸名（不新增第二套遍历机制）；SHARDNUMSUB 形按隔离键查 shard_subscriptions 内层集合长度，与 num_subscriptions 同构
3 测试：wpubsub/tests/namespace_isolation.rs 补跨租户断言——租户 7 SSUBSCRIBE 后租户 8 的 SHARDCHANNELS/SHARDNUMSUB 不可见、本租户可见；分片订阅不计入 NUMSUB、普通订阅不计入 SHARDNUMSUB（三表分离既有契约回归）
