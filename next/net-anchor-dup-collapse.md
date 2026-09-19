优先级：低（注释锚点卫生：同一 C# 符号挂多个 rust 函数，check.js 重复定义报告在列，追溯链一 C# 多 rust 落点、主副不分；只改注释不动代码）
分拣来源：next/agy.net.md 条 10/11/12/13/14/15/16 + next/muse.net.md 条 1/2/3/4/5/6/7（两轮独立审查同题七组，合并单票；源档已分拣清空删除）

问题
net 域 6 组 C# 锚点复挂：同一 C# 函数符号同时挂到「语义全量对位的主锚点」与「子步骤/转发/注入器的副锚点」上，check.js 重复定义报告全部在列。动作统一为去副保主，只改注释。
（GetSslServerAuthenticationOptions 双挂组同题并入 next/design-anchor-remount-batch.md 组 1 承载，本票不含，勿双花。）

取证（2026-09-19 主仓 dev，bun js/check.js 实跑 + 逐处符号核对；行号按 fn 定义行）

组 1 libs/client/ClientSession/GarnetClientSession.cs:GarnetClientSession（构造）
- 保留：wedb/wconn/src/session.rs:40 GarnetClientSession::new（构造本体）
- 去：wedb/wconn/src/session.rs:74 GarnetClientSession::set_network_pool、wedb/wconn/src/client.rs:114 GarnetClient::set_network_pool、wedb/wconn/src/network/pump.rs:50 resolve_network_pool（池注入/解析非构造；回退说明可挂 garnet/libs/cluster/Server/Migration/MigrationManager.cs:GetNetworkPool :37，真实存在）

组 2 libs/client/GarnetClient.cs:ConnectAsync
- 保留：wedb/wconn/src/client.rs:133 GarnetClient::connect_async（全量建连）
- 去：wedb/wconn/src/tls.rs:98 ClientTlsConfig::connect（仅 SslStream AuthenticateAsClientAsync 等价的 TLS 握手子步骤，保留现散文说明即可）

组 3 libs/client/GarnetClient.cs:GarnetClient（构造）
- 保留：wedb/wconn/src/client.rs:65 GarnetClient::new
- 去：wedb/wconn/src/client.rs:104 GarnetClient::set_tls（TLS 注入器非构造；agy 档所指 facade set_tls 已漂移，现复挂点在 wconn 本件）

组 4 libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs:NetworkIterativeSlotVerify
- 保留：wedb/wedb/src/server/cluster_session/slot_verify.rs:52 ClusterSession::network_iterative_slot_verify（状态机本体）
- 去：wedb/wedb/src/server/cluster_manager_slot_gate.rs:351 ClusterManager::evaluate_iterative_key_gate（挂起扩展，非单键状态机；单键状态机本体锚点 wedb/wedb/src/server/slot_verify.rs:91 已正确单挂 ClusterSlotVerify.cs:SingleKeySlotVerify，不动）

组 5 libs/server/PubSub/SubscribeBroker.cs:StartAsync
- 保留：wedb/wnode/src/service.rs:607 spawn_pubsub_consume_task（宿主消费任务全量）
- 去：wedb/wpubsub/src/subscribe_broker.rs:564 SubscribeBroker::consumer_finish（done 收尾子步骤；如需锚点改挂 garnet/libs/server/PubSub/SubscribeBroker.cs:Dispose :392 收口说明）

组 6 libs/server/Servers/GarnetServerTcp.cs:HandleNewConnection
- 保留：wedb/wnode/src/net/handler/drive.rs:35 NetworkHandler::process_stream（全量连接处理）
- 去：wedb/wnode/src/servers/consumer_registry.rs:357 ConsumerRegistry::try_acquire_connection（计数递增 + 容量门子步骤）
- 顺带核对项（已核无缺口，勿动）：C# HandleAcceptError 退避在 rust wedb/wnode/src/server.rs:893-930 handle_accept_error 已对标在位（指数退避 + 停机取消竞速），TCP/UDS 双循环消费

C# 对标
各组符号名即列（garnet 相对路径见组内）；GetNetworkPool 见 garnet/libs/cluster/Server/Migration/MigrationManager.cs:37，SubscribeBroker.Dispose 见 garnet/libs/server/PubSub/SubscribeBroker.cs:392。

修法建议
- 只改注释锚点：副锚点行删除或改散文对标说明（形态参照 wbase/src/align.rs:115-117 散文先例），不改任何代码行为
- 每组保留的唯一主锚点注释里可加一句「唯一锚点」声明，防后续复挂回潮
- 验收：bun js/check.js 重复定义报告中上述 6 组清零（GetSslServerAuthenticationOptions 组归 design-anchor-remount-batch 验收）；symbolCheck 无新增虚构符号失败；cargo check 零警告（纯注释改动）

边界
- check.js 重复报告中非 net 域组（GarnetRecordTriggers.cs:OnDispose、LuaRunner、Metrics、RangeIndex、VectorManager、StoreWrapper、Tsavorite 等）不在本票射程，归 data/db/design 域分拣处置
- 与 next/replication-comment-anchor-drift.md 不交叠（那单管复制链注释里虚构 C# 文件名与失效 task 文档引用，本单管同符号复挂）
