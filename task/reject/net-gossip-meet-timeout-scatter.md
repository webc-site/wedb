裁决：不成立（取证不实：超时口径并未「散在两层」——node_connection 层无任何独立建连超时，超时单点收在 gossip_manager 层且注释已挂对标；版本门（先验版本后反序列化）两档自认已与 C# 一致，无待办）
来源：next/muse.net.md 条 11。核销 2026-09-19。

一句话结论：rust 侧 meet 超时 = gossip_manager.try_meet_async 的 wait_async(cluster_node_timeout)
（对标 C# GarnetServerNode.cs:227 TryMeetAsync 的 WaitAsync(clusterTimeout)）、gossip 单次收发超时 =
try_gossip 任务的 timeout(gossip_delay)（对标 C# TryGossip 轮询挂起判超时形态），建连本身在
node_connection.initialize_async 无独立计时器——口径单层单点，「散在 manager 与 node_connection 两层」
不成立。

逐条核销
1. rust 实测：wedb/wedb/src/server/gossip/gossip_manager.rs:136-143 meet 停等
   wait_async(cluster_node_timeout, try_meet_async)，:136-137 注释已写明对标 WaitAsync(clusterTimeout)
   与 0=无限口径；:378/:385 gossip 任务 timeout(gossip_delay_ms, try_gossip_async)。node_connection.rs
   :120-132 initialize_async 全文无 Duration/timeout；connect_async 亦无内嵌超时。
2. C# 实测：GarnetServerNode.cs:111-113 建连 ReconnectAsync().WaitAsync(gossipDelay)（在 InitializeAsync
   内）、:224-228 TryMeetAsync 外层 WaitAsync(clusterTimeout)。rust 把建连+收发合入外层整体超时
   （meet=cluster_node_timeout、gossip=gossip_delay），是转写实现形态的自然收敛，语义等价且注释在位；
   C# TryGossip 的 gossip 收发本无 per-call WaitAsync（挂起由下轮轮询判超时），rust 的单次超时
   + 「挂起连接由下一轮 CAS 失败判超时移除」（gossip_manager.rs:263-264 注释）与其对齐。
3. 版本门：gossip_manager.rs:138-158（先 try_peek_version 验线格式再 from_byte_array 反序列化，
   注释挂 Gossip.cs:196-205）与 muse 档自述「与 C# 一致」相符，无增量诉求。
4. 动作「超时与版本门注释收敛一处」无明确落点：现状注释已分别挂在唯一持有该口径的位置，不存在
   第二处需收敛的口径。
