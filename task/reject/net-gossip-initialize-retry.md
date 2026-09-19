裁决：不成立（取证不实：C# 的 initialized 同样先置 1 失败不复位，重连不靠复位标志，靠「gossip 失败移除连接 + 下轮 GetOrAdd 重建」；rust 已有等价闭环）
来源：next/agy.net.md 条 2。核销 2026-09-19。

一句话结论：initialized 卡 true 不会导致节点永久失联：下一轮 gossip 调用 gossip_async 会因未连接返回
Err(NotConnected)，任务失败分支 try_remove_if_current 移除该连接，再下一轮 gossip_step_async 对缺失连接
get_or_add 重建新 NodeConnection（initialized=false）重新建连——与 C# GossipMainAsync 的
TryRemoveConnectionAsync + InitConnectionsAsync 重建闭环逐步对齐。

逐条核销
1. C# 对标实测：garnet/libs/cluster/Server/Gossip/GarnetServerNode.cs:110-113 InitializeAsync 同样
   `if (initialized != 0 || Interlocked.CompareExchange(ref initialized, 1, 0) != 0) return default;`
   先置 1 再 ReconnectAsync，失败不复位（全文件 initialized 仅 :96 构造复位为 0）。C# 的自愈路径在
   Gossip.cs GossipMainAsync：BroadcastGossipSendAsync 失败/超时 :441-448 TryRemoveConnectionAsync，
   下轮 InitConnectionsAsync :381-411 对 !GetConnection(nodeId) 的节点重新 GetOrAddAsync 新建。
2. rust 等价闭环在位：wedb/wedb/src/server/gossip/gossip_manager.rs:384-422 try_gossip spawn 任务体
   Ok(Err) 分支 :407-413 与 timeout 分支 :415-421 均 try_remove_if_current；gossip_step_async
   :241-251 对 config.get_worker_info_for_gossip() 中 !is_banned 且不在 store 的节点 get_or_add 重建
   （注释自标对标 C# InitConnectionsAsync）。
3. 失败必然传播：wedb/wconn/src/session.rs:80 `self.tx.as_ref().ok_or(Error::NotConnected)`——
   connect 失败后任何 gossip_async / gossip_with_meet_async 调用即返 Err，触发第 2 点移除；
   meet 路径同理（gossip_manager.rs:176-216 三处失败 try_remove）。
4. 提议的「失败时复位 initialized 为 false」反而是 C# 没有的行为（C# 从不复位），与 transpile
   1:1 对标原则冲突。
