优先级：中

3 [MED] DisklessSyncSession::set_status 丢掉 C# SetStatus 的 FAILED→摘除 AOF 推流驱动一侧，
data-loss 拒绝路径留下旧驱动继续向已判败副本推流
具体问题：C# ReplicaSyncSession.SetStatus 是三语句：error 首错保留、状态后置、
SUCCESS/FAILED 各自 signalCompletion.Release()，且 FAILED 分支额外
AofSyncDriverStore.TryRemove(AofSyncDriver)（diskless 分部 :129）——即「会话判败
必连带摘除该副本的 AOF 推流驱动」。rust set_status（:112-125）只承接前两语句，
Failed 分支无驱动摘除，doc 注释 :110-111 也只引到 signalCompletion.Release 一半。
rust 唯一的摘除点 try_remove(origin_node_id)（:293）位置在 begin_aof_sync 内
「重新注册新驱动之前」，属置换语义不属失败清理。后果链：副本二次 attach 走无盘链
（首次已由 diskbased attach_replica_wire:91 注册过驱动），begin_aof_sync 在
:data_loss_check 处判败（:277-285，AOF 在快照传输期间被截断，是该函数的设计触发
场景）→ set_status(Failed) 直接 return Err，早于 :293 的 try_remove → 上一轮驱动
连同其 TcpSessionWire 继续在 pump 上向该副本推 AOF 帧、继续推进其已发送水位，
而本会话已判败、副本侧并未恢复完成；C# 同路径由 SetStatus 当场摘除断流。
rust 会话结构也不持 AofSyncDriver 字段（check/miss 登记 DisklessReplication/
ReplicaSyncSession 的 AddAofSyncTask 零映射即此因），故 set_status 内无从摘除，
这是链路缺环而不是注释缺环。
rust：wedb/wedb/src/server/replication/diskless_replication/replica_sync_session.rs:112-125
set_status（缺 Failed 摘除面）、:277-285 data_loss_check 失败早返、:292-293 唯一
try_remove 位置、:53-75 结构体无驱动字段；diskbased 对位注册点
replica_sync_session.rs:80-109 attach_replica_wire
c#：garnet/libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/
ReplicaSyncSession.cs:116-131 SetStatus（:129 AofSyncDriverStore.TryRemove(AofSyncDriver)）、
:109 AddAofSyncTask、:224-263 BeginAofSyncAsync（catch → SetStatus(FAILED)）
修法：DisklessSyncSession 增驱动持有面（attach 成功后写入，或按 replica_meta.
origin_node_id 经 aof_sync_driver_store 点查），set_status 的 Failed 分支补摘除，
同时把 :293 的 try_remove 提前到 begin_aof_sync 入口（建连之前），使
attach_sync/data_loss 两处失败早返都不残留旧驱动。

主代理复核改判（不立开发票）：dev HEAD 已有 C# RunAsync finally AofSyncDriverStore.TryRemove(this) 的承接点 —— wedb/wnode/../server/replication/aof_replication_pump.rs:186/:225/:229 走 store.try_remove_current(&driver)，即「驱动自身退场时摘册」，与 aof_sync_driver.rs:386 的实例匹配移除同族；票面要求的 set_status 内摘除只是换了位置，若照票再摘一次会成第二套退场机制。票文与取证留此备查。
