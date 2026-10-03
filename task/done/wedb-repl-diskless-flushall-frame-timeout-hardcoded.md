审核结论：通过（2026-09-30 独立审核代理现码复验，逐点亲证）
1. 真实性成立：replica_sync_session.rs:58 REPLICA_SYNC_CMD_TIMEOUT 死常量 30 秒、:255 issue_flush_all_async timeout(REPLICA_SYNC_CMD_TIMEOUT, ...) 硬编码消费均亲验；同文件唯两个限时点，:349-350 ATTACH_SYNC 帧已走 provider.repl_attach_timeout()、replication_snapshot_iterator.rs:761 快照扇出已走 provider.replica_sync_timeout()，唯清库帧残留死常量。C# 锚 ReplicaSyncSession.cs:143 WaitAsync(storeWrapper.serverOptions.ReplicaSyncTimeout, token) 亲验吻合（SetFlushTask :139-158）。消费接口可直达：flags.rs:64 provider.replica_sync_timeout() 在位，调用点 replication_sync_manager.rs:381 所在上下文已持 provider（:354-360 同函数即取 provider 凭证建连），纯参数接线。
2. 非重复非灭失成立：done 票 repl-sync-timeout-knob.md C# 锚清单第 7 行明列 DisklessReplication/ReplicaSyncSession.cs:143，其 rust 收口清单（:20-25）不含 diskless_replication/replica_sync_session.rs 此点；收口验证词边界 \bREPLICA_SYNC_TIMEOUT\b 不命中 REPLICA_SYNC_CMD_TIMEOUT（不同符号）。task 池 grep：todo/reject 无本族票；done 池 repl-sync-timeout-infinite-sentinel-timer-overflow.md（哨兵 <=0 溢出 panic）与 wnode-flushall-destroys-acl-user-records-auth-lockout.md（副本侧 ACL）主题均不同，确系该族收口漏项。
3. 架构合规：删常量改取 provider.replica_sync_timeout()，与扇出停等帧同源单机制，零新增机制。执行注意：:255 现为 compio::time::timeout 裸 Duration，承接 Option<Duration> 应改 crate::server::wait_async（flags.rs:56-62 全仓约定：无限哨兵折 None 不挂计时器，禁折 u64::MAX 送 compio 定时器溢出 panic），与同文件 :349 帧同形制。
4. 票面瑕疵备案（不影响判定）：第 5 行「:140 SnapshotTransmissionDriver 构造同源传 ReplicaSyncTimeout」引证失真——C# :140 实为 if (task != null)，SnapshotTransmissionDriver 类名于 garnet 仓 git grep 零命中；核心锚 :143 与同旋钮论点不受影响。
5. 验证闭环成立：调小 replica-sync-timeout 模拟副本不应答清库帧、断言按配置超时，可观测面充分。

无盘链 FLUSHALL 清库帧超时硬编码 30 秒，绕过 replica-sync-timeout 活旋钮（在册超时旋钮票收口漏项）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# DisklessReplication/ReplicaSyncSession.cs:SetFlushTask :143 flushTask = ContinueFlushTaskAsync(task).WaitAsync(storeWrapper.serverOptions.ReplicaSyncTimeout, token)，清库帧应答限时走 ReplicaSyncTimeout 活配置，与同文件快照扇出驱动（:140 SnapshotTransmissionDriver 构造同源传 ReplicaSyncTimeout）一致。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   rust wedb/wedb/src/server/replication/diskless_replication/replica_sync_session.rs:58 const REPLICA_SYNC_CMD_TIMEOUT: Duration = Duration::from_secs(30)，:255 issue_flush_all_async 用 timeout(REPLICA_SYNC_CMD_TIMEOUT, ...) 硬编码。同文件 ATTACH_SYNC 帧已改取 provider.repl_attach_timeout()（:350）、快照扇出停等帧已取 provider.replica_sync_timeout()（replication_snapshot_iterator.rs:761），唯此一处仍持死常量。在册票 task/done/repl-sync-timeout-knob.md 的 C# 锚清单明列 DisklessReplication/ReplicaSyncSession.cs:143，系该票收口后新增 diskless 会话模块的遗留漏项，非重复提报。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
   副本无响应/半死时，主端全量扇出前清库帧挂满硬编码 30 秒（配置缺省 5 秒、或运维调小后失效），批量多副本逐个串行放大，全量同步整体拉长；超时旋钮对该帧形同虚设。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/replication/diskless_replication/replica_sync_session.rs:REPLICA_SYNC_CMD_TIMEOUT
wedb/wedb/src/server/replication/diskless_replication/replica_sync_session.rs:issue_flush_all_async

对应 c# 文件与函数：
garnet/libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicaSyncSession.cs:SetFlushTask

精炼执行方案：
1. 删除 REPLICA_SYNC_CMD_TIMEOUT 常量，issue_flush_all_async 改取 provider.replica_sync_timeout()（与快照扇出停等帧同源单机制）。
2. 测试验证点：配置 replica-sync-timeout 调小后模拟副本不应答清库帧，断言主端按配置超时而非 30 秒。

终态注记：
- 合入收口形态：删除 REPLICA_SYNC_CMD_TIMEOUT 常量，issue_flush_all_async 改取 provider.replica_sync_timeout() 经 crate::server::wait_async 异步等待，消灭 30s 硬编码超时；补充超时配置与成功路径集成测试。
- 合入哈希：bc33bb7
- 状态：已收口归档。

