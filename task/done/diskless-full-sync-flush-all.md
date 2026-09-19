无盘全量同步补主端→副本清库帧，修主从数据面永久发散

来源：next/qcode.net.md 第 5 轮条 5（判定成立，已剪除）。取证基线：主仓 dev。

现状（缺口）

- C# 无盘全量同步在判定 NeedToFullSync 后，先向副本发 CLUSTER FLUSHALL 清库
  再流式下发快照记录，且副本 attach 前故意不 Reset，清库完全依赖该帧
  （ReplicationSyncManager.cs:279 `Sessions[i].SetFlushTask(IssueFlushAllAsync())`
  → ReplicaSyncSession.cs:51 → AofSyncDriver.cs:189
  `aofSyncTasks[0].garnetClient.ExecuteAsync(["CLUSTER","FLUSHALL"])`；
  副本侧 ReplicaDisklessSync.cs:108 `if (!disklessSync)` 明示 diskless 不本地
  Reset，等主端清库帧）。
- rust 三处皆无清库面：
  1. 副本 attach 体 replica_diskless_attach
     （replica_diskless_sync.rs:83）只复位驱动 store、挂起 Primary 任务，
     不清键（与 C# 对齐，本应如此）。
  2. 主端全量支在 ReplicationSyncManager 攒批里
     （replication_sync_manager.rs:264-294）连上副本 client 后直接
     set_client + 收进 full_sessions，:311 run_snapshot_fanout 推记录帧，
     中间无 FLUSHALL 发送。
  3. 副本恢复体 try_replica_diskless_recovery
     （replica_diskless_sync.rs:167）full_sync=true 支仅
     wal.safe_initialize 对齐地址空间（:186-190），不动键数据。
- 接收侧 cluster_sync_slow（replication.rs:118）记录帧只有 upsert 语义
  （Str→upsert_string :229、Env→upsert_tag :233），全量支无 tombstone 扫面、
  无 flush-on-init 标记。
- 后果：副本曾持有而主端已无的键（含已过期键）在快照扫描中无对应记录帧、
  永不删除，全量同步完成后主从数据面永久发散。

修法（二选一，勿双做；优先方案 A 对齐 C# 消息序）

A. 主端在 fanout 前向每个 full 会话发 CLUSTER FLUSHALL 并等 +OK：
   replication_sync_manager.rs 的 full_sessions 建连循环（:279-293）内，
   s.set_client 之后、run_snapshot_fanout 之前，对 is_full 会话在既有
   client 上发 `["CLUSTER","FLUSHALL"]` 并 await +OK（仅全量支发，
   PartialResync 不发，对齐 C# NeedToFullSync 门）。副本接收臂已就位：
   RespCommand::ClusterFlushall（command_table.rs:396）→
   cluster_flush_all_slow（replication.rs:88）走 flush_all_databases 物理
   截断秒清。发送失败即该会话判败摘除（对标 C# SetFlushTask 收敛）。
   与 C# 一致点：清库帧先于记录帧、由主端发起、副本不本地 Reset。
B. 等价改在副本侧 try_replica_diskless_recovery 的 full_sync=true 臂调
   store.flush_all_databases（行为等价但偏离 C# 消息序：清库由副本自发，
   主端无往返）。选 B 须在 replica_diskless_sync.rs:179 全量支补注释声明
   偏离理由，勿与 A 双做。

优先级：功能缺口（数据正确性洞，主从永久发散）。

协调与边界

- 不做向下兼容、不留双源：二选一，禁 A/B 同时发清库（会双清）。
- 同文件串行：本单触 replication_sync_manager.rs / replica_diskless_sync.rs，
  与 task/ing/aof-driver-register-pre-transfer.md 同域，落地须串行协调。
- 无盘全量走的是 CLUSTER SYNC 记录帧（cluster_sync_slow），非检查点文件流
  （diskbased 走 receive_checkpoint_handler），两路互不影响，勿混改。
- 验收以只读源码复核 + 后续 fork 内 cargo check / replication e2e 用例为准
  （本代理不跑构建）。

细化方案（认领后核实追加，采方案 A）

核实结论：票据四组锚点全部成立（C# ReplicationSyncManager.cs:271-279
chooseBetweenFullAndPartialSync：NeedToFullSync → SetFlushTask(IssueFlush-
AllAsync) → 循环尾 WaitForFlushAsync；AofSyncDriver.cs:189 经快照流同一
garnetClient 发 ["CLUSTER","FLUSHALL"]；ReplicaDisklessSync.cs !disklessSync
才本地 Reset）。rust 侧 replication_sync_manager.rs stream_sync prepare 段
建连后直接 set_client + 入 full_sessions，无清库帧；副本 attach/recovery 均
不动键；cluster_sync_slow 恒 upsert。接收臂已就位：wnode command_table.rs:396
FLUSHALL → cluster_flush_all_slow → flush_all_databases 物理截断，副本非
primary 不二次入队广播，回 +OK。

改动三处（分支 f41-diskless-flush）：

1. wedb/wedb/src/client.rs：GarnetClient 加 issues_flush_all_async（挂
   AofSyncDriver.cs:IssuesFlushAllAsync 锚；facade 形态与
   execute_cluster_flushall_ns_async 同构：execute_for_string_result_async
   + 断连 reap）。
2. wedb/wedb/src/server/replication/diskless_replication/replica_sync_session.rs：
   DisklessSyncSession 加 issue_flush_all_async（挂 ReplicaSyncSession.cs:
   IssueFlushAllAsync 锚）：30s 停等（REPLICA_SYNC_CMD_TIMEOUT 模块常量
   一处定义，begin_aof_sync 的 attach 30s 同值改共用此常量）+ resp=="OK"
   判定（C# ContinueFlushTaskAsync 同口径，非 OK 返回 Err携原文）。
3. wedb/wedb/src/server/replication/diskless_replication/replication_sync_manager.rs：
   stream_sync prepare 段，client 建连成功后、set_client 之前对 full 会话
   发清库帧：失败 client.dispose() + set_status(Failed) + continue（复用
   同段连接失败路径形态，与 C# SetFlushTask 失败收敛 + Sessions[i]=null
   摘除同位）；成功才 set_client + push full_sessions。仅 FullResync 发
   （is_full 分支内，PartialResync 免），消息序对齐 C#：connect → FLUSHALL
   → 快照记录帧。模块文档 16-19 行"不含此两步"口径同步修订为磁盘链限定。

不改副本侧（replica_diskless_sync.rs 维持不本地 Reset，禁方案 B 双做）。

落地状态（f41-diskless-flush 分支，提交 88305ed2）

- 三处改动已在分支落地，干净基线（b518522b）cargo check 通过（独立
  target /tmp/wt-target-f41，零警告）；merge dev 后 --features tls 亦通过。
- 合并受阻：dev 基线损坏——wedb/src/server/replication/replica_wire.rs:36
  与 wedb/src/server/migration/migrate_driver/keys.rs:16 无条件
  use wconn::tls::ClientTlsConfig，而 wconn::tls 为 cfg(feature="tls")
  门控（wconn/src/lib.rs:10），默认 cargo check 必坏（E0432 两处）。
  该两文件为本单无关的并行合入，等 60 秒重 merge 一次仍坏，已按流程
  reset 回 88305ed2 保留分支。
- 待基线修复（补 cfg 门或修依赖布线）后重走：worktree merge dev →
  cargo check → 主仓 merge --no-ff f41-diskless-flush → 移票 task/done/
  → 清 worktree 与分支。
