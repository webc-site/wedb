# 复制导入失败/续传位置/attach 复位三处 C# 语义订正与有意分叉登记（主控亲办）

> 收口（2026-09-28 13:3x）：主控亲办，零行为改动，随本票归档销案。
> - 交付面两处注释在 HEAD 复验在册：`wedb/wedb/src/server/cluster_session/replication.rs:766-778`
>   （-ERR 收口不断连为有意分叉 + C# `GarnetException.cs:20/:22` 双默认真 +
>   `RespServerSession.cs:544-563` 先 -ERR 再 `DisposeNetworkSender` + rust 等价承接面
>   `snapshot_transmission.rs`/`receive_checkpoint_handler.rs` 的 `device_contaminated` +
>   禁回摆条款）；`wedb/wedb/src/server/replication/assembly.rs:185-193`
>   （attach 期 `applied_repl_offset` 不清零的次序依据 C# `ReplicaDiskbasedSync.cs:125` 的
>   `SetValue(0)` 前提在 `:146` `storeWrapper.Reset()`，rust attach 期无清库臂、该字段是 applied
>   位点权威源 + 禁回摆条款）。
> - 落笔形制：两笔注释编辑未及自提，被并发席 `4854e6b chore: 终态注记与代码格式化` 代提交收走，
>   内容逐字存活于其后每一 dev tip（本轮 `git show HEAD:` 双档复验），非我笔丢失。
> - 票内三面裁决与两桩否决（候选 4/5）一并归档；台账缺位说明见「背景」节，不另登 deviations。
> - 门禁：纯注释面零测试语义；本波 `./test.sh`/`./sh/clippy.sh` 因并发席 nightly-2026-09-28
>   链红（`task/issue/chain-upgrade-nightly-0928-waof-const-try-compile-break.md`）统一延后，
>   与本票改动面零交集。

定级：P3（治理面：注释对 C# 语义表述失真会诱导后续席位重复立案或按误读回摆；无行为改动）

甄别结论：通过（2026-09-28 只读审计席出候选，主控现树 + garnet 现树双侧亲验后收口）

## 背景：台账缺位

本仓 doc/zh/deviations.md 在 2026-09-28 机器重置与历史 squash 后已不在任何 ref
（git ls-files doc、git fsck 悬空对象、/tmp/fork 各 worktree 三面查净），而 94 个源文件
仍在正文注释里引用其 § 节号。故本票的「登记」不落台账，改落两处代码注释与本票票体
（票体随 task/done 常驻，可 grep 查重），待台账重建票统一回收条目。

## 三项判定与登记（逐条双侧锚点）

1. 检查点/migration 导入失败后 C# 拆会话、rust 仅回 -ERR 不断连（有意分叉，非缺陷）
   C# 侧：garnet/libs/common/GarnetException.cs:20 ClientResponse 默认真、:22 DisposeSession
   默认真；garnet/libs/server/Resp/RespServerSession.cs:538-563 catch(GarnetException) 先按
   ClientResponse 写 -ERR，再按 DisposeSession 调 networkSender.DisposeNetworkSender(true)
   拆会话；迁移臂 garnet/libs/cluster/Session/RespClusterMigrateCommands.cs:135 throw →
   :566-570 catch → Dispose。
   rust 侧：wedb/wedb/src/server/cluster_session/replication.rs:755-763 失败臂只
   write_resp_error 后挂 pending_slow，返回 true 不断连。
   等价承接证据：发送端逐帧等应答并判 OK、非 OK 整轮报错
   （wedb/wedb/src/server/replication/snapshot_transmission.rs:264-269，与 C#
   DiskbasedReplication/FileTransmitSource.cs:42-49 同构同文案）；接收端自持污染闸门
   （wedb/wedb/src/server/replication/receive_checkpoint_handler.rs:361-372
   session_gate 拒收余流）与迁移侧显式拒绝即重置接收态
   （wedb/wedb/src/server/migration/frame_import.rs:437-443/:468-473）。
   拆会话的目的（不再往坏流喂帧）既已承接，P2 危害不成立，降为登记；原注释把 C# 写成
   「clientResponse 默认形态，会话不断连」系表述失真，已就地订正并禁回摆。

2. RI/File 落盘臂缺「续传位置违约探测器」（纯防御面，非字节分歧）
   C# 侧：garnet/libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/
   RangeIndexFileDataSink.cs:87-91 WriteChunk 先判 stream.Position != startAddress 即抛
   GarnetException，再 stream.Write(data)；设备臂 FileDataSink.cs:63-77 写绝对 startAddress
   并校验扇区对齐；偏移源 RangeIndexFileDataSource.cs:70 StartOffset => 0、:118-140
   CurrentOffset 单调；EOF 哨兵 FileTransmitSource.cs:56-58 带 CurrentOffset。
   rust 侧：wedb/wedb/src/server/replication/receive_checkpoint_handler.rs:213-216
   SinkTarget::File 臂 open_truncate 建槽（:76-86）后 seek(SeekFrom::Start(start_address))
   再 write_all；发送端偏移与 EOF 帧亦逐点对齐
   （snapshot_transmission.rs:230-237 以 src.span().cursor 收尾）。
   判定：C# 的位置检查是违约探测器而非续传规则，良序流上两侧落盘字节完全等价；
   不可达面为发送端游标单调（rust 与 C# 同源），故不开修复票。残留登记：rust 缺该探针，
   主端若有 bug 会静默 seek 越过未写区写成稀疏洞，纯防御面。

3. attach 期复位次序与位点清零两处为有意分叉（照抄即回归已 done 的 P1 票语义）
   C# 侧：ReplicaDiskbasedSync.cs:116 先 await gcs.ConnectAsync(ReplicaSyncTimeout...)
   → :120 ResetReplicaReplayDriverStore → :123 aofSyncDriverStore.Reset() →
   :125 replicationOffset.SetValue(0)，其前提是同函数 :146 storeWrapper.Reset() 抹本地数据；
   diskless 臂 ReplicaDisklessSync.cs:87/:90 本就先复位后建连且 :220-222 把该字段当
   partial 续传位点读（不清零）。
   rust 侧：wedb/wedb/src/server/replication/assembly.rs:184-186 三处复位先于 :233
   client.connect_async()，且全无位点清零；该字段被两臂读作 applied 位点权威源
   （replica_diskbased_sync.rs:132 get_sublog_replication_offset、
   replica_diskless_sync.rs:190 get_current_replication_offset）。
   判定：rust 无 attach 期清库臂，清零即令部分重同步自 0 重放整库 wal，或误判
   granted < applied（replica_diskbased_sync.rs:133-137）强制全量重灌，正是
   task/done/wedb-repl-diskbased-partial-resync-skips-replica-recover-clamp.md 建立的语义，
   同轴勿重开；次序差三条可达性逐一拆净（断链/换主时本节点不在供流态，角色门见
   cluster_session/replication.rs:566-572；接收槽残留在上轮失败点
   receive_checkpoint_handler.rs:568-572 已置位，提前复位不新增闭锁）。
   已就地登记：assembly.rs 复位序列后补注，声明两处不复位/不摆回的理由。

## 涉及代码（本轮实改，纯注释）

rust 文件与函数：
- wedb/wedb/src/server/cluster_session/replication.rs:ClusterSession::network_cluster_snapshot_data
  （文档注释段订正 C# 语义表述并钉等价承接两处锚点）
- wedb/wedb/src/server/replication/assembly.rs:recover_replication（复位序列后新增分叉登记注）

对应 c# 文件与函数：
- garnet/libs/common/GarnetException.cs:20/:22
- garnet/libs/server/Resp/RespServerSession.cs:538-563
- garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:116/:120/:123/:125/:146
- garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDisklessSync.cs:87/:90/:220-222
- garnet/libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/RangeIndexFileDataSink.cs:87-91
- garnet/libs/cluster/Session/RespClusterMigrateCommands.cs:135/:566-570

## 候选 4/5 的否决记录（不开票，防再占复制链席位）

- 「UpdateLastPrimarySyncTime 应外提至同步开始处一次」否决：C# 本就在逐帧热路径刷
  （ReceiveCheckpointHandler.cs:51-53/:84-86/:105-107 三入口各一次，定义
  ReplicationManager.cs:40），rust receive_checkpoint_handler.rs:361-372 的 session_gate
  为三入口共用（:436/:519/:605），粒度 1:1；rust 另多两处 C# 没有的挂点
  （cluster_replication_session.rs:229-233、replica_diskless_sync.rs:178），消费面唯一是
  INFO master_sync_last_io_seconds_ago（cluster_provider/traits.rs:427-430 对位
  ClusterProvider.cs:254），外提零收益。
- 「File arm 位置连续性缺实现」如 §2 判为不可达分歧，不另开票。

## 边界与纪律（主控亲办，已遵守）
- 只改上述两文件的注释面，零行为改动，无新增/删除函数，无测试改动（注释不产测）。
- 禁触在途席域：wedb/wedb/src/server/replication/replica_wire.rs、
  replica_sync_session.rs、diskless_replication/replication_snapshot_iterator.rs、
  wedb/wedb/src/server/cluster_provider/flags.rs、wedb/wconn/**、wedb/wtxn/**、
  wedb/wkv/src/read_cache/**、wedb/wreviv/**。
- 提交带显式 pathspec（主树他席在途脏文件不入我笔），禁 git add -A，禁 stash。
