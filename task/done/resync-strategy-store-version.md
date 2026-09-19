# 同步策略 store_version 归位：拆分磁盘同步与无盘同步两套判定（承接 next/resync-strategy-store-version.md，票据已核销删除）

## 核实结论（HEAD 415e0d0）

票据判断整体成立，但其取证基线为旧 sha 74f9576，本轮 dev 已推进（diskless-fanout 波
36fcc822 等），部分落点已落地、部分行号漂移。本文件为承接后的唯一载体并给出修正口径。

已落地面（不再列为待做，照 HEAD 事实核销）：
1. 无盘副本侧 ATTACH_SYNC 上报不再写死 0：replication/replica_diskless_sync.rs
   replica_diskless_attach（现 :83）构造元数据在 :137-140 已取
   provider.try_store().map(|store| store.current_version()).unwrap_or(0)，
   对标 C# ReplicaOps/ReplicaDisklessSync.cs:164 currentStoreVersion: storeWrapper.store.CurrentVersion。
   票据「副本侧 current_store_version: 0 硬编码」这一条已不成立。
2. 无盘主端恢复帧亦已带版本：diskless_replication/replica_sync_session.rs:249-257
   recover_meta 的 current_store_version 同取 store.current_version()，
   对标 DisklessReplication/ReplicaSyncSession.cs:237 currentStoreVersion 回给副本。
   版本源在位，无盘链路的 store 版本维度不再整体丢失。

成立且待做面（逐条对照当前代码核实）：
1. 合并判定体仍是磁盘与无盘两条 C# 链路的单函数：
   replication/replication_manager.rs determine_resync_strategy（现 :705-815）被两条链路
   同挂——磁盘臂调用方 replication/replica_sync_session.rs:139（initiate_replica_sync，
   其文档注释 :110-111 锚 DiskbasedReplication/ReplicaSyncSession.cs:SendCheckpointAsync）、
   无盘臂调用方 diskless_replication/replication_sync_manager.rs:258（prepare 段逐会话协商，
   锚 DisklessReplication/ReplicationSyncManager.cs:PrepareForSyncAsync）。
2. 合并判据里有非 C# 对应物的错误子句：:802
   if is_partial_possible && (replay_aof_mask > 0 || replica_meta.current_store_version > 0)
   —— 该 current_store_version > 0 支路在两条 C# 链路都无对应：
   磁盘链路 ValidateMetadata 的 skip 判据只由两侧 CheckpointEntry 的
   storeHlogToken / storeVersion / storePrimaryReplId 决定
   （DiskbasedReplication/ReplicaSyncSession.cs:73，rust 已在 :733-741 对标实现），
   全函数不读 currentStoreVersion；
   无盘链路 NeedToFullSync 的语义是版本不相等才全量
   （DisklessReplication/ReplicaSyncSession.cs:204
   sendMainStore = !sameHistory || replicaSyncMetadata.currentStoreVersion != currentStoreVersion），
   与 rust 的 > 0 方向相反：现下 any 非零版本都会让 is_partial_possible 直接判 PartialResync，
   版本不一致 / AOF 位点越界 / 待回放量超阈值这三条 NeedToFullSync 全量臂完全缺失。
3. 磁盘路径的字段错配残留：cluster_session/replication.rs:604
   network_cluster_initiate_replica_sync 合成的 replica_meta 在 :648 仍 current_store_version: 0。
   C# 磁盘入口 NetworkClusterInitiateReplicaSync
   （Session/RespClusterReplicationCommands.cs:259-296）根本不构造带 store 版本的
   SyncMetadata，只把 replicaCheckpointEntry 与 AOF 起止位点交给
   TryBeginDiskbasedSyncAsync；rust 把该字段喂进合并判定，才逼出 :802 那条子句。
   拆臂后磁盘臂不再引用该维度，:648 的 0 成为磁盘链路无效字段（注释说明即可，禁再造裸 0 语义）。
4. 既成后果（磁盘臂）：同历史同版本、无 AOF 增量的重连，
   skip_local_checkpoint 为真但 replay_aof_mask == 0、current_store_version == 0，
   :802 恒假 → 落 :811 FullResync，调用方 replica_sync_session.rs:156-159 随即调
   send_checkpoint_and_recover（:219），而该函数仅在本地无检查点条目时跳过下发
   （:309 "Skip checkpoint send: no local checkpoint"），
   故同历史同版本重连确会多推一遍检查点并触发副本侧导入。
5. 文档注释锚点错配：determine_resync_strategy 现 :697-698 一条函数挂两条 C# 锚点
   （NeedToFullSync + PrepareForSyncAsync），两条都属无盘链路，磁盘调用方借用无盘锚点，
   check.js 会把两个 rust 调用面计入同一 C# 引用，拆臂后各归一锚。

## 实现方案（拆两套判定，一处共用）

1. 拆分 determine_resync_strategy 为磁盘臂与无盘臂两个入口，共用体只保留 AOF 接续位点
   计算（现 :743-800 子日志循环：same_main_store_checkpoint_history、skip_local_checkpoint、
   replay_aof_mask / sync_start_address 推进）抽私有 helper，两处调用方各挂各的判定：
   - 磁盘臂（调用方 replica_sync_session.rs:139）：partial/full 由 skip_local_checkpoint
     与 replay_aof_mask 决定，删去 replica_meta.current_store_version > 0 子句，
     对标 DiskbasedReplication/ReplicaSyncSession.cs ValidateMetadata（:73）+
     SendCheckpointAsync 的 skipLocalMainStoreCheckpoint 协商段；不读 store 版本维度。
   - 无盘臂（调用方 replication_sync_manager.rs:258）：按 NeedToFullSync 四条件重写
     （DisklessReplication/ReplicaSyncSession.cs:204）——主从历史不一致、
     副本 store 版本 != 主端当下版本（主端当下版本取 provider store.current_version()，
     与 replica_sync_session.rs:254 同源，不另立版本源）、副本 AOF 尾位点越界、
     待回放量超 ReplicaDisklessSyncFullSyncAofThreshold 阈值。
     方向是 != 而非 > 0。
2. 阈值面不臆造：仓内 grep 无 ReplicaDisklessSyncFullSyncAofThreshold 对应配置
   （server_options / fast_aof_truncate 同级皆无）。按 .agents/skills/transpile/SKILL.md
   不新增第二套门限常量、不自造默认值，本票只登记该阈值臂在 rust 配置面缺席，
   待门限配置单独立项时接上；无盘臂先按前三条件落地，第四条件留注释标注 C# 锚点与缺席原因。
3. 磁盘链路 :648 归位：拆臂后磁盘臂不引用 current_store_version，
   network_cluster_initiate_replica_sync 构造处对该字段加注释说明「磁盘链路判定不消费此维度，
   与 C# 磁盘入口不构造 store 版本一致」，或改由 CheckpointEntry.store_version 单点填充，
   二者择一，禁止再留无语义裸 0。
4. 锚点单点化：拆出的两函数各自文档注释只挂本链路的「在 garnet 中的相对路径:函数名」，
   磁盘臂锚 DiskbasedReplication/ReplicaSyncSession.cs、无盘臂锚
   DisklessReplication/ReplicaSyncSession.cs，消除 :697-698 一函数双锚，避免 check.js 重复计数。
5. 立项前查重：task/ing 现有 dbmeta-atomic-batch、migration-frame-import-core、
   tiered-background-demote、vector-key-ttl-fourth-domain 皆非本主题，无重。
   本主题无未合入分支（cluster-suspend-await-lock 分支属集群挂起锁，与本票无关）。

## 验收对照

- 现有断言按合并判据写死，拆臂后须按两套新判据改写、不得只改期望值蒙门禁：
  tests/replication_manager.rs:299-347（current_store_version: 10 参与 :802 partial 判定）、
  tests/replication_pipeline.rs:188-249（current_store_version: 100 两处），
  按磁盘臂（不读版本）与无盘臂（!= 判等）分别重写期望。
- 补端到端断言：同历史同版本、无 AOF 增量的副本重连，磁盘链路不再触发检查点全量下发
  （经 send_checkpoint_and_recover 且断言走 :309 之外的续传臂 / 不落 FullResync）。
- 验证纪律：仅 cargo check --workspace --all-targets（私有 target 目录），
  REAL_EXIT=0、0 error 0 warning；test.sh/clippy 由中央整合轮执行。

优先级：功能缺口（同步策略误判致磁盘重连全量重传、无盘 partial 语义与 C# 反向且缺三条全量臂）。
修订前须先出报告确认拆界面与无盘臂四条件的落地边界（尤其阈值缺席的登记口径）。

## 落地记录（分支 resync-strategy-split，合入 dev 26de243）

拆界结果与票据口径一致，三处单点落在
wedb/wedb/src/server/replication/replication_manager.rs：

1. negotiate_resync（私有）＝共用的检查点下发与 AOF 接续位点协商体，锚点自
   DiskbasedReplication 的错配路径归位到
   libs/server/AOF/GarnetAppendOnlyFile.cs:ComputeAofSyncReplayAddress（原
   ResyncStrategy 枚举头上那条 DiskbasedReplication...:ComputeAofSyncReplayAddress
   错锚一并撤掉，一函数一锚）。
2. disk_resync_strategy ＝磁盘臂，只由 skip_local_checkpoint（含两侧
   CheckpointEntry.store_version 比对）与 is_partial_possible 决断，
   current_store_version > 0 子句删除，磁盘重连同历史同版本无增量不再落
   FullResync。锚 DiskbasedReplication/ReplicaSyncSession.cs:ValidateMetadata，
   并把该符号从 js/check/ignore/cluster.yml 摘除（既有 rust 实现，不再 ignore）。
3. diskless_resync_strategy ＝无盘臂，按 NeedToFullSync 三条件落地：主从历史
   不等、副本 current_store_version != 主端当下版本（主端版本由
   replication_sync_manager 取 provider store.current_version()，与恢复帧同源，
   不复挂 ReplicationSyncManager.cs:PrepareForSyncAsync 锚——该锚归位到
   stream_sync 本体）、副本 AOF 尾位点 is_out_of_range(主 begin, 主 tail)；
   为此补 waof/src/aof/address.rs:AofAddress::is_out_of_range（C# 同名 1:1）。
   第 4 条件 ReplicaDisklessSyncFullSyncAofThreshold 按票据登记缺席不臆造，
   GarnetAppendOnlyFile.yml 的 ignore 理由同步改指 negotiate_resync。
4. cluster_session/replication.rs 磁盘入口合成元数据的 current_store_version
   裸 0 改由 checkpoint_entry.metadata.store_version 单点填充（C# 磁盘链的
   store 版本维度本就只从 CheckpointEntry 取），不留无语义 0。
5. 无额外开关、无兼容分支、无 #[allow，旧合并函数整体删除（全仓
   determine_resync_strategy 零残留）。

测试：tests/replication_manager.rs 拆成 test_disk_resync_strategy_partial_and_full
（补 idle 重连 Partial 回归臂、上报版本抬到任意值不改判、两侧条目版本不等才
Full）与 test_diskless_resync_strategy_need_full_sync_conditions（版本 != 双向、
尾位点越上界/下界、历史不一致、四条件不成立且有增量才 Partial）；
tests/replication_pipeline.rs 三处改挂磁盘臂并改名。

一处连带修正：tests/diskless_sync_ri_vector.rs 原把副本上报的
current_primary_repl_id 写成主端的 id（真副本取本端 rm.primary_repl_id()，
兄弟两用例即如此），合并判据下靠假全量臂兜住，拆臂后改按真判据构拟，
快照流断言不变。端到端「磁盘链不再下发检查点」按策略层断言承接
（磁盘调用方仅在 FullResync 上走 send_checkpoint_and_recover），未新造
双节点网络用例。

门禁实测（分支树，合并 dev 后复跑）：cargo check --workspace --all-targets
0 error 0 warning；cargo nextest run -p wedb --no-fail-fast replication
64/65（唯一红 server::replication::checkpoint_store::tests::
test_purge_all_except_entry_cleans_orphan_files，本票未碰该文件，属
task/ing/checkpoint-store-purge-entry-list-csemantics.md 在途域）；
resync + diskless 过滤 7/7 全绿。

合并副作用与处置：merge --no-ff 落 dev 后，三方解析把一次并发过期工作树提交
（vector-preview-production-enable 波的 83fc02b/0a42c90 与 wcol/wconf 收敛）
的旧态当本侧变更带入，吞回 vector_set_production_switch.rs（103 行）与
vector_manager/service/wcol sorted_set_object/wconf node_options 约百行，以及
三张票的 done 归档位；已逐路径核 blob（确认自合并后无人再改）在 dev 上以
7a83b8c 撤回。他人未提交在途的 wedb/wlua/src/hash_key.rs（use 顺序漂移）与
task/ing/cluster-slot-gate-sync-spin.md 两处故意不动，留其本主处理。

dev 现存非本票红：wnode 在 HEAD 编译失败——b0afcfe6（rm-wacl-auth-settings）
删了 SessionDependencies.acl_settings 字段而其调用方 service.rs 的同步修改
仍在其 worktree 未落，本票合入前后两侧调用点同态，属该票落地时收口。

