# MIGRATE 源端生产链：命令解析触发驱动 + SLOTS 变体装配 + 幽灵命令裁决（next/clude.md 条 1、next/gemini.md 条 3 M3、next/glm.md 条 8 M3、next/net.md 条 19）


## 甄别结论

三点全部成立，两处按 C# 证据修正执行口径。


### 1. MIGRATE 源端驱动未接生产、SLOTS 变体缺失：成立

C# 生产链完整闭环：

- libs/cluster/Session/ClusterSession.cs:110 顶层 MIGRATE 分派 →
  MigrateCommand.cs:NetworkTryMIGRATE 解析（host port <key|""> db timeout
  [COPY] [REPLACE] [AUTH pw] [AUTH2 user pw] [[KEYS ks]|[SLOTS ss]|
  [SLOTSRANGE a b ...]]），逐形态校验（UNKNOWNTARGET/TARGETNODENOTMASTER/
  SLOTNOTLOCAL/CROSSSLOT/SLOTOUTOFRANGE/MULTISLOTREF/INCOMPLETESLOTSRANGE）→
  TryAddMigrationTask → MigrationDriver.cs:TryStartMigrationTaskAsync。
- KEYS 变体：同步阻塞执行 MigrateKeysAsync（BlockingWait）→ +OK/-ERR，
  finally TryRemoveMigrationTask。
- SLOTS/SLOTSRANGE 变体：fire-and-forget BeginAsyncMigrationTaskAsync
  后台任务，命令立即 +OK；后台链 = 远端 IMPORT → 本端 MIGRATING →
  BumpAndWaitForEpochTransition → 游标扫描（INITIALIZING 收键 →
  TRANSMITTING 传输 → DELETING 删除 → 清 sketch → cursor 前进，直到无键）→
  SuspendConfigMerge + TryMeetAsync → 远端 NODE → RelinquishOwnership →
  TryMeetAsync → SUCCESS；任一失败点 TryRecoverFromFailureAsync；finally
  TryRemoveMigrationTask。

rust 缺口核实：RespCommand::Migrate 已被 wnode/src/resp/resp_server_session.rs
显式路由进集群会话，但 cluster_session.rs process_cluster_commands 无
Migrate 臂，落 `_ =>` 兜底回「unknown subcommand 'CLUSTER'」；无 SLOTS
驱动；run_keys_migration_driver 仅测试调用；迁移任务结束不从
MigrationManager 移除（上轮 migrate-wait-timeout 遗留记录）。

修正口径两处：

- NOTMIGRATING 检查不移植：C# KEYS 形态要求槽位已被管理员 CLUSTER SETSLOT
  MIGRATING 预置，其 KEYS 驱动（MigrateKeysAsync）不做任何槽位编排；
  rust 驱动（run_keys_migration_driver，上轮验收基线）自动编排
  IMPORTING → MIGRATING → NODE → relinquish（try_prepare_local_for_migration
  要求槽位 STABLE 起步）。两套语义互斥，若保留预置检查则 rust 驱动必然
  失败回滚。裁决：以 rust 驱动既有语义为准，解析层保留 SLOTNOTLOCAL 与
  CROSSSLOT 检查，不做 NOTMIGRATING 检查，代码注释声明偏差。
- AUTH/AUTH2 选项照做：红线禁的是微软认证与凭据透传；MIGRATE 第 6 位的
  AUTH password / AUTH2 username password 是 redis MIGRATE 标准选项，供
  源端连目标端认证（C# MigrateCommand.cs:172-180），MigrateTaskSpec 的
  username/passwd 字段与 GarnetClient::with_auth 上轮已备，解析层对齐
  填充即闭环（此前字段无处流入）。

Hostname DNS 解析裁剪：C# 解析失败时 Dns.GetHostEntry 重试。rust 无此
基建，address 仅按集群配置精确匹配（get_worker_node_id_from_address），
不匹配即 UNKNOWNTARGET。登记差异，后续有解析基建再补。


### 2. 六个集群幽灵命令：成立，全部属 M4 checkpoint 传输流，无一属 MIGRATE 语义

C# 对照：ATTACH_SYNC/BEGIN_REPLICA_RECOVER/SEND_CKPT_FILE_SEGMENT/
SEND_CKPT_METADATA/SNAPSHOT_DATA/SYNC 六臂（RespClusterReplicationCommands.cs
NetworkClusterAttachSync 族）全部落在 replicationManager.recvCheckpointHandler
检查点接收链，是全量同步三段握手（INITIATE_REPLICA_SYNC → ATTACH_SYNC →
SYNC/SEND_CKPT 检查点流）的后续两段，与 MIGRATE 无涉。任务描述里的
DisableClusterCheckpointFromFileProvider 配置形态在 garnet 源码中不存在
（全仓 grep 无命中），不可作为对齐锚点。

裁决（按 ds.net 条 1 短期方案，口径修正一处）：从
wnode/src/resp/parser/resp_command.rs 的 CLUSTER_SUBTABLE 摘除六项，客户端
发送即「unknown subcommand」，消除注册而无臂。ds.net 条 1 建议同时摘
resp_commands_info_data.rs——过时：该文件在 resp-command-strum-dedup
（task/done）后收敛为 strum 全枚举双射（自述覆盖全枚举、含非真实命令），
摘项即破坏枚举↔名映射，不摘。wresp RespCommand 枚举变体同样保留：C#
保留全部枚举成员，is_cluster_sub_command 的区间判定（ClusterAddslots..
=ClusterSync）依赖上界，摘变体需重排编号且破坏区间语义。C# 顶层
"SYNC" → CLUSTER_SYNC（RespCommandHashLookupData.cs:370）的注册 rust 本就
没有，维持现状，M4 复制同步立项时随实现重建。


### 3. 孤儿键投影未声明：成立，顺手补

SLOTS 变体删除游标基于「已确认 ACK 的键才删」，但远端置 NODE 且 gossip
传播后、源端物理删除完成前的窗口内，槽位所有权已移交而键仍在源端——
按槽位路由的读在源端命中旧键投影（孤儿键）。C# 相同（DELETING 在 NODE
交权前逐批发生，但 gossip 传播与删除交错的窗口同样存在）。属安全投影
（不丢数据、最终一致），migrate_driver.rs 模块注释补声明。


## 对标与改动点

1. wedb/wedb/src/server/migration/migrate_driver.rs
   - 新增 run_slots_migration_driver（对标 MigrateSessionSlots.cs:
     MigrateSlotsDriverInlineAsync + ScanStoreTaskAsync，串行单任务投影，
     并行迁移任务明确不做）：IMPORTING → MIGRATING → bump_and_wait →
     逐槽游标循环（get_keys_in_slot 批量取键 → 收录 sketch →
     TRANSMITTING 分批停等传输 → DELETING 删除已传键 → 清 sketch）→
     完成哨兵 → NODE → relinquish；失败统一 try_recover_from_failure；
     finally try_remove_migration_task（对标 C# TryStartMigrationTaskAsync
     finally）。对象键经 probe 预检拒绝（同 KEYS 路径守卫）。
   - run_keys_migration_driver 补 sketch 收录与状态推进（对标
     MigrateKeysAsync 的 TRANSMITTING/DELETING/MIGRATED 序列），使
     can_access_key 门控真实生效；结束移除任务。
   - 模块注释补孤儿键投影声明（net.md 条 19）。

2. wedb/wedb/src/server/cluster_session.rs
   - 新增 RespCommand::Migrate 臂（对标 MigrateCommand.cs:NetworkTryMIGRATE）：
     参数解析（strict_i32 端口/db/timeout）、COPY/REPLACE/AUTH/AUTH2 选项、
     KEYS/SLOTS/SLOTSRANGE 三形态槽位校验与错误文案（对标
     HandleCommandParsingErrors）、目标节点与角色校验（UNKNOWNTARGET/
     TARGETNODENOTMASTER）。
   - KEYS 形态：sketch 收录 → 同步 await run_keys_migration_driver →
     +OK/-ERR（对标 BlockingWait）。
   - SLOTS/SLOTSRANGE 形态：spawn detached 后台任务跑
     run_slots_migration_driver，命令立即 +OK（对标 fire-and-forget）。
   - MigrateTaskSpec 增 transfer_option（迁移 migration_manager.rs 已定义
     未消费的 TransferOption，对标 C# MigrateSession.transferOption）。

3. wedb/wnode/src/resp/parser/resp_command.rs
   - CLUSTER_SUBTABLE 摘除 ATTACH_SYNC/BEGIN_REPLICA_RECOVER/
     SEND_CKPT_FILE_SEGMENT/SEND_CKPT_METADATA/SNAPSHOT_DATA/SYNC 六项。

4. wedb/wedb/tests/cluster_migration.rs
   - SLOTS 驱动直调：全链成功（帧序 + 删除 + 交权）、失败 recover。
   - MIGRATE 命令入口经 RespSessionConsumer：KEYS 同步 +OK/删键、SLOTS
     后台 +OK + 轮询驱动至完成、解析错误路径（未知目标、cross-slot、
     slot not local）。


## 范围外仅记录

- 顶层 SYNC 顶层注册缺失（C# SYNC → CLUSTER_SYNC），M4 复制同步立项对标。
- COMMAND 目录（wresources 内嵌 JSON）仍含六命令描述，M4 重建时对齐。
- KEYS 驱动删除时点在 relinquish 之后（C# KEYS 路径无交权步骤），
  属上轮验收基线语义，不回改。


## 验收口径

1. ./clippy.sh 零警告（禁 allow）。
2. ./test.sh 全过。
3. bun ./js/check.js 无新增缺失/重复。
4. CLUSTER_SUBTABLE 六项摘除后无新增「注册而无臂」；run_keys_migration_driver
   与 run_slots_migration_driver 均有生产调用点。
