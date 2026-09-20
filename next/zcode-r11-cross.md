r11 跨域交互:客户端命令 × 集群/复制/迁移组合态

方法:列状态向量(节点角色 master/replica/迁移中/导入中/failover 中/复位中 × 目标键 本地/迁移中/已迁出/导入中/升阶中/TTL 将到期),对产生跨域决策的格子逐格核 rust 判定链(文件:函数)与 C# 对位。已立发现不重复:迁移锚死默认域(r3)、attach 双泵(r5)、KILL 对阻塞挂起无效力(r4)、vdb 析构竞态(r2)、迁移半失败 +OK(r8-c)、resync 起点错位(r10-idem)。

一、立项(真差异,3 条)

1. 事务 EXEC × 迁移传输窗:rust 立即 TRYAGAIN 作废事务,C# 服务端自旋等待后照常提交

rust 判定链
wnode/src/resp/txn_resp_commands.rs network_exec → wnode/src/resp/resp_server_session/txn.rs verify_cluster_txn_keys → wnode/src/cluster_session.rs network_multi_key_slot_verify → wedb/wedb/src/server/cluster_manager_slot_gate.rs evaluate_multi_key_gate → resolve_can_operate 命中 SketchStatus::Transmitting(can_access_key 仅读放行)→ KeyOperable::AccessPending → GateVerdict::Wait → verify_cluster_txn_keys 的 Wait 臂 `let _ = cluster.take_pending_slow();` 丢弃等待体、写 ERR TRYAGAIN、事务队列 reset。

C# 对位
libs/server/Transaction/TxnKeyManager.cs VerifyKeyOwnership → libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs NetworkIterativeSlotVerify → ClusterSlotVerify.cs CanOperateOnKey:`while (!CanAccessKey(...)) { Thread.Yield(); }` 自旋至传输窗结束(MIGRATED),Exists 为真即 OK,事务在源端照常提交;仅键间状态混合(Migrated 混存留)才落 TRYAGAIN。

影响
迁移传输窗内 EXEC:C# 最终提交;rust 客户端收 TRYAGAIN 且队列已弃(须整段重发 MULTI+命令)。EXEC 是事务终点无法回退游标重评(代码注释已自认该约束),但客户端可见结果与 C# 相反,属会话状态错乱,非理论。对照:脚本内 redis.call 重入路径(wnode/src/resp/resp_server_session/lua.rs dispatch_resp)对槽门 Wait 是同步驱动等完再回——同一迁移窗内脚本等待后执行、EXEC 立即失败,rust 内部两事务面行为也互相矛盾。

附:verify_cluster_txn_keys 注释称「按 C# VerifyKeysInRange 迁移中混合态应 TRYAGAIN」只覆盖键间混合态;纯等待态 C# 无 TRYAGAIN。修复/登记时勿以该注释为 C# 锚。
方向:EXEC 预校验 Wait 臂做一次有界短重评(probe+重评一轮),或显式登记差异;至少修正注释。

2. 无盘全量同步 Blocking 相:开窗期间全库(全槽位)写命令冻结,窗长 O(全库键枚举),超时后写命令收 TRYAGAIN;C# 流式检查点对客户端零门

rust 判定链
wedb/wedb/src/server/replication/diskless_replication/replication_snapshot_iterator.rs run:ScanGateGuard::register(即入 PHASE_BLOCKING)→ 纪元静止取锚 → 逐域 storage.get_keys_in_slot(slot, usize::MAX) 全量枚举全部活跃域全部键 → gate.begin_scan() 才切栅放相。客户端写侧:wnode/src/resp/resp_server_session_slot_verify.rs can_serve_slot → cluster_manager_slot_gate.rs scan_gate_blocks → scan_key_gate.rs blocks_write(PHASE_BLOCKING 恒 true,不看键不看槽)→ wait_key_gate 挂起,deadline = cluster_node_timeout(默认 30000ms,wedb/wedb/src/args.rs:256;0 = 无限)→ memo.exhausted → 终评 TryAgain。

C# 对位
libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/(MainStreamingSnapshotDriverAsync + TakeStreamingCheckpointAsync):Tsavorite 流式检查点自洽快照,客户端写全程照常服务,不存在任何客户端可见停窗或错误。

影响
rust 全量同步(唯一复制形态,主二进制会话恒带集群切面)每次都以「全库写冻结 + 全键枚举」开窗;大库上窗内写入全部挂起,30s 后开始成批 TRYAGAIN——且此时槽位是 STABLE,TRYAGAIN 语义(迁移窗/reshard)被挪用,C# 侧无任何对位错误。数据不损坏(门正确性成立),错在客户端可见可用性与错误面。
方向:键集枚举挪出门前窗(注册门后仅按枚举产物栅放,锚点语义另证)、或给扫描门独立短超时预算并与迁移门分离、至少在文档登记该停窗与 TRYAGAIN 挪用。

3. slot_wait_memo 跨命令泄漏:迭代门路径 clone 不 take,超时 exhausted 与按键下标的 exists 缓存毒化后续命令门评

rust 判定链
wedb/wedb/src/server/cluster_session/slot_verify.rs:network_iterative_slot_verify 第 71 行 `let memo = self.slot_wait_memo.lock().clone();`(取快照不消费),park_gate_wait 第 256 行覆写登记;全仓仅 evaluate_multi_key_slot_gate 第 208 行 `take()` 消费。RUNTXP 迭代门(Pending → park → pending_rearm 重驱)走 clone 路径,终评后 memo 常驻会话(wedb/wedb/src/server/cluster_session/mod.rs:71)。SlotWaitMemo(wedb/wedb/src/server/cluster_manager_slot_gate.rs):exhausted 置位后压制后续一切等待点;exists 缓存按「键下标 idx」而非键名。

后果链
a) 某次 wait_key_gate 超时(exhausted=true)后,该会话下一条普通命令在 evaluate_multi_key_slot_gate take 到毒 memo → ctx.force()=true:MIGRATING 键免等待直评 NotOperable→ASK,扫描门键直评 TRYAGAIN。一次泄漏毒化一条命令。
b) exists 缓存按 idx 复用:前条命令磁盘候选键(resolve_can_operate probe Ok(None))的存活裁决,被后条命令同下标异名键取用——Some(false) → NotOperable → 对实际存活的冷盘候选键出 ASK,客户端被重定向到无该键的目标节点。

C# 对位
ClusterSlotVerify.cs CanOperateOnKey 每次全新自旋 + Exists,无任何跨命令记忆;毒化面为 rust 挂起化自引入。
方向:迭代门路径同样 take(或命令终评/事务收口统一清 slot_wait_memo);exists 缓存改按键名制或消费即清。

二、逐格核查(判定一致)

迁移域
- GET/TTL/TYPE/EXISTS × MIGRATING 双源期(源残留+目标已写):读臂放行(cluster_manager_slot_gate.rs evaluate_single_key → resolve_can_operate:MigrateSession.rs can_access_key Transmitting 仅读放行 / Initializing|Migrated 放行 → probe_key_alive 存在即 serve)。C# MigrateSessionKeyAccess.cs CanAccessKey 同表、ClusterSlotVerify.cs SingleKeyReadSlotVerify 同臂。一致。
- SET/DEL/EXPIRE × TRANSMITTING:写挂起(Transmitting 仅读),C# 自旋等同一谓词。唯一差别是等待预算:rust 30s 强制终评(MIGRATING→ASK),C# 无限自旋——该差异在 cluster_manager_slot_gate.rs 模块头已声明为 compio 适应设计,不另立。一致。
- SET × MIGRATED 双源窗(源未删):放行落源端,随后 DELETING 相删除致该写丢失,目标持旧值。C# MIGRATED→true 同语义同丢失,上游固有。一致。
- DEL × 传输中键:删除属写被门;驱动删除清单只含已 ACK transferred 键(migrate_driver/keys.rs :905-913),竞态过期/删除键 Gone 不入清单(keys.rs LiveValue::Gone 臂),键权不误移。C# DeleteKeysAsync 同制。一致。
- 多键命令 × 迁移混合态:any_operable && any_moved_away → TRYAGAIN(cluster_manager_slot_gate.rs evaluate_multi_key_gate);C# VerifyKeysInRange 状态枚举不一致 → TRYAGAIN。一致。
- SCAN/KEYS × 迁移窗:两侧均无槽位过滤、全量扫本地存储(C# ArrayCommands.cs NetworkSCAN → storageApi.DbScan;rust 慢路径同制),双源期均会返回已传未删键。一致。
- sketch 门粒度:全局状态 + bloom 位图、哈希碰撞键同样被门(wedb/wedb/src/server/migration/sketch.rs probe 对 C# Sketch.cs Probe)。一致。
- 多迁移任务并存:TaskStore 按槽位索引单会话查门(migrate_session_task_store.rs :128 对 MigrateSessionTaskStore.cs CanAccessKey)。一致。
- TTL 将到期 × 迁移:载荷携 expire_unix_ms(keys.rs :575-579),读值时已过期键归 Gone 不发帧;C# 迁移读侧过期即不可见。一致。

复制/副本域
- 副本写命令面(集群态):single_key_read_write_slot_verify role==Replica → MOVED(slot_verify.rs :181);BLPOP/EVAL 均属写区间(wresp/src/command.rs is_read_only/is_data_command 区间对 C# RespCommand.cs First/LastWrite/Read/DataCommand 逐界同源,目录键规格 RespCommandsInfo.json 逐条同文),副本上同收 MOVED。一致。
- 副本 FLUSHDB/FLUSHALL:wnode/src/resp/basic_commands/mod.rs flush_replica_read_only_gate:集群切面在(is_replica)拒 READONLY_REPLICA,切面缺席(独立面恒放行);对 C# BasicCommands.cs:1038/1060 `EnableCluster && IsReplica() && !IsInternalWriteSession`(rust 回放不经 RESP 分派,豁免天然成立)。一致。
- EVAL 内 redis.call:lua.rs dispatch_resp 重入 try_consume_messages 全门链(含槽门/扫描门),挂起同步驱动闭环;C# 脚本重入 TryConsumeMessages 同链。一致。
- 副本恢复态读/写:is_recovering 读臂 Replica→MOVED、Primary→CLUSTERDOWN,写臂先 role 后 recovering(slot_verify.rs 两臂)对 C# SingleKeyReadSlotVerify/SingleKeyReadWriteSlotVerify 逐臂同构。一致。
- 会话 ASKING 计数:置 2、每命令尾递减(wnode/src/resp/resp_server_session/core.rs :682 对 C# RespServerSession.cs:736);门 Wait 重驱不重复递减(break 在指标段前)。一致。
- 全量同步×读命令:扫描门只栅写,读全程放行(scan_key_gate.rs blocks_write 仅写消费);C# 检查点窗读亦不受阻。一致。

failover 域
- 停写机制:try_stop_writes = MakeReplicaOf+AssignSlots(STABLE)(wedb/wedb/src/server/cluster_manager.rs :576 对 C# ClusterManager.cs :360),旧主写 MOVED、普通读 MOVED(READONLY 会话经 is_local_expensive 副本读臂继续服务;cluster_config/mod.rs is_local 对 C# ClusterConfig.cs IsLocal/IsLocalExpensive 逐臂同构)。客户端会话保持连接,不杀不迁。一致。
- 角色切换 × 在途事务/阻塞臂/订阅:翻转不终止在途 EXEC(门评已过即落旧节点,两侧同),阻塞观察者持旧对象至超时空返(两侧同),订阅节点本地不随角色迁移(pubsub 本地制两侧同)。一致。
- failover 后旧主 reset:复位收口回发 FAILSTOPWRITES(replica_failover_session.rs reset_if_needed,abort 后仍复位属对 C# 缺陷的登记不对标,代码内已注)。一致。

导入/复位/清库域
- IMPORTING × 客户端:非本地+IMPORTING:ASKING→OK 否则 MOVED;导入帧内部写与 ASKING 客户端写并发 last-win。两侧同制。一致。
- CLUSTER RESET × 迁移/客户端:槽内有键整体拒绝(ResetWithKeysAssigned),迁移中键在本地槽→必拒,迁移会话不清(两侧同);HARD 清库不问 reset 成败(cluster_session/basic.rs cluster_reset_slow 对 RespClusterBasicCommands.cs:481-496);客户端会话不感知。一致。
- FLUSHALL_NS × 并发:换号+延时 GC,在途写落旧域的丢失窗与 C# 物理删除竞态窗同阶;会话经代数 bump 刷新(vdb 析构竞态已有票,不重复)。一致。

升阶(wbftree)× 迁移叠加
- 带外流写路径:传输窗内树快照为独占锁 + claim 下的 CPR 快照临时文件(range_index_manager_migration.rs snapshot_range_index_and_create_reader),流读快照文件、不触源树,客户端写又被 TRANSMITTING 门封,后台 TTL/紧缩不撕裂快照。一致(rust 自扩展,无 C# 对位,核其自洽)。
- 同键换入封窗(MigrationBusy):KEYS 路径本轮跳过、槽属主未动、下轮重扫(range_index_manager_migration.rs get_range_index_keys_for_migration);SLOTS 路径快照仍封窗即显式判败 recover(migrate_driver/slots.rs :257),无静默丢键。一致。
- 删除臂覆盖面:delete_string 同步快路径失败降级完整异步删(wnode/src/storage/session/storage_session.rs :553),Meta 元记录+树文件排空闭环,升阶键迁后源端不留幽灵。一致。

CLIENT 域
- CLIENT PAUSE/UNPAUSE:两侧均无实现。一致。
- CLIENT KILL × 门等待/阻塞挂起:杀会话即弃挂起体;对阻塞挂起无效力已立(r4-client),不重复。沿用既立。

三、无增量确认

以上「一致」格均为本轮实码核对:rust 侧函数级判定链、C# 侧对应函数逐格对照(SlotVerify 三文件、MigrateSessionKeyAccess.cs、Sketch.cs、ClusterConfig.cs IsLocal、ClusterManager.cs TryStopWrites、ClusterManagerWorkerState.cs TryReset、BasicCommands.cs FLUSH 门、TxnKeyManager.cs、RespCommandsInfo.json 键规格、RespCommand.cs 区间),非凭记忆;既立票(迁移锚死默认域、attach 双泵、vdb 析构、迁移半失败 +OK、resync 错位、KILL 挂起)仅确认未复述。

视角结论:有增量
