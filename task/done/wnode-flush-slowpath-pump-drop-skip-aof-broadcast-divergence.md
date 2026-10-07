终态：合入 ce345a61（worktree 提交，经 089472e6 合并入 dev），FLUSH 族换号-广播整链经 detach_flush_body 单机制必达包裹（spawn detach + oneshot 回传，执行体被泵丢弃只丢收端、段体照常跑完入队），manager 三入口 + 非 0 租户总线广播臂 + CLUSTER FLUSHALL 慢路径全部收口，cluster 臂内联 enqueue 第二套入队删除改经常驻漏斗；整链一体必达使 wkv 编排不再暴露于丢弃面，票面方案 4（wkv 重排/补跑）不再需要、wkv 零改动。新增测试 test_flush_body_survives_executor_drop（锁夹具钉死换号前丢弃 + 轮询钉死换号后丢弃，副本回放判死旧域闭环），cluster_resp_session 装配补 set_database_manager/attach_flush_gate（boot 生产同形）。

甄别结论：通过（定级：高危——主从换号域静默发散，数据正确性主害）

甄别复核记录（双侧现码亲验，行号以现树复核为准）：
1. C# 侧五锚全部成立：BasicCommands.cs ExecuteFlushDb（:1939 附近，sync 臂网络线程内联、async 臂 Task.Run 全程执行不拆解）→ StoreWrapper.cs:648-653/:660-662 → SingleDatabaseManager.cs:365-378（FlushDatabase 换号 → SafeFlushAOF 同一同步序列）→ :425-433（SafeTruncateAOF → EnqueueSafeFlushAOF 先截断后入队同序）；RespClusterReplicationCommands.cs NetworkClusterFlushAll（:653-654 storeWrapper.FlushAllDatabases(unsafeTruncateLog: false) 同步内联）。
2. Rust 侧八锚全部成立：admin_commands.rs:245 route_slow_command 挂 pending_slow；slow/admin.rs:82 flush_command_slow 三臂 + 尾部非 0 租户 FLUSHALL 总线广播 bcast.await；consume.rs:259 附近 probe_race 三路竞速、RaceEnd::Disposed 丢弃执行体（race.rs:104/138/153 多点）；single_database_manager.rs flush_namespace(:548)/broadcast_flush_domain(:576-587 reclaim → safe_flush_aof → 二次 reclaim 多 await 序列)/flush_all_databases(:614-621 同形)；wkv keyspace.rs:576-596 换号编排（lock await × 2 → remount await → shift_begin_address await → range_index_blocking(clear_all) await → vdb.reset/clear_bftree_domains/bump_acl 同步收尾）——shift 之后 clear_all await 让渡点即撕裂面 a；cluster_flush_all_slow（replication.rs:133-148，store.flush_all_databases().await 之后内联 enqueue_safe_flush_aof_if_primary 之前）与 FLUSHALL_NS 接收端（:338-345 dm.flush_namespace 挂 pending_slow）撕裂面 b 成立。try_store 返回 wkv WedbStore（assets.rs:105）确证 cluster 臂现状绕过 manager 漏斗直调物理换号，AOF 入队为第二套内联实现。
3. 非重复确认：姊妹票 wedb-cluster-replicate-sync-pump-drop-recovery-lock-orphan 治 REPLICAOF/CLUSTER REPLICATE 恢复锁滞留（改动点 replication.rs queue_try_replicate_sync 区与 assembly.rs），本票治 FLUSH 族换号/AOF 广播撕裂（改动点 single_database_manager.rs/admin.rs/cluster_flush_all_slow 区），同文件不同函数、判据面不相交，不并案不覆盖。done 池 wedb-cluster-flushall-ns-caller-gate（gossip 身份伪造判据）、whlog-flush-overlapping-page-writes（hlog 页写）、waof-scan-iterator（AOF 扫描）均无交集。deviations.md 不存在（doc/zh 仅 collection.md/db.md）。
4. 架构合规：守卫先例 failover.rs:390-460 EpochDrainGuard（Drop spawn 补跑 + disarm 幂等 + Runtime::try_current 兜底留痕）亲验成立。执行方案见 commit message 论证：以「整链 spawn 必达」单机制收口（比票面分段守卫更贴 C# 网络线程恒完成语义——C# 连换号都无取消面），wkv 编排因此不再暴露于丢弃面，撕裂面 a（编排中段）随之消失，票面方案 4 的重排/补跑不再需要；cluster 臂内联 enqueue 收口进 manager 唯一漏斗（删第二套）。

FLUSH 族慢路径执行体被泵丢弃后换号已生效而 AOF 广播条目未入队，主从换号域静默发散

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# FLUSHDB/FLUSHALL 在网络线程内联整段执行：libs/server/Resp/BasicCommands.cs:1939 → libs/server/StoreWrapper.cs:FlushDatabase(:648-653)/FlushAllDatabases(:660-662) → libs/server/Databases/SingleDatabaseManager.cs:FlushDatabase(:365 SafeFlushAOF(FlushDb))/FlushAllDatabases(:369-378 SafeFlushAOF(FlushAll))，广播条目入队（:425-433 EnqueueSafeFlushAOF）内嵌于换号编排同一同步序列，先截断后入队（SafeTruncateAOF → Enqueue 同序）恒必达。CLUSTER FLUSHALL 总线帧接收端同形（libs/cluster/Session/RespClusterReplicationCommands.cs:654 storeWrapper.FlushAllDatabases(unsafeTruncateLog: false)）。网络线程 BlockingWait 语义下执行序列不可被客户端断连拆解——「本端换号生效」与「Flush 族广播条目入队」是同一不可分单位，副本经 AOF 回放条目收敛，主从换号域一致。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust FLUSH 族慢路径执行体挂 pending_slow（wedb/wnode/src/resp/admin_commands.rs:245 route_slow_command → garnet_api/slow/admin.rs:82 flush_command_slow），网络泵以 probe_race 三路竞速驱动（wedb/wnode/src/net/handler/drive/consume.rs:247-291、race.rs）。终止广播（CLIENT KILL/停机令牌）、对端 EOF、dead-conn 任一胜出即 RaceEnd::Disposed，执行体随竞速败侧丢弃。换号漏斗 SingleDatabaseManager::flush_all_databases（wedb/wnode/src/database/single_database_manager.rs:614-621）与 flush_namespace（:556-571，经 broadcast_flush_domain :576-590 收口）内部为多 await 序列：wkv 物理换号编排（wedb/wkv/src/store/keyspace.rs:576-596 flush_all_databases：lock_dbmeta/lock_acl 异步闸 → remount_truncation_survivors 重挂扫描与回写 await → shift_begin_address await → range_index_blocking(clear_all) await → vdb.reset → clear_bftree_domains）→ reclaim_registry().await → safe_flush_aof（Flush 族广播条目唯一入队点）→ 二次 reclaim。执行体在以下任一 await 点被丢弃即撕裂：
a. wkv 编排中段（shift_begin_address 已过、vdb.reset/clear_bftree_domains 未达）：换号半完成态——日志已截断而虚拟映射与树域未清零，映射仍指旧代、树元数据仍引用已截断日志地址；
b. wkv 换号已整体生效、执行体在 reclaim_registry await 点或 CLUSTER FLUSHALL 臂（wedb/wedb/src/server/cluster_session/replication.rs:133-148 cluster_flush_all_slow：store.flush_all_databases().await 之后 enqueue_safe_flush_aof_if_primary 尚未执行）被丢弃：本端数据已清而 FlushAll/FlushDb/FlushNs 广播条目永不入队。
admin.rs:144-148 非 0 租户 FLUSHALL 总线广播臂同理：本地换号完成后 bcast.await 被丢弃，协调者区已清、其余主节点区不清。
CLUSTER FLUSHALL_NS 接收端（replication.rs:338-345 → dm.flush_namespace）同一撕裂面。

3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
触发面常规：FLUSHALL/FLUSHDB 发起端 pipeline 发完即关、客户端超时拆连、RST、CLIENT KILL、停机排空，任一与换号窗（含重挂扫描，大日志下秒级）交叠即命中。危害 a：本端处于「数据已截断、映射/树域未清」的 limbo——旧命名空间键读取落已截断地址区间返回空/残缺，集合树悬挂引用截断地址，直至下次成功 FLUSH 才收敛。危害 b 为数据正确性主害：主从静默发散——本端已清、副本保留全量数据，复制位点连续无 divergent 判据（记录流照常推进），副本旧键此后被主端命令按缺失语义重写时序列错位，清库事实永不达副本；且主端 AOF 中 Flush 条目缺席使重启恢复的副本同样不清。不可复现原因：丢弃点与秒级换号窗及连接终止事件精确交叠，生产表现为低频滞留态而非即时错误；可用 future drop 注入确定性构造。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/garnet_api/slow/admin.rs:flush_command_slow
wedb/wnode/src/database/single_database_manager.rs:flush_all_databases
wedb/wnode/src/database/single_database_manager.rs:flush_namespace
wedb/wnode/src/database/single_database_manager.rs:broadcast_flush_domain
wedb/wnode/src/database/single_database_manager.rs:safe_flush_aof
wedb/wkv/src/store/keyspace.rs:flush_all_databases
wedb/wkv/src/store/keyspace.rs:flush_namespace
wedb/wedb/src/server/cluster_session/replication.rs:cluster_flush_all_slow
wedb/wedb/src/server/cluster_session/replication.rs:network_cluster_flushall_ns
wedb/wnode/src/net/handler/drive/race.rs:probe_race（RaceEnd::Disposed 丢弃执行体）
wedb/wnode/src/net/handler/drive/consume.rs:NetworkHandler::drive_loop（慢臂竞速）

对应 c# 文件与函数：
garnet/libs/server/Resp/BasicCommands.cs:ExecuteFlushDb（:1939 FlushAllDatabases 调用）
garnet/libs/server/StoreWrapper.cs:FlushDatabase（:648）
garnet/libs/server/StoreWrapper.cs:FlushAllDatabases（:660）
garnet/libs/server/Databases/SingleDatabaseManager.cs:FlushDatabase（:365）
garnet/libs/server/Databases/SingleDatabaseManager.cs:FlushAllDatabases（:369-378）
garnet/libs/server/Databases/SingleDatabaseManager.cs:SafeFlushAOF（:425-433）
garnet/libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterFlushAll（:654）

精炼执行方案：
1. 单机制分段守卫（对标 failover.rs:EpochDrainGuard 与 SyncBatchGuard 的 Drop 取消收口 + disarm 幂等先例）：换号漏斗入口建守卫，判据锚「物理换号是否已生效」（wkv 换号成功返回为唯一翻转点）。
2. 换号生效前丢弃：零登记零残留，守卫退场无操作（现状语义保持）。
3. 换号生效后丢弃：广播条目必达——wkv 换号 Ok 返回点 disarm 并将尾段（reclaim_registry → safe_flush_aof → 二次 reclaim，non-ns 臂另含 enqueue_safe_flush_aof_if_primary）改为 spawn(detach) 独立任务执行、慢路径 future 仅 await 其完成句柄取应答；future 被丢弃时任务照常跑完入队（对标 C# 网络线程恒完成语义）。CLUSTER FLUSHALL 臂与 FLUSHALL_NS 接收端复用同一漏斗单点，不另立第二套补跑机制。
4. wkv 编排中段撕裂（shift 与 vdb.reset/clear_bftree_domains 之间）按最小切口收敛：复核 keyspace.rs:576-596 await 点次序，把唯一含 await 的卸载段（range_index_blocking clear_all）后置至同步收尾段（vdb.reset/clear_bftree_domains/bump_acl_generation 均无 await）之后，令「截断生效」与「映射/树域清零」之间无 await 让渡点（同核 compio 协作调度下即原子段）；若 clear_all 前置存在正确性依赖，则以守卫 Drop 臂 spawn 补跑幂等编排承接（vdb.reset/清域均幂等）。
5. 测试验证点：集成用例——主从装配后对主端注入「FLUSHALL 执行体在 wkv 换号 Ok 返回后、safe_flush_aof 前被 drop」（分段守卫插入点前现状可复现副本残留），断言副本 AOF 收到 FlushAll 条目并完成换号、主从键空间一致；换号前 drop 臂断言零残留零广播；wkv 单测断言编排收尾段无 await 让渡点。
