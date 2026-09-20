轮9 压测者视角:并发负载运行态推演(zcode-r9-load)

方法
与 r3-perf(静态拷贝/写放大)互补,本轮只推演并发负载下的动态行为,只立会出现在压测曲线上的真串行点/饱和点;理论争用不立。已立发现不重复:r3-perf 静态拷贝、r4-observe 统计口径、r6-mem 记账/预算、r2-concurrency 锁序、r5-repl enqueue_reserved 次序与 attach 双泵竞态。

运行时基线
thread-per-core:每核一线程一 compio runtime,SO_REUSEPORT 分流连接(wedb/wnode/src/server.rs start_tcp_workers);每连接一个泵任务 + 一个 RespServerSession + 一个 StoreSession;全部连接共享单 WedbStore(HashIndex + whlog 环形日志)与单物理 AOF。纪元批入:每命令一次 enter_batch 进入 LightEpoch(wedb/wkv/src/session/mod.rs:529),读路径无锁、写路径键级桶闩。

场景1 高并发点查(GET/SET 小值,100 连接流水线)

瓶颈序列(按先爆顺序):
1. 并发连接数硬上限(先爆)。每连接 new_session 向 LightEpoch 登记一个 Participant 槽位(wedb/wkv/src/store/mod.rs:488 epoch.register);槽表容量 max_sessions = clamp(核数x16 升 2 幂, 128, 1024)(wedb/wkv/src/config.rs:326-329)。槽尽 → new_session Err → get_session None → 握手即 ConnectionRefused 断连(wedb/wnode/src/service.rs:1709, wedb/wnode/src/net/handler/drive.rs:120-124)。8 核机 128 槽再扣内部会话(GC/重放/检查点/节点服务),约 120+ 并发连接封顶;压测曲线表现为「连接被拒」而非延迟上升。
2. 点查读放大(常数因子,非串行点)。GET 命中 = 3 次哈希探针:String 存在探针 + TTL 记录读 + String 二次读执行闭包(wedb/wnode/src/storage/session/common/ttl_sync.rs read_adjudicated_tag_sync_with_prefix 的「先探后裁再二次读」);GET 缺失 = String/信封/Meta 三域各一探针(read_adjudicated_user_sync_with_prefix)。全部为无锁原子读,随核扩展,无串行化点;单核吞吐常数因子劣于 C# 约 2-3 倍。
3. 写路径全局原子点。hlog tail CAS(wedb/whlog/src/hlog/append.rs append) + AOF tail CAS(wedb/waof/src/wal/pipeline.rs reserve_address) + WatchVersionMap fetch_add(wedb/wtxn/src/watch_version_map.rs) + WAL 在途槽(线程亲和零竞争,acquire_inflight_slot)。每写约 7-8 次共享原子 RMW,串行上限在数百万 ops/s 量级,先于它爆的是核间 cacheline 往返,不是锁。
4. 会话输出缓冲与指标全在连接本地:output Vec、每连接一份 SessionMetricsHandle 原子计数(wedb/wnode/src/service.rs:1729-1752),command_stats 每会话一把 Mutex 仅采样轮加锁(wedb/wnode/src/resp/resp_server_session/core.rs:142)。无跨连接争用。

C# 同场景对照:连接不占纪元槽(LightEpoch 槽位 per-thread TLS,kTableSize = max(128, ProcessorCount*2),garnet/libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:100);UnifiedStore 点查单记录单次 Read 无三域探测(libs/server/Storage/Session/UnifiedStore);写侧同为 tail 分配原子 CAS + 索引 CAS + watchVersionMap(garnet AllocatorBase.TryAllocate fetch_add)。

判定:串行点上无明显差异;连接上限一条可能先爆(C# 无此面);点查吞吐常数因子可压过 C# 的多核扩展性但单核效率不及。

场景2 大流水线混合读改写(同键高竞争 INCR/APPEND)

瓶颈序列(按先爆顺序):
1. AOF 环满报错风暴(先爆,真雷)。AOF 入队失败(BufferFull,wedb/waof/src/wal/pipeline.rs reserve_address 扇区窗容不下即拒)发生在 hlog 记录已写入之后:原位改写臂 try_update_in_place 成功后 notify_write_listener 以 `?` 上抛(wedb/wkv/src/session/raw/write/inplace.rs:209-211)→ 命令回 ERR 但原位写已生效(幻影写);INCR 同型(wedb/wkv/src/session/raw/write/rmw.rs try_rmw_sync → try_upsert_raw_sync_unprotected),客户端重试即双重递增;对象臂更直接吞错仅 log::error(wedb/wnode/src/resp/objects/rmw_helpers.rs:793),AOF 缺 ObjectStoreRMW 镜像,主从静默发散。触发条件:aof-memory 默认 64MB 环窗(wedb/wconf/src/runtime_server_options.rs aof_memory_size 64m),committer 单协程 fsync 滞后(检查点 flush_all / 紧缩 I/O 抢占同盘)即窗满。
2. 同键桶闩自旋预算放大。try_rmw_window 同步 1024 次 CAS 自旋,失败降级异步臂再 1024 轮让核重试(wedb/wkv/src/session/rmw_window.rs RMW_LATCH_SPIN_ATTEMPTS/RMW_LATCH_YIELD_BUDGET)。热键下每个竞争命令在 reactor 线程内联烧 10-20us 纯自旋;RMW 窗口持闩期仅微秒级(读-算-写纯内存),此项为中度放大,真正打空预算的是场景3 的毫秒级持闩。
3. 热点键不闩级拖垮整库。每键独立哈希桶闩,热键串行不阻塞他键;全局点仅两个 tail CAS(场景1.3),压测曲线形态 = 热键 ops 封顶 + 所在核延迟抬升,无全库停摆点。
4. 组提交聚合窗行为良好。auto_commit(默认 aof-commit-freq=0 逐操作自动提交,与 C# 默认一致)每写发容量1折叠信号 → 单 committer 协程 commit_to(wedb/wnode/src/aof/waof_sublog.rs committer_loop + wedb/waof/src/wal/flush.rs GroupCommitPipeline leader 级联/ follower 合并);WAIT 客户端并发汇入同一流水线,合批零重复 fsync。

C# 同场景对照:AOF 分配失败走 flushEvent.Wait() 阻塞等待页回收,网络线程挂起出延迟毛刺,无错误无发散(garnet/libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:1424-1455 AllocateBlock);ephemeral 闩单次尝试失败即 RETRY_LATER 转pending重试(libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRMW.cs);组提交 TryEnqueueCommitRecord + CommitTaskAsync 同型。

判定:第1条可能先爆(慢盘/检查点窗口下 wedb 出客户端错误+主从发散,C# 只出延迟毛刺);其余无明显差异。

场景3 大集合压测(十万成员 hash/zset 高频 HDEL/HRANDFIELD/ZADD 混合)

瓶颈序列(按先爆顺序):
1. 整对象互斥 + 闩内全量序列化(先爆,数量级差距)。run_sync_rmw 持本键桶排他闩跨「from_blob 全量反序列化 10 万成员 → operate 改一字段 → to_blob 全量序列化 → 整值信封回写」全程(wedb/wnode/src/resp/objects/rmw_helpers.rs:739-801)。单命令 CPU 成本 O(整对象体积)而非 O(字段),MB 级信封持闩毫秒级 → 同键并发整键串行且闩自旋预算持续打空(叠加场景2.2,竞争者空转烧核殃及同核他租户)。读命令同型:HGET/HRANDFIELD 也走 obj_load_typed_sync 全量反序列化(wedb/wnode/src/resp/objects/object_store_utils.rs:778)。r3-perf 已立静态拷贝数,本条立负载形态:闩持有时长随对象体积线性放大,同键大集合吞吐塌方至每秒数百 ops。
2. 升阶触发的行为切换尖峰。should_promote(条目数>=65536 或 heap>=4MB,wedb/wcol/src/types/garnet_object.rs:67)命中即同步臂回 Degrade(rmw_helpers.rs:758),转异步漏斗 promote_collection_to_bftree:导出全量条目 + 树内重建 + 原子换入(wedb/wnode/src/resp/objects/tiered_collection_ops/common.rs:288-350),期间该键全部操作在闩/降级通道排队,切换瞬间单键延迟长尖峰;65536/32768 双门限迟滞防反复升降,切换后稳态走树内原生臂 O(字段)。C# 无此分层,无对应面。
3. OBJECT_DELTA 追加日志:当前代码无此形态(全仓 grep 无 OBJECT_DELTA 实现,SKILL.md 所述与 HEAD 不符),集合增量以 AOF ObjectStoreRMW 命令镜像逐条承载(wedb/wnode/src/service.rs on_aof_store_event TieredCollectionWrite 臂),条目 O(命令) 不随对象体积放大,AOF 侧无增量日志增长问题。该子项不存在,不立。

C# 同场景对照:C# 对 logRecord.ValueObject(活 IGarnetObject 引用)在记录锁内原位 Operate,零反序列化/序列化,单操作 O(字段)(garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:119, Unsafe.As<IGarnetObject>(logRecord.ValueObject).Operate);序列化仅发生在 checkpoint(GarnetObjectSerializer)。

判定:第1条可能先爆(对 C# 数量级差距,压测曲线最先塌的就是大对象热点键);升阶尖峰为 wedb 特有;其余无明显差异。

场景4 复制压测(主库写满带宽 + 两副本慢拉)

瓶颈序列(按先爆顺序):
1. 单泵串行扇出(先爆)。pump_backlog 单协程顺序遍历全部 driver,逐记录 scan + reconstruct_frame(每帧一次 Vec 分配,wedb/waof/src/wal/record.rs:52)+ consume(wedb/wedb/src/server/replication/aof_replication_pump.rs:227-293);慢副本的补扫(GB 级盘段拉取 + 逐帧重建)独占泵协程,快副本的增量推流被推迟到下一波唤醒(容量1折叠信号)。两副本推流吞吐/延迟被最慢副本支配。C# 每副本独立 RunAsync 常驻任务并行推流(garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs)。
2. 溢流断连与回拉。副本拉不动 → wire 溢流队列按 10000 条 + byte_cap(产线 MAX_UNFLUSHED_SEND_BYTES = 4x(2<<24) = 128MiB,wedb/wconn/src/types.rs:32)双闸,超限报错断驱动(wedb/wedb/src/server/replication/replica_wire.rs:380-393)→ 副本重连走位点协商;主库数据面不阻塞(WalScanIterator 透明跨内存环窗与磁盘段)。注意断连驱动出册即解除 AOF 截断线钉制(aof_replication_pump.rs try_remove_current),重连位点若已被 checkpoint 截断则升级全量重灌,压测里表现为「慢副本周期性掉线-全量重灌」振荡。
3. 背压闸门。默认 aof-sync-max-lag-bytes = -1 禁用(与 C# 默认同,wedb/wconf/src/runtime_server_options.rs:107)。若启用,append 阻塞面 backpressure.wait 为同步 listener.wait()(wedb/wnode/src/aof/garnet_log/single_log_branch.rs:21-27 → wedb/wnode/src/aof/aof_backpressure.rs wait_slow),阻塞的是 compio reactor 线程,同核全部连接停摆;C# 同步 wait 阻塞的是该 shard 网络线程,爆炸半径等同。
4. 组提交与推流互锁:无。推流读 safe_tail_address(内存窗完成即可读),不等 commit/fsync;WAIT 等待与 checkpoint 截断在 commit_lock 上串行为标准形态。

C# 同场景对照:AofSyncTask.Consume/Throttle 每副本独立任务 + NetworkWriter 发送缓冲顶(garnet libs/server/AOF/AofBackpressure.cs 同名机制,默认同为禁用);无单泵共享面。

判定:第1条可能先爆(多副本负载形态下与 C# 的结构性差异);其余无明显差异。

场景5 多租户混合(数十 ns,每 ns 数库,命令混杂)

瓶颈序列(按先爆顺序):
1. 全租户单 fsync 域(先爆,wedb 特有)。全部 ns/db 共享单物理 AOF(wedb/wnode/src/aof/waof_sublog.rs single_log_aof 装配 vec![backend],boot 强制 aof_physical_sublog_count==1):单 commit_lock + 单 GroupCommitPipeline + 单 committer 协程。任一热租户写满磁盘 fsync 带宽,全部租户的 WAIT 延迟与提交水位同被 pace;场景2.1 的 AOF 环满错误面波及全租户。C# 对位:非默认库无 AOF/不持久化,不存在跨租户共享 fsync 面——这是 wedb 能力扩展同时引入的跨租户串行点。
2. GC/过期清除跨租户干扰(轻)。过期清除热区 [read_only, tail) 每轮无游标全量内存扫(wedb/wkv/src/gc/ttl_sweep.rs:125-134),冷区游标 + max_scan_records 预算;单 GC 会话占 1 个纪元槽在共享 runtime 上跑,扫描成本正比于可变窗字节,热写租户撑大窗口摊薄全租户核时间。无停世界点:删除走键级闩 RMW,FLUSH 换号 O(1)(r2-crash/r6 已覆盖,不重复)。
3. 会话前缀与路由零争用。virtual_domain 快路径 3 次 Relaxed 原子读,代数落后才走 papaya 慢路径重解析(wedb/wkv/src/session/mod.rs:346-362);SessionPrefixBuf 栈上 19 字节 const 编码(wedb/wval/src/ns_codec.rs:44);每命令 ACL 门为代数比较 + 连接本地位图(wedb/wnode/src/resp/admin_commands.rs:86-99),改权收敛慢路径仅代数落后触发。跨租户无共享锁。

C# 同场景对照:C# network_string_get 单记录读无前缀重算;LightEpoch 槽位 per-thread 不随连接涨;ExpiredKeyDeletionScan 走哈希索引迭代会话(成本正比索引规模而非日志窗);无 ns 多租户共享持久化面。

判定:第1条可能先爆(跨租户 fsync 互 pace);其余无明显差异。

汇总(压测先爆序,全视角)
1. 场景3.1 大对象热点键:闩内全量序列化,同键吞吐塌方 + 自旋烧核(对 C# 数量级差距)。
2. 场景2.1 AOF 环满:已生效写被报错/静默缺账,慢盘或检查点窗口触发,主从发散。
3. 场景1.1 连接上限:纪元槽 = 并发连接数,压测超限即拒连。
4. 场景4.1 复制单泵串行扇出:多副本被最慢副本支配。
5. 场景5.1 单 fsync 域:热租户 pace 全部租户。

视角结论:有增量
