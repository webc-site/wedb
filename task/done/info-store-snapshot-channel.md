INFO 存储域快照通道：STORE / STOREHASHTABLE / STOREREVIV / PERSISTENCE 与 MEMORY 的 store_* 段填充

结论一句话：SessionInfoSource::databases 恒返回空集，wmetric 已一处定义的存储域段集合全体脱钩失真；主仓 dev 只有零散的存储侧原语（wkv/waof/windex），没有聚合出口、没有会话侧注入通道。收口点必在存储侧（wkv），wnode 侧只承接两条既有 INFO 通道形态（同步句柄 + 慢路径），禁止在 wnode 侧再拼一次字段。

判定基线：主仓 dev 工作树 415e0d0e；next/ 旧文行号已随 dev 位移，判落地只认当前代码 grep。

优先级：功能缺口（对外可观测的 INFO 段失真 + 内存治理不可观测），不涉及死代码或两套架构（存储域未接、无重复实现）；排在 info-keyspace-single-stats-kernel（已合）与 task/done/info-cluster-segments-provider-homing.md（gossip/BPSTATS/CINFO 已并档完成）之后独立开工。

一、现状取证（当前 dev，全部行号已复核）

1. 空实现在会话侧
- wedb/wnode/src/resp/info_provider.rs:5 模块头注释自认「存储域段（STORE / PERSISTENCE）需存储域快照通道，当前返回空集」。
- wedb/wnode/src/resp/info_provider.rs:76-77 SessionInfoSource::databases 恒 Vec::new()。
- wedb/wnode/src/resp/info_provider.rs:233 / :242 / :251 InfoSlowScanSource 已定义并 impl InfoProvider，是「扫描侧数据源 + wmetric 段填充器一处定义」的既成范式，可直接承载本条的转储文本项。
- wedb/wnode/src/resp/info_provider.rs:268-270 第二个 databases() 属 KEYSPACE 源，注释明写「DbSnapshot 其余字段属 STORE 段，本数据源段集合不含 STORE，不输出」；KEYSPACE 单内核统计已由已合分支 info-keyspace-single-stats-kernel 收口，本条不再触及。

2. wmetric 段集合已一处定义、全部消费空集
- 数据契约：wedb/wmetric/src/info/garnet_info_metrics.rs:48 DbSnapshot / :110 ReadCacheSnapshot / :133 AofSnapshot。
- MEMORY 存储项：同文件 :469-477 store_index_size / store_mainlog_memory_size / store_readcache_memory_size / store_heap_memory_target_size / aof_memory_size，现值全由空集算出。
- gc_committed_bytes 等 gc_* 五项（同文件 :464-467）属 .NET 运行时专属，维持 0 常量并保留注释，不在本条扩张。
- STORE / STOREHASHTABLE / STOREREVIV / PERSISTENCE 段的字段表与跳过逻辑（AOF 关闭整段省略）在 garnet_info_metrics.rs 内已具，只等真实填充点。

3. 存储侧原语存在但分散、无聚合出口
- 日志地址：wedb/wkv/src/store/addr.rs:77 tail_address / :91 safe_read_only_address / :103 begin_address；版本：wedb/wkv/src/store/event.rs:185 current_version。
- GC：wedb/wkv/src/store/gc.rs:92 gc_stats → wedb/wkv/src/gc.rs:175 GcStatsSnapshot。
- 哈希索引：wedb/wkv/src/store/resize.rs:119 active_index → wedb/windex/src/overflow_pool.rs:209 allocated_count / :149 free_count；wedb/windex/src/table.rs 现只有 :42 pub size，next 旧引的 table.rs:614 overflow_bucket_count 在当前 dev 已不存在（旧引失效，落地以真实出口为准）。
- AOF：wedb/waof/src/wal/log.rs:675 begin_address / :681 tail_address / :687 flushed_until_address / :693 committed_until_address，AofSnapshot 六字段全部有真实来源，属纯接线。
- 库枚举：wedb/wnode/src/database/single_database_manager.rs:159 get_databases_snapshot（trait 声明在 wedb/wnode/src/database/i_database_manager.rs:111）已在；但 wedb/wnode/src/database/garnet_database.rs 的 GarnetDatabase<D> 只有 :49 new / :71 update_last_save / :79 last_save_ms / :88 aof_size 四个成员，不携带任何 store 统计，快照面等于不存在。

4. 确无来源、必须新增或显式判定不适用的字段
- 日志页数与实时内存（log_page_size_bytes / log_max_allocated_page_count / log_allocated_page_count / log_memory_size_bytes / log_heap_size_bytes / mainlog_target_size）：wedb/whlog/src 全量 grep 无 stats / memory_size / allocated_page / target_size / SizeTracker 出口，需在既有刷页/紧缩单点回写原子计数，禁新增扫树。
- 读缓存运行时页数与内存（ReadCacheSnapshot 全字段）：wedb/wkv/src/read_cache/mod.rs:64 struct ReadCache 只有配置侧页数，无运行时计数。
- 哈希分布转储（hash_distribution_dump，STOREHASHTABLE 段唯一内容）与复活统计转储（revivification_dump，STOREREVIV 段唯一内容）：windex / wkv 完全没有对应诊断出口（grep 无 distribution / reviv stats）。SKILL:78 严禁占位与虚设实现，不得用空串或 "Empty" 冒充。

5. 集成测试基座已在：wedb/wnode/tests/resp_info.rs。

二、C# 锚点（全部已复核）
- 快照入口：garnet/libs/server/StoreWrapper.cs:567 GetDatabasesSnapshot → libs/server/Databases/DatabaseManagerBase.cs / SingleDatabaseManager.cs 分派。
- 段填充：garnet/libs/server/Metrics/Info/GarnetInfoMetrics.cs:298 GetDatabaseStoreStats（CurrentVersion / IndexSize / IndexBucketCount / IndexMemorySizeBytes / ReadCache.* 直读 db.Store）；:332 PopulateStoreHashDistribution（db.Store.HashDistributionStats）；:343 PopulateStoreRevivInfo（RevivificationStats）；:354 PopulatePersistenceInfo；:366 GetDatabasePersistenceStats（直读 db.Store 与 appendOnlyFile）。
- 消费点：GarnetInfoMetrics.cs:87 / :288 / :334 / :345 / :356 均以 storeWrapper.GetDatabasesSnapshot() 为唯一入口，无第二处组装。

三、方案（一处新增 + 两条既有通道分派）

1. 存储侧聚合出口（wkv，唯一新增数据契约）
在 wedb/wkv 新增 StoreSnapshot 非泛型值结构（建议 wedb/wkv/src/store/stats.rs，与 addr.rs / event.rs / gc.rs 同级），把已有原语一次收成一份快照：日志地址三件套（tail/safe_read_only/begin）+ current_version + 索引桶与溢出桶计数（resize.active_index 与 overflow_pool.allocated_count/free_count）+ GC 计数（store/gc.rs:92 → gc.rs:175）。同一模块内新增最小统计面：
- whlog：页数 / 已分配页 / 实时内存 / 目标内存四组原子计数（AtomicU64），在既有刷页与紧缩点回写，禁止为统计新增扫描。
- read_cache：运行时页数 / 内存字节两组原子计数，在既有分配/释放点回写。
- 转储文本项按「四」的决策二选一，不在本节强推。

2. 快照通道按「是否需要跨 await」二分，复用既有 INFO 架构，禁止 wnode 第二处字段拼装
- 纯标量项（STORE 段、MEMORY 的 store_* 五项、PERSISTENCE 段）走同步面：新增类型擦除的存储句柄，形态沿用 wedb/wnode/src/cluster_provider.rs:303-466 ClusterProviderHandle 的静态虚表范式（RespServerSession 非泛型，不能直接持 Arc<WedbStore<D>>）。虚表出口只返回第 1 步的 StoreSnapshot；GarnetDatabase<D> 增补 store 统计的取数成员（不新增字段拼装路径），SessionInfoSource::databases 由单一入口按库枚举填充 DbSnapshot。
- 转储文本项（STOREHASHTABLE / STOREREVIV）走既有慢路径：把 wedb/wnode/src/resp/resp_server_session.rs:1714-1721 slow_scan_only 段名集合扩到这两段，在 garnet_api/slow.rs 的 Info 臂内一并采集，由 info_provider.rs:233-331 InfoSlowScanSource 承载；其 databases() 改为从扫描上下文带出的快照填充，不再新增第二个 INFO 数据源实现。

3. 完成后改写 info_provider.rs:3-6 模块头注释，删除「当前返回空集」的过期自述，避免留下第二处失真口径。

四、STOREHASHTABLE / STOREREVIV 的显式决策（不得留空段）
windex 分桶结构与 C# Tsavorite 固定桶 + 溢出链不同构，分布直方图口径无意义；reviv 复活统计在 rust 由 wkv/wcol 混合分层架构另行处理，与 C# RevivificationStats 不同构。二选一：
- 按 rust 索引真实结构给出等价可解读转储：桶占用直方图 + 溢出链长度分布（对应 overflow_pool 的 allocated/free 与桶占用），reviv 走 wcol 侧真实回收计数；
- 或经修订判定该诊断函数在 rust 不适用，在 js/check/ignore/garnet/libs/server/Metrics/Info/GarnetInfoMetrics.yml 登记（函数名: 为什么无需对位），并从 wmetric 的 InfoMetricsType 集合与 RESP 段名映射中删除 STOREHASHTABLE / STOREREVIV 两段，保持「缺就是缺、不输出空段」的单一口径。
两种结局都要求「段内容非空」或「段与 C# 函数一并删除」二者必居其一；不允许出现恒为 "Empty" 的新分支，也不允许仅在 info_provider 内写占位串绕过。

五、硬约束（不做）
- 不为通过门禁新增 wnode 侧第二处 DbSnapshot 组装点；wnode 只负责句柄注入与通道分派。
- 不为 STATS 引入扫树/全索引扫描（doc/zh/collection.md 的 O(1) 计数规约同样适用于统计上报）。
- 不动 gc_* 五项（.NET 运行时专属，维持 0 与注释）。
- 不动 KEYSPACE 段（已合）；不动 gossip / BPSTATS / CINFO 段（已在 task/done/info-cluster-segments-provider-homing.md 归档）；本条只做存储域。
- 不加 allow、不引 unsafe；对 ClusterProviderHandle 型静态虚表范式的复用不视为泛型化。

六、门禁
- ./fork.sh info-store-snapshot-channel 基于最新 dev（本方案行号取自主仓工作树，动工前重读当前 dev 复核位移）。
- ./sh/clippy.sh 零警告、禁 allow；./test.sh 全绿（失败隔离重跑 3 次排 flake）。
- bun js/check.js：缺失 0 / 重复 0；若走「四、决策」的 ignore 路径，须同步删除 wmetric 的段定义与 RESP 段名映射，不能只登记 ignore。
- 硬指标 grep 归零：rg "Vec::new\(\)" wedb/wnode/src/resp/info_provider.rs 存储段无空实现；rg "store_index_size\s*=\s*0|store_mainlog_memory_size\s*=\s*0" wedb/wmetric/src 无 0 常量占位。
- 一处定义断言：DbSnapshot 填充点全仓唯一；grep 证明 wnode 内不存在第二处 log_tail_address / index_bucket_count / current_version 组装。

七、集成测试验收（基座 wedb/wnode/tests/resp_info.rs）
- 写入确定量键值后，INFO memory 的 store_mainlog_memory_size / store_index_size 等于存储侧快照聚合值的同一算式（禁止测试内重算）；
- INFO store 0 的 CurrentVersion 随写入单调推进、IndexBucketCount 与 wkv resize 后 active_index + overflow_pool 实测一致；
- INFO persistence 在 AOF 开启时六个地址字段与 waof log.rs:675-693 四出口逐一对齐，AOF 关闭时整段省略；
- STOREHASHTABLE / STOREREVIV：段内容非空且为「四」选定的真实转储，或段已从 InfoMetricsType 删除并在 ignore 登记——两者必居其一，不出现 "Empty" 新分支。

八、串行依赖
- 无前置。已合入的 info-keyspace-single-stats-kernel 已把 KEYSPACE 单内核统计收口，本条不与之并发改同一 databases() 段。
- 同文件撞车：本条改动 info_provider.rs 与 resp_server_session.rs 的段名集合，若与 task/done/info-cluster-segments-provider-homing.md 的后续修订（gossip/BPSTATS/CINFO 段回填）并存，须各自重读行号并串接开工，避免同时改 databases()。

## 九、落地细化（f20-info-snapshot；对照当前 dev 复核，行号已校正）

复核修正（相对上文取证基线）：
- ClusterProviderHandle 已演进为 `Arc<dyn ClusterProvider>` trait 对象（wnode/src/cluster_provider.rs:285），GarnetApi 同为 `Arc<dyn GarnetApiFace>`（wnode/src/resp/garnet_api/mod.rs:133）。同步通道直接扩 GarnetApiFace（复用既有注入通道，零新句柄类型，RespServerSession.garnet_api 已在）。
- whlog 为常驻整页分配模型（whlog/src/config.rs:9 注释自认；CircularPageBuffer::new 一次性分配整环）→ 票据三.1 的「四组原子计数」降级为静态直读：allocated == max == num_pages、memory == num_pages×page_size。ReadCache 同构（capacity 静态、is_enabled 门控）。无需新增任何原子计数回写点，也不扫树。
- wreviv FreeRecordPool 有 put/take/hit/drop 四 pub 原子计数（RevivStats，wreviv/src/pool.rs:49）→ STOREREVIV 走「四」选项 1：真实回收计数转储。
- STOREHASHTABLE 走选项 1：桶占用直方图 + 溢出链长分布（windex 真实结构等价转储，纯内存原子读，显式请求才触发、慢路径执行域承接）。
- waof AofSublog.committed_begin 私有原子已有（wnode/src/aof/waof_sublog.rs:56），仅缺 getter。
- wkv 无 last_checkpointed_version 独立出口：current_version 语义即「最近 checkpoint 推进版本」（store/event.rs:116-119 注释），两字段同源直读；无 Restoring 中间态 → system_state 恒 Running（真实恒等态，注释说明，非占位）。
- InfoMetricsType::from_name 已解析 STOREHASHTABLE/STOREREVIV（wresp/src/metrics/info_metrics_type.rs:130），无需动 wresp。

改动清单：
1. wkv/src/store/stats.rs（新）：StoreSnapshot（非泛型纯数据）+ WedbStore::store_snapshot() 单点聚合（纯原子/静态读，无 epoch、无扫描）；WedbStore::hash_distribution_dump()（桶占用直方图+溢出链分布）；WedbStore::revivification_dump()（四计数+命中率，O(1)）。
2. wnode waof_sublog.rs：补 committed_begin_address() getter（真源直读）。
3. GarnetApiFace::store_snapshots() 默认空集（嵌入式/测试桩形态，与 C# GetDatabasesSnapshot 空数组同构）；StoreGarnetApi<D> 覆盖：经 checkpoint.database_manager db0 → wmetric::DbSnapshot 全仓唯一存储标量组装点（含 AofSnapshot 六字段）。
4. SessionInfoSource::databases() 单一转调 garnet_api.store_snapshots()；改写 info_provider.rs 模块头注释（删「当前返回空集」自述）。
5. resp_server_session.rs slow_scan_only 段集合扩 StoreHashtable|StoreReviv；slow.rs Info 臂采集两 dump（self.session.store() 可达）；InfoSlowScanSource 扩两转储字段，其 databases() 只填 dump 文本字段不碰存储标量（标量组装唯一性保持）。
6. wmetric 零改动（populate_* 与字段表已就绪）。
7. 集成测试基座 wedb/wnode/tests/resp_info.rs 增补（本流水线仅 cargo check，运行验证留主会话）。
