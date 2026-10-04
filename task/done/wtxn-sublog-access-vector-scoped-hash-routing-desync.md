终态注记: 已合入 main（commit: fa125f3）。收口形态：TxnKeyEntry 结构体增存 routing_hash（裸键路由哈希 whasher::fast_hash_i64(key)），register_run_preamble 现算存入；compute_sublog_access_vector 直读 routing_hashes() 展开位图与计算 participant_count，与 AOF 数据条目裸键路由槽位恒对齐；scoped_key_hash 文档注释补锁轨专用注记；增加多子日志拓扑回归测试。

甄别结论:通过(P2,2026-09-30 现码复核,双侧锚亲验成立,五池无重复,deviations 无同面在册裁决)

审核结论：通过（2026-09-30 审核席）。两域分叉逐行确证：key_entries 仅两填充点（register_run_preamble transaction_manager.rs:337 与 WATCH 并锁 txn_watched_keys_container.rs:116）均经 scoped_key_hash（txn_key_entry_comparison.rs:46-48 = fast_hash_with_seed(key, fast_hash(prefix))，whasher lib.rs:398-401 白名单亦明列 scoped_hash 合法消费面不含 AOF 路由）；AOF 写侧 GarnetLog::hash = fast_hash_i64（addresses.rs:26-28，数据条目 enqueue_with_header :76、分块 :165-172/:209）与读侧 entry_routing_hash（record_gate.rs:131-138）恒裸键域；默认拓扑两计数均 1 早退（transaction_manager.rs:484-486、wconf runtime_server_options.rs:36/:39），多子日志拓扑可达。危害表述收窄：两标记共用同一位图、标记持有侧 arrivals 与 participant_count 自洽收敛（栅栏自带超时收敛 aof_replay_coordinator.rs:99-104），「arrived 永达不到悬置」非主形态；主危害为数据/组分家——数据条目落位裸键虚拟子日志不在标记集合内时走 TxnAction::None 即刻裸放（协调器 :400 → 处理器 :512），副本栅栏与组聚合（aof_processor.rs:489-505/:815-817/:843-896）在无数据的标记子日志集上工作，read_consistency 水位坐标（read_consistency_manager.rs:119-127/:430）与标记推进坐标异源。C# 对标成立但票面引注勘误：TransactionalContext.cs:354 不存在，真身链 TxnKeyEntry.cs:82-90（文档注释明言 equal to GarnetLog.HASH）→ Tsavorite.cs:267 GetKeyHash → StoreFunctions.cs:40 GetKeyHashCode64 → GarnetKeyComparer.cs:31-36，与 GarnetLog.cs:92-93 HASH 同为裸键域，C# 无分叉形态。查重：deviations.md 零命中；引入来源票 task/done/wtxn-multi-queued-lock-hash-stale-across-generation.md 优化点 2b 未触及本面；wkv-wkv-keybucket 票系桶域正交。执行着陆修正（覆盖原方案 1）：主案「从 txn_keys 现算裸键哈希」在单机 WATCH 键面失效——add_txn_key 带 !cluster_enabled 早退（transaction_manager.rs:424-429），单机 WATCH 键入 key_entries（save_lock_hashes 无门控）而不入 txn_keys；应直接落兜底案：TxnKeyEntry 增存裸路由哈希字段，register_run_preamble 裸键在手处单次现算 GarnetLog::hash 口径（fast_hash_i64）入表，compute_sublog_access_vector 改直读该字段展开位图，兼顾 C# keyEntries 含 WATCH 键位语义且零额外分配；transaction_manager.rs:494-495 注释按裸键重算口径订正，scoped_key_hash 文档注释补「锁轨槽位专用，非 AOF 路由域哈希」。测试验证点相应改：断言按标记位图与数据条目实际路由子日志集一致设判（组员与数据分家为主害），单日志拓扑回归不回摆。

原票面：
事务围栏向量按 scoped 前缀哈希展开而 AOF 条目按裸键哈希路由，多子日志拓扑 TxnStart/TxnCommit 标记位图与 participant_count 漏覆盖实际承载子日志

问题分析：
1 Garnet 契约对齐（C# 原型行为与协议约定）：C# ComputeSublogAccessVector（libs/server/Transaction/TransactionManager.cs，对标注记见 rust 侧 :472-474「对标 C# 1939 号提交」）从事务 keyEntries 取 keyHash 展开——C# keyHash 经 StoreFunctions.GetKeyHashCode64（TransactionalContext.cs:354）为裸键哈希；C# GarnetLog.HASH 与 GetPhysicalSublogIdx/GetReplayTaskIdx（libs/server/AOF/GarnetLog.cs）同为裸键哈希路由。C# 标记位图与条目路由两源同键同哈希恒对齐，无分叉形态。
2 工程现状确证（Rust 现有实现路径与代码缺陷）：rust 多租户物理前缀改良使事务锁轨哈希携带前缀种子——key_entries 填充点 register_run_preamble（wedb/wtxn/src/transaction_manager.rs:336-339）以 TxnKeyEntryComparison::scoped_key_hash(lock_prefix, key)（wtxn/src/txn_key_entry_comparison.rs:46，fast_hash_with_seed(key, fast_hash(prefix))，前缀为 session_prefix 真值恒含 NsVarint/DbVarint 编码非空）现算入表。compute_sublog_access_vector（transaction_manager.rs:480-516）直接以 key_entries.key_hashes() 的 scoped 哈希模 physical_sublog_count、除模 replay_task_count 展开 physical_vector/virtual_vectors/participant_count，:495 注释自称「键哈希等同于 GarnetLog.HASH，直接使用无需重算哈希」与 :337 的 scoped 构造自相矛盾（同 scoped_key_hash 文档注释 txn_key_entry_comparison.rs:44-49 明言前缀种子域混入）。而 AOF 条目实际路由恒按裸键：写侧 GarnetLog::hash = whasher::fast_hash_i64(key)（wedb/wnode/src/aof/garnet_log/addresses.rs:26-27，enqueue_with_header 镜像），读侧 entry_routing_hash（wedb/wnode/src/aof/record_gate.rs:131-138）非分块取裸键哈希、分块取 chunk.key_hash 同为裸键域；恢复协调与读一致性面大量消费裸键哈希（aof_replaycoordinator、readconsistency/read_consistency_manager.rs:120/:430 等）。两源对同名键异值（前缀种子压倒性概率非零），enqueue_txn_marker（transaction_manager.rs:452-461）把按 scoped 展开的 SublogAccess 位图写入 TxnStart/TxnCommit 头。
3 逻辑危害确证（并发/数据丢失/恢复面实际危害）：触发面为 aof_physical_sublog_count > 1 或 replay_task_count > 1 的配置拓扑（garnet_log/mod.rs:72-74；单日志单任务 compute_sublog_access_vector :484 早退，默认拓扑不可达）。多子日志拓扑下：TxnStart/TxnCommit 头的 participant_count 与 physical_vector/virtual_vectors 按 scoped 哈希展开，组内实际写条目按裸键哈希落位，两 (physical_idx, replay_idx) 大概率不同——恢复期/副本侧协调器按 participant_count 聚合到达数（wedb/wnode/src/aof/replaycoordinator/aof_replay_coordinator.rs:106/:129 arrived >= participant_count）与条目实际路由错位：实际承载子日志不在标记位图内时 arrived 永达不到（事务组悬置滞留），或位图虚增 participant_count 令 unrelated 子日志被计入参与者；read_consistency 的虚拟子日志水位面按裸键哈希坐标与标记位图 scoped 坐标异源错配。非内存态执行面缺陷，系恢复/副本面事务组聚合错位。

涉及代码：
rust 文件与函数：
wedb/wtxn/src/transaction_manager.rs: compute_sublog_access_vector（:494-513 位图展开面）、enqueue_txn_marker（:452-461）、register_run_preamble（:336-339 scoped 哈希构造点）
wedb/wtxn/src/txn_key_entry_comparison.rs: scoped_key_hash（:46，前缀种子构造单点）
wedb/wnode/src/aof/garnet_log/addresses.rs: GarnetLog::hash（:26-27 裸键路由单点）、get_physical_sublog_idx/get_replay_task_idx（:32-41）
wedb/wnode/src/aof/record_gate.rs: entry_routing_hash（:131-138 读侧路由镜像）
wedb/wnode/src/aof/replaycoordinator/aof_replay_coordinator.rs: participant_count 到达聚合（:106/:129/:376-386）

对应 c# 文件与函数：
garnet/libs/server/Transaction/TransactionManager.cs: ComputeSublogAccessVector（keyEntries keyHash 展开）
garnet/libs/server/Storage/TransactionalContext.cs: GetKeyHashCode64（裸键哈希，与 GarnetLog.HASH 同源）
garnet/libs/server/AOF/GarnetLog.cs: HASH、GetPhysicalSublogIdx（裸键路由）

精炼执行方案：
1. 围栏向量侧统一到裸键路由域（AOF 路由域为全链既定契约：写侧 enqueue_with_header、读侧 entry_routing_hash、readconsistency 全按裸键，且 C# 两源同为裸键；严禁反向把 AOF 路由改 scoped）：compute_sublog_access_vector 改从 txn_keys 裸键字节现算 GarnetLog::hash 展开位图（txn_keys 恒持用户键裸字节、含 WATCH 键（watch() 同点 add_txn_key），与 key_entries 键集同域；零存储增量，对标 C# keyEntries 哈希本就是裸键的语义）。若执行席核出 key_entries 存在 txn_keys 之外的键源（现 grep 仅 register_run_preamble 与 WATCH 并锁两填充点、同源于 txn_keys），再议 key_entries 增存裸路由哈希字段方案。
2. transaction_manager.rs:494-495 注释按定裁结果订正（「键哈希等同于 GarnetLog.HASH」的说法在 scoped 构造下不成立，改述裸键重算口径）；scoped_key_hash 文档注释补一句「锁轨槽位专用，非 AOF 路由域哈希」防再误引。
3. 测试验证点：aof_physical_sublog_count>1 拓扑下 MULTI 跨子日志键写事务重启恢复，组聚合到达数等于实际承载子日志数、无组悬置无虚增参与者；副本侧同日志重放组闭合；单日志单任务拓扑回归不回摆；wnode/tests/transaction_manager_tests.rs sharded 用例断言改按裸键路由展开比对。
