拒绝结论：判净（核对 C# TxnKeyManager.cs 与 RespServerSession.cs，全写面均收口版本推进，逻辑域与物理锁轨分离，TOCTOU 经 shared 锁先行消除，全仓 6 套回归测试闭环，无缺陷无脱节）

WATCH 观察者版本栅栏全路径推进闭环审查报告

一、审查视角与背景说明
审查视角：WATCH 观察者版本栅栏全路径推进闭环（核查键变更全路径是否严格推进版本防事务脱节）
核查目标与范围：
1. 内存直写快路径（BatchStoreSession）、异步慢路径（StorageSession）、RMW 读改写与对象回写全路径。
2. 集合类型自适应分层（BfTree 分层树直写与降阶回信封）、RangeIndex、自定义对象（JSON/Roaring）写路径。
3. 键级生命周期（DEL/UNLINK/RENAME/RESTORE/GETDEL/GETEX）、TTL（EXPIRE/PERSIST/活跃到期清退/GC）。
4. 多租户与虚拟库换号（FLUSHDB/FLUSHNS/SWAPDB/SELECT 切库）版本轨与锁轨双轨域分离闭环。
5. 向量存储（VectorManager）与 AOF 回放（aof_processor_store_ops）写面推进。
6. 并发控制与 TOCTOU 竞态防御（Shared 锁窗保护与版本校验原子时序）。
7. 对标 C# Garnet TransactionManager、WatchVersionMap、TxnKeyManager 与 MainStore/ObjectStore/UnifiedStore 写钩子。
8. 核验 doc/zh/deviations.md 既有在册条款，杜绝将既定架构改良报为缺陷。

二、原型行为与对标核查（C# Garnet 事实确证）
1. C# 官方契约与源码现状
对应 c# 文件与函数：
garnet/libs/server/Transaction/WatchVersionMap.cs:WatchVersionMap.IncrementVersion
garnet/libs/server/Transaction/WatchVersionMap.cs:WatchVersionMap.ReadVersion
garnet/libs/server/Transaction/WatchVersionMap.cs:WatchVersionMap.ValidateVersion
garnet/libs/server/Transaction/TransactionManager.cs:TransactionManager.Watch
garnet/libs/server/Transaction/TransactionManager.cs:TransactionManager.Run
garnet/libs/server/Transaction/TransactionManager.cs:TransactionManager.ValidateWatchVersion
garnet/libs/server/Transaction/TxnKeyManager.cs:TxnKeyManager.AddWatch
garnet/libs/server/Transaction/TxnKeyManager.cs:TxnKeyManager.ValidateWatchVersion
garnet/libs/server/Storage/Functions/MainStore/UpsertMethods.cs:UpsertMethods.SingleWriter
garnet/libs/server/Storage/Functions/MainStore/UpsertMethods.cs:UpsertMethods.ConcurrentWriter
garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:RMWMethods.PostInitialUpdater
garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:RMWMethods.InPlaceUpdater
garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:RMWMethods.PostCopyUpdater
garnet/libs/server/Storage/Functions/MainStore/DeleteMethods.cs:DeleteMethods.SingleDeleter
garnet/libs/server/Storage/Functions/MainStore/DeleteMethods.cs:DeleteMethods.ConcurrentDeleter
garnet/libs/server/Storage/Functions/MainStore/DeleteMethods.cs:DeleteMethods.InitialDeleter
garnet/libs/server/Storage/Functions/ObjectStore/UpsertMethods.cs:UpsertMethods.SingleWriter
garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:RMWMethods.InPlaceUpdater
garnet/libs/server/Storage/Functions/ObjectStore/DeleteMethods.cs:DeleteMethods.SingleDeleter
garnet/libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs:RMWMethods.InPlaceUpdater
garnet/libs/server/Storage/Functions/UnifiedStore/UnifiedStoreOps.cs:UnifiedStoreOps.Rename
garnet/libs/server/Storage/Session/DatabaseManagerBase.cs:DatabaseManagerBase.FlushDatabase
garnet/libs/server/Resp/RespServerSession.cs:RespServerSession.SwitchActiveDatabaseSession

核查确证事实：
1) 写路径全量挂钩无条件推进：
在 C# Garnet 中，Tsavorite 存储引擎的 UpsertMethods（SingleWriter、ConcurrentWriter）、RMWMethods（PostInitialUpdater、InPlaceUpdater、PostCopyUpdater）、DeleteMethods（SingleDeleter、ConcurrentDeleter、InitialDeleter）在写操作成功或删除记录时，统一调用 functionsState.watchVersionMap.IncrementVersion(keyHash)。无论记录先前存在与否，新增写入、原位更新、覆写、墓碑删除（InitialDeleter）均无条件递增版本表槽位。
2) 事务校验与加锁时序：
在 Garnet TransactionManager.Run 中，事务执行前首先通过 TxnKeyManager 为全部在途 WATCH 键加 Shared 锁，随后在持有锁的状态下调用 ValidateWatchVersion 比对初始快照版本。若版本不一致，事务直接中止（返回 false，RESP 回显 nil 数组）。
3) 数据库隔离与换库行为：
C# Garnet 每库独持独立的 TransactionManager 与 WatchVersionMap 实例（GarnetDatabase.cs）。当会话执行 SELECT 切换数据库（SwitchActiveDatabaseSession）时，会话整体切换挂载的 TransactionManager，旧库在途 WATCH 容器随旧库管理器作废；执行 FLUSHDB（FlushDatabase）时，清库本身不主动触碰 WatchVersionMap（裸 WATCH 不误杀），但由于 WatchVersionMap 实例随库终身持有，换号后写入同名键必在该库的版本表同一槽位递增，使在途 WATCH 事务精准 abort。

三、工程现状确证（Rust wedb 实现核查）
1. 模块结构与核心实现路径
对应 rust 文件与函数：
wedb/wtxn/src/watch_version_map.rs:WatchVersionMap::new
wedb/wtxn/src/watch_version_map.rs:WatchVersionMap::increment_version
wedb/wtxn/src/watch_version_map.rs:WatchVersionMap::read_version
wedb/wtxn/src/watch_version_map.rs:WatchVersionMap::validate_version
wedb/wtxn/src/txn_watched_keys_container.rs:TxnWatchedKeysContainer::add_watch
wedb/wtxn/src/txn_watched_keys_container.rs:TxnWatchedKeysContainer::validate_watch_version
wedb/wtxn/src/txn_watched_keys_container.rs:TxnWatchedKeysContainer::save_lock_hashes
wedb/wtxn/src/transaction_manager.rs:TransactionManager::watch
wedb/wtxn/src/transaction_manager.rs:TransactionManager::run
wedb/wtxn/src/txn_session.rs:TxnKeyEntryComparison::scoped_key_hash
wedb/wkv/src/session/mod.rs:BatchStoreSession::bump_watch_version
wedb/wkv/src/session/mod.rs:BatchStoreSession::session_logical_prefix
wedb/wkv/src/store/mod.rs:WedbStore::set_watch_hook
wedb/wnode/src/storage/session/storage_session.rs:version_map_watch_hook
wedb/wnode/src/storage/session/storage_session.rs:vector_version_watch_hook
wedb/wnode/src/storage/session/storage_session.rs:StorageSession::upsert_tag
wedb/wnode/src/storage/session/storage_session.rs:StorageSession::delete_tag
wedb/wnode/src/storage/session/storage_session.rs:StorageSession::expire_at_ticks_opt
wedb/wnode/src/storage/session/storage_session.rs:StorageSession::persist_key
wedb/wnode/src/resp/array_commands.rs:RespServerSession::del
wedb/wnode/src/resp/array_commands.rs:RespServerSession::unlink
wedb/wnode/src/resp/array_commands.rs:RespServerSession::mset
wedb/wnode/src/resp/array_commands.rs:RespServerSession::msetnx
wedb/wnode/src/resp/key_admin_commands/keys.rs:RespServerSession::rename
wedb/wnode/src/resp/key_admin_commands/keys.rs:RespServerSession::renamenx
wedb/wnode/src/resp/key_admin_commands/keys.rs:RespServerSession::restore
wedb/wnode/src/resp/key_admin_commands/keys.rs:RespServerSession::getdel
wedb/wnode/src/resp/basic_commands/get.rs:RespServerSession::getex
wedb/wnode/src/resp/objects/tiered_collection_ops/common.rs:finish_tiered_arm
wedb/wnode/src/resp/objects/tiered_collection_ops/tiered_demote.rs:demote_to_envelope_entry
wedb/wnode/src/resp/objects/rmw_helpers.rs:apply_rmw_post_operate
wedb/wnode/src/resp/objects/rmw_helpers.rs:store_dest_cold_common
wedb/wnode/src/resp/objects/rmw_helpers.rs:bftree_drain
wedb/wnode/src/resp/objects/custom_object_commands.rs:RespServerSession::try_custom_object_rmw_sync
wedb/wnode/src/resp/objects/custom_object_commands.rs:RespServerSession::custom_object_rmw_async
wedb/wnode/src/resp/vector/vector_manager.rs:VectorManager::rename_vector_set
wedb/wnode/src/resp/vector/vector_manager.rs:VectorManager::bump_watch
wedb/wkv/src/gc/ttl_sweep.rs:SweepSession::purge_expired
wedb/wnode/src/aof/aof_processor_store_ops.rs:store_upsert
wedb/wnode/src/aof/aof_processor_store_ops.rs:store_delete
wedb/wedb/src/server/cluster_session/migrate.rs:migrate_restore_key

核查确证事实：
1) 单一哈希定义与全局写面收敛：
add_watch 与引擎写钩子统一通过 TxnKeyEntryComparison::scoped_key_hash 计算版本槽哈希。所有物理写入（快路径 BatchStoreSession 的 try_upsert_sync / try_delete_sync、慢路径 StorageSession 的 upsert_tag / delete_tag / expire_at_ticks_opt / persist_key）均通过 WedbStore::watch_hook 派发到 version_map_watch_hook，精准在逻辑域 (lns, ldb) 槽位递增版本。
2) 自适应分层集合与复合对象无死角推进：
集合对象在内存态经 apply_rmw_post_operate 写回信封；升阶至 BfTree 分层树后，树内所有写操作（HSET/HDEL/SADD/SREM/ZADD/ZREM/LPUSH/RPUSH 等）在 finish_tiered_arm 统一定点推进版本栅栏；删空时 bftree_drain 联动清理物理节点与随键元数据，版本恰一次推进；降阶时 demote_to_envelope_entry 经由用户键写入口推进，杜绝双计与遗漏。
3) 特殊键操作与边界命令覆盖：
DEL、UNLINK、MSET、MSETNX 直接复用底层存储删除与写入原语；RENAME 与 RENAMENX 分别推进源键与目标键版本（同名 RENAME k k 按 C# 语义原位短路返回）；GETDEL 在读出后触发 delete_tag 推进版本；GETEX 在设置新 TTL 时调用 expire_at_ticks_opt 推进版本；RESTORE 写入新键推进版本；TTL 后台清理与惰性清退（gc/ttl_sweep.rs）经 delete_tag 推进版本。
4) 向量存储与集群迁移闭环：
VectorManager 经 vector_version_watch_hook 注册，通过 VirtualDatabase::version_domain_of 将向量底层物理前缀转换为逻辑域前缀，写操作与 rename_vector_set 均可靠推进版本；集群槽位迁移（cluster_session/migrate.rs）的写入与恢复命令无条件挂接引擎写钩子。
5) 换号隔离与双轨域分离设计（deviations 第 115 条）：
版本轨以不可变的逻辑域 (lns, ldb) 为哈希种子，锁轨以当前物理域现算落桶。在 FLUSHDB / FLUSHNS / SWAPDB 换代后，同逻辑键的写操作依然命中同一版本槽，使换号前的在途 WATCH 事务可靠中止，精准复现 C# 每库终身持有版本表的语义；而未发生改写的裸 FLUSHDB 零推进，符合判净标准。
6) 并发互斥与 TOCTOU 竞态消除：
在 TransactionManager::run / run_exec 中，事务执行前必须通过 register_run_preamble / save_lock_hashes 率先对所有 WATCH 键在当前物理域取得 LockType::Shared 读锁。持有读锁后，才在临界区内调用 validate_watch_version 校验版本表。任何并发写者写入该键必须获取 LockType::Exclusive 锁，将被读锁排他拦截；若写者在加读锁前已完成写入，则其 bump_watch_version 已完成递增，读锁后的版本校验立即判失效。整个时序无检查与修改间隙（TOCTOU）窗口。

四、核查视角规约确证与偏离对齐
1. 架构改良在册性核验：
1) doc/zh/deviations.md 第 115 条：双轨域分离（版本轨以逻辑前缀为种子防换号脱节，锁轨以物理前缀现算防死旧桶），属多租户物理单存储架构的核心自研改良。
2) doc/zh/deviations.md 第 121 条：事务中止与放弃时的锁清理收敛。
3) doc/zh/deviations.md 第 123 条：对无 TTL 的键执行 PERSIST 属于纯探测，C# 原型在 InPlaceDeleter !Modified 分支同样判定无修改，wedb 维持零推进，符合判净与防误杀原则。
4) doc/zh/deviations.md 第 131 条：SELECT 切库主动注销在途 WATCH 登记，对齐 C# 换库事务管理器换任语义，彻底清除跨库误杀与跨库锁残留。
5) doc/zh/deviations.md 第 132 条：PFADD 在 HyperLogLog 寄存器未变时不推进版本，完全对齐 Redis 规范。
6) doc/zh/deviations.md 第 140 条：引擎写钩子槽采用 OnceLock，支持置换引擎通过钩子束重挂。

五、现有专项回归测试网核验
全仓已建立 6 套完备的高强度回归测试护栏，全路径覆盖了版本表写面推进：
1. wedb/wnode/tests/watch_version_regression.rs (655 行)：覆盖快路径写/删、跨租户与跨库隔离、DEL 与 TTL 改判臂、缺席键删除推进、恰一次推进、换号后改写 abort、裸 FLUSHDB 通过（判净 6）、直设臂逻辑域透传等。
2. wedb/wnode/tests/tiered_watch_fence.rs (1172 行)：覆盖分层态 BfTree 集合写、升阶/降阶、删空自愈与拒写臂零推进。
3. wedb/wnode/tests/range_index_watch_fence.rs (191 行)：覆盖 RangeIndex 新增/覆写/批量写/删空自愈版本栅栏。
4. wedb/wnode/tests/vector_vadd_watch_purge.rs (307 行)：覆盖向量存储过期残留清退与 VADD 版本推进。
5. wedb/wnode/tests/select_switch_db_invalidates_watch.rs (184 行)：覆盖 SELECT 切库作废旧库 WATCH 位点。
6. wedb/wkv/tests/delete_miss_watch.rs 与 wedb/wtxn/tests/txn_watched_keys.rs：覆盖底层缺席删除推进与事务管理器容器快照校验。

六、结论总结
经对 C# Garnet 原型与 Rust wedb 仓内全链路（存储层引擎钩子、数据结构快慢写路径、自适应分层 BfTree、RangeIndex、自定义对象、键级与 TTL 生命周期、换号双轨域、并发加锁时序）逐项核验确证：
1. 所有键值变更路径无一遗漏均受版本推进钩子收口；
2. 逻辑域哈希种子确保换号后改写精确命中在途 WATCH 槽位；
3. Shared 锁窗前置排他保证无任何 TOCTOU 时序空隙；
4. 排除条款（无 TTL PERSIST、零修改 PFADD、同键 RENAME）与 C# 及在册偏差完全一致。
全链路闭环，无缺陷、无脱节。

视角结论:已穷尽
