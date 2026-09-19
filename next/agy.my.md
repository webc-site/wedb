review-my 待办：自定义优化上下游打通与正确高效优雅审查清单

check.js 现状
本域聚焦于集合自适应分层存储、命名空间多库隔离、虚拟库秒级清库、零拷贝借用、批量折叠、严格删空自愈与原子墓碑、O(1) 计数规约及核心基础选型。经全链路审查，发现分层集合大键删除退化为全树物化与整树重建、从库回放换号未同步主库虚号导致主从路由失步、SWAPDB 缺失 AOF 入账、后台降阶局限根库、TTL 计数全树扫描、ACL 物理键与紧缩判死冲突等关键断层。

1. 分层集合删除退化为全树物化与整树销毁重灌
问题：因底层 bf-tree 0.5.6 的 ScanIter::next 存在跳过墓碑的尾递归栈溢出缺陷，代码完全移除了树内逐成员删除命令臂（HDEL/SREM/ZREM/LPOP/RPOP/SPOP 等）。分层态大集合的任意单元素删除操作，一律穿透至 run_async_rmw 物化降级通道，把整棵千万级条目的 B+ 树反序列化物化到内存，在内存中删改后，调用 handle_bftree_drain_and_delete 物理销毁整棵树，再调用 promote_collection_to_bftree 重新 bulk_load 全量建树。把 O(log N) 页级删除退化为 O(N) 扫全树 + O(N) 内存分配 + O(N log N) 重建全树，严重违背 collection.md 页级冷热换入换出与消除全量反序列化的核心承诺。
rust：wedb/wnode/src/resp/objects/tiered_collection_ops.rs fn tree_del_batch fn tiered_zset_arm，wedb/wnode/src/resp/objects/rmw_helpers.rs fn apply_rmw_post_operate，wedb/wkv/src/range_index/stub.rs fn promote_collection_to_bftree
c#：libs/server/Storage/Functions/ObjectStore/RMWMethods.cs fn InPlaceUpdaterWorker fn PostCopyUpdater
动作：循环化重构底层 bf-tree 的 ScanIter::next 尾递归消除墓碑跳过栈溢出，恢复分层树原生页级逐成员删除与范围删除命令臂，废止单元素删除触发整树物化重建的倒退逻辑。

2. 分层有序集合范围与排名命令全缺失，穿透全量物化
问题：分层态 SortedSet 在 B+ 树内仅按 member 字典序存储 member -> score，只实现了 ZADD/ZSCORE/ZMSCORE/ZCARD/ZINCRBY/ZEXPIRE/ZTTL/ZPERSIST。针对核心的范围检索与排名命令（ZRANGE/ZRANGEBYSCORE/ZREVRANGE/ZRANK/ZREVRANK/ZCOUNT/ZPOPMIN/ZPOPMAX 等），树内无分值有序索引支持，全部在 tiered_zset_arm 末尾回退 Ok(false)，穿透至 run_async_rmw 触发全量反序列化物化，对大集合造成巨大延迟与内存抖动。
rust：wedb/wnode/src/resp/objects/tiered_collection_ops.rs fn tiered_zset_arm
c#：libs/server/Objects/SortedSet/SortedSetObjectImpl.cs fn SortedSetRangeByScore fn SortedSetRange
动作：在 wbftree 建立以 score+member 编码的有序双向索引支持，实现原生页级范围扫描；或在规范中明确分层 ZSet 仅限点查，范围查询声明性能折损。

3. 从库回放换号未同步主库虚拟 ID，主从路由映射必然分叉
问题：从库回放 FlushDb/FlushNs 条目时，条目载荷仅携带换号前旧物理域 (vns, old_vdb)。从库本地调用 flush_db_virtual 时，通过本地 alloc_next_virtual_id 独立生成新虚库号，并更新本地 db_routing。主库随后写入的数据条目物理前缀携带的是主库生成的新虚库号。主从虚号自增序列因环境差异或漏步必然分叉，导致从库逻辑库路由指向本地新号，而物理数据落入主库新号，从库查询 logic_db 完全读不到主库新数据。且 service.rs 的 AOF 写端口按标签排除了 KeyTag::DbMeta，导致主库磁盘映射完全不向从库镜像。
rust：wedb/wnode/src/aof/aof_processor.rs fn aof_replay，wedb/wkv/src/vdb.rs fn flush_db_virtual fn flush_ns_virtual，wedb/wnode/src/database/single_database_manager.rs fn flush_database，wedb/wnode/src/service.rs fn on_aof_store_event
c#：libs/server/Databases/SingleDatabaseManager.cs fn SafeFlushAOF，libs/server/AOF/AofProcessor.cs fn SwitchActiveDatabaseContext
动作：FlushDb/FlushNs 广播条目载荷补齐主库分配的 (new_vns, new_vdb)，从库回放直接使用主库分配号设置本地映射，禁止本地独立取号；或开启 KeyTag::DbMeta 主从直接镜像。

4. SWAPDB 零 AOF 入账且无从库回放，主从拓扑与重启持久化断层
问题：主库执行 swap_databases 时，仅在本地单元格互换虚拟 ID 并将 DbSwap 写入本地 DbMeta，全链路未向 AOF 写入任何广播条目（AofEntryType 缺少 SwapDb 枚举，且 service.rs 拦截了 DbMeta 镜像）。从库对 SWAPDB 完全无感知，导致主从数据库完全对调颠倒；且若依靠增量 AOF 重启，SWAPDB 状态丢失。
rust：wedb/wkv/src/session/swap.rs fn swap_databases，wedb/waof/src/aof/entry_type.rs enum AofEntryType，wedb/wnode/src/service.rs fn on_aof_store_event，wedb/wnode/src/resp/garnet_api/slow.rs fn slow_path
c#：libs/server/Databases/MultiDatabaseManager.cs fn TrySwapDatabases
动作：在 AofEntryType 中新增 SwapDb 并在 swap_databases 执行后入队 AOF；在 aof_processor.rs 补齐 SwapDb 回放逻辑，驱动从库槽位路由表同步互换。

5. 后台懒降阶硬编码仅扫描主库根域 (ns 0, db 0)，非零租户与非零库永远无法降阶
问题：tiered_demote_round 调用 collect_demote_candidates 时，直接获取会话默认前缀（ns 0, db 0）的 prefix_slice，在全库 hlog 扫描中仅匹配该前缀。导致所有 ns > 0 的租户或 db > 0 的数据库中的分层集合，在后台永远被过滤跳过，落入死区之下的分层树永远无法被周期任务降阶释放。
rust：wedb/wnode/src/resp/objects/tiered_demote.rs fn collect_demote_candidates fn tiered_demote_round
c#：libs/server/Databases/DatabaseManagerBase.cs fn ExecuteObjectCollection
动作：collect_demote_candidates 移除会话默认前缀约束，扫描时按物理键标签为 KeyTag::Meta 解码 (vns, vdb, user_key)，跨全量租户与数据库收集降阶候选。

6. 分层集合带字段 TTL 时 HLEN 与 ZCARD 退化为 O(N) 扫全树
问题：分层集合持有字段级 TTL 且当前时间超过元记录 next_expiry 水位时，HLEN 与 ZCARD 降级至 exec_tiered_collect。该函数调用 collect_expired_members，自树起点无界扫描整棵树（scan_with_count_callback(&[0u8], usize::MAX, ...)），将所有到期键压入 Vec 并批量删除。对于千万级大集合，瞬时计数命令由承诺的 O(1) 骤降为 O(N) 全树扫盘与批量删除，同时引入尾递归栈溢出隐患。
rust：wedb/wnode/src/resp/objects/tiered_collection_ops.rs fn collect_expired_members fn exec_tiered_collect，wedb/wnode/src/resp/objects/object_store_utils.rs fn obj_length_sync fn obj_length_async
c#：libs/server/Objects/Hash/HashObject.cs fn Count，libs/server/Objects/SortedSet/SortedSetObject.cs fn Count
动作：分层态增加字段到期时间辅助索引或单批有界清理（限批截断），避免瞬时全树扫描；或将计数估算与物理出账彻底解耦。

7. ACL 物理键直写逻辑命名空间，日志紧缩误删存活用户
问题：AclStore 读写用户规则时，直接使用逻辑命名空间 caller_namespace 编码物理键前缀（SessionPrefixBuf::new(ns, 0)）。底层日志紧缩 is_deleted 判死逻辑中，将物理键前缀解析为 rec_vns 并直接比对 gc_dead。当系统中某个退役虚拟命名空间到期回收且其 ID 恰好等于某个租户的逻辑命名空间数值时，紧缩会把该租户的所有存活 ACL 记录误判为已死并物理抹除；且 KeyTag::Acl 未被加入紧缩豁免列表。
rust：wedb/wnode/src/resp/acl_store.rs fn prefix fn physical_key，wedb/wkv/src/compact.rs fn is_deleted
c#：libs/server/ACL/AccessControlList.cs
动作：在 compact.rs 的 is_deleted 中将 tag == KeyTag::Acl 加入与 KeyTag::DbMeta 同等的紧缩判死豁免，或在 AclStore 中将 logic_ns 统一通过 vdb 映射为 virtual_ns_id。

8. 在线连接 ACL 句柄缺乏版本失效机制，权限修改无法实时生效
问题：客户端认证成功后，将 UserHandle 缓存在连接私有的 acl_user_handle 中，后续命令鉴权仅读取本地句柄位图。当管理员通过 ACL SETUSER 或 ACL DELUSER 修改权限或删除用户时，仅更新了底层存储中的 KeyTag::Acl 记录，并未向已在线连接广播失效或递增代数。已认证客户端将无限期持有旧权限句柄直至断开重连。
rust：wedb/wnode/src/resp/acl_store.rs fn write fn delete，wedb/wnode/src/resp/resp_server_session.rs struct RespServerSession field acl_user_handle
c#：libs/server/ACL/AccessControlList.cs fn ChangeUserPassword fn DeleteUser
动作：在 AccessControlList 或全局上下文中维护 ACL 代数版本号，连接在命令鉴权时比对本地代数，版本落后时惰性重查存储刷新句柄。

9. ObjectOutput 携带临时 Vec 中转，违反对象命令零拷贝原则
问题：wcol::ObjectOutput 结构体定义中 payload 为 Vec<u8>，所有集合操作在执行 operate 时均在堆上构造临时 Vec 并格式化 RESP 字节，外层收到后再拷贝至网络发送缓冲区，产生不必要的单命令堆内存分配与数据复制。
rust：wedb/wcol/src/resp/output.rs struct ObjectOutput field payload，wedb/wnode/src/resp/objects/hash_commands.rs fn hash_get
c#：libs/server/Objects/Types/ObjectOutput.cs struct ObjectOutput（字段直接为 SpanByteAndMemory 借用网络会话缓冲）
动作：改造 ObjectOutput 支持直接传入网络会话可变缓冲区或 SmallVec，消除中间 Vec 分配。

10. ZADD 树态写入缺乏局部排序与批量折叠，逐成员多轮穿透 B+ 树
问题：tiered_zset_arm 的 ZADD 处理循环中，针对每一个成员，逐条调用 tree_member_state（读穿透）、tree.read_callback（二次读穿透）以及 tree_put_ok（写穿透）。未如 HSET/SADD 采用 tree_put_batch 批量下刷，也未在栈上对输入成员进行局部排序，导致批量 ZADD 引发剧烈的树页重复借用与频繁页分裂。
rust：wedb/wnode/src/resp/objects/tiered_collection_ops.rs fn tiered_zset_arm
c#：libs/server/Objects/SortedSet/SortedSetObjectImpl.cs fn SortedSetAdd
动作：对 ZADD 成员在栈上先按字节序进行局部排序去重，单次进入写锁后批量执行分值比对与树页批量 upsert。

11. GarnetObjectType 自造 RangeIndex 导致 Redis TYPE 命令响应分叉
问题：wval::GarnetObjectType 枚举在原版 C# 之外自行增加了 RangeIndex = 5 分支，导致 Redis TYPE 命令在面对分层态 RangeIndex 键时返回非标的 +rangeindex 类型字符串，破坏了与官方 Redis 协议与客户端生态的兼容性。
rust：wedb/wval/src/tag.rs enum GarnetObjectType，wedb/wnode/src/resp/array_commands.rs fn network_type
c#：libs/server/Objects/Types/GarnetObjectType.cs enum GarnetObjectType
动作：对标 C# 规范，将底层物理存储形态与逻辑 RESP 对象类型解耦；TYPE 命令对树态键按其原始集合类型（zset/hash 等）回显。

12. 从库向量重放反查逻辑域失败回退物理号，导致槽位计算错误
问题：aof_processor.rs 回放向量条目时，通过 logic_domain_of(vns, vdb) 反查逻辑域以计算 repl_slot。若从库路由表中未在册该物理库，logic_domain_of 会回退返回物理库号 vdb，进而调用 slot_of(vns, vdb) 计算集群槽位。由于混合器要求输入逻辑域，混入物理号将产生错误的槽位号，导致从库的向量索引挂载到错误的集群槽位中。
rust：wedb/wnode/src/aof/aof_processor.rs fn aof_replay，wedb/wkv/src/vdb.rs fn logic_domain_of
c#：libs/cluster/Server/Migration/VectorSetMigration.cs
动作：向量 AOF 镜像条目直接持久化原始逻辑槽位或逻辑域，避免在从库回放端通过易失的逆向表反查推导。

13. lock_dbmeta 在多核高竞争场景下忙轮询空转 CPU
问题：换号元数据串行锁 lock_dbmeta 采用 AtomicBool compare_exchange 配合 yield_now().await。在 compio 多核运行环境中，如果一个核持有锁执行阻塞落盘或页翻转，其他核上的任务在独占线程中进入 yield_now 循环，因跨核调度无法协作让出 CPU，会导致其他核心 100% 忙轮询空转。
rust：wedb/wkv/src/store/mod.rs fn lock_dbmeta
c#：libs/server/Databases/DatabaseManagerBase.cs
动作：换号串行锁改用跨线程异步等待队列或基于 crossfire / event_listener 的异步 Mutex，彻底消除跨核忙轮询。

14. ACL LIST 与 USERS 两次扫描无快照，并发增删导致应答条数不一致
问题：ACL LIST 和 ACL USERS 采用两遍流式扫描：第一遍仅计算条数并写出 RESP 数组长度，第二遍重新扫描存储并写出每个用户的数据。在两遍扫描的间隙若有并发连接增删用户，第二遍实际写出的元素个数将与第一遍宣告的数组长度发生偏差，破坏客户端协议解析。
rust：wedb/wnode/src/resp/acl_commands.rs fn network_acl_list fn network_acl_users
c#：libs/server/Resp/ACL/ACLCommands.cs fn NetworkAclList
动作：首遍扫描在栈上或轻量小缓冲收集用户名快照，次遍按快照点查或直接流式收集，保证宣告长度与实际元素强一致。

15. 升阶过程数据流广播与元记录落盘非原子，崩溃留双态残留
问题：promote_collection_to_bftree 中，先将快照文件分块发送 RangeIndexStream 到 AOF，随后落盘 Meta 存根，最后删除信封。如果在 AOF 入队成功后、元记录落盘前进程崩溃，从库将建立起完整的 BfTree，而主库本地却只有信封旧数据；反之若信封删除失败，主库本地留存双态。
rust：wedb/wkv/src/range_index/stub.rs fn promote_collection_to_bftree
c#：libs/server/Storage/Functions/ObjectStore/RMWMethods.cs fn InPlaceUpdaterWorker
动作：将升阶状态转换为事务批提交，在元记录与信封状态落定后再提交从库流同步，或者增加崩溃恢复时的双态自愈识别。

16. MSET 批量折叠堆分配打破零拷贝工程准则
问题：try_upsert_batch_sync 在执行批量折叠时，将迭代器全部 collect 进堆分配的 Vec<(K, V)> 中再进行排序。对于短小成对写入（例如 MSET k1 v1 k2 v2），强行引入了额外的堆分配与回收开销。
rust：wedb/wkv/src/session/mod.rs fn try_upsert_batch_sync
c#：libs/server/Storage/Session/MainStore/MainStoreOps.cs fn MSET_Conditional
动作：小批量（如 <= 16 对）采用栈上固定容量数组（类似 SmallVec 或 StackHeapBuf）完成局部排序，超量再降级堆分配。

17. bitcode 编码在集合序列化中缺失借用视图导致频繁深拷贝
问题：各对象的 Wire 结构体（如 HashWire、SortedSetWire）在序列化与反序列化时将字段与值全部 owned 拷贝成 Vec<u8>，大集合在升降阶转换与快照落盘时产生密集的堆内存二次分配。
rust：wedb/wcol/src/hash/hash_object.rs struct HashWire，wedb/wcol/src/zset/sorted_set_object.rs struct SortedSetWire
c#：libs/server/Objects/Types/GarnetObjectSerializer.cs fn Serialize
动作：引入带生命周期的借用视图类型（如 HashWire<'a>），在序列化写出时直接借用内存对象的 &[u8]，避免全量 clone。

18. WATCH 版本推进在 TTL 后台清理与 GC 物理清退时缺席
问题：WatchHook 经 StoreSession 挂载后，前台用户键写删操作与分层 drain 能正确推进版本号。但在后台 purge_expired 惰性清理过期键以及 compact/GC 物理丢弃记录时，未回调 WatchHook。如果客户端 WATCH 了一个已到期的键，后台清理发生时客户端事务未能感知版本更新。
rust：wedb/wkv/src/ttl.rs fn purge_expired，wedb/wkv/src/compact.rs fn compact_core，wedb/wtxn/src/watch_version_map.rs struct WatchVersionMap
c#：libs/server/Transaction/WatchVersionMap.cs fn IncrementVersion
动作：在 TTL 到期物理清除点注入 WatchHook 推进版本，或在设计规范中严密界定 WATCH 仅针对显式写命令生效。

19. 虚拟库号与逻辑库号在公共 API 混用未做新类型强隔离
问题：StoreSession、keyspace、vdb 等模块中，logic_ns/logic_db 与 virtual_ns_id/virtual_db_id 均使用裸 u64，参数位置极易混淆倒置。虽然加了注释约束，但在 AOF 回放、compact 扫描、ACL 存储中多次出现将逻辑域误当物理域或物理域误当逻辑域的隐患。
rust：wedb/wkv/src/session/mod.rs struct StoreSession，wedb/wkv/src/vdb.rs struct VirtualDbManager
c#：libs/server/Databases/DatabaseManagerBase.cs
动作：引入强类型包装结构体（如 struct LogicNs(u64)、struct VirtualNs(u64)），利用 Rust 零成本抽象在编译期根除混用。

20. papaya 并发表随机种子与全局确定性种子策略冲突隐患
问题：SKILL.md 规定 gxhash 默认使用随机种子防 DoS，确定性种子仅限持久化派生值。但在根 Cargo.toml 中为 gxhash 开启了 deterministic 特性，导致全局哈希表（含 papaya 并发表）默认变成了确定性哈希，丧失了针对 HashDoS 的随机化防御能力；若手动移除该 feature，SCAN 等依赖确定性迭代序的命令又可能产生游标漂移。
rust：wedb/wbase/src/map.rs fn new_concurrent_map，wedb/Cargo.toml feature deterministic
c#：libs/server/ConcurrentDictionary
动作：剥离全局 deterministic Cargo feature，在需要保序扫描的容器显式传入固定种子，常规并发字典与 HashMap 保持默认随机种子。
