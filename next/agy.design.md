review-design：数据链条 死代码 重复机制 常量工具 模块拓扑

check.js 现状与全仓审查
重复 20 组，数据链条与架构相关 9 组，真重复 3 处，其余为注释锚点复挂。全仓发现多处死代码、跨层状态枚举分裂、输入 token 散落裸写、以及对象层反向依赖等拓扑问题。

1. PEM 解析入站出站两份同形
问题：load_certs 与 load_private_key 在入站与出站配置各写一份，逐行同形。
rust：wedb/wnode/src/tls/config.rs fn load_certs fn load_private_key，wedb/wconn/src/tls.rs fn load_certs fn load_private_key
c#：garnet/libs/server/TLS/GarnetTlsOptions.cs fn GetCertificateIssuer
动作：下沉到 wbase 或 wconf 一处定义，两端直接调用。

2. 状态与结果枚举跨层三套各自定义
问题：GarnetStatus 在 wnode，StoreResult 在 wkv 会话，ObjLoad 在 wcol，三者描述命中、缺失、类型错误、异步降级等重叠语义，各层映射靠口头约定。
rust：wedb/wnode/src/types.rs enum GarnetStatus，wedb/wkv/src/session/raw/read.rs enum StoreResult，wedb/wcol/src/object_payload.rs enum ObjLoad
c#：garnet/libs/server/API/GarnetStatus.cs enum GarnetStatus，garnet/libs/storage/Tsavorite/cs/src/core/Index/Common/OperationStatus.cs enum OperationStatus
动作：在 wval 或 wbase 建立统一状态转换 trait 与映射契约，消除各层手写映射。

3. 输入 Token 常量散落裸字符串缺乏单点
问题：wresp::cmd_strings 仅收录输出帧与错误文案，输入侧 COUNT、WITHSCORES、LIMIT 等 token 在各命令解析处裸内联十余次。
rust：wedb/wresp/src/cmd_strings.rs，wedb/wcol/src/types/scan_input.rs，wedb/wnode/src/resp/objects/list_commands/read.rs，wedb/wnode/src/resp/objects/sorted_set_commands/write.rs
c#：garnet/libs/server/Resp/CmdStrings.cs
动作：在 wresp::cmd_strings 集中补齐输入 Token 常量组，各调用点改为引用常量。

4. 错误文案常量在命令模块自立私有常量
问题：部分命令模块脱离 wresp 单点，自立文件级 const 错误文案，存在重复与拼写漂移。
rust：wedb/wnode/src/resp/acl_commands.rs const RESP_ERR_ACL_FOREIGN_NAMESPACE const RESP_ERR_ACL_GENPASS_BITS_RANGE，wedb/wnode/src/resp/txn_resp_commands.rs const RESP_ERR_TRANSACTION_FAILED，wedb/wnode/src/resp/objects/object_store_utils.rs const RESP_ERR_CORRUPT_PAYLOAD，wedb/wnode/src/resp/objects/sorted_set_commands/mod.rs const RESP_ERR_MIN_OR_MAX_NOT_VALID_STRING_RANGE_ITEM，wedb/wnode/src/resp/array_commands.rs const RESP_ERR_LENGTH_AND_INDEXES
c#：garnet/libs/server/Resp/CmdStrings.cs
动作：全部归入 wresp::cmd_strings 单点，消除私有定义。

5. 读会话残存 C# IFunctions 悬空写能力死代码
问题：upsert_forbidden、rmw_forbidden、delete_forbidden 三函数同体，Rust 读会话类型已无写能力，属 C# 接口形态残留。
rust：wedb/wkv/src/session/consistent_read.rs fn upsert_forbidden fn rmw_forbidden fn delete_forbidden
c#：garnet/libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs fn Upsert fn RMW fn Delete
动作：直接删除，零生产调用。

6. 元记录切片写入第二出口死代码
问题：write_to_slice 为 to_bytes 第二出口，生产全量走 to_bytes，本函数悬空。
rust：wedb/wval/src/meta.rs fn write_to_slice
c#：garnet/libs/server/Objects/Types/GarnetObjectType.cs 相关元数据段
动作：删除 write_to_slice，保留单出口 to_bytes。

7. 信封裸编码函数零生产消费
问题：obj_encode 仅在测试中使用，生产全链路均使用零分配的 obj_encode_into 与 obj_encode_custom_into。
rust：wedb/wcol/src/object_payload.rs fn obj_encode
c#：garnet/libs/server/Objects/Types/GarnetObjectSerializer.cs fn Serialize
动作：删除裸 obj_encode，全链路统一使用预分配缓冲入口。

8. RESP 读取层零消费跳过与指针解析函数
问题：try_skip_byte_array_with_length_header 与 try_read_ptr_with_length_header 生产零调用。
rust：wedb/wresp/src/read.rs fn try_skip_byte_array_with_length_header fn try_read_ptr_with_length_header
c#：garnet/libs/server/Resp/RespServerSession.cs
动作：删除两处零消费函数。

9. 会话分派臂内联绕过方法体致基础命令变死代码
问题：PING、ASKING、ECHO 分派臂内联应答，导致对应的 network_* 方法在生产中零调用成为死代码。
rust：wedb/wnode/src/resp/basic_commands/mod.rs fn network_ping fn network_asking fn network_echo，wedb/wnode/src/resp/resp_server_session.rs
c#：garnet/libs/server/Resp/BasicCommands.cs fn NetworkPING fn NetworkASKING fn NetworkECHO，garnet/libs/server/Resp/RespServerSession.cs
动作：分派臂统一转调 network_* 方法，删除臂体内联副本，消除双轨。

10. RESP Null 与 Null Array 多模块内联版本判断
问题：hash_commands 与 list blocking 等处手写内联版本判断，单点已存在于 wresp::ext::RespVecExt。
rust：wedb/wnode/src/resp/resp_server_session_output.rs fn write_null fn write_null_array，wedb/wnode/src/resp/objects/hash_commands.rs fn write_null_array，wedb/wcol/src/resp/output.rs fn write_null fn write_null_array，wedb/wresp/src/ext.rs fn write_resp_null_ver fn write_resp_null_array_ver
c#：garnet/libs/server/Resp/RespServerSessionOutput.cs fn WriteNull fn WriteNullArray
动作：统一转调 wresp::ext::RespVecExt，消除局部重复。

11. 集合类型探测三连跳手写瀑布流重复
问题：obj_load_custom_sync、obj_load_typed_async、obj_length_sync、obj_length_async 逐个手写 Meta 到 ObjectEnvelope 到 String 的三连探针瀑布流。
rust：wedb/wnode/src/resp/objects/object_store_utils.rs fn obj_load_custom_sync fn obj_load_typed_async fn obj_length_sync fn obj_length_async
c#：garnet/libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs
动作：抽象统一 probe_key_domain 工具函数，收敛三步探针逻辑为一处定义。

12. 登记表复合键剥域用户键重复手写且兜底错误
问题：split_registry_key(rk).map_or(rk, ...) 在两处重复书写，且解析失败时错误地回退带域前缀的整键造成泄漏。
rust：wedb/wedb/src/server/replication/snapshot_iter/replication_snapshot_iterator.rs，wedb/wedb/src/server/cluster_session/migrate_session/migrate_session_vector_set.rs
c#：garnet/libs/server/Resp/Vector/VectorManager.cs
动作：在向量登记表模块统一定义 registry_user_key 单点，解析失败显式拒绝，两处直接复用。

13. User 对象持有无意义并发原语
问题：User 结构体挂 AtomicBool、ArcSwap 与 CAS 重试环，但当前架构下句柄每连接私有，无并发写者。
rust：wedb/wacl/src/user.rs struct User
c#：garnet/libs/server/ACL/User.cs
动作：User 改为只读不可变结构，消除内部 AtomicBool 与 ArcSwap 重试环，由外部连接句柄持有。

14. 索引桶闩锁与事务锁表同名同挂锚点
问题：windex HashBucket 四函数为原子位真实现，wtxn TxnLockTable 四函数为下标转发薄壳，两层同名同挂四组锚点报重复。
rust：wedb/windex/src/bucket.rs fn try_lock_shared fn unlock_shared fn try_lock_exclusive fn unlock_exclusive，wedb/wtxn/src/txn_lock_table.rs fn try_lock_shared fn unlock_shared fn try_lock_exclusive fn unlock_exclusive
c#：garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs fn TryAcquireSharedLatch fn ReleaseSharedLatch fn TryAcquireExclusiveLatch fn ReleaseExclusiveLatch
动作：wtxn 侧去除锚点并注明委托调用，消除 check.js 误报。

15. 范围索引锁面暴露裸引用破坏封装
问题：wbftree RangeIndexManager::locks 直接暴露 StripedRwLock 裸引用，wkv StoreSession::acquire_tree_write 是异步守卫封装，双入口管同一棵树易绕过守卫。
rust：wedb/wbftree/src/manager/mod.rs fn RangeIndexManager::locks，wedb/wkv/src/range_index/stub.rs fn StoreSession::acquire_tree_write
c#：garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs fn AcquireExclusiveForDelete
动作：locks 收为 crate 内可见或诊断专用，写路径统一走 StoreSession 异步守卫。

16. 快照统计三层同名结构体与双重投影冗余
问题：wkv WedbStore::store_snapshot 输出 StoreSnapshot，wnode 经两级投影转 DbSnapshot 与 AofSnapshot，再送入 wmetric，结构体字段大面积同名重复。
rust：wedb/wkv/src/store/stats.rs fn WedbStore::store_snapshot，wedb/wnode/src/resp/garnet_api/mod.rs fn project_db_snapshot fn project_aof_snapshot，wedb/wmetric/src/info/garnet_info_metrics.rs
c#：garnet/libs/server/StoreWrapper.cs fn GetDatabasesSnapshot，garnet/libs/server/Metrics/Info/GarnetInfoMetrics.cs fn GetDatabaseStoreStats fn GetDatabasePersistenceStats
动作：快照结构由 wmetric 统一定义，投影函数注明组装点，避免跨层多套同名镜像。

17. resp_server_session.rs 3172 行巨型文件待拆分
问题：单个文件同时承担网络缓冲区管理、协议解析、鉴权、事务、Lua、指标与命令分派。
rust：wedb/wnode/src/resp/resp_server_session.rs
c#：garnet/libs/server/Resp/RespServerSession.cs
动作：按 C# 职责拆分为 resp_server_session/ 目录模块（core, auth, lua, txn, client_info, metrics）。

18. tiered_collection_ops.rs 2237 行上帝分层文件待拆分
问题：单个文件同时承载 Hash、List、Set、ZSet 四类会话操作、公共底座与成员 TTL。
rust：wedb/wnode/src/resp/objects/tiered_collection_ops.rs
c#：garnet/libs/server/Storage/Session/ObjectStore/Common.cs、HashOps.cs、ListOps.cs、SetOps.cs、SortedSetOps.cs
动作：拆分为 tiered_collection_ops/ 目录模块（common.rs, hash.rs, list.rs, set.rs, zset.rs, scan.rs）。

19. set_commands.rs 与 hash_commands.rs 缺乏目录化拆分
问题：set_commands.rs 1431 行，hash_commands.rs 1116 行，而兄弟模块 list_commands 与 sorted_set_commands 均已目录化。
rust：wedb/wnode/src/resp/objects/set_commands.rs，wedb/wnode/src/resp/objects/hash_commands.rs
c#：garnet/libs/server/Resp/Objects/SetCommands.cs，garnet/libs/server/Resp/Objects/HashCommands.cs
动作：对齐 list 与 sorted_set 模式，拆为 set_commands/ 与 hash_commands/ 目录模块（read, write, slow, mod）。

20. vdb.rs 1495 行单文件混合路由表、DbMeta 持久化与 GC 队列
问题：单个文件同时包含并发路由表 DbRoutingTable、DbMetaRecord 编解码、后台 GC 队列与纪元保护。
rust：wedb/wkv/src/vdb.rs
c#：garnet/libs/server/Storage/Session/MainStore/
动作：拆分为 vdb/ 目录模块（routing.rs 路由表，meta.rs DbMeta 记录，gc.rs 垃圾回收队列与安全纪元）。

21. wcol 反向依赖 wresp 导致对象层与线协议层倒置
问题：底层集合对象层 wcol 的 Cargo.toml 直接依赖上层 wresp，仅用于获取 ObjectOutput 与 RespInputFlags。
rust：wedb/wcol/Cargo.toml dependencies wresp，wedb/wcol/src/resp/output.rs input.rs
c#：garnet/libs/server/Garnet.server.csproj
动作：将 ObjectOutput 与 RespInputFlags 移至 wresp 或 wnode，解除 wcol 对 wresp 依赖，还原纯数据对象层。

22. wcol 承载异步任务调度器与运行时强耦合
问题：阻塞取件经纪 CollectionItemBroker 依赖 compio 与 crossfire 运行时，被置于底层集合 crate 内。
rust：wedb/wcol/src/itembroker/collection_item_broker.rs
c#：garnet/libs/server/Objects/ItemBroker/CollectionItemBroker.cs
动作：将 itembroker 移至 wnode 业务服务层，从 wcol 剥离异步调度与通道依赖。

23. 单调时钟与实时时钟混淆隐患
问题：now_stopwatch_ticks 注释称 100ns 单调域，实现却取 Clock::now_since_epoch 实时域，消费点存在裸减回绕隐患。
rust：wedb/wbase/src/time.rs fn now_stopwatch_ticks，wedb/wnode/src/resp/metrics_commands.rs
c#：garnet/libs/server/Utilities/Stopwatch.cs
动作：严格区分实时时钟（绝对时间戳）与单调时钟（Stopwatch 单调计数），业务层耗时统计增加防回拨防护。

24. 元布局常量半公开
问题：SIZE_OFFSET 与 META_VALUE_SIZE 公开，TYPE_OFFSET 与 NEXT_EXPIRY_OFFSET 私有，外部无法直读 type 偏移。
rust：wedb/wval/src/meta.rs const SIZE_OFFSET const META_VALUE_SIZE const TYPE_OFFSET const NEXT_EXPIRY_OFFSET const U64_LEN
c#：garnet/libs/server/Objects/Types/GarnetObjectType.cs
动作：偏移量统一收敛为私有常量，通过 MetaValue 关联读取方法暴露字段，防止外部硬编码。

已确认单点无需动
wval tag.rs KeyTag 与 GarnetObjectType 与 CustomObjectType 三枚举一处定义。
wkv session/mod.rs session_prefix 与 with_prefix 三口径合一。
wbase time now_ticks 与 convert utc_now_ticks 时间单源。
wresp ext null 与 error 版本分派单源。
