review-design：数据链条死代码 重复机制 常量工具 模块拓扑

check.js 现状
重复 20 组，数据链条相关 9 组，真重复 3 处，其余为注释锚点复挂。实现缺失 3 项无数据链条项。net 侧 7 组见 next/muse.net.md，此处不重复。

1. PEM 解析两份逐行同形，真重复
问题：load_certs 与 load_private_key 在入站出站各写一份，注释称同一 rustls-pemfile 栈但代码仍两份。
rust：wedb/wnode/src/tls/config.rs fn load_certs fn load_private_key，wedb/wconn/src/tls.rs fn load_certs fn load_private_key
c#：garnet/libs/server/TLS/GarnetTlsOptions.cs fn GetCertificateIssuer 相关载入段
动作：下沉到 wbase 或 wconf 一处定义，两侧薄包装。

2. 服务端 TLS 三函数同挂一锚点
问题：from_pem_files 与 from_der 只是两构造入口，server_config 才是装配单点，三处同挂 GetSslServerAuthenticationOptions 报重复。
rust：wedb/wnode/src/tls/config.rs fn server_config 保留，fn ServerTlsConfig::from_pem_files fn ServerTlsConfig::from_der 去锚点
c#：garnet/libs/server/TLS/GarnetTlsOptions.cs fn GetSslServerAuthenticationOptions
动作：只改注释。

3. WriteNull 三处各写一遍版本分派
问题：hash_commands::write_null_array 自拼数组头加循环，blocking::write_collection_item_result 内联空分支版本判断，单点已在 wresp::ext::RespVecExt。
rust：wedb/wnode/src/resp/objects/hash_commands.rs fn write_null_array，wedb/wnode/src/resp/objects/list_commands/blocking.rs fn write_collection_item_result，wedb/wresp/src/ext.rs fn write_resp_null_ver fn write_resp_null_array_ver
c#：garnet/libs/server/Resp/RespServerSessionOutput.cs fn WriteNull fn WriteNullArray
动作：前两者转调 wresp 单点，本层只留占位与收口语义。

4. WriteSetLength 三层同义
问题：RespWriter::write_set_length 泛型单态真源，cmd_strings::write_set_len 版本分派薄壳，set_commands::write_set_members 与 tiered 侧直接调壳再循环写成员。
rust：wedb/wresp/src/resp_memory_writer.rs fn write_set_length，wedb/wresp/src/cmd_strings.rs fn write_set_len，wedb/wnode/src/resp/objects/set_commands.rs fn write_set_members fn set_head_resp2_array_resp3_set
c#：garnet/libs/server/Resp/RespServerSessionOutput.cs fn WriteSetLength
动作：保留真源加薄壳，成员循环侧注明调用不另挂锚点。

5. OnDispose 一挂四处，链路过深
问题：network_del 经 wkv 双域删除判未命中回调 vector_registry_delete_hook 再到 delete_vector_set，with_vector_manager 只是装配注入，四处同挂 OnDispose。
rust：wedb/wnode/src/resp/array_commands.rs fn RespServerSession::network_del 保留，wedb/wnode/src/storage/session/storage_session.rs fn vector_registry_delete_hook，wedb/wnode/src/resp/vector/vector_manager.rs fn VectorManager::delete_vector_set，wedb/wnode/src/resp/garnet_api/mod.rs fn StoreGarnetApi::with_vector_manager 去锚点
c#：garnet/libs/server/Storage/Functions/GarnetRecordTriggers.cs fn OnDispose
动作：只留删除单点锚点，注入侧去锚点。

6. HashSet 锚点挂到批量漏斗
问题：tree_put_batch 是分层树内批量写漏斗，本体在 wcol HashObject::hash_set，前者复挂后者锚点报重复。
rust：wedb/wcol/src/hash/hash_object_impl.rs fn HashObject::hash_set 保留，wedb/wnode/src/resp/objects/tiered_collection_ops.rs fn tree_put_batch 去锚点
c#：garnet/libs/server/Objects/Hash/HashObjectImpl.cs fn HashSet
动作：只改注释。

7. 锁面两套入口
问题：wbftree RangeIndexManager::locks 直接暴露 StripedRwLock 裸引用，wkv StoreSession::acquire_tree_write 是异步守卫封装，两入口管同一棵树易绕过守卫。
rust：wedb/wbftree/src/manager/mod.rs fn RangeIndexManager::locks，wedb/wkv/src/range_index/stub.rs fn StoreSession::acquire_tree_write
c#：garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs fn AcquireExclusiveForDelete
动作：locks 收为 crate 内或注明诊断专用，写路径统一走守卫。

8. VectorManager 构造锚点挂到非构造
问题：ensure_cleanup_tasks_started 是后台清理任务启动，with_vector_set_preview 是预览开关 Builder，两者皆非构造。
rust：wedb/wnode/src/resp/vector/vector_manager_cleanup.rs fn VectorManager::ensure_cleanup_tasks_started，wedb/wnode/src/service.rs fn StorageSessionProvider::with_vector_set_preview 去锚点
c#：garnet/libs/server/Resp/Vector/VectorManager.cs fn VectorManager
动作：只改注释。

9. 快照三级结构体加两级投影
问题：wkv WedbStore::store_snapshot 出 StoreSnapshot，wnode project_db_snapshot 与 project_aof_snapshot 转 DbSnapshot 与 AofSnapshot，再喂 wmetric 两统计函数，三结构体字段大面积同名报两组重复。
rust：wedb/wkv/src/store/stats.rs fn WedbStore::store_snapshot，wedb/wnode/src/resp/garnet_api/mod.rs fn store_snapshots fn project_db_snapshot fn project_aof_snapshot，wedb/wmetric/src/info/garnet_info_metrics.rs fn GarnetInfoMetrics::get_database_store_stats fn GarnetInfoMetrics::get_database_persistence_stats
c#：garnet/libs/server/StoreWrapper.cs fn GetDatabasesSnapshot，garnet/libs/server/Metrics/Info/GarnetInfoMetrics.cs fn GetDatabaseStoreStats fn GetDatabasePersistenceStats
动作：trait 默认空实现与真实现分挂锚点，投影函数注明组装点不复挂统计锚点。

10. 闩锁双层同名
问题：windex HashBucket 四函数是原子位真实现，wtxn TxnLockTable 四函数是按桶下标转发的薄壳，两层同名同挂四组锚点。
rust：wedb/windex/src/bucket.rs fn HashBucket::try_lock_shared fn unlock_shared fn try_lock_exclusive fn unlock_exclusive 保留，wedb/wtxn/src/txn_lock_table.rs fn TxnLockTable::try_lock_shared fn unlock_shared fn try_lock_exclusive fn unlock_exclusive 去锚点
c#：garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs fn TryAcquireSharedLatch fn ReleaseSharedLatch fn TryAcquireExclusiveLatch fn ReleaseExclusiveLatch
动作：转发侧注明委托实现。

11. 预取跨层撞名
问题：windex prefetch_batch_probes 是索引探测预取，wkv read_batch_with 是批量读闭包，同挂 ContextReadWithPrefetch。
rust：wedb/windex/src/table.rs fn HashIndex::prefetch_batch_probes 保留，wedb/wkv/src/session/raw/batch.rs fn StoreSession::read_batch_with 去锚点
c#：garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs fn ContextReadWithPrefetch
动作：只改注释。

12. Lua 快路径分支复挂总入口
问题：try_fast_path_set 与 try_fast_path_get 是总入口内 SET 与 GET 两分支快路径，与总入口同挂一锚点。
rust：wedb/wlua/src/functions/redis.rs fn LuaRunnerFunctions::process_command_from_scripting 保留，fn try_fast_path_set fn try_fast_path_get 去总锚点改挂分支说明
c#：garnet/libs/server/Lua/LuaRunner.Functions.cs fn ProcessCommandFromScripting
动作：只改注释。
13. 错误文案四处散落常量
问题：全仓文案已收敛 wresp cmd_strings 与 cluster_cmd_strings，仍有四处文件级 const 自立。
rust：wedb/wnode/src/resp/acl_commands.rs const RESP_ERR_ACL_FOREIGN_NAMESPACE const RESP_ERR_ACL_GENPASS_BITS_RANGE，wedb/wnode/src/resp/array_commands.rs const RESP_ERR_LENGTH_AND_INDEXES，wedb/wnode/src/resp/txn_resp_commands.rs const RESP_ERR_TRANSACTION_FAILED，wedb/wnode/src/resp/objects/object_store_utils.rs const RESP_ERR_CORRUPT_PAYLOAD
c#：garnet/libs/server/Resp/CmdStrings.cs 相关常量段
动作：迁入 wresp 单点或注明本文件独有用。

14. 信封编解码六函数一字节事
问题：obj_encode 与 obj_encode_custom 与 obj_encode_into 与 obj_encode_custom_into 加 obj_decode 与 obj_decode_custom，六函数只做一字节标签压栈与校验。
rust：wedb/wcol/src/object_payload.rs fn obj_encode fn obj_encode_custom fn obj_encode_into fn obj_encode_custom_into fn obj_decode fn obj_decode_custom
c#：garnet/libs/server/Objects/Types/GarnetObjectSerializer.cs fn Serialize fn DeserializeInternal
动作：收敛为 encode_into 与 decode 两入口，其余转调或删除。

15. 结果四枚举各说各话
问题：ObjLoad 四态在 wcol，StoreResult 在 wkv 会话，Rmw 在 wnode 对象层，三者映射靠口头约定。
rust：wedb/wcol/src/object_payload.rs enum ObjLoad，wedb/wkv/src/session/mod.rs 相关 StoreResult，wedb/wnode/src/resp/objects/rmw_helpers.rs 相关 Rmw
c#：garnet/libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs 相关 GarnetStatus 段
动作：映射表写一处注释，或统一为一枚举加 Degrade 扩展。

16. 元布局常量半公开
问题：SIZE_OFFSET 与 META_VALUE_SIZE 公开，TYPE_OFFSET 与 NEXT_EXPIRY_OFFSET 与 U64_LEN 私有，外部直读 size 却无法直读 type。
rust：wedb/wval/src/meta.rs const SIZE_OFFSET const META_VALUE_SIZE const TYPE_OFFSET const NEXT_EXPIRY_OFFSET const U64_LEN，fn MetaValue::read_size fn read_collection_type fn from_slice
c#：garnet/libs/server/Objects/Types/GarnetObjectType.cs 相关布局段
动作：偏移量同级公开或全私有只留读函数。

17. 集合阈值与哑值散两文件
问题：四阈值加 should_promote 与 should_demote 在 wcol lib.rs，LIST_SEQ_BASE 在 types/garnet_object.rs，SET_MEMBER_DUMMY_VALUE 在 lib.rs，无统一 constants 面。
rust：wedb/wcol/src/lib.rs const TIERED_PROMOTE_THRESHOLD const TIERED_DEMOTE_THRESHOLD const TIERED_PROMOTE_BYTES const TIERED_DEMOTE_BYTES const SET_MEMBER_DUMMY_VALUE，wedb/wcol/src/types/garnet_object.rs const LIST_SEQ_BASE
c#：garnet/libs/server/Objects 相关阈值段
动作：并入一 constants 模块或注明分属。

18. 巨文件待拆分
问题：resp_server_session.rs 3172 行上帝会话，tiered_collection_ops.rs 2237 行上帝分层，set_commands.rs 1432 行，garnet_api slow.rs 1101 行，wkv vdb.rs 1495 行，ns_codec.rs 779 行。
rust：wedb/wnode/src/resp/resp_server_session.rs，wedb/wnode/src/resp/objects/tiered_collection_ops.rs，wedb/wnode/src/resp/objects/set_commands.rs，wedb/wnode/src/resp/garnet_api/slow.rs，wedb/wkv/src/vdb.rs，wedb/wval/src/ns_codec.rs
c#：garnet/libs/server/Resp/RespServerSession.cs，garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs，garnet/libs/server/Resp/Objects/SetCommands.cs
动作：会话按命令族拆模块，分层按 hash set zset list 拆臂，vdb 按映射与 GC 拆。

19. wcol 反向依赖 wresp
问题：对象层 wcol Cargo 直接依赖 wresp 取 ObjectOutput 与 RespInputFlags，C# Garnet.server 不引用 modules 的分层被倒置。
rust：wedb/wcol/Cargo.toml dependencies wresp，wedb/wcol/src/resp 相关输出类型
c#：garnet/libs/server/Garnet.server.csproj 对 modules 的不引用关系
动作：输出类型上移到 wresp 或 wnode，wcol 只留纯对象。

20. garnet_api 四文件边界模糊
问题：mod.rs 575 行 trait 加投影，raw.rs 540 行快路径，slow.rs 1101 行慢路径，objects.rs 271 行对象臂，快慢与对象三切面正交。
rust：wedb/wnode/src/resp/garnet_api/mod.rs，wedb/wnode/src/resp/garnet_api/raw.rs，wedb/wnode/src/resp/garnet_api/slow.rs，wedb/wnode/src/resp/garnet_api/objects.rs
c#：garnet/libs/server/Resp/RespServerSession.cs 相关分派段
动作：按 raw slow objects 三执行域各自收敛 trait，mod 只留装配与投影。

已确认单点无需动
wval tag.rs KeyTag 与 GarnetObjectType 与 CustomObjectType 三枚举一处定义。wkv session/mod.rs session_prefix 与 with_prefix 三口径合一。wbase time now_ticks 与 convert utc_now_ticks 时间单源。wresp ext null 与 error 版本分派单源。


