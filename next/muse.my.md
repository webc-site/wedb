review-my 待办：自定义优化上下游打通与正确高效优雅

check.js 现状
重复 23 组中本域相关 5 组多为跨层调用误报，实现缺失 3 项无本域真缺口。本域零真缺失，问题集中在分层双写非原子、计数口径分裂、类型标签擦除、前缀与零拷贝未全收敛。

1. 升阶双写非原子，崩溃留双态残留
问题：promote 先落 Meta 再删信封两条独立物理写，中间崩溃或删信封 IO 失败即留信封旧快照加 Meta 存根加树三件套，删空后回落信封域幽灵复活。现靠排空单点信封幂等墓碑兜底，非原子。
rust：wedb/wkv/src/range_index/stub.rs fn promote_collection_to_bftree，wedb/wkv/src/range_index/stub.rs fn handle_bftree_drain_and_delete，wedb/wkv/src/session/collection.rs fn delete
c#：libs/server/Storage/Functions/ObjectStore/VarLenInputMethods.cs fn GetRMWModifiedFieldInfo，libs/server/Storage/Functions/ObjectStore/RMWMethods.cs fn InPlaceUpdaterWorker
动作：收敛单条原子换域或先墓碑信封再落 Meta，删信封禁静默吞错。

2. 降阶只有后台懒降阶，体积维预筛缺失
问题：日常删除不触发降阶正确，删空自愈正确。但降阶要 count 加 heap_bytes 双维，后台预筛只走 meta.size 单维，体积维靠物化后判定，大体积小条目树永不降阶。tiered_demote 头注自称双维单点与实现矛盾。
rust：wedb/wcol/src/lib.rs fn should_demote，wedb/wcol/src/types/garnet_object.rs fn should_demote，wedb/wnode/src/resp/objects/tiered_demote.rs fn tiered_materialize_blob
c#：无对位，分层为自研扩展，对标 doc/zh/collection.md 3.2 3.3
动作：预筛补体积水位或注释写清体积维只在物化后判定。

3. RIPROMOTE RIRESTORE 无命令无枚举，文档与实现脱钩
问题：collection.md 称存根句柄经 RIPROMOTE RIRESTORE 保序，实则 command.rs 无此两枚举，解析表无条目，恢复靠 acquire_tree_read 惰性自愈。文档原语名全仓零实现。
rust：wedb/wresp/src/command.rs enum RespCommand，wedb/wkv/src/range_index/stub.rs fn acquire_tree_read，wedb/wnode/src/resp/parser/command_table.rs const PRIMARY_TABLE
c#：libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs fn PromoteToTail
动作：文档改称惰性恢复自愈，或补内部原语名与调用链。

4. 计数三口径分裂，内存 count 带副作用非只读
问题：信封 4B 头 count_of_blob 与分层 MetaValue.size 两 O1 快道正确。但 wcol 内存 count 为 mut 自毁 purge，先 delete_expired_items 再 len，与 IGarnetObject::count 只读契约同名不同义，升阶用 raw len 对外用 purge 版易误用。
rust：wedb/wcol/src/object_payload.rs fn count_of_blob，wedb/wnode/src/resp/objects/object_store_utils.rs fn obj_length_sync fn obj_length_async，wedb/wcol/src/zset/sorted_set_object.rs fn count，wedb/wcol/src/hash/hash_object.rs fn count
c#：libs/server/Objects/SortedSet/SortedSetObject.cs fn Count，libs/server/Objects/Hash/HashObject.cs fn Count
动作：拆 raw_len 只读与 live_count 两口，命令面只走 O1 快道。

5. RI.COUNT 单点正确，区间计数靠注释防误用
问题：range_index_count 只读 size 不触树不加锁，RI.LEN 归一同一枚举正确。区间计数靠 SCAN RANGE FIELDS KEY 投影，若误取 size 即错值，现靠注释约束无类型隔离。
rust：wedb/wkv/src/range_index/ops.rs fn range_index_count，wedb/wnode/src/resp/rangeindex/resp_server_session_range_index.rs fn network_ri_count，wedb/wresp/src/command.rs enum RespCommand::Ricount
c#：无对位本仓扩展，区间侧对标 libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs fn NetworkRiScan fn NetworkRiRange
动作：区间入口加断言禁调 range_index_count，或保持注释约束。

6. GarnetObjectType.RangeIndex 等于 5 自造分叉
问题：wval 自称 1比1对标 C# GarnetObjectType.cs，实则 C# 只有 Null SortedSet List Hash Set All，无 RangeIndex。TYPE 把 rangeindex 当类型串回显分叉，Meta 借对象枚举判树态混用。
rust：wedb/wval/src/tag.rs enum GarnetObjectType fn as_str fn from_u8，wedb/wnode/src/resp/array_commands.rs fn network_type
c#：libs/server/Objects/Types/GarnetObjectType.cs enum GarnetObjectType
动作：注释改本仓扩展，或拆存储形态与对象类型两枚举。

7. 自定义标签解析层擦为 u8 退回运行时比串
问题：清单 CUSTOM_OBJECT_ENTRIES 单点正确，但 parser 存为 u8 object_tag，消费侧 obj_decode_custom 按 u8 比对，与强类型双轨。custom_object_type_name 线性扫描仍运行时比串。
rust：wedb/wnode/src/resp/custom_objects.rs fn custom_object_type_name fn match_custom_object_command，wedb/wcol/src/object_payload.rs fn obj_decode_custom fn obj_encode_custom，wedb/wnode/src/resp/resp_server_session.rs struct SessionParseState field object_tag
c#：libs/server/Custom/CustomCommandManager.cs fn MatchCustomCommand，libs/server/Resp/RespServerSession.cs fn NetworkCustomObjCmd
动作：清单 tag 保持强类型，解析槽存索引，消 u8 臂。

8. ObjectOutput 自带中转 Vec 违零拷贝
问题：ObjectOutput.payload 为 Vec 中转，20 处构造先写中转再拷出。点查 read_batch_with 与 read_tag_with 切片借用已落地，对象命令面仍走中转。
rust：wedb/wcol/src/resp/output.rs struct ObjectOutput field payload，wedb/wkv/src/session/raw/batch.rs fn read_batch_with，wedb/wnode/src/storage/session/storage_session.rs fn read_tag_with
c#：libs/server/Objects/Types/ObjectOutput.cs struct ObjectOutput
动作：operate 改直写会话缓冲，或注明中转必要代价。

9. 批量单次折叠半边落地，写批量缺漏斗
问题：升阶 bulk_load 栈上排序单次借用单次 size 回写正确，MGET 读批量 read_batch_with 前缀外提正确。但通用 run_sync_rmw 逐命令取锁，tree_put_batch 仅分层一臂，写批量缺 enter_batch 单纪元漏斗。
rust：wedb/wkv/src/range_index/stub.rs fn promote_collection_to_bftree，wedb/wnode/src/resp/objects/tiered_collection_ops.rs fn tree_put_batch，wedb/wkv/src/session/raw/batch.rs fn read_batch_with，wedb/wkv/src/session/mod.rs fn enter_batch
c#：libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs fn ContextReadWithPrefetch，libs/server/Resp/MGetReadArgBatch.cs
动作：写批量补单纪元多写漏斗，或注释写清仅读批量与升阶享折叠。

10. 前缀外提只读批量做到，写面逐次拼前缀
问题：SessionPrefixBuf 栈上 19B 零堆正确，读批量单次外提正确。但 service 入账与向量 registry_key 高频逐次拼前缀，MSET 每键一次 varint 重算。隔离正确缺外提。
rust：wedb/wval/src/ns_codec.rs struct SessionPrefixBuf，wedb/wnode/src/service.rs fn enqueue_raw，wedb/wnode/src/resp/vector/vector_manager_locking.rs fn registry_key
c#：无对位自研，前缀对标 libs/server/Storage/Session/StoreSession 前缀拼接
动作：批量写面单次取 prefix_slice 透传 with_prefix 系列。

11. WATCH 栅栏后台清除漏推进
问题：WatchHook 经 StoreSession 注入 wtxn 表，显式写删推进正确，分层 drain 显式推进正确。但 TTL purge_expired 与 GC sweep 物理清除不推进，WATCH 客户端漏感知。
rust：wedb/wkv/src/session/mod.rs type WatchHook，wedb/wtxn/src/watch_version_map.rs struct WatchVersionMap，wedb/wkv/src/session/raw/write/append.rs fn delete_raw，wedb/wkv/src/ttl.rs fn purge_expired
c#：libs/server/Transaction/WatchVersionMap.cs fn IncrementVersion，libs/server/Storage/Functions/ObjectStore/RMWMethods.cs fn PostInitialUpdater fn InPlaceUpdaterWorker
动作：后台清除补推进或注释写清 WATCH 只盯显式写。

12. 虚拟换号打通，回放把物理域当逻辑域二次映射
问题：物理键 NsVarint DbVarint KeyTag Payload 正确，会话 u64 标量正确，FLUSHDB 换格 SWAPDB 双格原子正确，FLUSHALL 广播正确。从库回放把条目物理 vns vdb 当逻辑域调 set_context flush_database 二次映射，与 db.md 不二次映射矛盾。
rust：wedb/wval/src/ns_codec.rs struct NamespaceDbCodec，wedb/wkv/src/vdb.rs fn flush_database fn flush_namespace，wedb/wkv/src/session/swap.rs fn swap_database，wedb/wnode/src/aof/aof_processor.rs fn replay_flush
c#：libs/server/Databases/DatabaseManagerBase.cs fn FlushDatabase，libs/server/AOF/AofProcessor.cs fn ReplayOp fn SwitchActiveDatabaseContext
动作：回放改 set_virtual_context 直设物理域。

13. 库级定槽正确，迁移 sketch 仍按 key 哈希易误读
问题：slot_of 按 ns db 整数混合定槽废除 CRC16 正确。但 sketch 按 key 字节哈希分槽，注释称仅迁移去重不参与定槽，两哈希域分叉易误为键哈希回潮。
rust：wedb/wbase/src/hash_slot.rs fn slot_of，wedb/wedb/src/server/migration/sketch.rs fn probe_with_seed，wedb/wedb/src/server/migration/migrate_driver/keys.rs fn collect_keys
c#：libs/cluster/Utils/HashSlotUtils.cs fn GetSlot，libs/cluster/Server/Migration/Sketch.cs
动作：头注写清 sketch 不参与定槽，或改槽位单元素传入。

14. ACL 点查正确，活连接不刷新与 LIST 无快照
问题：KeyTag::Acl 落盘 User bitcode 正确，全局无大字典正确，连接本地 Arc UserHandle 正确，AUTH 缺省走会话租户正确。但 SETUSER 不刷新已在线句柄权限滞后，LIST 两遍扫描无快照并发可漂移。
rust：wedb/wnode/src/resp/acl_store.rs fn set_user fn delete_user，wedb/wacl/src/user_handle.rs struct UserHandle，wedb/wacl/src/user.rs struct User，wedb/wnode/src/resp/acl_commands.rs fn network_acl_setuser fn network_acl_list
c#：libs/server/ACL/AccessControlList.cs，libs/server/Resp/ACL/ACLCommands.cs fn NetworkAclSetUser fn NetworkAclList
动作：SETUSER 后广播刷新或版本失效重查，LIST 加代数校验。

15. u128 内部二进制正确，协议 hex 与文件 base32 双轨
问题：内部 u128 纯二进制正确，文件名 base32 26 字符正确，hash128 显式种子正确。但 gossip 复制协议渲染 32 字符 hex 传 id，每心跳编解码一次分配。
rust：wedb/whasher/src/lib.rs fn hash128，wedb/wbase/src/base32.rs fn encode_u128，wedb/wbase/src/hex.rs fn hex_str_u128，wedb/wbftree/src/manager/mod.rs fn hash_prefix，wedb/wcpr/src/meta.rs fn token_to_base32
c#：libs/server/Resp/RangeIndex/RangeIndexManager.cs fn KeyId，libs/cluster/Server/ClusterConfig.cs 节点 id Guid 串
动作：协议改 base32 定长或注明 hex 仅为 Guid N 兼容。

16. papaya 随机种子与 deterministic 双轨易误拆
问题：map 进程级 OnceLock 随机种子防 DoS 正确，SCAN 续扫走 deterministic 42 保序正确。但 workspace gxhash 开 deterministic 全局，SKILL 写默认随机，新人易摘 feature 致游标漂移。
rust：wedb/wbase/src/map.rs fn seed fn new_concurrent_map fn new_concurrent_set，wedb/Cargo.toml gxhash deterministic 特性
c#：libs/server/ConcurrentDictionary 族无种子对位
动作：SCAN 容器改显式种子去全局依赖，或加编译期断言。

17. bitcode 单格式正确，借用缺 u8 致全 clone
问题：四对象统一 bitcode 无双格式正确。但 bitcode 0.6 借用仅支持 str 不支持 u8，编码侧被迫 owned 全 clone，大集合升阶每条目一次分配，与零拷贝冲突。注释已写约束但无替代。
rust：wedb/wcol/src/hash/hash_object.rs struct HashWire fn to_blob fn from_blob，wedb/wcol/src/zset/sorted_set_object.rs struct SortedSetWire，wedb/wcol/src/list/list_object.rs fn to_blob，wedb/wcol/src/set/set_object.rs fn to_blob
c#：libs/server/Objects/Types/GarnetObjectSerializer.cs fn Serialize fn DeserializeInternal
动作：保持 bitcode，导出改流式编码或注明 owned 最优。

18. parking_lot 收敛，换号锁自研与规范字面差
问题：全仓无 std Mutex RwLock 正确。但换号串行锁实为 AtomicBool 认领加 yield，因 parking_lot 不可跨 await，与 SKILL 字面差易被误改死锁。
rust：wedb/wkv/src/vdb.rs fn flush_database fn swap_database，wedb/wkv/src/session/swap.rs fn swap_database，wedb/wbase/src/group_commit.rs struct GroupCommitPipeline
c#：libs/server/Databases/DatabaseManagerBase.cs fn FlushDatabase
动作：注释写清换号锁不用 parking_lot 原因，SKILL 加例外。

19. sonic_rs 落地正确，pretty 面每应答分配
问题：wext_json 全员 sonic_rs from_slice to_vec，wlua cjson 用 sonic_rs Value，与规范一致。wresp 无 serde_json 残留正确。pretty 每应答分配属性能非正确。
rust：wedb/wext_json/src/json_object.rs fn get fn set，wedb/wlua/src/functions/cjson.rs fn encode fn decode
c#：modules/GarnetJSON/JsonCommands.cs，libs/server/Lua/LuaRunner.Functions.cs fn JsonEncoding fn JsonDecoding
动作：保持，pretty 加缓冲复用可选。

20. coarsetime 分工正确，TTL 未误用粗时钟
问题：实时域 Clock 高精度，单调域 Instant 锚点，TTL 用 now_ticks，慢日志用 now_stopwatch_ticks，粗粒度禁入 TTL 正确。
rust：wedb/wbase/src/time.rs fn now_ticks fn now_stopwatch_ticks，wedb/wkv/src/ttl.rs fn expire_at
c#：libs/common/ConvertUtils.cs，System.DateTimeOffset.UtcNow.UtcTicks
动作：保持。

21. fearless_simd 只收敛键比对，位图未走单点
问题：fast_key_eq 经 Level 令牌多版本正确。但 bit_count 走 u64 count_ones 自动向量化，未经 wbase simd 单点，选型面分两轨。头注已声明差异。
rust：wedb/wbase/src/simd.rs fn fast_key_eq，wedb/wbitmap/src/bit_count.rs fn bit_count，wedb/wbitmap/src/bit_op.rs fn bit_op
c#：libs/server/Resp/Bitmap/BitmapManagerBitCount.cs
动作：位图保持自动向量化，补互指注释。

22. nested_text 单格式正确
问题：wconf 只读 .nt，旧双格式与 Azure 导入已删，CLI 三层合并正确。
rust：wedb/wconf/src/node_options.rs struct NodeArgs，wedb/wconf/src/runtime_server_options.rs struct RuntimeServerOptions
c#：libs/host/GarnetServerOptions.cs，libs/server/Config/ServerSettingsManager.cs fn TryParseCommandLineArguments
动作：保持。

23. lua 快路径拆分致 check 重复
问题：运行时 luau 正确。但 process_command_from_scripting 拆两快道 try_fast_path_set try_fast_path_get 同挂一锚点报重复，实为一入口两快道。
rust：wedb/wlua/src/functions/redis.rs fn process_command_from_scripting fn try_fast_path_set fn try_fast_path_get
c#：libs/server/Lua/LuaRunner.Functions.cs fn ProcessCommandFromScripting
动作：快道去锚点留说明，锚点只留总入口。

24. 向量构造与 HashSet 跨层误报
问题：ensure_cleanup_tasks_started 与 with_vector_set_preview 同挂 VectorManager 构造实为两切面，hash_set 与 tree_put_batch 同挂 HashSet 实为内存写与分层写两层，均非真重复。
rust：wedb/wnode/src/resp/vector/vector_manager_cleanup.rs fn ensure_cleanup_tasks_started，wedb/wnode/src/service.rs fn with_vector_set_preview，wedb/wcol/src/hash/hash_object_impl.rs fn hash_set，wedb/wnode/src/resp/objects/tiered_collection_ops.rs fn tree_put_batch
c#：libs/server/Resp/Vector/VectorManager.cs fn VectorManager，libs/server/Objects/Hash/HashObjectImpl.cs fn HashSet
动作：锚点只留装配与 wcol 一处，切面改挂说明。
