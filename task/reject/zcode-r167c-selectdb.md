拒绝结论：判净（核对 C# DatabaseManager 与 GarnetServer，64位大整数库ID、多租户前缀注入与纯净剥离、SWAPDB 单元格 O(1) 换号、MULTI 事务切库与 Lua 恢复、AUTH 重新认证后状态机闭环，全链路无缺陷）

SELECT 虚拟库隔离与多租户物理前缀注入全路径审查报告

一、审查视角与背景说明
审查视角：SELECT 虚拟库隔离与多租户物理前缀注入（64位库ID、多租户前缀注入与剥离一致性）
核查目标与范围：
1. 64位库ID架构对齐与边界约束：RESP 命令层输入解析、int32 与 u64 分层设计、max_databases 上下界收口、集群模式（Cluster Mode）多库演进。
2. 多租户物理前缀注入与剥离一致性：NamespaceDbCodec 编解码、会话前缀 SessionPrefixBuf、KeyTag 标签系统、SCAN / KEYS / DBSIZE / COUNTKEYSINSLOT / GETKEYSINSLOT 键提取与纯净剥离、RANDOMKEY 两侧一致性。
3. 虚拟库换号与库级生命周期管理：SWAPDB 槽位路由单元格 O(1) 置换、DbMeta 原子持久化、FLUSHDB / FLUSHNS / FLUSHALL 换代隔离。
4. 事务与 WATCH 交叉一致性：MULTI 事务窗内 SELECT 命令排队与拒绝门禁、切库成功提交点 invalidate_watch_on_db_switch 注销 WATCH 位点。
5. 冷库按需异步懒加载：ColdContextPending 状态机挂起、park_cold_context_load 与 SlowWait 闭环、materialize_into 延迟物化与物理域撕裂防御。
6. Lua 脚本与认证身份切换隔离：脚本执行后外层 active_db_id 恢复闭环、AUTH 跨租户重新绑定与事务清理。
7. 对标 C# Garnet SingleDatabaseManager、MultiDatabaseManager、DatabaseManagerBase、RespServerSession、ArrayCommands、TxnRespCommands 契约。
8. 核验 doc/zh/db.md、doc/zh/deviations.md 在册条款，杜绝将既定架构改良报为缺陷。

二、原型行为与对标核查（C# Garnet 事实确证）
1. C# 官方契约与源码现状
对应 c# 文件与函数：
garnet/libs/server/Resp/Parser/SessionParseState.cs:SessionParseState.TryGetInt
garnet/libs/server/Resp/Parser/ParseUtils.cs:ParseUtils.TryReadInt
garnet/libs/common/RespReadUtils.cs:RespReadUtils.TryReadInt32Safe
garnet/libs/server/Resp/ArrayCommands.cs:ArrayCommands.NetworkSELECT
garnet/libs/server/Resp/ArrayCommands.cs:ArrayCommands.NetworkSWAPDB
garnet/libs/server/Resp/RespServerSession.cs:RespServerSession.TrySwitchActiveDatabaseSession
garnet/libs/server/Resp/RespServerSession.cs:RespServerSession.SwitchActiveDatabaseSession
garnet/libs/server/Databases/SingleDatabaseManager.cs:SingleDatabaseManager.TryGetOrSetDatabaseSession
garnet/libs/server/Databases/MultiDatabaseManager.cs:MultiDatabaseManager.TryGetOrSetDatabaseSession
garnet/libs/server/Databases/MultiDatabaseManager.cs:MultiDatabaseManager.TrySwapDatabases
garnet/libs/server/Databases/DatabaseManagerBase.cs:DatabaseManagerBase.GetDatabase
garnet/libs/server/Transaction/TxnRespCommands.cs:TxnRespCommands.ProcessTransactionalCommand
garnet/libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:ArrayKeyIterationFunctions.DbScan
garnet/libs/host/Configuration/Options.cs:Options.GetServerOptions

核查确证事实：
1) 线面输入解析与错误码分档：
在 C# Garnet NetworkSELECT 中，使用 parseState.TryGetInt 解析数据库下标。若解析失败（非纯整数字符、空参数或超出 int32 范围，如 3000000000），返回 GenericErrValueIsNotInteger（-ERR value is not an integer or out of range\r\n）；若成功解析但数值为负（index < 0），或数值大于等于 opts.MaxDatabases（默认 16，配置约束为 1 至 256），则返回 ErrDbIndexOutOfRange（-ERR DB index is out of range\r\n）。NetworkSWAPDB 对第一个和第二个库参数分别返回 InvalidFirstDbIndex（-ERR invalid first DB index\r\n）与 InvalidSecondDbIndex（-ERR invalid second DB index\r\n）。
2) 集群模式限制：
在 C# Garnet RespServerSession 中，若开启集群模式（clusterMode）且不是副本节点，SELECT 命令遇到 index != 0 时直接返回 GenericErrSelectClusterMode（-ERR SELECT is not allowed in cluster mode\r\n），即 Garnet 原生集群完全禁止多库。
3) 会话切库与事务管理器换任：
C# Garnet 每库独立实例化 GarnetDatabase、TransactionManager 与 WatchVersionMap。在 RespServerSession.SwitchActiveDatabaseSession 中，底层库切换成功后，会话整体替换 txnManager 为新库的 TransactionManager。旧库上注册的 WATCH 状态随旧管理器解除绑定。
4) MULTI 事务内 SELECT 限制：
在 C# Garnet TxnRespCommands.ProcessTransactionalCommand 中，若处于 MULTI 事务排队阶段，执行 parseState.TryGetInt(0, out var index)。若 index != activeDbId，直接调用 AbortTransaction 并返回 ErrSelectInTxnUnsupported（-ERR SELECT inside MULTI is not allowed\r\n）；若 index == activeDbId，作为相同库 no-op 放行并入队。
5) 库隔离物理模型：
C# Garnet 在 MultiDatabaseManager 下为每个库创建完全独立的 Tsavorite 存储实例（GarnetDatabase），依靠实例间物理内存隔离实现分库，键名内部无需注入库前缀；但也导致跨库无法共享内存缓冲与索引，SWAPDB 需进行实例指针互换。

三、工程现状确证（Rust wedb 实现核查）
1. 模块结构与核心实现路径
对应 rust 文件与函数：
wedb/wbase/src/num.rs:parse_db_index
wedb/wbase/src/num.rs:strict_i32
wedb/wbase/src/num.rs:strict_i64
wedb/wresp/src/check_args.rs:parse_db_index_arg
wedb/wnode/src/resp/array_commands.rs:RespServerSession::network_select
wedb/wnode/src/resp/array_commands.rs:RespServerSession::network_swapdb
wedb/wnode/src/resp/resp_server_session/core.rs:RespServerSession::try_switch_active_database_session
wedb/wnode/src/resp/resp_server_session/core.rs:RespServerSession::invalidate_watch_on_db_switch
wedb/wnode/src/resp/resp_server_session/core.rs:ColdContextPending::materialize_into
wedb/wnode/src/resp/resp_server_session/pump.rs:RespServerSession::park_cold_context_load
wedb/wnode/src/resp/resp_server_session/pump.rs:RespServerSession::resolve_slow_wait_into
wedb/wnode/src/resp/resp_server_session/auth.rs:RespServerSession::apply_authenticated_handle
wedb/wnode/src/resp/resp_server_session/auth.rs:RespServerSession::materialize_authenticated_handle
wedb/wnode/src/resp/resp_server_session/lua.rs:RespServerSession::run_lua_command
wedb/wnode/src/resp/resp_server_session/lua.rs:RespServerSession::resume_suspended_script
wedb/wnode/src/resp/txn_resp_commands.rs:process_transactional_command
wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs:StorageSession::active_user_key_at
wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs:StorageSession::live_key_at
wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs:StorageSession::scan_cursor
wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs:StorageSession::db_keys
wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs:StorageSession::db_size
wedb/wval/src/ns_codec.rs:NamespaceDbCodec::encode_tagged_key
wedb/wval/src/ns_codec.rs:NamespaceDbCodec::encode_with_session_prefix
wedb/wval/src/ns_codec.rs:NamespaceDbCodec::strip_session_prefix
wedb/wval/src/ns_codec.rs:NamespaceDbCodec::extract_live_user_key
wedb/wval/src/ns_codec.rs:SessionPrefixBuf::new
wedb/wval/src/tag.rs:KeyTag::is_user_visible
wedb/wkv/src/session/mod.rs:StoreSession::set_active_db
wedb/wkv/src/session/mod.rs:StoreSession::session_prefix
wedb/wkv/src/session/mod.rs:StoreSession::session_logical_prefix
wedb/wkv/src/session/swap.rs:StoreSession::swap_databases
wedb/wkv/src/vdb/routing.rs:DbRoutingTable::swap_virtual_id
wedb/wkv/src/store/vdb_load.rs:load_routes_of_vns

核查确证事实：
1) 64 位库 ID 与线面 32 位整型分层设计：
线面严格对标 C# int32 契约：parse_db_index 在 wbase/src/num.rs 中通过 strict_i32 先行校验语法与值域，超 int32 字面量（如 3000000000）返回 DbIndexError::NotInteger，对应 RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER；int32 域内负数返回 DbIndexError::OutOfRange，对应 RESP_ERR_DB_INDEX_OUT_OF_RANGE；大于等于 max_databases（配置范围 1 至 256）返回 RESP_ERR_DB_INDEX_OUT_OF_RANGE。内部标量（active_db、active_vdb、namespace、active_vns）全线升级为 64 位（AtomicU64 / u64），底层虚拟库路由 DbRoutingTable 全链路支持 64 位扩展，既保证协议层 100% 对齐 Garnet，又消灭内部 32 位截断溢出风险。
2) 集群模式原生多虚拟库路由：
对标 doc/zh/db.md 4.1 节与 deviations.md，wedb 突破了 Garnet 单机多库、集群禁多库的局限。在集群模式下，通过 slot_of(namespace, db) 进行库级分片映射（Slot = Mixer(namespace, db) & 0x3FFF）。当执行 SELECT 切换到非本节点负责的槽位时，通过集群门禁抛出 MOVED 重定向，在集群模式下原生支持安全的多虚拟库隔离。
3) 多租户物理前缀注入收敛与性能优化：
底层单存储共享模型下，所有键采用前缀隔离：[NsVarint][DbVarint][KeyTag: 1B][Payload]。会话初始化或切库时，SessionPrefixBuf 预计算 2 至 18 字节的前缀切片。所有会话读写操作（BatchStoreSession、StorageSession）统一经由 session_tag_key 或挂接 session_prefix() 注入物理前缀，消除重复计算；物理键编码前缀按命令边界重算，批量命令外提前缀，保证高吞吐。
4) 全键空间扫描前缀剥离与纯净性：
在 ArrayKeyIterationFunctions 中，SCAN、KEYS、DBSIZE、COUNTKEYSINSLOT 等全库遍历链路统一调用 NamespaceDbCodec::extract_live_user_key 与 strip_session_prefix。剥离算法基于 SIMD 内存前缀匹配，并由 KeyTag::is_user_visible（仅放行 String、Meta、ObjectEnvelope）过滤底层 TTL 旁路记录、向量子键、ACL 规则和 DbMeta 元数据。返回给客户端的键名纯净完整，无任何内部前缀泄漏或元记录污染。
5) RANDOMKEY 两侧一致性确证：
C# Garnet 官方代码库与命令清单中均未实现 RANDOMKEY 命令（API 兼容性表格标注 ➖）。Rust wedb 同样未实现，在 wnode/src/resp/key_admin_commands/mod.rs 头注明确记录两侧一致无。
6) SWAPDB 零搬移原子置换与集群互斥：
StoreSession::swap_databases 通过槽位级单元格路由表（ArcSwap<u64>）在 O(1) 复杂度内互换两库的虚拟数据库 ID，免去任何键值搬迁。操作全程持有 lock_dbmeta 元数据串行锁，并向 RocksDB 伴随盘追加 0x06 成对互换记录与 0x02 DbMeta 映射，支持崩溃恢复。在集群模式下，network_swapdb 强校验两库槽位是否均属于当前本地节点且处于 Stable 态，严禁在 MIGRATING/IMPORTING 窗口内换号。
7) 事务与切库 WATCH 作废闭环：
在 MULTI 事务窗内，txn_resp_commands.rs 严格对标 C# TxnRespCommands.cs:163：异库 SELECT 立即报错 RESP_ERR_SELECT_IN_TXN_UNSUPPORTED 并中止事务；同库 SELECT 作为 no-op 排队放行。当独立连接执行 SELECT 切库成功时，try_switch_active_database_session 与 materialize_into 统一调用 invalidate_watch_on_db_switch，注销该会话在旧库上登记的全部 WATCH 位点，彻底杜绝跨库事务误杀与锁残留（deviations §131）。
8) 冷库异步加载与物化防护：
当切入未装载的冷库时，try_switch_active_database_session 不提前修改 active_db_id，而是将上下文存入 ColdContextPending，通过 park_cold_context_load 挂起。在 resolve_slow_wait_into 收到异步装载成功应答后，才原子物化标量并作废旧库 WATCH。若磁盘加载失败，暂存载荷直接丢弃，会话标量严格保留旧库，杜绝标量与底层存储域发生撕裂。
9) Lua 与 AUTH 隔离状态机：
在 run_lua_command 与 resume_suspended_script 中，执行脚本前暂存外层 outer_active_db_id，执行结束后无条件恢复该库号，阻断脚本内 redis.call('SELECT') 篡改外部连接状态。在 AUTH 跨租户认证成功后，若租户变更，materialize_authenticated_handle 主动清理旧租户订阅、清空旧租户事务与 WATCH 容器，并把新租户与当前库号绑定至存储上下文。

四、核查视角规约确证与偏离对齐
1. 架构改良在册性核验：
1) doc/zh/deviations.md 第 424 行：两层设计规约。线协议层兼容 Redis / Garnet 32 位整型（parse_db_index 返回 DbIndexError::NotInteger / OutOfRange 映射至 Garnet 对应错误码）；内部虚拟库 ID、命名空间全线升级为 64 位（active_vdb: AtomicU64 等），彻底消除 32 位溢出隐患。
2) doc/zh/deviations.md 第 131 条：SELECT 切库主动注销在途 WATCH 登记，对齐 C# 换库事务管理器换任语义，彻底清除跨库误杀与跨库锁残留；注记回原库不复活。
3) doc/zh/deviations.md 第 32 条：strict_i32 拒绝前导零（如 007），为 Rust 严格文法收口。
4) doc/zh/deviations.md 第 48 条：多租户单存储架构下，物理键采用命名空间 + 库编号变长前缀编码（方案 A），消除 Garnet 每库独立 Tsavorite 实例带来的内存放大。
5) doc/zh/deviations.md 第 75 条：集群模式原生支持多数据库分片（通过 slot_of(ns, db) 定槽），消除 Garnet 集群仅支持 db 0 的历史局限。
6) doc/zh/db.md 第 1、2、4 节：虚拟数据库路由表、O(1) 换号机制、DbMeta 落盘协议与集群定槽规范。

五、现有专项回归测试网核验
全仓已建立多套高强度回归测试，全面覆盖虚拟库隔离、切库与前缀剥离：
1. wedb/wnode/tests/select_switch_db_invalidates_watch.rs (184 行)：覆盖异库切库注销旧库 WATCH 位点、同库切库保留 WATCH、切库后写旧库不误杀新库事务。
2. wedb/wnode/tests/array_commands_tests.rs：覆盖 SELECT 参数个数、负数越界、大数越界、非整数字符串错误码精细校验。
3. wedb/wnode/tests/keyspace_commands.rs：覆盖多库并发写入下 KEYS、SCAN、DBSIZE 物理前缀精确隔离与剥离，断言返回键名无前缀、内部 Meta/TTL 键不透传。
4. wedb/wnode/tests/cluster_select.rs：覆盖集群模式下跨槽 SELECT 触发 MOVED 重定向、本节点负责槽位正常切库。
5. wedb/wkv/tests/store_vdb_tests.rs：覆盖多租户下 SWAPDB 单元格 O(1) 换号、DbMeta 原子落盘与冷启动路由表恢复。

六、结论总结
经对 C# Garnet 原型与 Rust wedb 仓内全链路（RESP 线面解析、64位内部架构、多租户前缀注入与 SIMD 剥离、SCAN/KEYS 键空间纯净性、SWAPDB 单元格换号与集群门禁、MULTI/WATCH 事务安全、冷库懒加载状态机、Lua 库号复原）逐项审查与交叉确证：
1. 线面 32 位兼容与内部 64 位宽架构分层清晰，无截断或溢出风险；
2. 物理前缀注入与剥离链路完整闭环，无内部标签或租户前缀泄露；
3. 集群多库演进与切库注销 WATCH 等改良项均已在 deviations.md 与 db.md 规范完备登载；
4. 冷库挂起与 Lua/AUTH 状态恢复机制严密，无上下文撕裂风险。
全链路闭环，无缺陷、无脱节。

视角结论:已穷尽
