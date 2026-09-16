# gemini.design 待办

1. [P1] 集群迭代槽位校验五处重复定义与双层转发
   位置：wedb/wedb/src/server/cluster_session.rs:229、wedb/wedb/src/server/cluster_session.rs:1181、wedb/wnode/src/cluster_session.rs:196、wedb/wedb/src/server/slot_verify.rs:380、wedb/wedb/src/server/cluster_manager.rs:727
   对标：garnet/libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs:NetworkIterativeSlotVerify
   机制：C# ClusterSession 为 partial class 直接实现 IClusterSession 接口，NetworkIterativeSlotVerify 单点维护 cachedVerificationResult、configSnapshot 与 initialized 状态，首键初始化并验证，后续键遇槽位改变记 CROSSSLOT，状态改变记 TRYAGAIN。
   问题：check.js 扫描出 3 个符号共 12 处重复定义。wedb/src/server/cluster_session.rs 定义了固有方法 network_iterative_slot_verify，随后在 impl ClusterSessionFace 时又包装了一层仅转发固有方法的同名方法；同时 slot_verify.rs:380 (iterative_slot_verify_step)、cluster_manager.rs:727 (evaluate_iterative_key_gate) 以及 wnode/src/cluster_session.rs 的 trait 默认方法均被打上相同 C# 路径注释，造成 5 处符号冲突与双层固有包装。
   方案：
   - 彻底删除 wedb/src/server/cluster_session.rs:229-300 的固有方法，直接在 impl ClusterSessionFace for ClusterSession 中实现 reset_cached_slot_verification_result、network_iterative_slot_verify 与 write_cached_slot_verification_message。
   - 移除 slot_verify.rs 与 cluster_manager.rs 上冒充 C# 入口函数的文档注释，改为 Rust 内部实现辅助说明。
   - wnode/src/cluster_session.rs 的 ClusterSessionFace trait 声明保留契约并取消 C# 路径绑定（因实现落在 wedb::ClusterSession），确保 check.js 扫描单一权威源。

2. [P1] wnode 越层直依赖 wbftree 导致 RangeIndex 双包装
   位置：wedb/wnode/Cargo.toml:42、wedb/wnode/src/aof/aof_processor.rs:70、wedb/wnode/src/service.rs:28、wedb/wnode/src/resp/rangeindex/（8 个文件：range_index_chunked_deserializer.rs、range_index_chunked_serializer.rs、range_index_manager_index.rs、range_index_manager_locking.rs、range_index_manager_migration.rs、range_index_manager_replication.rs、range_index_migration_reader.rs、resp_server_session_range_index.rs）
   对标：garnet/libs/server/Storage/Session/MainStore/RangeIndexOps.cs:ScanRangeIndex 与 libs/server/Resp/RangeIndex/RangeIndexManager.cs
   机制：C# RangeIndexOps 属于 Storage/Session/MainStore 层，底座为独立管理的 RangeIndexManager 实例，RespServerSession 仅通过 IGarnetAdvancedApi 与 StorageSession 进行交互，不直接碰底层数据结构。
   问题：wnode 越过存储引擎门面 wkv 直连底座 wbftree，并在 wnode/src/resp/rangeindex/ 自行封装 8 个文件，而 wkv/src/range_index.rs 已经封装了一套基于 wbftree 的 RangeIndexManager 与操作接口，导致 wnode 绕过 wkv 与 wbftree 双轨依赖，拓扑层次倒挂。
   方案：
   - 跨 crate 依赖流向重构：wnode -> wkv -> wbftree，严禁 wnode 越级依赖 wbftree。从 wnode/Cargo.toml 移除 wbftree 依赖。
   - 将 wnode/src/resp/rangeindex/ 下的 chunked 序列化、反序列化、迁移与复制数据流等底座逻辑全部收敛下沉至 wkv::range_index 模块。
   - wkv::range_index 对外暴露统合接口：ri_set, ri_get_callback, ri_del, ri_len, ri_scan, ri_range，以及迁移块读取与重组接口；wnode 仅保留协议解析与命令派发，彻底解耦底层 BfTree。

3. [P1] DatabaseManagerBase 对象扫描物理键标签与对象类型混淆
   位置：wedb/wdatabase/src/database_manager_base.rs:334-335（execute_object_collection）
   对标：garnet/libs/server/Databases/DatabaseManagerBase.cs:ExecuteObjectCollection
   机制：C# ExecuteObjectCollection 依次调用 ExecuteHashCollect 与 ExecuteSortedSetCollect，通过 StorageSession 在对象存储区按类型扫描。
   问题：Rust 侧 execute_object_collection 在剥离会话前缀 key.strip_prefix(prefix_slice) 后，将随后的第 1 个字节 rest.first() 当成 GarnetObjectType 并校验 (SortedSet..=Set).contains(&tag)。然而 wedb 物理键编码为 [NsVarint] + [DbVarint] + [KeyTag: 1B] + [Payload]，前缀后的首字节是 KeyTag！内存对象信封标签为 KeyTag::ObjectEnvelope (0x0C = 12)，永远不可能在 1..=4 范围内，导致信封存储的集合对象全部漏判；且若匹配到 1，实际对应 KeyTag::Meta，与 SortedSet(1) 仅为数值巧合，语义彻底错乱。
   方案：
   - 严格遵循 NamespaceDbCodec 编解码规范：调用 NamespaceDbCodec::decode_tag(key) 提取 KeyTag。
   - 若 KeyTag == KeyTag::ObjectEnvelope，则从记录负载 payload (rec.value()) 读取第 1 个字节解出 GarnetObjectType::from_u8(val[0])；若 KeyTag == KeyTag::Meta，则通过 MetaValue::from_slice(rec.value()) 读取元数据中的 collection_type。
   - 修正类型过滤逻辑，恢复对象收集计数的正确语义。

4. [P1] COMMITAOF 提交命令通道与异步提交空壳
   位置：wedb/wnode/src/resp/admin_commands.rs:136（commit_aof_async）、:152（network_commitaof）
   对标：garnet/libs/server/Resp/AdminCommands.cs:NetworkCOMMITAOF 与 garnet/libs/server/Storage/Common/StoreWrapper.cs:CommitAOFAsync
   机制：C# NetworkCOMMITAOF 提取可选 dbId，在网络线程通过 BlockingWait 调用 storeWrapper.CommitAOFAsync(dbId)，由底层 appendOnlyFile.CommitAsync 推进 committedUntilAddress 物理持久化，等待落盘完成后响应 +AOF file committed。
   问题：wnode 中 commit_aof_async 为纯空壳直返 Ok(true) 且全仓 0 引用；network_commitaof 甚至不调用 commit_aof_async，直接向输出缓冲无条件写入 +AOF file committed，既不推进 AOF 物理落盘，也不推进 committed_until 水位，命令纯属欺骗性回包。
   方案：
   - 在 StoreGarnetApi（以及底层 DatabaseAof / WaofSublog）中打通物理提交端口 commit_to(safe_tail)。
   - network_commitaof 仿照 SAVE/BGSAVE 机制，构造 SlowWait 慢路径挂起体交由 compio 异步执行域驱动，在后台推进 AOF 物理提交与 committed_until 位点。
   - 提交完成后写出 +AOF file committed\r\n，并补齐端到端测试断言 committed_until 推进。

5. [P1] wresources 未纳入工作区 Cargo.toml 成员管理
   位置：Cargo.toml:3-38（members 列表）、wedb/wresp/Cargo.toml:29、wedb/wnode/Cargo.toml:55
   对标：garnet/libs/resources/Garnet.resources.csproj
   机制：C# Garnet.resources 作为独立程序集，通过 EmbeddedResource 统一内嵌 RespCommandsInfo.json 与 RespCommandsDocs.json，供服务端与测试程序集引用。
   问题：wedb 建立了独立的 wedb/wresources crate 承载两份核心 JSON，wresp 与 wnode 均通过相对路径 ../wresources 依赖，但在工作区根目录 wedb/Cargo.toml 中，workspace.members 遗漏了 wresources，且 workspace.dependencies 亦未声明该 crate，导致工作区拓扑不全，cargo 无法进行统一版本解析与全量静态检查。
   方案：
   - 在 wedb/Cargo.toml 的 [workspace.members] 中追加 "wresources"。
   - 在 [workspace.dependencies] 声明 wresources = { version = "0.1.0", path = "wresources" }。
   - wresp/Cargo.toml 与 wnode/Cargo.toml 依赖统一改为 wresources.workspace = true。

6. [P1] RespCommandsInfo 元数据全表双重反序列化与内存膨胀
   位置：wedb/wresp/src/catalog/mod.rs:15、wedb/wnode/src/resp/resp_commands_info.rs:13
   对标：garnet/libs/server/Resp/RespCommandsInfo.cs
   机制：C# RespCommandsInfo 静态构造时单次反序列化 Garnet.resources 内嵌的 RespCommandsInfo.json，构建一份不可变的全局命令字典与 ACL 分类索引，全进程共享。
   问题：wresp::catalog 与 wnode::resp_commands_info 分别独立引入 wresources::RESP_COMMANDS_INFO_JSON，并在运行时各自通过 sonic_rs 进行反序列化，生成各自独立的 OnceLock 静态表；且 wresp::catalog 为每个命令生成 String 成员名，不仅重复消耗 158KB JSON 解析 CPU，还在堆上冗余分配双份元数据对象。
   方案：
   - 将 RespCommandsInfo.json 的单点反序列化与静态索引构建完全收敛至 wresp::catalog。
   - wresp::catalog 统一对外暴露静态切片与零拷贝结构体（使用 &'static str 与 RespCommand 枚举），提供按名称查询、按枚举索引、ACL 类别与 KeySpec 查询功能。
   - wnode::resp_commands_info 彻底废弃独立反序列化逻辑，转为直接引用 wresp::catalog 导出的权威元数据视图。

7. [P2] 集合算子入参切片化与 ArgSlice 裸指针生命周期解绑
   位置：wedb/wcol/src/resp/input.rs:67（ObjectInput.parse_state）、wedb/wresp/src/argslice/arg_slice.rs:7（ArgSlice 裸指针）、wedb/wcol/src/object_store_utils.rs:25-34
   对标：garnet/libs/server/Objects/Types/GarnetObject.cs:Operate 与 libs/server/InputHeader.cs:ObjectInput
   机制：C# Tsavorite 依赖固定内存指针，以 PinnedSpanByte 包装内存切片传递给 Operate；C# 通过指针偏移解析参数。
   问题：Rust 侧为模拟 C# 机械定义了 ArgSlice { ptr: *const u8, length: usize }，并在 as_slice 中使用 unsafe from_raw_parts 产生解绑生命周期的切片。每当调用集合命令，object_store_utils::make_object_input 都将入参转换为 Vec<ArgSlice>，塞入 SmallVec 分配的 SessionParseState，再包入 ObjectInput 传进 operate，产生大量指针装包、解包与小向量伸缩开销。
   方案：
   - 彻底废除 ArgSlice 裸指针机制与 SessionParseState 在 wcol 中的传递。
   - 重构 ObjectInput：
     pub struct ObjectInput<'a> {
       pub header: RespInputHeader,
       pub arg1: i32,
       pub arg2: i32,
       pub args: &'a [&'a [u8]],
     }
   - GarnetObjectBase::operate 方法签名重构为直接消费 &[&[u8]] 切片视图，实现全链路零堆分配与纯安全 Rust 借用。

8. [P2] 自定义对象命令同步异步执行器双份实现
   位置：wedb/wnode/src/resp/objects/custom_object_commands.rs:35（try_custom_object_command）vs wedb/wnode/src/resp/garnet_api.rs:838（custom_object_slow）
   对标：garnet/libs/server/Custom/CustomRespCommands.cs:TryCustomObjectCommand
   机制：C# TryCustomObjectCommand<TGarnetApi> 通过泛型参数化存储执行者 TGarnetApi（同步会话或慢路径上下文），将装载、NeedInitialUpdate、Updater/Reader、NotFound 与回写/删除统一为同一套状态机模板。
   问题：Rust 侧在 objects/custom_object_commands.rs 编写了一套同步执行器，又在 resp/garnet_api.rs 编写了一套异步执行器 custom_object_slow。两处长达数百行代码重复实现了对象反序列化、字符串键反探 WRONGTYPE、空对象删空回收等 5 个状态流转，维护时改一处漏一处。
   方案：
   - 在 wcustom 中定义通用对象存取适配器 Trait 或在 wnode 抽象执行内核：
     fn execute_custom_object_workflow<L, S, D>(
       load: L, save: S, delete: D,
       obj_cmd: &CustomObjectCommand, fns: &CustomObjectFns,
       args: &[&[u8]], output: &mut Vec<u8>
     ) -> Result<WorkflowOutcome, ()>
   - 将具体的同步 BatchStoreSession 读写与异步 StorageSession 读写作为闭包传入，统一业务流转逻辑，彻底消除双重实现。

9. [P2] SET/SETEX 双跳 AOF 写放大
   位置：wedb/wnode/src/service.rs:204-211、wedb/wnode/src/resp/basic_commands.rs:59-65
   对标：garnet/libs/server/Resp/BasicCommands.cs:533（NetworkSETEX）
   机制：C# NetworkSETEX 将计算得到的截止时间 valMetadata (DateTimeOffset.UtcNow.Ticks + expiryTicks) 直接封入 StringInput(RespCommand.SETEX, 0, valMetadata)，底层存储引擎落盘单条带过期元数据的记录，AOF 仅追加单条 SETEX 记录。
   问题：Rust 当前实现中，SET EX 分两步：先写入普通值记录，随后触发 StoreEvent::TtlWrite 事件，在 service.rs 中再独立追加一条 RespCommand::Pexpireat 的 StoreRMW AOF 记录。在高频 SET 带过期场景下，写放大翻倍，造成巨大的 AOF 刷盘与复制带宽浪费。
   方案：
   - 扩展值操作存储输入，允许将 expiration_ticks 随行编入值记录头/元数据。
   - 当命令为 SETEX/PSETEX 或带 EX/PX 选项的 SET 时，直接生成单条携带元数据的 AOF 条目；StoreEvent::TtlWrite 仅用于响应显式的独立 EXPIRE / PEXPIRE / PERSIST 命令。

10. [P2] 双 main 启动恢复装配逻辑同构重复
    位置：wedb/src/main.rs:106-124 与 wedb_standalone/src/main.rs:122-140
    对标：garnet/libs/host/GarnetServer.cs:Start
    机制：C# 服务端启动时由单一入口根据 GarnetServerOptions 调度恢复流程与会话工厂装配。
    问题：wedb（集群版）与 wedb_standalone（单机版）的 main.rs 中，关于 (node.recover, node.aof) 的四路 match（open_recovered_with_aof, open_recovered, open_with_aof, open）以及后续 requirepass、pubsub_config、runtime_config 的链式装配存在 35 行逐字相同的同构代码。
    方案：
    - 在 wnode::StorageSessionProvider 下新增统一启动装配便利函数：
      pub async fn open_from_node_options<F>(
        options: &NodeOptions, session_factory: F
      ) -> Result<StorageSessionProvider>
    - 将数据路径推导、四路恢复匹配、ACL/PubSub/运行时配置装配收敛为单点，两 main.rs 各简化为单行调用。

11. [P2] 并发字典锁策略碎片化收敛至 ConcurrentMap
    位置：wedb/wvector/src/provider.rs:320-324、wedb/src/server/replication/driver_registry.rs:28、wedb/wnode/src/servers/consumer_registry.rs:209、wedb/wnode/src/resp/vector/vector_manager.rs:169-179
    对标：garnet/libs/server/Storage/Session/ObjectStore/ 与 garnet/libs/cluster/Server/Replication/
    机制：C# 使用 ConcurrentDictionary<TKey, TValue> 实现高并发无锁读与条带化更新，避免全局锁瓶颈。
    问题：Rust 侧缺乏统一并发字典设计，散落大量粗粒度锁包装的哈希表，如 wvector 中的 neighbor_cache/start_point_cache 使用 RwLock<HashMap>，driver_registry 与 consumer_registry 使用 RwLock<HashMap>，vector_manager 更是内嵌了 5 个 Mutex<HashMap>，在多线程并发读写时产生严重的锁争用。
    方案：
    - 严格遵循技能规范，全仓统一收敛至 wbase::map::ConcurrentMap（基于 papaya + gxhash）：
      pub type ConcurrentMap<K, V> = papaya::HashMap<K, V, gxhash::GxBuildHasher>;
    - 替换上述所有 RwLock<HashMap> 与 Mutex<HashMap>，采用无锁并发读取与原子的 pin 作用域写入，杜绝粗粒度读写锁。

12. [P2] 键空间迭代函数散落重复物理标签常量与手动前缀剥离
    位置：wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs:19-25、:371、:434、:480
    对标：garnet/libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs
    机制：C# Tsavorite 遍历底层记录时，统一通过公共迭代器与键过滤器提取逻辑键。
    问题：array_key_iteration_functions.rs 中局部私自定义了 TAG_STRING、TAG_META、TAG_TTL、TAG_ENVELOPE 裸常量，并在 3 处键遍历循环中手工调用 strip_prefix(prefix_slice) 后再执行 strip_prefix(&[TAG_STRING]).or_else(|| rest.strip_prefix(&[TAG_ENVELOPE]))，绕过了权威模块，散落多套前缀剥离逻辑。
    方案：
    - 删除 array_key_iteration_functions.rs 内的 TAG_* 常量定义。
    - 全链路单点接入 wval::NamespaceDbCodec::extract_live_user_key(key, session_prefix)，单次调用同时完成会话前缀比对、KeyTag 提取与用户可见性过滤，彻底消除裸切片操作。

13. [P2] 集群 Worker 与配置高频克隆未引入 hipstr
    位置：wedb/src/server/worker.rs:27（Worker 结构体）、wedb/src/server/cluster_config.rs:33、:1502
    对标：garnet/libs/cluster/Server/Worker.cs 与 ClusterConfig.cs
    机制：C# Worker 节点信息在 gossip、配置变更与故障转移时频繁传递，C# 引用类型指针赋值开销极低。
    问题：Rust 中 Worker 结构体包含大量 String（nodeid, address, replica_of_node_id, hostname）。在集群节点轮询与配置传播时，频繁执行 Worker.clone() 与 ClusterConfig.clone()，触发大量堆内存分配；在序列化时为了切片甚至执行 self.workers.get(1..).unwrap_or(&[]).to_vec() 进行全量克隆。
    方案：
    - 在 wedb crate 引入 hipstr::HipStr（64位平台内联高达 23 字节，超长为只读引用计数，克隆成本 O(1)）。
    - 将 Worker 的 nodeid, replica_of_node_id, address, hostname 全部替换为 HipStr 或固定 40 字节十六进制内联数组。
    - 序列化 wire 结构 ConfigWire 改为借用视图结构 ConfigWire<'a>，消除 to_vec() 堆分配。

14. [P2] whyperlog 稠密稀疏校验无谓引入 BTreeMap
    位置：wedb/whyperlog/src/lib.rs:13、:1067（compare_sparse_to_dense）
    对标：garnet/libs/server/Resp/HyperLogLog/HyperLogLog.cs:CompareSparseToDense
    机制：C# CompareSparseToDense 为测试与调试时使用，在 C# 中构建一个局部字典比对。
    问题：whyperlog 在 lib.rs 顶部引入 std::collections::BTreeMap，并在 compare_sparse_to_dense 中循环 16384 次将全部非零稠密寄存器插入 BTreeMap，产生成千上万个红黑树节点堆内存分配，且违背了除 gxhash 外不乱引集合类型的原则。
    方案：
    - 从 whyperlog 彻底移除 BTreeMap 导入。
    - 重构校验算法：直接线性遍历稀疏 RLE 操作码流，计算当前寄存器下标 offset；遇到非零值时，直接调用 self.get_register(regs, offset) 现场就地比对，时间复杂度 O(N)，辅助空间复杂度 O(1)，零堆分配。

15. [P2] resp_server_session 超四千行巨型文件拆分
    位置：wedb/wnode/src/resp/resp_server_session.rs:1-4249
    对标：garnet/libs/server/Resp/（C# 采用 partial class 拆分为 RespServerSession.cs, RespServerSessionString.cs, RespServerSessionObjects.cs, RespServerSessionAdmin.cs 等）
    机制：C# 以 partial class 机制将巨大的会话类型按职责分拆在多个文件中独立实现。
    问题：单文件长达 4249 行，集成了连接生命周期、网络缓冲读写、字符串命令分派、事务管理、ACL 鉴权，且在 2872 行后内嵌了 1377 行集成测试，维护极其困难。
    方案：
    - 建立模块化目录 wnode/src/resp/resp_server_session/：
      - mod.rs：定义 RespServerSession 结构体核心字段、构造与生命周期状态机。
      - string_cmds.rs：impl RespServerSession 处理 GET, SET, MGET, MSET 等。
      - admin_cmds.rs：处理 AUTH, PING, ECHO, SELECT, QUIT, CONFIG 等。
      - txn_cmds.rs：处理 MULTI, EXEC, DISCARD, WATCH, UNWATCH 等。
      - object_cmds.rs：处理自定义与通用对象分派。
    - 将 2872 行后的 62 个跨 crate 集成测试全部迁入 wnode/tests/resp_server_session_tests.rs。

16. [P2] cluster_session 超两千六百行巨型文件拆分
    位置：wedb/src/server/cluster_session.rs:1-2614
    对标：garnet/libs/cluster/Session/（C# 拆分为 ClusterSession.cs, RespClusterCommands.cs, RespClusterSlotVerify.cs, RespClusterReplicationCommands.cs, RespClusterIterativeSlotVerify.cs 等）
    机制：C# 集群会话通过 partial class 将槽位校验、命令管理、主从复制与迁移解耦在独立子文件中。
    问题：单文件长达 2614 行，集成了槽位校验、集群管理、跨节点复制、迁移状态机等全部逻辑，远超 1500 行规范。
    方案：
    - 就地拆分为模块目录 wedb/src/server/cluster_session/（严格遵照 task/reject/wcluster-premature-split.md 驳回经验，禁止提前拆出 wcluster crate）：
      - mod.rs：ClusterSession 结构体定义及生命周期。
      - slot_verify.rs：单键与多键槽位门控判定、重定向错误消息写出。
      - iterative_verify.rs：迭代槽位校验缓存与事务校验步进。
      - cluster_cmds.rs：CLUSTER NODES, SLOTS, SHARDS, INFO, MEET, FORGET 等命令分派。
      - repl_cmds.rs：REPLICAOF, ASKING, READONLY, READWRITE 等主从命令。
      - migrate_cmds.rs：MIGRATE 槽位迁移控制与状态机驱动。

17. [P2] wlua functions 与 runner 超两千行巨型文件拆分
    位置：wedb/wlua/src/functions.rs:1-2415、wedb/wlua/src/runner.rs:1-2026
    对标：garnet/libs/server/Lua/LuaRunner.Functions.cs 与 LuaRunner.cs
    机制：C# LuaRunner 将宿主函数库通过 partial 类与辅助模块解耦。
    问题：functions.rs 堆积了 redis 宿主 API、bitop 全套位操作、cjson 编解码、cmsgpack 打包解包与 struct 编解码；runner.rs 堆积了 RESP 输出转换、事务锁管理、内存跟踪器与执行生命周期，双双突破 2000 行。
    方案：
    - wlua/src/functions 拆分为 functions/ 目录：
      - mod.rs：模块注册与 C 蹦床分发映射。
      - redis_api.rs：redis.call, redis.pcall, redis.log, redis.sha1hex 等。
      - bitop.rs：位操作函数族（B_NOT, B_OR, B_AND, B_XOR, B_LSHIFT 等）。
      - cjson.rs：基于 sonic-rs 的 JSON 编解码扩展。
      - cmsgpack.rs：Msgpack 打包与解包实现。
      - struct_codec.rs：Lua 格式化结构体编解码。
    - wlua/src/runner 拆分为 runner/ 目录：
      - mod.rs：LuaRunner 核心结构体、编译与调用生命周期。
      - resp_convert.rs：Lua 栈变量与 RESP2/3 协议双向转换逻辑。
      - lock_guard.rs：事务锁资源校验与守卫。
      - context.rs：线程局部 HostShared 上下文管理。

18. [P2] wvector provider 超两千行巨型文件拆分
    位置：wedb/wvector/src/provider.rs:1-2009
    对标：garnet/libs/server/Storage/Session/ObjectStore/VectorManager.cs 与微软官方 provider.rs
    机制：微软官方实现将数据读写、量化推断、图索引遍历解耦在独立子模块中。
    问题：文件长达 2009 行，将 DiskANN 官方底层抽象 DataProvider、动态量化状态机 DynamicQuantization、邻接表与起始点内存缓存、以及持久化回调 Callbacks 全部塞在一个文件中。
    方案：
    - 拆分为 wvector/src/provider/ 模块目录：
      - mod.rs：数据提供者核心门面与类型定义。
      - data_provider.rs：外部 ID 与内部 ID 映射、SetElement 与 Delete 实现。
      - dynamic_quant.rs：全精度与量化双轨检索状态机及剪枝策略。
      - cache.rs：邻居缓存与起始点量化向量缓存。
      - callbacks.rs：底座持久化与版本维护回调。

19. [P2] aof_processor 与 garnet_log 巨型文件拆分
    位置：wedb/wnode/src/aof/aof_processor.rs:1-1862、wedb/wnode/src/aof/garnet_log.rs:1-1819
    对标：garnet/libs/server/AOF/AofProcessor.cs 与 GarnetLog.cs
    机制：C# AofProcessor 关注回放逻辑分派，GarnetLog 专注日志物理 IO 与分片拓扑。
    问题：aof_processor 揉杂了数据条目重放、事务分片重放、检查点标记推进与存储过程；garnet_log 揉杂了单日志/分片多日志路由、背压队列、以及末尾多达 570 行的内嵌测试。
    方案：
    - wnode/src/aof/aof_processor 拆为 aof_processor/ 目录：
      - mod.rs：AofProcessor 结构体与主重放循环。
      - replay_data.rs：字符串、集合对象、范围索引与向量数据重放。
      - replay_txn.rs：单日志与分片事务边界（Commit/Abort）重放。
      - replay_ckpt.rs：快照标记与数据库级别管理条目重放。
      - prepare.rs：键预处理与拓扑时间戳对齐。
    - wnode/src/aof/garnet_log 拆为 garnet_log/ 目录：
      - mod.rs：GarnetLog 门面与公共类型。
      - backend.rs：SublogBackend trait 与内存/物理日志实现。
      - routing.rs：物理子日志与虚拟子日志哈希路由。
      - backpressure.rs：并发写入背压与水位等待。
      - 570 行内嵌测试迁入 wnode/tests/garnet_log_tests.rs。

20. [P2] basic_commands 与 sorted_set_commands 巨型文件拆分
    位置：wedb/wnode/src/resp/basic_commands.rs:1-1830、wedb/wnode/src/resp/objects/sorted_set_commands.rs:1-1762
    对标：garnet/libs/server/Resp/BasicCommands.cs 与 SortedSetCommands.cs
    机制：C# 按命令族群将命令分派实现拆散为细颗粒度的方法集。
    问题：basic_commands 包含 30 余个字符串读写与数值修改命令，sorted_set_commands 包含数十个 ZSet 命令复杂解析，均突破 1700 行。
    方案：
    - basic_commands 拆为 basic_commands/ 目录：
      - mod.rs：命令入口分发与公共错误常量。
      - string_ops.rs：GET, SET, MGET, MSET, APPEND, GETRANGE, SETRANGE, STRLEN 等。
      - numeric_ops.rs：INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT 等。
      - ttl_ops.rs：TTL, PTTL, EXPIRE, PEXPIRE, EXPIREAT, PERSIST 等。
    - sorted_set_commands 拆为 sorted_set_commands/ 目录：
      - mod.rs：ZSet 统一分派。
      - add_rem.rs：ZADD, ZREM, ZINCRBY 等。
      - range_ops.rs：ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZRANGEBYLEX 等。
      - score_rank.rs：ZSCORE, ZMSCORE, ZRANK, ZREVRANK, ZCOUNT, ZLEXCOUNT 等。
      - pop_scan.rs：ZPOPMIN, ZPOPMAX, BZPOPMIN, BZPOPMAX, ZSCAN 等。

21. [P2] whyperlog 与 cluster_config 巨型文件拆分
    位置：wedb/whyperlog/src/lib.rs:1-1575、wedb/src/server/cluster_config.rs:1-1608
    对标：garnet/libs/server/Resp/HyperLogLog/HyperLogLog.cs 与 libs/cluster/Server/ClusterConfig.cs
    机制：C# 将核心结构体、算法实现与线协议格式拆分清晰。
    问题：whyperlog 单文件塞满 1575 行，稠密/稀疏算法与大段单测混杂；cluster_config 单文件 1608 行，包含 64KB 槽位图映射、节点字典原地合并与 bitcode 网络载荷编解码。
    方案：
    - whyperlog 拆为模块化目录：
      - src/lib.rs：HyperLogLog 实例结构体与外部 API。
      - src/sparse.rs：RLE 变长操作码流编码、解码与零段维护。
      - src/dense.rs：6-bit 寄存器数组位操作与稠密表示。
      - src/cardinality.rs：基数估算修正算法与多 HLL 并集合并。
      - 测试拆入 whyperlog/tests/。
    - cluster_config 拆为 wedb/src/server/cluster_config/：
      - mod.rs：ClusterConfig 核心结构体与配置版本。
      - slot_map.rs：16384 槽位状态机与槽位所有权流转。
      - worker_map.rs：本地与远程 Worker 列表管理与合并规则。
      - wire_codec.rs：基于 bitcode 的 ConfigWire 网络线格式编解码。

22. [P2] wnode 源码内嵌千行跨 crate 集成测试迁 tests
    位置：wedb/wnode/src/resp/resp_server_session.rs:2872-4249（62 个测试）、wedb/wnode/src/session_parse_state_extensions.rs（15 个测试）、wedb/wnode/src/aof/garnet_log.rs:1250-1820（11 个测试）
    对标：garnet/test/standalone/Garnet.test/
    机制：C# 严格区分代码工程与测试程序集，libs/server 专注生产逻辑，Garnet.test/ 独立程序集负责组装 ACL、PubSub、事务执行集成测试。
    问题：wnode/src 下多个源文件末尾内嵌重度跨 crate 集成测试（累计超过 2000 行），这些测试强行引入 wacl, wpubsub, wtxn, waof, tempfile 等全家桶依赖，导致任何底层微小变动都会引发整个 src 单元的超重重编。
    方案：
    - 制定内嵌测试留存规范：src 内部仅允许保留针对私有函数、纯算法与状态转移矩阵的轻量无 I/O 单元测试。
    - 所有需构造 RespServerSession、实例化真实存储、组装网络循环与多 crate 协作的集成测试，全部剥离并外迁至 wnode/tests/ 目录，减重 src 编译单元。

23. [P3] wconf 遗留无盘与原生 IO 设备配置死代码清理
    位置：wedb/wconf/src/device_config.rs:20（DeviceType::Null）、:26（IoBackend）、:38（NativeDeviceOptions）、:53（LocalMemoryDeviceOptions）
    对标：Tsavorite.core/DeviceOptions.cs
    机制：C# 支持多种可插拔底层设备（NullDevice, NativeStorageDevice, LocalMemoryDevice）。
    问题：wedb 在架构上已经将存储后端收敛为 SegmentedDevice（以及用于无盘测试的 InMemorySublog），task/reject/null-device-broken-chain.md 已明确拒绝接入 NullDevice 并清理了运行时设备。但 wconf 中仍然残留 DeviceType::Null 枚举项，以及全仓 0 引用的 IoBackend、NativeDeviceOptions、LocalMemoryDeviceOptions 等死配置结构体。
    方案：
    - 从 DeviceType 枚举中移除 Null 变体。
    - 彻底删除 IoBackend 枚举、NativeDeviceOptions 结构体、LocalMemoryDeviceOptions 结构体及其 Default 实现。
    - 同步清理 wconf/src/lib.rs 中的无用导出。

24. [P3] VectorManager 孤儿函数与占位清理
    位置：wedb/wnode/src/resp/vector/vector_manager_cleanup.rs:278（wait_for_disk_ann_index_drop_async）、:300（vector_set_potentially_deleted）、:325（checkpoint_completed）、wedb/wnode/src/resp/vector/vector_manager_context_metadata.rs:535（get_context_state）、wedb/wnode/src/resp/vector/vector_manager_filter.rs:154（evaluate_candidate_filter）、wedb/wnode/src/resp/vector/vector_manager_locking.rs:185（needs_recreate）、:399（read_for_delete_vector_index）、:420（remove_stored_index）
    对标：garnet/libs/server/Storage/Session/ObjectStore/VectorManager.cs
    机制：C# VectorManager 管理原生 DiskANN 索引的上下文、过滤器与销毁。
    问题：上述 8 个方法系早期按 C# 签名直译生成的产物。当前 Rust 侧向量索引生命周期已全面下沉至 wvector crate 的 DataProvider 与 Callbacks 闭环，上述 8 个函数全仓生产与测试调用计数均为 0，属于死函数与虚设接口。
    方案：
    - 直接删除上述 8 个零引用函数。
    - 在 js/check/ignore/ 中登记对应的 C# VectorManager 方法豁免原因（底层已被 wvector 官方 Rust 提供者等价接管），确保 check.js 校验清洁。

25. [P3] 基础工具类全仓零引用公共函数清理
    位置：wedb/wbase/src/buf.rs:28（put_header_payload）、wedb/whasher/src/lib.rs:317（hash_set_with_capacity）、wedb/windex/src/entry.rs:106（absolute_address）、wedb/wlua/src/sender.rs:124（get_response_object_head）、:132（get_response_object_tail）、:140（exit）、:145（exit_and_return_response_object）、wedb/wmetric/src/garnet_server_monitor.rs:234（tracks_latency）、wedb/wval/src/meta.rs:151（with_encoding）
    对标：garnet/libs/common/ 与 garnet/libs/server/
    机制：C# 中特定底层抽象遗留的未消费属性与辅助工具。
    问题：上述函数在 Rust 侧经过架构重构后已被更高内聚的原语取代，全仓 src 与 tests 引用计数均为 0；尤其 wlua/src/sender.rs 中的 get_response_object_head/tail 返回未受保护的 *mut u8 / *const u8 裸指针，且 exit 方法为空函数，留存既增加认知负担又存在安全隐患。
    方案：
    - 逐一剔除上述零引用死函数与裸指针暴露接口。
    - 涉及的 C# 对标按 API-parity 规则在 check/ignore 中补充注销理由。
