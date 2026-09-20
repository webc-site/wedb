# 轮8 架构拓扑、数据链条、死代码清理与复用专项审查

审查范围与方法
对照 garnet/libs 与 rust 侧跨 crate 实现（wbase, wval, wresp, wconn, wnode, wcol, wbitmap, wbftree, wkv, wconf, wedb 等）。
排查未使用的死代码、多套重复机制、散落魔法常量、数据链条脱节、模块依赖与拓扑设计。
审查意见严格对齐 C# 原作设计与工程质量要求。

1. 架构拓扑：底层存储引擎 wkv 反向依赖上层配置库 wconf
具体问题：
1) 存储引擎底层 crate wedb/wkv 依赖了上层服务配置 crate wedb/wconf（Cargo.toml 中 wconf.workspace = true）。
2) 实际消费仅有两处：在 config.rs 与 compact.rs 中引入 wconf::LogCompactionType，在 vdb_load.rs 中读取常量 wconf::MAX_DATABASES_MAX。
3) 事实上，底层压缩抽象 crate wedb/wcompact 自身已定义了 CompactionType 枚举，而库号上限 MAX_DATABASES_MAX 属于存储引擎运行时参数或常量，应当属于存储层内部或由调用方通过参数注入。
4) C# 对应层级中，Tsavorite 纯存储库完全不感知 Garnet 的上层配置系统，由调用方通过参数注入。该反向依赖破坏了存储引擎的纯净叶子层拓扑。
rust 文件与函数：
wedb/wkv/Cargo.toml (:32)
wedb/wkv/src/config.rs: config (:5)
wedb/wkv/src/gc/compact.rs: compact (:10)
wedb/wkv/src/store/vdb_load.rs: load (:198)
c# 对应文件与函数：
libs/storage/Tsavorite/cs/src/core/Compaction/CompactionOptions.cs: CompactionType
libs/storage/Tsavorite/cs/src/core/Engine/TsavoriteKV.cs: TsavoriteKV 构造
建议动作：
改由 wcompact::CompactionType 承载压缩类型，MAX_DATABASES_MAX 移入存储层内部定义或构造器注入，移除 wkv 对 wconf 的依赖。


5. 数据链条：VectorManager 属性提取链条脱节与内联重复
具体问题：
1) VectorManager 定义了 fetch_single_vector_element_attributes 与 fetch_vector_element_attributes，但全仓生产代码零调用。
2) 上游 network_vgetattr 处理 VGETATTR 命令时，直接穿透底层调用 self.manager.service.get_attribute，旁路了 VectorManager 包装层。
3) 在 find_similar_vectors_common 中，属性组装逻辑通过两次逐元素循环内联实现，未复用 fetch_vector_element_attributes。
rust 文件与函数：
wedb/wnode/src/resp/vector/vector_manager.rs: fetch_single_vector_element_attributes (:962)
wedb/wnode/src/resp/vector/vector_manager.rs: fetch_vector_element_attributes (:981)
wedb/wnode/src/resp/vector/resp_server_session_vectors.rs: network_vgetattr (:1021)
c# 对应文件与函数：
libs/server/Resp/Vector/VectorManager.cs: FetchVectorElementAttributes
libs/server/Resp/Vector/RespServerSessionVectors.cs: NetworkVGETATTR
建议动作：
使 network_vgetattr 统一通过 VectorManager 的对应方法获取属性，并收敛相似度搜索内部的重复组装逻辑。

7. 双轨机制：向量公开接口硬编码 resp3 导致外部方法退化为测试孤岛
具体问题：
1) resp_server_session_vectors.rs 中 network_vadd, network_vsim, network_vismember, network_vsetattr 将 resp3 固定传 false，并将逻辑委托给其伴生 network_*_impl(..., resp3)。
2) 实际生产入口 garnet_api/mod.rs:473-490 直接调用了 network_*_impl 传入当前会话的真实 resp3 状态。
3) 导致外层的 network_vadd 等公开方法在生产上没有任何调用方，只在集成测试中被调用。
4) C# 中对位方法仅有一套单源入口 NetworkVADD 等。
rust 文件与函数：
wedb/wnode/src/resp/vector/resp_server_session_vectors.rs: network_vadd (:286), network_vsim (:662), network_vismember (:1082), network_vsetattr (:1205)
wedb/wnode/src/resp/garnet_api/mod.rs: garnet_api 向量分发 (:473-490)
c# 对应文件与函数：
libs/server/Resp/Vector/RespServerSessionVectors.cs: NetworkVADD, NetworkVSIM, NetworkVISMEMBER, NetworkVSETATTR
建议动作：
将 network_v* 与 network_v*_impl 合并为统一接收 resp3 的单源方法，消除双接口冗余。

8. 死代码/散落常量：wresp 错误模板常量与对象存储手写字面量脱节
具体问题：
1) wresp/src/cmd_strings.rs 定义了 GENERIC_ERR_MANDATORY_MISSING 与 GENERIC_ERR_MUST_MATCH_NO_OF_ARGS 模板常量，全仓零引用。
2) 而 wnode/src/resp/objects/object_store_utils.rs:95-116 又手写了 4 处硬编码字符串字面量（包含 FIELDS, MEMBERS, numFields, numMembers 的具体组合）。
3) 模板常量未被使用，业务侧手写具体错误，造成常量散落。
rust 文件与函数：
wedb/wresp/src/cmd_strings.rs: GENERIC_ERR_MANDATORY_MISSING (:321), GENERIC_ERR_MUST_MATCH_NO_OF_ARGS (:325)
wedb/wnode/src/resp/objects/object_store_utils.rs: mandatory_missing_err (:95), must_match_args_err (:111)
c# 对应文件与函数：
libs/server/Resp/CmdStrings.cs: GenericErrMandatoryMissing, GenericErrMustMatchNoOfArgs
建议动作：
清理 cmd_strings.rs 中未引用的死模板，或在 object_store_utils.rs 中引用标准常量进行单源管理。

9. 死代码：VectorManager 清理控制机制全仓生产代码零调用
具体问题：
1) VectorManager 清理模块实现了 pause_cleanup_async, resume_cleanup, queue_cleanups。
2) 在 C# 原作中，PauseCleanupAsync 与 ResumeCleanup 在副本全量同步 ReplicaDisklessSync.cs:112 与 ReplicaDiskbasedSync.cs:143 中于接收快照前后调用以防止竞争，QueueCleanups 在检查点完成恢复后调用。
3) 当前 Rust 实现中，wedb/server/replication 完全未接入 pause_cleanup_async 与 resume_cleanup，检查点恢复处亦未调用 queue_cleanups，上述方法在生产路径零引用，沦为单测孤岛。
rust 文件与函数：
wedb/wnode/src/resp/vector/vector_manager_cleanup.rs: pause_cleanup_async (:324), resume_cleanup (:331), queue_cleanups (:353)
c# 对应文件与函数：
libs/server/Resp/Vector/VectorManager.Cleanup.cs: PauseCleanupAsync, ResumeCleanup, QueueCleanups
libs/cluster/Server/Replication/ReplicaOps/ReplicaDisklessSync.cs: RunSync (:112)
libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs: RunSync (:143)
建议动作：
在副本快照接收逻辑与检查点恢复链路中补全接入调用，使清理保护机制在生产中真正生效。

10. 死代码清理：跨 crate 零引用辅助函数与未接入校验
具体问题：
以下函数在生产链路中零引用，部分仅在单元测试中调用或仅有声明：
1) wedb/wnode/src/primary_tasks.rs: commit_task_running (:147)。AOF 周期提交运行状态判定函数，全仓零调用。
2) wedb/wtxn/src/transaction_manager.rs: add_transaction_store_types (:325)。与单数版 add_transaction_store_type 重复，全仓零调用。
3) wedb/wbitmap/src/bitfield/parse.rs: is_large_enough_for_type (:134)。C# 原版仅在 Debug.Assert 中使用，Rust 侧导出但零调用。
4) wedb/wbase/src/pool/limited.rs: as_slice (:42), as_mut_slice (:48)。PooledRefBuffer 已实现 Deref/DerefMut，这两个独立方法全仓零调用。
5) wedb/wnode/src/resp/resp_server_session/pump.rs: write_direct_large (:197)。仅在测试中用于注入字节，生产代码零调用。
6) wedb/wconf/src/runtime_server_config.rs: ensure_valid_kind (:1039), ensure_supported_enum (:1065)。注释称在建表处 debug_assert 调用，实则未接入，仅在测试中被调。
7) wedb/wedb/src/server/cluster_manager.rs: get_range (:507)。槽位合并格式化输出函数，全仓生产零调用。
rust 文件与函数：
wedb/wnode/src/primary_tasks.rs: commit_task_running (:147)
wedb/wtxn/src/transaction_manager.rs: add_transaction_store_types (:325)
wedb/wbitmap/src/bitfield/parse.rs: is_large_enough_for_type (:134)
wedb/wbase/src/pool/limited.rs: as_slice (:42), as_mut_slice (:48)
wedb/wnode/src/resp/resp_server_session/pump.rs: write_direct_large (:197)
wedb/wconf/src/runtime_server_config.rs: ensure_valid_kind (:1039), ensure_supported_enum (:1065)
wedb/wedb/src/server/cluster_manager.rs: get_range (:507)
c# 对应文件与函数：
libs/server/Transaction/TransactionManager.cs: AddTransactionStoreTypes
libs/server/Resp/Bitmap/BitmapManagerBitfield.cs: IsLargeEnoughForType
libs/server/Resp/RespServerSession.cs: WriteDirectLarge
libs/server/Config/RuntimeServerConfig.cs: EnsureSupportedEnum
libs/cluster/Server/ClusterManager.cs: GetRange
建议动作：
清理冗余死函数，缺失的配置静态校验接入初始化断言。
