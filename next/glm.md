# glm 待办

1. [P1] 集群拓扑不落盘
   位置：wedb/wedb/src/server/cluster_manager.rs:208 flush_config 只做 fetch_add；wedb/wedb/src/args.rs:51 cluster_config_path 全仓零调用；wedb/wedb/src/server/cluster_manager.rs:93 init_local 零调用（启动恢复断链）
   对标：garnet/libs/cluster/Server/ClusterManager.cs（构造 ClusterUtils.ReadDevice 恢复；FlushTaskAsync 周期刷盘）
   问题：停机与槽位变更的 flush_config 调用点已接（cluster_provider.rs:508、cluster_manager_slot_state.rs 多处、server.rs 停机段），但写盘与启动恢复缺失，重启后 MEET/Gossip 全部重来
   改法：flush_config 落盘写 cluster_config_path；启动读盘后 from_byte_array 恢复并 init_local(recover_config=true)

4. [P1] 副本重连超时默认值与注释双错
   位置：wedb/wedb/src/main.rs:36 REPLICATION_REESTABLISHMENT_TIMEOUT_SECS = 1，注释称 C# 默认 1
   对标：garnet/libs/host/defaults.conf:527（ClusterReplicationReestablishmentTimeout = 0 = 禁用）；garnet/libs/cluster/Server/Replication/ReplicationManager.cs:184-189（pollFrequency==0 直接 return）
   问题：Rust 默认 1 启用自动重连，C# 默认禁用，注释声明与上游相反
   改法：默认改 0，或更正注释声明故意差异

5. [P1] wkv 无索引在线扩容通道
   位置：wedb/wdatabase/src/database_manager_base.rs:330-335 grow_index_if_needed_async 空转返回；wedb/wnode/src/task.rs:40 IndexAutoGrowTask 仅枚举无任务体；wedb/wnode/src/resp/config_commands.rs:327-331 按增长失败降级
   对标：garnet/libs/server/StoreWrapper.cs:798 IndexAutoGrowTaskAsync、garnet/libs/server/Databases/DatabaseManagerBase.cs:317 GrowIndexesIfNeededAsync
   问题：索引满后无自动扩容，CONFIG 增长请求恒报失败
   改法：补后台 IndexAutoGrow 任务消费 grow_indexes_if_needed_async；备选按 SKILL check/ignore 登记差异。关联 StoreWrapper.Reset（Pause+Reset+Resume）同未落地

6. [P1] no-script 位图未接线
   位置：wedb/wnode/src/resp/resp_server_session.rs:2305 no_script_details 仅测试调用（:3321）；:844-876 process_messages 门自认仅承载 ACL
   对标：garnet/libs/server/Lua/LuaRunner.cs:242（脚本期挂 noScriptBitmap）+ garnet/libs/server/Resp/AdminCommands.cs:95-115 CheckScriptPermissions
   问题：EVAL 脚本内执行 multi/subscribe 等不回 NOSCRIPT，构建器现成只差接线
   改法：run_lua_command 进入脚本期挂位图，命令门加拦截

7. [P1] 指标接线两缺
   位置：wedb/wnode/src/server.rs:116 metrics_sampling_frequency builder 与 :242 启动门已在，但 wedb/wedb/src/main.rs 与 wedb_standalone 均未调用（恒 0 监视器永不启动）；wedb/wmetric/src/command_stats.rs 表无 per-command 递增，wedb/wnode/src/resp/info_provider.rs 无 commandstats 段
   对标：garnet/libs/server/Resp/RespServerSession.cs:587-598、:683-715 CommandStatsMonitor；garnet/libs/host/Configuration/Options.cs:344-360
   问题：监视器与命令统计全链断
   改法：宿主入口透传采样频率；monitor 开启时挂 CommandStats 表并补 INFO 段

8. [P1] MIGRATE 剩余两段（M3 SLOTS 变体 / M4 checkpoint 网络导入）
   位置：M3：wedb/wedb/src/server/migration/ 仅 MigrateSession/Sketch 脚手架（migrate_session.rs:28），扫描-传输-删除游标循环缺失（原语已在 wnode/src/storage/session/common/array_key_iteration_functions.rs:214/263）；M4：SNAPSHOT_DATA/SEND_CKPT_* 仅枚举（wnode/src/resp/resp_commands_info_data.rs:375-383）会话无臂，恢复件已在 wcpr/src/manager.rs:599 recover_checkpoint_components
   对标：garnet/libs/cluster/Session/RespClusterMigrateCommands.cs NetworkClusterMigrate、Migrate/MigrateSessionSlots.cs、Server/Replication/ReplicaOps/ ReceiveCheckpointHandler
   问题：槽位迁移无可搬运循环，checkpoint 网络导入面未落
   改法：先打通 execute_cluster_migrate_async 应答解析与超时（client.rs 先例），再写停等循环；M4 按 staging 目录 → SegmentedDevice::single_file → recover_checkpoint_components → WedbStore::from_components → set_store

9. [P1] INFO 复制段缺 5 个副本侧指标
   位置：wedb/wedb/src/server/cluster_provider.rs:525-599 get_replication_info
   对标：garnet/libs/cluster/Server/ClusterProvider.cs:255-259
   问题：缺 replication_offset_vector_lag、replication_offset_acc_lag、aof_replay_max_lag_bytes、physical_sublog_max_sequence_vector、physical_sublog_max_drift_sequence_vector（原语 AofAddress::diff/AggregateDiff 等已在）
   改法：按 C# 逐项补齐 INFO REPLICATION 输出

10. [P1] 集群态 PUBLISH/SPUBLISH 缺跨节点广播钩子
    位置：wedb/wpubsub/src/session_commands.rs:390-398 network_publish 对 shard 直接回 CLUSTER_DISABLED
    对标：garnet/libs/server/Resp/PubSubCommands.cs:140-147（EnableCluster 时阻塞等 ClusterPublishAsync）
    问题：RESP 会话路径无集群回调，跨节点发布断
    改法：network_publish 增加集群回调位，有集群会话时转发后合并应答

11. [P1] Lua 装配缺口
    位置：wedb/wlua/src/cache.rs:125 set_user_handle 认证后无生产调用；wedb/wlua/src/loader.rs:132-134 status_reply 返回 {ok=...}
    对标：garnet/libs/server/Resp/RespServerSession.cs:311（认证变更即 SetUserHandle）；garnet/libs/server/Lua/LuaRunner.Loader.cs:136-138（status_reply 直接返回 text）
    问题：脚本缓存用户句柄不传播；回复形态差异未声明
    改法：认证成功/切换用户处向脚本缓存传播；status_reply 对齐或补差异注释

12. [P1] 配置面加载接线
    位置：wedb/wconf/src/node_options.rs:227-234 from_nested_text_str/from_file 无生产调用、无 --config 参数
    对标：garnet/libs/host/Configuration/Options.cs（130+ 项全集）、:483-506 config import/export
    问题：CLI 约 15 项仍缺 reviv 系、slowlog、gossip-delay/gossip-sp/cluster-timeout、max-inline-key/value-size、index-resize、max-databases、protected-mode、aof 系尺寸等（lua 三项已补）
    改法：入口增补 --config <path>，CLI 覆盖文件值；按运维优先级补常用项，其余登记 check/ignore

14. [P1] Group Commit / 级联刷盘状态机抽取公共组件
    位置：wedb/wkv/src/store/flush.rs:29 FlushPipeline、wedb/waof/src/log.rs:39 CommitPipelineState（注：wedb/wnode/src/aof/waof_sublog.rs 的三态 CAS 已彻底清理收敛，转调 wal.commit_to / wal.commit 单轨流水线）
    对标：garnet/libs/storage/…/TsavoriteLog.cs CommitTask/ongoingCommitRequests
    问题：wkv FlushPipeline 与 waof CommitPipelineState 仍存在重复实现的 Leader/Follower 级联合并模式
    改法：抽公共 GroupCommitPipeline 入 wbase，由 wkv 与 waof 统一复用

15. [P1] 全 workspace 零引用 pub 项清理
    位置：wedb/wnode/src/resp/basic_commands.rs:1049 network_quit、:1104 network_readonly 等（grep 零调用）；wedb/wnode/src/aof/replaycoordinator/aof_replay_coordinator.rs process_synchronized_operation；wedb/wlua/src/limited_allocator.rs、runner.rs、functions.rs 部分导出；wmetric add_total_write 等随第 7 条接线复活勿贸删
    对标：garnet 对应调用点（ClusterSession.cs 单/多键归一等）
    问题：批量死导出面残留（cluster_manager verify_key/delete_keys_in_slots 已转活可豁免）
    改法：逐项 grep 确认零调用后删除或私有化，check.js 保持 0 缺失
17. [P1] 集合命令 numkeys/count 解析宽度 i64 vs C# int32
    位置：wedb/wnode/src/resp/objects/sorted_set_commands.rs:491、:908、:1134（ZMPOP/ZINTERCARD/BZMPOP numkeys 用 try_parse_i64）
    对标：garnet/libs/server/Resp/Objects/SortedSetCommands.cs（全族 parseState.TryGetInt int32，溢出报 not-integer）
    问题：六处（含 ZRANDMEMBER/ZINTERCARD/BLMPOP/GEO COUNT）宽度超集，溢出行为与 C# 不一致
    改法：收敛 strict_i32（hash_commands.rs:484 HRANDFIELD、list_commands.rs 先例）

18. [P1] HCOLLECT `*` 缺 already-in-progress 互斥
    位置：wedb/wresp/src/cmd_strings.rs:89 RESP_ERR_HCOLLECT_ALREADY_IN_PROGRESS 零引用；wedb/wnode/src/resp/garnet_api.rs:531-560 全库收集臂无锁
    对标：garnet/libs/server/Storage/Session/ObjectStore/Common.cs:812-814 _hcollectTaskLock.TryWriteLock() 失败回该文案
    问题：并发 HCOLLECT 可重入
    改法：补进行标志互斥，失败映射常量文案

19. [P1] INFO KEYSPACE 带 TTL 键计数恒 0
    位置：wedb/wnode/src/resp/info_provider.rs:99-100 keyspace_stats 恒 (0,0)
    对标：garnet/libs/server/StoreWrapper.cs:790 GetKeyspaceStats → garnet/libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:382 UnifiedStoreGetKeyspaceStats
    问题：库快照通道未接，TTL 键统计缺失（EXPDELSCAN 拦截差异已在 admin_commands.rs:556 注释声明）
    改法：接存储域键空间扫描通道回填计数

20. [P1] waof_sublog memory_size_bytes 返回容量常量
    位置：wedb/wnode/src/aof/waof_sublog.rs:326-327 返回 wal.config().buffer_size；ShardedLog/SingleLog 同实现复制
    对标：garnet/libs/storage/…/TsavoriteLog.cs:196 MaxMemorySizeBytes（容量）vs :201 MemorySizeBytes（当前占用）
    问题：两语义塌缩为一
    改法：SublogBackend 拆 max_memory_size_bytes/memory_size_bytes 两方法

21. [P1] committed_begin_address 塌缩为 begin_address
    位置：wedb/wnode/src/aof/single_log.rs:49-51、wedb/wnode/src/aof/sharded_log.rs:163-169
    对标：garnet/libs/storage/…/TsavoriteLog.cs:120 CommittedBeginAddress（独立字段，恢复自 commit 记录）
    问题：提交边界与起始地址混用
    改法：引入独立 committed 字段并自 commit 记录恢复

24. [P1] aof_processor object_store_rmw 四对象块复制四份
    位置：wedb/wnode/src/aof/aof_processor.rs:1358-1372 起（Hash/List/Set/SortedSet 各一分支）
    对标：garnet/libs/server/AOF/AofProcessor.cs（经对象序列化器多态单通道）
    问题：同构 match 四份
    改法：抽 (obj_type, from_blob, to_blob) 泛型单循环

25. [P1] 占位函数 prefetch_key_sequence_number 空实现且有生产调用
    位置：wedb/wnode/src/aof/readconsistency/virtual_sublog_replay_state.rs:187（空体）；调用点 read_consistency_manager.rs:329
    对标：garnet/libs/server/AOF/ReadConsistency/VirtualSublogReplayState.cs:119（真实缓存预热）、ReadConsistencyManager.cs:305
    问题：读一致性预热降级为空操作且未声明
    改法：实现预热或注释声明降级

26. [P1] sharded 多物理日志拓扑无装配点
    位置：wedb/wnode/src/aof/recover/aof_recover.rs:71 multi_log_recover 零调用；生产 service.rs 单 WaofSublog
    对标：garnet/libs/server/AOF/GarnetAppendOnlyFile.cs 多 sublog 拓扑 + AofProcessor.cs 装配
    问题：ShardedLog/ReadConsistencyManager/ReplayAlignBarrier 生产不可达
    改法：--aof 点亮时定多日志拓扑装配，或登记单日志差异并清死链

27. [P1] NodeArgs.compaction_freq_secs 死旋钮与配置域收敛
    位置：wedb/wconf/src/node_options.rs:85（全仓零消费；StoreConfig 级同名项已删，见 wkv/src/config.rs:66 注释）
    对标：garnet/libs/host/Configuration/RuntimeServerConfig.cs 单域 + StoreWrapper 每轮重读
    问题：GcConfig/StoreConfig/NodeArgs/RuntimeServerConfig/ServerOptions 多面声明同语义参数
    改法：删死旋钮；以 RuntimeServerConfig 为运行时单一来源，其余面收敛声明


31. [P2] iterate_version_chain 仅测试调用
    位置：wedb/whlog/src/hlog/io.rs:509
    对标：garnet/libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorScan.cs IterateHashChain（C# 服务端同样无生产调用）
    问题：生产零调用
    改法：移入 cfg(test) 或标注 API-parity 保留

32. [P2] 对标注释风格统一
    位置：全仓 94 处「在 garnet 中的相对路径:函数名」范式
    对标：SKILL 文档注释格式条款
    问题：与事实标准简式并存
    改法：统一为简式

33. [P2] whyperlog 补 readme 说明与 whlog 区别
    位置：wedb/whyperlog/（无 readme；whlog 有 README.md + readme/）
    对标：garnet/libs/server/Resp/HyperLogLog/（数据结构）vs garnet/libs/storage TsavoriteLog（日志）
    问题：命名易混且 whyperlog 无说明文档（不合并，两域正交，见 task/reject/whyperlog-whlog-unification.md）
    改法：补 readme 注明区别

34. [P2] 浮点格式化三处收敛
    位置：wedb/wresp/src/resp_memory_writer.rs:18（权威）、wedb/wcol/src/resp/output.rs:202/208、wedb/wcol/src/hash/hash_object_impl.rs:67
    对标：garnet/libs/common/ConvertUtils.cs 格式化单点
    问题：三份实现
    改法：收敛到 wresp 单点，wcol 转调

35. [P2] 错误枚举收敛
    位置：wedb/wnode/src/error.rs:8 与 wnode/src/service.rs:74 双 Error（service 版对外零引用）；wedb/wcpr/src/error.rs:62/66 ChecksumMismatch/MetaChecksumMismatch 同文件双变体；wvector 5 枚举散落无中心 error.rs
    对标：garnet GarnetStatus 单点；SKILL rust_review「错误在 error.rs 或独立模块中定义」
    问题：crate 内枚举分裂
    改法：wnode 变体并入根 error.rs，wcpr 双变体合一，wvector 建中心（leaf 留本地是对的，不上收 wbase）

36. [P2] hex 微工具两处
    位置：wedb/wacl/src/acl_password.rs:65 hex_val 与 wedb/wlua/src/hash_key.rs:48 from_hex
    对标：SKILL「一处定义」
    问题：hex 解码双实现
    改法：下沉 wbase hex 特性

37. [P2] E1-E4 增量产出的重复/质量复查
    位置：wedb/wnode/src/resp/config_commands.rs、wnode/src/txn_resp_commands.rs、wedb/wcustom/src/module.rs、泵直读/回退双路、wedb/wedb/src/server/cluster_session.rs 各 arm 之间
    对标：对应 garnet 命令实现
    问题：原定第 3 轮质量复审未执行
    改法：对标逐段复查重复与偏差

38. [P2] src 内嵌集成测试迁 tests/
    位置：wedb/wkv/src/session/consistent_read.rs（1）、wnode/src/aof/garnet_log.rs（11）、aof_backpressure.rs（7）、aof_processor.rs（4）、wnode/src/task.rs（3）、wnode/src/resp/rangeindex/ 与 wcol/src/itembroker/ 内嵌面、wedb/wedb/src/server/gossip/node_connection.rs（1）；wval/tests/tag.rs、meta_and_subkey.rs、ns_codec.rs、zset_codec.rs 应内嵌 src
    对标：SKILL「集成测试要放到 crate 的 tests 文件夹」
    问题：单元/集成测试位置混放
    改法：集成测试迁 tests/，自有 codec 单元测试内嵌

39. [P2] wedb_standalone 测试归属整理
    位置：wedb_standalone/tests/（42 文件约 1.9 万行）vs src 133 行；service.rs/range_index_tests.rs/wal_replay_e2e.rs 越层直用 wbftree:: 类型
    对标：garnet/test/standalone/（测试工程引用 server 库）；SKILL 同上
    问题：命令级 e2e 全挂在 bin crate 且越层断言
    改法：命令级 e2e 迁 wnode/tests 或专用集成测试 crate，越层断言改经 wkv 公开 API，wedb_standalone 只留启动冒烟

40. [P2] 删除 wedb_standalone/src/lib.rs 空壳
    位置：wedb_standalone/src/lib.rs（1 行注释，tests 无 wedb_standalone:: 引用）
    对标：garnet main/GarnetServer 为纯 bin 工程
    问题：bin crate 不需要 lib 目标
    改法：删除

41. [P2] aof/mod.rs 通配转发
    位置：wedb/wnode/src/aof/mod.rs:23 pub use waof_sublog::*
    对标：SKILL rust_review「暴露的接口清晰优雅」
    问题：通配导出
    改法：逐项列名导出

42. [P2] CONFIG GET 应答未接 RESP3 map 头
    位置：wedb/wnode/src/resp/config_commands.rs CONFIG GET 恒 RESP2 双倍数组（HELLO map 已接，见 resp_server_session.rs:1672）
    对标：garnet/libs/server/Config/ServerConfig.cs:69 WriteMapLength
    问题：RESP3 下形态偏差
    改法：按协议版本写 map 头

43. [P2] SCAN 参数两处口径
    位置：wedb/wnode/src/resp/array_commands.rs:110-112 未知选项报语法错（C# if/else-if 无 else 静默跳过）、:83-84 COUNT 负/零保留默认 10（注释称 C# 扫描层钳 1，与 C# :298 原样透传口径冲突待复核）
    对标：garnet/libs/server/Resp/ArrayCommands.cs:275-313
    问题：未知选项行为与 C# 不一致；COUNT 口径存疑
    改法：对齐 C# 静默跳过；核实 C# 扫描层后统一 COUNT 口径

44. [P2] 错误文案：SUBSTR 报 GETRANGE、PEXPIRETIME 命令名口径
    位置：wedb/wnode/src/resp/basic_commands.rs:577 network_get_range 对 SUBSTR 恒报 GETRANGE；wedb/wnode/src/resp/key_admin_commands.rs:479-483 PEXPIRETIME 用实名（C# quirk 恒 expiretime）
    对标：garnet/libs/server/Resp/ArrayCommands.cs:494（cmd.ToString()）；garnet/libs/server/Resp/KeyAdminCommands.cs:537
    问题：文案偏差
    改法：SUBSTR 按实际命令名报错；PEXPIRETIME 对齐 quirk 或登记差异

45. [P2] 集合命令错误文案批
    位置：wedb/wnode/src/resp/objects/sorted_set_commands.rs:1556 WEIGHTS 报 "weight value is not a float"（C# 为 "ERR value is not a valid float"）；ZINTERCARD LIMIT 两态已对齐，其余（ZPOPMIN positive 文案、BLMPOP Parameter 版、GEO lon/lat 两态）逐条复核
    对标：garnet/libs/server/Resp/CmdStrings.cs:251/268/338、SortedSetCommands.cs:1211-1219
    问题：部分文案未逐 token 对齐
    改法：逐条比对 CmdStrings 权威文案修正

46. [P2] RI.CREATE 数值选项非数字吞错
    位置：wedb/wnode/src/resp/rangeindex/resp_server_session_range_index.rs:105-133 五处 unwrap_or(0)
    对标：garnet/libs/server/Resp/Parser/SessionParseState.cs:402-410 + ParseUtils.cs:64-77（RespParsingException 协议错误）
    问题：非法数值静默取 0
    改法：改协议错误

47. [P2] network_rilen 无分派 arm
    位置：wedb/wnode/src/resp/rangeindex/resp_server_session_range_index.rs:516（内含两层定义 :516/:584）
    对标：garnet/libs/server/Resp/Parser/RespCommandHashLookupData.cs:228-236（RI 仅 9 命令，无 RILEN）
    问题：C# 无此 RESP 命令（SKILL ri_len 指内部 API，非 RESP 面）
    改法：删 RESP 面或按 check/ignore 登记超集

48. [P2] key 级 TTL 缺 4-bit coarse 粗化
    位置：wedb/wkv/src/ttl.rs put_ttl 存全精度 ticks；HFE 侧已 1:1（wresp/src/options.rs:136）
    对标：garnet/libs/server/ExpirationWithOption.cs:22-23（ticks>>4<<4）
    问题：与 C# 粗化口径差异未声明
    改法：对齐或注释登记差异

49. [P2] 副本回放 PEXPIREAT 丢条件未声明
    位置：wedb/wnode/src/aof/aof_processor.rs:1243 Pexpireat 臂无差异注释（SET 条件族 :1293 已注释）
    对标：garnet/libs/server/Storage/…/UnifiedStore/PrivateMethods.cs:108（Deterministic 标志 + 副本重评估）
    问题：SKILL 允许的帧差异缺注释
    改法：补注释

50. [P2] waof_sublog scan 手写环形帧解析
    位置：wedb/wnode/src/aof/waof_sublog.rs:213-256（恢复链已统一 scan_async）
    对标：garnet TsavoriteLog 扫描单点
    问题：与 waof/src/iterator.rs 同构
    改法：waof 补内存窗口同步扫描 API 后删手写段

51. [P2] 序列号提取多处同构
    位置：wedb/wnode/src/aof/aof_processor.rs:1356 前后 fallback 判定与 replaycoordinator/aof_replay_coordinator.rs:250 txn_header_sequence_number 等
    对标：garnet ShardedHeader/AofHeader 序列号字段单点
    问题：提取逻辑分散
    改法：waof 头层加单一 sequence_number_of(entry, fallback)

52. [P2] 存储过程参数区编解码两份
    位置：wedb/wnode/src/aof/replaycoordinator/stored_proc_replay.rs:17-28 vs wnode/src/aof/aof_processor.rs:373；stored_proc_payload 为 skip_header 薄包装
    对标：garnet RespInputHeader/StringInput 布局单点；waof/src/header.rs AofHeader::skip_header
    问题：同一布局两份解析
    改法：收敛 waof 头层单一解析

53. [P2] waof 内分 wal/（物理）与 aof/（语义）两模块
    位置：wedb/waof/src/header.rs:136-646 语义 AofHeader/AofChunkHeader 转写住在物理层模块（8B RecordHeader :128 才是物理帧）
    对标：garnet/libs/server/AOF/AofHeader.cs + AofChunkHeader.cs vs TsavoriteLog 帧头分层
    问题：语义层误植物理 crate
    改法：模块内分目录，归属对齐 C# 分层（不动 crate 边界）

56. [P2] AofAddress 三重编码面收敛
    位置：wedb/waof/src/address.rs:18 bitcode derive（仅 :402 测试用）、:108/:119 serialize/deserialize（仅测试调用）
    对标：garnet/libs/server/AOF/AofAddress.cs Serialize/Deserialize（API-parity 可登记）
    问题：derive 死挂 + 双编码面
    改法：删 derive；serialize/deserialize 按 API-parity 登记或删

57. [P2] wkv CheckpointManager 剩余转发面
    位置：wedb/wkv/src/checkpoint.rs:476/482 purge_checkpoint 与 purge_all 双入口逐字转发 wcpr（take_cpr_snapshots/recover_cpr_snapshots 已删）
    对标：garnet/libs/server/GarnetCheckpointManager.cs 单类无双面
    问题：别名转发残留
    改法：保留单一入口，调用方直用 wcpr 或收敛别名

58. [P2] 读路径双探针枚举合一
    位置：wedb/wkv/src/session/raw/mod.rs:28/39 ReadProbeResult/TraceBackResult 同四态改名，raw/read.rs 两处同构 match
    对标：garnet Tsavorite InternalRead.cs:105-131 单一分类枚举
    问题：双枚举四份 match
    改法：两枚举合一，收敛单探针闭包

59. [P2] TTL 读取双实现
    位置：wedb/wkv/src/ttl.rs:121 ttl_of（3 行）vs wkv/src/compact.rs:104 read_ttl_expiry（30 行重写）
    对标：garnet 紧缩经统一 CompressFunctions 无第二读取器
    问题：重复实现
    改法：compact.rs 改转发 ttl_of

60. [P2] 惰性过期裁决内联复制收敛
    位置：wedb/wkv/src/session/collection.rs:96/214、raw/modify.rs:199、raw/read.rs:758 等 has_ttl_tag && check_expired 复制
    对标：garnet SessionFunctionsUtils 过期判定单点
    问题：7 处内联复制
    改法：抽单一 probe_alive 助手

61. [P2] 删除 wrecord/src/chunk.rs 死模块
    位置：wedb/wrecord/src/chunk.rs（lib.rs:24 导出，全仓零消费）
    对标：garnet NativeStorageDevice 扇区对齐路径无此物；紧凑编码不在此
    问题：零引用模块（与 wdev::chunk 同名不同物，后者活）
    改法：删除模块与导出

62. [P2] ReadCache 薄包装与生产开关
    位置：wedb/wkv/src/read_cache.rs:37 tag_read_cache_addr 等地址位薄包装；引擎本体生产未开通
    对标：garnet/libs/server/Storage/ReadCache.cs / TryCopyToReadCache.cs
    问题：包装转发 wbase::addr 重复；开关无配置位
    改法：删薄包装；开关接 NodeArgs 或显式登记

63. [P2] 零调用 pub 面收敛批
    位置：wkv RunGuard/GcStatsSnapshot/ListTree/compact_lazy/compact_with_filter/tag_read_cache_addr 等；wcol 双 ScanInput（wcol/src/resp/input.rs:196 vs wcol/src/types/garnet_object_base.rs:22）与 ObjectOutputFlags/ExpirationQueue/CUSTOM_TYPE_ID_START 等零上层引用项
    对标：garnet 对应单点
    问题：pub 面超供与同名双结构
    改法：逐项 grep 后删/私有化，双 ScanInput 合一

66. [P2] 单实现 trait 收敛评估
    位置：wedb/wkv/src/ri.rs:16 RiTreeOps（仅 impl for BfTreeService :61）及 wbftree TreeOps 族
    对标：C# 无对应 trait 层
    问题：单实现 trait 抽象冗余（wcol::CollectionItemStore 是依赖倒置点，保留）
    改法：评估降为固有方法块

67. [P2] wnode 穿透引擎字段
    位置：wedb/wnode/src/resp/rangeindex/range_index_manager_migration.rs:117 &session.store.range_index
    对标：garnet RangeIndexManager 经 storeWrapper 获取
    问题：跨层直取字段
    改法：改 wkv 显式访问器

68. [P2] bitcode derive 死挂清理
    位置：wedb/wcpr/src/meta.rs:3（IndexMeta/HlogMeta）、wedb/whlog/src/flush.rs:1（PageFlushRange）（whlog/address.rs 已清）
    对标：SKILL「数据格式别搞多格式」
    问题：持久化走手写定长，derive 仅测试用
    改法：删 derive 与对应测试

69. [P2] wreviv/wcompact 并入 wkv
    位置：wedb/wreviv、wedb/wcompact 独立 crate（对标 Tsavorite RevivificationManager/Compact，C# 本就在 core 内）
    对标：garnet/libs/storage/tsavorite/cs/src/core/
    问题：单消费者薄 crate
    改法：并入 wkv（wconn 对位 libs/client 保留，其余小 crate 定位成立）

70. [P2] 未用依赖与死 feature 清理
    位置：wedb/wnode/Cargo.toml:41 声明 wbase feature "ascii" 与 wbase/src/ascii.rs 模块级双死（wnode 源码仅用 std eq_ignore_ascii_case）；dev-dep 7 项（wbitmap aok / wdatabase compio / whlog whasher / whyperlog aok / wvector futures-executor / whasher aok+ctor+log_init / wbase ctor+log_init）逐项复核
    对标：garnet csproj 依赖面
    问题：依赖死重
    改法：删 ascii feature 与模块、清理未用 dev-dep

71. [P2] workspace 依赖表收口
    位置：wedb/Cargo.toml workspace.dependencies 无 crossfire、clap（8 crate/3 crate 直写版本）
    对标：SKILL「crossfire 消息队列」规定依赖应集中管理防漂移
    问题：版本散落
    改法：入根 workspace.dependencies 表

72. [P2] wcol 对象层 operate 直收切片与 ArgSlice offset 化
    位置：wedb/wcol/src/resp/input.rs ObjectInput 包装层
    对标：SKILL「读路径借用零拷贝 / 批量接口单次折叠」
    问题：operate 经 ObjectInput 包装、ArgSlice 依赖 unsafe Send/Sync 裸指针契约
    改法：单独立项：operate 直收 &[&[u8]]，ArgSlice offset 化

73. [P2] 测试对标剩余缺口
    位置：无 ReplayAlignBarrier N>2 并发轮次测试、ShardedHeader scatter parts 回读测试；低内存/大值磁盘、真实 TCP/UnixSocket、GarnetClient、CacheSizeTracker 等存储扩展配套、AOF 降级版本/双重回放/枚举稳定、Garnet.fuzz/BDN bench 均无对应；SCAN 族深度（C# RespScanCommandsTests 810 行/25 tests）与 flush_evict flaky 根治待复核
    对标：garnet/test/（ClusterReplicationAsyncReplay.cs、NetworkTests.cs、CacheSizeTrackerTests.cs、RespAofDownlevelVersionTests.cs、PersistedEnumStabilityTests.cs 等）
    问题：集群复制/负面/迁移、ACL/Lua/ETag/事务、T1-T6/T9 已有套件（wedb/tests 17 文件、standalone 42 文件），上列仍缺
    改法：按对标优先级补齐；T11 先以确定性调度根治再撤 nextest 重试兜底
