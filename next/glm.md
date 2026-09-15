# glm 待办

1. [P1] 集群拓扑不落盘
   位置：wedb/wedb/src/server/cluster_manager.rs:208 flush_config 只做 fetch_add；wedb/wedb/src/args.rs:51 cluster_config_path 全仓零调用；wedb/wedb/src/server/cluster_manager.rs:93 init_local 零调用（启动恢复断链）
   对标：garnet/libs/cluster/Server/ClusterManager.cs（构造 ClusterUtils.ReadDevice 恢复；FlushTaskAsync 周期刷盘）
   问题：停机与槽位变更的 flush_config 调用点已接（cluster_provider.rs:508、cluster_manager_slot_state.rs 多处、server.rs 停机段），但写盘与启动恢复缺失，重启后 MEET/Gossip 全部重来
   改法：flush_config 落盘写 cluster_config_path；启动读盘后 from_byte_array 恢复并 init_local(recover_config=true)

5. [P1] wkv 无索引在线扩容通道
   位置：wedb/wdatabase/src/database_manager_base.rs:330-335 grow_index_if_needed_async 空转返回；wedb/wnode/src/task.rs:40 IndexAutoGrowTask 仅枚举无任务体；wedb/wnode/src/resp/config_commands.rs:327-331 按增长失败降级
   对标：garnet/libs/server/StoreWrapper.cs:798 IndexAutoGrowTaskAsync、garnet/libs/server/Databases/DatabaseManagerBase.cs:317 GrowIndexesIfNeededAsync
   问题：索引满后无自动扩容，CONFIG 增长请求恒报失败
   改法：补后台 IndexAutoGrow 任务消费 grow_indexes_if_needed_async；备选按 SKILL check/ignore 登记差异。关联 StoreWrapper.Reset（Pause+Reset+Resume）同未落地

7. [P1] 指标接线两缺
   位置：wedb/wnode/src/server.rs:116 metrics_sampling_frequency builder 与 :242 启动门已在，但 wedb/wedb/src/main.rs 与 wedb_standalone 均未调用（恒 0 监视器永不启动）；wedb/wmetric/src/command_stats.rs 表无 per-command 递增，wedb/wnode/src/resp/info_provider.rs 无 commandstats 段
   对标：garnet/libs/server/Resp/RespServerSession.cs:587-598、:683-715 CommandStatsMonitor；garnet/libs/host/Configuration/Options.cs:344-360
   问题：监视器与命令统计全链断
   改法：宿主入口透传采样频率；monitor 开启时挂 CommandStats 表并补 INFO 段

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

50. [P2] waof_sublog scan 手写环形帧解析
    位置：wedb/wnode/src/aof/waof_sublog.rs:213-256（恢复链已统一 scan_async）
    对标：garnet TsavoriteLog 扫描单点
    问题：与 waof/src/iterator.rs 同构
    改法：waof 补内存窗口同步扫描 API 后删手写段

53. [P2] waof 内分 wal/（物理）与 aof/（语义）两模块
    位置：wedb/waof/src/header.rs:136-646 语义 AofHeader/AofChunkHeader 转写住在物理层模块（8B RecordHeader :128 才是物理帧）
    对标：garnet/libs/server/AOF/AofHeader.cs + AofChunkHeader.cs vs TsavoriteLog 帧头分层
    问题：语义层误植物理 crate
    改法：模块内分目录，归属对齐 C# 分层（不动 crate 边界）

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
