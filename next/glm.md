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

32. [P2] 对标注释风格统一
    位置：全仓 94 处「在 garnet 中的相对路径:函数名」范式
    对标：SKILL 文档注释格式条款
    问题：与事实标准简式并存
    改法：统一为简式

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

69. [P2] wreviv/wcompact 并入 wkv
    位置：wedb/wreviv、wedb/wcompact 独立 crate（对标 Tsavorite RevivificationManager/Compact，C# 本就在 core 内）
    对标：garnet/libs/storage/tsavorite/cs/src/core/
    问题：单消费者薄 crate
    改法：并入 wkv（wconn 对位 libs/client 保留，其余小 crate 定位成立）

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
