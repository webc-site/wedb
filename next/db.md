# db 待办

1. [P1] windex 无在线索引扩容：满载即 OverflowPoolExhausted，对标 C# GrowIndexAsync/SplitIndex 缺失
   位置：wedb/wkv/src/store/mod.rs:257（open 时 HashIndex::new(config.index_size) 定容）；wedb/wkv/src/store/mod.rs:187（check_index_capacity 恢复预检强制等容）；wedb/windex/src/overflow_pool.rs:38（MAX_CHUNKS=4096，池顶 2^22 桶）；wedb/windex/src/error.rs:22（OverflowPoolExhausted）
   对标：garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:850 GrowIndexAsync；garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/SplitIndex.cs
   问题：主表打开定容且运行期无任何 grow/resize/split 路径（windex 全 crate grep 扩容语义零命中）；键数持续增长 → 负载因子升高 → 溢出链拉长点查退化 → 溢出池耗尽写路径报错，C# 同场景 epoch 保护在线桶分裂继续服务。
   改法：对标 SplitIndex.cs 实现 epoch 保护的在线桶分裂（wepoch drain 语义与桶原子条目已齐，可承载新旧表切换）；最低限度把「容量上限 + 满载行为」升级为显式运维约束（容量水位监控 + OverflowPoolExhausted 前置预警），写进 SKILL「有意差异」清单留档。

2. [P2] AOF 回放主动漂移扫描配置面缺失：机制已实现，选项域无三参，永远关闭
   位置：wedb/wnode/src/aof/garnet_append_only_file.rs:224（建 ReadConsistencyManager 时阈值硬编码 -1，注释自认缺口）；机制本体已齐：wedb/wnode/src/aof/readconsistency/read_consistency_manager.rs:50
   对标：garnet/libs/server/Servers/GarnetServerOptions.cs:128 AofReplayDriftThreshold、:139 AofReplayDriftCheckFreq、:1255 ProactiveReplayDriftCheckEnabled；garnet/libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:36
   问题：漂移机制（proactive/reactive 双模式、窗口步长、跨子日志栅栏）已 1:1 落地且默认行为与 C# 一致（C# threshold 默认 -1 同为关闭），但 rust 选项域未暴露三参数，部署侧无法开启主动扫描。
   改法：配置面补 AofReplayDriftThreshold / AofReplayDriftCheckFreq / ProactiveReplayDriftCheckEnabled 并接到 create_or_update_key_sequence_manager 调用点；若判定不暴露，进 js/check/ignore 留档。
