# ds.data 待办

2. [P2] CONFIG SET 内存尺寸/索引参数恒错降级
   位置：wedb/wnode/src/resp/config_commands.rs:289-307（handle_memory_size_change）、:314-332（handle_index_size_change）
   对标：garnet/libs/server/ServerConfig.cs:HandleMemorySizeChange、ServerConfig.cs:HandleIndexSizeChangeAsync（GrowIndexAsync 成功即生效）
   问题：main-log-memory / read-cache-memory 格式校验后无条件报 tracker not running，index 校验后无条件报 grow failed；C# 三者是真实可设参数。
   改法：wkv StoreSession 暴露日志尺寸变更与索引在线扩容通道后接线（索引扩容本体归 next/db.md N1）。
3. [P2] DEBUG PANIC / FLUSHANDEVICT 降级报错且 HELP 文案保留虚假承诺
   位置：wedb/wnode/src/resp/admin_commands.rs:352-356（PANIC 回 RESP_ERR_GENERIC）、:385-397（FLUSHANDEVICT 回 RESP_ERR_GENERIC）、:451-470（DEBUG_HELP 仍列 FLUSHANDEVICT :458、PANIC :466）
   对标：garnet/libs/server/Resp/AdminCommands.cs:742-744（PANIC 真抛 GarnetException）、AdminCommands.cs:772-790（真 FlushAndEvict 回 OK head=.. tail=..）
   问题：HELP 承诺的两条子命令执行必失败；FLUSHANDEVICT 在 C# 是可观测真实语义（HeadAddress 抬至 TailAddress）。
   改法：wkv 暴露 flush_and_evict 接线 FLUSHANDEVICT，HELP 文案按真实能力裁剪；PANIC 属 rust 禁 panic 刻意偏差，留档即可。
4. [P2] DBID 校验常量硬编码，集群分支死代码
   位置：wedb/wnode/src/resp/admin_commands.rs:38（const MAX_DATABASES: i64 = 16）、:41（const CLUSTER_ENABLED: bool = false）、:614-636（try_parse_database_id）
   对标：garnet/libs/server/Resp/AdminCommands.cs:TryParseDatabaseId
   问题：CLUSTER_ENABLED 恒 false 使集群禁非零 DBID 分支永不可达；MAX_DATABASES 不读运行时选项，用户配更多库（如 32）时 SAVE/BGSAVE/LASTSAVE/COMMITAOF/EXPDELSCAN 的 DBID 被误拒。
   改法：try_parse_database_id 改读会话已装配的 runtime_config 与集群门。
