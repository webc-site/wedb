优先级：中

问题
SWAPDB 零 AOF 入账，从库对换库完全无感知。主库 swap_databases 仅本地单元格互换虚库 ID 并把 DbSwap 成对记录写本地 DbMeta；AofEntryType 无 SwapDb 变体，swap_command_slow 全链不入队，service.rs 又拦截 DbMeta 镜像。从库路由表保持旧指向，主从数据库视图对调失步；failover 后换库效果反转。C# 在集群模式直接拒绝 SWAPDB，不存在此暴露面；rust 按 SKILL「删除 allow_multi_db 与集群限制，全模式支持自由切库」放宽后，换库传播成为自定义点的必备下游，当前缺失。

取证（dev 当下代码重取）
wedb/wkv/src/session/swap.rs:33-94 swap_databases（换格 + bump_generation + persist_dbmeta_batch([DbSwap, DbMap x2])，无任何 AOF 事件）。wedb/wnode/src/resp/garnet_api/slow.rs:964-986 swap_command_slow（仅调 swap_databases，无入账）。wedb/waof/src/aof/entry_type.rs:9-49 AofEntryType 无 SwapDb。wedb/wnode/src/service.rs:173 DbMeta 不入 AOF 镜像。

C# 对标
garnet/libs/server/Databases/MultiDatabaseManager.cs:674-717 TrySwapDatabases（无 AOF 条目）；garnet/libs/server/Resp/ArrayCommands.cs:170-173 NetworkSWAPDB 集群模式拒绝（RESP_ERR_GENERIC_SWAPDB_CLUSTER_MODE）——C# 以禁用规避，rust 放宽即须补传播。

修法建议
AofEntryType 新增 SwapDb（载荷 vns + 双逻辑库 + 双换后 vdb），swap_databases 锁内换格成功后入队；aof_processor.rs 补回放臂直设两格映射（复用 insert_db_mapping，不本地取号）。与 next/my-flush-replica-virtual-id-divergence.md 同根同修向（从库映射继承），可同棒落地但分两 commit。
