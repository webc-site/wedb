优先级：高

问题
从库回放换号条目时本地独立取新虚号，主从映射体系必然分叉。FlushDb/FlushNs 广播条目载荷只携换号前旧域 (vns, old_vdb)；从库 flush_db_virtual 经本地 alloc_next_virtual_id 生成新虚库号换格。「主从分配水位保持同步」仅是注释假设：虚号计数器是节点本地 AtomicU64，凡有一侧发生不经 AOF 镜像的分配（主库 resolve_context 为新租户/新库冷装载取号、从库自身会话解析取号），两侧计数即永久错位。此后主库数据条目落主库新号、从库路由格指从库本地号，从库按 logic_db 读不到主库新数据，failover 提升后整库视图错位。且 service.rs 的 AOF 写端口按标签排除 KeyTag::DbMeta，主库磁盘映射完全不向从库镜像，从库也无从装载收敛。

取证（dev 当下代码重取）
wedb/wnode/src/database/single_database_manager.rs:294-307 flush_database -> safe_flush_aof(AofEntryType::FlushDb, vns, domain_db=旧 vdb)（载荷 16B 仅旧域，aof_processor.rs:1163-1171 parse_flush_domain）。wedb/wkv/src/vdb.rs:1046-1080 flush_db_virtual :1060 `alloc_next_virtual_id()` 本地取号换格，:1037-1039 注释自称「主从分配水位因此保持同步」；:715-717 alloc_next_virtual_id 节点本地计数器；:747-759 get_or_create_ns 与 :773 起 get_or_create_db（resolve_context 冷装载分配路径）不经 AOF。wedb/wnode/src/aof/aof_processor.rs:572-617 回放臂调 flush_virtual_database/flush_virtual_namespace（即上述本地取号路径）。wedb/wnode/src/service.rs:173 on_aof_store_event `tag != KeyTag::String && tag != KeyTag::Acl` 即返回，DbMeta 不镜像。

C# 对标
garnet/libs/server/Databases/SingleDatabaseManager.cs:SafeFlushAOF（C# 单租户 databaseId 1 字节、无虚号域，条目即完整域语义）；rust 多租户虚号域是 SKILL「虚拟数据库 ID 映射与秒级清库」自定义设计，doc/zh/db.md 明文「从库完全继承主库的映射体系，不进行本地二次映射」。

修法建议
FlushDb/FlushNs 条目载荷补齐主库分配的 (new_vns, new_vdb)，回放侧改用主库号直设映射格（insert_db_mapping 同格换指 + 旧域照旧判死），本地取号仅留作无新号旧条目的兼容回退并可随「不需向下兼容」原则直接删除；同时决定 DbMeta 是否开镜像（或以条目承载足够信息使镜像非必要）。联动 next/my-swapdb-replica-routing-sync.md（同一映射继承缺口的 SWAPDB 面）与 next/my-vector-replay-slot-fallback.md。
