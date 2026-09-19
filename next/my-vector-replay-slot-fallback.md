优先级：中

问题
从库向量 AOF 回放经 logic_domain_of 反查逻辑域，未在册即回退物理号参与定槽，槽位算错。回放向量条目时以 logic_domain_of(vns, vdb) 取逻辑域再 slot_of 定槽挂载向量索引；从库路由表未装载该物理库（冷租户快照未装载、或映射分叉）时反查按物理号原样返回，物理号混入本应逻辑域输入的 slot_of 产出错误槽位，向量索引挂错集群槽位，后续按槽寻址取不到。

取证（dev 当下代码重取）
wedb/wnode/src/aof/aof_processor.rs:940-948 `let (slot_ns, slot_db) = session.batch.store().vdb.logic_domain_of(vns, vdb); let repl_slot = slot_of(slot_ns, slot_db);` 后投递 replay_vector_set_index/replay_vector_set_add。wedb/wkv/src/vdb.rs:857-872 logic_domain_of 两腿反查 `unwrap_or(vns)`/`unwrap_or(vdb)` 回退物理号；:853-856 注释自述「冷租户快照未装载即无格可查…物理号即其唯一身份」。wedb/wbase/src/hash_slot.rs slot_of 库级定槽单点。

C# 对标
garnet/libs/cluster/Server/Migration/VectorSetMigration.cs（C# 槽位按 key CRC16 全键空间；库级定槽为 rust 自定义，SKILL「集群以 namespace -> db 为唯一分片」）。

修法建议
向量 AOF 条目在入账侧直接持久化原始逻辑域（或槽位）随条目走，回放端免逆向表反查（逆向表是易失推导量，从库冷态/分叉态不可靠）；或回放前强制装载该 vns 路由快照并失败即留痕重试，禁静默回退物理号。与 next/my-flush-replica-virtual-id-divergence.md 同根（从库映射继承缺口）：该票修好后本条仍需独立确认冷装载时序与回退分支删除。
