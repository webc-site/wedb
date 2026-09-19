重启或检查点基线后回放向量条目：非根租户路由表按 0 常驻条款不装载，向量定槽反查回退物理号（可复现用例票）

优先级：中

判定
票面原主张（从库实时复制流反查失联致静默错槽）已核销——同根票 task/done/my-flush-replica-virtual-id-divergence.md 的 DbMeta 镜像承接已覆盖该路径。收窄后的残余成立面是「重启 / 检查点基线后回放」这同一回退分支，静态可读通但无既有用例覆盖，须实装用例取证，留开发队列。

问题
向量族回放按 logic_domain_of(vns, vdb) 反查逻辑域再 slot_of 定槽（库级定槽为 rust 自定义单点：SKILL「集群以 namespace -> db 为唯一分片」.agents/skills/transpile/SKILL.md:35，doc/zh/db.md:302-304「同一个 DB 是同一个槽位」）。该反查的 DB 腿只在本租户库级路由表在册时有效，而两条装载事实相冲：
1 重建面对非根租户路由表刻意不装载：store/mod.rs:534-552 自述「非根域库级路由表按 0 内存常驻条款不装载（首访经 resolve_context 点查回建）」，DbMap 臂仅对 ROOT_VIRTUAL_ID 执行 insert_db_mapping（:586-597 DbSwap 臂同口径）；ns 腿走 active_vns 逆表，重建期 0x01 记录全量装载，恒在册。
2 回放侧版本闸先于 DbMeta 分派：record_gate.rs:25-27 判 store_version 低于恢复基线即旧代条目，aof_processor.rs:802-814 命中即 return Ok，而 DbMeta 镜像臂在 replay_op 之内（:836-841）——早于基线的映射条目一律不被应用，apply_dbmeta_record 的非根域补格（keyspace.rs:204-209）因此不生效。
两条叠起，节点重启或副本自检查点恢复后重放基线之后的向量条目时，该租户路由表为空，DB 腿 unwrap_or(vdb) 回退物理号，slot_of(logic_ns, vdb) 不等于在线面 slot_of(logic_ns, logic_db)；该错值经 create_index_locked 一次性盖章进登记表 context 的 hash_slot（vector_manager_locking.rs:475-501，:482），且既有登记命中即几何参数原样放行、无换槽路径（:445-457），此后按槽寻址与按槽迁移枚举（vector_manager_context_metadata.rs:100-115 按 hash_slots 过滤）取不到该向量集，整键在槽级操作中漏发。缺口只在 DB 腿，ns 腿不受影响。

取证（现刻 HEAD dev 965e16d 行号）
回退分支与设计自述：wedb/wkv/src/vdb.rs:865-893（:877-891 两腿 unwrap_or，:872-876「冷租户快照未装载即无格可查…物理号即其唯一身份」）。
回放面唯一消费者：wedb/wnode/src/aof/aof_processor.rs:978-985（logic_domain_of → slot_of → replay_vector_set_index/replay_vector_set_add）；:1217-1231 KeyContextGuard 头注「逻辑槽全程不触碰…需要逻辑域的槽位口径经 logic_domain_of 逆向表反查真值」，且守卫与回放全程零逻辑解析、不点查装载（全文件无 resolve_context 调用）。
非根域不装载：wedb/wkv/src/store/mod.rs:534-552、:586-597。
版本闸先于 DbMeta 应用：wedb/wnode/src/aof/record_gate.rs:25-27；wedb/wnode/src/aof/aof_processor.rs:802-814、:836-841。
镜像补格仅在条目被应用时生效：wedb/wkv/src/store/keyspace.rs:188-237（:194-196 注释「副本该租户活跃时内存格已在册」即预设暖表）；放行端 wedb/wnode/src/service.rs:174-188。
重启装配顺序（向量域先于重放，故重放确走向量承接面）：wedb/wnode/src/service.rs:1257-1277。
错槽不可自愈：wedb/wnode/src/resp/vector/vector_manager_locking.rs:445-457、:475-501；按槽枚举 wedb/wnode/src/resp/vector/vector_manager_context_metadata.rs:100-115。
既有用例形态不覆盖本缺口：wedb/wnode/tests/aof_replay_domain.rs:543-620（replay_face_slot_matches_online_face_slot 先经 resolve_context 真映射装载再回放，只证暖表两端同值）；wedb/wnode/tests/vector_replication_replay.rs:19（SLOT0 = slot_of(0,0) 根域，逻辑域即物理域，反查永不失配）。

可复现用例（落地棒先做这一步，证伪即撤票转 reject）
在 aof_replay_domain.rs 现有骨架上加一例，全用既有公开原语，不改生产码：
1 open_node 起节点，resolve_context(NS_A, DB_A) 取 (vns, vdb)，在线面以 slot_of(NS_A, DB_A) 建非空向量集（复刻 :548 用例前段，含 VectorAofSink 装配）。
2 拍一次检查点抬升 store_version 基线，使该租户的 DbMap 镜像条目落入基线以下（既有拍检查点入口，勿自造）。
3 释放存储句柄，以同一 device 与 checkpoint_dir 重开节点（重建装载形态，期间绝不点查该租户），重放基线之后的向量条目，重放器换全新 VectorManager（登记表为空）。
4 断言重放登记 context 的 hash_slot 等于 slot_of(NS_A, DB_A)；若等于 slot_of(vns, vdb) 即回退实锤。再以 get_namespaces_for_hash_slots(在线槽) 断言可枚举出该登记前缀。
若第 3 步实际执行时该租户路由表已被别的路径暖到（恢复期存在先于向量重放的 resolve_context 点查、或重建策略另有装载臂），本票不成立，转 reject 并记明暖表来源与行号。

修法方向（用例成立后择一，一处定义不并存两套）
甲 回放面在反查前按条目 vns 强制点查装载该租户路由快照，复用 wedb/wkv/src/store/vdb_load.rs 既有磁盘点查内核（不建第二套装载机制），装载未命中即显式上抛留痕——与恢复路径显式暴露原则同口径，禁静默回退。
乙 若裁决认为重启面 hash_slot 无须与在线面同值，则须把回退从静默改为可诊断：向量定槽面 DB 腿未命中即失败上抛，并在 vdb.rs:865-876 自述里改记该口径，杜绝「无逻辑入口即身份」与「回放面唯一反查入口」两种口径同时挂在一个函数上。
原票建议的「条目载荷持久化原始逻辑域或槽位随条目走」不采：违 doc/zh/db.md:203「从库完全继承主库的映射体系，不进行本地二次映射」与条目键一律物理的既定形态，并与 6dc1cb6 建立的映射同步唯一通道构成第二套机制。

原票面核销记录（本波轻裁逐条复核，现刻 HEAD）
1 「从库回放向量条目时路由表未装载该物理库（冷租户快照未装载、或映射分叉）→ 反查回退 → 向量索引挂错集群槽位」：不成立，已被并发合入的 6dc1cb6/2b6a4d0 承接（两提交均为 HEAD 祖先，git merge-base 已验）。主库全部 DbMeta 落盘（首映射/换号批/SWAPDB/墓碑注销）经 service.rs:174-188 放行镜像，从库 aof_processor.rs:836-841 交 keyspace.rs:188-237 apply_dbmeta_record 应用，其中 :204-209 专为非根域在册格补 insert_db_mapping，映射条目在 AOF 全序中恒先于该域任何数据条目，故实时复制流与「副本重启后映射从本节点磁盘装载，与 AOF 截断位点解耦」（keyspace.rs:182-184）的暖表形态下反查必在册；回退分支自身注释（vdb.rs:872-876）把未在册裁决为「本节点无逻辑入口，物理号即其唯一身份」，与根域恒等形态同形，属已裁决设计而非缺口。
2 「C# 对标 garnet/libs/cluster/Server/Migration/VectorSetMigration.cs」：论据虚构。find . -name "VectorSetMigration*" 全库零命中，该目录实际只有 MigrateSession*.cs、MigrationDriver.cs、MigrateState.cs、Sketch.cs 等；C# 键级 CRC16 与 rust 库级定槽的形态差异另见 task/reject/my-sketch-key-hash-comment.md，不构成本条论据。票面行锚亦已漂移（原引 aof_processor.rs:940-948、vdb.rs:857-872，现刻为 :978-985、:865-893）。
3 票面修法后半「或回放前强制装载该 vns 路由快照并失败即留痕重试，禁静默回退物理号」：这半条即上面甲方向，仅在重启/基线后回放面成立，本票射程据此收窄；「与 my-flush-replica-virtual-id-divergence.md 同根、该票修好后本条仍需独立确认冷装载时序与回退分支删除」的自述，与本波裁决一致，残余即冷装载时序面。
