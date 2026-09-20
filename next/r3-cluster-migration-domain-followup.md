问题：槽位迁移驱动的库级分片域覆盖（r3-cluster 止血段的全量收尾）

背景与本票已落地的部分

本仓已切换到库级定槽分片模型，槽位由 `wbase::hash_slot::slot_of(namespace, db)` 单点复合而来，
一个槽位对应的数据是命中该槽的全部逻辑库，而不是某一个库。`wedb/src/server/cluster_session/slot_mgmt.rs`
里的 `for_each_db_in_slot` 是读侧已经确立的正确范式：枚举 `store.vdb.list_logic_dbs()`，按 `slot_of` 过滤，
逐域 `session.set_context(ns, db)` 后开会话再回调。CLUSTER 的 COUNTKEYSINSLOT 与 GETKEYSINSLOT 慢路径
已经走这条聚合链，槽位视图在语义上是全域的。

迁移链则没有。源端驱动 `wedb/src/server/migration/migrate_driver/slots.rs` 的 `execute_slots_migration`
与 keys 臂 `wedb/src/server/migration/migrate_driver/keys.rs` 都是 `store.new_session()` 之后直接
`StorageSession::new(batch)`，从不 `set_context`，因此会话恒在默认域 `(0, 0)`。键的收口判据在
`wnode/src/storage/session/common/array_key_iteration_functions.rs` 里是
`if slot != self.session_slot() || key_count == 0 { return Ok(()) }`，非默认域会话取默认域槽位恒空；
接收端 `wedb/src/server/migration/frame_import.rs` 的 `import_migration_frames` 在
`accept_domain_frames = false` 时对 kind=7 落域上下文帧与 kind=8 DbMeta 帧一律显式拒绝
（`"ERR Unexpected domain frame in migration payload"`），MIGRATE 链在
`wedb/src/server/cluster_session/migrate.rs` 正是这个开关的假臂。两端合起来意味着：非默认域的键
既扫不到、也落不下。

本次已在 `wedb/src/server/cluster_session/mod.rs` 立了 `MIGRATION_SUPPORTED_SLOT` 与
`migration_slot_supported` 这一处门禁单点，并在 `migrate.rs` 的 `collect_migrate_slot`（SLOTS 与
SLOTSRANGE 臂）与选项循环后的 KEYS 臂、以及 `slot_mgmt.rs` 的 SETSLOT 与 SETSLOTSRANGE MIGRATING 臂
接到解析期显式拒绝，文案由 `wresp::cmd_strings::migration_slot_domain_error` 单点模板渲染。
这属于止血：把静默空迁交权变成显式失败，代价是除默认域 `(0, 0)` 之外任何库的槽位都不能迁移。
全量落地的判据之一就是把这些门禁连同其注释一并删除。

剩余工作的具体落点

源端逐域扫描与传输，改 `slots.rs:execute_slots_migration`。当前它自建一个默认域全功能会话并在
`for slot in sorted_slots` 外层循环里反复 `get_keys_in_slot`。要把它改成域优先的嵌套推进：外层仍是
槽序稳定推进，内层枚举 `store.vdb.list_logic_dbs()` 中 `slot_of(ns, db) == slot` 的域，对每个域开一个
带 `set_context(ns, db)` 的全功能 `StorageSession`（注意 `for_each_db_in_slot` 目前用
`StorageSession::new_readonly`，迁移源端在同一会话内既有读也有 DELETING 收口的删键，不能直接复用只读版，
需要把 `slot_mgmt.rs:for_each_db_in_slot` 提为对上层可见的共用件并参数化读写模式，或者抽出一个接受
会话构造闭包的变体，判据与域过滤逻辑绝不允许出现第二份）。每个域内部保留现有的
`untouchable` 游标剔除循环、`RangeIndexManagerMigration::get_range_index_keys_for_migration` 取带外键、
`sketch.set_status(Transmitting)`、`hash_and_store`、`bump_and_wait_for_epoch_transition_async` 纪元静止与
分块传输节奏，只是这些都必须在域上下文中执行。`wkv_session` 目前只在默认域上取 RI，也要随域重建。

域上下文的承载，改帧协议使用面而不是帧格式。kind=7 `MigrationDomainContext { vns, vdb, ns, db }`
与 kind=8 DbMeta 已在 `wconn/src/record.rs` 定义并被无盘全量同步快照链
（`replication/diskless_replication/replication_snapshot_iterator.rs`）生产与消费，MIGRATE 链要放行：
`migrate.rs` 接收壳里 `accept_domain_frames` 由 `false` 改为 `true`，并在每槽迁移开始时（即
`begin_migration_phase` 之后、首个记录帧之前）发送该槽涉及的域上下文帧，目标端 `frame_import.rs`
已有的 `session.set_virtual_context(ctx.vns, ctx.vdb)` 与 `vector_slot = slot_of(ctx.ns, ctx.db)` 换域
路径可直接复用。这里要一并确认 `prefix_cache` 置空不变式在换域后成立，接收端记录写入的会话前缀
不能残留上一域的外提结果。同时 DbMeta 映射帧要在迁移开始时随域发出，保证目标端逻辑域到物理域的
映射一致（否则目标端 `slot_of` 复算会落错物理域）。

`MigrateTaskSpec` 与不可迁移键声明。`wedb/src/server/migration/migrate_session.rs` 的 `MigrateTaskSpec`
目前不携带 ns/db，也不应携带：迁移任务的单位是槽位，域是槽位内部的枚举结果，把域塞进任务粒度会让
CLUSTER SETSLOT 的槽位状态机与任务生命周期错位。请在改动前先确认这一点，若确实不需要新字段就不要加。

带外通道的随域化。RI 流（kind=4）与向量集帧（kind=5/6）现在都在默认域上下文中产生与登记，
逐域改造后其对象句柄、`VADD` 落库槽位、以及 `get_range_index_keys_for_migration` 的取值会话都要跟着
换域。分层集合升阶的键也要一并核对。

交权与收口的域完整性。`begin_migration_phase` / `end_migration_phase`、`relinquish_ownership`、
DELETING 阶段的源端删键、以及 `migrate_session.rs` 的收口路径，都要确认在全部域完成传输后才交权；
任一域失败时按现有 recover 语义保留源端键，不得出现部分域已交权、部分域键滞留的中间态。

验收标准

第一，删除 `cluster_session/mod.rs` 的 `MIGRATION_SUPPORTED_SLOT` 与 `migration_slot_supported`，
删除 `migrate.rs` 的 `MigrateParseErr::SlotDomain` 臂与两处门禁调用、`slot_mgmt.rs` 的两处门禁调用，
删除 `wresp/src/cmd_strings.rs` 的 `GENERIC_ERR_MIGRATION_SLOT_DOMAIN` 与
`migration_slot_domain_error`。仓库里不残留任何与迁移域相关的拒绝分支。

第二，`wedb/tests/cluster_migration_domain.rs` 的
`slots_migration_non_default_db_must_not_hand_off_silently` 用例改造为正向断言（当前它是
「要么显式拒绝、要么真实移交」的复现锁，且 `slots_migration_default_db_still_migrates` 已覆盖默认域不被误伤），
并补齐：非默认库放键后发起该槽 SLOTS 迁移，目标端在同一个非默认库可见该键且值与 TTL 一致，
源端该键消失，槽位状态最终 STABLE，`CLUSTER COUNTKEYSINSLOT` 在迁移前后两侧守恒。

第三，非默认域的 RI 键、分层集合键、向量集键各补一条随域迁移用例，键在目标端可读且结构完整。

第四，多域同槽用例：构造两个不同的 `(ns, db)` 使 `slot_of` 命中同一槽位，两域各放一键，
对该槽发起迁移后两键都在目标端可见，证明按域枚举而非按库名单点。

第五，`cargo check --workspace` 零警告，`cargo nextest run -p wedb` 全绿，`CLUSTER SETSLOT ... MIGRATING`
在非默认域槽位上恢复正常受理路径。
