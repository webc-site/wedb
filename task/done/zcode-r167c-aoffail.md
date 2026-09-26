甄别结论：通过（甄别席 zc-fix-r16-aoffail，2026-09-26）定级 P1
核验记录（逐锚现码复跑）：
案一 rust 锚成立：wkv/src/range_index/ops.rs range_index_create :125-138、range_index_set :206-219、range_index_set_batch :303-317、range_index_del :457-470 皆 if let Err + log::error! 后无条件回 Ok；drain.rs:72-78 handle_bftree_drain_and_delete 同款吞错回 Ok(())；error.rs:91-94 AofEnqueue 契约原文「调用方须以错误拒绝该命令防主从发散」亲验；promote.rs:160-179 与 migration.rs:386-407 确为 return Err 冒泡臂，同域双标并存坐实。
案二 rust 锚成立：vector_manager_replication.rs:286-298 replicate_vector_set_rename 返回 () 且 :291 let _ = sink.enqueue 弃错；调用票面锚名微漂——实为 slow.rs:308 rename_vector_set_slow（票面写 rename_slow_path），:328 调用后 :330 直回 +OK 零检查，行为成立。
案三行为成立、锚名三处漂移：吞错实在 compact.rs:350 WedbCompactionFunctions::on_dropped 内 :390-399（票面函数名 LogCompactor::compact_record 不存在，全库零命中），吞错后 :417 回 Ok 紧缩继续前推；C# 锚实为 core/Compaction/TsavoriteCompaction.cs:21 Compact（票面写 Compaction/Compaction.cs，路径漂、目录与方法在现树亲验存在）；本臂既有失败臂 :401-406 已 return Err 保留记录，修复循单机制不新造。
C# 侧核验：GarnetLog.cs Enqueue :637 存在，其下 TsavoriteLog.cs:1311 为 void Enqueue 无吞错路径、故障沿调用栈上抛至 RESP 层报错，与 rust swallow 分歧成立；UnifiedStoreOps.cs 锚名 Rename 实为 RENAME（:220 存在）；「日志先行」注释实际出处为 Storage/Session/Common.cs:56（票面归至 RangeIndexOps.cs:30-32 系注释归属小误，不构成反证）。
非重复：doc/zh/deviations.md 全册 grep「入队失败/emit_event/RangeIndexDrop/aof」无本轴在册偏差或保留裁决；task/ing 空、task/reject 与 task/issue 及 todo 各票无同轴并案（同批 zcode-r167c-watchver 系 WATCH 版本轴，不涉 AOF 入账）。
架构合规：修复即向 error.rs:91 既有 AofEnqueue 契约与 promote/migration 既有冒泡臂收敛，单一机制、最小改动、无假桩无过度设计；缺陷现码仍存续，供 task/fix.md 直接消费，修复时请以现树真实函数名 on_dropped / rename_vector_set_slow 为准。

审核结论：通过，定级 P1。
确证 RangeIndex 增删改及树排空、向量集重命名、日志紧缩三处 AOF 追加失败时静默吞错回复成功，破坏 WAL 日志先行原则并导致主从发散与重放丢数据。执行方案完备，供 task/fix.md 直接消费。
合入哈希：17d823c 收口形态：ops.rs RI create/set/批量/del 四臂与 drain.rs 排空删键臂按 error.rs AofEnqueue 契约上抛拒绝命令（drain 尾臂经 Swapped 分级保 WATCH 判据）、向量 RENAME 合成条目 replicate 返回 Result 且 slow.rs 臂冒泡禁 +OK 冒答、compact.rs on_dropped 改日志先行（Drop 先入账后注销，入队失败树不动记录保守保留下轮重试必达），三处告警吞错死码删除，新增 wkv/wnode 真故障注入回归两文件（7+1 用例全绿，既有 rename/tiered/compact/vector 套件零回归）。

AOF 日志追加失败处理与一致性边界审查提案

审查背景与边界核验结论：
针对 AOF 安全截断边界与恢复重放幂等性进行了深度核验：
1. 快照安全截断边界方面：database_manager_base.rs:357 在 store.begin_version_shift 前原子锁定 covered = aof.log().tail_address()，并在快照元数据持久化落盘后才调用 aof.truncate_until_async(&covered)，截断严格以快照覆盖位点为下界，边界清晰安全。
2. 恢复重放幂等性方面：record_gate.rs 实现了 entry_address < aof_floor 与 header.store_version < store_version 双重位点与版本屏障，主存操作采用 StoreUpsert 终值记录，对象与范围索引具备覆盖幂等性，重放边界安全。
但在 AOF 日志追加失败时，发现 3 处静默吞错并向调用方误报成功的严重缺陷，破坏了“日志先行、入队失败必须拒绝客户端请求防数据发散”的核心持久化原则。

案一：RangeIndex 增删改及树排空写操作在 AOF 追加失败时静默吞错并返回成功，导致主从数据分歧与崩溃重放数据丢失

问题分析：
1. Garnet 契约对齐：C# Garnet 在 RangeIndexOps.cs 与 PrivateMethods.cs:WriteLogRMW 中，范围索引变更通过 GarnetLog.cs:Enqueue 入队 AOF，一旦发生写入失败或日志异常，立即向上抛出异常终止当前写事务，会话向客户端返回错误响应。Rust 规范在 wedb/wkv/src/error.rs:91 中同样明文规定 AofEnqueue: 调用方须以错误拒绝该命令防主从发散，且在 promote.rs:160 与 migration.rs:386 中均严格向外冒泡错误。
2. 工程现状确证：wedb/wkv/src/range_index/ops.rs 与 drain.rs 中，范围索引的创建、单键写入、批量写入、删除以及整树排空清空逻辑中，调用 self.store.emit_event() 返回 Err(e) 时，均采用 if let Err(e) = self.store.emit_event(...) { log::error!(...); } 模式，仅记录日志便静默吞没错误，随后无条件返回 Ok(())、Ok(inserted) 或 Ok(true)。代码注释声称“复制/重建面可自树状态收敛”，该假设完全背离系统架构：副本节点（Replica）并不共享主节点的内存与底层 B-tree 物理文件，从节点必须且仅能依赖 AOF 增量复制流同步变更；崩溃恢复重放亦完全依赖 AOF。
3. 逻辑危害确证：当磁盘写满、AOF 环形缓冲积压或 IO 故障导致 emit_event 失败时，主库内存中树索引已经变更且直接向客户端返回执行成功，但 AOF 日志中完全遗失对应操作。导致：① 副本节点无法通过 AOF 增量重放获取变更，主从 RangeIndex 数据严重发散；② 主库一旦崩溃重启，从 AOF 重放恢复后该部分写入彻底丢失；③ 破坏 WAL“日志必须先于确认落盘/入队”的基本一致性契约。

涉及代码：
rust 文件与函数：
wedb/wkv/src/range_index/ops.rs:StoreSession::range_index_create
wedb/wkv/src/range_index/ops.rs:StoreSession::range_index_set
wedb/wkv/src/range_index/ops.rs:StoreSession::range_index_set_batch
wedb/wkv/src/range_index/ops.rs:StoreSession::range_index_del
wedb/wkv/src/range_index/drain.rs:StoreSession::handle_bftree_drain_and_delete

对应 c# 文件与函数：
garnet/libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexCreate
garnet/libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexSet
garnet/libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexDel
garnet/libs/server/Storage/Functions/MainStore/PrivateMethods.cs:WriteLogRMW
garnet/libs/server/AOF/GarnetLog.cs:Enqueue

精炼执行方案：
1. 统一移除 range_index/ops.rs 与 drain.rs 中对 emit_event 错误的 swallow 模式，使用 ? 操作符将 AofEnqueue 错误向外冒泡至外层命令执行入口，由 RESP 框架向客户端返回具体错误信息。
2. 对于树排空 handle_bftree_drain_and_delete，若 AOF 入队失败，同样中断执行并上抛错误，避免在 AOF 未持久化记录时推进物理树的清空与销毁。
3. 测试验证点：在 AOF 处于只读或注入入队失败故障时，发起 ZADD/ZREM 等范围索引写请求，断言请求明确返回错误码，且主节点拒绝写入或在入队失败时立即回滚内存修改，确保主从和重放强一致。


案二：向量集重命名 AOF 合成条目追加失败被无条件丢弃并回复成功，导致从节点及重启重放静默丢失重命名状态

问题分析：
1. Garnet 契约对齐：C# Garnet 在 UnifiedStoreOps.cs:Rename 中，键与元数据重命名作为关键元数据变更记入 AOF（UnifiedStore/PrivateMethods.cs:WriteLogRMW），任何 AOF 追加失败均向外报错，绝不允许在持久化失败的情况下向客户端单方面确认成功。
2. 工程现状确证：wedb/wnode/src/resp/vector/vector_manager_replication.rs 中，replicate_vector_set_rename 函数构建向量集重命名哨兵条目后，使用 let _ = sink.enqueue(...) 强行忽略 AOF 追加错误，该函数返回值直接定义为 ()。而在 wedb/wnode/src/resp/key_admin_commands/slow.rs:rename_slow_path 中，调用完该函数后完全无错误检查，直接向客户端回复 +OK\r\n。
3. 逻辑危害确证：当 AOF 队列出现故障或反压丢弃时，主库内存中 VectorManager 已将向量集旧名称解绑并绑定到新名称，但副本节点与故障重启重放端由于未收到该 AOF 条目，根本不知道发生了 RENAME。副本节点上旧键依然存在且新键缺失，后续所有向从库发起的新键向量查询（如 VSEARCH）均会报 KeyNotFound；主库崩溃重启后向量集名称也将丢失，主从状态永久分裂。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/vector/vector_manager_replication.rs:VectorManager::replicate_vector_set_rename
wedb/wnode/src/resp/key_admin_commands/slow.rs:rename_slow_path

对应 c# 文件与函数：
garnet/libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:Rename
garnet/libs/server/Storage/Functions/UnifiedStore/PrivateMethods.cs:WriteLogRMW
garnet/libs/server/AOF/GarnetLog.cs:Enqueue

精炼执行方案：
1. 将 replicate_vector_set_rename 返回类型修改为 Result<(), StorageError>，内部移除 let _ = 忽略逻辑，显式将 sink.enqueue(...) 的错误上抛。
2. 在 rename_slow_path 中增加对向量复制同步结果的错误传播，若 AOF 条目追加失败，停止向客户端回复 +OK，并向客户端返回对应错误响应，必要时回滚内存中的名称映射。
3. 测试验证点：构造 AOF 入队失败测试桩，执行 RENAME 操作，验证当向量重命名 AOF 追加失败时命令返回错误响应，且从节点不会产生孤儿旧键或缺失新键。


案三：在线日志紧缩清理废弃分层树时 RangeIndexDrop 写入失败吞错，致副本孤儿树资源永久泄漏

问题分析：
1. Garnet 契约对齐：C# Tsavorite 紧缩引擎 Compaction.cs:Compact 中，紧缩产生的墓碑或修剪标记向日志追加时，一旦发生日志入队或 IO 故障，紧缩任务立即异常中断，禁止在关键释放日志遗失的情况下继续向前推进日志截断点。
2. 工程现状确证：wedb/wkv/src/compact.rs:LogCompactor::compact_record 中，当紧缩扫描到已被标记删除/排空的分层树记录时，会调用 self.store.emit_event(RangeIndexDrop) 广播销毁该树。当 emit_event 返回 Err(e) 时，代码仅 log::error!(...) 记录错误，没有向紧缩调用循环传播错误，紧缩流程继续向前移动日志位点并执行物理截断。
3. 逻辑危害确证：紧缩位点正常前推并截断历史日志，而 AOF 却永久遗失了 RangeIndexDrop 清理事件。副本节点依据 AOF 重放推进时，永远无法获知该分层树已被完全释放，导致副本节点的内存和磁盘中永久残留废弃的孤儿 B-tree 物理文件与内存结构，造成句柄与内存泄露；此外，当后续以相同名称再次创建分层树时，从节点将因本地孤儿树残留而发生创建冲突。

涉及代码：
rust 文件与函数：
wedb/wkv/src/compact.rs:LogCompactor::compact_record

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Compaction/Compaction.cs:Compact
garnet/libs/server/AOF/GarnetLog.cs:Enqueue

精炼执行方案：
1. 在 compact_record 中，将 emit_event(RangeIndexDrop) 的执行结果改为错误传播（使用 ? 或将错误向上层 compact 循环返回），中断当前批次的紧缩与日志位点截断操作。
2. 确保在紧缩过程中任何清理事件入队失败时，不向前移动 shift_begin_address，保留原始日志直到重试成功。
3. 测试验证点：模拟紧缩过程中 AOF emit_event 返回 Err，断言 compact 流程立即中止并返回错误，日志截断位点不发生前移，副本不会遗漏 RangeIndexDrop 事件。

视角结论:有增量
