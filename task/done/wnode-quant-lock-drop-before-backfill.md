甄别结论：通过（甄别席 zc-fix-r16-quantdrop，2026-09-26）定级 P2
核验记录（逐锚现码复跑）：
C# 侧成立：VectorManager.Quantization.cs:133 using ReadVectorIndexCore(nonBlocking:true) 罩 :152 BuildQuantizationTable(context,indexPtr) 与 :166 BackfillQuantizedVectors(context,indexPtr,…) 全程，:147 ReadIndex 出 indexPtr；DiskANNService.cs:111-119 确以 (context,nint index) 二元组入原生 P/Invoke；VectorManager.Locking.cs:556 ReadForDeleteVectorIndex 经 :566 AcquireExclusiveLocks 取排他。
rust 侧成立：vector_manager_quantization.rs:155 drop(lock) 确在 :143-146 read_vector_index_core 与 :163 build / :182 backfill 之间；:160-162 自死锁辩护注释原文一致；vector_manager_locking.rs 模块头 :6-10 自证 async_lock 条带守卫可跨 await、:409-412 自证 parking_lot/inline_wait 旧形态约束已消除；rust_review SKILL.md:132 明载 inline_wait 已全删禁复活——drop 处注释确系过时顾虑。
危害链成立：DEL 经 vector_manager.rs:818 条带独占锁→:847/:770 request_deletion（:785 投通道、:791 同步 drop_index）→cleanup :409/:416 purge_context→:437 finished_cleaning_up 归还 context；next_vector_set_context（context_metadata.rs:328）自 :334 起顺扫低位优先复用；backfill_quant_vectors（dynamic_quant.rs:270）:313/:340 仅按 context.term(Quantized) write_iid 寻址物理键，无代际校验；wkv vector_cleanup.rs:58-60 purge 契约自陈依赖调用方隔离保证——锁截短即孤儿写与复用写穿两害成立。
无重入成立：service.rs:1194-1214 build/backfill 经 :1020 index(context) 并发字典 Arc 直下 provider，wvector 层无 vector_set_locks 面；vector_store_callbacks.rs 仅独立 StripedSerialLock RMW 锁，非同条带锁；_guard 跨 await 保活有 delete_vector_set:818 与 resp 只读臂 :1352 等在案编译先例；守卫至函数返回即亡，:105-108 会话守卫循环纪律与重试让步语义不触。
查重成立：deviations §127/§128 只覆盖 enable_quantization 屏障与 train_quantizer 全序，不触及 manager 条带锁生存期；姊妹票 wnode-vadd-lock-drop-before-try-add 系 network_vadd_slow:1055 另一站点另一 C# 锚，replay_vector_set_add 的 drop(_lock)（vector_manager_replication.rs:253 附近）另有重放语境自陈辩护，非本宗；quant_enable_barrier_race/quant_backfill_restart/quant_train_barrier 三锁文件在册，验证闭环可执行。方案最小单机制（改名保活+注释改述），架构合规，纯文本格式合格。

审核结论：通过
审核席：zcode-r17-review-quant（2026-09-26，dev 分支）
双侧源码亲验属实：rust drop(lock) 位于 vector_manager_quantization.rs:155（read_vector_index_core 与 build/backfill 之间），:160-162 自死锁注释原文一致；C# VectorManager.Quantization.cs:133 using 罩 BuildQuantizationTable（:152）与 BackfillQuantizedVectors（:166）全程，:147 ReadIndex 出 indexPtr，DiskANNService.cs:111-119 确以 (context, index) 二元组 P/Invoke 入原生库；ReadForDeleteVectorIndex（VectorManager.Locking.cs:556）走 AcquireExclusiveLocks 排他等待成立。危害链成立：drop 锁后 DEL 全链（request_deletion:770 同步 drop_index → process_request_cleanup mark_cleaning_up → process_cleanup:416 purge_context → finished_cleaning_up 归还）可与 build/backfill 并发完成；backfill_quant_vectors（dynamic_quant.rs:313/:340 write_iid，另 :260/:370 Metadata 域写点同险）仅按 context 号寻址物理键，purge 契约自陈「快照后追加记录不可能属于本上下文」依赖调用方隔离保证（wkv session/vector_cleanup.rs:59-61），锁被截短即保证破裂；next_vector_set_context（vector_manager_context_metadata.rs:328）自低位槽顺扫分配，归还即速复用，旧回填写穿新集合同号物理键空间成立。
修复合规性核验：条带锁为 async_lock::RwLock（vector_manager_locking.rs 模块头自证守卫可跨 await，持锁任务不被等锁任务饿死；read_vector_index_core:409-412 自陈「原 parking_lot 同步锁形态下这正是 inline_write 收割的死锁根源，async 锁替换后该约束消除」），rust_review 运行时纪律明载 inline_wait 已全删禁复活，drop 处自死锁注释确系旧形态过时顾虑，现基座持锁跨 await 不死锁；build_quantization_table/backfill_quantized_vectors 经 service.rs:1194-1214 self.index(context) 取 Arc 直下 provider，全程不取 vector_set_locks 条带锁，store callbacks（vector_store_callbacks.rs）亦无条带锁面，无重入自死锁；读锁共享，多分片回填并发兼容；WouldBlock 在守卫生成前返回 false，协作让步重试语义不变。§127/§128 在册仅覆盖 enable_quantization 屏障与 train_quantizer 全序（fsm/provider 内部次序面），不触及 manager 条带锁生存期维度；姊妹票 wnode-vadd-lock-drop-before-try-add（会话 VADD 臂，已通过）系另一站点另一 C# 锚（VectorStoreOps.cs VectorSetAdd），非重复提报。方案最小单机制：仅守卫改名保活 + 注释改述，无新增锁与胶水。

整理执行方案（供 task/fix.md 直接消费）：
1. wedb/wnode/src/resp/vector/vector_manager_quantization.rs:143-155：删去 drop(lock)，绑定改 let (index, _lock)，守卫随 async 栈帧跨 build_quantization_table / backfill_quantized_vectors 的 await 存活至函数返回（对齐 C# using 全程；借用面全为 &self 不可变借用无冲突，同文件只读臂与 VREM/VSETATTR _guard 保活为编译在案先例）。函数返回守卫即亡，外层 run_quantization_task_loop 的 yield_now/sleep 仍在无守卫态执行，:105-108 循环纪律注释保持有效。保活后 DEL 的条带独占锁等待量化完成，删除链整体后移，孤儿写与复用写穿窗口消除。
2. 订正 :160-162 注释：删「读锁就地释放绝不跨 await……自死锁风险」过时辩护理由（旧 parking_lot 同步锁 / inline_wait 形态顾虑，该形态已全删禁复活），改述为守卫跨 await 的现行锁纪律（async_lock 条带锁，量化全程持共享锁对齐 C# using 锁域，杜绝 DEL 竞速孤儿量化记录与 context 归还复用写穿）。与 vector_manager.rs:805-808 delete_vector_set 排空集注释「全部落在屏障内」修复后自洽，不动该注释。
3. 测试验证点：Bin 系集合建表回填进行中并发 DEL 同键，断言 cleanup 通道排空、context 归还后存储内无该 context 残留量化记录（孤儿零）；以低位 context 强制复用（删除后新建集合分得同号 context）断言新集合量化记录不被旧回填写穿（同 iid 逐字节仍为新量化器产物）；既有 quant_enable_barrier_race.rs / quant_backfill_restart.rs / quant_train_barrier.rs 全绿零回归。

量化 worker 在训练与回填前显式 drop 索引读锁，与 C# using 全程持锁分叉，删除竞态致孤儿量化记录与 context 复用污染

问题分析：
1. Garnet 契约对齐：C# TryProcessQuantizationRequest 以 using (self.ReadVectorIndexCore(..., nonBlocking: true, ...)) 把锁的生存期罩住 BuildQuantizationTable 与 BackfillQuantizedVectors 全程（VectorManager.Quantization.cs:133-174），并且向原生库传 (context, indexPtr) 二元组（:152/:166）做代际校验。锁持有期内并发 DEL 的排他锁等待，量化训练与回填写完成之前索引不亡、context 不归还，写目标集合恒为活集合。
2. 工程现状确证：rust 侧 try_process_quantization_request 在 read_vector_index_core 返回 Hit(index, lock) 后立即 drop(lock)，随后才执行 service.build_quantization_table / backfill_quantized_vectors（多分片、长 await 存储 I/O）。drop 处注释以「读锁就地释放绝不跨 await，彻底杜绝 compio 单线程调度器下的自死锁风险」辩护，但该风险属旧 inline_wait 同步收割形态的顾虑：条带锁已全面改为 async_lock（vector_manager_locking.rs 模块头明说「守卫可随 async 栈帧跨 .await 存活……持锁任务不会被等锁任务饿死」），且 service 内部对索引的持有本就经并发字典 get(&context).cloned() 以 Arc 保活，注释给出的自死锁场景在现行基座上不存在，理由过时。会话层 replay_vector_set_add 的同款 drop(_lock) 有「副本端顺序重放无并发删除面」的可行辩护，量化 worker 无此豁免：它与用户 DEL/FLUSHDB 在生产上真实并发。
3. 逻辑危害确证：drop(lock) 后 DEL 全链（request_deletion 同步 drop_index、request_cleanup 协程 mark_cleaning_up、cleanup 协程 purge_context 物理清扫、finished_cleaning_up 归还 context）可与量化训练/回填并发完成。backfill 随后仍向 context 前缀的 Term::Quantized 域逐 id 补写量化记录（backfill_quant_vectors 的 write_iid）：purge_context 已完成时这些记录成永久孤儿（context 已归还，无人再清，存储泄漏）；context 已归还并被后续 VADD 复用分配给新集合时，旧回填写入直接落进新集合的量化记录物理键空间，同 iid 记录被旧集合数据写穿（检索错距、数据损坏）。C# 侧锁全程 + indexPtr 代际校验双防线在 rust 均缺位。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/vector/vector_manager_quantization.rs:VectorManager::try_process_quantization_request（drop(lock) 位于 read_vector_index_core 与 build/backfill 之间）
wedb/wnode/src/resp/vector/vector_manager.rs:VectorManager::request_deletion（drop_index + cleanup 通道投递）
wedb/wnode/src/resp/vector/vector_manager_cleanup.rs:VectorManager::process_cleanup / VectorManager::process_request_cleanup（mark_cleaning_up、purge_context、finished_cleaning_up 归还 context 的竞速链）
wedb/wvector/src/provider/dynamic_quant.rs:WedbProvider::backfill_quant_vectors（Term::Quantized 域 write_iid 写点）

对应 c# 文件与函数：
garnet/libs/server/Resp/Vector/VectorManager.Quantization.cs:TryProcessQuantizationRequest（:133 using 锁域；:152/:166 向 BuildQuantizationTable/BackfillQuantizedVectors 传 indexPtr）

精炼执行方案：
1. try_process_quantization_request 将 (index, lock) 的 lock 改为 _lock 保活，跨 build_quantization_table 与 backfill_quant_vectors 的 await 存活至函数返回（非阻塞获取语义不变：WouldBlock 仍返回 false 交由外层让步重试；读锁与多分片 backfill 并发共享兼容；build/backfill 链路不取同键 vector_set_locks 条带锁，无重入自死锁）。保活后 DEL 的条带独占锁等待量化完成，删除链整体后移，孤儿写与复用污染窗口消除。
2. 删除 drop 处与 lock 相关的过时自死锁注释，改述为守卫跨 await 的现行锁纪律。
3. 测试验证点：Bin 系集合（需训练回填）建表回填进行中并发 DEL 同键，断言 cleanup 通道排空、context 归还后存储内无该 context 的残留量化记录；以低位 context 强制复用（删除后新建集合分得同号 context）断言新集合量化记录不被旧回填写穿。

合入哈希：479abf9 收口形态：try_process_quantization_request 守卫改 let (index,_lock) 随 async 栈帧跨 build_quantization_table/backfill_quantized_vectors 的 await 全程保活（1:1 对齐 C# using 锁域），删除旧「绝不跨 await 防自死锁」过时辩护、改述为 async_lock 条带共享守卫跨 await 现行锁纪律（绑定处注释为单源）；新增集成测试 vector_quant_backfill_delete_lock.rs 覆盖并发 DEL 竞速下「孤儿量化记录」与「低位 context 复用写穿」双危害；追平 dev 后 cargo check 门禁零警告零错误，快进合入 dev。
