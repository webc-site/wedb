归档注记：合入 80b1e54d，rebuild put 败归 None 整轮拒启(fail-closed)，不撤标记防活上下文清丢，与§140 副本拒启同口径

甄别结论：通过（甄别席 J2，2026-09-27，定级 P2——恢复登记先落标记后写表，写败半恢复致上下文滞留免清理）。全链亲读：Index 臂先落 recovered_indexes 标记后 put_stored_index（vector_registry_recovery.rs:246-257），失败仅 log::error 照常 Some；put 失败回滚只摘 key_index_registry（vector_manager_locking.rs:596-608）不撤标记；模块契约 :174-176 与调用方 None 判 Err（single_database_manager.rs:361-367）亲核；sweep 判据 !contains_key（vector_manager.rs:1509-1512）坐实 in_use 滞留免清理；C# SanitizeAndTrackIngestedRecordIfApplicable VectorManager.cs:437 在场无二次写透。deviations §140（:1915）「回建失败即拒」系副本侧同口径在册条目，为修复方向背书非并案。派沙箱席 c01c。

审核结论：通过（P2 真案。①亲验坐实：rebuild_registry_from_store Index 臂先经 recovered_vector_set_index_key 落 recovered_indexes 标记（vector_manager.rs:1538 insert），put_stored_index 失败臂仅 log::error 后继续循环，不归 None 亦不撤标记；put_stored_index 失败回滚（vector_manager_locking.rs:601-607）实测只回滚 key_index_registry，不触 recovered_indexes，翻案条件不成立。调用方 single_database_manager.rs:363-367 仅对 None 判 Err，半截恢复静默放行，与模块头 :174-176 fail-closed 契约及 deviations §140 副本侧「回建失败即本轮全量收口失败拒授予位点」同口径相抵。②reconcile sweep 判据（vector_manager.rs:1509-1512 contains_key 免清理）确证 in_use 滞留、集合隐身、原生索引不重建；危害限本次运行期（盘上旁路记录仍在，重启回建成功即愈，「永久」表述按运行期生命周期理解），故 P2 非 P1。③C# 恢复趟 SanitizeAndTrackIngestedRecordIfApplicable 无二次写透无失败面，票自陈 rust 写透为自创肢按自身契约收口失败臂，非误读原型。④查重：五池无同题票，deviations 无在册裁决覆盖此失败臂）

vector 登记回建 put_stored_index 失败臂吞成半截恢复：内存登记回滚而 recovered_indexes 标记不撤销，该向量集整体隐身、context 永滞 in_use、原生索引与盘上元素行永久孤儿（违背本模块自订 fail-closed 契约）

问题分析：
1 Garnet 契约对齐：C# 索引记录驻 Tsavorite 主存即登记，恢复趟 SanitizeAndTrackIngestedRecordIfApplicable（garnet/libs/server/Resp/Vector/VectorManager.cs:437-465）对记录值仅原位清指针（ClearIndexPointer）无二次写透，不存在「登记写回失败」面；rust 登记表为主存旁路记录的内存镜像，回建写透为自创肢，须按自身契约收口失败臂。
2 工程现状确证：wedb/wnode/src/resp/vector/vector_registry_recovery.rs:246 先执行 vm.recovered_vector_set_index_key(&value)（vector_manager.rs:1538 向 recovered_indexes 插入 context 标记），:250-257 put_stored_index 失败臂仅 log::error 后照常返回 Some——而 put_stored_index 失败时 vector_manager_locking.rs:601-607 已回滚内存登记（key_index_registry.pin().remove）。模块头自订契约（vector_registry_recovery.rs:174-176）「返回 None 表示回建不可行……调用方须显式报错拒启，禁止静默吞成半截恢复」，调用方 single_database_manager.rs:363-367 只对 None 判 Err——put 失败恰是半截恢复却未归 None 也未撤销标记。
3 逻辑危害确证：恢复期旁路写透失败（设备错）→ 该向量集整体隐身（键不存在语义）；reconcile（vector_manager.rs:1509-1512 sweep 判据 !recovered_indexes.contains_key(&context)）视该 context 已恢复免清理 → in_use 位（已随元数据旁路记录持久）永滞 → 用户 VADD 新建集拿新 context，旧 context 原生索引永不重建、盘上元素行永久孤儿、槽位单调占用——与 create_index_locked（vector_manager_locking.rs:534-543「创建失败回收刚占位的上下文……杜绝单调泄漏」）自订纪律同格冲突。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/vector/vector_registry_recovery.rs:rebuild_registry_from_store Index 臂（:245-257）、模块头契约（:174-176）
wedb/wnode/src/resp/vector/vector_manager_locking.rs:put_stored_index 失败回滚（:593-608）、create_index_locked 回收纪律（:534-543）
wedb/wnode/src/resp/vector/vector_manager.rs:recovered_vector_set_index_key（:1538）、reconcile sweep（:1509-1512）
wedb/wnode/src/database/single_database_manager.rs:调用方 None 判 Err（:363-367）

对应 c# 文件与函数：
garnet/libs/server/Resp/Vector/VectorManager.cs:SanitizeAndTrackIngestedRecordIfApplicable（:437-465，无二次写透无失败面）

精炼执行方案：
1 put_stored_index 失败臂按契约归 None（整轮拒启 fail-closed）或失败时撤销 recovered_indexes 标记（recovered_indexes.pin().remove(&context)）保 reconcile 可清理——二择一按最小改动与拒启面评估裁
2 锁测：注入 put_stored_index 失败桩，断言拒启或标记撤销后 reconcile 收敛、in_use 不泄漏

审核裁定执行方案（审核席订正版，供 fix 消费）：
1 裁定归 None 整轮拒启：put_stored_index 失败即 return None（弃「撤销标记交 reconcile 清理」备选——撤销标记会把可由盘上既有记录再回建的活上下文交给清理协程物理丢弃，制造本运行期清丢、重启复活的运行期两态分叉；归 None 复用既有 None 判 Err 拒启通道，单机制零新增，与 deviations §140 副本侧拒启口径同口）。返回 None 前无须清理 recovered_indexes 与 recovered_metadata 暂存（拒启即进程拒绝启动，无后续消费面）
2 锁测：注入 RegistryPersistence::put 失败桩（返 false），断言 recover_vector_sets 返回 Err 拒启、不再产出 Some 半截计数放行启动；对照臂：put 成功路径恢复计数与登记表条目一致
