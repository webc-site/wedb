归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 eee858c8（P1，r25 审核票直派；四源文件净 +93/−25、锁 +808 另册），收口形态：迁移链九处纪元栅栏返值承判——keys.rs TRANSMITTING/DELETING 与 slots.rs 同相四点走 recover_and_fail! 判败，MIGRATED 归位臂保留放行补两行不变量声明（对位 C# finally、两态键门均放行无收敛不变量，防再圈），RI/向量两子链四栅栏沿既有失败臂 Err 上抛接 slots.rs:306/:419 既有 recover Err 臂（票面「调用侧 keys.rs:333」失准按现码订正已申报），零新机制零新配置；§95 尾句双席合流（failov 族收编文一字未动承接＋迁移族九点续排，slots.rs 旧枚举定性失实一并更正，原「留后续席统一圈批」句两族收口后删除）。锁 tests/migrate_epoch_drain_failclose.rs 八用例 *_fence_fails_closed 三形态夹具＋纪元推进增量 delta 精确指认判败位点杜绝 begin 门假锁，revert-proof 八位点逐处转红 8/8、还原复绿；不回退面 cluster_migration 46＋failov 邻席全绿。

审核结论：通过（r25 审核席，定级 P1；数据丢失 + 源端复活键双害，槽位交权后无自愈路径）。全锚亲验属实：checkpoint.rs:54 返 bool，九处 `let _ =` 弃返值实点 keys.rs:741/906/932、slots.rs:209/258、range_index:88/137、vector_set:89/131；超时返 false 真实可达（wait.rs:80-85，args.rs:16 默认 60s 有界），C# ClusterProvider.cs:373-386 无限自旋恒 true 故类型层抹除返值无失语义、rust 有界化后弃返值即语义缺口；begin 闸兜不住（keys.rs:728 KEYS 链 epoch_gate=false）；deviations.md:1230「留后续席统一圈批」漏计 keys/RI/向量三链且对 slots.rs 定性失实。属应承接的统一圈批，非已裁决偏离。

审核裁定执行方案（供 fix 直接消费，替代文末原方案）：
1. keys.rs 两处（:741 TRANSMITTING、:906 DELETING）沿本文件 recover_and_fail!（:373-381 形）收口；slots.rs 两处（:209、:258）沿 :206 既有 FAIL+recover 形态；RI 子链 :88/:137 与向量子链 :89/:134 四处 Err 上抛（调用侧 keys.rs:333 既有 Err 臂已在位，不新建收口面）。
2. keys.rs:932（MIGRATED 归位臂）保留放行，补一行注释声明该位点无数据收敛不变量、与 §95 无盘快照键门同口径，防后续席再圈。
3. deviations.md:1230 尾句改写：迁移族已由本票收口，更正 slots.rs 定性失实，并把枚举补全至九处实点。
4. 新增 tests/migrate_epoch_drain_failclose.rs：复用 diskless 夹具（:282 set_cluster_node_timeout_ms 形）注入超时，断言四链超时即 FAIL、源键零删、目标零导入；revert-proof 需转红。
5. 遗留（不入本票）：primary_failover_session.rs:257/289 同类弃返点属 failover 域，另席圈批；本票不扩面。

集群槽位迁移链九处纪元静止栅栏一律丢弃 bump_and_wait_for_epoch_transition_async 返值，排空未达成照样放行致已 ACK 写丢失与源端复活键

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 原语结构上不可能失败：garnet/libs/cluster/Server/ClusterProvider.cs:BumpAndWaitForEpochTransitionAsync(:366-389) 对每个 server 的 ActiveClusterSessions 走 while(true) + goto retry 无限自旋，直到全部会话 LocalCurrentEpoch 追平，末尾恒 return true。因此迁移链各栅栏调用点（MigrateSessionKeys.cs:35/:187/:194、MigrateSessionSlots.cs:236/:247、MigrateSession.RangeIndex.cs:114/:135、MigrateOperation.cs:211/:222）经 MigrateSessionKeyAccess.cs:WaitForConfigPropagationAsync(:17-25) 统一转调，该方法声明为 Task（非 Task<bool>），返值在类型层即被抹除——C# 侧「忽略返值」无失语义，因为不存在未达成态。
唯一 C# 检查返值处是迁移起始闸：MigrationDriver.cs:160 `if (!await clusterProvider.BumpAndWaitForEpochTransitionAsync()) return;`。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 侧把该原语有界化（wedb/wedb/src/server/cluster_provider/checkpoint.rs:bump_and_wait_for_epoch_transition_async:54-66，上限 cluster_node_timeout()，超时返 false；默认 DEFAULT_CLUSTER_NODE_TIMEOUT_MS = DEFAULT_CLUSTER_TIMEOUT * 1000，args.rs:16，即默认配置下 false 可达），其头注 :50-53 明文：false 仅表达静止未达成，"调用方按各自窗口不变量裁决"，并举无盘快照键门为返值承判先例（§95 裁决，replication_snapshot_iterator.rs:232 判败重拍，锁面 wedb/wedb/tests/diskless_epoch_drain_failclose.rs）。
同链起始闸已按 C# 对位承判：migrate_driver/keys.rs:begin_migration_phase :367-381（`if epoch_gate && !...` → recover_and_fail!）。
但迁移数据收敛相的九处栅栏全部 `let _ =` 丢弃返值：
- migrate_driver/keys.rs:execute_keys_migration :741-744（TRANSMITTING）、:906-909（DELETING）、:932-935（MIGRATED）
- migrate_driver/slots.rs:execute_slots_migration :209-212（TRANSMITTING）、:258-261（DELETING）
- migrate_session_range_index.rs:migrate_range_index_keys_async :88-91、:137-140
- migrate_session_vector_set.rs:migrate_vector_set_keys_async :89-92、:131-134
四条子链的取数相栅栏位点注释一律自陈「返值忽略：C# 无限自旋，rust 有界放行」（keys.rs:740、slots.rs:208、RI 子链 :87、向量子链 :88）。
登记面查重：deviations.md §95:1230 把同款忽略返值形态列为「留后续席统一圈批」，其枚举仅 slots.rs:203-206 且定性为「用途系配置传播屏障或角色切换整备，无数据收敛不变量挂其上」——与现码不符（该处现注释即为「等 TRANSMITTING 键门对全会话生效、批内在途操作排空后再传输」），并整体遗漏 keys.rs 与 RI/向量两条子链。故本形态非已裁决偏离，属本票应承接的统一圈批。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
keys.rs:736-740 的现码注释自证该栅栏所挂不变量：堵「写已过键门 → 删除落地后写才持久化 → 源端复活键」窗。返值丢弃即该窗重开，两条成害路：
- TRANSMITTING 栅栏未达成即开始取数：尚有会话批内写未排空，read_live_value/整树快照读到的值不含该写，目标端导入旧值；随后 DELETING 删源键，该已 ACK 写永久丢失，且槽位交权后无任何路径自愈。
- DELETING 栅栏未达成即落删除：在途写在源键删除之后才持久化，源端复活一个目标端已有副本的键；槽位所有权已翻转，此后写按新主走，两端各存一份且逐写发散（AOF 重放与副本链同样带此洞）。
- RI/向量两条子链同源：整树快照/整集合导出对「窗内并发写」零防御（其封堵件恰是 sketch 键门 + 另票的 claim 面），栅栏放行即取数即删。
MIGRATED 归位臂（keys.rs:932-935）对位 C# finally，INITIALIZING 与 MIGRATED 两态对 CanAccessKey 均放行，无收敛不变量挂其上，可维持放行。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/cluster_provider/checkpoint.rs:bump_and_wait_for_epoch_transition_async（返值契约单点）
wedb/wedb/src/server/migration/migrate_driver/keys.rs:begin_migration_phase（承判先例）/ execute_keys_migration
wedb/wedb/src/server/migration/migrate_driver/slots.rs:execute_slots_migration
wedb/wedb/src/server/migration/migrate_session_range_index.rs:migrate_range_index_keys_async
wedb/wedb/src/server/migration/migrate_session_vector_set.rs:migrate_vector_set_keys_async
对应 c# 文件与函数：
garnet/libs/cluster/Server/ClusterProvider.cs:BumpAndWaitForEpochTransitionAsync
garnet/libs/cluster/Server/Migration/MigrateSessionKeyAccess.cs:WaitForConfigPropagationAsync
garnet/libs/cluster/Server/Migration/MigrateSessionKeys.cs:MigrateKeysFromStoreAsync / DeleteKeysAsync
garnet/libs/cluster/Server/Migration/MigrateSessionSlots.cs（:236/:247 两栅栏）
garnet/libs/cluster/Server/Migration/MigrationDriver.cs:BeginAsyncMigrationTaskAsync（:160 承判对位）
garnet/libs/cluster/Server/Migration/MigrateSession.RangeIndex.cs（:114/:135）

精炼执行方案：
1. 八处取数/删除相栅栏改返值承判，零新机制、复用各函数既有失败收敛口：
   - keys.rs 两处（TRANSMITTING/DELETING）走本文件既有 recover_and_fail! 单点（同 begin_migration_phase:367-381 形），文案沿用「迁移纪元转换等待失败」口径；
   - slots.rs 两处沿既有失败路（置 FAIL + recover，与 :206 注释同位）；
   - RI/向量两条子链四处直接 `?`/Err 上抛，由调用侧驱动既有 recover 臂收敛（其 :149 delete 失败已是 Err 上抛口径，同形）。
2. MIGRATED 归位臂（keys.rs:932-935）保留放行，注释补明判据（两态对键门均放行、无收敛不变量），禁各臂另设第二判点或引入二次验证旁路（§95 严禁软化为重试通过同纪律）。
3. 收尾登记：deviations.md §95:1230 的「留后续席统一圈批」尾句按本票结论改写（迁移族承判收口 + 更正 slots.rs 定性与补全 keys.rs/RI/向量枚举面）。
4. 测试验证点：
   - 新增锁面 wedb/wedb/tests/migrate_epoch_drain_failclose.rs，复用 diskless_epoch_drain_failclose.rs 的恒不追平夹具（批首纪元快照会话 + 极小 cluster_node_timeout_ms），断言 KEYS/SLOTS/RI/向量四链在 TRANSMITTING 与 DELETING 栅栏即判败：迁移终态 FAIL、走 recover（远端收 END/清理）、源端键零删除、目标端零导入；revert-proof：还原任一处 `let _ =` 对应用例转红。
   - 不回退既有面：wedb/wedb/tests/cluster_migration.rs、migrate_fail_inject.rs 族与 slot_verify 相关用例全绿。