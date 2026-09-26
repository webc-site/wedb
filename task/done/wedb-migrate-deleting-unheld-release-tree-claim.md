归档注记（主代理 2026-09-27 fix.md 波批合并）：合入 a22ce773（P1，r25 审核案 4 直派；src 净 +2、锁 +589 另册、deviations §95 尾注一笔不取号零撞号），收口形态：两处盲释循环节（keys.rs tree_keys 逐键 release 与 RI 带外子链同款）连 id_key 派生行整删，DELETING 收口回归 C# 一手「只删不释」形（DeleteKeysAsync/DeleteRangeIndex 只调 DELETE＋delRes 留痕，键级 claim 系上游 TODO）；keys.rs 删除臂 `let _ =` 吞错改 log::error 留痕（控制流不变），RI 链 Err 上抛臂现状保持——持他人在册 claim 键删除撞 DEL 闸正当回 MigrationBusy，留痕/上抛落源，杜绝窃取九写闸封堵后排空销毁他人搬运中的树。锁 tests/migrate_deleting_unheld_claim.rs 两测复用 failclose 恒不追平＋纪元哨兵阶梯夹具（全生产原语无假 mock），装载门 MigrationBusy 在位断言非字面计数，revert-proof 双测逐处转红留档。票面锚漂移已按现码订正执行（:910→:932、:143→:157 等，eee858c8 插注释致下移），危害链九写闸/DEL 臂/纪元门锚逐点复核属实无降级。邻波回归：cluster_migration 46＋failclose 8＋rename_semantics 12＋tiered/scan 点名件全绿。

审核结论：通过（r25 审核席，定级 P1；终态损坏形：源端复活树/双域残留 + 主从发散，不可自愈；触发仅需后台降阶轮与 DELETING 窗合法重合）。全锚亲验属实：全仓 try_claim_migration 生产登记口仅 migration.rs:314/317 与 promote.rs:275 两处，集群迁移链零登记、唯二 release 点即 keys.rs:915 与 migrate_session_range_index.rs:148；manager/mod.rs:651-653 release 按 key_id 无条件 remove 不校验持有者，id_key 与登记侧同为 session_meta_key（keys.rs:911 对 promote.rs:271），故确系清除他人 claim 而非 no-op，主张不降级；migration.rs:178-179/229-238 头注明文禁「未持有即调用」，生产码自证违规。危害可达：tiered_demote.rs:265 持窗跨全 await 且该文件零 can_access_key/slot 引用，槽门唯一点 cluster_manager_slot_gate.rs:403-425 系 RESP 派发层，九写闸（stub.rs:155/213、heal.rs:40、ops.rs:58、collection.rs:80、write/mod.rs:421/552、ttl.rs:225-229/540-547）全以 migration_claimed 单判据，盲释即时解除；range_index:149 delete_string 走 DEL 臂，释后自毁自家封堵。查重：§96/§99/§118 系换代 TTL 幽灵/域钉族不涉 claim 注册表，recv-swallow 票、r20-cluster 零涉，r16-tiered:37/41 审过纪律未揪此两点。方案单机制：删两盲即回归 C# 对位（DeleteKeysAsync:182-195、DeleteRangeIndex:282-289 只删不释，Migration.cs:168/209 键级 claim 系上游 TODO），不引入第二套。

审核裁定执行方案（供 fix 直接消费，替代文末原方案）：
1. 删 keys.rs:910-916 与 migrate_session_range_index.rs:143-148 两处 release 循环节，连 :911/:144 的 id_key 派生行一并删除（防 unused 警告破坏 :893-900 注释锚），DELETING 收口回归「只删不释」。
2. keys.rs:917-919 的 `let _ =` 吞错伴生面改 match 记 log::error 留痕，控制流不变；两处各补一行头注引 migration.rs 持有者纪律锚。deviations 登记归 swallow 族台账注记一笔，不另立条。
3. 新注入锁测：树键 K 经 try_swap_in_window 挂窗，起 KEYS/SLOTS 含 K 迁移，断言迁移后 migration_claimed(K) 仍在册、持有者释放后收敛，源端不静默排空。
4. 回归：rename_semantics.rs、tiered_read_stale_meta.rs、scan_family_dualstate_frames.rs、cluster_migration.rs 全绿。

集群槽位迁移 DELETING 收口对树键调用未持有的 release_migration_claim，窃取并发 RENAME/换入窗封堵

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 集群迁移链没有「树键迁移 claim」这一件：garnet/libs/cluster/Server/Migration/MigrateSessionKeys.cs:DeleteKeysAsync(:182-195) 与 garnet/libs/cluster/Server/Migration/MigrateOperation.cs:DeleteRangeIndex(:282-289) 的 DELETING 收口只调 BasicGarnetApi.DELETE 并记调试日志，不触碰任何键级登记；键级 claim 在 C# 侧是尚未落地的 TODO（garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs:168 "claim the key atomically before moving the file"、:209 "claim the key using some transactional mechanism"）。C# 迁移期的唯一封堵件是 sketch 状态机（garnet/libs/cluster/Server/Migration/MigrateSessionKeyAccess.cs:CanAccessKey:35-61）。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 的 migrating 注册表（wbftree/src/manager/mod.rs:try_claim_migration:636 / migration_claimed:646 / release_migration_claim:651）是本仓自设的 RENAME 与换入窗封堵件，其纪律成文于 wkv/src/range_index/migration.rs:174-244（头注段一 + claim 释放配对表）：release 按 key_id 判等移除、不校验持有者，仅限 try_claim 成功后的持有者调用，"未持有即调用会误删并发 RENAME 的同源键 claim"。
全仓 claim 登记口只有两处：wkv/src/range_index/migration.rs:rename_range_index 段一（:314/:317）与 wkv/src/range_index/promote.rs:try_swap_in_window（:270-280，SwapInWindowGuard RAII 配对释放）。集群迁移链（KEYS/SLOTS/RI/向量四子链）零登记（全仓 grep 亲验）。
但迁移 DELETING 收口两处对树键盲释 claim：
- wedb/wedb/src/server/migration/migrate_driver/keys.rs:execute_keys_migration :910-916（tree_keys 逐键 release_migration_claim）
- wedb/wedb/src/server/migration/migrate_session_range_index.rs:migrate_range_index_keys_async :143-148（range_index_keys 逐键 release 后紧跟 delete_string）
两处均无注释、无对应登记口，系早期以 claim 作源端封堵、后被 sketch 键门与 snapshot_under_claim 取代后的残留调用。

3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
claim 在册期间的封堵判点面（全部以 migration_claimed 为唯一判据）：装载与升阶探测门（wkv/src/range_index/stub.rs:155/:213、heal.rs:40、ops.rs:58-59 的 RI.CREATE 早退）、DEL/GETDEL 两臂（session/collection.rs:80、session/raw/write/mod.rs:421/:552）、TTL 写闸（ttl.rs:540 migration_claim_busy 经 :225-229 ttl_write_gate 宏命中回 MigrationBusy）。
误删他人 claim 的后果分两层：
- 若落在 RENAME 段一复核（migration.rs:363-368）之前，仅令本条 RENAME 伪报错（可重试，危害轻）。
- 若落在复核之后（换入窗/RENAME 段二至段五全程跨多个 await，可横跨整个迁移 DELETING 窗），该键的九处封堵即时解除，而此刻正有集群迁移 DELETING 在删同一键：升阶/重灌臂（promote.rs:try_swap_in_window）的换入窗失去封写，其「建树快照 → 先发流块 → 原子换入 → 落 meta」后半程在源键已被迁移删除之后照样落笔，产出「目标端只有旧快照、源端复出一棵带新 meta 的树」的双域残态；同时 AOF 侧 RangeIndexStream 流块与迁移导入帧交叉入账，重放与副本序无法收敛。
- 该盲释本身还破坏了 migrate_session_range_index.rs:149 的自有语义：delete_string 走 wkv DEL 臂（collection.rs:80）本应被 claim 挡住回 MigrationBusy，盲释后阻挡消失，直接排空销毁他人正在搬运的树。
窗口前提无需依赖他票：命令级槽门只在 RESP 派发处裁决（wedb/wedb/src/server/cluster_manager_slot_gate.rs:resolve_can_operate:403-425 → migration_manager.can_access_key，与 C# 同形——C# CanAccessKey 唯一调用面亦为 ClusterSlotVerify.cs:121），后台臂全程不过此门。后台懒降阶评估轮即持 claim 跨整轮物化写回（wedb/wnode/src/resp/objects/tiered_demote.rs:demote_candidate:265 try_swap_in_window，宿主为 primary_tasks 的 ObjectCollectTaskAsync 节拍），其窗与迁移 DELETING 瞬时重合完全合法，此时盲释即刻成害。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/migration/migrate_driver/keys.rs:execute_keys_migration（DELETING 臂 :910-919）
wedb/wedb/src/server/migration/migrate_session_range_index.rs:migrate_range_index_keys_async（DELETING 臂 :143-156）
封堵面与纪律：wedb/wbftree/src/manager/mod.rs:try_claim_migration / migration_claimed / release_migration_claim；wedb/wkv/src/range_index/migration.rs:rename_range_index；wedb/wkv/src/range_index/promote.rs:try_swap_in_window / SwapInWindowGuard::drop

对应 c# 文件与函数：
garnet/libs/cluster/Server/Migration/MigrateSessionKeys.cs:DeleteKeysAsync
garnet/libs/cluster/Server/Migration/MigrateOperation.cs:DeleteKeys / DeleteRangeIndex
garnet/libs/cluster/Server/Migration/MigrateSessionKeyAccess.cs:CanAccessKey
garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs:PublishMigratedIndex（键级 claim 仅存 TODO 注释）

精炼执行方案：
1. 删除 keys.rs:910-916 与 migrate_session_range_index.rs:143-148 两处 for 循环内的 release_migration_claim 调用（含其 id_key 派生行），迁移 DELETING 收口回归 C# 对位形：只删不释，源端封堵单点仍是外层 sketch 状态机。
2. 收口删除失败面统一为留源记错口径：keys.rs:917-919 的 `let _ = storage.delete_string(key)` 改记 error（对位 MigrateOperation.cs:288 的 LogDebug 留痕语义），零新增机制、不引入第二套回收；migrate_session_range_index.rs:149 的 Err 上抛臂保持现状。
3. 测试验证点：
   - 定向注入：键 K 为分层树键，先经 try_swap_in_window（或 rename_range_index 段一）持有 K 的 claim 并在窗内挂起，再起 KEYS 与 SLOTS 两链迁移含 K 的批，断言迁移完成后他人 claim 仍在册（migration_claimed 为真、由持有者自身收尾释放），且迁移不静默排空该树。
   - 不回退既有锁面：wedb/wkv/tests/store/rename_semantics.rs 的 claim 配对用例、wedb/wnode/tests/tiered_read_stale_meta.rs 与 scan_family_dualstate_frames.rs 的 claim 用例全绿；wedb/wedb/tests/cluster_migration.rs 迁移族全绿。