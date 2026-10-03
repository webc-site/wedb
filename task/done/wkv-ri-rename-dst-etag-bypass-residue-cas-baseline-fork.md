甄别结论：通过（2026-09-29 主控甄别，定级 P2——段三 migration.rs:421-426 dst 清退仅 str_k+env_k 两域漏 del_etag(new_key)，TTL 有成对迁移臂而 ETag（KeyTag::Etag）无对应臂；先例 collection.rs:348-353 del_ttl+del_etag 成对且注释明陈漏清危害。C# UnifiedStoreOps.cs:298 RENAME 重写 new 记录 ETag 不迁移。修复：补 del_etag 与段五 del_ttl 对称；副本 del_etag 裸删须 AOF 回放侧级联同调，AOF 重放终态等值测试须真验）

审核结论：通过（2026-09-29 甲轮34-A，P2 级）。段三仅两域清退全函数零 etag 触点、collection.rs:347-354 del_ttl+del_etag 成对先例与「漏清即孤儿 ETag 破坏条件写并被紧缩误判回拷永生」危害自证、C# 记录级覆写一体消亡均复核成立。执行席遵照：副本收敛面补 del_etag 系裸删无 AOF 入账——回放侧换入级联同调 del_etag 或经 EtagWrite 确定性条目入账，票内「AOF 重放终态等值」测试点须真验；随迁面对齐普通 RENAME 双臂先例（有 etag 回填/无清退）或登记刻意偏差二选一，禁第三形态。

原票面：
rename_range_index 段三 dst 清退只墓碑 String/信封两域漏清 ETag 旁路记录，换名后新键残留 dst 旧 etag 破坏条件写基线

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）：C# RENAME（garnet/libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:298 一带）为 GET old + DELETE old + 记录级重写 new：new 记录全新分配，dst 旧记录连同尾随 ETag 可选字段一体消亡（对位 RMWMethods.cs 删除臂连尾随字段同亡），换名后 dst 侧无条件写基线残留形态。
2. 工程现状确证：rename_range_index（wedb/wkv/src/range_index/migration.rs:420-432）段三清退仅 delete_raw(&str_k) + delete_raw(&env_k) 两域；TTL 经段五 ttl_of → put_ttl/del_ttl 成对收敛（:518-536），ETag 旁路记录（KeyTag::Etag）无对应臂。仓内删键臂先例（wedb/wkv/src/session/collection.rs:348-353 drain_and_delete_collection_meta keep_ttl=false 臂）明陈「漏清即遗留无主孤儿 ETag，破坏条件写语义并被紧缩误判存活回拷永生」并成对 del_etag——delete_raw 不级联旁路（drain 臂单独清退即证），rename dst 清退臂恰漏同款。dst 曾为条件写键时，换名后 new_key 携 dst 旧 etag：后续条件写比对错基线（伪 412 / 伪成功），且该残留无 AOF 清除条目，主从各自残留同一错值终态一致但均偏离 C# 契约。
3. 逻辑危害确证：条件写（CAS）语义契约分叉、孤儿旁路记录残留至键消亡才随 drain 清退、紧缩误判存活回拷风险；dst 带 etag 的 RENAME 即确定性触发，非窄窗竞态。

涉及代码：
rust 文件与函数：
wedb/wkv/src/range_index/migration.rs:rename_range_index（段三 ：420-432 清退两域、段五 ：518-536 TTL 成对迁移对照）
wedb/wkv/src/session/collection.rs:drain_and_delete_collection_meta（:353 del_etag 删键臂成对清退先例）
对应 c# 文件与函数：
garnet/libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:RENAME（:298 记录级重写，dst 尾随字段一体消亡）
garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs（删除臂连尾随 ETag 一体消亡对位）

精炼执行方案：
1. 段三清退补 del_etag(new_key)（幂等，缺席零写零入账），与段五 del_ttl 对称收口，并入既有 clear_res async 块零新增通道
2. 若审核另裁「etag 应随 RENAME 自 old 迁移」，须另立对齐裁决并经 EtagWrite 镜像入账——现码两态皆缺，不得维持静默
3. 测试验证点：条件写置 dst etag → RENAME old dst → 断言 new_key ETag 旁路缺席、条件写基线正确、AOF 重放终态与主端等值

终态注记（2026-09-29 执行席收口）：
合入 462eaca / merge d74b8c0。收口形态：migration.rs 段三 clear_res 块补 del_etag(new_key)（哈希探针幂等清臂，缺席零写零入账），失败臂沿既有段三收尾；在场摘除经写监听 KeyTag::Etag 快速分流恰发一次 EtagWrite(None) → Setwithetag(0) 条目 → 副本回放侧 del_etag 同调（审核钉「经 EtagWrite 确定性条目入账」选项承接，实证 del_etag 底层 delete_raw 全路径——原位墓碑/盲追加/ReadCache/copy_to_tail 冷臂——均恰发一次镜像，无裸删漏账臂）。随迁面依审核钉选项一对齐普通 RENAME 双臂先例：内核只清不迁（C# UnifiedStore/VarLenInputMethods.cs:147 GetUpsertFieldInfo 硬编码 HasETag=false、LogRecord.TryCopyOptionals RemoveETag 已亲验——C# RENAME 确不迁移 ETag、dst 旧记录尾随字段一体消亡；Expiration 照迁），old 键 etag 回填由调用方 finish_rename_move 统一承载（快路径 keys.rs 同形），禁第三形态达成，无新增在册偏差。函数头注段三条目与失败语义表同步扩形。测试：wkv/tests/store/rename_etag_residue.rs（内核级，修复前红——内核单独收敛即清 dst 残留 + 恰一次墓碑入账 + 旧键段五排空臂清退不迁移 + 缺席臂零事件）；wnode/tests/rename_tiered_aof_replay.rs 增 rename_tiered_over_etag_dst_replay_leaves_no_orphan_etag（SETWITHETAG 置 dst etag=1 → 分层键 RENAME 覆入 → 主端基线重置 + 副本端到端重放终态等值——AOF 重放终态等值真验）。执行席仅跑 cargo check --all-targets（绿），test.sh/clippy 由主代理门禁统一跑。遗留：普通 RENAME（Str/Obj 域）etag 随迁回填（finish_rename_move / keys.rs 快路径 put_etag 臂）相对 C# HasETag=false 属既有未在册偏差，本票范围外，建议另立裁决票。
