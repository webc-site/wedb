甄别结论：通过（2026-09-29 主控甄别，定级 P1——删空臂 ops.rs:467-497 drop(tree) 放条带锁后 drain_and_delete_collection_meta 无 key_id/size 复核（collection.rs:343 无条件物理墓碑），并发 SET 提交 ACK 后墓碑后至即湮灭、save 后至同 key_id upsert 复活幽灵半键；RENAME 段五 drain_guard_ok（migration.rs:571-576）先例在位本臂恰缺。修复：drain_guard_ok 扩形 expect_size 守卫包重读复核+delete_raw，取锁循有界 try+让核纪律）

审核结论：通过（2026-09-29 甲轮27-B 独立审核席，P1 级）。SET 端无门禁专项复核：wval/meta.rs:134-136 is_live 对 RI 空 meta 恒活、heal.rs:43-52 无 size=0 拦截、stub.rs:245-270 只拦缺失/TTL/类型——drop(tree) 至墓碑间并发 SET 完整提交坐实；反向序幽灵半键成立；C# RangeIndexOps.cs:411-449 RMW 锁内逻辑墓碑即终态无此窗。审核席方案修正（执行席遵照）：
1. 守卫窗须闭合而非仅收窄：票方案 1 重读与墓碑间仍留 TOCTOU，改用现成原语 mgr.acquire_exclusive_for_delete（migration.rs:445 同款）包住「重读复核+delete_raw(meta_k)」再放锁，之后原序 del_ttl/del_etag 与 delete_index（其自取同条带锁，ops.rs:416-418 防死锁次序不变）。
2. drain_guard_ok 扩形须参数化：expect_size 参数（删空臂传 Some(0)、迁移三消费点传 None 维持纯 key_id），守「禁第二判据函数」铁律。
3. 判否臂按已生效收尾成立；DEL 字段 AOF 条目发射序倒窗系非删空臂既有面，另案观察不阻断。

原票面：
RI.DEL 删空自愈臂放锁后无条件墓碑元记录，与并发 RI.SET 写臂交错产生已 ACK 写丢失与半死幽灵键

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）：C# 无 RI 删空自愈——RangeIndexOps.cs:RangeIndexDel 仅递减计数，空索引驻留主存；同键写删经 TsavoriteKV RMW 记录 X 锁全程串行（RMWMethods.cs InPlaceUpdater 前置记录锁），删除判定与后续写入天然序化，结构上不存在「删臂放锁后另行无条件墓碑」的窗口。rust 删空自愈系 doc/zh/collection.md 3.3 既定改良，改良本身不报，本票报改良引入的竞态窗。
2. 工程现状确证：range_index_del（wedb/wkv/src/range_index/ops.rs:434-514）在条带独占写锁内 ri_del + dec_size 判空后 drop(tree) 放锁，再调 handle_bftree_drain_and_delete（wedb/wkv/src/range_index/drain.rs:47）→ drain_and_delete_collection_meta（wedb/wkv/src/session/collection.rs:337，:343 delete_raw(&meta_k) 无条件物理墓碑），无 key_id/size 复核——同仓 rename_range_index 段五有 drain_guard_ok 排空前守卫（wedb/wkv/src/range_index/migration.rs:493-508），本臂恰缺同款。并发 range_index_set（ops.rs:211）可在 DEL 放锁后、墓碑落盘前的多 await 窗（env 墓碑 await + meta 墓碑 await）内完整提交：load_range_index_stub（meta 仍 live、size=0 无门禁）→ 取锁 → refresh_tiered_meta（live 放行）→ ri_set 新增 → inc_size(1) → save → 命令 +OK ACK。随后删空臂墓碑无条件落下，已 ACK 字段写湮灭；反向交错（墓碑先落、SET 的 save 后至）则以同一 key_id 复活记录，留下 live 元记录指向已销毁树的幽灵半键（EXISTS=1 而读恒 NotFound）。
3. 逻辑危害确证：已 ACK 写丢失（「SET 后 DEL 字段」的任何串行序都保留新字段，实际终态不对应任何串行序，非可串行化）；幽灵半键令 EXISTS 与读应答自相矛盾；热键删空并发写场景窗内真实可达，非理论窄窗。AOF 序最终收敛但客户端成功契约已破损。

涉及代码：
rust 文件与函数：
wedb/wkv/src/range_index/ops.rs:range_index_del（删空臂 :483-497）
wedb/wkv/src/range_index/drain.rs:handle_bftree_drain_and_delete
wedb/wkv/src/session/collection.rs:drain_and_delete_collection_meta（:343 无条件墓碑）
wedb/wkv/src/range_index/migration.rs:rename_range_index 段五 drain_guard_ok（:493-508 同仓守卫先例）
对应 c# 文件与函数（无直位对，参照维度：竞态与记账真实）：
garnet/libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexDel（无删空自愈，计数递减即终态）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/TsavoriteKV.cs:RMW 记录 X 锁全程序化对位

精炼执行方案：
1. 删空臂墓碑前复核：drain 落墓碑前重读 meta_k，仅当 key_id 相符且 size == 0 才落墓碑（并发复活记录 size >= 1 即中止排空，收敛为「字段删后又有写」的正常串行终态）；判据并入 drain_guard_ok 单点扩形（key_id + size 两元），禁第二判据函数
2. 复核判否臂按已生效成功收尾：字段实删已入树、AOF 字段条目照常入账，不报错不回滚
3. 测试验证点：定向交错用例（DEL 末字段 × 并发 SET 注入于 drop(tree) 与墓碑之间），断言 SET ACK 后键存活且 count == 1、无幽灵半键、AOF 重放终态与主端一致

终态注记（2026-09-29 执行席收口）：已合入 dev（merge ebb490a88bb936a85fcecc285ef392d3b945ea86，分支 fix-ri-del-drain-guard）。收口形态：drain_guard_ok 扩形 expect_size（迁移三点 None / 删空守卫 Some(0)）+ 删空臂锁内先落 size=0 基线（守卫判据前提，兼修并发 SET 增量基线陈旧的计数虚高）+ tombstone_meta_guarded 条带独占写锁内「重读复核+元记录墓碑」原子完成（try_lock_tree_write 有界 try+让核档，与 SET 锁内 refresh→save 临界区互斥即窗闭合）+ handle_bftree_drain_and_delete 收敛为唯一内核 drain_and_delete_index 的守卫缺席门面（22 处既有调用面零改动）+ 守卫门面 handle_bftree_drain_and_delete_guarded（判否按已生效成功收尾，零墓碑零树销毁）；测试 store/range_index.rs 追加交错终态闭合映射（120 轮）与 AOF 镜像序回放收敛。遗留（另案，审核注记 3 辖面）：判否臂字段条目与并发 SET 条目的 AOF 相对入账序不定（SET 放锁后 emit 前微窗守卫可抢先入账），W(f1 del) 先至形回放端经删空自愈收敛键亡与主端键活发散。
