键 TTL 到期物理清除零 WATCH 版本推进：惰性过期与后台紧缩双面漏推，EXEC 无值比对兜底

来源：glm.my 第 2 条（分拣判定成立且待做）。取证基线：主仓 HEAD 50d1cb5f，行号为当下实况。

现状（全仓 bump 位点普查后确认缺口）
- 清除内核不发版本：/Users/z/git/db/wedb/wedb/wkv/src/ttl.rs:327-340 purge_expired
  仅 del_ttl + delete + emit_event(StoreEvent::TtlPurge)，无 bump_watch_version；
  wkv 全 crate 的 bump 位点经 grep 只有 5 处，全在用户键写入口：
  /Users/z/git/db/wedb/wedb/wkv/src/session/raw/write/mod.rs:155、:198、:280 与
  /Users/z/git/db/wedb/wedb/wkv/src/session/raw/write/rmw.rs:77、:102。
  即 delete_raw / collection.rs delete / ttl.rs / gc.rs 四条面均无用户键版本推进。
- 中招触发面（谓词入口 check_expired 于 ttl.rs:410，probe_alive :401 内部调它）：
  读路径 /Users/z/git/db/wedb/wedb/wkv/src/session/raw/read.rs:803、:821；
  /Users/z/git/db/wedb/wedb/wkv/src/session/raw/modify.rs:131；
  /Users/z/git/db/wedb/wedb/wkv/src/session/collection.rs:85、:177；
  /Users/z/git/db/wedb/wedb/wkv/src/range_index/stub.rs:85、:245；
  /Users/z/git/db/wedb/wedb/wkv/src/store/keyspace.rs:57；
  后台紧缩终审 /Users/z/git/db/wedb/wedb/wkv/src/gc.rs:474。
- 唯一不受影响的臂：RMW 前置过期（/Users/z/git/db/wedb/wedb/wkv/src/session/raw/write/rmw.rs:98
  调 check_expired，:102 紧随 bump），一命令一推进恰覆盖。
- 对照组（同类键状态变化都有推）：/Users/z/git/db/wedb/wedb/wnode/src/storage/session/storage_session.rs:399
  expire_at_ticks、:415 persist_key、:363/:383 delete 降级臂均 bump_watch_version。
- 校验无兜底：/Users/z/git/db/wedb/wedb/wtxn/src/txn_watched_keys_container.rs:65-71
  validate_watch_version 只比 read_version(hash) == slice.version，无重读值比对，
  故版本未推即 EXEC 通过。
- 仓内自身判据同向：/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/tiered_collection_ops.rs:330
  与 :357 附近头注明确「不推进即 WATCH 漏通知（版本号看似未变而数据已改）」，
  且 task/ing/ri-del-empty-drain.md 已按同判据要求 RangeIndex 删空臂补 bump——
  本条是同栅栏在 TTL 清除面的另一半漏项。

C# 参考
- /Users/z/git/db/wedb/garnet/libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs:51-58
  PostInitialUpdater 无条件 `watchVersionMap.IncrementVersion(rmwInfo.KeyHash)`，
  其上游正是过期裁决臂（同文件 :67/:185 置 RMWAction.ExpireAndStop / ExpireAndResume，
  :166 同族推进）：C# 的「惰性过期即物理删除」每次都推版本。
- /Users/z/git/db/wedb/garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:379 同向；
  /Users/z/git/db/wedb/garnet/libs/server/Storage/Functions/MainStore/DeleteMethods.cs:16
  InitialDeleter 无条件推进（rust 该面已在位）。
- 后台过期扫描：/Users/z/git/db/wedb/garnet/libs/server/StoreWrapper.cs:752-782
  ExpiredKeyDeletionScanTaskAsync → databaseManager.ExpiredKeyDeletionScan → 统一 DEL 路径
  → InitialDeleter 推版本；rust gc.rs:474 走 purge_expired 因而已断开该链。
- 规范源 /Users/z/git/db/wedb/.agents/skills/transpile/SKILL.md
  「O(1) 墓碑逻辑秒删：DEL key 写物理墓碑（WATCH 版本栅栏由 wtxn watch_version_map 单点推进，
  对标 C# watchVersionMap.IncrementVersion）」条。

修法
1. 单点收口在 purge_expired 闭环处：emit_event(StoreEvent::TtlPurge) 同点（ttl.rs:334 前后）
   经会话 bump_watch_version(user_key) 推进恰一次，使读路径惰性过期与后台 GC sweep 共用一处，
   禁在 8 个调用面各自补推（那会造出第二套栅栏口径）。
2. 双计防护：rmw.rs:98 前置过期后 :102 已推一次，该臂须免二次推进——
   以 purge 返回「本次是否真空清除」布尔（现返回 Ok(())）承接，或按会话内一次性抑制守卫
   （同 PurgeNotifyGuard :328 的既有形态，用于 AOF 单条化抑制）做同链去重，二者择一。
3. 与 AOF/复制面口径对齐：TtlPurge 事件已保证「清除未闭环不得宣告过期」（ttl.rs:320-326 注释），
   版本推进须落在同一闭环判定之后，失败早退臂（`?`）不推。
4. 派生一致性检查：EXISTS/纯读触发清除后同事务再 WATCH 该键的期望值须与新栅栏一致，
   测试面按此写断言而非按现行为写。

优先级
功能缺口（事务隔离语义正确性缺陷：WATCH 漏通知致 EXEC 提交基于已消失键的错序结果，
且该缺陷对读路径与后台紧缩同时存在，非纯打磨）。

协调
- 不碰分层升降阶域（tiered 命令臂/升降阶在途票），本票只在 wkv TTL 清除单点 + rmw 去重；
  tiered_collection_ops 侧注释仅作判据引用，不改其代码。
- 与 task/ing/ri-del-empty-drain.md 同栅栏不同事实（那条补 RI 删空臂，本条补 TTL 清除面），
  落地时 bump 计数口径须一致（一命令一推进）。
- next/storage-session-dead-watch-registry.md（task/done）已收的 watch_versions 死注册面
  与本票无关，勿据其结论误判栅栏已废弃。

验收
- 新增用例：WATCH k（k 带 TTL）→ 时间推进过期 → 任一连 GET/EXISTS k 触发物理清除 →
  EXEC 必须返回 NIL（现行为返回非空）；同型用例覆盖 GC sweep 触发面（经内置紧缩周期或测试钩子）。
- 回归：rmw 前置过期臂版本只推一次（断言 read_version 增量 == 1）。
- 现有事务/TTL 测试期望复核，禁仅改期望值放行。
- 验证纪律：仅 cargo check --workspace --all-targets（私有 target 目录）零 error 零 warning。

甄别结论：拒绝（2026-09-19，对照 garnet C# 逐链核实）

核心主张（TTL 清除不推 WATCH 版本是隔离语义缺陷、EXEC 应 abort）与 C# Garnet 真实语义相反，票据的 C# 论据三处失实：

1. 「后台过期扫描 → 统一 DEL 路径 → InitialDeleter 推版本」不成立。
   实际链：StoreWrapper.cs:752 ExpiredKeyDeletionScanTaskAsync → ArrayKeyIterationFunctions.cs:217
   ExpiredKeyDeletionScan.DeleteIfExpiredInMemory → DELIFEXPIM（RMW 命令，非 DEL）→
   UnifiedStore/RMWMethods.cs:69/:185 置 ExpireAndStop → Tsavorite
   InternalRMW.cs:157/:422/:651 三臂全部直接 SetTombstone/HandleRecordElision，
   不经任何 sessionFunctions 挂点。全仓 30 处 watchVersionMap.IncrementVersion
   （grep 收口：MainStore/UnifiedStore/ObjectStore/VectorStore 的 Upsert/Delete/RMW）无一在
   过期清除臂上。

2. 「PostInitialUpdater 无条件推进 = 惰性过期即物理删除每次推版本」不成立。
   UnifiedStore/RMWMethods.cs:54 的推进对应"过期后重建新值"（ExpireAndResume →
   InternalRMW.cs:699 ReinitializeExpiredRecord → InitialUpdater 成功才调），是写入事实的
   合并计数；其 NeedInitialUpdate=false / InitialUpdater=false 的
   "Expiration with no insertion" 臂（纯清除无重建）置墓碑不推版本。rust 现状已对齐：
   upsert_rmw（rmw.rs:98 前置过期 → :102 重建推恰一次）与 try_rmw_sync（Due 臂 :77 恰一次）。
   C# 读路径过期更不物理删除（MainStore/ReadMethods.cs:37 CheckExpiry → 读为空），
   rust 的"读即物理清除"是自有取舍，其版本口径（清除不推、重建推一次）与 C# 一致。

3. 「对照组」类推失实。wnode storage_session.rs 的 expire_at_ticks:409 / persist_key:425 /
   delete 降级臂 :373/:393 bump 对应 C# EXPIRE/PERSIST 经 RMW InPlace/PostCopy 推版本与
   DEL 的 InitialDeleter——全是"用户命令写/删"事实；expire_at 的 purge 臂返回 -2 不推，
   恰与 C# EXPIRE 命中过期键 NeedInitialUpdate=false 墓碑不推一致。

Redis 语义同向佐证：expireIfNeeded → dbSyncDelete/dbAsyncDelete + propagateDeletion 不调
touchWatchedKey——时间流逝非并发修改，WATCH 带 TTL 键过期后 EXEC 不 abort。票据验收
「EXEC 必须返回 NIL」若实施将引入假阳性 abort，属正确性倒退。

附带澄清（供后续票据引用，本票不改代码）：
- 票据引作"仓内自身判据"的 wnode/resp/objects/tiered_collection_ops.rs:330/:357 头注属同类
  AI 误注（循环论证）；真正对应 C# ObjectStore/RMWMethods.cs:125（集合删空自愈
  HasRemoveKey → ExpireAndStop 前推一次）的是 range_index/ops.rs:309 已在位的 RI 删空臂，
  与 TTL 面无关。
- wkv/session/raw/write/mod.rs:69-73 头注「后台 GC 亦不推进（C# 后台清除经 functions 推
  版本，待后续对齐）」对 C# 的认知同样失实——C# 后台清除（DELIFEXPIM/ExpireAndStop）
  不推版本，rust 现状即终态，无待对齐项。该注释宜在触及该文件的后续票据中顺手订正。
