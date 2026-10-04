归档注记：合入 080bd658，导入帧写回收入 rmw 窗(快慢两臂)，窗内 clear_ttl 无闩变体，TTL 降级先出窗

甄别结论：通过（甄别席 J3，2026-09-27，定级 P1——非可串行化丢写/EXPIRE 条件臂误裁/ACK 删键永久发散）。窗外裸写链亲验：frame_import.rs 写回三步（:313 upsert_string、:321 persist_key、:328 upsert_tag、:351 put_ttl_sync）全函数零 try_rmw_window；ttl_sync.rs:120-124 头注前置契约「调用方须持本键读改写窗口闩……本函数不自取闩」收口名单确无 frame_import，系唯一契约外生产调用方。rmw_window.rs 模块头铁律在码且自证 C# 锁形：InternalUpsert.cs:67 与 InternalRMW.cs:70 同取 FindOrCreateTagAndTryEphemeralXLock 亲验命中。用户写持窗先例亲证：set.rs:256/:414 try_rmw_window。ASKING 放行使并发窗真实可达。派沙箱席 c01b。

审核结论：通过（P1 真案。①落帧链无窗坐实：frame_import.rs 三臂经 storage_session.rs:808/692→wkv try_upsert_tag_sync 仅 enter_gated 纪元守卫无桶闩（其契约注 :43-46 明言调用方须持 try_rmw_window）；put_ttl_sync 头注 :120-124 前置契约「不自取闩」，收口名单无 frame_import——实测系唯一契约外生产调用方；上层逐查 cluster_migrate_slow/cluster_sync_slow 两壳仅 enter_batch，全链零 try_rmw_window，非虚报；rmw_window.rs:47-54 铁律在册。②C# 实证：InternalUpsert.cs:67 与 InternalRMW.cs:70 同取 FindOrCreateTagAndTryEphemeralXLock，RespClusterMigrateCommands.cs:201/295 SET 同锁表串行。③ASKING 可达与判净段无矛盾——slot_verify.rs:194 与 C# 同形放行，放行恰证竞窗真实；用户写族均持窗（set.rs:256/414），唯导入旁路，丢写/EXPIRE 条件臂误裁/ACK 删键永久发散推导成立。④五池无同题票）

整理执行方案（审核席订正版，供 fix 消费）：
1 窗内只配无闩变体（clear_ttl/put_ttl_sync，仿 set.rs:414 形），失窗才降级带闩全量口；严禁窗内双取同址桶闩自锁
2 ttl_sync.rs 头注收口名单补登 frame_import
3 锁测：IMPORTING 态 ASKING 用户写与落帧并发窗（丢写/TTL 误裁双臂）断言可串行化终态

迁移帧导入落帧臂为窗外裸写，绕开 RMW 窗桶闩纪律，与目的端 ASKING 用户写构成非可串行化丢写

问题分析：
1 Garnet 契约对齐（C# 侧）：导入核心 RespClusterMigrateCommands.cs:Process 的
   记录写入走 basicGarnetApi.SET(in diskLogRecord)，引擎侧 InternalUpsert.cs:67
   与 InternalRMW.cs:70 同取 FindOrCreateTagAndTryEphemeralXLock、共用同一份
   store.LockTable 桶闩，故导入 SET 与并发用户 RMW 的读—算—写全程在内存上严格
   串行，任意交错结局均可串行化。值与 TTL 一体随记录原子落笔，无分步窗。
2 工程现状确证（Rust 侧）：frame_import.rs:import_migration_frames 记录写回
   三步（Env 臂 persist_key 清退、Str/Env 臂 upsert_string/upsert_tag 值写、
   put_ttl_sync TTL 回填）全程不取本键 RMW 窗桶闩。值写臂直落
   wkv session/raw/write/mod.rs:try_upsert_tag_sync_unprotected（裸写内核）；
   TTL 回填臂 wnode/src/storage/session/common/ttl_sync.rs:put_ttl_sync 系自述
   无闩变体，其头注前置契约明言「调用方须持本键读改写窗口闩，本函数不自取闩」，
   收口名单仅 EXPIRE/PERSIST/GETEX/GETDEL/SETEX/RENAME 族，frame_import 为契约外
   未收口调用方。wkv/src/session/rmw_window.rs 模块头铁律「裸写回族（SET/DEL 等）
   同纪律持窗……绝不容裸写游离闩域外」在导入链被整链旁路。目的端 ASKING 用户写
   并发路径真实存在：slot_verify.rs:verify_slot_state IMPORTING 臂与 C#
   ClusterSlotVerify.cs 同形（ASKING 即放行本端执行）。
3 逻辑危害确证：迁移 REPLACE 导入与目的端 ASKING 用户写同键并发时——
   string RMW 臂（INCR/APPEND/SETRANGE 等）无落笔前复验（复验仅对象信封族），
   用户窗内读旧值后、写回前，导入裸写落域，用户写回覆盖迁移值：源端已 ACK 删键，
   迁移数据永久丢失，两侧对拍发散不可收敛；信封臂复验与写回之间同样存在导入裸写
   落笔间隙（复验非闩内原子）；窗外 put_ttl_sync 无闩裸写落在他者持闩 TTL RMW 的
   读—写间隙，条件臂（EXPIRE NX/GT）按旧 TTL 裁决覆盖迁移 TTL，目的端键提前过期
   或永生，均为 C# 锁形下不可产生的非可串行化终态。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/migration/frame_import.rs:import_migration_frames
（Str/Env 写回臂、旧 TTL 清退臂、TTL 回填 put_ttl_sync 直调位）
wedb/wedb/src/server/cluster_session/migrate.rs:cluster_migrate_slow（导壳）
wedb/wkv/src/session/raw/write/mod.rs:try_upsert_tag_sync_unprotected
wedb/wnode/src/storage/session/common/ttl_sync.rs:put_ttl_sync/del_ttl_sync（前置契约）
wedb/wkv/src/session/rmw_window.rs:BatchStoreSession::try_rmw_window（同址闩源）

对应 c# 文件与函数：
garnet/libs/cluster/Session/RespClusterMigrateCommands.cs:Process
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalUpsert.cs
（与 InternalRMW.cs 同锁表 ephemeral 排他闩）
garnet/libs/cluster/Session/SlotVerification/ClusterSlotVerify.cs:SingleKeyReadWriteSlotVerify

精炼执行方案：
1 import_migration_frames 记录写回臂逐键先取本键窗
  （同步域 BatchStoreSession::try_rmw_window，须降级时对齐用户 SET 族改走
  rmw_window 异步域窗），persist/值写/TTL 回填三段并入同一窗临界区；
  失闩臂降级既有带闩全量口（persist_key/expire_at_ticks 系 batch.persist/
  batch.expire_at 自带键闩）保持判败口径不变。
2 窗外裸写变体直调面收敛登记：ttl_sync.rs 两函数头注收口名单补 frame_import，
  防后续新调用方再旁路。
3 测试验证点：新增竞态锁测（仿 store_sync_arm_save_failure_ttl_intact 真会话
  夹具形制）：IMPORTING+ASKING 用户 INCR/APPEND/EXPIRE NX 与 REPLACE 导入帧
  定向交错，断言终态恒为两串行序之一（迁移值或用户增量叠加值、TTL 无第三种
  非可串行化终态）；既有 migrate_import_batch_equivalence、cluster_migration
  全绿不回退。
