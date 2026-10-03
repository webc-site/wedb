终态（2026-09-30 合入）：定裁 (a) 对齐 FieldInfo 收口，合入哈希 991d14c（merge d1b1c98）。
- 收口形态：String 域覆写内核 `try_upsert_tag_sync_unprotected_with_prefix` 在 TTL 腿删除同点成对清退 etag 旁路（新增 `try_strip_etag_sync_unprotected_with_prefix`，del_etag 幂等、页翻转沿既有降级臂交异步 `upsert_tag` 以绝对值幂等重做），异步 `upsert_tag` 于 `del_ttl` 同侧补 `del_etag`；SETEX/SETNX/MSET/GETSET 随单点覆盖。
- 三联动：`rmw.rs` Pass 健在键覆写落笔前剥 etag（守 §17/§18 TTL 恒保留）、`try_grow_in_place` 原位臂同判据、`upsert_rmw` 未过期臂补 `del_etag`；撤销旧 zcode-r157c-srethead「SET 内核保 etag 故 Due 臂自持对偶探针」成文例外（内核已单源清退），INCR/APPEND/SETRANGE 族 RMW 写回与 SET 同判据，杜绝同旁路两套覆写语义。`etag.rs`/`wval/tag.rs` 头注「普通 SET 覆写保留 etag」误读论据随裁订正（TryCopyOptionals 实受 FieldInfo.HasETag 门限执行 RemoveETag）。
- 测试改钉：`garnet_etag.rs` plain_set_overwrite_keeps_etag→plain_set_overwrite_clears_etag，新增 setex_overwrite_clears_etag_baseline；`rmw_expired_rebuild_etag_cascade.rs` alive_rmw_never_touches_etag→alive_rmw_overwrite_clears_etag。EtagWrite(None)→AOF Setwithetag(0) 镜像与回放端已闭环，副本/恢复终态同收敛。定向 cargo check + garnet_etag/rmw_expired_rebuild_etag_cascade/etag_port/etag_conditional_degrade_replay/ri_cold_etag/rename_tiered_aof_replay/recover_test/aof_store_rmw_replay/ttl_sidecar_order/write_kernel_failpath 全绿。

甄别结论:通过(P2,2026-09-30 现码复核,双侧锚亲验成立,五池无重复,deviations 无同面在册裁决)

审核结论：通过（2026-09-30 甲轮45-C，P2 级）。String 域覆写命令族（SET/SETEX 等）未清退 KeyTag::Etag 旁路记录且登记论据误读 C# TryCopyOptionals 事实确证，C# FieldInfo 恒 HasETag=false 确定性清除，导致条件写基线发散。执行席遵照：二选一定裁优先按 (a) 对齐 FieldInfo 规范——覆写内核在 TTL 清除同点成对清退 etag 旁路记录，del_etag 幂等处理，收敛 CAS 基线。

复核席注记（2026-09-30 独立复核同向通过）：C# 四点全链亲验——FieldInfo 恒 false（VarLenInputMethods.cs:393-416/:211-221）、TryCopyOptionals 门限（LogRecord.cs:1416「!srcDataHeader.HasETag || !sizeInfo.FieldInfo.HasETag 即 RemoveETag」逐字）、内联残留臂门（SessionFunctionsUtils.cs:99「ValueIsInline && (expiration == 0 || HasExpiration)」，SETEX 无旧 TTL 恒走重分配臂）、CopyUpdater SET 注释原文（RMWMethods.cs:1081 只提 Expiration 不含 etag）；且 upsert 重排臂另有 OptionalFieldsShift.Restore（OptionalFieldsShift.cs:39-58 ClearHasETag）独立收敛于清除——清除系 FieldInfo 单源规范非布局偶然。定裁倾向 (a) 对齐清退：同族 RI 票（task/done/wkv-ri-rename-dst-etag-bypass-residue-cas-baseline-fork）已取对齐向收口，现登记论据系误读非刻意偏差。执行席取 (a) 时三联动一并收口：INCR/APPEND/SETRANGE 写回清退同判据定裁（C# copy-to-tail 臂经 TryCopyFrom :1330 清、IPU 内位臂残留，rmw.rs:60-61 头注已在册登记该上游自相矛盾）；rmw.rs:120-122「SET 内核裁决保 etag 故不入内核」注释与 zcode-r157c-srethead 例外表述随裁订正；etag.rs 头注与 plain_set_overwrite_keeps_etag 钉测（garnet_etag.rs:569-583，等长覆写臂 C# 恰残留）按裁改写。取 (b) 则 deviations 台账补条并禁再引 TryCopyOptionals 作对位依据。

原票面：
String 域覆写命令族不清退 KeyTag::Etag 旁路记录，在册登记论据误读 C# TryCopyOptionals，条件写基线偏离原型

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）：C# etag 为 LogRecord 尾可选字段，重写后记录是否携带 etag 由 RecordFieldInfo.HasETag 单源规定。MainStore VarLenInputMethods.cs 的 GetUpsertFieldInfo（SET/SETEX/APPEND 臂）与 GetRMWModifiedFieldInfo（默认 HasETag=false，SET/SETEXX/SETEXNX 臂不置位）对 String 域覆写族一律规定 HasETag=false；凡经可选域在场管理插线的重写臂（TrySetValueSpanAndPrepareOptionals 的重分配臂、RMW CopyUpdater 的尾部重写臂）经 LogRecord.cs:TryCopyOptionals/Restore 执行 RemoveETag/ClearHasETag，尾部全新分配臂天然无 etag——即按 FieldInfo 规范 SET 族重写后 etag 消亡。唯一残留臂是 InPlaceWriterForSpanValue 的内联原位捷径（SessionFunctionsUtils.cs：TrySetPinnedValueSpan 不做可选域在场管理，旧 etag 原样残留），且该臂有门：expiration == 0 || HasExpiration 才进入；SETEX 作用于无 TTL 键时恒走重分配臂，C# 侧 etag 确定性清除。C# CopyUpdater SET 分支注释原文为 "along with optionals from source record including Expiration"，不含 etag。
2. 工程现状确证：rust etag 为 KeyTag::Etag 独立旁路记录（wkv/src/etag.rs）。String 域覆写内核 try_upsert_tag_sync_unprotected_with_prefix 只做 TTL 腿删除与信封域清退，无 etag 清退臂；异步 upsert_tag 同；SETEX/SETNX/MSET/GETSET/APPEND/SETRANGE/INCR 族全线不清。wkv/src/etag.rs 模块头登记「普通 SET 覆写保留 etag」并钉测试 plain_set_overwrite_keeps_etag，但登记论据「C# CopyUpdater SET 分支 TryCopyOptionals 连同 etag 复制」系误读——TryCopyOptionals 受 sizeInfo.FieldInfo.HasETag 门限（!srcDataHeader.HasETag || !sizeInfo.FieldInfo.HasETag 即 RemoveETag），SET 族 FieldInfo 恒 false，实际执行的是清除。钉测场景（等长 SET 覆写）恰落在 C# 内联残留臂的覆盖面内，与清除臂不冲突，掩盖分叉。现状形态为「自称对标 C# 而实际偏离」，既非对齐亦非登记偏差。
3. 逻辑危害确证：条件写基线契约分叉且确定性可达（无需竞态与崩溃）——SETWITHETAG k v（etag=1）→ SETEX k 10 v2（C#：无旧 TTL，etag 确定性消亡）→ rust GETWITHETAG 回 [1, v2] 而 C# 回 [0, v2]；SETIFMATCH k v3 1 在 rust 误命中并将 etag 抬至 2，C# 回 [0, v2] 拒写。孤儿旁路记录存活至键删除才随级联清退，与已审通过的在册同族票 wkv-ri-rename-dst-etag-bypass-residue-cas-baseline-fork（RI 换名清退臂漏 etag，P2）危害同款：CAS 基线错乱，主从经 EtagWrite 镜像同残、终态一致但均偏离原型。

涉及代码：
rust 文件与函数：
wedb/wkv/src/session/raw/write/mod.rs:try_upsert_tag_sync_unprotected_with_prefix（String 域 TTL/信封清退，缺 etag 清退）
wedb/wkv/src/etag.rs:模块头「普通 SET 覆写保留 etag」登记条目
wedb/wnode/src/resp/basic_commands/set.rs:apply_set_with_expiry（SET 族写共同体，未触 etag）
wedb/wnode/tests/garnet_etag.rs:plain_set_overwrite_keeps_etag（钉测，注记含误读论据）

对应 c# 文件与函数：
garnet/libs/server/Storage/Functions/MainStore/VarLenInputMethods.cs:GetUpsertFieldInfo、GetRMWModifiedFieldInfo（SET 族 HasETag=false 规范）
garnet/libs/storage/Tsavorite/cs/src/core/Allocator/LogRecord.cs:TryCopyOptionals、RemoveETag、TrySetContentLengthsAndPrepareOptionals
garnet/libs/server/Storage/Functions/SessionFunctionsUtils.cs:InPlaceWriterForSpanValue（内联残留臂及其门条件）
garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:CopyUpdater SET 分支

精炼执行方案：
1. 二选一定裁，禁维持「保留 + 自称对标 C#」的矛盾现状：(a) 对齐 FieldInfo 规范——String 域覆写内核在 TTL 腿删除同点成对清退 etag 旁路记录（del_etag 幂等，EtagWrite{None} 镜像条目与回放端已闭环，副本/恢复同收敛），SETEX/SETNX/MSET/GETSET 与异步臂同判据单点收口；(b) 裁定维持保留语义，则订正 wkv/src/etag.rs 头注论据与钉测注记为如实登记的刻意偏差并补 deviations 台账，不得再引 TryCopyOptionals 作对位依据。
2. 若取 (a)，INCR/APPEND/SETRANGE 族 RMW 写回是否随覆写清退须按同一判据一并定裁（C# copy-to-tail 臂清、内位臂残留，同属上游布局偶然），避免同一旁路出现两套覆写语义。
3. 测试验证点：SETWITHETAG → SETEX（无旧 TTL）→ GETWITHETAG 逐字节对位 C# [0, v]；SETIFMATCH 携陈旧 etag 拒写回 [0, 旧值]；普通 SET 后条件写基线断言与 AOF 重放终态等值；等长覆写臂（C# 残留臂）按定裁结果改钉。
