甄别结论：通过（甄别席 zc-fix-r25-乙，2026-09-26）定级 P1
核验记录：rust 四吞错点现码逐点复跑——rmw_helpers.rs notify_object_rmw 失败仅 log::error! 后照常收尾（:1160-1163「降级为重放面可收敛的告警」注释现读亲见）；object_store_utils.rs:1162-1164 notify_envelope_upsert 失败仅告警回 Ok(true)；keys.rs:577-579 rename_sync 对象域仅告警（:572-573 同点注释自认「新键无从建立，集合键丢失」自证危害）；common.rs:786-788 emit_tiered_mirror 仅告警——四点全数现存续，无合入灭失；契约锚 error.rs:93-94 AofEnqueue「调用方须以错误拒绝该命令防主从发散」现读亲见，正确对偶 storage_session.rs obj_save ? 上抛在案（同域双标坐实）。C# 亲验——RMWMethods.cs:249-257 PostRMWOperation→WriteLogRMW 入队、TsavoriteLog.cs Enqueue 为 public unsafe void（:1075/:1107）无吞错路径。查重：ing 现池无 aoffail 票（其修复面 range_index/向量/紧缩文件与本四点互不重叠，keys.rs 对象域与 slow.rs 向量域系同命令不同域非并案）；deviations 全册零同族命中。架构：四点沿既有 Result 链上抛+终态错误信号严禁借用 Degrade（防非幂等算子二次施加）、不回滚内存值使发散可见，单点收敛 AofEnqueue 契约无第五机制；测试注入桩闭环可执行。格式：纯文本、双侧齐全。定级 P1：入队失败条件下假成功应答+AOF 永久缺条目致主从发散与重启丢写，触发需盘满/背压故障条件，循同族 r167c-aoffail 先例 P1。

审核结论：通过（审核席 zcode-r20-review-aofswallow，2026-09-26）定级 P1
核验记录（逐锚现码亲验）：
四吞错点全部属实：rmw_helpers.rs:1118-1120 notify_object_rmw 失败仅 log::error! 后照常返回 Present；object_store_utils.rs:1162-1164 notify_envelope_upsert 失败仅告警回 Ok(true)；keys.rs:577-579 rename_sync 对象域新键入账失败仅告警（:572-573 同点注释自认「漏记则重放端只删旧键、新键无从建立，集合键丢失」，自证危害）；common.rs:786-788 emit_tiered_mirror 失败仅告警（:761-763 注释自引「与 RI.SET 逐条镜像先例同口径」——该「先例」正是 r167c-aoffail 已判 P1 在办修复的 range_index/ops.rs 吞错，先例本身即缺陷）。
契约锚全部属实：error.rs:91-94 AofEnqueue 契约原文「主存写入已生效，AOF 缺条目，调用方须以错误拒绝该命令防主从发散」；session/mod.rs:1156-1181 三通知口文档均注明「AOF 入队失败沿写路径上抛拒绝该命令」；store/event.rs:167-177 StoreEventSink::new 同口径。正确对偶属实：storage_session.rs:653（upsert_tag 内 notify ?）、:690-691（同步臂 Some(r) => r?）、:695（防御补投 ?）——obj_save 与 resp 层 obj_save_custom_notified 几乎同构，前者 ? 上抛后者吞错，同域双标坐实。
C# 锚全部属实：RMWMethods.cs:249-258 PostRMWOperation 按 NeedAofLog 经 WriteLogRMW 入队；:122-129 与 :207-215（InPlaceUpdater 与 PostCopyUpdater 删空臂）均先置 NeedAofLog 再 ExpireAndStop；PrivateMethods.cs:75-89 WriteLogRMW 直调 appendOnlyFile.Log.Enqueue；GarnetLog.cs:637 internal void Enqueue；TsavoriteLog.cs:1311 public unsafe void Enqueue，ValidateAllocatedLength/AllocateBlock 故障以异常上抛至 RESP 层，无吞错路径。
排重完成：doc/zh/deviations.md 全册 grep「notify_object_rmw / notify_envelope_upsert / notify_tiered_collection_write / 主存先行 / 重放面可收敛 / emit_tiered_mirror / obj_save_custom_notified」零命中，本四点非在册裁决；task/todo 空、task/reject 无同轴；task/ing/zcode-r167c-aoffail.md 修复面为 range_index/ops.rs、drain.rs、vector_manager_replication.rs、slow.rs(rename_vector_set_slow)、compact.rs，与本四点文件（rmw_helpers.rs、object_store_utils.rs、keys.rs、tiered_collection_ops/common.rs）互不重叠，两票并行无冲突；注意 keys.rs(rename_sync 对象域) 与 slow.rs(rename_vector_set_slow 向量域) 系同命令不同域不同文件，非重复立案。
方案核实：run_sync_rmw 返回 ObjLoad 枚举，Degrade 会触发慢臂整体重放对 HINCRBY/ZINCRBY/LPUSH 非幂等算子二次施加（内存 double-apply），严禁借用，须增设终态错误信号，票面方案正确；obj_save_custom_notified 已返回 wkv::Result<bool> 可直接 ? 上抛；rename_sync 可用既有 bail_err_frame! 宏落错误帧；emit_tiered_mirror 可循 save_tiered_meta 既有 Result<(), ()> 漏斗（臂尾 break 'arm Err(()) 落 RESP 错误帧）。单点收敛至 AofEnqueue 契约，无第二机制。
供 task/fix.md 直接消费，执行时按下方原票方案推进。

对象族 RMW 四处 AOF 入队失败静默吞错回复成功，违反 AofEnqueue 单点契约致主从发散与重启丢写（r167c-aoffail 同族未覆盖面）

问题分析：
1. Garnet 契约对齐：C# 对象族 RMW 三钩子（ObjectStore/RMWMethods.cs 的 PostInitialUpdater / InPlaceUpdater / PostCopyUpdater）在记录锁内置 NeedAofLog 标记，PostRMWOperation（RMWMethods.cs:249-258）统一经 WriteLogRMW（ObjectStore/PrivateMethods.cs:75-91）入队 ObjectStoreRMW 条目；删空臂（InPlaceUpdaterWorker output.HasRemoveKey，RMWMethods.cs:122-129）同样先置 NeedAofLog 再返回。Enqueue 链 GarnetLog.cs:637 → TsavoriteLog.cs:1311 为 void Enqueue，无吞错路径，任何入队故障沿调用栈上抛至 RESP 层报错，绝不在 AOF 缺条目状态下向客户端确认成功。rust 侧同契约有明文：wkv/src/error.rs:91-95 AofEnqueue 错误文档「主存写入已生效，AOF 缺条目，调用方须以错误拒绝该命令防主从发散」；wkv/src/session/mod.rs:1156-1182 三个通知口（notify_object_rmw / notify_tiered_collection_write / notify_envelope_upsert）与 wkv/src/store/event.rs:167-177 StoreEventSink::new 均注明「AOF 入队失败沿写路径上抛拒绝该命令，杜绝静默缺条目」；同域正确对偶为 wnode/src/storage/session/storage_session.rs:653/688/695 异步 obj_save 臂用 ? 上抛。
2. 工程现状确证：对象族 RMW 镜像面四处调用点将上述通知口的 Err 用 log::error! 吞掉后无条件按成功收尾：其一 wnode/src/resp/objects/rmw_helpers.rs:1117-1120，run_sync_rmw 在 obj_save_or_gc_raw Ok(true)（信封写回或删空均已生效）后 notify_object_rmw 失败仅告警，照常返回 Present 应答（注释自称「降级为重放面可收敛的告警」）；其二 wnode/src/resp/objects/object_store_utils.rs:1152-1164，obj_save_custom_notified 在信封单次成形写成功后 notify_envelope_upsert 失败仅告警回 Ok(true)（注释自称「主存先行语义」）；其三 wnode/src/resp/key_admin_commands/keys.rs:575-580，rename_sync 对象域新键入账 notify_envelope_upsert 失败仅告警，而同点上方注释自认「漏记则重放端只删旧键、新键无从建立，集合键丢失」，自证危害却仍吞错；其四 wnode/src/resp/objects/tiered_collection_ops/common.rs:786-788，emit_tiered_mirror 的 notify_tiered_collection_write 失败仅告警（注释以「重放自树状态与后续条目收敛」辩解，与 r167c-aoffail 案一已驳斥的「副本可自树状态收敛」同一误判：副本不共享主库内存与物理树文件，从库仅能依赖 AOF 增量流）。
3. 逻辑危害确证：磁盘写满、AOF 背压或刷盘流水线中断（wbase::group_commit::Broken）使 enqueue 返回 Err 时，主库内存已变更且向客户端回成功帧，AOF 永久缺条目：HSET/HMSET/HSETNX/HINCRBY/HINCRBYFLOAT/SADD/ZADD/ZINCRBY/LPUSH/RPUSH 族同步快臂增量条目（ObjectStoreRMW）丢失致副本信封缺字段缺键、HINCRBY/ZINCRBY 计数永久落后；HCOLLECT/ZCOLLECT/ZUNIONSTORE/GEOADD/SPOP/LPOP 族整值条目（ObjectStoreUpsert）丢失致副本持旧值；RENAME 对象域条目丢失致副本只删旧键不建新键；分层稳态树写镜像丢失致副本树缺字段且 meta.size 滞后。主库崩溃重启重放后同等丢写，主从静默发散仅能全量重同步收敛，破坏 WAL 日志先行契约。与已判 P1 的 task/ing/zcode-r167c-aoffail.md（range_index ops/drain、向量重命名、紧缩三案）同族同危害，但该票修复面不含本四点，修复后缺陷仍存续。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/rmw_helpers.rs:run_sync_rmw（notify_object_rmw 吞错 :1117-1120）
wedb/wnode/src/resp/objects/object_store_utils.rs:obj_save_custom_notified（notify_envelope_upsert 吞错 :1152-1164）
wedb/wnode/src/resp/key_admin_commands/keys.rs:rename_sync（RenameDomain::Obj 臂吞错 :575-580）
wedb/wnode/src/resp/objects/tiered_collection_ops/common.rs:emit_tiered_mirror（notify_tiered_collection_write 吞错 :786-788）
契约锚：wedb/wkv/src/error.rs:AofEnqueue（:91-95）、wedb/wkv/src/session/mod.rs:BatchStoreSession::{notify_object_rmw,notify_tiered_collection_write,notify_envelope_upsert}（:1156-1182）、wedb/wkv/src/store/event.rs:StoreEventSink::new
正确对偶：wedb/wnode/src/storage/session/storage_session.rs:StorageSession::obj_save（:653/:688/:695 用 ? 上抛）

对应 c# 文件与函数：
garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:PostRMWOperation（:249-258）与 InPlaceUpdaterWorker 删空 NeedAofLog 臂（:122-129）
garnet/libs/server/Storage/Functions/ObjectStore/PrivateMethods.cs:WriteLogRMW（:75-91）
garnet/libs/server/AOF/GarnetLog.cs:Enqueue（:637）
garnet/libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:Enqueue（:1311，void 无吞错路径，故障上抛至 RESP 层）

精炼执行方案：
1. 四点统一按 AofEnqueue 契约向命令层传错：通知失败沿各自调用链上抛（run_sync_rmw 需为「写已生效、入队失败」增设终态错误信号，严禁借用 Degrade——慢臂重放会对 HINCRBY/ZINCRBY/LPUSH 非幂等算子二次施加；obj_save_custom_notified / rename_sync / emit_tiered_mirror 经既有 Result 链上抛），命令层落错误帧应答，与 storage_session.rs obj_save 异步臂 ? 同款单机制收口，禁第五处继续吞错。
2. 写已生效后的入队失败不回滚内存值（与 r167c-aoffail 案一修复口径一致：显式错误帧使发散可见可感知，客户端得错误而非假成功），删除四处「主存先行/可收敛」误导注释并回指 AofEnqueue 契约与 r167c-aoffail 票。
3. 测试验证点：AOF 入队失败注入桩下，HSET/HINCRBY/LPUSH（同步臂与慢臂）、RENAME 对象键、分层态键 ZADD 各断言回错误帧且不回 +OK/:1/计数帧；对照异步 obj_save 臂同故障同帧；确认修复不与 r167c-aoffail 在办修复面冲突（本四点文件互不重叠）。

合入哈希：2911227685c5e8b0e356525ce845f362775bf176 收口形态：四处吞错点沿既有 Result 链上抛至 AofEnqueue 契约单点收敛（SyncRmwOutcome 增设 AofFail 终态臂 17 处 match 补全），真故障 sink 注入五案矩阵回归 5/5 绿，workspace cargo check 零告。
