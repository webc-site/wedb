审核结论：通过
- 真实性：双锚亲验成立——C# ReplicaSyncSession.cs SendCheckpointAsync:118 于建连前无条件调 AcquireCheckpointEntryAsync，:274 取 GetMinAofCoveredAddress，:301-303 注释+TryAddReplicationDriver 前置钉线（部分重同步形态 skipLocalMainStoreCheckpoint=true 亦先钉后往返，:199 二次 TryAdd 置换授予位点），AofSyncDriverStore.cs SafeTruncateAof:71-117 以在册驱动 previousAddress 取小钳制且快/默认两臂共用同一钳制值；rust initiate_replica_sync（replica_sync_session.rs:387）仅 FullResync 臂 :792-803 预锁，PartialResync 臂 :439-446 直入 begin_replica_recover_clamp:558，函数体全程零入册，钉线迟至 start_aof_sync:515 attach 才注册——比 C# 少前置段属实。
- 危害链：rust safe_truncate_aof（aof_sync_driver.rs:451-470）钳制源仅为 min_aof_address_from_active_sync_tasks（:415-424 fold 以 i64::MAX 为单位元，空册即无钳制，且无 C# replicationUpperBound 兜底项），钳制窗内 add_new_checkpoint_entry（cluster_provider/traits.rs:138-163）以当下复制水位为 truncate_until 推进 truncated_until 可越过授予位点，attach 经 start_gate_ok（:284-295）拒绝；negotiate_resync :904 `rep_tail < trunc_floor && !fast_aof_truncate` 坐实默认配置退化全量、fast 豁免预检成拒绝重试环。
- 非重复：与 done/wedb-repl-diskbased-partial-resync-skips-replica-recover-clamp（往返缺失+漏应用面，其修复即本票所审钳制臂）、done/wedb-repl-aof-pump-wireless-pin-consume-defeats-truncation-pin（泵破既有钉线面）异轴正交，deviations §116 仅在册 repl_offset2 钳判据；task/todo、task/ing（空）、task/reject 无同面票。
- 架构合规：复用 try_add_replication_driver 同 node_id 原地置换单机制、退钉对齐全量臂既有 try_remove 口径（replica_sync_session.rs:837-842 与 :468-473），不建第二套钉线，无假桩无过度设计。
- 可执行度：改动点集中于 begin_replica_recover_clamp 入口预锁与判败臂摘除，授予位点置换由既有 attach 链零新增承接，验证点含并发截断与 fast 分支收敛，闭环可落。
- 格式纯粹：纯文本无加粗/表格/横线，双侧代码路径齐全。

磁盘链 PartialResync 钳制往返臂缺截断钉线前置段，窗口内检查点截断可越过授予位点

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# DiskbasedReplication/ReplicaSyncSession.cs:AcquireCheckpointEntryAsync 在 SendCheckpointAsync 开头即无条件入册 AOF 同步驱动（:301-304 注释 Enqueue AOF sync task with startAofAddress to prevent future AOF truncations，:274 startAofAddress = cEntry.GetMinAofCoveredAddress()，:303 TryAddReplicationDriver），SafeTruncateAof（AofSyncDriverStore.cs）据此把截断线钳在钉位点之下，保到二次 TryAdd 以授予位点置换、finally RemoveReader 释放为止——钳制往返全程主端绝不截断越过副本将恢复的位点。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   rust initiate_replica_sync（wedb/wedb/src/server/replication/replica_sync_session.rs）仅 FullResync 臂经 send_checkpoint_and_recover 预锁钉线（:792-803 pin_start + try_add_replication_driver）；PartialResync 臂（:439-443）直入 begin_replica_recover_clamp（:558），从协商到副本回传位点全程零驱动入册，钉线迟至 start_aof_sync 的 attach_replica_wire 才注册——比 C# 少了前置段。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
   钳制往返窗口（秒级）内主端并发检查点完成（add_new_checkpoint_entry → safe_truncate_aof）即令 truncated_until 越过 granted 位点，随后挂流 start_gate_ok（aof_sync_driver.rs）拒绝：默认配置下 attach 失败副本轮询重试，下轮协商 rep_tail < trunc_floor 判 is_partial_possible=false 退化整库快照重灌（C# 同场景钉线保住部分重同步）；fast_aof_truncate=true 且 allow_data_loss=false 时 trunc_floor 预检被 fast 豁免，每轮仍 PartialResync、每轮被 attach 闸拒绝，attach-往返-拒绝重试风暴不收敛，直至写负载间歇。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/replication/replica_sync_session.rs:initiate_replica_sync
wedb/wedb/src/server/replication/replica_sync_session.rs:begin_replica_recover_clamp
wedb/wedb/src/server/replication/replica_sync_session.rs:send_checkpoint_and_recover
wedb/wedb/src/server/replication/aof_sync_driver.rs:start_gate_ok
wedb/wedb/src/server/replication/aof_sync_driver.rs:AofSyncDriverStore::safe_truncate_aof

对应 c# 文件与函数：
garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:AcquireCheckpointEntryAsync
garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:SendCheckpointAsync
garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:SafeTruncateAof

精炼执行方案：
1. begin_replica_recover_clamp 进入时按 C# 同序以本地检查点 min_aof_covered_address 预锁入册 AofSyncDriver（无检查点形态取 wal begin，与既有 begin_span 同源），钳制往返全程受 safe_truncate_aof 以该钉线取小钳制；授予位点回传后由既有 attach_replica_wire→attach_stream_driver 的 try_add_replication_driver 以同 node_id 原地置换钉线为授予位点（复用既有置换语义，不建第二套钉线机制，零新增代码路径）。
2. 判败/超时臂按全量臂同口径摘除预锁驱动（往返失败 try_remove，对齐 send_checkpoint_and_recover 传送失败退钉与 initiate_replica_sync res.is_err() 退钉两处既有口径），杜绝孤儿钉线滞留；预锁自身被 start_gate_ok 拒绝（fast 豁免窗内 trunc_floor 已越覆盖位）即降级 FullResync 走 send_checkpoint_and_recover，不留 Err 直败（对齐 wedb-repl-aof-pump-wireless-pin-consume-defeats-truncation-pin 修复形）。
3. 测试验证点：构造钳制往返窗内并发 safe_truncate_aof 用例，断言 truncated_until 恒不越过 granted 位点、attach 必成功；fast_aof_truncate=true 且预锁被拒场景断言单轮降级全量收敛不风暴；默认分支断言截断越线后下轮协商退化全量而非静默丢段。

终态注记：
- 收口形态：在 wedb/src/server/replication/replica_sync_session.rs 中 begin_replica_recover_clamp 入口以本地检查点 min_aof_covered_address 预锁入册 AofSyncDriver，并在授予位点回传后由 attach_replica_wire 原地置换，失败时摘除预锁，杜绝往返期间并发检查点截断越过授予位点；补齐并发截断与 fast 降级全量锁测。
- 合入哈希：0cf8006
- 状态：已收口归档。

主控收票审计（2026-10-01 r9 波，沙箱 dev 尖亲跑）：
- 方案 1 达形且零新机制：预锁以 pin_start（检查点 min_aof_covered_address，无检查点取 wal begin，
  与 begin_span 同源单值）入册 AofSyncDriver，往返后仍由既有 attach_replica_wire 同 node_id 原地置换，
  未建第二套钉线；begin_span 与预锁位点共享同一 pin_start 变量，杜绝两处取小口径分叉。
- 方案 2 达形：往返体收进 async block，失败/超时臂 try_remove 退钉与全量臂两处既有退钉口径齐；
  预锁被拒走 send_checkpoint_and_recover 降级、不留 Err 直败。
- 观测口合规复核（本席疑点亲验）：ClampGrant / begin_replica_recover_clamp 由私域升为
  `#[doc(hidden)] pub` 非新枚口——同文件 send_checkpoint_and_recover、aof_replication_pump 泵直驱口、
  replication_snapshot_iterator 三处早已循该轨为集成测直驱，本笔沿用既有轨，未破「门钉不落生产路径」纪律
  （doc(hidden) 即本仓生产面直驱口的既定形）。
- 退钉收敛性亲验：Ok(None) 两臂（无本地检查点 / 检查点目录未接线）后，3~5 建连段 attach_replica_wire
  恒执行置换，段失败由既有统一退钉臂兜底，无孤儿钉线滞留窗。
- 残留观（不并案）：降级臂 `full_start.unwrap_or(sync_start)` 在 None 形态下不再经 DataLossCheck
  位点回传比对（副本未收 BEGIN_REPLICA_RECOVER、位点未动），衔接缺口由副本侧收流时的 gap 检测
  退化全量承接，非静默丢段，且票面方案 2 明判「不留 Err 直败」，判为既定裁决形，登记不立项。
- 门禁：本席沙箱复跑新册 replica_partial_resync_truncation_pin 4/4 绿，构建零告警。

