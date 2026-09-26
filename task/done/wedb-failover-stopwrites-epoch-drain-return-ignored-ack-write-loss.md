归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 89f53631（P1，r26 审核票直派，src 净 +45/−8、锁 +436 另册），收口形态：三缝同封单机制——停写应答慢路径返值承判（false 即 try_restore_stop_writes 赎回＋-ERR 零位点应答，对 C# BlockingWait 静止达成才回位点）、primary 让渡后栅栏 drain_settled 承判（false 不探测不下发落既有赎回臂；赎回后归位栅栏判净注记：松绑方向批边界自收敛）、replica pause 臂 `_ => String::new()` 坍缩拆臂（超时/打断/空串 from_string("")=Some(零位点) 假追平点一律 return false，对 C# catch 语义）；§95 尾句 failover 族收编＋旧定性失实更正＋行号去钉化（单句改写段身留迁移席）。锁 tests/failover_epoch_drain_failclose.rs 三用例，三处 revert 各自实测转红还原全绿；邻域 cluster_failover 22 例＋diskless_epoch_drain 不回退。锚漂移：§95 尾句现位 :1245+ 按内容定位执行零违例。

审核结论：通过（r26 审核席，定级 P1；已 ACK 写永久丢失、槽位让渡后无自愈路径，与 r25 迁移族同原语同害同判。成害需故障转移进行中会话批滞留超 cluster-node-timeout，默认 60s 窗窄但旋钮可被运维调小且后果不可逆，故不降 P2）。逐锚实测全部属实、行号零漂移：failover.rs:166 裸语句 await 弃返值且 :173-176 照采位点回应答；primary_failover_session.rs:254-258 弃返值、:266 探测、:274-275 TAKEOVER、:282-291 既有赎回臂在位；replica_failover_session.rs:114-132 超时/打断 `_ =>` 臂 String::new()，client.rs:222 unwrap_or_default 同形坍缩，address.rs:141-147 实测 from_string("")=Some(零位点)，any_lesser（address.rs:299-305）对 len 0 目标恒 false → replication_manager.rs:1094-1100 快路径立即判「已追平」→ 无确认直入接管，第二害成立。C# 硬锚亲验：ClusterProvider.cs:366-389 无限自旋恒 true、RespClusterFailoverCommands.cs:128-129 BlockingWait 后才回位点、ReplicaFailoverSession.cs:161 fire-and-forget（该臂判净属实）、MigrationDriver.cs:160 承判先例；C# FromString("") 虽返 length-1 零值但错误路走 WaitAsync 抛 → catch(:107-112) → false，错误与零位点绝不混同。危害链无兜底：批首纪元快照 core.rs:820-829，try_stop_writes 让渡后已过槽门的滞留批仍提交、位点越过采样值，检查点截断线与 AOF 重放只覆盖 ≤S，新主带缺口接管、旧主降副本被整店覆没。唯二漂移：deviations §95 尾句实位 :1230（票写 :1226，指该段无碍），§95 登记 failover.rs:159 系旧快照且其「无收敛不变量」定性失实，本票收编更正合规。查重：与 r25 迁移票正交（其票头尾注明言 failover 域留后续席，本票即该席）；todo/ing/reject/done 内容级扫描无撞票。

审核裁定执行方案（供 fix 直接消费，替代文末原方案）：
1. failover.rs:162-166：闭包增捕 cm 的 Arc clone（现闭包只捕 provider），`if !provider...await { cm.try_restore_stop_writes(); out.write_resp_error("ERR epoch drain not settled within cluster-node-timeout"); return; }`，达成才走 :173-177 采位点。不设第二判点。
2. primary_failover_session.rs:254-258：返 false 即不探测、不下发，直接置 success=false 落 :282-291 既有 !success && stopped_writes 赎回臂；:286-290 归位臂仅补判据注释（松绑方向、批边界自收敛），与 r25 keys.rs:932 同形。
3. replica_failover_session.rs:114-132：拆臂——超时/打断/None 一律 return false；resp 为空串（含 client.rs:222 坍缩形与主端 replication_manager 缺席空应答形）亦判败 return false，from_string 只接非空有效文本。不改 from_string 与 client.rs 本体。
4. deviations.md:1230 尾句改写：failover 族由本票收口，更正 :159 定性失实，补计 primary 两处，其余判净位点一句话指回票内第 3 条枚举。
5. 新增 wedb/wedb/tests/failover_epoch_drain_failclose.rs：复用 diskless_epoch_drain_failclose.rs:275-282 恒不追平夹具 + set_cluster_node_timeout_ms，断言 (a) FAILSTOPWRITES 判败回 -ERR 且角色/槽位赎回原状；(b) 主发起判败零 TAKEOVER；(c) pause 臂注入超时/断连/错误帧/空串即 false 且不入 TAKING_OVER_AS_PRIMARY；revert-proof 须转红，cluster_failover.rs 既有用例不回退。
6. 顺带（不单立）：failover.rs:173-176 replication_manager 缺席时 unwrap_or_default 产空串应答属主端应答面，随方案 1 以同一 -ERR 口径覆盖。slot_mgmt/replica_of 的 unsafe_bump（mod.rs:169-174）同步弃返形态本席已判净，不扩面。

failover 停写位点快照链两处丢弃 bump_and_wait_for_epoch_transition_async 有界栅栏返值，静止未达成照样采样位点并放行接管，已 ACK 写随槽位让渡永久丢失（副本端 pause 臂空应答坍缩零位点同害）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 原语结构上不可能失败：garnet/libs/cluster/Server/ClusterProvider.cs:BumpAndWaitForEpochTransitionAsync(:366-389) 对每个 server 的 ActiveClusterSessions 无限自旋直至全部会话 LocalCurrentEpoch 追平，恒 return true。因此 C# failover 链各调用点丢弃返值无失语义——不存在未达成态。停写链的契约承重两对位点：
- 被接管主端应答面：garnet/libs/cluster/Session/RespClusterFailoverCommands.cs:NetworkClusterFailStopWrites(:127-130)——TryStopWrites 后 BlockingWait(UnsafeBumpAndWaitForEpochTransitionAsync)（garnet/libs/cluster/Session/ClusterSession.cs:191-196，内部 `_ = await` 抹返值，因原语恒真），等待必达后才把 ReplicationOffset 作为应答位点回给候选新主。契约语义：该位点回出之时，全会话批内在途写已提交完毕，位点即终态水位。
- 主发起让渡面：garnet/libs/cluster/Server/Failover/PrimaryFailoverSession.cs:BeginAsyncPrimaryFailoverAsync(:107-131)——TryStopWrites(:113) 后 `_ = await BumpAndWaitForEpochTransitionAsync()`(:114)，静止达成才进 WaitForFirstReplicaSyncAsync(:117) 读本地位点探测副本追平，再下发 TAKEOVER。
副本端消费面：garnet/libs/cluster/Server/Failover/ReplicaFailoverSession.cs:PauseWritesAndWaitForSyncAsync(:71-112)——应答经 WaitAsync(failoverTimeout, cts.Token) 限时，超时/异常走 catch(:107-112) 直接 return false 放弃本次 failover；成功臂才把应答交 AofAddress.FromString(:90) 解析为追平目标。即 C# 世界里「无有效应答」与「位点为零」绝不混同。
返值承判先例同族在案：garnet/libs/cluster/Server/Migration/MigrationDriver.cs:160 `if (!await ...BumpAndWaitForEpochTransitionAsync()) return;`——结构上不可达的分支仍显式承判，rust 有界化后该形态即成强制纪律（§95、r25 迁移族票同判）。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 侧把该原语有界化：wedb/wedb/src/server/cluster_provider/checkpoint.rs:bump_and_wait_for_epoch_transition_async(:54-66) 以 cluster_node_timeout()（flags.rs:92-97，0 = 无限与 C# 同构；默认 DEFAULT_CLUSTER_NODE_TIMEOUT_MS = DEFAULT_CLUSTER_TIMEOUT(60) * 1000，args.rs:16、wconf/src/runtime_server_options.rs:18）为上限，超时返 false（wepoch wait.rs:80-85），头注 :50-53 明文「false 仅表达静止未达成，调用方按各自窗口不变量裁决」。failover/拓扑演进全域调用位点枚举（除 §95 无盘键门与 r25 迁移族九处已收口外共 9 位点），其中两处挂数据收敛不变量却裸语句丢弃返值：
- 位点一 wedb/wedb/src/server/cluster_session/failover.rs:network_cluster_fail_stop_writes(:133-181)：:156 try_stop_writes 让渡槽位后，慢路径 :166 `provider.bump_and_wait_for_epoch_transition_async().await;` 弃返值，排空未达成照样在 :173-176 读 get_current_replication_offset 作应答位点回出。对位 C# :128-130「静止必达才回位点」，此处 OK 语义被改写成「至多等 60 秒，等不到也回」。
- 位点二 wedb/wedb/src/server/failover/primary_failover_session.rs:begin_async_primary_failover_async(:250-263)：:251 try_stop_writes 后 :254-258 同形弃返值，随即进 :264-266 wait_for_first_replica_sync_async 于 :134-139 读本地位点发探测、追平即 :274-275 下发 TAKEOVER。对位 C# :114。
配套承重缺陷（位点一判败信号的对方消费臂，不修则本票修复不可达且其自身即成害）：wedb/wedb/src/server/failover/replica_failover_session.rs:pause_writes_and_wait_for_sync_async(:114-133)——应答超时/中止打断落 `_ =>` 臂坍缩为 String::new()，断连与主端错误帧亦经 client.rs:execute_cluster_fail_stop_writes_async(:211-223) 的 unwrap_or_default() 坍缩为空串；而 waof/aof/address.rs:AofAddress::from_string(:141-147) 对空串回 Some(零位点)，:151-158 以零位点为追平目标必然立即达成——「无有效应答」被误判为「主端位点为零、副本已追平」，未经任何位点确认即在缺口上直入 take_over_as_primary_async。C# 对位超时臂 WaitAsync 抛异常 → catch(:107-112) → false 放弃，绝无此形。
同域其余位点判净（依据见第 3 条尾逐位点判定）：primary_failover_session.rs:286-290、replica_failover_session.rs:176-180 与 :199-203、replication/assembly.rs:312-314、cluster_session/slot_mgmt.rs:431-433 与 :500-504、cluster_session/replica_of.rs:43-44。
登记面查重：deviations.md §95(:1226 段尾) 把 failover.rs:159、replica_failover_session.rs:179/:202、assembly.rs:314 列为「用途系配置传播屏障或角色切换整备，无数据收敛不变量挂其上……留后续席统一圈批」——对 failover.rs 停写应答位点定性失实（该处现码即「静止达成才采位点回应答」的数据收敛栅栏，与 r25 票更正 slots.rs 同型），且整体漏计 primary_failover_session.rs 两处；本席即该圈批之 failover 族后续席，§95 所举 replica 两处与 assembly/slot_mgmt/replica_of 经逐点判定确属无收敛不变量位，判净依据随本票落档。

3. 逻辑危害确证（并发/数据丢失/死锁/资源泄露的具体成害链）
成害链一（位点一，副本发起 DEFAULT 故障转移，可推演）：旧主在途写会话批首取纪元快照（wnode/resp/resp_server_session/core.rs:820-829 批内持快照），命令已过槽门、存储提交尚未落地；该批滞留超 cluster_node_timeout（60s 默认档，或运维调小该旋钮，wait.rs:81-84 判超时返 false）→ 候选副本收到 FAILSTOPWRITES 应答的位点 S 系排空前采样，批内滞留写此后提交、位点越过 S → 副本 :151-158 确认「追平 S」后 :187 try_take_over_for_primary 翻转槽位所有权并 TAKEOVER 成主，越 S 的已 ACK 写不随让渡迁移；旧主随后被广播改挂为副本，本地存量随全量同步整店覆没 → 已 ACK 写永久丢失，槽位所有权已翻，无任何路径自愈。C# 侧同一链路由 :128 BlockingWait 无限等待结构上封死，rust 有界化 + 弃返值即把该封死窗重开。
成害链二（位点二，主发起 FAILOVER）：:254-258 栅栏 false 后 :134-139 读 local_offset 探测，滞留批内写同上在探测与 TAKEOVER 之间落地并被 ACK，新主带缺口接管；:271-275 一旦下发 TAKEOVER 即无回头，成害面与链一全等。
成害链三（配套消费臂）：pause 臂对主端断连/超时/错误帧一律按零位点放行——候选副本在主端停写应答根本未达成的情况下直入 TakingOverAsPrimary，缺口宽度无上界（整个未同步增量都可能缺），且全链仅一条 warn 无痕。该臂亦封死位点一改判败后的信号消费（-ERR 帧坍缩空串 → 零位点 → 照常接管），故与本票同缝同修。
逐位点判净依据（不入立案面）：
- primary_failover_session.rs:286-290（赎回后栅栏）：排空方向为「停写恢复可写」的松绑，后续动作仅状态归位与返回 false，无放行危险动作；残余批会话持旧保守视图拒写，批边界自收敛。返值可弃，需补判据注释防再圈。
- replica_failover_session.rs:176-180（接管前栅栏）：本端系副本，客户端写面被角色/槽门拦截；在途批为读/admin 或位点匹配门已封顶的回放条目（位点一收口后，超位点增量的源头即封）；TryTakeOverForPrimary 与 reset 面无可推演的收敛违反。
- replica_failover_session.rs:199-203（接管后栅栏）：C# 对位 ReplicaFailoverSession.cs:161 本就不 await 的 fire-and-forget（`_ = ...ConfigureAwait(false)`），契约此处不要求静止；rust await 有界后弃返值，方向严于 C#。
- replication/assembly.rs:312-314（副本 attach 纪元等待）：对位 C# ReplicaDiskbasedSync.cs:55 / ReplicaDisklessSync.cs:49；本端即将成副本，本地残写按 REPLICATE 语义本就弃置，换店整备由 swap_online_store 断连清扫（checkpoint.rs:145-197）承接，栅栏不挂数据收敛不变量。
- cluster_session/slot_mgmt.rs:431-433、:500-504 与 replica_of.rs:43-44（同步形态 unsafe_bump_and_wait_for_epoch_transition，mod.rs:169-174）：状态变更已在写锁内达成，弃返值仅令 OK 回显的静止断言提前；下游消费者（迁移起始闸 keys.rs:367-381、本票停写链、§95 键门）各挂自己的返值承判静止栅栏，残窗在下游判败栅栏处收敛，无独立成害链。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/cluster_provider/checkpoint.rs:bump_and_wait_for_epoch_transition_async（有界返值契约单点）
wedb/wedb/src/server/cluster_session/failover.rs:network_cluster_fail_stop_writes（位点一）
wedb/wedb/src/server/failover/primary_failover_session.rs:begin_async_primary_failover_async（位点二）
wedb/wedb/src/server/failover/replica_failover_session.rs:pause_writes_and_wait_for_sync_async（配套消费臂）
wedb/waof/src/aof/address.rs:AofAddress::from_string（空串 → 零位点坍缩点）
wedb/wedb/src/client.rs:execute_cluster_fail_stop_writes_async（错误/断连坍缩空串点）
对应 c# 文件与函数：
garnet/libs/cluster/Server/ClusterProvider.cs:BumpAndWaitForEpochTransitionAsync
garnet/libs/cluster/Session/RespClusterFailoverCommands.cs:NetworkClusterFailStopWrites
garnet/libs/cluster/Session/ClusterSession.cs:UnsafeBumpAndWaitForEpochTransitionAsync
garnet/libs/cluster/Server/Failover/PrimaryFailoverSession.cs:BeginAsyncPrimaryFailoverAsync
garnet/libs/cluster/Server/Failover/ReplicaFailoverSession.cs:PauseWritesAndWaitForSyncAsync / TakeOverAsPrimaryAsync
garnet/libs/cluster/Server/Migration/MigrationDriver.cs:BeginAsyncMigrationTaskAsync（:160 承判先例对位）

精炼执行方案：
1. 位点一 failover.rs 慢路径返值承判：`if !provider.bump_and_wait_for_epoch_transition_async().await` → 经在握 cm 句柄调既有赎回单点 try_restore_stop_writes，再 write_resp_error（文案口径「ERR epoch drain not settled within cluster-node-timeout」，对 §95 判败措辞），不回位点应答；达成则照旧。判据只用该有界原语既有 bool，零新机制、不设第二判点。
2. 位点二 primary_failover_session.rs:254-258 返值承判：false → 跳过位点探测与 TAKEOVER（success 置 false），直接走 :282-291 既有 `!success && stopped_writes` 赎回臂回滚返回。不改 :286-290 赎回后栅栏的放行形态，仅补一行判据注释（松绑方向、会话批边界自收敛、无后续危险动作），与 r25 keys.rs:932 归位臂处理同型。
3. 配套消费臂（令案 1 判败信号可被消费，并补齐 C# catch 语义本体）：replica_failover_session.rs:114-132 把 `_ =>` 坍缩臂拆开——超时/中止打断/断连/主端错误帧一律 return false 放弃本次 failover（C# 对位 WaitAsync 抛出 → ReplicaFailoverSession.cs:107-112 catch → false），AofAddress::from_string 仅接有效应答文本；不改动 from_string 自身空串语义（其零位点形为其它调用面在途依赖，勿在本票动）。
4. 登记收尾：deviations.md §95(:1226 段尾「同款忽略返值形态他域多点」句)改写——failover 族本票收口（failover.rs 停写应答与 primary_failover_session 两处系数据收敛栅栏，更正「无数据收敛不变量挂其上」定性失实并补计 primary_failover_session.rs 两处）；其余位点判净依据以一句话指回本票第 3 条。
5. 测试验证点：新增 wedb/wedb/tests/failover_epoch_drain_failclose.rs，复用 wedb/wedb/tests/diskless_epoch_drain_failclose.rs 恒不追平夹具（批首纪元快照会话 + 极小 set_cluster_node_timeout_ms 形）：(a) FAILSTOPWRITES 栅栏未达成即回 -ERR 且停写让渡已赎回（角色与槽位归属复查原状）；(b) 主发起 FAILOVER 栅栏判败即止、TAKEOVER 零下发、槽位赎回成功；(c) pause 臂注入断连/超时/错误帧即 return false，状态不入 TAKING_OVER_AS_PRIMARY。revert-proof：还原任一处弃返值/坍缩臂，对应用例转红。不回退既有面：r16-repl failover 状态机闭环用例与 §95 锁面 diskless_epoch_drain_failclose.rs 全绿。
