审核结论：通过但收窄（2026-09-28 独立审核席 + 主控采纳，定级 P2 条件形维持）

唯一收窄（必守，违者比原缺陷更重）：判败分支严禁在 replicate_sync_async 体内裸 return Err。
该函数体在 supervise_item 任务内，直返上抛会绕过收尾族 finish_replica_sync 的 catch 臂
（allow_replica_reset_on_failure 时 try_reset_replica + resume_primary_tasks）与 finally 臂
（release_attach_recovery 释放恢复锁）；而三个前台驱动点与重连臂经 try_add_replica_async 握
ClusterReplicate 恢复锁、启动臂经 begin_recovery(InitializeRecover) 握锁——裸 Err 即恢复锁永持，
角色已翻 REPLICA 却不复位，后续一切恢复/重连被 ERR_RECOVERY_LOCK 拒死，无自愈通路。
正解：判败一律经 finish_replica_sync(&provider, opts, Err(文案)) 构造返回，
恰为 C#「该 await 抛异常 → catch(TryResetReplica) → finally(EndRecovery)」的 rust 同构收口。

审查确证（席一手，供 fix 直接引用，不必重跑）：
1 真实性：assembly.rs:338 现位即裸语句弃 bool，行号未漂；原语 checkpoint.rs:54-66 以
  cluster_node_timeout() 为上限超时返 false（None/0 = 无限档）；追平判定 checkpoint.rs:90-111
  只枚举 cluster_sessions 弱引用表、快照 0（批外）放行；批内快照生命周期见
  wedb/wnode/src/resp/resp_server_session/core.rs:906-919（批首 acquire、批尾 release）。
2 C# 一手：ClusterProvider.cs:366-389 while(true)+goto retry 恒真无限自旋；
  ReplicaDiskbasedSync.cs:52-55 与 ReplicaDisklessSync.cs:46-49 注释「Wait for threads to agree」，
  位置在 TryAddReplica 与换代之后、attach 发起之前，票据叙述逐字属实。
3 破坏性段起点为第 4 步 attach（recover_replication 清重放仓库/接收态/推流驱动后连主发
  INITIATE_REPLICA_SYNC，主端回连推检查点，接收侧引擎置换 swap_online_store 与 AOF 代际衔接）；
  后续无栅栏兜窗：恢复锁只互斥恢复操作不拦客户端批，引擎置换只约束置换后写向、
  不回收置换前已 ACK 落旧引擎的写，角色门不重检脚本批。危害面成立不夸大。
  口径订正一处：rust attach 期无 C# storeWrapper.Reset 清库臂（assembly.rs:186-193 明文禁回摆，
  replicationOffset 亦有意不清零），票面「库重置」在 rust 实为「检查点导入 + 引擎置换」，
  落地注释按 rust 实际序列表述，勿按 C# 字面回摆。
4 竞态可达性反证已破：集群形态下全部客户端连接即集群会话（boot.rs:165 单源装配），
  票据所述滞留批恰在原语受护群体内；C# 对位同样只枚举 ActiveClusterSessions
  （ClusterProvider.cs:374-386），同一盲区不构成「承判即无意义」的反证。
  达 false 窗条件：超长 EVAL/FUNCTION 循环批、超大 pipeline 整段重载、或 CONFIG 调小 cluster-timeout；
  compio 每核单线程下他核自旋等待、慢核批不终结即恒不追平。
5 形态裁决：采逐点承判（既有 failover.rs:171 判败帧口径的直接扩展，单机制不新建）。
  反证：改无限档判拒（把「命令臂悬挂 + 恢复锁永持」替代赛跑窗，且与纪元族全仓有界化裁决
  造成第二套口径）；等待前置判拒（C# 栅栏语义是角色翻转化代生效后、attach 前的全批静止，
  前置会新开翻转后批不追平的窗，且形成本仓两套发起序）；判空转判拒（无后续栅栏已验）。
6 查重：deviations.md §95 仅在册于号位空缺清单（正文不可回收），本票系承判族新增消费点，
  非回收 §95；五池内 assembly.rs / replicate_sync_async 另仅命中
  task/done/wedb-cluster-replicate-missing-sequence-manager-generation.md（换代面，正交，
  其票内 :310/:314 锚为换代并入前行号，现漂至 :320/:338）；与同批 replicaof-noone、setslot
  两票不并票（本面为异步发起骨架经收尾族，彼两面为同步命令臂裸调、无收尾族）。
7 票面瑕疵（落地时一并订正）：原案「return Err 字符串」措辞须按本注记收窄形施工；
  票面回归名 replica_diskbased_sync / replica_diskless_sync 非测试文件名（前者是 src 接收模块），
  实际回归面见执行方案 3。

执行方案（fix 直接消费）：
1 改动点唯一：wedb/wedb/src/server/replication/assembly.rs:replicate_sync_async 的 :338 语句，
  换为「if !provider.bump_and_wait_for_epoch_transition_async().await {
  return finish_replica_sync(&provider, opts, Err(文案)); }」；
  文案口径参 failover.rs:174 与 replication_snapshot_iterator.rs:238-240
  （「epoch drain not settled within cluster-node-timeout」），前缀改
  「Failed to initiate replica sync」贴合本窗；:338 注释补「返值承判 + 必须经收尾族」说理。
  diskless 支（replication/replica_diskless_sync.rs:63）共用骨架，零改动即同获。
2 测试：夹具复用 tests/diskless_epoch_drain_failclose.rs 的恒不追平构造
  （注册会话先行 acquire_current_epoch + set_cluster_node_timeout_ms 极小值，见该文件 :185 语句位）。
  断言四件：前台 SYNC 形回 -ERR；复制驱动注册表零发起记录；恢复锁经 finish_replica_sync 正常释放
  （随后再发起不被 ERR_RECOVERY_LOCK 拒）；allow_replica_reset_on_failure 臂角色复位回 Primary。
  重连臂（cluster_session/replication.rs:252）判败仅告警且 allow_role_change 照常。
3 回归面：tests/replicate_switch_generation.rs、replicaof_ring_role_gate.rs、primary_task_role_gate.rs。
  注意 replication_egress_tls_gate.rs 正被在途 tls 席整册迁移至 wedb/wnode_tls_test/tests/，
  本票禁触该文件，回归由迁移后的新位跑。
4 禁触线：仅 assembly.rs 一处 src 改动 + 新测试；禁改 checkpoint.rs 原语语义、禁动 cluster_node_timeout
  值域、禁触 wedb/wnode_tls_test/** 与两个 Cargo.toml（在途席域）、禁 #[allow]、禁假桩夹具。
审核结论：通过但收窄（2026-09-28 独立审核席 + 主控采纳，定级 P2 条件形维持）

唯一收窄（必守，违者比原缺陷更重）：判败分支严禁在 replicate_sync_async 体内裸 return Err。
该函数体在 supervise_item 任务内，直返上抛会绕过收尾族 finish_replica_sync 的 catch 臂
（allow_replica_reset_on_failure 时 try_reset_replica + resume_primary_tasks）与 finally 臂
（release_attach_recovery 释放恢复锁）；而三个前台驱动点与重连臂经 try_add_replica_async 握
ClusterReplicate 恢复锁、启动臂经 begin_recovery(InitializeRecover) 握锁——裸 Err 即恢复锁永持，
角色已翻 REPLICA 却不复位，后续一切恢复/重连被 ERR_RECOVERY_LOCK 拒死，无自愈通路。
正解：判败一律经 finish_replica_sync(&provider, opts, Err(文案)) 构造返回，
恰为 C#「该 await 抛异常 → catch(TryResetReplica) → finally(EndRecovery)」的 rust 同构收口。

审查确证（席一手，供 fix 直接引用，不必重跑）：
1 真实性：assembly.rs:338 现位即裸语句弃 bool，行号未漂；原语 checkpoint.rs:54-66 以
  cluster_node_timeout() 为上限超时返 false（None/0 = 无限档）；追平判定 checkpoint.rs:90-111
  只枚举 cluster_sessions 弱引用表、快照 0（批外）放行；批内快照生命周期见
  wedb/wnode/src/resp/resp_server_session/core.rs:906-919（批首 acquire、批尾 release）。
2 C# 一手：ClusterProvider.cs:366-389 while(true)+goto retry 恒真无限自旋；
  ReplicaDiskbasedSync.cs:52-55 与 ReplicaDisklessSync.cs:46-49 注释「Wait for threads to agree」，
  位置在 TryAddReplica 与换代之后、attach 发起之前，票据叙述逐字属实。
3 破坏性段起点为第 4 步 attach（recover_replication 清重放仓库/接收态/推流驱动后连主发
  INITIATE_REPLICA_SYNC，主端回连推检查点，接收侧引擎置换 swap_online_store 与 AOF 代际衔接）；
  后续无栅栏兜窗：恢复锁只互斥恢复操作不拦客户端批，引擎置换只约束置换后写向、
  不回收置换前已 ACK 落旧引擎的写，角色门不重检脚本批。危害面成立不夸大。
  口径订正一处：rust attach 期无 C# storeWrapper.Reset 清库臂（assembly.rs:186-193 明文禁回摆，
  replicationOffset 亦有意不清零），票面「库重置」在 rust 实为「检查点导入 + 引擎置换」，
  落地注释按 rust 实际序列表述，勿按 C# 字面回摆。
4 竞态可达性反证已破：集群形态下全部客户端连接即集群会话（boot.rs:165 单源装配），
  票据所述滞留批恰在原语受护群体内；C# 对位同样只枚举 ActiveClusterSessions
  （ClusterProvider.cs:374-386），同一盲区不构成「承判即无意义」的反证。
  达 false 窗条件：超长 EVAL/FUNCTION 循环批、超大 pipeline 整段重载、或 CONFIG 调小 cluster-timeout；
  compio 每核单线程下他核自旋等待、慢核批不终结即恒不追平。
5 形态裁决：采逐点承判（既有 failover.rs:171 判败帧口径的直接扩展，单机制不新建）。
  反证：改无限档判拒（把「命令臂悬挂 + 恢复锁永持」替代赛跑窗，且与纪元族全仓有界化裁决
  造成第二套口径）；等待前置判拒（C# 栅栏语义是角色翻转化代生效后、attach 前的全批静止，
  前置会新开翻转后批不追平的窗，且形成本仓两套发起序）；判空转判拒（无后续栅栏已验）。
6 查重：deviations.md §95 仅在册于号位空缺清单（正文不可回收），本票系承判族新增消费点，
  非回收 §95；五池内 assembly.rs / replicate_sync_async 另仅命中
  task/done/wedb-cluster-replicate-missing-sequence-manager-generation.md（换代面，正交，
  其票内 :310/:314 锚为换代并入前行号，现漂至 :320/:338）；与同批 replicaof-noone、setslot
  两票不并票（本面为异步发起骨架经收尾族，彼两面为同步命令臂裸调、无收尾族）。
7 票面瑕疵（落地时一并订正）：原案「return Err 字符串」措辞须按本注记收窄形施工；
  票面回归名 replica_diskbased_sync / replica_diskless_sync 非测试文件名（前者是 src 接收模块），
  实际回归面见执行方案 3。

执行方案（fix 直接消费）：
1 改动点唯一：wedb/wedb/src/server/replication/assembly.rs:replicate_sync_async 的 :338 语句，
  换为「if !provider.bump_and_wait_for_epoch_transition_async().await {
  return finish_replica_sync(&provider, opts, Err(文案)); }」；
  文案口径参 failover.rs:174 与 replication_snapshot_iterator.rs:238-240
  （「epoch drain not settled within cluster-node-timeout」），前缀改
  「Failed to initiate replica sync」贴合本窗；:338 注释补「返值承判 + 必须经收尾族」说理。
  diskless 支（replication/replica_diskless_sync.rs:63）共用骨架，零改动即同获。
2 测试：夹具复用 tests/diskless_epoch_drain_failclose.rs 的恒不追平构造
  （注册会话先行 acquire_current_epoch + set_cluster_node_timeout_ms 极小值，见该文件 :185 语句位）。
  断言四件：前台 SYNC 形回 -ERR；复制驱动注册表零发起记录；恢复锁经 finish_replica_sync 正常释放
  （随后再发起不被 ERR_RECOVERY_LOCK 拒）；allow_replica_reset_on_failure 臂角色复位回 Primary。
  重连臂（cluster_session/replication.rs:252）判败仅告警且 allow_role_change 照常。
3 回归面：tests/replicate_switch_generation.rs、replicaof_ring_role_gate.rs、primary_task_role_gate.rs。
  注意 replication_egress_tls_gate.rs 正被在途 tls 席整册迁移至 wedb/wnode_tls_test/tests/，
  本票禁触该文件，回归由迁移后的新位跑。
4 禁触线：仅 assembly.rs 一处 src 改动 + 新测试；禁改 checkpoint.rs 原语语义、禁动 cluster_node_timeout
  值域、禁触 wedb/wnode_tls_test/** 与两个 Cargo.toml（在途席域）、禁 #[allow]、禁假桩夹具。
CLUSTER REPLICATE 与 REPLICAOF 复制发起链丢弃纪元静止等待返值，超时未静止即让副本同步破坏性段与滞留写在途批赛跑，已 ACK 写丢失

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# ClusterProvider.cs:366-389 BumpAndWaitForEpochTransitionAsync 为无限自旋（while(true) + goto retry，直至全会话 LocalCurrentEpoch 追平或清零才 break），结构上恒真。ReplicaDiskbasedSync.cs:52-55 与 ReplicaDisklessSync.cs:46-49 在 TryAddReplica 臂与一致性管理器换代后、attach 发起前 await 该原语，注释明示 "Wait for threads to agree configuration change of this node"：副本同步的破坏性段（连主 RESET、库重置/检查点导入、引擎置换、AOF 代际切换）严格 happens-after 全会话批静止，绝无跨角色翻转的在途写批。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 侧原语已改写为有界等待（cluster_provider/checkpoint.rs:54 bump_and_wait_for_epoch_transition_async 以 cluster_node_timeout() 为上限，默认 60000ms，args.rs:17 DEFAULT_CLUSTER_NODE_TIMEOUT_MS，超时返 false）。replication/assembly.rs:338 replicate_sync_async 前置第 3 步 `provider.bump_and_wait_for_epoch_transition_async().await;` 返值直接丢弃，无承判、无留痕，超时后照常进入第 4 步 attach 发起（前台臂与 Background 即发即忘臂均不区分）。
3. 逻辑危害确证（并发/数据丢失/资源泄露等实际危害）
静止未达成即 attach：滞留会话的 bump 前在途批继续执行本地写，与副本同步任务的换库/清库/引擎置换赛跑——写落旧引擎随置换蒸发而批语义应答照发（已 ACK 写丢失），或落新主代际 AOF 与主端全序分叉（副本漂移无自愈路径）。与本原语已收口的 failover 族（tests/failover_epoch_drain_failclose.rs，deviations §95 判败口径）与 r25 迁移族同原语同害同判；复制发起链属该有界化族跨域残余圈批中本轮新登的返值丢弃消费点（failover.rs:171 与 replication_snapshot_iterator.rs:237 两处承判先例在册）。
定级：条件形（须有批滞留超过节点超时窗：默认 60s 窗内长 Lua 批/大键空间重载可达，CONFIG 调小 cluster-timeout 即易达），害真为 ACK 写丢/主从漂移，故不降过 P2。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/replication/assembly.rs:replicate_sync_async（:338 返值丢弃点）
wedb/wedb/src/server/cluster_provider/checkpoint.rs:bump_and_wait_for_epoch_transition_async（:54 有界返 false 原语）

对应 c# 文件与函数：
garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:TryReplicateDiskbasedSyncAsync（:55）
garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDisklessSync.cs:TryReplicateDisklessSyncAsync（:49）
garnet/libs/cluster/Server/ClusterProvider.cs:BumpAndWaitForEpochTransitionAsync（:366-389 恒真无限自旋）

精炼执行方案：
（本段原案已被顶部审核结论收窄覆盖：字面 return Err 会绕过 finish_replica_sync 收尾族致恢复锁永持，
 施工一律按顶部注记的「经 finish_replica_sync 构造 Err」形态，禁直读本段。）
1. assembly.rs:338 承判返值：`if !provider.bump_and_wait_for_epoch_transition_async().await { return Err("Failed to initiate replica sync: epoch drain not settled within cluster-node-timeout"); }`，复用既有 Err 消费面（驱动点写 -ERR、supervise_item 尾段 finish_replica_sync 走 catch 复位 + finally 释放），杜绝未静止即 attach。
2. 测试验证点：复用 tests/diskless_epoch_drain_failclose.rs 恒不追平夹具（注册会话先行批首 acquire_current_epoch + set_cluster_node_timeout_ms 极小值），断言返 Err、attach 零发起（复制驱动注册表无记录、恢复态经 finish_replica_sync 正常释放）、前台臂回 -ERR 帧；既有复制面（replica_diskbased_sync/replica_diskless_sync 用例）不回退。

## 终态注记（fix 席，分支 repl-sync-epoch-drain-failclose）

### 1 承判点对位
- wedb/wedb/src/server/replication/assembly.rs `replicate_sync_async` 第 3 步：原 :338 裸语句
  `provider.bump_and_wait_for_epoch_transition_async().await;`（弃 bool）→ 现位 :358-367
  `if !provider.bump_and_wait_for_epoch_transition_async().await { return finish_replica_sync(&provider, opts, Err(文案)); }`。
  opts 为 Copy（replicate_sync_options.rs:8 derive Clone/Copy），在 :361 值传不夺后用于 :373 起 attach 臂。
  diskless 支（replication/replica_diskless_sync.rs 共用 replicate_sync_async 骨架）零改动即同获承判。

### 2 经收尾族的构造返回原文与文案
  Err("Failed to initiate replica sync: epoch drain not settled within cluster-node-timeout")。
  文案口径系既有 failover.rs:173「ERR epoch drain not settled within cluster-node-timeout」+
  replication_snapshot_iterator.rs:239「diskless scan gate: epoch drain not settled within cluster-node-timeout」
  同族判败帧，前缀改「Failed to initiate replica sync」贴合本发起窗（与 assembly.rs:283-285
  attach 失败既有帧「Failed to initiate replica sync to {primary}: {reason}」同前缀族）。
  落地注释（:336-357）按 rust 实际序列表述为「检查点导入 + swap_online_store 引擎置换 + AOF 代际衔接」，
  明文引本文件 :186-193「attach 无 C# storeWrapper.Reset 清库臂、replicationOffset 有意不清零」，未写「库重置」。

### 3 为何绕开裸 Err（锁与复位证据链锚）
- 该臂体跑在 supervise_item 任务内（:311）：块内 `return` 产出 supervise_item 的 Ok(Err(..))，
  经 :362 `Ok(result) => result` 直接上抛，不经 finish_replica_sync。
- finish_replica_sync（:373-403）catch 臂：result.is_err() && opts.allow_replica_reset_on_failure && cluster_manager 在场
  → cm.try_reset_replica()（cluster_manager.rs:633 置本端角色 Primary、make_replica_of(None) 清主指针）
  + provider.resume_primary_tasks()（assets.rs:156 恢复周期任务/GC）；
  finally 臂：release_attach_recovery（:69 → end_recovery，upgrade_lock=false 释到 NoRecovery）。
- 恢复锁：rm.begin_recovery（replication_manager.rs:600）从 NoRecovery 握 ClusterReplicate，
  end_recovery（:629）状态矩阵（:644-659 ClusterReplicate→NoRecovery 合法）释放。三个前台驱动点/重连臂经
  try_add_replica_async 握 ClusterReplicate（cluster_manager_worker_state.rs:171 begin_recovery(ClusterReplicate)），
  启动臂经 begin_recovery(InitializeRecover)（cluster_provider/replication.rs:328）。
  裸 Err 即跳过 release_attach_recovery → 恢复锁永持，后续取锁被 ERR_RECOVERY_LOCK（cluster_manager_worker_state.rs:212）拒死、
  角色滞留 REPLICA 无 try_reset_replica 复位 → 无自愈。本形制恰为 C# 一手锚 ClusterProvider.cs:366-389
  该 await 抛异常 → ReplicaDiskbasedSync.cs:188-196 catch(TryResetReplica) → :197-214 finally(EndRecovery) 的 rust 同构收口。

### 4 测试臂清单与断言实质
- 新增 wedb/wedb/tests/replicate_sync_epoch_drain_failclose.rs `#[compio::test]
  replicate_sync_epoch_drain_failure_finishes_via_cleanup_family`：真实装配（ClusterProvider::new +
  initialize_replication_manager + WedbStore/WalLog 接线 + ClusterManager/ClusterConfig 角色 REPLICA 挂靠主端、
  主端 endpoint 指向出站记录靶），前置 begin_recovery(ClusterReplicate) 握锁，恒不追平夹具
  （bump_current_epoch → create_cluster_session → acquire_current_epoch → set_cluster_node_timeout_ms(50)），
  直驱 try_replicate_diskbased_sync_async。断言四件（真实断言、非类型/非假桩）：
  (1) 返回 Err 文案含「epoch drain not settled within cluster-node-timeout」且前缀「Failed to initiate replica sync」；
  (2) 出站记录靶零帧（attach 从未连主发 INITIATE_REPLICA_SYNC）；
  (3) rm.recovery_status()==NoRecovery 且随后 begin_recovery(ClusterReplicate) 返 true（锁已释放、不被 ERR_RECOVERY_LOCK 拒）；
  (4) allow_replica_reset_on_failure 臂 config.local_node_role()==Primary 且 local_node_primary_id()==None（复位回主）。
  夹具释放后排空等待恢复放行（注册面无泄漏锁死静止判定）双向确证。
- revert-proof：还原裸语句弃返值 → 进第 4 步 attach，靶收帧（断言 2 红）+ try_replicate 返 Ok（expect_err 红）；
  还原裸 return Err（绕收尾族）→ 锁不释放（断言 3 红）+ 角色滞留 REPLICA（断言 4 红）。三面独立转红。
- 夹具复用形态对位 diskless_epoch_drain_failclose.rs:180-187（park 工装）；本臂自持装配未 mod common，
  系因 common/mod.rs 的 replica_host/ReplicaSessionProvider 在本臂用不到、--tests 单测目标会产 dead_code
  触 clippy -D warnings 红（其余 12 个 mod common 消费点均调 replica_host，唯本臂若复用则孤例），故照 failover_epoch_drain_failclose.rs
  同族自持装配形制。

### 5 锚漂移订正
- 票面「assembly.rs:338」行号：施工前实测未漂（原裸语句确在 :338）；加承判注释块（:336-357）后 if 判败形制落 :358。
- 票面「failover.rs:171 判败帧」：实位于 wedb/wedb/src/server/cluster_session/failover.rs:171（非 replication/failover.rs，
  审核结论 §5 所指 failover.rs:171 系此点；:173 为文案行）。
- 票面 §3「replication_snapshot_iterator.rs:238-240」：实测 :237 `if !provider...`、:239 文案行，微漂 1 行，符号名一致。
- checkpoint.rs:54-66 async 臂返 bool、:90-111 追平判定只枚举 cluster_sessions 弱引用表快照 0 放行；
  C# ClusterProvider.cs:366-389 while(true)+goto retry 恒真、只枚举 ActiveClusterSessions（:374-386）；
  ReplicaDiskbasedSync.cs:52-55 / ReplicaDisklessSync.cs:46-49「Wait for threads to agree」在换代后 attach 前——均一手实读确证，无漂。

### 6 未尽面（归姊妹票，本票不越界）
- 同步命令臂裸调 unsafe_bump_and_wait_for_epoch_transition 三点（cluster_session/slot_mgmt.rs:427/:502、
  replica_of.rs:45）与包装位 cluster_session/mod.rs:163 的 #[must_use] 属姊妹票
  task/todo/wedb-cluster-mgmt-epoch-drain-warn-only-trace.md 域，本票禁触、未做。
- 重连臂 cluster_session/replication.rs 驱动点判败仅告警且 allow_role_change 照常（执行方案 2 述），
  其消费面即本骨架返回值 Err，本票已在发起臂收口；重连臂的告警形为既有行为未改动。

### 7 自查风险
- 待主控门禁：本席仅 cargo check --offline -p wedb --test replicate_sync_epoch_drain_failclose 清编译零告警；
  未跑 cargo test/clippy（门禁归主）。compio 单测下 begin_recovery 握锁→判败→end_recovery 序列已按
  状态矩阵（replication_manager.rs:612/:644-659）手工核验。
- 恒不追平夹具依赖 ClusterProvider::new() 已登记 self_weak（create_cluster_session 方入 cluster_sessions 弱表）——
  与 diskless/failover 同族夹具同前提，已由二者在册绿灯确证。
- 出站记录靶 accept 循环为 detached 常驻，compio::test runtime 随测函数返回回收；修复态下从不被连，无悬挂风险。
- 分支 repl-sync-epoch-drain-failclose；单 commit 交付（承判改动 + 回归臂 + 本注记同 commit），
  HEAD 哈希 见 git rev-parse --short HEAD（本 commit 由 b81d443 amend 收编注记而来，最终 HEAD 即工单回报所载哈希）。

---

## 主控验票注记（2026-09-28 r436 收票）

### 1 并回与净面
- merge `c915695`（单 commit `36b90dd`），对 first-parent 净面 3 文件：本票面 +78、
  `wedb/wedb/src/server/replication/assembly.rs` +33/-2、新测试臂 217 行，与回报所列一致，零越界。

### 2 源码复核（独立双参 diff）
- 判败路径确经 `finish_replica_sync` 构造返回，未采裸 `return Err`——catch 臂
  （`try_reset_replica` + `resume_primary_tasks`）与 finally 臂（`release_attach_recovery`）
  的可达性因此保住，此为本票核心危害面（恢复锁永持 + 角色已翻 REPLICA 却不复位、无自愈通路）的收口点，形态正确。
- 注释块自陈「禁另起第二套判败机制」，与本原语既有 failover 判败帧同判，符合单机制规矩。

### 3 测试面复核
- 四断言（Err 文案 / 零出站帧 / `RecoveryStatus::NoRecovery` 且锁可再取 / 角色复位回 Primary）
  覆盖票面判据全集；弃返值与裸 Err 两形制各有 revert-proof，属真回归臂非烟测。
- 真实装配 + 记录型假主端，无 `#[allow]`/`#[expect]`、无假桩、无未闭环占位。
- 「刻意不 `mod common`」的理由经一手核验成立：`common/mod.rs` 的 `replica_host` /
  `ReplicaSessionProvider` 本臂确不消费，若复用则在该测试二进制内成 dead_code，
  触 `clippy -D warnings` 红；与 `failover_epoch_drain_failclose.rs`、
  `diskless_epoch_drain_failclose.rs` 同族自持装配形制一致，非孤立怪形。

### 4 锚漂移订正：采纳
- `failover.rs:171` 实位于 `wedb/wedb/src/server/cluster_session/failover.rs`（非 replication/ 下），席之订正对；
  `replication_snapshot_iterator.rs` 微漂 1 行、符号名一致。票面与台账一律禁钉行号，以符号名为准。

### 5 主控侧追加订正（commit `5eca964` → 本波末再订）
- 先删注释与票面里「deviations §95 承判口径的同族扩展消费点」引述：§95 在台账仅存号位（正文不可回收），
  作不得承判据；改以本码内 failover 判败帧为据。
- 姊妹票落地后，「有精确逆件判败回滚 / 无逆件管理臂 warn 留痕」族分界已在台账 **§182** 在册（正文含本 replicate-sync 族），
  本码注释随即将「族分界待落册」改为直引 §182，承判口径自此有册内单源。

### 6 姊妹票状态
- `wedb-cluster-mgmt-epoch-drain-warn-only-trace`（`slot_mgmt.rs` SETSLOT/SETSLOTSRANGE 各臂、
  `replica_of.rs` 升主臂 warn-only 留痕 + `cluster_session/mod.rs` 包装位 `#[must_use]`）由并发席先行落地
  （merge `cb8bb69` / 实现 `3f028d5` / 归档 `aeb704f`，并自带 §182 台账条目），本席未重复派单；
  两票同域前后脚并入，无第二套判败机制、无分叉口径。
- 重连臂 `cluster_session/replication.rs` 告警形为既有行为，本票只收口发起臂返回值消费面，未扩面，正确。

## 主控门禁回填（2026-09-28 收口席）
- 单轮门禁于 dev 尖 eeda83e 跑毕：./test.sh --no-fail-fast 5198 tests run / 5198 passed /
  1 skipped / EXIT=0；./sh/clippy.sh 三组 EXIT=0；bun js/check.js EXIT=0。
  本票测试臂 replicate_sync_epoch_drain_failclose.rs 两形制 revert-proof 与四断言实证落绿。
- 该测试文件残留一类非阻断警告：clippy::await_holding_lock（:201 取 current_config 读守卫
  跨 :214 屏障 await）——该 lint 官方语义即不吃 -D warnings，测试全绿无死锁复现，
  归入 r437/r438 门禁档第二节未尽面，若后续扩为并发多守卫另票收口，不借门禁顺改。
- 承判先例引述与 §182 直引（commit 5eca964、c2f62f9）现册在册，本票收口无挂起项。
