领票注记（2026-09-28 主控）：甄别结论通过，五条标准逐项现验（真实性/非重复/架构合规/可执行度/格式纯粹）；
定级 P3 条件形维持，派席沙箱 /tmp/fork/wedb-cluster-mgmt-epoch-drain（分支 wedb-cluster-mgmt-epoch-drain）；
管理臂有界纪元排空三处弃值改承判留痕（warn 记录未静止事实），应答按生效照实回，绝不假报失败回滚。

审核结论：通过但收窄，并与 replicaof-noone 票合票（2026-09-28 独立审核席 + 主控裁决，定级 P3 条件形）

族票覆盖面（三弃值点、两文件、同一原语同步档、同一包装件 cluster_session/mod.rs:169）：
1 wedb/wedb/src/server/cluster_session/slot_mgmt.rs:network_cluster_set_slot :427
2 wedb/wedb/src/server/cluster_session/slot_mgmt.rs:network_cluster_set_slots_range :502
3 wedb/wedb/src/server/cluster_session/replica_of.rs:network_replicaof NO ONE 臂 :45
  （原 task/issue/wedb-cluster-replicaof-noone-epoch-drain-return-ignored.md 判拒，
  其「回判败帧」主张不采纳，残余「静默弃值无留痕」并入本票第三点，见 reject 票顶注记）

全族统一判据（本票裁决的核心口径，落码注释与后续审查均按此条）：
有界纪元排空返 false 的处理，按「本臂有无精确逆件」分界，不按「有无返值」一刀切——
  有精确逆件 / 有干净失败通道 → 承判判败 + 回滚（先例 failover.rs:171 的 try_restore_stop_writes、
    迁移族七点、以及本批 replicate-sync 票 assembly.rs:338 经 finish_replica_sync 收尾族）；
  无精确逆件的管理臂 → 承判留痕（warn 记槽号/区间/臂位 + 未静止事实），应答按「变更已生效」照实回，
    绝不假报失败、绝不回滚、绝不给收口钩加门。
理由：本原语 false 描述的是本地活性（别的核上有批没收尾），不是本命令的成败；
  C# 原语恒真（ClusterProvider.cs:366-389 末尾 return true），且 C# 包装件本身就是弃值
  （ClusterSession.cs:191-196 签名为 async Task，:194 `_ = await ...`），
  「OK 蕴含全静」是 C# 靠无限自旋兜住的结构事实，不是对客户端的承诺；
  Redis 原生 SETSLOT / REPLICAOF NO ONE 亦为变更落位即 +OK，无静止前置承诺。

审查确证（席一手，供 fix 直接引用，不必重跑）：
1 弃值属实：:427 与 :502 裸语句弃 bool，紧接 :435/:511 无条件 write_resp_simple_string("OK")；
  包装件 mod.rs:169-174 确为 release→provider 有界等待→无条件重取快照→透传 bool，
  全仓该包装仅此三调用点，无一处留痕。
2 C# 一手：RespClusterSlotManagementCommands.cs:484 先 TryPrepareSlotForOwnershipChange、
  :490 判成分支、:493 BlockingWait（:492 注释明示网络线程不可避阻塞）、静止后才 :495 写 RESP_OK；
  SETSLOTSRANGE 臂 :581→:593→:595 同型；ReplicaOfCommand.cs:44-47 变更先行、:48 BlockingWait、
  :50-51 StartPrimaryTasks、finally :55-56 EndRecovery、:111-112 回 OK。票面锚未漂移。
3 无逆件坐实（本票不得改成判败的技术根据）：NODE 臂 cluster_manager_slot_state.rs:208-244 把
  Migrating/Importing/其余三型统一落 Stable + 新属主，改后 try_reset_slot_state（:272 状态门）
  对 Stable 是 no-op；批量臂 :247-266 无条件覆掉集合内每槽既有个案态，
  reset_multi_slot_state（cluster_config/mod.rs:722-736）只能一律拉平 Stable，无法还原；
  且 :234/:260 已 bump_local_node_config_epoch、每臂均过 flush_config（cluster_manager.rs:484-492，
  :491 config_version 自增推动 gossip），变更在等待之前已落盘并对外传播，
  回滚动作自身是第二次落盘 + 第二次版本推进，对全集群观感比「静止晚点达成」更坏。
4 钩子加门判拒（必守）：revoke_shard_subscriptions_for_slots（cluster_provider/assets.rs:196-206 起）
  是会话级分片订阅收口，前提是「槽权已离本节点」，该事实在 :423/:498 的 Ok 分支已成立且已落盘，
  与全会话纪元是否静止无因果；本仓内部同构权移点 migrate/migrate_session.rs:168-184
  （relinquish_ownership 做与 SETSLOT NODE 同一个 try_prepare_slots_for_ownership_change + 同一个 revoke 钩）
  全程不调本原语。故严禁把钩子改挂到静止达成臂——那会复现
  task/done/wpubsub-shard-subscription-no-slot-anchor-hang-on-migration.md 已收口的危害。
5 危害降档：本域无数据面消费者、无删键、无引擎置换、无检查点/AOF 步骤、不启迁移驱动
  （slot_mgmt.rs:413-422 → cluster_manager_slot_state.rs 纯配置面 + flush_config）；
  落位在 current_config.write() 写锁内逐命令原子完成（:210/:253），不会撕裂单槽态。
  实际后果收敛为「协议应答只保证变更生效、不保证无旧视图在途批」+ 内部不变量静默化，故 P3。
  若后续查证席坐实存在常年持快照的内置集群会话（使 false 常态化），定级回升 P2，形态不变。
6 反证其余形态：等待前置（先 bump+wait 再改配置）判拒——紧随等待后开批的会话快照即等于本次 bump 纪元
  （core.rs:913），立刻穿过 :98 判定门，恰恰跨过配置写，等于没等，且背离 C# 变更→等待次序；
  改无限档判拒——同步档 wait.rs:97 park 占住 compio worker 线程，无限档即该核全体会话
  （心跳/gossip/复制/别的客户端）被一个滞留批永久钉死，且要 C# 严格形态零改动即可：
  cluster-node-timeout 置 0 = None = 无限档（checkpoint.rs:50-53、flags.rs:115）；
  判空转判拒——三点弃值且零留痕属实，原语文档 :51-53 明写「false 由调用方按各自窗口不变量裁决」，
  无人接即该不变量静默失效。

执行方案（fix 直接消费）：
1 slot_mgmt.rs:network_cluster_set_slot :427 取返值，false 时 log::warn 一条，
  写明槽号 + 臂位 + 「槽权变更已生效，纪元排空未在 cluster-node-timeout 内达成，旧视图在途批仍可能收尾」；
  :430-434 的 NODE 收口钩与 :435 的 OK 一律不动。
2 slot_mgmt.rs:network_cluster_set_slots_range :502 同型，warn 携带区间（Self::get_range 现件 :74-79/:198-203）
  与臂位；:505-510、:511 不动。
3 replica_of.rs:45 同型留痕（warn 记「解附已生效、纪元排空未达成」），
  :52 resume_primary_tasks、:53 end_recovery、:54 OK 次序一律不动（恢复锁内先后是已收口面，勿碰）；
  同函数 :40-43 的 try_reset_replica / try_update_for_failover / reset_replica_replay_driver_store 不改。
4 STABLE 臂口径：:427/:502 为各臂共用点，warn 天然覆盖 STABLE 臂（其本身无可回滚物），
  落地时在注释里写明该点覆盖全部四臂，勿按票面原案只列 NODE/IMPORTING/MIGRATING 三臂。
5 编译期收口（推荐采行，零运行时成本、不新增机制）：cluster_session/mod.rs:169 包装件加 #[must_use]，
  使同族第四点起新弃值必须在编译期显式表态；严禁改用 #[allow] 绕过。
6 测试：夹具复用 tests/failover_epoch_drain_failclose.rs:99-107 park_lagging_session
  （bump 后建会话 + acquire_current_epoch 恒不追平 + set_cluster_node_timeout_ms 极小值）。
  断言三件：恒不追平夹具下 SETSLOT NODE 与 SETSLOTSRANGE NODE 仍回 +OK；warn 恰一条；
  槽态与 config_version 单调推进、revoke_shard_subscriptions_for_slots 照常触发；
  REPLICAOF NO ONE 臂同形（仍 OK、恢复锁正常释放、可再次发起不被 ERR_RECOVERY_LOCK 拒）。
  反向断言（「未达成即不回 OK」）严禁写入——预设的是本票已否的判败语义。
7 回归面：tests/cluster_management.rs、cluster_config.rs、cluster_config_persist.rs、
  cluster_pubsub_peer_shutdown.rs 与 wpubsub 分片订阅收口锁测不回退。
8 禁触线：禁改 checkpoint.rs 原语语义与 cluster_node_timeout 值域（上限闸另族票）；
  禁动 failover.rs:171 与迁移族七点既有承判；禁触 wedb/wnode_tls_test/** 与两个 Cargo.toml（在途 tls 席域）；
  禁 #[allow]/#[expect]、禁为凑 warn 新建日志子系统或第二套排空机制。
9 入册：落码后在 doc/zh/deviations.md 追加一条（顺编册尾、禁钉行号），
  登「有界纪元排空 false 的有逆件/无逆件分界口径」，防 §95 被后续读成「凡 false 必判败」。
审核结论：通过但收窄，并与 replicaof-noone 票合票（2026-09-28 独立审核席 + 主控裁决，定级 P3 条件形）

族票覆盖面（三弃值点、两文件、同一原语同步档、同一包装件 cluster_session/mod.rs:169）：
1 wedb/wedb/src/server/cluster_session/slot_mgmt.rs:network_cluster_set_slot :427
2 wedb/wedb/src/server/cluster_session/slot_mgmt.rs:network_cluster_set_slots_range :502
3 wedb/wedb/src/server/cluster_session/replica_of.rs:network_replicaof NO ONE 臂 :45
  （原 task/issue/wedb-cluster-replicaof-noone-epoch-drain-return-ignored.md 判拒，
  其「回判败帧」主张不采纳，残余「静默弃值无留痕」并入本票第三点，见 reject 票顶注记）

全族统一判据（本票裁决的核心口径，落码注释与后续审查均按此条）：
有界纪元排空返 false 的处理，按「本臂有无精确逆件」分界，不按「有无返值」一刀切——
  有精确逆件 / 有干净失败通道 → 承判判败 + 回滚（先例 failover.rs:171 的 try_restore_stop_writes、
    迁移族七点、以及本批 replicate-sync 票 assembly.rs:338 经 finish_replica_sync 收尾族）；
  无精确逆件的管理臂 → 承判留痕（warn 记槽号/区间/臂位 + 未静止事实），应答按「变更已生效」照实回，
    绝不假报失败、绝不回滚、绝不给收口钩加门。
理由：本原语 false 描述的是本地活性（别的核上有批没收尾），不是本命令的成败；
  C# 原语恒真（ClusterProvider.cs:366-389 末尾 return true），且 C# 包装件本身就是弃值
  （ClusterSession.cs:191-196 签名为 async Task，:194 `_ = await ...`），
  「OK 蕴含全静」是 C# 靠无限自旋兜住的结构事实，不是对客户端的承诺；
  Redis 原生 SETSLOT / REPLICAOF NO ONE 亦为变更落位即 +OK，无静止前置承诺。

审查确证（席一手，供 fix 直接引用，不必重跑）：
1 弃值属实：:427 与 :502 裸语句弃 bool，紧接 :435/:511 无条件 write_resp_simple_string("OK")；
  包装件 mod.rs:169-174 确为 release→provider 有界等待→无条件重取快照→透传 bool，
  全仓该包装仅此三调用点，无一处留痕。
2 C# 一手：RespClusterSlotManagementCommands.cs:484 先 TryPrepareSlotForOwnershipChange、
  :490 判成分支、:493 BlockingWait（:492 注释明示网络线程不可避阻塞）、静止后才 :495 写 RESP_OK；
  SETSLOTSRANGE 臂 :581→:593→:595 同型；ReplicaOfCommand.cs:44-47 变更先行、:48 BlockingWait、
  :50-51 StartPrimaryTasks、finally :55-56 EndRecovery、:111-112 回 OK。票面锚未漂移。
3 无逆件坐实（本票不得改成判败的技术根据）：NODE 臂 cluster_manager_slot_state.rs:208-244 把
  Migrating/Importing/其余三型统一落 Stable + 新属主，改后 try_reset_slot_state（:272 状态门）
  对 Stable 是 no-op；批量臂 :247-266 无条件覆掉集合内每槽既有个案态，
  reset_multi_slot_state（cluster_config/mod.rs:722-736）只能一律拉平 Stable，无法还原；
  且 :234/:260 已 bump_local_node_config_epoch、每臂均过 flush_config（cluster_manager.rs:484-492，
  :491 config_version 自增推动 gossip），变更在等待之前已落盘并对外传播，
  回滚动作自身是第二次落盘 + 第二次版本推进，对全集群观感比「静止晚点达成」更坏。
4 钩子加门判拒（必守）：revoke_shard_subscriptions_for_slots（cluster_provider/assets.rs:196-206 起）
  是会话级分片订阅收口，前提是「槽权已离本节点」，该事实在 :423/:498 的 Ok 分支已成立且已落盘，
  与全会话纪元是否静止无因果；本仓内部同构权移点 migrate/migrate_session.rs:168-184
  （relinquish_ownership 做与 SETSLOT NODE 同一个 try_prepare_slots_for_ownership_change + 同一个 revoke 钩）
  全程不调本原语。故严禁把钩子改挂到静止达成臂——那会复现
  task/done/wpubsub-shard-subscription-no-slot-anchor-hang-on-migration.md 已收口的危害。
5 危害降档：本域无数据面消费者、无删键、无引擎置换、无检查点/AOF 步骤、不启迁移驱动
  （slot_mgmt.rs:413-422 → cluster_manager_slot_state.rs 纯配置面 + flush_config）；
  落位在 current_config.write() 写锁内逐命令原子完成（:210/:253），不会撕裂单槽态。
  实际后果收敛为「协议应答只保证变更生效、不保证无旧视图在途批」+ 内部不变量静默化，故 P3。
  若后续查证席坐实存在常年持快照的内置集群会话（使 false 常态化），定级回升 P2，形态不变。
6 反证其余形态：等待前置（先 bump+wait 再改配置）判拒——紧随等待后开批的会话快照即等于本次 bump 纪元
  （core.rs:913），立刻穿过 :98 判定门，恰恰跨过配置写，等于没等，且背离 C# 变更→等待次序；
  改无限档判拒——同步档 wait.rs:97 park 占住 compio worker 线程，无限档即该核全体会话
  （心跳/gossip/复制/别的客户端）被一个滞留批永久钉死，且要 C# 严格形态零改动即可：
  cluster-node-timeout 置 0 = None = 无限档（checkpoint.rs:50-53、flags.rs:115）；
  判空转判拒——三点弃值且零留痕属实，原语文档 :51-53 明写「false 由调用方按各自窗口不变量裁决」，
  无人接即该不变量静默失效。

执行方案（fix 直接消费）：
1 slot_mgmt.rs:network_cluster_set_slot :427 取返值，false 时 log::warn 一条，
  写明槽号 + 臂位 + 「槽权变更已生效，纪元排空未在 cluster-node-timeout 内达成，旧视图在途批仍可能收尾」；
  :430-434 的 NODE 收口钩与 :435 的 OK 一律不动。
2 slot_mgmt.rs:network_cluster_set_slots_range :502 同型，warn 携带区间（Self::get_range 现件 :74-79/:198-203）
  与臂位；:505-510、:511 不动。
3 replica_of.rs:45 同型留痕（warn 记「解附已生效、纪元排空未达成」），
  :52 resume_primary_tasks、:53 end_recovery、:54 OK 次序一律不动（恢复锁内先后是已收口面，勿碰）；
  同函数 :40-43 的 try_reset_replica / try_update_for_failover / reset_replica_replay_driver_store 不改。
4 STABLE 臂口径：:427/:502 为各臂共用点，warn 天然覆盖 STABLE 臂（其本身无可回滚物），
  落地时在注释里写明该点覆盖全部四臂，勿按票面原案只列 NODE/IMPORTING/MIGRATING 三臂。
5 编译期收口（推荐采行，零运行时成本、不新增机制）：cluster_session/mod.rs:169 包装件加 #[must_use]，
  使同族第四点起新弃值必须在编译期显式表态；严禁改用 #[allow] 绕过。
6 测试：夹具复用 tests/failover_epoch_drain_failclose.rs:99-107 park_lagging_session
  （bump 后建会话 + acquire_current_epoch 恒不追平 + set_cluster_node_timeout_ms 极小值）。
  断言三件：恒不追平夹具下 SETSLOT NODE 与 SETSLOTSRANGE NODE 仍回 +OK；warn 恰一条；
  槽态与 config_version 单调推进、revoke_shard_subscriptions_for_slots 照常触发；
  REPLICAOF NO ONE 臂同形（仍 OK、恢复锁正常释放、可再次发起不被 ERR_RECOVERY_LOCK 拒）。
  反向断言（「未达成即不回 OK」）严禁写入——预设的是本票已否的判败语义。
7 回归面：tests/cluster_management.rs、cluster_config.rs、cluster_config_persist.rs、
  cluster_pubsub_peer_shutdown.rs 与 wpubsub 分片订阅收口锁测不回退。
8 禁触线：禁改 checkpoint.rs 原语语义与 cluster_node_timeout 值域（上限闸另族票）；
  禁动 failover.rs:171 与迁移族七点既有承判；禁触 wedb/wnode_tls_test/** 与两个 Cargo.toml（在途 tls 席域）；
  禁 #[allow]/#[expect]、禁为凑 warn 新建日志子系统或第二套排空机制。
9 入册：落码后在 doc/zh/deviations.md 追加一条（顺编册尾、禁钉行号），
  登「有界纪元排空 false 的有逆件/无逆件分界口径」，防 §95 被后续读成「凡 false 必判败」。
CLUSTER SETSLOT 与 SETSLOTSRANGE 成功臂丢弃纪元静止等待返值，超时未静止仍照回 OK，槽权让渡可在旧视图在途批未收口时被宣告完成

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 侧 ClusterProvider.cs:366-389 BumpAndWaitForEpochTransitionAsync 为无限自旋（while(true) + goto retry，直至全部活跃集群会话批内纪元快照追平或清零），结构上恒真。RespClusterSlotManagementCommands.cs:491-496（SETSLOT）与 :591-596（SETSLOTSRANGE）在槽态变更成功后于网络线程 BlockingWait 该原语（注释明示 "Cannot avoid blocking here we're on the network thread"），静止达成才回 RESP_OK：即 OK 应答对编排器承诺「本节点无一在途命令批始于本次属主/槽态变更之前」，管理操作与全体数据面批被静止等待串行化，后续迁键/置 STABLE/交权协议可安全推进。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 原语有界化：cluster_provider/checkpoint.rs:74 bump_and_wait_for_epoch_transition 以 cluster_node_timeout() 为上限（默认 60000ms，args.rs:15-17；0 = 无限档与 C# 一致），超时返 false。会话包装 cluster_session/mod.rs:169 unsafe_bump_and_wait_for_epoch_transition 透传返值但调用侧全数弃判：slot_mgmt.rs:427（network_cluster_set_slot）与 slot_mgmt.rs:502（network_cluster_set_slots_range）裸调 `self.unsafe_bump_and_wait_for_epoch_transition();` 后即无条件 write_resp_simple_string("OK")（:435/:511），NODE 臂还照常触发 revoke_shard_subscriptions_for_slots 收口钩子（:430-434/:505-510），全程零留痕。
3. 逻辑危害确证（并发/数据丢失/资源泄露等实际危害）
静止未达成即回 OK：滞留会话的变更前在途批继续按旧槽视图执行——对刚让出槽权的节点写入应被 MOVED 拒收的键（写落在非属主节点成孤儿分叉），对刚获得槽权的节点以旧映射做路由判定；编排器依 OK 推进下一跳协议（新主置稳定、源端清键），跨界写入随清键永久丢失、无自愈路径。并发管理批（另一连接的 SETSLOT/MIGRATING 编排步）亦不再被静止等待串行化，可与本次变更交错落库成混合槽态。与本原语已收口的 failover 族（tests/failover_epoch_drain_failclose.rs，deviations §95）、r25 迁移族同原语同害同判；failover.rs:171 承判先例已给出判败帧措辞。
定级：条件形——false 仅在有集群会话批滞留超超时窗时可达（默认 60s 窗：超长 EVAL 批/重负载让步饥饿可达；CONFIG SET cluster-timeout 调小即轻易达），缺省配置下非即达，故按 P2 登录并如实限定窗口。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/cluster_session/slot_mgmt.rs:network_cluster_set_slot（:427 弃返值、:435 无条件 OK）
wedb/wedb/src/server/cluster_session/slot_mgmt.rs:network_cluster_set_slots_range（:502 弃返值、:511 无条件 OK）
wedb/wedb/src/server/cluster_session/mod.rs:unsafe_bump_and_wait_for_epoch_transition（:169 透传无留痕）
wedb/wedb/src/server/cluster_provider/checkpoint.rs:bump_and_wait_for_epoch_transition（:74 有界返 false 原语）

对应 c# 文件与函数：
garnet/libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterSetSlot（:491-496 BlockingWait 恒静止后才 OK）
garnet/libs/cluster/Session/RespClusterSlotManagementCommands.cs:NetworkClusterSetSlotsRange（:591-596 同型）
garnet/libs/cluster/Server/ClusterProvider.cs:BumpAndWaitForEpochTransitionAsync（:366-389 无限自旋恒真）
garnet/libs/cluster/Session/ClusterSession.cs:UnsafeBumpAndWaitForEpochTransitionAsync（:191-196）

精炼执行方案：
1. slot_mgmt.rs:427 与 :502 承判返值：未达成即不回 OK——NODE/IMPORTING/MIGRATING 臂先按臂位回滚槽态（STABLE 复位已有 try_reset_slot_state/try_reset_slots_state 单点可复用），再回判败帧 `ERR epoch drain not settled within cluster-node-timeout`（措辞对齐 failover.rs:171 先例），并 warn 留痕；revoke_shard_subscriptions_for_slots 钩子仅在静止达成臂触发。同步形态在 compio 线程阻塞有界等待，无需改慢路径（异步化留裁）。
2. 测试验证点：复用 tests/failover_epoch_drain_failclose.rs 恒不追平夹具（注册会话先行批首 acquire_current_epoch + set_cluster_node_timeout_ms 极小值），断言 SETSLOT NODE 与 SETSLOTSRANGE NODE 回判败帧、槽态回滚原状、分片订阅收口钩零触发、OK 路径不回；既有 cluster_slot 面用例不回退。

## 终态注记（2026-09-28 合入）
- 合入 commit: `cb8bb69`
- 修复成果：
  1. `unsafe_bump_and_wait_for_epoch_transition` 增加 `#[must_use]` 属性；
  2. `slot_mgmt.rs`（`network_cluster_set_slot` 与 `network_cluster_set_slots_range`）与 `replica_of.rs`（`NO ONE` 臂）三处无精确逆件的管理臂承判有界纪元排空，超时返回 `false` 时通过 `log::warn!` 携带槽号/区间留痕，应答照实返回 `+OK`，消除静默假不变量；
  3. 顺编补充 `doc/zh/deviations.md` [§182]「集群管理臂有界纪元排空无精确逆件承判 warn 留痕」；
  4. 新增 `tests/cluster_mgmt_epoch_drain_warn.rs` 锁测覆盖三处管理臂超时告警路径。

