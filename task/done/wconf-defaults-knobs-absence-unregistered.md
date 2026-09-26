归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 e172601（P4 登记级），收口形态：deviations.md §158 族目（a 回显删员两员 compaction-force-delete/aof-null-device、b 缺席旋钮登记含 drift 恒默认自陈），四代码锚回指（runtime_server_options.rs/server_config_type.rs/gc/mod.rs/config_defaults.rs），零行为改动 src+test 净 +21−2、台账 +33。续排注：票面锚漂移五处（node_options :1347 等）执行席按现码落锚已在申报核可。

甄别结论：通过（甄别席 zc-fix-r16-wconfdef，2026-09-26）定级 P4
核验记录（双侧现锚逐点亲验，非票面背书）：
1. 回显删员属实：C# ServerConfigType.cs:30/:62 双员在册（判别 16/37 亲数）；RuntimeServerConfig.cs:157-158 AOF_NULL_DEVICE SetReadOnly 恒回 UseAofNullDevice 形（defaults.conf:364 false → "no"）、:185 COMPACTION_FORCE_DELETE Set(Bool,0,1)，Set/SetReadOnly 均 IsRuntime:true（:101/:109），BuildRuntimeTypes（:233-240）纳入 GET *；rust wconf/src/server_config_type.rs:5-12 刻意删员属实（force-delete 详注、null-device 仅半句），单查二名不在 NAME_LOOKUP（runtime_server_config.rs:414-426 仅由 RUNTIME_TYPES 铸造）回空。微瑕不反证：「META 35 槽对 36 槽」系计数口径含糊（实况 rust 表 36 槽 33 实员对 C# 38 槽 35 实员），删二员实质成立。
2. 缺席十项属实：defaults.conf :97/:109/:170-176/:194/:283/:319/:322/:340/:530 逐行亲验；Options.cs :147-148/:230-240/:264-266/:369-371/:418-420/:424-426/:699-700 亲验；StoreWrapper.cs :236 loggingFrequency、:967-969 CompactionFrequencySecs>0 注册门亲验。rust 树 CheckpointThrottle/FastCommit/LoggingFrequency/PubSubPageSize 四词全树零命中；AofReplayBarrierSpinUs 无字段；replay_drift 两字段（runtime_server_options.rs:138-141，Default 播种 :203-204）无 CLI 旋钮（node_options.rs 零 drift 命中）、消费链 garnet_append_only_file.rs:83-84→:339-344 亲验恒默认；gc/mod.rs:22-26、config_defaults.rs:4、slots.rs:61、replication_manager.rs:1358-1359、node_options.rs:1347 自陈锚全在位。
3. 订正两点属实：MainMemoryReplication 确为弃用别名（Options.cs:446、GetFastAofTruncate :1080-1086 日志自认 deprecated），rust 不接别名自陈 node_options.rs:1347、残字 cluster_replication_session.rs:296，「勿写零命中」采纳。
4. 非重复非灭失：deviations.md 全册 2032 行对本族全部关键词零命中；§86 尾注「缺席旋钮归配置旋钮族登记口（§111 收形先例）本册不立目」（:1993 附近）与 §111 族目（:1452 起）在册，本票即补该登记口。与 wlua 票（lua 四旋钮，P2 真断链补线，本票十项不含 lua 族）及 confwire 票（假旋钮真接线＋index auto-grow 门，本票明示勿动 handle_index_size_change）三面域互斥，不并案；ing/todo/done/reject 四池 grep 本族关键词仅命中本票。
5. 合规与可执行度：纯台账＋两处注释锚，零行为改动零新机制，符合 transpile/rust_review 单向单机制纪律与 §111/§145 顺编撞号先例；票面纯文本格式合规。定级 P4 登记级维持。

wconf defaults.conf 旋钮缺席与 CONFIG 回显删员族零台账登记（登记级，不改行为）

审核结论：通过（审核席 zcode-r19-review-knobs，2026-09-26，登记级 P4 维持）

审核亲验记录（双侧源码逐点复核，全部属实）：
1. 回显删员两项属实：C# ServerConfigType.cs:30/:62 双员在册；RuntimeServerConfig.cs AOF_NULL_DEVICE SetReadOnly（formatter 回 UseAofNullDevice ? "yes" : "no"，defaults.conf:364 false → 恒 "no"）与 COMPACTION_FORCE_DELETE Set(Bool, 0, 1) 均 IsRuntime:true，BuildRuntimeTypes 纳入 CONFIG GET * 回显；rust wedb/wconf/src/server_config_type.rs:5-12 刻意删员（前者详注、后者半句），rust META 表 35 槽对 C# 36 槽。单查该二名 rust 回空列表、C# 回 "no"。
2. 旋钮缺席十项属实：defaults.conf 十二处默认值逐行亲验（:97 4k、:109 1、:170-176 -1/1/0、:194 0、:283 5、:319 0、:322 1000、:340 false、:530 false）；Options.cs 注册行亲验（:230-240 漂移三员、:264-266 CompactionFrequencySecs、:369-371 LoggingFrequency、:147-148 PubSubPageSize、:419-421 CheckpointThrottleFlushDelayMs、:424-426 FastCommitThrottleFreq、:699-700 ClusterReplicaResumeWithData）；StoreWrapper.cs:966-969 紧缩注册门（CompactionFrequencySecs>0 才注册，默认 0 即默认不注册）与 :236 loggingFrequency 消费亲验。rust 侧：CheckpointThrottleFlushDelayMs / FastCommitThrottleFreq / LoggingFrequency / PubSubPageSize 四项全树零命中；CompactionFrequencySecs 自陈在 wkv/src/gc/mod.rs:22-26 另有 wkv/tests/config_defaults.rs:4 头注一处；ParallelMigrateTaskCount 自陈 slots.rs:61；ClusterReplicaResumeWithData 自陈 replication_manager.rs:1358；漂移两字段（runtime_server_options.rs:139/:141，Default 播种 :203-204）消费闭环经 garnet_append_only_file.rs:83-84 → :339-344 ReadConsistencyManager::new，全链无 CLI 旋钮无投影赋值恒默认；AofReplayBarrierSpinUs 无字段无对物。
3. 零运行期危害判断成立：十项行为恒同 C# 默认部署形态，唯二可观测差异即已裁决删员的回显投影，非缺陷。
4. 零台账登记属实：deviations.md 全册（2032 行）对 compaction-force-delete / aof-null-device / AofReplayDrift / FastCommit / LoggingFrequency / ParallelMigrate / PubSubPageSize / MainMemoryReplication / CheckpointThrottle / CompactionFrequency 全关键词零命中。
5. 先例合规：§86 尾注「缺席旋钮归配置旋钮族登记口（§111 收形先例），本册不立目」在册（:1993）；§111 五组族目（:1452 起）、wconf-net-tls 补登（:1300）均为 P3/P4 登记级纯台账先例；与 todo/zcode-r167c-confwire.md（index auto-grow 门）、todo/wlua-lua-options-config-disconnect.md（lua 四旋钮）域互斥不重复。

审核订正两处（执行时采纳，不动结论）：
1. 原票「MainMemoryReplication rust 全树零命中且无任何码内自陈」措辞失真：C# 侧该旋钮实为弃用别名（Options.cs:445-446，GetFastAofTruncate :1080-1086 日志自认 is deprecated. Use --fast-aof-truncate instead），rust 不接别名已有码内自陈 wconf/src/node_options.rs:1347，另有移植日志文案残字 wedb/src/server/replication/cluster_replication_session.rs:296。实质（无独立旋钮、行为恒同默认 false）成立，台账登记措辞改用「弃用别名不接、无独立旋钮」，勿写「零命中」。
2. CompactionFrequencySecs 码内自陈除 wkv/src/gc/mod.rs:22-26 外另有 wkv/tests/config_defaults.rs:4 头注一处，补注释锚时两处一并回指新条目号。

优化执行方案（供 task/fix.md 直接消费，零行为改动）：
1. doc/zh/deviations.md 补一条族目（顺册尾实况编号，撞号让位不覆写，循 §111/§145 先例纪律），分两小节：a) CONFIG GET * 回显删员两项——compaction-force-delete（详注已在 server_config_type.rs:9-12，补台账回指）、aof-null-device（补完整删员理由：rust 无 null device 对物、wdev 无 NullDevice、紧缩经设备截断无条件回收，旋钮无可承接行为按「不留可写不可用旋钮」删除；MainMemoryReplication 系弃用别名不接，见订正 1）；b) 旋钮缺席族十项逐项钉 C# 锚（defaults.conf 行号 + Options.cs 行号，按亲验记录第 2 点行号）与 rust 现状锚（自陈处或恒默认字段处），裁决措辞统一：行为面偏差即本条旋钮缺席或回显删员，严禁按 C# 形态回改、勿判转写漏项、勿重复疑报。
2. 码内补注释锚：wconf/src/server_config_type.rs 的 AOF_NULL_DEVICE 半句注扩为完整删员理由并回指新条目号；wconf/src/runtime_server_options.rs 的 replay_drift 两字段注补「恒默认、CLI 面缺席」自陈并回指条目号。零行为改动。
3. 测试验证点：纯登记零代码行为改动，无需新增测试；现有 wconf 单测（test_node_args_defaults 等）与 wkv config_defaults 保持绿。对账验证：后续席 grep 台账词（compaction-force-delete、aof-null-device、replay-drift、LoggingFrequency、FastCommitThrottle、PubSubPageSize、MainMemoryReplication）命中本条即停。

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# defaults.conf 在册且 CLI 可配（Options.cs 注册）的一批旋钮，与 CONFIG GET * 回显成员，构成对账基准面：
a) CONFIG GET * 回显删员两项。C# ServerConfigType 枚举含 COMPACTION_FORCE_DELETE（判别 16，RuntimeServerConfig.cs:185 Set(Bool, 0, 1) 运行时可设）与 AOF_NULL_DEVICE（判别 37，:157-158 SetReadOnly 恒回 UseAofNullDevice 形态），二者 IsRuntime 均真，BuildRuntimeTypes 纳入 CONFIG GET * 回显（各回 "no" 默认值）；CONFIG GET compaction-force-delete / aof-null-device 单查同回 "no"。
b) 旋钮缺席族十项。defaults.conf 在册且 Options.cs 注册 CLI：CompactionFrequencySecs（:265，defaults.conf:194 默认 0）、CheckpointThrottleFlushDelayMs（:418，defaults.conf:319 默认 0）、FastCommitThrottleFreq（:423，defaults.conf:322 默认 1000）、LoggingFrequency（:371，defaults.conf:283 默认 5）、PubSubPageSize（defaults.conf:97 默认 4k）、ParallelMigrateTaskCount（defaults.conf:109 默认 1）、ClusterReplicaResumeWithData（defaults.conf:530 默认 false）、AofReplayDriftThreshold / AofReplayDriftCheckFreq / AofReplayBarrierSpinUs（Options.cs:232-240，defaults.conf:170-176 默认 -1 / 1 / 0）、MainMemoryReplication（defaults.conf:340 默认 false）。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 侧上述各项均无对位旋钮或回显槽，且 doc/zh/deviations.md 全册零登记（compaction-force-delete / aof-null-device / AofReplayDrift / 漂移 / FastCommit / LoggingFrequency / ParallelMigrate / 紧缩频率 / 进度日志频率等关键词全零命中）。逐项现状：
a) 回显删员：wconf/src/server_config_type.rs:9-12 对 COMPACTION_FORCE_DELETE 删除有详注（紧缩经设备截断无条件物理回收，forceDelete 次序无对位需求），AOF_NULL_DEVICE 仅一句带过（C#=37 尾员）；wnode config_commands.rs:433-435 的 handle_index_size_change 注释亦自陈「本仓无 index 自动增长选项」（该失真已由 todo/zcode-r167c-confwire.md 案二另立，不属本票）。wnode/src/aof/readconsistency/read_consistency_manager.rs:62-81 消费 replay_drift_threshold / replay_drift_check_freq（wconf/src/runtime_server_options.rs:138-141 字段在、默认 -1/1 对齐 C#），但 NodeArgs 无 CLI 旋钮、runtime_server_options() 投影无赋值，恒为默认值；AofReplayBarrierSpinUs 无字段无对物。
b) 旋钮缺席：CompactionFrequencySecs 由 wkv/src/gc/mod.rs 模块头自陈裁决（紧缩判定不设独立周期旋钮、默认 None 关闭常规阈值紧缩、对标 C# CompactionTask 默认不注册）；ParallelMigrateTaskCount 由 wedb/wedb/src/server/migration/migrate_driver/slots.rs:61 自陈（并行扫描投影为串行单任务）；ClusterReplicaResumeWithData 由 wedb/wedb/src/server/replication/replication_manager.rs:1357-1358 自陈（配置面未落地，副本重启后等待主同步，C# 未配置时同语义）；CheckpointThrottleFlushDelayMs / FastCommitThrottleFreq / LoggingFrequency / PubSubPageSize / MainMemoryReplication 五项 rust 全树零命中且无任何码内自陈。FastMigrate 缺席已有 deviations §86 尾注随行注记（本册不立目），系本族先例。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
零运行期危害：全部缺席项行为恒同 C# 默认部署形态（C# 默认 CompactionTask 不注册、漂移屏障禁用、日志频率 5 仅影响进度日志节奏、串行迁移即 ParallelMigrateTaskCount=1、副本恒等待重同步即 ResumeWithData=false），不构成行为分叉；唯二可观测差异为 CONFIG GET * 回显少 compaction-force-delete 与 aof-null-device 两成员、该二名单查 rust 回空列表而 C# 回 "no"（属已裁决删员的协议回显投影，非缺陷）。危害纯在治理面：本席全维度扫描即因此反复撞面取证（每项均需双侧亲验才能判净），后续 defaults.conf 对账席与 CONFIG 面对拍席无台账可引，必然重复疑报或将已裁决删员误判为转写漏项，或将恒默认字段（replay_drift 两员）误报为假旋钮。§111 五组（尺寸/reviv/缓冲池/pagecount/max-inline）与本票域互斥且已设回锚，本票即补齐 §86 尾注所称「配置旋钮族登记口」的剩余覆盖。

涉及代码：
rust 文件与函数：
wedb/wconf/src/server_config_type.rs:ServerConfigType（删员注释段）
wedb/wconf/src/runtime_server_options.rs:RuntimeServerOptions（replay_drift_threshold / replay_drift_check_freq 恒默认字段）
wedb/wnode/src/aof/readconsistency/read_consistency_manager.rs:ReadConsistencyManager（漂移阈值消费）
wedb/wkv/src/gc/mod.rs:GcManager（紧缩周期不设旋钮自陈）
wedb/wedb/src/server/migration/migrate_driver/slots.rs:SlotsMigrateDriver（串行投影自陈）
wedb/wedb/src/server/replication/replication_manager.rs:ReplicationManager::recover_async（ResumeWithData 自陈）
wedb/wnode/src/resp/config_commands.rs:ServerConfig::handle_index_size_change（回显删员关联注释，勿动其 auto-grow 门——该面归 confwire 票）

对应 c# 文件与函数：
garnet/libs/server/Config/ServerConfigType.cs:ServerConfigType（COMPACTION_FORCE_DELETE / AOF_NULL_DEVICE 枚举成员）
garnet/libs/server/Config/RuntimeServerConfig.cs:BuildMeta（:157-158 AOF_NULL_DEVICE 只读槽、:185 COMPACTION_FORCE_DELETE 可设槽）
garnet/libs/host/Configuration/Options.cs:Options（:232-240 漂移三旋钮、:265 CompactionFrequencySecs、:371 LoggingFrequency、:418 CheckpointThrottleFlushDelayMs、:423 FastCommitThrottleFreq）
garnet/libs/host/defaults.conf（:97/:109/:170-176/:194/:283/:319/:322/:340/:364/:530 各默认值）
garnet/libs/server/StoreWrapper.cs:CompactionTaskAsync / loggingFrequency（紧缩任务注册门 :967-969 与日志频率消费 :236）

精炼执行方案：
1. deviations.md 补一条族目（建议顺册尾编号），分两小节登记：a) CONFIG GET * 回显删员两项（compaction-force-delete 详注已在码内、aof-null-device 补删员理由：rust 无 null device 对物、wdev 无 NullDevice、MainMemoryReplication 单模型不落地）；b) 旋钮缺席族十项逐项钉 C# 锚（defaults.conf 行号 + Options.cs 行号）与 rust 现状锚（自陈处或恒默认字段处），裁决措辞统一：行为面偏差即本条旋钮缺席或回显删员，严禁按 C# 形态回改、勿判转写漏项、勿重复疑报。
2. 码内补两处注释锚：wconf/src/server_config_type.rs 的 AOF_NULL_DEVICE 半句注扩为完整删员理由并回指新条目号；wconf/src/runtime_server_options.rs 的 replay_drift 两字段注补「恒默认、CLI 面缺席」自陈并回指条目号。零行为改动。
3. 测试验证点：纯登记零代码行为改动，无需新增测试；现有 wconf 单测（test_node_args_defaults 等）保持绿即可。对账验证：后续席 grep 台账词（compaction-force-delete、aof-null-device、replay-drift、LoggingFrequency、FastCommitThrottle）命中本条即停。

视角结论:有增量（登记级 P4）
