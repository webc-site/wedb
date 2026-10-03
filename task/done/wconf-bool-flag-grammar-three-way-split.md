审核结论：通过（立案方向改判：非「对账分叉」而是「全仓 bool 旗标文法单一真源收口」，板块 1。三态抽验全实：a 族 bare-only（aof:555/disable_pubsub:561/recover:566/clean_cluster_config:65，bool+default_value_t=false→clap 自动 SetTrue 两式全拒）；b 族强制带值（enable_scatter_gather_get:691/on_demand_checkpoint:735/protected_mode:794/latency_monitor:881，Set 无 default_missing_value 裸用拒启）；c 族可选带值（tls 双旋钮 :487/:504，num_args=0..=1 三式全通）。C# ConsolidateFlagArguments:366-426 单机制属实但票面「C# 全 bool? 恒双通」须订正——实测 ProtectedMode 为 CommandLineBooleanOption(:603)、EnableVectorSetPreview(:704)/ClusterReplicaResumeWithData(:700)/EnableRangeIndexPreview(:714) 裸 bool，C# 侧本身亦三态，rust 分裂非纯对账分叉。deviations 全册无 CLI 布尔文法条款；危害仅启动期可用性皆 fail-fast 无静默假生效。collapse 到现存 c 族形系板块 1 单一真源，非新引胶水，可落；--aof=false 翻转属可用性改良非契约纠偏）

整理执行方案（审核席订正版，供 fix 消费）：
1 全仓 bool 旋钮文法收敛至 c 族形（num_args=0..=1 + default_missing_value="true"，tls 先例体例），a/b 两族 19 旋钮逐一无新机制平替
2 票面「C# 契约」段按上述三态订正改写，立案理由改「单一真源收口」
3 锁测：bare/--x=true/--x=false 三式对 19 旋钮全通；启动期其余文法不回退

wconf 布尔旗标文法三态分裂：同一 NodeArgs 内 bare-only、强制带值、可选带值三套形态并存，C# 侧为单一统一形态

问题分析：
1. Garnet 契约对齐（C# 原型行为）：C# 全部布尔旋钮统一声明为 bool?（Options.cs:209 EnableAOF、:343 LatencyMonitor、:430-431 EnableScatterGatherGet、:453 OnDemandCheckpoint、:602 ProtectedMode、:159 CleanClusterConfig 等），由 ServerSettingsManager.cs:366-426 ConsolidateFlagArguments 单机制统一两式：旗标式（--recover 裸用自动注入 true）与赋值式（--recover=false 及空格 --recover false 皆可）对每一个布尔旋钮一律合法；取值文法单点 commandline 库 TypeConverter.cs:106-107 IsBooleanString 只认 true/false（大小写不敏感），1/0 两侧同拒。即 C# 契约：任意布尔旋钮，bare 式与 =true/=false 式恒双通。
2. 工程现状确证（Rust 现有实现）：rust 侧布尔字段按声明微差被 clap_derive 分入三套互不相通的文法（以 clap 4.6.7 实测逐形坐实，属性原文照抄自本仓字段）：
   a) bare-only 族（SetTrue 形，只认 --flag）：aof、recover、disable_pubsub、aof_commit_wait、repl_diskless_sync、fast_aof_truncate、quiet、disable_console_logger、enable_lua、enable_vector_set_preview（node_options.rs:555/561/566/578/587/726/755/765 段）、clean_cluster_config（wedb/src/args.rs:64）、hlog.read_cache/reviv/copy_reads_to_tail（node_options.rs:168/191/206）——--aof=false 报 TooManyValues、--aof false 报 unexpected argument，两式全拒。
   b) 强制带值族（action=Set）：enable_scatter_gather_get（--sg-get，node_options.rs:691-697）、on_demand_checkpoint（:735-741）、protected_mode（:794-800）、latency_monitor（:881-887）、commandstats_monitor（:890-896）——裸用 --sg-get 报「a value is required」拒启，而 C# 裸用即置位。
   c) 可选带值族（num_args=0..=1 + default_missing_value）：tls_client_cert_required、tls_server_cert_required（node_options.rs:486-512）——bare、=true/=false、空格三式全通（1/0 拒）。
   override_explicit 的 value_source 判定对三族均正确，无静默丢弃；本票只裁文法面。
3. 逻辑危害确证：迁移对账面双向踩雷且报错形态逐旋钮不一：按 C# 心智写 --aof=false 或 --protected-mode（C# 裸用合法）者启动即死，同一条命令线上不同布尔旋钮各以不同 ErrorKind（TooManyValues / InvalidValue / UnknownArgument）拒回，无一旋钮可归纳出统一文法；--help 输出亦无法表达每旋钮所属族（三族帮助文案同字面）。属同族概念未收口单一真源（审查维度板块 1「概念抽象单一真源/接口最小暴露」），非既定改良：仓内规范与 doc/zh/deviations.md 全无布尔旗标三态裁决在册（grep 旗标/num_args/SetTrue/GetoptMode 零命中），五池亦无同面票。危害为启动期可用性与契约对账分叉，无静默假生效面（幸皆 fail-fast），故立票定调统一文法，而非报运行时缺陷。

涉及代码：
rust 文件与函数：
wedb/wconf/src/node_options.rs：NodeArgs 布尔字段（aof :555、disable_pubsub :561、recover :566、aof_commit_wait :578、repl_diskless_sync :587、tls_client_cert_required :486-494、tls_server_cert_required :504-512、enable_scatter_gather_get :691-697、fast_aof_truncate :726、on_demand_checkpoint :735-741、quiet :755、disable_console_logger :763-768、protected_mode :793-800、latency_monitor :880-887、commandstats_monitor :890-897、enable_lua :900-903、enable_vector_set_preview :952-955）、HlogOptions（read_cache :168、reviv :191、copy_reads_to_tail :206）
wedb/wedb/src/args.rs：ClusterArgs::clean_cluster_config :64

对应 c# 文件与函数：
garnet/libs/host/Configuration/Options.cs:EnableAOF(:209)/LatencyMonitor(:343)/EnableScatterGatherGet(:430)/OnDemandCheckpoint(:453)/ProtectedMode(:602)/CleanClusterConfig(:159)（全 bool? 单型）
garnet/libs/host/ServerSettingsManager.cs:ConsolidateFlagArguments(:366-426)（bare 式统一注 true 单机制）
commandline 库 2.9.1 Core/TypeConverter.cs:ChangeTypeScalarImpl(:106-107)+Infrastructure/StringExtensions.cs:IsBooleanString(:66-70)（true/false 文法单点）

精炼执行方案：
1. 全部布尔 CLI 字段收口为 c) 族单形：#[arg(long, num_args = 0..=1, default_missing_value = "true", action = clap::ArgAction::Set)]（即现 tls 双旋钮形），a) b) 两族逐字段并入；取值文法维持 clap bool 默认（true/false），与 C# IsBooleanString 同界，不放宽 1/0。
2. 统一形下逐字段回归锁测（tests/ 新增或扩 wconf/tests/node_options.rs）：每布尔旋钮三用例——bare 置 true、=false 关、空格 false 关；另 --aof=false 与 --sg-get 裸用两迁移形翻正断言；三态旧文法断言全数消亡（rg 核 SetTrue 裸形与 action=Set 无值形不再可达）。
3. 配置文件与 value_source 覆盖链不动（现链已对三族一致正确），CONFIG GET/REWRITE 面不经此文法，零波及；--help 文案随之单形展示。
