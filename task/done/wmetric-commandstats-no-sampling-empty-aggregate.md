甄别结论：通过（甄别席 zc-fix-r25-乙，2026-09-26）定级 P2
核验记录：C# 亲验——GarnetInfoMetrics.cs:233 分支键确为 MetricsSamplingFrequency > 0、:244 起频率 0 臂 history+ActiveConsumers 逐会话补并（现树亲见）；GarnetServerOptions.cs 校验仅拒 Latency+freq0，commandstats 单开合法。rust 亲验——info_provider.rs:182 仍以 tracks_command_stats()（构造期开关投影）判「无需补并」；garnet_server_monitor.rs:213-217 command_stats_aggregate 无条件优先 global_command_stats，global 表仅采样循环 add_current_server_stats（:292/:305/:435）填充；server.rs:925 采样循环 frequency_secs > 0 才拉起——条件键错位链路现码原样，无合入灭失；审核补充之 sampling_frequency_secs()（:206-208 亲见）已在位、方案零新增接口属实。查重：deviations 命中行系 commandstats 零计数过滤谓词（他轴），聚合路由面零登记；四池零同轴。架构：判别单点改频率真源、臂位注释订正，单机制无过度设计；测试夹具两形态闭环。格式：纯文本、双侧齐全。定级 P2：观测面整体静默失效（INFO COMMANDSTATS 恒空段），无数据面危害。

审核结论：通过（审核席 zcode-r22-review-wmetric，2026-09-26）

审核亲验锚点：
C# 侧 GarnetInfoMetrics.cs PopulateCommandStatsInfo 实码分支键确为 serverOptions.MetricsSamplingFrequency > 0（else 臂 history + 遍历 Servers.ActiveConsumers 逐会话 GetCommandStats 补并）；GarnetServerOptions.cs:839-840 仅拒 LatencyMonitor && freq==0；defaults.conf 两开关缺省 CommandStatsMonitor=false / MetricsSamplingFrequency=0，commandstats 单开合法形态成立。
rust 侧 info_provider.rs:182 确以 tracks_command_stats()（构造期 commandstats 开关投影，garnet_server_monitor.rs:193-200）判「无需补并」；command_stats_aggregate（:212-219）无条件优先 global_command_stats，而该表仅由采样循环 add_current_server_stats（:305-310，调用点 :435）填充、server.rs:925 频率门 freq==0 不拉起；garnet_server_metrics.rs:66 构造期 global_command_stats 即 Some(零表)，恒零过滤（garnet_info_metrics.rs:505 空表置 None）后 COMMANDSTATS 段整段缺席。链路完整，缺陷属实。查重：deviations.md 定向 grep 零登记，历史档零撞面。
方案补充：sampling_frequency_secs()（garnet_server_monitor.rs:204-206）已存在，方案 2 零新增接口；commandstats 关 + freq>0 的组合在 populate 层 garnet_info_metrics.rs:496 已拦截，新条件键下该臂不可达，无行为回归面。

INFO COMMANDSTATS 在 commandstats-monitor 开启而采样频率为 0 的合法形态下恒空（聚合路由条件键错位：tracks_command_stats 冒充采样频率，global 恒零不补并活跃会话）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# PopulateCommandStatsInfo（garnet/libs/server/Metrics/Info/GarnetInfoMetrics.cs:222-258）按 MetricsSamplingFrequency 分两路：:233-238 频率 > 0 时直接取 globalCommandStats（采样循环已含 history+活跃会话上轮值）；:244-256 频率 == 0 时按需聚合——historyCommandStats（dispose 归并累计）+ 遍历全部服务器 ActiveConsumers 逐会话补并 GetCommandStats。该配置合法：启动校验仅拒 LatencyMonitor && freq == 0（garnet/libs/server/Servers/GarnetServerOptions.cs:839-840），CommandStatsMonitor 单开（freq 缺省 0，defaults.conf:265/274）是 C# 明确支持形态，INFO COMMANDSTATS 应出实数行。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
wnode 聚合路由条件键错位：wedb/wnode/src/resp/info_provider.rs:180-187 的 match 以 Some(m) if m.tracks_command_stats() => false 判「周期采样开启、无需补并」，但 tracks_command_stats（wedb/wmetric/src/garnet_server_monitor.rs:193-200）只反映构造期 commandstats 开关，与采样频率无关——commandstats 开 + freq 0 形态误入「不补并」臂；且 wedb/wmetric/src/garnet_server_monitor.rs:212-219 command_stats_aggregate 无条件优先返回 global_command_stats，而 global_command_stats 仅由采样循环 add_current_server_stats（:305-310）填充，freq 0 时采样循环不启动（server.rs:925 频率门）、global 恒零——聚合结果恒为零表，逐命令过滤（info_provider.rs:219 两栏判零）后全部丢弃，INFO COMMANDSTATS 应答体整段缺席。注释自述（info_provider.rs:171-176「仅命令统计开启（无周期采样）时取 history 并遍历活跃消费者补并」）与 C# 分支同形，但实码条件使该臂仅 tracks_command_stats()==false（开关关）时可达，意图与实现脱节。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
--commandstats-monitor 单开（freq 缺省 0）节点执行任意命令后 INFO COMMANDSTATS 恒空段，对 C# 同配置实数行为逐字节发散；命令统计观测面整体静默失效（既非禁用提示行也非零值行，整段不出），运维无法区分「开关未生效」与「链路断裂」，注释与实现相反将持续误导后续对账轮。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/info_provider.rs:SessionInfoSource::command_stats（:177-208，:182 条件键错位、:171-176 注释与实码脱节）
wedb/wmetric/src/garnet_server_monitor.rs:GarnetServerMonitor::command_stats_aggregate（:212-219，global 优先无条件、未按频率选 history）
wedb/wnode/src/server.rs:install_server_monitor（:884-889 构造传参）与 start_server_monitor（:925 采样循环频率门）

对应 c# 文件与函数：
garnet/libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateCommandStatsInfo（:222-258，:233 频率分支与 :244-256 按需聚合臂）
garnet/libs/server/Servers/GarnetServerOptions.cs:GarnetServerOptions 校验（:839-840 仅拒 latency 形态）
garnet/libs/host/defaults.conf:265/:274（CommandStatsMonitor false、MetricsSamplingFrequency 0 缺省）

精炼执行方案：
1. 判别单点改为采样频率：command_stats_aggregate（wmetric 侧）按 self.monitor_sampling_frequency.as_secs() > 0 选 global，否则选 history_command_stats（对齐 C# :233-243 两路真源选择）
2. info_provider.rs:182 条件改为 Some(m) if m.sampling_frequency_secs() > 0 => false（tracks_command_stats 不再作判别键），Some(_) => true 自然承接 commandstats 单开形态的 history + 活跃会话补并；同步订正 :171-176 注释的臂位描述
3. 测试验证点：commandstats 开 + freq 0 夹具下执行若干命令后 INFO COMMANDSTATS 断言含 cmdstat_ 实数行（会话在架即非零）；freq > 0 形态既有行为回归不破（global 单源不补并）

合入哈希：6041106 收口形态：聚合路由判别键订正为采样频率真源——command_stats_aggregate 按频率选 global/history、info_provider 补并臂以 sampling_frequency_secs()>0 判别，commandstats 单开（freq0）形态 INFO COMMANDSTATS 出 history+活跃会话合并实数行；tracks_command_stats 零调用删除；wmetric 两臂真源锁＋wnode freq0 端到端行在场锁（回退 src 即红）＋freq>0 global 单源不补并回归锁
