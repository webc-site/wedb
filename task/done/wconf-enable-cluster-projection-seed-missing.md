终态：合入 d51f956（修复提交 bcb6db2），run_cluster_server 装配段以 provider 在位为真值源单点播种 enable_cluster=true（投影后置位、set_runtime_config 前经 with_runtime_server_options 重装 runtime_config 落位），新增 tests/cluster_identity_seed.rs 集群 e2e 四面断言 + 单机缺省链四面回归，cargo check --all-targets 全绿。

审核结论：通过（2026-10-01 方案审核席）

判定要点：
1. 真实性亲验吻合。投影段 node_options.rs:1600-1680（runtime_server_options 全函数）逐字段播种确无 enable_cluster 赋值；全仓 grep 复核非测试码触达仅四处——runtime_server_options.rs:118 定义、:221 Default=false、runtime_server_config.rs:63-65 fmt_cluster_enabled（:346 注册槽 27，CONFIG GET cluster-enabled 读链确经此回读）、消费面 info_provider.rs:88-98/:113/:119/:121/:408/:437 与 garnet_info_metrics.rs:197/:357-366/:451-452/:489，零置位点。生产构造点唯二：service.rs:1252 RuntimeServerOptions::default()、:1362 with_runtime_server_options（投影调用点 :1663/:1684/:1701）。boot.rs:215 open_from_args、:353 set_runtime_config 透传不补种（:365-367 仅有 cluster-node-timeout try_set 播种先例，反证该段尚无 enable_cluster 播种）。测试 cluster_resp_session.rs:3465-3470 set_runtime_config 显式 enable_cluster:true 绕过生产链，坐实缺位。C# 四锚全中：Options.cs:155-156 [Option("cluster")] EnableCluster bool?、:915 EnableCluster.GetValueOrDefault()、RuntimeServerConfig.cs:135-136 SetReadOnly(CLUSTER_ENABLED)、GarnetInfoMetrics.cs:69 run_id/:71 redis_mode/:154-155 cluster_enabled、StoreWrapper.cs:176 RunId => EnableCluster ? clusterProvider.GetRunId() : runId。
2. 非重复成立。task 池 grep enable_cluster|cluster-enabled|redis_mode|run_id 仅本票命中；§119（doc/zh/deviations.md:297）判据面为节点 id 渲染 32hex 身份形状条，与本面（enable_cluster 槽位播种缺位）不重叠。
3. 危害定级恰当。观测/身份面错报：CONFIG GET cluster-enabled 恒 no（info_provider.rs:113 即读该槽）、run_id 回落进程级随机串（:119 经 resolve_run_id :91 门）、redis_mode 恒 standalone（garnet_info_metrics.rs:359-366）、cluster_enabled 恒 0（:452）、needs_gossip 恒 false（info_provider.rs:437）INFO STATS 丢 gossip 段。数据面不受断链影响亲验：attach.rs:214 txn.cluster_enabled = self.cluster_session.is_some()，取自会话切面非配置槽。
4. 方案甄别（执行方案订正）：弃「NodeArgs 增 --cluster 旋钮直投影」，取「boot 装配链以 cluster provider 在位为真值源播种」。理由：C# 单一二进制以 --cluster 旋钮选模，旋钮即模式真源，直投影同形单源；rust 模式由装配入口决定——生产二进制 main.rs:37 恒走 run_cluster_server（boot.rs:175 with_cluster_provider 恒在位），嵌入式宿主走 NodeService 无 provider，全仓不存在 --cluster 布尔旋钮。若于 NodeArgs 造旋钮即立第二真源：嵌入式宿主携 true 无 provider 则 redis_mode 谎报 cluster（消费门 info_provider.rs:91 enable_cluster && let Some(provider) 亦以 provider 共判），run_cluster_server 携缺省 false 则本票缺陷仍在。provider 在位播种单点落 run_cluster_server 装配段（boot.rs:353-371 区间，端点 accept 之前，同段 cluster-node-timeout try_set 已立先例），单机/嵌入式保持 RuntimeServerOptions::default() 缺省 false（service.rs:1252），全仓仍一处置位点，单机制不破。

整理执行方案：
1. run_cluster_server 装配段（boot.rs:353 set_runtime_config 前后、端点 accept 之前）以 cluster provider 在位为真值源单点播种 enable_cluster=true 入 provider.runtime_config（同段 cluster-node-timeout 秒槽 try_set 播种先例同形），杜绝第二播种点；NodeArgs 不增旋钮，runtime_server_options 投影段不动。
2. 测试验证点：run_cluster_server 形态断言 CONFIG GET cluster-enabled 为 yes、INFO redis_mode 为 cluster、cluster_enabled 为 1、run_id 与 provider.get_run_id() 一致、INFO STATS 含 gossip 段；嵌入式/单机形态（service.rs:1252 缺省链）四面回归仍为 no/standalone/0/进程级 run_id。

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# Options.cs:156（--cluster CLI 旋钮 EnableCluster bool?）经 :915 GetServerOptions 投影 EnableCluster.GetValueOrDefault() 入 GarnetServerOptions；RuntimeServerConfig.cs:135-136 SetReadOnly(CLUSTER_ENABLED, o.EnableCluster ? "yes" : "no")；GarnetInfoMetrics.cs:69/:71/:155 的 run_id（经 StoreWrapper.cs:176 RunId => serverOptions.EnableCluster ? clusterProvider.GetRunId() : runId）、redis_mode、cluster_enabled 1/0 全按该布尔渲染——配置输入直达身份观测面。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   wedb/wconf/src/node_options.rs:NodeArgs::runtime_server_options（:1600-1680）投影段逐字段播种但无 enable_cluster 赋值；全仓非测试码对该字段仅四处触达——runtime_server_options.rs:118 定义、:221 Default=false、runtime_server_config.rs:63-65 fmt_cluster_enabled 格式器、info_provider.rs 消费，零置位点。生产构造点唯二：service.rs:1252 嵌入式缺省与 :1362 投影，均不可能携带 true；boot.rs:215/:353 集群总装仅 set_runtime_config 透传不补种。测试 wedb/wedb/tests/cluster_resp_session.rs:3468 显式 set_runtime_config enable_cluster:true 绕过生产链，坐实缺位。后果链：CONFIG GET cluster-enabled 恒 no（info_provider.rs:113 即读该槽）；:119 run_id 回落进程级随机单机串（不走 provider.get_run_id()）；garnet_info_metrics.rs:357-361 redis_mode 恒 standalone、:451-452 cluster_enabled 恒 0；info_provider.rs:437 needs_gossip 恒 false，INFO STATS 的 11 行 gossip 观测段判据灭。
3. 逻辑危害确证
   生产集群节点在 CONFIG GET 与 INFO 全部身份面自报 standalone：监控与编排按 cluster-enabled、redis_mode、cluster_enabled、run_id 判读集群态全错，INFO STATS 丢 gossip 观测段。无数据面危害——事务面 cluster_enabled 取自 cluster_session.is_some()（attach.rs:214）不受此断链影响。

涉及代码：
rust 文件与函数：
wedb/wconf/src/node_options.rs:NodeArgs::runtime_server_options
wedb/wconf/src/runtime_server_options.rs:RuntimeServerOptions（enable_cluster 定义与 Default）
wedb/wconf/src/runtime_server_config.rs:fmt_cluster_enabled
wedb/wnode/src/resp/info_provider.rs:facts
wedb/wnode/src/resp/info/garnet_info_metrics.rs

对应 c# 文件与函数：
garnet/libs/host/Configuration/Options.cs:GetServerOptions
garnet/libs/server/Config/RuntimeServerConfig.cs:SetReadOnly
garnet/libs/server/Metrics/Info/GarnetInfoMetrics.cs
garnet/libs/server/Servers/StoreWrapper.cs:RunId

精炼执行方案：
1. runtime_server_options 投影段补一行 enable_cluster 播种（NodeArgs 增 --cluster 旋钮或由 boot 装配链以 cluster provider 在位为真值源单一播种，取与 C# --cluster 旋钮直投影同形的最小改动），杜绝第二播种点。
2. 测试验证点：集群形态启动后断言 CONFIG GET cluster-enabled 为 yes、INFO redis_mode 为 cluster、cluster_enabled 为 1、run_id 与 provider.get_run_id() 一致、INFO STATS 含 gossip 段；单机形态四面回归不变。
