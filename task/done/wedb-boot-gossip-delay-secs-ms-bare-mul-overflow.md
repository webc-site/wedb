终态：已合入 dev（2026-09-27）。5c0943a boot.rs:303(漂移锚) saturating_mul(1000) 单行+兄弟位点同源;三点真启动冒烟+落槽消费链上界锁测

甄别结论：通过 | 定级 P3 | 2026-09-27 zcode fixloop 甄别席（双侧锚现码亲验，零撞票）
执行提示：saturating_mul(1000)；饱和先例实路径 wbase/src/convert.rs:130/139

审核结论：通过（rust boot.rs:261 裸乘、u64 无 range 校验、dev/test overflow-checks 默认开均实读坐实；C# 全链 TimeSpan 秒源无毫秒槽亲验属实；订正：GarnetServerNode.cs 补 Gossip/ 路径段；其余裸乘散点属他域一票一域不并）

boot 播种 gossip 周期秒值裸乘 1000 转毫秒未走饱和单点，超大 CLI/配置秒值 debug 溢出 panic、release 环绕成畸形周期

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# gossip 周期全链单一秒源，绝无秒乘 1000 的毫秒折算：CLI/选项面 GossipDelay 为 int 秒（garnet/libs/host/Configuration/Options.cs:295-296，默认 5，defaults.conf:223），GarnetServerOptions.GossipDelay 同为 int（garnet/libs/server/Servers/GarnetServerOptions.cs:246），消费前一次性 TimeSpan.FromSeconds(GossipDelay)（garnet/libs/cluster/Server/ClusterManager.cs:112，基于 double，int 值域内恒不溢出），之后 gossipDelay 一路以 TimeSpan 形直用（garnet/libs/cluster/Server/Gossip/Gossip.cs:354 Task.Delay(gossipDelay)、Gossip/GarnetServerNode.cs:113/191 WaitAsync(gossipDelay)）。C# 无毫秒中间槽，故不存在溢出面。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 改采毫秒原子槽承载 gossip 周期，播种位点做了秒→毫秒折算，却用裸乘且不走本仓法定饱和单点：
- 入参 gossip_delay_secs 为无界 u64，CLI 与 TOML 配置面均零校验：wedb/wedb/src/args.rs:74-75（#[arg(long, default_value_t = DEFAULT_GOSSIP_DELAY_SECS)] u64，无 range 约束）、args.rs:34（ClusterFileOptions.gossip_delay_secs: Option<u64>，配置文件同形无界）。
- 播种臂裸乘：wedb/wedb/src/server/boot.rs:261 cluster.set_gossip_delay_ms(args.gossip_delay_secs * 1000)。u64 * 1000 无 saturating，溢出界在 1.8446744e16 秒（2^64/1000）。
- 落槽与消费：flags.rs:101-103 set_gossip_delay_ms 直存无钳；gossip/gossip_manager.rs:56-57 Duration::from_millis(cluster_provider.gossip_delay_ms())，:110 sleep(this.gossip_delay()) 每轮驱动、:414 建连超时同取。
- 同族兄弟位点已用饱和单点，唯此位点漏走：同毫秒域折算的 cluster-node-timeout 在 runtime_server_config.rs:849 用 new_value.saturating_mul(1000)，convert.rs duration_seconds_to_ticks/expire_after_to_ticks 亦一律 saturating_mul。秒→毫秒折算散点未收敛到饱和单点，本票即该纪律的一处破口。

3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
debug 构建：--gossip-delay-secs 取 >= 2^64/1000 的合法 u64（例如配置文件写 gossip_delay_secs = 18446744073709551615）即 attempt to multiply with overflow 在 boot 播种臂 panic，节点无法启动。
release 构建：环绕取模 2^64 得任意小/畸形毫秒值，Duration::from_millis 照收，gossip 主循环 sleep 周期当场失真（环绕至接近 0 即退化为忙轮询、烧满单核，或落到无意义巨值使 failover/meet 建连 WaitAsync 上界随之失真）。属边界输入未拦截 + 溢出无守卫，违 review.md 板块 4.1 算术溢出保护与板块 5.1 边界输入响应。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/boot.rs:ClusterProvider 播种臂 gossip_delay 行（:261）
wedb/wedb/src/args.rs:ClusterArgs::gossip_delay_secs / ClusterFileOptions（:34,:74-75）
wedb/wedb/src/server/cluster_provider/flags.rs:set_gossip_delay_ms / gossip_delay_ms（:101-108）
wedb/wedb/src/server/gossip/gossip_manager.rs:gossip_delay（:56-57 消费）
对照饱和单点先例：wedb/wconf/src/runtime_server_config.rs:apply_cluster_node_timeout_update（:849 saturating_mul）

对应 c# 文件与函数：
garnet/libs/host/Configuration/Options.cs:GossipDelay（:295-296 int 秒）
garnet/libs/server/Servers/GarnetServerOptions.cs:GossipDelay（:246 int 秒）
garnet/libs/cluster/Server/ClusterManager.cs:gossipDelay = TimeSpan.FromSeconds(...)（:112）
garnet/libs/cluster/Server/Gossip/Gossip.cs:Task.Delay(gossipDelay)（:354）

精炼执行方案：
1 boot.rs:261 秒→毫秒折算改走饱和单点：args.gossip_delay_secs.saturating_mul(1000)，与 runtime_server_config.rs:849 同形，杜绝 panic 与 release 环绕（大值钳到 u64::MAX 毫秒即事实上的永不到期上界，行为可预测）。
2 或在 args 层对 gossip_delay_secs 设上界校验（对标 C# int 秒域上界 2^31-1 秒），超限拒启并回 InvalidArgument；二选一由处理席定夺，倾向方案 1（最小改动、与兄弟位点单源一致）。
3 测试验证点：gossip_delay_secs = u64::MAX 与 = 2^64/1000 边界下 boot 不 panic、set 后 gossip_delay_ms() == u64::MAX（或钳制值）且 gossip_delay() 返回可预期上界 Duration，非环绕畸形小值。
