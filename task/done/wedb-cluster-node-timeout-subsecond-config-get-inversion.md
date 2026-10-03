终态：已合入 dev（2026-09-27）。2ee9815 方案A:ms!=0且<1000 拒启豁免0哨兵;伴生面 cluster_node_timeout_seed_secs 饱和+try_set 弃收落warn;拒ceil保单换算

甄别结论：通过 | 定级 P2 | 2026-09-27 zcode fixloop 甄别席（双侧锚现码亲验，零撞票）
执行提示：方案A：ms!=0 且 <1000 拒启、豁免 0 哨兵；reject 票伴生面 as i32 环绕+try_set 弃收建议一并收

审核结论 2026-09-27：通过，裁定方案 A（CLI 启动校验亚秒拒绝）。校验条件必须为 ms != 0 且 ms < 1000 报错，豁免 0 无限哨兵（args.rs:67 明写 0 = 无限超时，字面 ms < 1000 会误杀）；建议对齐 boot.rs gossip_sample_percent 范围校验先例返 NodeError::InvalidArgument。不动 CONFIG SET 整秒 ×1000 单向投影（runtime_server_config.rs apply_cluster_node_timeout_update + config_owner.rs 投影消费已闭环），不引入双机制。拒 B 理由：秒槽 SECONDS 粒度承载不了亚秒，ceil 回显仍失真（500ms 显 1s），且 ceil 播种与 ×1000 SET 两路换算规则分叉即双机制。全部锚点亲验属实（args.rs:67-70 / boot.rs:242-248 / flags.rs:79-97 / wconf runtime_server_config.rs:941-943 与 840-851 / config_owner.rs:83-87 / Options.cs:298-300 / RuntimeServerConfig.cs:314-329）。
亚秒 cluster-node-timeout 毫秒粒度下 CONFIG GET 回显 0（无限语义）与实际生效值语义反转

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# CLI --cluster-timeout 为秒整型（Configuration/Options.cs:299-300），无法表达亚秒值，槽值与生效值恒一致；0 → GetTimeSpan 归 Timeout.InfiniteTimeSpan（RuntimeServerConfig.cs:314-329），回显与生效语义同向。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust CLI --cluster-node-timeout 采毫秒粒度 u64（wedb/src/args.rs:67-70，无下界校验）。boot.rs:242-245：set_cluster_node_timeout_ms(args.cluster_node_timeout_ms) 直投毫秒生效（gossip/failover 槽位真实 500ms），同时 `(ms / 1000) as i32` 整除截断播种 CONFIG 秒槽——亚秒值落 0；runtime_server_config.rs:941-943 seconds_from_time_span 正值直存。CONFIG GET cluster-node-timeout 按全仓「非正即无限」约定回显 0 即「无限超时」，与实际 500ms 生效语义相反。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
监控/审计按 CONFIG GET 判读得出「无超时」错误结论；此后任意 CONFIG SET cluster-node-timeout <n> 以整秒毫秒值覆盖 provider 槽，亚秒精度静默丢失。分叉系 rust 毫秒粒度自引入（改良引入的回显面漏洞），非 C# 原生形态。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/boot.rs:集群参数播种臂
wedb/wedb/src/server/cluster_provider/flags.rs:ClusterNodeTimeout 消费面

对应 c# 文件与函数：
garnet/libs/host/Configuration/Options.cs:ClusterTimeout（秒粒度）

精炼执行方案：
1 播种臂亚秒值处置二选一由审核席裁定：A 亚秒值拒绝启动（CLI 校验 <1000 报错，回显与生效恒一致）或 B 秒槽播种取 ceil（ms+999)/1000 保非零回显、CONFIG SET 路径同步换算
2 测试验证点：500ms 配置下 CONFIG GET 回显与 provider 生效槽一致、语义不反转
