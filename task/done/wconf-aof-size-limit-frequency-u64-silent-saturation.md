终态：合入 825f9b0，check_time_knobs 增第五枚 TimeKnobAboveContractUpper 上限闸（超 i32::MAX 拒启），投影 min 收窄保留为纯类型适配。

aof-size-limit-enforce-frequency u64 值域静默饱和，C# fail-fast 拒启退化为静默改写且 CONFIG 回显与输入不符

审核结论：通过（2026-10-01 独立审核席现码复跑，P3 维持）
- 真实性全锚亲验：node_options.rs:997 u64 定义 clap 无上界；validate :1350-1594 全函数零闸，上限门确为四枚（:1532 repl-attach-timeout / :1538 index-resize-frequency / :1544 metrics-sampling-freq / :1555 tls-cert-refresh-freq）不含本字段；投影 :1676-1678 .min(i32::MAX as u64) as i32 静默饱和；service.rs:534-549 freq_secs 闭包每轮现读槽位 sleep，饱和即约 68 年周期。C# 侧 Options.cs:260-261 [IntRangeValidation(0, int.MaxValue)] int 强类型、ServerSettingsManager.cs:228-240 notParsed 臂 + :69-70 return false 拒启，超 int 域值解析失败即退确证。
- 既定形态抗辩不成立：投影注释「u64 饱和收窄 i32 槽宽」（:1674-1675）系类型适配标注，doc/zh/deviations.md 该字段零命中、无在册裁决出处。族对比：wconf-node-args-time-knobs-upper-gate 族票形态裁决明文「采 a（validate 补上限闸拒启）」并否决 saturating/哨兵案（「saturating 案不予采纳」「禁巨大时限作无限」）；wconf-boot-interval-gates 族票审核明列本字段为「u64 …上界 C# i32::MAX 缺」待裁枚（该票 :156-159），同批待裁的 aof_commit_ms（validate :1428-1430 已闸）与 tls_cert_refresh_freq（:1555-1560 已闸）均已收口，本枚系族内登记在案的剩余未收口臂，非既定形态。C# 拒启 / rust 静默钳放行系 rust 比上游更宽，不属防御收口修复型分叉。
- P3 恰当：无崩溃无数据破坏，纯配置错误静默生效 + 行为分叉 + CONFIG GET 回显饱和值与输入不符的可观测失真，较 node-args 族（P3 条件形、有 panic 面）更轻。
- 方案可落：与仓内 TimeKnobAboveContractUpper 闸同族同形（变体与 wconf/tests/node_options.rs 拒启册均现成扩臂即可），投影 min 收窄保留作类型适配符合族票「投影/消费面零改动」口径。

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# Options.cs:260-261 AofSizeLimitEnforceFrequencySecs 为强类型 int；启动经 CommandLine.Parser 解析（ServerSettingsManager.cs:TryParseArguments notParsed 臂 :231-240），超 int 域值解析即失败打印错误退出——fail-fast，非法输入绝不带病运行。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   wedb/wconf/src/node_options.rs:997 aof_size_limit_enforce_frequency_secs 为 u64（clap 无上界）；validate（:1350-1594 全臂枚举）无此字段闸——上限门四枚只收 repl-attach-timeout/index-resize-frequency/metrics-sampling-freq/tls-cert-refresh-freq；投影口 :1676-1678 .min(i32::MAX as u64) as i32 静默饱和（码面注释自陈 u64 饱和收窄 i32 槽宽）。消费者 service.rs:spawn_aof_size_limit_task（:548-549）每轮现读槽位 sleep，饱和值即约 68 年周期。与已收口票不同形：wconf-node-args 族是溢出 panic，wconf-boot-interval-gates 族审核明注字段均 i32（无 u64 输入面），本旋钮属两族枚举外的新臂；deviations 无饱和机制登记。
3. 逻辑危害确证
   运维笔误写巨值当「永不大检」：C# 拒启，rust 静默钳成 2147483647 秒继续跑——AOF 体积限额自动检查点任务事实上永不触发且零告警；CONFIG GET aof-size-limit-enforce-frequency 回显 2147483647 与用户输入不符，故障难定位。

涉及代码：
rust 文件与函数：
wedb/wconf/src/node_options.rs:aof_size_limit_enforce_frequency_secs（定义 :997、validate 无闸、投影 :1676-1678）
wedb/wnode/src/service.rs:spawn_aof_size_limit_task

对应 c# 文件与函数：
garnet/libs/host/Configuration/Options.cs:AofSizeLimitEnforceFrequencySecs
garnet/libs/host/ServerSettingsManager.cs:TryParseArguments

精炼执行方案：
1. validate 增该字段上界闸（超 i32::MAX 拒启报错，对齐 C# fail-fast 与仓内上限门四枚同族机制），投影处 min 收窄保留作类型适配。
2. 测试验证点：巨值启动断言拒启与错误文案；边界 i32::MAX 与缺省值回归通过。
