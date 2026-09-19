会话指标启用双轨：选项侧布尔死轨与装配侧真实轨并存，删死轨把会话指标收敛为装配单点

来源：qcode 第 10 轮 net 视角审查条 5（原主张文件 next/qcode10.net.md 已清空删除，
裁决记录见 /Users/z/git/db/wedb/task/reject/qcode10.net.md 与
/Users/z/git/db/wedb/task/reject/qcode10-net-session-metrics-dup.md），本文是该条的唯一载体。
取证基线：主仓 /Users/z/git/db/wedb 当前 dev 工作树，行号按符号定位。

现状

一、同一事实（本连接是否采样会话指标）有两处声明点，其中一处恒不生效。

死轨在会话选项侧：/Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session.rs:140-141 声明
`pub metrics_sampling_frequency: bool`，注释按 C# 整型口径写成「指标采样频率（> 0 时启用会话
指标，C# MetricsSamplingFrequency）」——布尔字段配「> 0」判据，注释与类型互相矛盾；
:171 Default 为 false；:483-485 构造期据此建句柄
（`session_metrics: options.metrics_sampling_frequency.then(|| Arc::new(SessionMetricsHandle::default()))`）。
该布尔在生产路径上从未被置真：全仓只有两个生产构造点，分别是
/Users/z/git/db/wedb/wedb/wedb/src/server/boot.rs:50 与
/Users/z/git/db/wedb/wedb/wedb_standalone/src/main.rs:68，两者皆走
`RespServerSessionOptions::from(node)`，而该投影（resp_server_session.rs:187-214）只列
max_databases / latency_monitor / command_stats_monitor / lua / aof 等字段，采样位由 :213 的
`..Self::default()` 兜成 false。于是 :483-485 这条分支在生产恒不成立，只有单测
（/Users/z/git/db/wedb/wedb/wnode/tests/resp_server_session_tests.rs:459-462、
client_commands_tests.rs:85-87、session_metrics_slowlog_tests.rs:76-78 / :155-157 / :281-283、
server_monitor_tests.rs:131-134 / :177-180）把它置真。

真实轨在装配侧：/Users/z/git/db/wedb/wedb/wnode/src/service.rs:1582-1583 按
`self.metrics_sampling_frequency_secs > 0` 建一个 Arc，:1609
`with_session_metrics(session_metrics.clone())` 交存储执行域、:1611
`consumer.attach_session_metrics(session_metrics)` 注入会话；注入口是
/Users/z/git/db/wedb/wedb/wnode/src/resp/resp_session_consumer.rs:119-123，写侧带
「装配期注入」的理由注释。配置源为
/Users/z/git/db/wedb/wedb/wconf/src/node_options.rs:378 的 metrics_sampling_frequency_secs，
经 :723 投影、service.rs:1299 `.with_metrics_sampling_frequency_secs(..)` 入 provider。

后果：真实轨的注入是无条件覆盖还是仅在有值时覆盖，取决于 attach_session_metrics 的
`if metrics.is_some()` 判定，即死轨若哪天被误置真也会被真实轨盖掉——两处声明中只有一处
能决定结局，另一处纯属误导；后续按 options 字段调参、或按「构造期自建」假设改代码，都会
静默失效。会话侧字段本身（resp_server_session.rs:306 `pub session_metrics`）与其读者
（/Users/z/git/db/wedb/wedb/wnode/src/resp/info_provider.rs:57 以
`self.session.session_metrics.is_some()` 决定 INFO 的采样位）都是活的，问题只在创建口有两条。

二、同族副条（采样与延迟监视缺一致性判定）：C# 在装配期有一条互校验
`if (LatencyMonitor && MetricsSamplingFrequency == 0) throw new Exception("LatencyMonitor
requires MetricsSamplingFrequency to be set")`（garnet/libs/server/Servers/GarnetServerOptions.cs:839-840，
字段单值声明见同文件 :297）。rust 两字段各自缺省、各自解析、各自投影
（/Users/z/git/db/wedb/wedb/wconf/src/node_options.rs:378 与 :389，Default 段 :550-551，
投影段 :723-724），`latency_monitor = true` 而 `metrics_sampling_frequency_secs = 0` 的配法
在 rust 静默接受：会话侧延迟指标仍建（resp_server_session.rs:446-459 构造期只读
options.latency_monitor），聚合侧监视器三条件任一即起
（/Users/z/git/db/wedb/wedb/wnode/src/server.rs:280-288
`(metrics_sampling_frequency > 0 || commandstats_monitor || latency_monitor)`）。
该副条的修法已被启动投影票收进其方案（见交叉引用），本票不重复立第三处校验点。

C# 参考（单一来源对照）

- garnet/libs/server/Resp/RespServerSession.cs:264 会话构造期以
  `storeWrapper.serverOptions.MetricsSamplingFrequency > 0 ? new GarnetSessionMetrics() : null`
  单点建 sessionMetrics；:28 字段为 `readonly GarnetSessionMetrics sessionMetrics`，全仓无第二写入点；
  :309 / :1627 / :1667 把同一实例透传给 clusterSession 与存储执行域。
- garnet/libs/server/Servers/GarnetServerOptions.cs:297 采样频率单值声明、:839-840 装配期互校验。
- garnet/libs/host/Configuration/Options.cs 声明与投影各一处（对应 rust wconf 的字段 + From 投影）。

即 C# 也是「构造期一处判定」，只是其真值源就是配置投影本身；rust 把真值源搬到了 provider
按连接创建（这一搬迁是对的，因为本仓会话指标须与存储执行域共持同一对象，见
resp_session_consumer.rs:114-118 的理由注释），但把 C# 那条判定壳留在了选项侧没删。

修法

1. 删 /Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session.rs:140-141 的
   `metrics_sampling_frequency` 选项字段、:171 的 Default 行、:483-485 的构造期分支，
   构造处 session_metrics 直接取 None，并在 :306 字段注释标明「唯一写入点是
   resp_session_consumer.rs 的 attach_session_metrics，由 service.rs 装配期注入」。
   不留「选项字段在、恒 false、恒被覆盖」的第三态，也不反向把选项改成 u64 与 C# 同形
   （那会让 service.rs 的 attach 通路变成第二套，两轨并存正是本票要消的东西）。
2. 上述单测改走真实轨：构造后经 attach_session_metrics 注入句柄，或删除为死轨而写的断言；
   禁止用「保留选项字段以便测试」的理由留死轨。
3. 副条不另立校验点：与启动投影票并档，由其在投影单点落
   `latency_monitor && metrics_sampling_frequency_secs == 0` 的启动期拒绝（wconf 既有
   `validate()` 漏斗 /Users/z/git/db/wedb/wedb/wconf/src/node_options.rs:613-628 与
   NodeOptionsError 变体 :197-218 是现成落点），本票只做第 1、2 步的删轨。
   两票开工顺序：副条校验先落、死轨后删亦可，但交付时不得出现「选项侧仍留布尔字段且无读者」。
4. 不做向下兼容：不加「选项字段兼容期」注释、不留 deprecated 别名。

优先级

重复/多套架构（同一事实两处声明、一处生效），排在死代码清理之后、功能缺口之前。

交叉引用

- 副条与启动装配投影同主题：task/ing/boot-assembly-projection-single-source.md
  （其修法末段已含 C# :839-840 该校验的落点，本票不重复立点）。
- 会话巨文件族的拆分不动本票语义：task/ing/wnode-service-split.md（纯移动拆分里禁夹带删改）。
- DEBUG 保护档的选项投影缺位是同文件同形态（From<&NodeArgs> 用 `..Self::default()` 兜字段）：
  见 task/ing/qcode10-enable-debug-command-knob.md，两票若同批开工共用一次文件改动，
  但判定各自独立。

验收

- resp_server_session.rs 内 grep `metrics_sampling_frequency` 归零（选项侧无残留字段/注释）。
- 会话指标唯一创建点为 service.rs:1582-1583，唯一注入口为 attach_session_metrics。
- 采样开启（secs > 0）时 INFO 与命令计数行为不变（info_provider.rs:57 读侧判据不变）。
- cargo check --workspace --all-targets 零 error 零 warning；test.sh/clippy 由中央整合轮执行。

落地补记（fixloop d-sessmetrics 棒，2026-09-19）

一、认领时的既成事实（票面取证已过期，须记账）

- 本票修法第 1 步已由同主题棒先行落地并合入 dev：提交 c66a5483
  「refactor(resp): 删会话指标选项侧布尔死轨，收敛为 service.rs 装配单点注入」
  经合并提交 2d20499d（Merge branch 'sess-metrics' into dev）进入 dev。
  派发指令的查重判据 `git branch -a | grep -iE "sessmetrics|session-metrics"`
  不命中该分支名拼写（sess-metrics），故未拦住——查重口径须覆盖连字符变体。
  删掉的死轨符号：`RespServerSessionOptions::metrics_sampling_frequency: bool`
  （原 :140-141 声明、:171 Default 行、:483-485 构造期 `.then(..)` 分支），
  构造期改为 `session_metrics: None`。
- 同文件的 `latency_monitor` 与 `metrics_sampling_frequency_secs` 互校验（本票副条）
  已由启动投影票落在 wconf 单点：node_options.rs:868
  `if self.latency_monitor && self.metrics_sampling_frequency_secs == 0` 走 validate()
  漏斗，本票未另立第三处校验点。

二、本棒收口的票面缺口（第 2 步当时未落地）

c66a5483 把 4 处单测改成 `session.session_metrics = Some(..)` 字段直写，并在注释里
自称「模拟 …attach_session_metrics 单点写入」，与本棒接手时 src 字段注释「唯一写入点是
resp_session_consumer.rs 的 attach_session_metrics」直接矛盾：仓库里存在第二写入机制。

- resp_server_session.rs:895 新增 `RespServerSession::attach_session_metrics`，
  为 `session_metrics` 字段唯一写入方法；与同文件 `attach_monitor`（:872 晚装配换
  latency_metrics）同形态。:297 字段注释与 :485 构造期注释同步改指该口。
- resp_session_consumer.rs:104 `attach_session_metrics` 改为纯转发（对齐同文件
  `attach_acl` / `set_item_broker` / `set_slow_log_container` 的既有转发约定），
  并删掉双轨时代遗留的 `if metrics.is_some()` 判定：构造期恒 None 后它与无条件赋值
  等价，该判定只在「选项侧可能自建句柄」的旧形态下才有语义。
- session_metrics_slowlog_tests.rs 三个用例整体改真实轨：
  `StorageSessionProvider::open_with_config(..).with_metrics_sampling_frequency_secs(secs)`
  → `get_session(WireFormat::Ascii, id)` 取会话，命令经
  `MessageConsumerFace::try_consume_messages_into` 单泵消费（本文件只此一泵，
  原 `feed` + `drain_output` 双口撤除）；新增 `sampling_gate_controls_session_metrics`
  覆盖门控两臂（secs>0 → Some，secs==0 → None，对标 C# StoreWrapper.trackStats 门）。
  出向记账的精确断言（`total_net_output_bytes` == 累计冲出应答字节）一并移入真实轨用例，
  故 resp_server_session_tests.rs `output_pipeline_and_metrics` 的 13 字节记账断言
  不需在该处重建（该处只保留冲写管线断言，注入口改调 attach_session_metrics）。
- resp_server_session_tests.rs:581、server_monitor_tests.rs:138 两处字段直写改调注入口。
  这两处不经 provider：前者是会话冲写管线内測（需 `write_direct_large` + 冲写前
  `pending_output_len`），后者只验 dispose 归并，均非「句柄如何产生」的断言面。

三、grep 判据（合入后 dev = 435bec6c 实测）

- `grep -c metrics_sampling_frequency wnode/src/resp/resp_server_session.rs` → 0
- `grep -rn "Arc::new(SessionMetricsHandle::default())" --include=*.rs wedb | grep -v /tests/`
  → 唯一一条：wnode/src/service.rs:1647（`(self.metrics_sampling_frequency_secs > 0).then(..)`）
- `grep -rn "session_metrics *=[^=]" --include=*.rs wnode/src` → 除注入口
  resp_server_session.rs:896 外，仅执行域自身 builder 两处
  （garnet_api/mod.rs:341、storage/session/storage_session.rs:74，均 service.rs:1667
  `with_session_metrics(..)` 一次装配，与会话共持同一 Arc）
- `grep -rn "attach_session_metrics" wnode/src` → 定义 2（会话写口 + 消费者转发）、
  调用 1（service.rs:1669），别无第二调用点
- `grep -rn "session_metrics = Some" wnode/tests` → 0

四、门禁

- `cargo check --workspace --all-targets`：合并前基线 exit 0 / 0 warning；
  `git merge dev`（dev = 52f03c98）后复跑 exit 0 / 0 warning。
- `bun js/check.js`：在该 worktree 内跑，exit 0；与「同树 dev 基线」逐字节相同
  （A/B 用 cp + `git checkout <ref> -- <本票文件>` 还原，未用 stash；
  js/check/ignore/*.yml 无回写）。
- 定向用例（合并 dev 后）：session_metrics_slowlog_tests 4 passed、
  server_monitor_tests 3 passed、client_commands_tests 1 passed、
  resp_server_session_tests 68 passed / 1 failed。
- 越界发现（非本票引入，dev 既有红）：resp_server_session_tests.rs:782
  `acl_limited_user_filters_commands` 在 dev 52f03c98 上即红（A/B：把该文件退回
  dev 版本单跑同一用例仍红），肇因 52f03c98 带入的 wedb/wacl/src/user.rs 重写
  （526 行改动）与 acl_parser.rs / access_control_list.rs / acl_commands.rs 联动，
  断言 `s.check_acl_permissions(RespCommand::Get)` 为假。与同批
  next/wacl-user-local-concurrent-primitives.md 同族，转该票或另立票处理。
