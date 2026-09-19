裁决：不成立（过期票——票面主张的修法已在当前 dev 代码全部落地，无改动可做）
核销：2026-09-19 fixloop dev 棒（单问题票 now/stopwatch ticks 与时钟口径）
处置：next/ 该票删除并转本档案，不建 worktree、不动代码、不跑 cargo（拒绝路径按
task/reject 规程收口，且拒绝票不在共享主仓跑 check.js 以免回写他人在途 ignore 语料）

一句话结论：now_stopwatch_ticks 早已不接在实时墙上钟上，现锚在 std::time::Instant 单调基
（wbase/src/time.rs:47-52 进程锚点 STOPWATCH_ANCHOR、:62-65 由 elapsed() 换算 100ns 刻度并
返回 u64），票面「问题」段的实现描述与当前代码不符，票面「修法」三条全部在位，且已配
域纪律回归测试（wbase/tests/main.rs:145-158），本票属旧快照生成的僵尸票。

逐条裁决（票面主张 → 当前代码事实）
1. 主张「now_stopwatch_ticks 实现为 now_nanos() / NANOS_PER_TICK」：不成立。
   现实现 wbase/src/time.rs:62-65 为 `(STOPWATCH_ANCHOR.elapsed().as_nanos() /
   u128::from(NANOS_PER_TICK)) as u64 + 1`，基底是 std::time::Instant（Linux CLOCK_MONOTONIC），
   全函数不引用 now_nanos/Clock::now_since_epoch；NANOS_PER_TICK 仅作为除数在位。
   票面行号亦为旧坐标（称 :42-44，实际定义在 :62，now_nanos 实际在 :40）。
2. 主张「:38 文档注释说 i64 单调域，与实现矛盾」：不成立。现注释 wbase/src/time.rs:54-60
   写明「u64，100ns 单调计时域，对标 C# System.Diagnostics.Stopwatch.GetTimestamp」，
   i64 只属于实时时间戳域的 now_ticks（:67-74，对标 DateTimeOffset.UtcNow.UtcTicks），
   两域措辞与实现一致。
3. 主张「模块头 :3-11 的域纪律里唯独该计时源既非单调也未标注实时」：不成立。
   模块头 wbase/src/time.rs:10-12 单列「区间计时（延迟直方图/慢日志）取 now_stopwatch_ticks，
   源为 std::time::Instant……实时钟被 NTP/手动回拨不影响其差值」，:14-16 又立「时间戳域与
   计时域分工……两域禁互推」，纪律闭合。
4. 主张「resp_server_session.rs:1089 latency.start(... as u64)、:1105 let now =
   now_stopwatch_ticks() as u64」：不成立。现点 wnode/src/resp/resp_server_session.rs:1105
   （慢日志起点）、:1109（latency.start）、:1125（let now = now_stopwatch_ticks()）、
   :1134（latency.stop）均为原生 u64 直传，全文件已无刻度相关 as u64；
   slow_log_start_ticks 字段本身即 u64（:429-430）。
5. 主张「metrics_commands.rs:108-110 为 i64 裸减 now_ticks - self.slow_log_start_ticks，
   release 下回绕」：不成立。现实现 wnode/src/resp/metrics_commands.rs:107-113 两侧皆 u64，
   第 111 行 `now_ticks.saturating_sub(self.slow_log_start_ticks)`，与阈值同为 u64 比较，
   无回绕面。
6. 主张「负刻度 as u64 灌进 HDR 直方图使 P99 失真」：不成立。直方图侧接口一律 u64
   （wmetric/src/latency/latency_metrics_entry_session.rs:52 start、:62 record_value；
   garnet_latency_metrics_session.rs:78/93/106/142 start/get/stop_and_switch/stop），
   区间差用 saturating_sub（latency_metrics_entry_session.rs:66），越界值收敛到
   HISTOGRAM_UPPER_BOUND（:67-73、:96-98），不存在负值进桶路径。
7. 主张「C# 起止刻度全取 Stopwatch.GetTimestamp」：成立且 rust 已对齐，见
   garnet/libs/server/Metrics/Latency/LatencyMetricsEntrySession.cs:36/:44 与
   garnet/libs/server/Resp/RespServerSession.cs:488，rust 对应
   wnode/src/resp/resp_server_session.rs:1098-1113（起点与延迟计时同源，
   LatencyMetrics.Get(NET_RS_LAT)），无缺口。
8. 主张「修法：换单调基 / 实时时间戳仍留 now_secs/now_ms/now_ticks / elapsed 一律
   saturating 差、禁 as u64 直转」三条：全部已在位（依次见 wbase/src/time.rs:47-65、
   :26-42 与 :67-74、上文第 4/5/6 条），无剩余动作。
9. 主张「一套计时源」：已满足。全仓计时源唯 wbase/src/time.rs:62 一处
   （grep now_stopwatch_ticks 消费面仅 wnode/src/resp/resp_server_session.rs、
   wnode/src/resp/metrics_commands.rs、wnode/src/storage/session/storage_session.rs:163-165
   三处，均同域），wbase/wconn 内无第二个 Instant/Stopwatch 取时函数。
10. 票面边界引用的 sibling 档案（task/done/tick-scale-conversion-single-source.md、
    task/ing/pending-lat-no-timing-site.md）现均不在册（task/ing 仅余他人认领件、
    task/done 目录不存在），换算单源已在 wbase/src/convert.rs:26-45 落地，
    PENDING_LAT 计时点已在 wnode/src/storage/session/storage_session.rs:163-165 接线，
    三票的分工亦已无待办残值。

越界登记（本票射程外，本代理只取证未动码，建议主代理另开票）
range_index 活动计时用了实时域且单位与 C# 不同阶：
wnode/src/rangeindex/range_index_replication_activities.rs:7,35,66 与
wnode/src/rangeindex/range_index_migration_activities.rs:30,41,46,56,107,118,132,173,193,211
以 wbase::time::now_nanos（实时域 SystemTime，wbase/src/time.rs:40-42）取区间端点，
而 C# 对应点全取单调秒表：garnet/libs/cluster/Server/Migration/RangeIndex/
RangeIndexMigrationActivities.cs:29/:31/:33/:37/:41-44 用 Stopwatch.GetTimestamp +
Stopwatch.GetElapsedTime(...).Ticks。两点偏差：其一域选错（回拨窗口内区间失真，
现虽已 saturating_sub 不会回绕，但会静默钳零）；其二单位不同阶，rust 端点存 ns
（字段名 started_ns/transmitting_ns/ended_ns）却在 :66/:123 等处命名 total_ticks 并
按 C# 的 100ns tick 字段名入日志（对照 range_index_replication_activities.rs:68 的
totalTicks=），数值比 C# 大 10 倍。另 range_index_replication_activities.rs:4-5 模块头
自称「以 wbase::time 粗粒度时钟承接」，与 now_nanos 的 VDSO 实时域事实也不符。
此类改动落在 range-index 域（next/range-index-replication-shutdown-dispose.md 同文件），
本代理按单问题票纪律未动。

票面原文（逐字保留，原位置 next/now-stopwatch-ticks-wall-clock.md）

优先级：低
分拣注记（qw.my 第 11 轮条 5 拆出；浅核 2026-09-19：now_nanos wbase/src/time.rs:31、now_stopwatch_ticks :42 在场；边界成立——done/tick-scale-conversion-single-source.md 管 tick↔秒换算常量三形态，ing/pending-lat-no-timing-site.md 管 PENDING_LAT 缺计时点，本票管取时源所在域的选择，三票互不覆盖；修法区分时间戳/计时两域，与 SKILL:24 不冲突）

now_stopwatch_ticks 把「单调计时域」声明落在可回拨的实时墙上时钟
问题：wbase/src/time.rs:42-44 now_stopwatch_ticks 实现为 `now_nanos() / NANOS_PER_TICK`，而
:31-33 now_nanos 是 `Clock::now_since_epoch().as_nanos()`；coarsetime-0.1.37/src/clock.rs:84-90
该入口即 `time::SystemTime::now()`（实时域，可被 NTP/手动回拨），与 :38 文档注释「i64，100ns
单调域，对标 C# System.Diagnostics.Stopwatch.GetTimestamp」、:40「延迟度量/慢日志域的统一计时源」
直接矛盾——本模块头 :3-11 自己立的「TTL 用实时域高精度、粗粒度时钟禁入判定路径」域纪律里，
唯独这个计时源既非单调也未标注实时。
消费面是逐命令区间计时而非时间戳：wnode/src/resp/resp_server_session.rs:1085
slow_log_start_ticks 起点、:1089 `latency.start(LatencyMetricsType::NetRsLat, ... as u64)`、
:1105 `let now = now_stopwatch_ticks() as u64` 传给 latency.stop；
wnode/src/resp/metrics_commands.rs:108-110 `let elapsed = now_ticks - self.slow_log_start_ticks`
（release 下 i64 减法回绕）后 `elapsed > slow_log_threshold`。墙钟回拨窗口内 elapsed 为负即慢
命令静默漏记，:1089/:1105 的 `as u64` 把负刻度回绕成巨值灌进 HDR 直方图，P99 失真且无告警。
C#：garnet/libs/server/Metrics/Latency/LatencyMetricsEntrySession.cs 与 RespServerSession.cs 的
起止刻度全取 Stopwatch.GetTimestamp（QueryPerformanceCounter 单调），与 DateTimeOffset.UtcNow
的实时域严格分开，不存在回拨影响。
修法：该计时源换单调基——std::time::Instant 取进程级锚点后换算 100ns 刻度（或 quanta 一类
TSC/VDSO 单调源），实时域时间戳仍留 now_secs/now_ms/now_ticks；elapsed 一律
saturating/checked 差，禁 `as u64` 直转；若裁定保留实时源则须反向收口：改写 :38-40 注释为实时域
并给两处消费点加回拨防护，不得维持「注释说单调、实现是墙上钟」。
条款：SKILL.md:24（取时间戳用 coarsetime，时间戳域与计时域分工）、SKILL.md:10（自定义改造须自
证正确）。
边界：task/ing/tick-scale-conversion-single-source.md 管 tick↔秒↔微秒换算常量的三形态，本条是
取时源所在的域选择；task/ing/pending-lat-no-timing-site.md 管 PENDING_LAT 缺计时点，本条是其余
计时点用的域错了。
