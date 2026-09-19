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
