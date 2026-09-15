# conversion-single-point 换算单点化

## 甄别结论

### 一、expire 换算饱和算术单点化：部分成立，做

事实修正：C# 无 TickConverter 类。换算集中于 garnet/libs/common/ConvertUtils.cs（UnixTimestampInSecondsToTicks / UnixTimestampInMillisecondsToTicks，无钳制，C# unchecked 语境），命令端换算为 libs/server/Resp/KeyAdminCommands.cs:421-427 的 switch（AddSeconds / AddMilliseconds / 两个 ToTicks）。C# 重放端（UnifiedInput）直接携带主端线性化后的绝对 ticks，不重算。

rust 侧因 AOF 记录原始参数（既有格式决策，不动），重放端在 aof_processor.rs 重算换算。现状乘法与钳制公式命令端、重放端各自手写：

命令端
- key_admin_commands.rs:72-83 ExpireCmd::expire_at_ticks 四分支（saturating_add + saturating_mul；min((i64::MAX - UNIX_EPOCH_TICKS) / TICKS_PER_*)）
- key_admin_commands.rs:192 RESTORE 的 SET EX 同构换算

重放端（aof_processor.rs）
- Expire / Pexpire：arg1.max(0).saturating_mul(TICKS_PER_*) → expire_in_ticks 相对域
- Expireat / Pexpireat：unix_timestamp_in_*_to_ticks(arg1.clamp(0, (i64::MAX - UNIX_EPOCH_TICKS) / TICKS_PER_*))
- Setex / Psetex：arg1.max(0).saturating_mul(TICKS_PER_*) 相对域

公式漂移即重放与原命令不等价，成立。落位 wbase::convert（该模块已是 ConvertUtils.cs 镜像，跨模块公共件走 wbase；wval::ttl 为 TtlCodec 编解码面，不承载换算）。C# 钳制公式逐字对照不成立（C# 无钳制），rust saturating 为既有确定性降级决策，保留并单点化，边界测试补齐（负值、i64::MAX 饱和、两端逐位一致）。

### 二、时间戳小面三点：成立，做

- metrics_commands.rs:25 now_stopwatch_ticks（C# Stopwatch.GetTimestamp，.NET Core Frequency 恒 10 MHz 即 100ns/tick）→ 移 wbase::time 单点，resp_server_session.rs:47 use 行同调（最小连锁）
- range_index_replication_activities.rs:13 now_ns 私有再包装 now_nanos，零附加值 → 删包装，直用 wbase::time::now_nanos
- failover_session.rs:9 直连 coarsetime::{Instant, Duration} → wbase::time 单点 re-export + now_instant()；C# FailoverSession 用 DateTime.UtcNow 差值域，rust 转写按 SKILL 选 coarsetime 单调域，只单点化引用面不改域

### 范围外记录（不改）

- wedb/src/server/cluster_provider.rs:9 直连 coarsetime::Instant（不在任务三点）
- basic_commands.rs GETEX/SET 族 compute_relative_expiry / compute_absolute_expiry 与 MAX_UNIX_TIME_SECONDS/MILLISECONDS（253_402_300_799，DateTimeOffset.MaxValue 语义：参数校验报错，非饱和钳制）与 hash_commands.rs:643-645 HEXPIRE 族为平行换算，语义不同不强行合并
- wmetric latency_metrics_entry.rs TICKS_PER_MICROSECOND 为 wmetric 内部换算面

## 实施

1. wbase::convert 增补：MAX_UNIX_TIMESTAMP_SECONDS / MAX_UNIX_TIMESTAMP_MILLISECONDS 常量、duration_seconds_to_ticks / duration_milliseconds_to_ticks（相对域饱和乘法）、expire_after_to_ticks / expire_after_ms_to_ticks（now + 饱和时长 → 绝对）、expire_at_seconds_to_ticks / expire_at_milliseconds_to_ticks（负夹 0、上界钳制）
2. wbase::time 增补：NANOS_PER_TICK、now_stopwatch_ticks、now_instant、re-export coarsetime::{Instant, Duration as InstantDuration}
3. 命令端与重放端同调 wbase 函数；时间戳三点同调 wbase::time
4. wbase convert 边界与等价性测试；wnode 命令端换算面与 wbase 函数逐位一致断言

新函数注释不写 `.cs:函数名` 映射格式（防 check.js 重复登记；既有映射 unix_timestamp_in_*_to_ticks 已占用 ConvertUtils.cs 对应函数）。

## 验证结果

分支 w3-convert-single（6 提交）已合并回主目录 dev（41fd24c），worktree 与分支已清理。

- ./clippy.sh：零警告（无 allow，--fix 仅 rustfmt 格式化与 use 合并）
- ./test.sh：2014 全过（首轮 1 例 wkv rapid_churn 并发 flaky，单跑与全量重跑均过，与本次改动无关）
- bun ./js/check.js：退出码 0，无新增缺失、无重复定义（新函数注释均以行号引用 C#，未注册重复映射）
- 等价性回归：wbase convert 4 测（饱和边界/钳制不动点/cap 常量一致性/相对-绝对同源差值），wnode 3 测（EXPIREAT/PEXPIREAT 命令端与重放端同输入逐位一致；EXPIRE/PEXPIRE 非饱和 1 秒时钟容差同源、饱和边界逐位 i64::MAX）
