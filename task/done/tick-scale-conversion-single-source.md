秒↔tick↔微秒刻度换算收口为 wbase::convert 单点：keyspace 裸乘 10000000 与
wmetric/wconn 两份本地派生组归一

来源：task/ing/tick-scale-conversion-single-source.md（next/glm.design.md 第 12 轮条 1
立票）。取证基线主仓 /Users/z/git/db/wedb，分支 dev，行号按落地时当下代码。
结论：票面成立，已落地，无拒绝项。

甄别
1. 三套刻度形态在位，取证准确（行号按 dev 当下符号重定位）：
   - 单点 wedb/wbase/src/convert.rs:11 TICKS_PER_SECOND、:12 TICKS_PER_MILLISECOND，
     确缺 tick/微秒成员与 u64 侧秒→tick 形态；
   - 生产码裸乘绕行 2 处：wkv keyspace flush_database、flush_namespace 的
     `now + (db_gc_reclaim_delay_secs * 10000000) as i64`（原 :107、:163，收口前实况
     同），且乘法在 u64 域内做再 as i64 收窄——debug 构建溢出 panic、release 构建环绕
     成过去时刻，与单点饱和路径不一致；该文件 :4 已 `use wbase::time::now_ticks`、
     wedb/wkv/Cargo.toml 已开 wbase `convert` 特性，单点零成本可达却绕行，判定成立；
   - 本地派生组 2 份：wmetric/src/latency/latency_metrics_entry.rs:45-61 `time_stamp`
     模块（:50 TICKS_PER_SECOND、:52 TICKS_PER_MICROSECOND、:54 TICKS_PER_SECOND_UNIT、
     :58 seconds()），wconn/src/metrics.rs:31、:33 两个本地 const——两份 TICKS_PER_MICROSECOND
     逐字重复，且都把 i64 单点 cast 成 u64 后用；由此还拖出跨 crate 反向取常量的消费面
     （wmetric garnet_latency_metrics.rs:12、wmetric slowlog/resp_slowlog_commands.rs:166、:168、
     wnode resp/metrics_commands.rs:12 为拿一个刻度常量穿进 wmetric 内部模块）。
2. 票面修法第 1 条有一处数值错： TICKS_PER_MICROSECOND 不是「值 1」。100ns tick 域
   tick/微秒 = 10（.NET `TimeSpan.TicksPerMicrosecond` = 10；C#
   garnet/metrics/HdrHistogram/OutputScalingFactor.cs:26 `TimeStampToMicroseconds`
   = `Stopwatch.Frequency / 1_000_000` = 10），原两份本地派生组算出的也是 10。落地按
   派生式编译期算出，不写死数值，也不写 1。
3. 票面修法第 3 条的疑问（Stopwatch.Frequency 与 .NET ticks 是否口径差）复核为无口径差：
   本仓 wedb/wbase/src/time.rs:36 NANOS_PER_TICK = 100 与 :42 now_stopwatch_ticks、
   :51 now_ticks 同为 100ns tick，差在纪元（UNIX vs 0001-01-01）与整数域
   （Histogram<u64> vs i64 TTL 记录），不在刻度。故按「同单位、不同整数域」在单点内
   以 stopwatch 子域具名表达，禁第二套数值，未保留票面假设的「口径差」。
4. 射程外判活（保留现状，改则偏离对标 C#）：
   - wedb/wconf/src/runtime_server_config.rs:1002、:1008 的 `* 1_000_000` /
     `/ 1_000_000` 是配置时长单位互转（秒/毫秒/微秒），无 tick 语义，1:1 对标
     C# RuntimeServerConfig.ConvertDuration 的同一写法，且本身已是单点；
   - wedb/wkv/src/session/mod.rs:228 `route_idle_evict_secs * 1000` 是秒→毫秒定时器域；
   - wedb/wnode/src/logging.rs:60 `% 1_000_000 / 100` 是 C# `ffff` 日志小数段格式化；
   - 测试内的 `10_000_000i64` 时间基准常量按票面边界不入射程。
5. 无「用 std 既有方法替换自研实现」类主张，未触发 rustc 探针前置。

落地
1. wedb/wbase/src/convert.rs 增 `pub mod stopwatch`（:26-43），对标 C# HdrHistogram 的
   OutputScalingFactor + TimeStamp 两个库常量面（rust 无 partial，合一处）：
   :34 `pub const TICKS_PER_MICROSECOND: u64 = TICKS_PER_SECOND as u64 / 1_000_000`（编译期
   派生，= 10），:40 `pub const fn seconds(seconds: u64) -> u64`（对标 C#
   `TimeStamp.Seconds` = `Stopwatch.Frequency * seconds`，const fn 故可入直方图边界常量）。
   模块与文件头 i64 常量的分工写进注释：同 100ns tick，仅整数域不同。
2. wedb/wbase/src/convert.rs:157-167 `expire_after_to_ticks` 文档改为「now + 相对秒 =
   绝对 ticks 截止」的唯一换算并点名第二个消费方（DbMeta 死亡账本 expired_at）；
   不新增近似重复的 seconds→ticks 函数。
3. wedb/wkv/src/store/keyspace.rs:16-26 新增私有 `reclaim_expired_at(now, delay_secs)`，
   内部走 `expire_after_to_ticks`；:119、:175 两处换号截止改用它，消除裸乘 10000000
   与 u64 乘后 as i64 收窄。配置秒数超 i64 值域时 `i64::try_from(..).unwrap_or(i64::MAX)`
   钳到「截止永不到期」——宁可旧域滞留磁盘，也不透支「严防幽灵读取」的安全纪元窗
   （提前回收才是可见性事故）。默认 86400 秒路径逐位等价，行为不变。
4. wmetric 侧：删 latency_metrics_entry.rs 的 `time_stamp` 模块与其专属测试（该测试断言
   随常量迁到 wbase convert.rs:247 `stopwatch_scale_derives`）；
   latency_metrics_entry.rs:2、latency_metrics_entry_session.rs:2 引 `stopwatch::seconds`；
   garnet_latency_metrics.rs:5 改引 wbase 因子；slowlog/resp_slowlog_commands.rs:1-5、
   :168-172 秒侧直接用 i64 的 `TICKS_PER_SECOND`（同时消除 TICKS_PER_SECOND_UNIT 这个
   同义别名与其 `as i64`），微秒侧 `TICKS_PER_MICROSECOND as i64`。
5. wconn 侧：删 metrics.rs 两个本地 const，:25 引 `stopwatch::{TICKS_PER_MICROSECOND,
   seconds}`，:34 直方图上界改 `seconds(100)`（对标 C# GarnetClient.cs:183
   `TimeStamp.Seconds(100)` 同形）。
6. wnode 侧：resp/metrics_commands.rs:8 的刻度因子改从 wbase 取，断开「wnode 为常量
   穿 wmetric 内部模块」的反向依赖；wmetric 的 `pub mod time_stamp` 删除后全仓
   `time_stamp`/`TICKS_PER_SECOND_UNIT` 零残留。
7. 锚点面：无删除或改挂 C#↔rust 映射注释（被删注释仅指 `OutputScalingFactor.*`、
   `Garnet.common TimeStamp.Seconds` 这类非 `.cs:函数` 形态的对标说明，check.js 正则
   不采集），故 `js/check/ignore/*.yml` 零登记；`metrics/HdrHistogram/TimeStamp.cs`
   早已整文件在 js/check/ignore/metrics.yml:81 登记（rust 用 hdrhistogram crate），
   本单未新增该族锚点，不与之冲突。

验收
1. 全仓裸乘点收口前后 grep 读数（`grep -rn "10000000" wedb --include="*.rs"`，排除
   tests/ 与 assert）：
   - 前：wkv/src/store/keyspace.rs:107、:163（生产码裸乘 tick 刻度）+
     wbase/src/convert.rs:11 单点定义 1 处 + 测试常量 6 处；
   - 后：生产码 0 处；只剩 wbase/src/convert.rs:11 单点定义，以及两个与刻度无关的
     字符串/测试数字（wnode/src/resp/vector/resp_server_session_vectors.rs:67 的
     RESP 错误文案「0 and 100000000」、wbase/src/num.rs:390 与
     wnode/src/resp/objects/sorted_set_geo_commands.rs:952 的断言字面量）。
2. `grep -rn "TICKS_PER_MICROSECOND:" wedb --include="*.rs"` 收口前 2 处定义
   （wmetric latency_metrics_entry.rs:52、wconn metrics.rs:33）→ 收口后 1 处
   （wbase/src/convert.rs:34）；`/ 1_000_000` 派生式全仓唯此一处，其余命中为
   wconf 配置单位互转与非 tick 域格式化。秒→tick 换算面收口后只有两个具名入口：
   i64 饱和域 convert.rs:144/:167，u64 直方图域 convert.rs:40。
3. 门禁在 worktree /tmp/fork/tick-scale-single 内跑（CARGO_TARGET_DIR 独立）：
   - `cargo check --workspace --all-targets` exit=0，零 warning 涉本次 8 文件
     （合并 dev@8e452fd8、dev@0b26d23d 后各复跑一次，均 exit=0）；
   - `bun js/check.js` exit=0，输出与开工基线逐条比对，新增两「重复定义」条目均为
     他代理在飞面（`ClusterSlotVerify.cs:SingleKeySlotVerify`、
     `VectorManager.cs:VectorManager`），与本单无关；本单零新增重复定义、零新增缺失、
     语料零改写（跑后仅 check.js 自身对 js/check/ignore/common.yml 的既有陈旧项做
     归一化改写，已 `git checkout --` 还原，不入本单提交）。
4. 未跑 ./test.sh 与 ./sh/clippy.sh（按 fixloop 规程交主代理合并后统一跑）。
5. 合入 dev：merge commit 6fc87518（父 dev@0b26d23d + 分支 tick-scale-single），
   payload 已同步主仓工作树。

对账
- js/check/ignore/common.yml 在 dev 上处于「跑 check.js 即被改写」的陈旧态
  （`libs/common/Endpoint.cs` 族的 TryParseAddressList 已被 endpoint-parse-fail-fast
  实现却仍挂 ignore，外加一处 YAML 折行未归一）。非本单射程，主代理跑门禁时会被
  check.js 自动规范化，建议顺手单独提交，勿与本单混提。
- 本单落地过程中 dev@7af87717 曾出现过 wnode 破口（SessionDependencies 已删
  acl_settings 而 service.rs:1473 仍传参，E0560），非本单引入，dev@8e452fd8 之后
  已由 wnode 面自愈，记录备查。
