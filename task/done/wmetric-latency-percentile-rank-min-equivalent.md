甄别结论：通过（甄别席 zc-fix-r25-乙，2026-09-26）定级 P2
核验记录：C# 亲验——HistogramBase.cs:368-389 GetValueAtPercentile 逐行核对：:371 取秩 ((p/100)*TotalCount + 0.5) 截断就近、:372 Math.Max(,1) 钳、:381-383 命中臂恒 HighestEquivalentValue（p=0 亦钳秩 1 取桶上沿）现读亲见。rust 亲验——三读出点 garnet_latency_metrics.rs:121/:178/:227-232 全部直耗 hdrhistogram value_at_percentile（库语义 ceil 取秩、quantile==0 取 lowest_equivalent），全仓无 value_at_percentile_cs 单点定义——现码无修复合入；审核执行注记（N mod 20 ∈ (0,10) 精确判据、min 行无条件触发）已录票顶，断言以例证为准。查重：deviations 全册 percentile/百分位零命中；四池零同轴（size 行另票他轴）。架构：私有单点收敛三读出点消三处重复消费、mean 面判不动（双侧 median 累计等价）、冷路径零数据面开销，合规。格式：纯文本、双侧齐全。定级 P2：分位值系统性偏移（取秩+1 桶、min 恒低一等价区间）致 LATENCY 核心观测面逐字节对拍必炸，误导 SLO 对账，无数据面危害。

审核结论：通过（审核席 zcode-r22-review-wmetric，2026-09-26）

审核亲验锚点：
C# 侧 HistogramBase.cs:368-389 行号逐行核对无误（:371 取秩 +0.5 就近、:372 Max(,1) 钳、:381-383 恒 HighestEquivalentValue 含 p=0）；HighestEquivalentValue = NextNonEquivalentValue-1 = lowest+range-1（HistogramExtensions.cs:69-70），min 例子亲算 bucket=2、range=4、highest(1000)=1003 → 100.3 µs，rust quantile==0 走 lowest_equivalent=1000 → 100.0 µs，恒差一个等价区间属实。取秩差亲算：x 小数位∈(0,0.5) 时 ceil 比 floor(x+0.5) 大 1（1006 样本 p=5 → 50.3：C# 50、rust 51），小数位=0/≥0.5 时双侧同值，例证成立。
rust 侧 hdrhistogram 7.6.0 value_at_quantile 实码核对（count_at_quantile = fractional_count.ceil()、finish 闭包 quantile==0.0 取 lowest_equivalent）；三读出点 garnet_latency_metrics.rs:121/:178/:227-232 亲见；mean 面双侧同以 median_equivalent 累计（HistogramExtensions.cs TotalValueToThisValue 与 crate mean() 折叠式），票面「不动」判断成立。查重：deviations.md 定向 grep 零登记。
方案确认：value_at_percentile_cs 私有单点收敛三读出点（iter_recorded 累计命中与 C# 逐桶累计在零计数桶上等价），消除三处重复消费，冷路径零数据面开销。
执行注记：票面「N 非 20 的倍数即触发取秩差」为概数表述（5th 精确条件是 N mod 20 ∈ (0,10)；min 行则无条件触发），执行与断言以 min 行与 1006/p=5 例证为准。

LATENCY 百分位读出双点与 C# 分叉（取秩 ceil vs 就近取整；min 行取桶下沿 vs C# 桶上沿等价值）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# GetValueAtPercentile（garnet/metrics/HdrHistogram/HistogramBase.cs:368-389）两点语义：其一取秩 countAtPercentile = (long)(((p/100.0)*TotalCount) + 0.5)（:371 就近取整，半位向上）且钳 >=1（:372）；其二命中桶恒回 HighestEquivalentValue（:381-383，含 p=0 的 min——p=0 时秩钳 1 仍取桶上沿等价值）。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 直用 hdrhistogram 7.6.0 原生 value_at_percentile（~/.cargo/.../hdrhistogram-7.6.0/src/lib.rs:1336-1402 value_at_quantile）两点皆异：其一 count_at_quantile = fractional_count.ceil()（:1354-1355 向上取整）——当 (p/100)*N 的小数位落在 (0, 0.5) 时 rust 取秩比 C# 大 1（如 N=1006、p=5：x=50.3，C# floor(50.8)=50，rust ceil(50.3)=51）；其二 finish 闭包对 quantile==0.0 取 lowest_equivalent、其余取 highest_equivalent（:1359-1364）——min 行 rust 回桶下沿、C# 回桶上沿，恒差一个等价区间宽（2 位有效数字桶相对宽约 0.4%~0.8%，如 min 落 1000 tick 时等价区间 [1000,1004)，C# min 呈 1003/10=100.3 µs、rust 呈 100.0 µs）。读出点共三处：get_percentiles（wedb/wmetric/src/latency/garnet_latency_metrics.rs:94-127）、get_resp_histogram（:132-183）、dump（:219-236）。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
同一数据集双侧 LATENCY HISTOGRAM 分位帧（min/5th/50th/95th/99th/99.9th）与 INFO 侧 GetLatencyMetrics 值在常见样本数（N 非 20 的倍数即触发取秩差；min 行无条件触发）下与 C# 发散，逐字节对拍必炸；分位值是延迟监控的核心观测面，系统性偏移（取秩+1 桶、min 低半桶）误导容量评估与 SLO 对账。

涉及代码：
rust 文件与函数：
wedb/wmetric/src/latency/garnet_latency_metrics.rs:GarnetLatencyMetrics::get_percentiles（:94-127）/ get_resp_histogram（:132-183）/ dump（:219-236）——三处经 hist.value_at_percentile(p) 单点间接消费 crate 取秩与等价值语义

对应 c# 文件与函数：
garnet/metrics/HdrHistogram/HistogramBase.cs:GetValueAtPercentile（:368-389，:371 取秩 +0.5 就近、:381-383 恒 HighestEquivalentValue）
garnet/libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:GetPercentiles（:70-126）/ GetRespHistogram（:128-158）/ Dump（:202-217）

精炼执行方案：
1. garnet_latency_metrics.rs 新增私有单点 fn value_at_percentile_cs(hist: &Histogram<u64>, p: f64) -> u64：取秩 rank = ((p/100.0)*hist.len() as f64 + 0.5) as u64 且 .max(1)（对齐 :371-372），经 hist.iter_recorded() 累计 count 命中 rank 后回 hist.highest_equivalent(v.value_iterated_to())（对齐 :381-383，含 p=0）
2. 三读出点（get_percentiles :121、get_resp_histogram :178、dump :227-232）的 hist.value_at_percentile(p) 全部改调该单点；mean 面不动（双侧同以 median 等价值累计，已等价）
3. 测试验证点：构造样本集使 (p/100)*N 小数位 < 0.5（如 1006 样本 p=5）断言取秩对齐 C#；单样本 1000 tick 断言 min 行输出 highest_equivalent 换算值（100.3 µs 形）；多分位既有测试回归
合入哈希：546a42b 收口形态：value_at_percentile_cs 私有单点收敛三读出点（get_percentiles/get_resp_histogram/dump），取秩 ((p/100)*N+0.5) 截断就近钳 >=1、命中桶恒回 highest_equivalent 含 p=0，与 C# HistogramBase.cs:368-389 双侧同值；min=100.30 与 1006 样本 5th=5.00 契约锁 3 测全绿，mean 面未动。
