归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 8f71f42（P2），收口形态：size 行补 512 头部项对齐 HistogramBase.cs:431（净+16 编译期常量授权上限内）＋真形态断言扩测。续排注：本票沙箱席与方案详情见下文。

甄别结论：通过（甄别席 zc-fix-r25-乙，2026-09-26）定级 P2
核验记录：C# 亲验——HistogramBase.cs:429-431 GetEstimatedFootprintInBytes = 512 + WordSizeInBytes * CountsArrayLength 现读亲见（LongHistogram WordSizeInBytes=8 在案）。rust 亲验——garnet_latency_metrics.rs:152-153 size 行仍 (hist.distinct_values() as i64) * size_of::<u64>() 且注释自称「C# GetEstimatedFootprintInBytes 的等价估算」——+512 头部常数仍缺、名实不符注释现码原样，全仓无修复合入；审核席桶几何亲算（3072×8 同值、恒差 512）依 hdrhistogram 7.6.0 distinct_values()=counts.len() 语义成立。查重：deviations 全册 footprint/脚印零命中；四池零同轴（percentile 另票他轴）。架构：编译期常量单点+注释订正+既有测试扩一行断言，冷路径零开销，最小闭环。格式：纯文本、双侧齐全。定级 P2：LATENCY HISTOGRAM size 行同数据集恒差 512 之确定性对拍分叉+误导注释妨碍后续复勘，观测面数值失真与 commandstats 同谱。

审核结论：通过（审核席 zcode-r22-review-wmetric，2026-09-26）

审核亲验锚点：
C# 侧 GetRespHistogram size 行实码 :metrics[idx].latency.GetEstimatedFootprintInBytes()；HistogramBase.cs GetEstimatedFootprintInBytes = 512 + WordSizeInBytes * CountsArrayLength，LongHistogram.cs:117 WordSizeInBytes => 8，GetLengthForNumberOfBuckets = (bucketCount+1)*(SubBucketCount/2)。
rust 侧 garnet_latency_metrics.rs:151-153 实码 (distinct_values())*8，hdrhistogram 7.6.0 distinct_values() = counts.len()（lib.rs:303-305），new_with_bounds 同式分配 num_bins = (buckets+1)*sub_bucket_half_count（lib.rs:1748-1751）。桶几何亲算双侧同值：low=1、high=1e9（wbase TICKS_PER_SECOND=10MHz，seconds(100)=1e9，与 C# Windows Stopwatch 同频域）、sigfig=2 → subBucketCount=2^8=256（双侧 largest=2*10^2、ceil(log2(200))=8 同式）、bucketCount=23（C# 从 255 起步、rust 从 256 起步，同界结果一致）→ counts 全长 3072，3072*8=24576、C# 25088，恒差 512 属实。查重：deviations.md 定向 grep 零登记。
方案确认：+512 编译期常量单点、冷路径零数据面开销、最小改动闭环。

LATENCY HISTOGRAM size 行恒缺 +512 头部常数（C# GetEstimatedFootprintInBytes = 512 + 8×桶数组长，rust 仅 8×桶数组长且注释自称等价估算）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# GetRespHistogram 的 size 行取 latency.GetEstimatedFootprintInBytes()（garnet/libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:145），其实现为 512 + WordSizeInBytes * CountsArrayLength（garnet/metrics/HdrHistogram/HistogramBase.cs:429-432；LongHistogram.cs:117 WordSizeInBytes => 8）——直方图保守内存脚印估计 = 512 字节头部开销 + 计数数组全长×8，数据无关的常量（桶几何：lowest=1、highest=TimeStamp.Seconds(100)、2 位有效数字，HistogramBase.cs:194-212 幂二 subBucketCount 与 CountsArrayLength=(bucketCount+1)*subBucketCount/2）。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust get_resp_histogram 的 size 行（wedb/wmetric/src/latency/garnet_latency_metrics.rs:151-153）写 (hist.distinct_values() as i64) * (size_of::<u64>() as i64)，注释自称「C# GetEstimatedFootprintInBytes 的等价估算」。hdrhistogram 7.6.0 的 distinct_values() 实为 counts.len()（已分配计数数组全长，~/.cargo/.../hdrhistogram-7.6.0/src/lib.rs:303-305），同界下桶几何与 C# 同源（sub_bucket_count 同为 2^8、num_bins=(buckets+1)*128 同式），故 ×8 部分数值对齐（Windows 同频域 10MHz 下 3072×8=24576），但恒缺 +512 头部项（C# 同域 512+3072×8=25088），每次 LATENCY HISTOGRAM 应答的 size 行恒差 512，注释「等价估算」名实不符。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
同一数据集双侧 LATENCY HISTOGRAM 对拍 size 行恒发散 512 字节；观测面数值失真虽小但为确定性偏差（非随机），且失真注释妨碍后续对账轮定位（据「等价估算」跳过复勘）。

涉及代码：
rust 文件与函数：
wedb/wmetric/src/latency/garnet_latency_metrics.rs:GarnetLatencyMetrics::get_resp_histogram（:151-153 size 行与等价估算注释）

对应 c# 文件与函数：
garnet/libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:GetRespHistogram（:145 size 行）
garnet/metrics/HdrHistogram/HistogramBase.cs:GetEstimatedFootprintInBytes（:429-432，512 + WordSizeInBytes*CountsArrayLength）
garnet/metrics/HdrHistogram/LongHistogram.cs:WordSizeInBytes（:117 => 8）

精炼执行方案：
1. garnet_latency_metrics.rs:153 size 行改为 (hist.distinct_values() as i64) * (size_of::<u64>() as i64) + FOOTPRINT_HEADER_BYTES，新增常量 const FOOTPRINT_HEADER_BYTES: i64 = 512（对齐 HistogramBase.cs:431 的 512 头部项）
2. 订正 :152 注释：footprint 口径 = 512 头部 + 8×计数数组全长（CountsArrayLength 同源），删「等价估算」虚述；另注记 C# Linux Stopwatch.Frequency=1e9 桶几何随频域放大属在册 tick 域选择，本行仅对齐同频域常数面
3. 测试验证点：单样本直方图 get_resp_histogram 输出断言 size 行值 = 512 + 8*distinct_values()（现有 test_global_latency_metrics_resp_and_reset 扩一行断言即可）
