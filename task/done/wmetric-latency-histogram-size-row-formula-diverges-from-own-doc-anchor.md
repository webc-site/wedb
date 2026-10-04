锁定注记（2026-10-01 r9 波主控，基线本票落笔时 dev 尖；双侧现码亲验）：
- rust 现位亲验：wmetric/src/latency/garnet_latency_metrics.rs:138-142 注释自陈口径为
  「512 头部开销 + 8 × 计数数组全长（CountsArrayLength 同源，HistogramBase.cs:431）」，
  :143-145 实写 `(hist.distinct_values() as i64) * size_of::<u64>() + FOOTPRINT_HEADER_BYTES`
  （常量 FOOTPRINT_HEADER_BYTES = 512 在同文件），直方图类型为外部 hdrhistogram 7
  （wmetric/Cargo.toml:15），distinct_values 语义是「已记录不同值个数」，量程与桶几何不进式。
  同仓另一消费面仅 wmetric/tests/latency_metrics.rs:23（拿同一 accessor 断言），非实现面。
- C# 现位亲验：garnet/libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:145 size 行直取
  `metrics[idx].latency.GetEstimatedFootprintInBytes()`。
- 判据可亲验性如实标注（本票定级的关键前提）：`GetEstimatedFootprintInBytes` 源在外部
  HdrHistogram 包，盘上无 HistogramBase.cs（本席 find 零命中），其内部公式主控不可亲验；
  因此「实现应改为计数数组全长」无独立真值源支撑，禁止在无亲验判据下改实现追公式。
  本票可亲验的缺陷面只有一个：**注释与本体脱同步**（注释声称的判据与代码实算式两轨），
  以及在册 §166b 的 size 行推论前提被实现破坏（§166 只登 a page_size / b tick 域 10MHz /
  c TreeCache，size 行推论见 task/done/wmetric-info-observable-divergence-registry-trio.md
  的 b 段，其成立前提正是「size 由数组全长决定」）。
- 非重复亲验：deviations.md grep footprint|distinct_values|GetEstimatedFootprintInBytes 零命中；
  task 池同词仅命中上述 §166b 档（频域轴，异轴）与 wmetric 可观察面三件（size 行值/页大小/TreeCache，
  非公式口径轴）；filter 有效性由同库 latency_metrics 命中链实证。
- 禁触域：wmetric/src/latency/garnet_latency_metrics.rs 以外的刻度域选择面（10MHz 单刻度轨）
  系 §166b 既定裁决，本票禁复活；wnode/src/resp/objects/**（同侪在途）禁碰。
- 定级：P3 治理/登记级（对外可观察面 size 低报不致错判危害，主要是自陈契约与在册判据失真，
  后续轮次按错前提推口径必误）。

审核结论：待审（主控亲立案，判据不可亲验面已明标）

LATENCY HISTOGRAM size 行实算式与自身注释及在册判据脱同步，注释声称的数组全长口径无实现对应

问题分析：
1. Garnet 契约对齐：C# size 行取直方图估算足迹单点
   garnet/libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:GetEstimatedFootprintInBytes；
   rust 侧对应位为同一 `size` 字段的手写算式，本应与其注释自陈的口径同源。
2. 工程现状：注释钉的是「512 + 8 × 计数数组全长」并点名 HistogramBase.cs:431 与 CountsArrayLength
   同源，实现写的是「512 + 8 × distinct_values」——同一函数体内两轨，量程/桶几何完全不进入实现式；
   在册 §166b 的「量程随频域放大 → size 行与 C#-Linux 上界差 100 倍」推论，其前提正是注释那条口径，
   实现既已脱钩，该推论在现码上不成立。
3. 逻辑危害确证：稀疏样本 + 大量程时 size 行系统性低报直方图内存占用（灌 1 条高量程样本，
   注释口径恒为数组全长决定值，实现回 512+8）；更实的危害在判据面——后轮以注释或 §166b 文本
   为前提推 size 口径必误推，属自陈契约失真族（与本波 wedb-self-file-relative-line-anchor-family
   同谱：文档/注释与本体脱同步）。

涉及代码：
rust 文件与函数：
wedb/wmetric/src/latency/garnet_latency_metrics.rs:write_histogram_info 的 size 行渲染段（注释与实算式两轨）
wedb/wmetric/src/latency/garnet_latency_metrics.rs:FOOTPRINT_HEADER_BYTES（头部开销常量单点）
wedb/wmetric/tests/latency_metrics.rs（distinct_values 同 accessor 的现册锁测）

对应 c# 文件与函数：
garnet/libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:GetEstimatedFootprintInBytes 消费位
（外部包公式盘上不可亲验，本票不据此改实现）

精炼执行方案：
1. 默认单路径「保实现、订口径」：把 size 行注释改写为实口径（512 + 8 × distinct_values，
   即已记录不同值个数），并显式写明「与 C# GetEstimatedFootprintInBytes 的口径关系未经盘上亲验，
   本仓以 distinct_values 为真值源」，禁保留现有把 HistogramBase.cs:431 当同源出处的表述。
2. 同票在 deviations.md 册尾顺编立节登记该口径（符号锚 GarnetLatencyMetrics::GetEstimatedFootprintInBytes、
   garnet_latency_metrics::FOOTPRINT_HEADER_BYTES、distinct_values；来源指针本票），并把 §166b
   在册文本的 size 行推论限缩为「量程/值域面」，勿使后轮再按数组全长推 size；
   落册前必重新 grep 现册最大号位（尾号让号），禁钉行号。
3. 禁路径：不得只改注释不落登记（该面属对外可观察口径，须有真值源）；不得在无亲验判据下新增
   桶几何推导代码去追 C# 内部公式（属过度设计与判据伪造）；不得复活 10MHz 刻度域讨论。
4. 测试：wmetric/tests 内补一条同图灌 1 值 vs 灌 N 分散值的 size 行差分断言，
   锁「实现口径为唯一真值源」并防后续被按注释口径回改；断言随册不改生产码。

---

## 终态注记
- **合入哈希**：`3349d70`（cherry-pick 自 `620bd3f`）
- **收口形态**：
  1. 将 `wedb/wmetric/src/latency/garnet_latency_metrics.rs` 中 `write_histogram_info` 的 size 行注释订正为实口径（512 + 8 × distinct_values），并消除将不存在的 `HistogramBase.cs:431` 视为同源出处的失真表述。
  2. 在 `doc/zh/deviations.md` 顺编登记 `[§194] wmetric LATENCY HISTOGRAM size 行以 distinct_values 为真值源`，并将 `[§166b]` 文本限缩为量程/值域面推论。
  3. 在 `wedb/wmetric/tests/latency_metrics.rs` 补充单值 vs 多值差分断言，锁定实现口径。
- **门禁验证**：`cargo check -p wmetric --all-targets` 与 `cargo test -p wmetric` 全部通过。

## 主控全量复核（2026-10-01）

- **实现零改动守住**（本票红线）：`garnet_latency_metrics.rs` 的 size 行仍是
  `distinct_values() × size_of::<u64>() + FOOTPRINT_HEADER_BYTES`，`FOOTPRINT_HEADER_BYTES = 512` 未动，
  本次 src 侧 11 行全为注释体重写（原「C# `GetEstimatedFootprintInBytes` 口径：512 + 8 × 计数数组全长（`CountsArrayLength` 同源）」
  的假同源声称被删除并改指 §194）——与票面「保实现订口径、禁在无亲验判据下改实现追 C# 公式」逐字一致。
- **§194 登记与 §166b 限缩双到位**：`doc/zh/deviations.md` 新增「[§194] wmetric LATENCY HISTOGRAM size 行以
  distinct_values 为真值源」，且 §166b 条内「判据 b」原文被就地改写为把历史「size 行按数组全长推论」显式作废并回指 §194，
  即票面要求的「限缩 §166b 的 size 推论」不是靠册尾新条遮蔽，而是原条订正，无二义残留。号位无让号竞争（§193 为 wtxn 登记，§194 顺延）。
- **差分锁测**：`wmetric/tests/latency_metrics.rs` 新增单值 vs 多值差分用例，从 RESP 帧里按
  `$4\r\nsize\r\n:` 标记直读 size 整数并断言随 `distinct_values` 而非桶全长变化，把「真值源」钉成可观测断言；
  `6276416` 对该册仅 rustfmt 折行、零语义改动。
- **反证不适用**说明：本票属登记级（订口径 + 钉观测），实现未变，故无「撤修复必转红」可做；
  其有效性判据是差分测本身——若日后有人把 size 改回数组全长，`test_latency_metrics_size_row_distinct_values_differential` 必红。
