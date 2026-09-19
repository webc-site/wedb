.NET "N2" 定点格式化器双实现且舍入语义分叉：wconn 私有 format_n2 与 wmetric pub fmt_n2

来源：glm.design 第 7 条（分拣判定成立且待做）。取证基线：主仓 HEAD 3c4f74a（认领时的代码实况）。

甄别结论：立论成立并已落地（dev 提交 b8dd92d）。修法的三处细节经对照代码事实与仓库规范后修订，
被剪掉的原文与拒绝原因见 task/reject/dotnet-n2-formatter-single-source.md。

现状（立论部分，落地前）
- 甲：/Users/z/git/db/wedb/wedb/wconn/src/metrics.rs `fn format_n2(v: f64) -> String`（私有），
  `(v * 100.0).round() as u64`，无 NaN/inf 分支、无负号分支；负值与非有限值经 f64→u64
  饱和转换为 0（符号丢失）。消费面为同文件百分位与 mean 两处。
- 乙：/Users/z/git/db/wedb/wedb/wmetric/src/latency/garnet_latency_metrics.rs
  `pub fn fmt_n2_into` + `pub fn fmt_n2`，非有限值走 `{v:.2}`、取绝对值后带负号、
  `(abs_v * 100.0 + 0.5000000001).floor() as u64`（epsilon 推挤的 half-up）。
  消费面同文件两处，以及 /Users/z/git/db/wedb/wmetric/src/info/garnet_info_metrics.rs
  的 garnet_hit_rate（INFO 输出面）。
- 分叉实证（IEEE754 复核，同一输入两版产出不同字符串）：
  1.005（double 值 1.00499999999999989）甲 "1.00" / 乙 "1.01"；
  -1234.56 甲 "0.00"（符号丢失）/ 乙 "-1,234.56"；NaN 甲 "0.00" / 乙 "NaN"；
  +inf 甲 "0.00" / 乙 "inf"（乙对 inf 亦不脱饱和，只是 `{:.2}` 恰好接住）。
- C# 参考成立：三处指标输出零手写格式化，全走 BCL
  `double.ToString("N2", CultureInfo.InvariantCulture)`
  （garnet/libs/client/GarnetClientMetrics.cs:35-41、
  garnet/libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:80-86、
  garnet/libs/server/Metrics/Info/GarnetInfoMetrics.cs:203），rust 两份手写属转写自造重复面。

落地（收敛后的单一来源）
- 单点：/Users/z/git/db/wedb/wedb/wresp/src/metrics/n2_format.rs
  `pub fn fmt_n2_into(v: f64, out: &mut String)` + `pub fn fmt_n2(v: f64) -> String`（薄封装，
  非第二实现），经 /Users/z/git/db/wedb/wedb/wresp/src/metrics/mod.rs 与 MetricsItem 同层导出。
  位置沿用票据判断：wconn 与 wmetric 均已依赖 wresp，wresp::metrics 已是 client/server 共引的
  指标单点（MetricsItem/InfoMetricsType），零新增依赖边、零新模块。
- 舍入口径：`(v.abs() * 100.0).round() as u64`，半值远离零（沿用甲的 `round`，删乙的
  `+0.5000000001` 魔法数）；符号按 `is_sign_negative`（-0.0 → "-0.00"，同 BCL）；
  非有限值输出 BCL `NumberFormatInfo` 的 NaN/Infinity/-Infinity 字面量
  （不再是乙的 `{v:.2}` 的 nan/inf，也不能不判：饱和转换会把 inf 打成 u64::MAX 的荒谬串）。
- 删除：甲乙两处本地实现与各自 N2 断言（wconn 的 n2_invariant_formatting、
  wmetric 的 test_fmt_n2_various_numbers）全部删除，期望值并入单点侧
  n2_format.rs tests（n2_formatting / n2_sign_and_non_finite），并补 0.125 精确中点、
  1.005 十进制中点、-0.0、1e15 分组裕量、NaN/±inf 各一条，锁定唯一口径。
- 调用面直接 use 单点，不做二次导出：
  /Users/z/git/db/wedb/wedb/wconn/src/metrics.rs:26、
  /Users/z/git/db/wedb/wedb/wmetric/src/latency/garnet_latency_metrics.rs:5-8、
  /Users/z/git/db/wedb/wedb/wmetric/src/info/garnet_info_metrics.rs:4。
- 输出不变性：非负有限值路径逐值复核与两侧旧实现全等（既有断言 "1,228.80"、"1,234.05"、
  "1,234,567.89"、"1,000.00"、"987.65" 等），只有原已分叉的三类输入（负值、非有限值、
  十进制中点）改取单点口径，属本票要消除的分叉本身。

协调
- 未触碰 wresp 协议面（RespWriter/命令分派/cmd_strings 的 RESP_RETURN_VAL_N2 与本票无关，
  那是 RESP 的 -2 回复常量，非 .NET N2）。

验收（本轮实测）
- grep 唯一性：`fn format_n2` / `fn fmt_n2_into` / `fn fmt_n2` 定义全仓仅
  wedb/wresp/src/metrics/n2_format.rs 一处，其余命中皆为调用与导出。
- cargo check -p wresp -p wconn -p wmetric --all-targets 零 error 零 warning
  （worktree 私有 target：/tmp/fork/dotnet-n2-formatter-single-source/target）。
