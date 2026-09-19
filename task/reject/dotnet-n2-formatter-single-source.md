.NET "N2" 定点格式化单点票：修法细节拒绝（立论与主体已落地）

来源：task/ing/dotnet-n2-formatter-single-source.md（原 next/dotnet-n2-formatter-single-source.md，
glm.design 第 7 条）。本票的「双实现且舍入语义分叉」立论成立、已按 dev 提交 b8dd92d 落地，
此处只登记被剪掉的三处修法与一处验收：它们与代码事实、BCL 事实或仓库规范不符。

1）剪掉：修法 3 的「在 wmetric 内以 pub use 重导出单点」

原文：
> 乙（wmetric）保留函数名与否按最小改动定：优先删本地实现、在 wmetric 内以 `pub use`
> 重导出单点（避免下游 :243/:253/:525/:584 全量改路径），但重导出必须是转发而非第二实现。

拒绝原因：
- 与 /Users/z/git/db/wedb/wedb/AGENTS.md「代码与风格约束」的「禁止二次导出」直接冲突。
- 省下的改动极小：下游只有三个导入点（wconn/src/metrics.rs、
  wmetric/src/latency/garnet_latency_metrics.rs、wmetric/src/info/garnet_info_metrics.rs），
  其中 INFO 面本就与 MetricsItem 同行导入 wresp::metrics，加一个名字即可；
  换成一层的 `pub use` 反而在 wmetric 的公开面上继续挂着两个不属于它的符号。
- 落地形态：调用面 `use wresp::metrics::{fmt_n2, fmt_n2_into}` 直取单点。

2）剪掉：修法 1 的「以乙为基线（NaN/inf 走 {v:.2}）」中的 `{v:.2}` 一支

原文：
> 实现取「语义更完备」的一版为基线（乙的负号 + NaN/inf 分支），并把两版差异写成用例锁定：
> 0.005/1.005 等 x.xx5 边界、负值、NaN、inf、≥1e15 大值各一条。

拒绝原因（只拒 `{v:.2}` 这个实现形态，「非有限值必须分支处理」的判断成立并保留）：
- 乙的 `write!(out, "{v:.2}")` 输出 rust 的 `nan`/`inf`，而本票要对齐的 BCL
  `ToString("N2", InvariantCulture)` 走 `NumberFormatInfo` 的 `NaNSymbol`/`PositiveInfinitySymbol`
  即 `NaN`/`Infinity`。用 `{v:.2}` 当基线等于把一处口径错误一并搬进单点。
  本仓另有 `nan`/`inf` 口径，但那是 RESP 的 double 输出面（C# `NumUtils.WriteDouble`，
  见 wedb/wnode/tests/session_output.rs 的注释），与 "N2" 不同源、不可借用。
- 不分支处理的后果实测存在：`(f64::INFINITY * 100.0) as u64` 饱和为 u64::MAX，
  会打出 "184,467,440,737,095,516.15" 这类荒谬串，故单点保留非有限值分支，只把字面量改对。
- 「0.005/1.005 等边界各一条」的用例价值核实过：1.005 的 double 值是 1.00499999999999989，
  甲的 `round` 与乙的 epsilon 推挤正是于此分叉（"1.00" vs "1.01"），已按此写用例锁定单点口径。

3）剪掉：修法 4 的「+ double 最短往返表示」与保留 epsilon 推挤的可能

原文：
> 舍入语义以 .NET `ToString("N2", InvariantCulture)` 为对齐目标（half away-from-zero +
> double 最短往返表示），单点实现须在注释点名 C# 三处锚点；禁两侧继续各自微调。

拒绝原因：
- 「half away-from-zero」采纳（即沿用甲的 `f64::round`）；「double 最短往返表示」不采纳：
  真正的 BCL 语义要先产出最短往返十进制数字串、再对数字串做舍入，rust 侧等价实现要走
  zmij 的数字串再手写一遍十进制舍入与进位，长度与机制数都超过被删的两份手写实现，
  属"为对齐 BCL 而新建第三套机制"，与 .agents/skills/rust_review 的「复杂度对标 C# 不加额外
  复杂度」相悖；三类调用面（直方图 value_at_percentile/mean 除以 10、INFO 的整数计数比）
  取不到值得该复杂度的收益。
- 反向选项（保留乙的 `+0.5000000001`）亦不采：该 epsilon 推挤只对"decimal 中点"类值生效，
  对"中点下沿 1e-10 以内"反而误进位，且它本身是魔法数字，违 wedb/AGENTS.md
  「禁止硬编码字面量：魔法数字必须定义为常量」。
- 「注释点名 C# 三处锚点」按 check.js 的映射语义做了折中：单点无 C# 对应函数（C# 是 BCL），
  锚点写成 `文件.cs:行区间` 形态。若写成 `GarnetLatencyMetrics.cs:GetPercentiles` 这类
  `文件.cs:函数名`，js/check/rustScan.js 的 CS_REF_REGEX 会把一个 BCL 内置格式化器登记成
  本函数的 C# 映射，等于用注释冒领 GetPercentiles 的实现面（该锚点本就由
  wmetric 的 get_percentiles 与 wconn 的 percentiles 持有）。

4）剪掉：验收的 cargo check --workspace --all-targets

原文：
> cargo check --workspace --all-targets（私有 target 目录）零 error 零 warning。

拒绝原因：fixloop 开发波禁全量门禁（十余个并发 worktree 撞共享 target 会出假失败并排队），
本次只跑受影响 crate：`cargo check -p wresp -p wconn -p wmetric --all-targets`
零 error 零 warning（被删符号在其余 crate 零引用，已按全仓 grep 复核）。
