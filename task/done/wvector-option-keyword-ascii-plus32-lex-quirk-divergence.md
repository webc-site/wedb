终态注记：合入 0113e75，裁修复型收窄分叉落册 deviations §187（向量选项关键字 ASCII 折叠字母域收窄，同谱 §18/§32）+ equals_ignore_case 文档注释认记锚 + 钉形测试 vector_lex_ascii_plus32_quirk.rs 三用例，行为零改动。执行订正：甄别面所拟 XPREXX 活病形逐位复核不成立（'X'-32=0x38≠'Q'=0x51，C# 亦拒），XPREQ8 真活病形为 XPREQX（尾位 'X'-32=0x38 命中 '8'），判据按验算成立形落册、XPREXX 以拒收词面钉形在测见证。

甄别结论：通过（2026-09-29 主控甄别，定级 P3——C# AsciiUtils.cs 二参版 EqualsUpperCaseSpanIgnoringCase 逐字节 b1==b2||b1-32==b2 无字母域限制（Release 无 Debug.Assert），消费面 RespServerSessionVectors.cs:232 Q8/:392 L2/:801 FILTER-EF/:128-149 XU8/XB8/XI8 均二参版；rust wbase/src/ascii.rs eq_ascii_case 走 eq_ignore_ascii_case 仅字母位折叠严格收窄，消费链 wresp/options.rs 全在位。deviations §184 在册但登记工作未做。甄别订正：票面第四例 XUX8 算术不成立（4 字节对 3 字节长度即拒，双侧一致），正确钉形 XUX/XBX/XIX（尾位 X-32=0x38 命中）；执行步骤 3 把 XPREQ8 列入判据清单（C# :256 二参版含数字位 8，XPREXX 为同族活病形）。修复：登记 §修复型家族+ascii 折叠语义订正+钉测）

审核结论：通过（2026-09-29 甲轮35-B，P3 级）。C# 二参版全字节位 +32 偏移仅 Debug.Assert（Release 无效）、四类病形算术逐一验真（'X'-32='8' 等）、rust eq_ascii_case 仅字母位折叠严格收窄、§18 修复型家族先例在册裁「登记不回改」方向正确。无修正意见。

原票面：
向量命令选项关键字比较 C# 加 32 偏移怪癖词法面 rust 收窄分叉未登记

问题分析：
1. Garnet 契约对齐：C# 二参版 EqualsUpperCaseSpanIgnoringCase 对全部字节位执行 `b1 == b2 || b1 - 32 == b2`，非字母字节同样适用加 32 偏移——任意 b1 = b2 + 32 的字节都命中。据此 Q8 尾位 '8'(0x38) 可被 'X'(0x58) 命中（VADD 选项 "QX" 识别为 Q8 量化器）、L2 尾位 '2'(0x32) 可被 'R'(0x52) 命中（XDISTANCE_METRIC 值 "LR" 识别为 L2）、FILTER-EF 的 '-'(0x2D) 可被 'M'(0x4D) 命中（VSIM 选项 "FILTERMEF" 识别为 FILTER-EF）、XU8/XB8/XI8 尾位同理（"XUX8" 等识别通过）。三参版（allowNonAlphabeticChars: true）用于 XDISTANCE_METRIC/XNOQUANT_*/XBIN_* 关键字本身，非字母位要求精确相等，但其字母位偏移路径与二参版一致。
2. 工程现状确证：rust 统一走 wbase::ascii::eq_ascii_case（u8::eq_ignore_ascii_case，仅字母位折叠），wresp::options::equals_ignore_case 与向量会话层 lookup/cur.at 全部消费之；上述 "QX"/"LR"/"FILTERMEF"/"XUX8" 四类形在 rust 侧分别落 "ERR invalid option after element"/"ERR invalid XDISTANCE_METRIC"/"Unknown option"/"ERR invalid vector specification"。rust 严格收窄且未在代码注释或 deviations 台账认记，属未登记的词法契约分叉。
3. 逻辑危害确证：按 C# 行为对拍的 parity 断言（如 12 命令全帧等值测试扩到表外词形）会误报回归；跨端客户端兼容上 C# 接受的病形在 rust 被拒，帧面分叉可观测。危害等级低（仅垃圾词形），但按「线索即登记」纪律应钉死裁决方向，防后续以 C# 为真值回改。

涉及代码：
rust 文件与函数：
wedb/wbase/src/ascii.rs:eq_ascii_case
wedb/wresp/src/options.rs:equals_ignore_case
wedb/wnode/src/resp/vector/resp_server_session_vectors.rs:lookup、Cur::at、VALUE_KINDS、QUANT_OPTS、METRIC_OPTS（全量消费点）

对应 c# 文件与函数：
libs/common/AsciiUtils.cs:EqualsUpperCaseSpanIgnoringCase（二参版 :69-90、三参版 :95-122）
libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVADD 与 NetworkVSIM 选项循环（二参版关键字比较消费面）

精炼执行方案：
1. 裁 rust 现状为修复型收窄分叉（上游比较原语把非字母位卷入加 32 偏移属缺陷，同 §18/§32 前导零收口谱系），登记 deviations 新节并在 wresp::options::equals_ignore_case 文档注释补认记锚；
2. 补四类钉形测试：VADD "QX"→错误帧、XDISTANCE_METRIC "LR"→错误帧、VSIM "FILTERMEF"→Unknown option、VADD "XUX8"→invalid vector specification，钉死严禁按 C# 病形回改；
3. 顺带核 wresp 其余 equals_ignore_case 消费面（NX/XX/GT/LT 等纯字母词形不受此怪癖影响，确认无第二处需认记）。
