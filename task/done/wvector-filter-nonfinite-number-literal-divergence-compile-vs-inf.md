锁定注记（2026-10-01 主控 r8 波，只读甄别席双侧现码亲验 + 主控现码复核）：甄别结论：通过，定级 P3（可观察应答集分叉，仅限 1e999 类极端字面量与 inf/nan 词形）。
- 现位复核（本波 09-30 tip 实测，票面行号微漂以本注记为准）：
  compiler.rs:247-261 parse_number（词法只取 `[0-9.eE-]` 前缀，:259 `.ok().filter(|v| v.is_finite())?`）；
  attribute_extractor.rs:397-402 parse_f64_exact（:401 `value.is_finite().then_some(value)`，:399 注释自陈「拒绝 inf/NaN 字面量」）；
  runner.rs:220-235 to_num Str 臂（:230 `parse::<f64>().ok().filter(is_finite)` 后 `.unwrap_or(0.0)`）；
  消费点 wnode/src/resp/vector/vector_manager_filter.rs:54-57 同位精确（两侧同形，本票不改该文件）。
- C# 侧复核：ExprCompiler.cs:248 ParseNumber、ExprRunner.cs:187 ToNum、AttributeExtractor.cs:211 ParseNumberToken
  均只走 Utf8Parser 词法 + consumed 全量校验，无有限性门；Utf8Parser 溢出臂置 PositiveInfinity 且 return true
  （dotnet/runtime Number.Parsing.cs Scale > MaxDecimalExponent 臂），且 Utf8Parser **不认** nan/inf/infinity 词形。
- 主控裁决（覆盖票面「二选一」，席按此执行，勿再自行裁向）：病灶不是「有没有有限门」，而是**门的形状错了**——
  三处都以「值域 is_finite」承担本该由「词法」承担的职责，于是溢出被误拒（分 C#）而 inf/nan 词形在
  attribute/runner 两径又被值域门顺带误拒（同 C#，但理由错位）。收口形制：
  以**单一数字词法解析函数**（比照 C# Utf8Parser 语义：只认十进制数字/小数点/指数、要求全量消费、
  溢出按 IEEE 折 ±Inf 并返回成功）替换三处口径，三处共用同一实现；
  即 compiler 与 attribute_extractor 的溢出值一律放行、runner Str 臂「归 0」第三口径随之消除，
  inf/nan 词形仍拒（词法层拒，非值域层拒）。
- 台账：本收口属 §18 修复型家族，须落 doc/zh/deviations.md 一条（新号取现册实测最大在册号 §190 之后
  首个未被「二、号位空缺清单」占用的裸号——落笔前必先 grep `§19`/`§2` 裸号，禁撞空缺位，禁钉行号）。
- 重复性：五池 grep try_compile/apply_post_filter/非有限/1e999 零命中；deviations.md 仅 §1（format_double 输出面）
  与 §80（ZSCAN 分值 inf 文本化）命中，均正交。rust 侧无既有锁测（四册 grep 1e999/is_finite/inf 零命中）。
- 禁触域（同侪主树在途编辑）：wedb/wnode/src/resp/**、wedb/wnode/src/aof/**、wedb/wnode/src/service.rs、
  wedb/wedb/src/server/replication/**、wedb/wcpr/**、wedb/wdev/**。本票改动面限定
  wedb/wvector/src/filter/**（三文件）＋ doc/zh/deviations.md ＋ wedb/wvector/tests/filter_*.rs。
- 锁测：钉字面量三处一致（`1e999` 编译/提取/运行三径同值 +Inf 照常命中；`inf`/`nan` 词形三径同拒；
  `-1e999` 折 -Inf），复用既有 filter_compiler.rs / filter_attribute_extractor.rs / filter_runner.rs 册形。

审核结论：通过（2026-09-30 甲轮48 审核席；P3。.NET 溢出语义经 dotnet/runtime 源码定谳非误报；消费点行号订正 :54-:57、ToNum Str 臂形态精确化为「C# 无门保留 Inf vs rust 有门归 0」、方案 (a) 补 runner 臂随收口一并消除）

过滤域非有限数字字面量收口未登记：C# 溢出解析为 +Inf 照常命中，rust 编译失败静默全员排除

问题分析：
1. Garnet 契约对齐：ExprCompiler.cs ParseNumber（:248-264）仅校验 Utf8Parser.TryParse 成功且全量消费——"1e999" 溢出按 .NET 语义返回 true 且值为 +Infinity（dotnet/runtime Utf8Parser.Float.cs TryParse(double) 语法成功后 value = Number.NumberToFloat(ref number); return true；Number.Parsing.cs NumberToFloat 的 Scale > MaxDecimalExponent 臂置 PositiveInfinity，1e999 的 scale 999 > 309；该语义 net8.0/net10.0 一致，double 从不因 overflow 抛 OverflowException），编译成功照常求值（`.a <= 1e999` 有限值全员命中）；ExprRunner.cs:187 ToNum 的 Str 臂对溢出文本解析成功后原样返回 ±Inf 参与比较、AttributeExtractor.cs:211 ParseNumberToken 同型，三者均无有限性门（grep IsFinite/IsInfinity/IsNaN 零命中）。
2. 工程现状：rust 三处收口且口径互异——compiler.rs:249-261 parse_number 对解析值 .filter(is_finite) 拒收（"1e999"→CompileError）、attribute_extractor.rs:396-401 parse_f64_exact 同型拒收、runner.rs:218-233 to_num Str 臂非有限归 0（对照 C# 无门保留 Inf，第三处口径漂移的准确形态）。编译失败下游 vector_manager_filter.rs:54-57 命中「编译失败 → 无结果通过」静默 return 0，零错误帧。
3. 逻辑危害确证：同一过滤表达式两侧行为可观察分叉（C# 全员通过 vs rust 全员排除且无错误回显），直接改变 VSIM 返回集；rust 拒收非有限浮点系边界防御改良，但 deviations.md 全册与 task 五池均无登记无锁测（已 grep 查重零命中），未登记改良按流程须落册。定 P3。

涉及代码：
rust 文件与函数：
wedb/wvector/src/filter/compiler.rs:249 parse_number（is_finite 拒收）
wedb/wvector/src/filter/attribute_extractor.rs:396 parse_f64_exact（同型拒收）
wedb/wvector/src/filter/runner.rs:218 to_num（Str 臂非有限归 0）
wedb/wnode/src/resp/vector/vector_manager_filter.rs:54（编译失败静默全员排除消费点）

对应 c# 文件与函数：
libs/server/Resp/Vector/ExprCompiler.cs:248 ParseNumber（溢出→+Inf 编译成功）
libs/server/Resp/Vector/ExprRunner.cs ToNum（同型无有限性门）
libs/server/Resp/Vector/AttributeExtractor.cs ParseNumberToken（同型无有限性门）

精炼执行方案：
1. 二选一先裁决：(a) 按 §18 修复型家族落册登记 rust 拒收现形并补编译期/提取期非有限拒收锁测（钉 "1e999" 编译失败+提取期拒收），runner to_num Str 臂当前归 0 口径与两向裁决均不兼容须随收口一并消除；(b) 对齐 C# 接受 ±Inf 走求值
2. 两向均须消除「编译拒收/提取拒收/运行归 0」三处口径漂移，收口为单一机制

终态注记：
- 合入收口形态：以单一数字词法解析函数 parse_f64_exact（比照 C# Utf8Parser 语义：只认十进制数字/小数点/指数，全量消费，溢出折 ±Inf）收口 compiler、attribute_extractor、runner 三处口径；放行编译期与提取期极端字面量（如 1e999 折 +Inf），消除 runner Str 臂非有限归 0 口径；词法层统一拒收 inf/nan 词形。在册台账登记入 doc/zh/deviations.md [§191]。补齐三处字面量一致性锁测。
- 状态：已收口归档。

