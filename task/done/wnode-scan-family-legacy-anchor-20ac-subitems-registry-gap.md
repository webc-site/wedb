终态注记: 已合入 main（commit: 9aa4a4f）。收口形态：doc/zh/deviations.md 册尾顺编落册 §188（COUNT 负/零钳 0 且 max(1)）与 §189（分层游标不对齐校验判 false），订正 array_commands.rs:153 与 scan.rs:612 悬空注锚为 §188 与 §189，全仓旧 20 条 a/c 注锚清零，零行为代码改动。

甄别结论:通过(P3,2026-09-30 现码复核,双侧锚亲验成立,五池无重复,deviations 无同面在册裁决)

审核结论：通过（2026-09-30 甲轮45-F，P3 级）。array_commands COUNT 负/零钳 0 与 whlog 游标回退对齐两处注释引「第 20 条 a/c」系旧册重建前遗留错指/悬空事实确证（与已收口的 §185 同谱）。执行席遵照：deviations.md 册尾顺编落册两目裁决（a 目修复型、c 目收窄型），两处代码注释订正指新节号，零行为代码改动。

复核席注记（2026-09-30 独立复核同向通过）：三处「第 20 条」注锚原文、现册 §20 TYPE 条占用、册与五池双零命中、§185 五处注锚清单未含此两处，逐点实证（C# 锚 ArrayCommands.cs:291-303 COUNT 臂无钳制、AllocatorScan.cs:268 count<=0 未扫描即中断、SpanByteScanIterator.cs:72-90 SnapCursorToLogicalAddress 均核实）。执行席两点订正：一、票面 C# 路径 garnet/libs/server/Resp/BasicCommands/ArrayCommands.cs 有误，实为 garnet/libs/server/Resp/ArrayCommands.cs（无 BasicCommands 子目录）；二、落册新节符号锚 rust 侧书现名 parse_scan_filter（array_commands.rs:108），票面「parse_type_filter 邻臂」系旧名滞后；scan_cursor 执行层 max(1) 面（array_key_iteration_functions.rs:262）可作 a 目补充符号锚。取号册尾顺编 §188 起；b 引用（array_commands.rs:169）吻合现册勿动。

原票面：
扫描族旧册「第 20 条」a/c 两子分目判据重建后散佚：array_commands COUNT 负/零钳 0 与 whlog 游标回退对齐两处注锚悬空错指，在役裁决登记空洞

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）：
a 目（COUNT 负/零钳 0）：C# SCAN COUNT 解析对负值/零值无防御——底层扫描 acceptedCount(0) >= count(<=0) 立即返回 true 未扫描即中断，外层无键匹配时硬写游标 0（garnet/libs/server/Resp/BasicCommands/ArrayCommands.cs SCAN COUNT 词元解析段，rust 侧 array_commands.rs:146-156 注释自陈对拍）。rust 侧做修复性规整：负/零钳 0 且扫描层 max(1) 保证至少扫 1 条。
c 目（游标回退对齐）：C# 对落在记录中间的游标自页首步进回退对齐后续扫（libs/storage/Tsavorite/cs/src/core/Allocator/SpanByteScanIterator.cs:SnapCursorToLogicalAddress，whlog/src/scan.rs:605-613 注释自陈锚）；rust 复用 scan_iter 步进链做纯校验，不对齐一律判 false，由 wnode scan_cursor 终结遍历回 (0, 空)，漏扫方向可容忍。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）：2026-09-28 裁决册重建后 §20 现为「TYPE 命令对空串及表外值归未知回空（含 a/b 分目）」，符号锚仅挂 array_commands.rs parse_type_filter。全仓「第 20 条」引用残留三处：wedb/wnode/src/resp/array_commands.rs:153「第 20 条 a」——其注释判据面为 COUNT 负/零钳 0，与现册 §20 a 分目「TYPE 命令解析已知类型」判据面不吻合，系错指；wedb/wnode/src/resp/array_commands.rs:169「第 20 条 b」空串 TYPE 空集——与现册 §20 b 分目吻合，不悬空（勿动）；wedb/whlog/src/scan.rs:612「第 20 条 c」游标回退对齐——现册 §20 无 c 分目，全册（含号位空缺清单）无游标对齐裁决，悬空坐实。两条在役裁决（COUNT 负零钳 0 修复形、游标不对齐收窄形）在 task/done|reject|issue|todo|ing 五池票体与 deviations 现册均无判据正文（与已收口的 §185「第 20 条 d」count i64 加宽同族散佚，§185 落册时票面五处清单未含此两处）。
3. 逻辑危害确证：非运行时行为缺陷（两裁决行为正确且有注释自陈）；危害在裁决单源断链——后续审查轮按锚查册落空，可能误报未登记新分叉，或以「册无此裁决」为由按 C# 形回改：COUNT 面回改复活「未扫描即中断+硬写游标 0」缺陷，游标面回改须为回退对齐重写三区分派逻辑。与 §185/§186/§187 悬空锚收口谱系同族，定级 P3。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/array_commands.rs:SCAN COUNT 词元解析臂（:153 注锚「第 20 条 a」错指；:169 b 引用吻合勿动）
wedb/whlog/src/scan.rs:validate_cursor（:605-613 注锚「第 20 条 c」悬空；C# 锚 SnapCursorToLogicalAddress 已附注）
对应 c# 文件与函数：
garnet/libs/server/Resp/BasicCommands/ArrayCommands.cs:SCAN COUNT 词元解析（负/零无防御面）
libs/storage/Tsavorite/cs/src/core/Allocator/SpanByteScanIterator.cs:SnapCursorToLogicalAddress（游标回退对齐）

精炼执行方案：
1. deviations.md 册尾顺编落册（正文现编至 §187，取号避开号位空缺清单与伪号面占用位）：a 目判据=SCAN COUNT 负/零钳 0 修复性规整（负值钳 0、扫描层 max(1) 至少扫 1 条，不复刻 C# 未扫描即中断硬写游 0 形，§18 修复型家族同谱）；c 目判据=分层扫描游标落记录中间不对齐回退、纯校验一律判 false 由调用方终结回 (0, 空)（Redis SCAN 本无快照保证，漏扫可容忍，免去三区分派重写）；两目可并一节分目或分两节，来源挂本票；符号锚 parse_type_filter 邻臂、validate_cursor。
2. 两处注释锚订正为新节号（严禁动行为代码）；b 引用（:169）吻合现册不动。
3. 测试验证点：涉及文件定向测试原样通过（零行为改动）；grep 全仓「第 20 条 a」「第 20 条 c」清零。
