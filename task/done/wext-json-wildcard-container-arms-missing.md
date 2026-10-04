甄别结论：通过（甄别席 J4，2026-09-27，定级 P2——通配符三交错臂缺失，GET $[*] 打对象根静默回空，标准用法丢命中）。三交错臂 rust 现码逐锚亲验——Field{None}（filter.rs:98-101）仅 as_object、ArrayIndex{None}（:114-133）as_array else 恒空、ScanArrayIndex（:174-201）cb 仅数组产出且 scan_descendants（:268-279）纯下钻；同构缺失在 mutate_recursive（path.rs:613-628/:638-655/:736-765）与 delete_recursive（:149-153/:174-177/:237-245/:271-286/:297-301/:368-392）亲验在位；Scan{None} 双容器对偶先例（scan_filter_walk :285-301、delete_scan :565-590）证明双臂形是仓内既定形态而非外来设计。C# 三 filter 交错臂逐锚亲验：FieldFilter.cs:59-61/:135-141、ArrayIndexFilter.cs:45-47/:115-121、ScanArrayIndexFilter.cs:59-61/:71-73 全部在位。端到端：GET $[*] 打对象根 rust 回 []、SET 同形落 :341-343 RESP_WRONG_STATIC_PATH，契约分叉实锤。查重：§15a-f 与「臂级澄记」（deviations :174-176，仅 MatchTokens/In）均不涉交错臂，四池无同轴票。现码勘误：票面 SET 门路径 wnode/src/resp/objects/json_object.rs 确系误写（全仓唯一 wext_json/src/json_object.rs），SET 门实际在 :341-343。派沙箱席 c01e。

审核结论：通过（P2 维持）

三臂 rust 现码与 C# 三 filter 交错臂逐锚亲验属实：filter.rs Field{None}（:98-101）仅 as_object、ArrayIndex{None}（:114-117）as_array else 恒空、ScanArrayIndex（:174-201）cb 仅 as_array 产出且 scan_descendants（:268-279）纯下钻；C# 侧 FieldFilter.cs:59-61/:135-141（Name is null 且 JsonArray → 产出）、ArrayIndexFilter.cs:45-47/:115-121（Index null 且 JsonObject → 产出 Values）、ScanArrayIndexFilter.cs:59-74（Index is null 时数组元素与对象属性值双 yield）三处交错臂确凿。parser（create_path_filter :136-142 / parse_indexer :194+）仅做映射不补臂，反证排查无别处补齐。端到端推演：JSON.GET k '$[*]'（对象根 {"a":1,"b":2}）C# 经 GarnetJsonObject.cs:372 Evaluate 命中 Values 回 [1,2]，rust try_get_to_writer（json_object.rs:207-213）空命中回 []；JSON.SET 同形 C# 全元素替换回 +OK，rust evaluate 空 → is_static_path false → RESP_WRONG_STATIC_PATH（json_object.rs:341-348，票面 :341-347 基本对位）——契约分叉实锤。危害面补记：rust 侧 wext_json 实注册 19+ 条 JSON 命令（dispatch.rs:104+，DEL/FORGET/NUMINCRBY/TOGGLE/ARRINSERT 等）同走 evaluate/mutate 引擎同受此缺失，票面「GET/SET 唯二」系 C# 注册面（JsonModule.cs:32-33 属实），rust 危害面比票面更大，P2 保守不虚。查重：deviations「臂级澄记」（:174 起）仅覆盖 MatchTokens 容器臂与 In 不可达，§15a-f（NX/XX、$ 根条件写、正则崩溃、键转义、解码防御帧、选项收尾）均不涉交错臂；task issue/done/ing/reject 四池无同轴票。定级 P2：通配标准用法静默丢命中丢目标，契约分叉面系统性，非崩溃非数据损坏。票面唯一硬伤：SET 门路径误写 wedb/wnode/src/resp/objects/json_object.rs，实为 wedb/wext_json/src/json_object.rs，执行时按本订正落锚。

问题分析：

问题分析：
1 Garnet 契约对齐：C# JSONPath 三容器交错臂齐全——FieldFilter.cs:59-61 与 :135（Name is null 且 current is JsonArray → 产出数组元素，即 .* 打数组根产出全部元素）；ArrayIndexFilter.cs:45-47（Index is null 且 current is JsonObject → 产出对象全部值，即 [*] 打对象根）；ScanArrayIndexFilter.cs:59-72（..[*] 下钻期对对象产出每个属性值）。
2 工程现状确证：wedb/wext_json/src/json_path/filter.rs 三臂齐缺：Field{None} 臂（:98-101）仅 as_object 无数组臂；ArrayIndex{None} 臂（:115-117）as_array else 恒空无对象臂；ScanArrayIndex（:174-201 经 scan_descendants :268-279）仅 as_array 产出、对象仅下钻不产出。变异/删除双引擎同构缺失：path.rs:613-628（mutate_recursive Field 臂仅 as_object）、:736-765（ScanArrayIndex 变异臂）、:271-286/:368-392（delete_recursive 同）。命中链：JSON.GET k '$.*'（数组根）C# 回 [1,2,3] rust 回 []；JSON.GET k '$[*]'（对象根）C# 回 [1,2] rust 回 []；JSON.SET k '$[*]' 5（对象根）C# 全元素替换回 +OK，rust evaluate 空命中回 wrong static path 错误帧。GET/SET 是 garnet 注册的唯二命令（JsonModule.cs），对位面即受击；两侧测试镜像只锁 .*-on-object 与 [*]-on-array 同向形，交错臂双侧零锁。
3 逻辑危害确证：通配符标准用法在交错容器形态静默丢命中（GET 丢结果、SET/扩展命令丢目标），C# 同输入行为迥异，契约分叉非既定改良；deviations「臂级澄记」仅覆盖 MatchTokens 容器臂与 In 不可达，不涉本条。

涉及代码：
rust 文件与函数：
wedb/wext_json/src/json_path/filter.rs:Field{None}（:98-101）、ArrayIndex{None}（:115-117）、ScanArrayIndex（:174-201、:268-279）
wedb/wext_json/src/json_path/path.rs:mutate_recursive Field 臂（:613-628）、ScanArrayIndex 变异臂（:736-765）、delete_recursive（:271-286、:368-392）
wedb/wnode/src/resp/objects/json_object.rs:SET 静态路径门（:341-347）

对应 c# 文件与函数：
garnet/modules/GarnetJSON/JSONPath/FieldFilter.cs:ExecuteFilter（:59-61）与 ExecuteFilterMultiple（:135）
garnet/modules/GarnetJSON/JSONPath/ArrayIndexFilter.cs（:45-47）
garnet/modules/GarnetJSON/JSONPath/ScanArrayIndexFilter.cs（:59-72）

精炼执行方案：
1 filter.rs 三臂补交错容器臂（Field{None} 补数组产出、ArrayIndex{None} 补对象值产出、ScanArrayIndex 下钻期对象产出属性值），形态对齐 C# 三 filter；path.rs 变异/删除双引擎同构补臂
2 锁测：'$.*' 打数组根、'$[*]' 打对象根、'$..[*]' 混合嵌套三案 GET/SET/DELETE 双侧对拍锁死（含 rust 现红案转绿）

审核裁定执行方案（供 task/fix.md 消费）：

1 补臂基准（filter.rs execute_filter）：
  a Field{None} 臂（:98-101）：as_object 无产出时补 as_array 臂产出全部元素（对标 FieldFilter.cs:59-61 与 :135-141，仅 Name is null 时成立，Some(name) 不得扩）
  b ArrayIndex{None} 臂（:114-133）：数组臂之外补 as_object 臂产出全部属性值（对标 ArrayIndexFilter.cs:45-47 与 :115-121）；Some(idx) 臂严禁扩对象（C# TryGetTokenIndex 仅认数组）
  c ScanArrayIndex None 臂（:174-201）：cb 内补对象属性值产出（对标 ScanArrayIndexFilter.cs:71-74，yield 严格在 Index is null 门内，Some(idx) 时对象仅下钻不产出，与现码 Some 臂行为一致）；scan_descendants（:268-279）本体不动
2 同构补臂（path.rs 双引擎）：
  a mutate_recursive：Field{None} 臂（:613-628）补数组元素下传 filter_idx+1（对标 C# Set 两阶段 Evaluate 命中数组根）；ArrayIndex{None} 臂（:638-655）补对象属性值下传；ScanArrayIndex{None} 臂（:736-765）补对象属性值命中下传 filter_idx+1 并遵守既有终结不重入纪律（对标 arr 臂 :754-759 的 continue 形）
  b delete_recursive：Field{None}（终结 :149-152、中间层 :279-285）、ArrayIndex{None}（终结 :174-177、中间层 :297-302）、ScanArrayIndex{None}（终结 :237-245 经 delete_scan_arrays :503-519、中间层 :368-392）按既有 Scan{None} 双容器对偶口径（delete_scan :573-589 对象清值与数组清元素并存先例）补交错臂：Field{None} 数组根清数组元素、ArrayIndex{None} 对象根删全部键值、ScanArrayIndex{None} 下钻期对象属性值纳入删除；删除引擎 C# 无对位命令（GarnetJSON 仅 SET/GET），按 rust 自有引擎容器对偶自洽收口，不引 C# 缺臂
3 SET 门零改动确认（wedb/wext_json/src/json_object.rs:341-394，票面路径误写 wnode/src/resp/objects/ 已订正）：补臂后 '$[*]' 打对象根 evaluate 非空走 :381-394 替换臂回 +OK，与 C# GarnetJsonObject.cs:372 Evaluate 同形；:391-393 replaced==0 防御锁不得被新增合法命中误触，锁测覆盖
4 危害面补测（超 C# 对位面）：JSON.DEL k '$[*]'（对象根）补臂后删全部键值回 :2；JSON.NUMINCRBY/TOGGLE/ARRINSERT 族走同引擎抽样一命令锁交错命中形
5 锁测闭环：
  a GET：'$.*' 打数组根现红 [] → [1,2,3]；'$[*]' 打对象根现红 [] → [1,2]；'$..[*]' 打 {"x":{"a":1},"y":[2]} → [{"a":1},1,[2],2]（先序对拍 C#）
  b SET：'$[*]' 打对象根现红 wrong static path → +OK 且 {"a":5,"b":5}
  c 回归钉防误伤：.* 打对象根、[*] 打数组根、Some(idx)/Some(name) 全部既有形不变
  d 落 wedb/wext_json/tests/ 与 C# 对拍镜像互指

收口记录（收票席 R3 批次，2026-09-28）：合入 6bb09215（验货 12d69bf0）。收口形态=三交错臂补全（FieldFilter/ArrayIndex/ScanArrayIndex 的 None 门对象↔数组双向产出，对标 C# 枚举器逐行同构；ScanArrayIndex None 并入 scan_filter_walk 单机制，Some 臂不扩容器）+ path.rs mutate/delete 终结与中间层九臂；命令级 GET/SET/DEL 与执行/变更/删除三层级共 23 锁测全绿，wext_json 全测面（19 文件）复跑零劣化。无偏差登记（向 C# 收敛）。风险备案：终结 $..[*] 打对象根遮蔽深层系 Scan{None} 既有纪律必然同构，C# 两阶段替换下有效树一致。
