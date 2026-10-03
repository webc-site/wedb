终态注记：已合入 dev（哈希 ec3fc1d）
收口形态：
- wedb/wext_json/src/json_path/parser.rs：移除 .. 遇 * 臂的 !query 门控，主路径与 query 上下文均统一归一至 PathFilter::Scan { name: None }；create_path_filter 增加 member=="*" 归 None 兜底，与 C# JsonPath.cs CreatePathFilter 契约逐点对齐。
- wedb/wext_json/tests/json_path_semantic_tests.rs：补齐 @..* 锁测用例 query_context_recursive_scan_wildcard_parses_and_evaluates，覆盖对象根下钻、组合路径与嵌套数组。

审核结论：通过
锚点订正：C# member=="*" 归 null 实位为 JsonPath.cs:237-242（'.' 分支）、:284-289（结尾段）、:198-203（'['/'(' 断点处，共三处非票面两处）；ScanFilter(null) 全量下钻消费点为 ScanFilter.cs:61-64（数组元素）与 :72-75（对象属性值）。rust query `..` else 臂实位 parser.rs:154-157（parse_member_name 断点集 :203-215 不含 *，`*` 被整体吞为成员名），主路径 None 专臂实位 parser.rs:151-153（带 !query 限定）；filter 消费实位 filter.rs:176-180 与 :302-318（判定行 :312 字面键匹配）。src 全域无 Some("*") 通配归一（rg 零命中）。执行方案维持票面并明确落点：去掉 parser.rs:151 专臂 !query 限定使主路径与 query 同归一 Scan{None}，补 query 内 @..* 对象根全量下钻锁测（先例 json_path_semantic_tests.rs:345-364），对照 C# Garnet.test JSONPath query 族用例。

JSONPath query 上下文 @..* 产出字面键匹配，偏离 C# "*" 归 null 全量下钻

问题分析：
1. Garnet 契约对齐：C# ParsePath 对 member=="*" 两处归 null（garnet/modules/GarnetJSON/JSONPath/JsonPath.cs:226-235 与 :299-310），ScanFilter(null) 语义为数组元素+对象属性值全量下钻。
2. 工程现状确证：rust parser.rs query `..` else 臂 parse_member_name 吞 `*` 产出 Scan{Some("*")}（parser.rs:126-134 一带），filter.rs:176-182 Scan 臂与 :302 scan_filter_walk 按字面 "*" 键匹配；主路径 `..*` 有 None 专臂不受影响。
3. 危害确证：`$.a..@..*.b` 类查询在对象容器上零命中（C# 命中全部属性值），查询静默漏配对；deviations.md 未登记该分歧。

涉及代码：
rust 文件与函数：
wedb/wext_json/src/json_path/parser.rs:126（query `..` else 臂）
wedb/wext_json/src/json_path/filter.rs:176（Scan 臂）
对应 c# 文件与函数：
garnet/modules/GarnetJSON/JSONPath/JsonPath.cs:226（ParsePath member=="*" 归 null）

精炼执行方案：
1. query `..` 臂遇 `*` 产 Scan{None}（与主路径 `..*` 专臂同归一）
2. 补 query 内 `@..*` 锁测断言（对象根全量下钻）
3. 测试验证点：对照 C# Garnet.test JSONPath query 族用例
