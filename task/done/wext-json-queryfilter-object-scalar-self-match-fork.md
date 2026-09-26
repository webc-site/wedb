甄别结论：通过（甄别席 zc-fix-r25-乙，2026-09-26）定级 P1
核验记录：C# 亲验——QueryFilter.cs:34-63 ExecuteFilter 三臂（JsonArray 遍历元素/JsonObject 遍历属性值/标量无臂零产出）现树逐行亲见；GarnetJsonObject.cs:Set :374-383 result.Length==0 且非静态路径 → RESP_WRONG_STATIC_PATH 亲见。rust 亲验——filter.rs:229-241 Query 臂仍两臂（as_array 遍历+else 对 current 自身求命中产出自身）；path.rs:442-444/:825-827 delete/mutate 中间臂标量自匹配兜底仍在位（审核订正「三引擎未单语义」现码坐实）；json_object.rs set 恒回 Success 忽略替换计数；tests/json_query_root_coercion.rs:126-137 反向锁 multiple_queries_chained_filter 断言 [2,3,5,6] 且注释误记「.NET 内置实现/标量命中自保留」亲见——全链缺陷无合入灭失。查重：deviations §15 臂级澄记只管 MatchTokens 容器臂与 In 臂，不涉 ExecuteFilter 节点三臂，零撞面；四池零同轴。架构：三引擎单语义收口+防御错误臂+反向锁改写为 C# 原断言，符合测试对标纪律，五步方案全可落点。格式：纯文本、双侧齐全。定级 P1：SET 谎报成功零写入（可达面仅过滤器对象/标量上下文、C# 同形本为错误帧即非合法写入形态，不升 P0）。

审核结论：通过（修复级，本批最高优先；filter.rs 两臂自匹配、path.rs 三臂 C# 语义、set 判命中与 replace 计数被弃、C# QueryFilter 三臂与 MultipleQueries 回 0 断言、反向锁双误记注释全部亲验属实。局部订正：path.rs 中间臂 :442-444/:825-827 尚存标量自匹配兜底，议题「delete/mutate 均按 C# 语义」局部言过，须一并收口；GarnetJsonObject.cs 锚实为 :374-383）

审核裁定执行方案（供 fix 直接消费，替代文末原方案）：
1. filter.rs Query 臂改三臂对齐 C#：数组遍历元素、对象遍历属性值、标量返回空 Vec。
2. 同步删除 path.rs:442-444/:825-827 标量自匹配兜底，三引擎（evaluate/delete/mutate）真正单语义收口。
3. json_object.rs:set 增设 count==0 防御分支回 RESP_WRONG_STATIC_PATH 同向帧（step1 后 evaluate 空+is_static_path 判非静态已自然落 :306-308，此臂为防御锁），杜绝 OK+零写。
4. 改写 multiple_queries_chained_filter 为 C# 原断言（回 0 条），订正 :127-129 「.NET 内置实现/标量命中自保留」双误记注释。
5. 新增锁：$.o[?(@.a==1)] 于 {"o":{"a":1}} GET 回 "[]"、SET 回 wrong static path、DEL 回 0；$.o[?(@>0)] 于 {"o":5} GET/DEL/变异三面同空。
6. 回归 json_path_execute_tests/semantic_tests 全绿（皆数组根上下文，预期零波及）。

wext_json QueryFilter 对象/标量上下文臂自匹配，与 C# 值遍历语义分叉且引发 SET 静默 no-op

问题分析：
1. Garnet 契约对齐：C# QueryFilter.cs:ExecuteFilter（单节点与 IEnumerable 两重载，
   modules/GarnetJSON/JSONPath/QueryFilter.cs:34-92）对当前节点分三臂：JsonArray
   遍历元素逐个过谓词；JsonObject 遍历属性值逐个过谓词；标量节点恒零产出。C# 测试
   JsonPathExecuteTests.cs:941-949 MultipleQueries 锁定
   `[?(@ <> 1)][?(@ <> 4)][?(@ < 7)]` 作用于 [1..9] 引擎级回 0 条（第二个过滤器
   收到的标量被整体丢弃），该测试使用 GarnetJSON 自有 JsonExtensions.SelectNodes
   （JsonExtensions.cs:67-72，.NET BCL 无 JsonNode.SelectNodes）。
2. 工程现状确证：rust wedb/wext_json/src/json_path/filter.rs:229-241 Query 求值臂
   仅两臂：数组遍历元素；其余（对象与标量）对 current 自身求命中并产出自身。对
   `$.o[?(@.a==1)]` 作用于 {"o":{"a":1}}：C# 遍历 {"a":1} 的属性值 1（标量谓词
   不命中）得空集，rust 自匹配得 [{"a":1}]。且同仓 delete_recursive / mutate_recursive
   的 Query 中间臂（path.rs:429-445/:812-828）与终结臂（path.rs:193-219）均按
   C# 值遍历语义实现，与求值臂 filter.rs 互为两套语义：同一路径 GET 可见命中而
   DEL/变异零命中，违反多路径行为同构。
3. 逻辑危害确证：对位命令 JSON.SET 直接受害。json_object.rs:set 先用 evaluate
   判命中（:299），非空即走 replace_matches 并恒回 Success（:341-348，替换计数被
   忽略）；而 replace_matches 内部 mutate 走值遍历臂，谓词在值上不命中则零替换。
   结果 `JSON.SET k $.o[?(@.a==1)] 42` 于 {"o":{"a":1}}：C# 回错误帧
   "Err wrong static path"（GarnetJsonObject.cs:Set :379-382），rust 回 OK 且
   文档原样未改——数据面静默丢写并谎报成功。既有测试全部用数组根上下文
   （json_path_execute_tests.rs:487-541），对象上下文臂零覆盖；
   json_query_root_coercion.rs:126-137 反向锁定 rust 形态，其注释把 C# 引擎行为
   误记为「.NET 内置实现」，事实锚为 GarnetJSON 自有引擎与其测试双证。

涉及代码：
rust 文件与函数：
wedb/wext_json/src/json_path/filter.rs:PathFilter::execute_filter Query 臂
wedb/wext_json/src/json_object.rs:GarnetJsonObject::set
wedb/wext_json/tests/json_query_root_coercion.rs:multiple_queries_chained_filter（反向锁）

对应 c# 文件与函数：
garnet/modules/GarnetJSON/JSONPath/QueryFilter.cs:QueryFilter.ExecuteFilter（两重载）
garnet/modules/GarnetJSON/JSONPath/JsonExtensions.cs:JsonExtensions.SelectNodes
garnet/test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:MultipleQueries
garnet/modules/GarnetJSON/GarnetJsonObject.cs:GarnetJsonObject.Set

精炼执行方案：
1. filter.rs Query 臂改三臂对齐 C#：数组遍历元素、对象遍历属性值、标量零产出
   （delete/mutate 臂已是该语义，改后三引擎单语义收口）。
2. json_object.rs:set 对 evaluate 非空但 replace_matches 计数为 0 的组合给出
   与 C# 同向应答（该形态在 C# 走 wrong static path 错误帧），杜绝 OK+零写。
3. 测试验证点：新增对象/标量上下文 [?(...)] 用例——
   `$.o[?(@.a==1)]` GET 回 "[]"、SET 回 "Err wrong static path"；
   [1..9] 链式三查询回 0 条（恢复 C# MultipleQueries 原断言），
   删除或改写 multiple_queries_chained_filter 的反向锁。
合入哈希：419b9e5 收口形态：filter.rs Query 臂改 C# 三臂值遍历（数组元素/对象属性值/标量零产出）、path.rs delete/mutate 中间臂标量自匹配兜底删除三引擎单语义、set 增设 replaced==0 防御臂回 wrong static path、反向锁 multiple_queries_chained_filter 及三例歪曲移植改写为 C# 原断言、新增对象/标量上下文九例锁
