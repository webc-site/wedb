甄别结论：通过（甄别席 zc-fix-r25-乙，2026-09-26）定级 P1
核验记录：C# 亲验——GarnetJsonObject.cs:Set :358-368 仅 pathStr=="$" 走根替换、rootNode null 落 RESP_NEW_OBJECT_AT_ROOT 错误帧（现树逐行见）；JsonPath.cs:122-127 空 filters 回 new List{t}（SelectNodes("") 产 [根] 带包裹）属实。rust 亲验——json_object.rs:264 set 臂 `path_str == "$" || path_str.is_empty()` 空路径同走根分支（缺键建根回 OK）、:197 try_get 单路径 p.is_empty() 回裸根、:234-262 多路径臂键值带包裹、set_get.rs:64 门放行空路径——三臂分叉现码全在，无合入灭失。查重：deviations 全册 JSON 空路径面零登记（§15d 键名转义、§19 缺键空壳红线均不同轴且与本票裁定同向）；四池同轴零命中。架构：path_is_root 判定单源收口、缺键不落空壳守 §19 红线，无第二机制；审核裁定三形态测试锁闭环可执行。格式：纯文本、双侧路径齐全。定级 P1：SET 缺键空路径双侧一错误帧一成功建根、GET 包裹形态双臂分叉，命令应答契约分叉。

审核结论：通过（修复级+登记级混合；三臂分叉全部亲验属实，C# 自身两臂不一致亦属实；deviations §15/§19 无登记、task 各池无同轴票）

审核裁定执行方案（供 fix 直接消费，替代文末原方案；原方案 b/c「择一形」已明确裁定）：
1. a 臂对齐 C#/RedisJSON：json_object.rs:set 根分支删 || is_empty()（既有键 "" 经空过滤器仍命中根，语义与 C# 等价），set_get.rs:64 门改仅放行 "$"，缺键空路径回 RESP_NEW_OBJECT_AT_ROOT 错误帧且不建键（§19 缺键不落空壳红线，不携 C# 空壳残留）。
2. b/c 臂单点收口："" 规范化为与 "$" 同形，GET 双臂统一回 "[<根>]"（取 C# 快路形），设 path_is_root 判定单源消除 rust 自身单/多臂不一致；与 C# 通用臂（带格式裸根）之残余差入 deviations.md 登记。
3. 测试锁：wext_json/tests 三形态——SET 缺键 "" 42 回错误帧且 EXISTS 0；GET "" 无/带格式两臂同形 "[<根>]"；GET "" $.a 键值带包裹；逐臂对拍 C# 锚行为。

JSON 空路径 "" 三臂双侧分叉：SET 缺键建根、GET 单路径无包裹、多路径键值带包裹

问题分析：
1. Garnet 契约对齐：C# 对空路径 "" 无规范化——SET 臂 GarnetJsonObject.cs:Set
   :356-368 仅 "$" 走根替换，"" 在缺键（rootNode null）时落
   RESP_NEW_OBJECT_AT_ROOT 错误帧；GET 臂 Reader 快路（JsonCommands.cs:165-178）
   单路径 "" 进 TryGetToWriter，SelectNodes("") 因空过滤器回 [根]
   （JsonPath.cs:Evaluate :124-127），产出带 [] 包裹的 "[<根>]"，而通用路
   （带格式选项，GarnetJsonObject.cs:TryGet :292-299）同路径 "" 回无包裹裸根，
   多路径分支（:147-173）内层走通用路同样回裸根——C# 自身两臂即不一致。
2. 工程现状确证：rust 把 "" 统一规范化为根语义——set（json_object.rs:279-292）
   `path_str == "$" || path_str.is_empty()` 同走根分支，缺键时 None 选项直接
   建根回 OK（need_initial_update 门 :64 亦放行空路径）；try_get 单路径臂
   :183-191 p.is_empty() 恒回裸根；多路径臂 :234-262 对 "" 按空过滤器求值得
   [根] 回带 [] 包裹。三处与 C# 对应臂逐臂不同。
3. 逻辑危害确证：三个可观测分叉均无登记无测试锁：
   a) `JSON.SET 新键 "" 42`：C# 回 "ERR new objects must be created at the root"
   （且初始更新器返回 true 可能残留空对象壳），rust 回 OK 建根——错误帧对成功写，
   属数据面收口分叉且 rust 侧偏离 RedisJSON（RedisJSON 拒空路径）；
   b) `JSON.GET k ""`（无格式选项）：C# "[<根>]"，rust "<根>"；
   c) `JSON.GET k "" $.a`：C# {"":裸根,...}，rust {"":[根],...}。

涉及代码：
rust 文件与函数：
wedb/wext_json/src/json_object.rs:GarnetJsonObject::{set,try_get}
wedb/wext_json/src/json_commands/set_get.rs:json_set_need_initial_update（:64 空路径放行门）

对应 c# 文件与函数：
garnet/modules/GarnetJSON/GarnetJsonObject.cs:GarnetJsonObject.Set（:356-368）
garnet/modules/GarnetJSON/GarnetJsonObject.cs:GarnetJsonObject.TryGet（:292-299）
garnet/modules/GarnetJSON/GarnetJsonObject.cs:GarnetJsonObject.TryGetToWriter（经 SelectNodes("") 空过滤器回根）
garnet/modules/GarnetJSON/JsonCommands.cs:JsonGET.Reader（:165-178 快路）
garnet/modules/GarnetJSON/JSONPath/JsonPath.cs:JsonPath.Evaluate（:124-127 空 filters 回 [t]）

精炼执行方案：
1. 裁定方向并单点收口：建议 a) 臂对齐 C#（set 根分支去掉 is_empty() 等价，
   need_initial_update 门同步拒空路径），b)/c) 臂择一形（建议多路径臂对齐
   C# 裸根、单路径臂按裁定登记或对齐 "[<根>]"），全部差异入 deviations.md。
2. 空路径语义常量单点（如 path_is_root 判定），杜绝三处各自比较。
3. 测试验证点：`SET 缺键 "" v`、`GET k ""`、`GET k "" $.a` 三形态断言锁死，
   与 C# 锚行为逐臂对拍。

合入哈希：b55fb5a 收口形态：SET 空路径缺键收口回 RESP_NEW_OBJECT_AT_ROOT 错误帧且不建根（set 根分支删 is_empty、need_initial_update 门仅放行 $，对齐 C#/RedisJSON 守 §19），GET 单/多臂经 path_is_root 单源将 "" 规范化为 $ 同形恒回 [根]（取 C# 快路形，与 C# 通用臂裸根残余差登记 deviations §156），wext_json/tests 六形测试锁闭环。
