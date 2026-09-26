甄别结论：通过（甄别席 zc-fix-r25-乙，2026-09-26）定级 P4
核验记录：C# 亲验——GarnetJsonObject.cs 四处 JsonNode.Parse（:95/:360/:391/:409 现树亲见）均无 options，吃 System.Text.Json 默认 MaxDepth 64；审核订正（Parse 实为四处、:95 系持久化构造器不宜作错误帧证据）与现树相符。rust 亲验——wext_json 全 crate grep 深度门零命中（仅 json_path/parser.rs 无关 array_depth），本机 registry sonic-rs-0.5.10 src/serde/de.rs:23 MAX_ALLOWED_DEPTH=u8::MAX 亲验（:42 remaining_depth 装载、:1364 注释自陈 256 层 RecursionLimitExceeded）——接受带全由 sonic 承载，现码未变。查重：deviations 全册 JSON 深度面零命中（§124c 链深系 TLS webpki 他域）；四池零同轴。架构：乙案零行为改动+头注单源说明+64/65/255/256 三档测试锁，符合宽向分叉在册先例族与零开销纪律（甲案 O(n) 预扫已被否）。格式：纯文本、双侧齐全。定级 P4：登记级，65-255 带一拒一收为未在册分叉补账，无崩溃无丢数。

审核结论：通过（登记级，裁定乙案：保留 255 接受带并登记；rust 无独立深度门、sonic de.rs:23 MAX_ALLOWED_DEPTH=u8::MAX 经本机 registry 源码亲验；C# 默认 64 属实但锚点订正——Parse 实为四处（议题漏计 :409），且 :95 系持久化恢复构造器不宜引作错误帧证据；甲案预扫给 SET 热路径加 O(n) 遍历，违零开销纪律，且 C# 64 系库默认事故非刻意契约，有 §117d 宽向分叉登记先例）

审核裁定执行方案（供 fix 直接消费，替代文末原方案）：
1. 零行为改动：deviations.md 新条登记 64/255 接受带分叉，双侧锚（JsonDocumentOptions 默认口径 + sonic de.rs:23），注明宽向裁量方向与严禁按 C# 加预扫回改；对拍口径注明 65-255 带 C# 拒/rust 收为预期分叉直引本条。
2. 深度唯一承载说明注释落 json_object.rs 头注（引 sonic 常量为单源），杜绝后人误加预扫或散落裸数字。
3. 测试锁：json_deviation_locks 族入 64/65 层（C# 界带）与 255/256 层（rust 界带）SET/GET 三档行为断言。

wext_json 解析深度接受带与 C# 不一致（sonic 上限 255 vs JsonNode 默认 64）

问题分析：
1. Garnet 契约对齐：C# JsonNode.Parse 默认 JsonDocumentOptions.MaxDepth=64
   （GarnetJsonObject.cs:95/:360/:391 三处 Parse 全部无 options 传入），嵌套
   超 64 层的 JSON 抛 JsonException，被 Set/TryGet 的 catch(JsonException) 收为
   错误帧（GarnetJsonObject.cs:422-426/:333-337），连接存活。
2. 工程现状确证：rust 侧 sonic-rs 0.5.10 Deserializer 的 MAX_ALLOWED_DEPTH=
   u8::MAX=255（src/serde/de.rs:23,:42,:228-239），嵌套 256 层才回
   RecursionLimitExceeded。json_object.rs:78/:277 的 from_slice 与
   set_get.rs:69 的载荷预检均无独立深度门，深度接受带完全由 sonic 默认值决定。
3. 逻辑危害确证：嵌套 65 至 255 层的合法构造载荷，C# 回错误帧拒绝写入，
   rust 接受落库并正常 GET——同一载荷双侧一拒一收，深度契约未登记任何裁量
   （deviations.md 全册无 JSON 深度面）。反向（>255）双侧均错但文案域不同。
   另 ruby 深链在 GET 序列化侧同为递归，与解析上限同带，无越界风险。

涉及代码：
rust 文件与函数：
wedb/wext_json/src/json_object.rs:GarnetJsonObject::{from_slice,set}
wedb/wext_json/src/json_commands/set_get.rs:json_set_need_initial_update
（深度上限实际承载于依赖 sonic-rs 0.5.10 src/serde/de.rs MAX_ALLOWED_DEPTH）

对应 c# 文件与函数：
garnet/modules/GarnetJSON/GarnetJsonObject.cs:GarnetJsonObject（JsonNode.Parse 三处）

精炼执行方案：
1. 二选一收口：甲案在 wext_json 入口设显式深度常量 64 对齐 C#（from_slice 与
   set 载荷预检单点前置校验，超深回错误帧）；乙案保留 255 并在 deviations.md
   登记接受带分叉及理由（如承接 RedisJSON 更宽深度）。
2. 无论甲乙，常量收敛在 error.rs 或 json_object.rs 单点，禁止裸数字散落。
3. 测试验证点：65 层与 255 层与 256 层三档载荷的 SET/GET 行为对拍锁死。


---

合入哈希：a2718269ef2c4d7247cd069c0c086ff161f71254 收口形态：deviations.md §161 登记 C# MaxDepth64 拒超深 vs rust wext_json 无逻辑深度门宽向分叉（现码亲验订正议题「255/256 拒」前提——Value 快路绕过 de.rs:23 MAX_ALLOWED_DEPTH）＋json_object.rs::parse_dom 头注深度单源说明＋json_deviation_locks 族 depth_64_65/depth_255_256_300 两锁，零行为改动
