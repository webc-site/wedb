归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 58b4463（P1），收口形态：parse_dom 单点（use_rawnumber＋整串 UTF-8＋尾部垃圾拒收）替换四处散装 from_slice::<Value>，数字原文词形 GET 逐字节保真，沙箱并 dev 之空路径分叉收口（b55fb5a）后 wext_json 全量测试绿；七例锁 json_number_raw_text.rs。续排注：残余差未发现，deviations 无须补登。

甄别结论：通过（甄别席 zc-fix-r25-乙，2026-09-26）定级 P1
核验记录：C# 亲验——GarnetJsonObject.cs:95/:360/:391/:409 全链 JsonNode.Parse 无 options（现树亲见），.NET JsonElement.WriteTo 数字臂拷原文系框架文档化设计，票面论断成立（执行期建议对拍钉死）。rust 亲验——wext_json 全 crate grep RawNumber/use_rawnumber 零命中，json_object.rs from_slice 归一化存储现样，Cargo.lock sonic-rs 0.5.10 与票面一致——无修复合入，归一化丢形仍在。查重：deviations §1 仅 RESP 浮点应答/INCRBYFLOAT 域，JSON DOM 数字往返面零登记；四池零命中。架构：甲案取 sonic 原生 RawNumber 单存储形态（按需数值化助手段、不存第二表示）、明禁 workspace feature 开关污染 wlua/wnode/wresp 共用 sonic，符合「json 用 sonic_rs」选型与单机制纪律；测试锁四形逐字节+幂等+C# 同款用例互锁，可执行。格式：纯文本、双侧齐全。定级 P1：JSON.GET 应答逐字节契约分叉、尾部零/大整数精度一经 SET 静默改形。

审核结论：通过（修复级，裁定甲案：数字节点保原文；rust 侧 sonic-rs 0.5.10 三态归一化、-0 丢负号、超 u64 落 f64 均亲验属实；C# element-backed WriteTo 拷原文系 System.Text.Json 文档化设计，建议执行时对拍一次钉死；deviations §1/§15 确不涉 JSON DOM 数字往返）

审核裁定执行方案（供 fix 直接消费，替代文末原方案）：
1. 解析面改 sonic_rs::Deserializer::config().use_rawnumber()（0.5.10 原生 API，禁止用 workspace feature 开关——sonic 同被 wlua/wnode/wresp 消费会污染）；序列化面单点回原文，数字为唯一存储形态。
2. 判型（json_type_name integer/number）、过滤器比较、NUMINCRBY/NUMMULTBY 取值全数改按需数值化助手段，不存第二数值表示（RawNumber 下 as_i64/as_f64 归 None 为强约束提示）。
3. 测试锁：SET "1.10"/"1e2"/"-0"/"18446744073709551616" 后 GET 逐字节对拍；GET 两次幂等；serialize_object→from_slice 重载保原文；C# JsonCommandsTest 同款用例互锁。残余差如另有发现入 deviations 补登。

wext_json 数字存储经 sonic_rs 归一化，JSON.GET 往返丢失原文词形（C# JsonNode 保原文）

问题分析：
1. Garnet 契约对齐：C# GarnetJsonObject 全链持 JsonNode（GarnetJsonObject.cs:95
   JsonNode.Parse / :118-123 SerializeObject / TryGet 系列直接序列化节点）。.NET
   JsonNode.Parse 产出的 JsonValue 由 JsonElement 承载，写回时按原文字节输出数字
   （JsonElement.WriteTo Number 臂拷贝原始文档字节），故 SET 载荷中的 "1.10"、
   "1e2"、"-0"、超 u64 大整数、高精度小数经 SET 落库后 GET 逐字节还原原文。
2. 工程现状确证：rust wedb/wext_json/src/json_object.rs:37 root_node 为
   sonic_rs::Value，:78 from_slice 与 :277 set 的 from_slice 把数字解析为
   sonic-rs 0.5.10 的 I64/U64/F64 三态（src/value/node.rs Meta::I64/U64/F64，
   见 sonic-rs-0.5.10/src/value/from.rs:20-26 与 node.rs:698-700），
   序列化（sonic_rs::to_vec）按数值最短表示重写文本。域内 i64/u64 整数往返保真，
   但 "1.10" 落 f64 回显 "1.1"、"1e2" 回显 "100.0"、"-0" 丢负号、
   18446744073709551616 起的大整数落 f64 回显科学计数，原文词形不可逆。
3. 逻辑危害确证：对位命令 JSON.SET/JSON.GET 的应答逐字节契约分叉——
   `JSON.SET k $ 1.10; JSON.GET k $` C# 回 "[1.10]"、rust 回 "[1.1]"。
   该面无任何登记（deviations.md §1 仅涉 RESP 浮点应答与 INCRBYFLOAT 落盘，
   不涉 JSON DOM 往返），亦无测试锁（json_from_slice.rs 往返用例全整数，
   C# 侧 JsonCommandsTest 仅 8.95/8.99 等双精度最短表示可原样往返的非判别值）。
   金融类尾部零与小数位精度数据一经 SET 即静默改形。

涉及代码：
rust 文件与函数：
wedb/wext_json/src/json_object.rs:GarnetJsonObject::{from_slice,set,serialize_object,try_get}
（根因在存储表示 sonic_rs::Value 的 Number 归一化，非单点函数）

对应 c# 文件与函数：
garnet/modules/GarnetJSON/GarnetJsonObject.cs:GarnetJsonObject（rootNode JsonNode 全链）

精炼执行方案：
1. 判定方向二选一并登记 deviations：甲案对齐 C#——数字节点保原文
   （sonic-rs RawNumber 或原文 span 承载，序列化直拷原文，仅过滤器比较时按需
   数值化）；乙案接受归一化——在 deviations.md 立登记条目锁双侧行为差与理由。
2. 若采甲案：TYPE 命令的 integer/number 判型与 NUMINCRBY/NUMMULTBY 取值仍按
   数值语义，仅序列化面走原文，避免双机制。
3. 测试验证点：SET "1.10"/"1e2"/"-0"/"18446744073709551616" 后 GET 逐字节
   对拍期望文本；往返序列化幂等（GET 两次同串）。
