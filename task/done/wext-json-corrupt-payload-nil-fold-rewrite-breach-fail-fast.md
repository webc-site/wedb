终态注记（2026-09-30 合入收口）：
合入哈希：017a328（fix-json-failfast 分支提交）/ 404eb93（dev --no-ff 合并，无冲突）
收口形态：覆盖面按复核席注记取七处（非票面四处）。七处 from_slice Err 臂统一写
「ERR JSON object decode failed」错误帧（收口为 wext_json error.rs 单源常量
ERR_JSON_DECODE_FAILED，与 json_set_updater 既有先例同串）并按 wcustom
CustomObjectFns 钩子契约回 false（reader false=错误已写中止、updater false=放弃
落库，零扩充）；变异侧 Mutate 分支不再触达，损坏载荷 Save 重写回库与重复 AOF 入账
消除（JSON.CLEAR 自持头同带面随 updater false 一并收口，端到端判据为 HLog tail 零
推进 + StoreEvent::EnvelopeUpsert 零追加，service.rs custom 入账通道单源）。
七处清单：
1. set_get.rs json_get_reader（JSON.GET/MGET 共用 fns，票面 :163）
2. common.rs eval_json_target（共用头：STRLEN/ARRLEN/ARRINDEX/OBJKEYS/OBJLEN）
3. common.rs mutate_json_target（共用头：NUMINCRBY/NUMMULTBY/TOGGLE/ARRAPPEND/
   ARRPOP/ARRINSERT/ARRTRIM/STRAPPEND）
4. mutate.rs json_del_updater（JSON.DEL/FORGET，票面 :174-180 已漂移，现 :214-220）
5. object.rs json_type_reader（JSON.TYPE，非共用头）
6. resp_encode.rs json_resp_reader（JSON.RESP，非共用头）
7. mutate.rs json_clear_updater（JSON.CLEAR，非共用头）
非折损点未误改：root None 臂（common.rs 空载荷建空对象合法语义）回 nil 原样钉形；
多键读臂 error_element_to_nil 协议整形不变（JSON.MGET 元素位 nil 端到端钉死）。
测试：wext_json/tests/json_corrupt_payload_fail_fast.rs（钩子级七处 reader/updater
错误帧+载荷字节零改写、root None 钉形、合法载荷全族逐字节回归）+ wnode/tests/
json_corrupt_payload_fail_fast.rs（合法标签+非 JSON 字节信封种子端到端：读族错误帧
非 nil、变异族零重写零 AOF 增量+真写入反空跑对照、MGET 整形、roaring R.GETBIT
同形对照）。只跑定向 cargo check/test（全量 test.sh/clippy 门禁留主代理）。

甄别结论:通过(P2,2026-09-30 现码复核,双侧锚亲验成立,五池无重复,deviations 无同面在册裁决)

审核结论：通过（2026-09-30 甲轮45-C，P2 级）。wext_json 读/变异四处把 GarnetJsonObject::from_slice 失败折叠为 nil/:0 成功应答且变异族原样重写回库事实确证，违背 corrupt fail-fast 纪律与错误契约对齐要求。执行席遵照：统一改写明确错误帧并返回 false 终止，变异侧返回 false 自然消除损坏载荷重写与重复 AOF 记录，对齐 json_set_updater 与 roaring 既有 fail-fast 先例。

复核席注记（2026-09-30 独立复核：通过判定维持，方案覆盖面订正 4→7）：from_slice 折损点实为七处非四处，「共用头单点收口」主张不成立——除票面四处（set_get.rs:163 json_get_reader、common.rs:25 eval_json_target、common.rs:62 mutate_json_target、mutate.rs 现行号 214-220 json_del_updater，票面 :174-180 已漂移）外，须同票收口 json_type_reader（object.rs:43-49 折 nil）、json_resp_reader（resp_encode.rs:31-37 折 nil）、json_clear_updater（mutate.rs:316-322 折 :0），三者不经共用头；其中 JSON.CLEAR 系 updater 同带损坏载荷 Save 重写 + AOF 重复入账面。错误帧同串「ERR JSON object decode failed」、reader/updater false 契约（wcustom/src/custom_object_fns.rs:61/:63-67 零扩充）与 error_element_to_nil 保留均复核无误。行号漂移订正：roaring ERR_DECODE 现位于 roaring_bitmap_commands.rs:220/:268-274。root None 臂（common.rs:33-36/:70-73，空载荷建空对象合法语义）非折损点，禁误改。

原票面：
wext_json 读/变异执行体把损坏载荷折叠为 nil 成功应答且变异族原样重写回库，违自定义通道 corrupt fail-fast 纪律（review.md 4.1 静默吞错）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 对象反序列化在工厂层先行于命令钩子：JsonModule.OnLoad（garnet/modules/GarnetJSON/JsonModule.cs:29）注册 GarnetJsonObjectFactory，反序列化经 GarnetJsonObject(byte, BinaryReader) 构造（garnet/modules/GarnetJSON/GarnetJsonObject.cs:94-95 reader.ReadString + JsonNode.Parse），载荷损坏即抛异常硬失败，Reader/Updater 钩子无从收到损坏对象，不存在「nil 成功应答」折叠形态。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
信封族 corrupt 纪律单源：wcol/src/object_payload.rs from_blob 契约明文「畸形/损坏载荷显式失败，严禁静默回退空对象」；wnode object_store_utils.rs corrupt_payload_reject 落 RESP_ERR_CORRUPT_PAYLOAD 错误帧并注「宁可回错，不可丢数据」。同通道 wext_roaring 同纪律（decode 失败写 ERR RoaringBitmap object decode failed 错误帧并中止，roaring_bitmap_commands.rs:263-267 checked + ERR_DECODE）。wext_json 异纪律：全族执行体把 GarnetJsonObject::from_slice 失败折叠为 nil 帧并返回 true（成功语义）——读侧 json_get_reader（wedb/wext_json/src/json_commands/set_get.rs:163-169）、读侧共用头 eval_json_target（wedb/wext_json/src/json_commands/common.rs:25-32，STRLEN/ARRLEN/ARRINDEX/OBJKEYS 等消费）、变异侧共用头 mutate_json_target（common.rs:62-68，NUMINCRBY/NUMMULTBY/TOGGLE/CLEAR/ARRAPPEND/ARRPOP/ARRINSERT/ARRTRIM/STRAPPEND 消费）、json_del_updater（wedb/wext_json/src/json_commands/mutate.rs:174-180 写 :0）。对照本 crate json_set_updater 同一失败形态写 "ERR JSON object decode failed" 错误帧（set_get.rs:106-111），crate 内自相分叉。变异侧危害加倍：updater 返回 true 后 dispatch_custom_object_rmw（wedb/wnode/src/resp/objects/custom_object_commands.rs:171-183）继续 is_empty 判定（损坏载荷非空）→ CustomObjMutation::Save 把损坏载荷原样重写回信封 + 重复 AOF 入账——每次变异族命令对同一损坏键重复写放大的 AOF 噪声记录。

3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
损坏（位蚀/残帧回放/装载事故）载荷被答成合法 nil/:0，事故信号被静默吞掉（review.md 4.1 异常收敛禁止静默吞错、5.1 错误契约对齐违例），客户端与监控无从分辨「键逻辑为 null」与「载荷损坏」；变异族每命令一次损坏载荷重写（写放大 + AOF 噪声；载荷字节不变，无数据破坏面、无 panic 面）。与信封族及同通道 roaring 的 fail-fast 形态不齐，违反 wcol object_payload 载荷编解码契约的通道侧对位。

涉及代码：
rust 文件与函数：
wedb/wext_json/src/json_object.rs: GarnetJsonObject::from_slice（损坏入口）
wedb/wext_json/src/json_commands/set_get.rs: json_get_reader（json_set_updater 既有错误帧为同 crate 对照）
wedb/wext_json/src/json_commands/common.rs: eval_json_target、mutate_json_target（两共用头单点）
wedb/wext_json/src/json_commands/mutate.rs: json_del_updater
wedb/wnode/src/resp/objects/object_store_utils.rs: corrupt_payload_reject（信封族纪律对照单源）
wedb/wext_roaring/src/roaring_bitmap_commands.rs: ERR_DECODE（同通道 fail-fast 先例）

对应 c# 文件与函数：
garnet/modules/GarnetJSON/GarnetJsonObject.cs: GarnetJsonObject(byte, BinaryReader)（工厂反序列化 JsonNode.Parse 硬失败）
garnet/modules/GarnetJSON/JsonModule.cs: OnLoad（GarnetJsonObjectFactory 注册形态）
garnet/libs/server/Custom/CustomObjectFactory.cs: Deserialize（工厂层反序列化先行于钩子的分层契约）

精炼执行方案：
1. wext_json 四处 from_slice Err 臂统一改 fail-fast：写明确错误帧（对齐 json_set_updater 既有 "ERR JSON object decode failed" 同串）并按钩子契约返回 false（reader false = 错误已写中止、updater false = 放弃落库，CustomObjectFns 既有语义零扩充），杜绝 nil/:0 成功应答
2. 单点收口零散改：eval_json_target / mutate_json_target / json_get_reader / json_del_updater 各改一处即全族覆盖；变异侧 false 返回同时消除损坏载荷重写回库与重复 AOF 入账（Mutate 分支不再触达）；多键读臂既有 error_element_to_nil 协议整形不变（错误帧禁入元素位）
3. 测试验证点：构造合法标签 + 非 JSON 字节信封载荷，JSON.GET 回错误帧非 nil；JSON.NUMINCRBY 回错误帧且存储载荷字节与 AOF 零增量（重写面消除）；JSON.DEL 回错误帧非 :0；roaring R.GETBIT 同形对照恒错误帧回归不回退；合法载荷全族应答逐字节回归
