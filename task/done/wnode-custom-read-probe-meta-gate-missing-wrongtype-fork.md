终态注记（2026-10-01）：合入哈希 456f1a5（--no-ff 合并，实现 08178f8，基点同步 9f5fd09）。收口形态：probe_custom_read_sync/async 信封探测前补 Meta 分层闸，复用装载族单源步进 step_load_meta + meta_probe + probe_tag_sync/async（三件升 pub(super) 属机械改动，判据零新造）；Done(WrongType)→ReadProbe::WrongType、Done(Degrade)→ReadProbe::Degrade；闸内出帧入丢弃型 sink 守探测面零出帧，四条 Read 消费臂零改动；分层存活键 R.GETBIT/R.BITCOUNT/R.BITPOS/JSON.GET 恒 -WRONGTYPE 与 RMW 臂同帧，JSON.MGET 元素位维持 nil；过期未清退分层键经 wkv 域内 TTL 单点门仍答缺失形（deviations §150 判死吸收形不回改）。新增 tests/custom_read_meta_gate 四则定向回归全绿（分层种子取计数升阶形 65546 短条目，规避 wbftree 契约闸 128B 键长拒长成员；冷键慢臂同帧、String/信封/本型键回归、过期缺失形并验），相邻套件 custom_object_recheck/get_slow_arm_object_wrongtype/expired_object_key_get_funnel_missing_shape/scan_tiered_read_arm_refresh/json_corrupt_payload_fail_fast 回归全绿，cargo check 通过。无遗留同面。

甄别结论:通过(P2,2026-09-30 现码复核,双侧锚亲验成立,五池无重复,deviations 无同面在册裁决)

审核结论：通过（2026-09-30 甲轮45-D，P2 级）。自定义对象 Read 通道探测面（probe_custom_read_sync/async）只探信封与 String 域而缺失 Meta 分层闸事实确证，分层态集合键在 Read 通道按缺失应答而在 RMW 通道与 C# 恒 -WRONGTYPE，多路径行为同构违例。执行席遵照：复用 step_load_meta + meta_probe + probe_tag 既有单源补齐 Meta 闸，保持探测面零出帧纪律。

原票面：
自定义对象 Read 通道探测面缺 Meta 分层闸，分层态异构键按缺失应答，同键 RMW 通道与 C# 恒 -WRONGTYPE（review.md 4.2 多路径行为同构违例）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 自定义对象命令单一对象存储，键存活即可判型：Read_ObjectStore 收割后经 CustomObjectBase.Operate（garnet/libs/server/Custom/CustomObjectBase.cs:77-96）判 (byte)input.header.type != this.type 即置 ObjectOutputFlags.WrongType，TryCustomObjectCommand（garnet/libs/server/Custom/CustomRespCommands.cs:158-231）Read 臂 GarnetStatus.WRONGTYPE 出 CmdStrings.RESP_ERR_WRONG_TYPE 错误帧。键驻内存或在盘不影响该判定，不存在「按缺失应答」的第三形态。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 自定义对象通道两臂装载判定核不同源。RMW 臂 try_custom_object_rmw_sync 与 custom_object_rmw_async 经 obj_load_custom_sync / obj_load_custom（wedb/wnode/src/resp/objects/object_store_utils.rs）三步探测树：先 Meta 分层闸（step_load_meta → meta_gate：存活 Meta 记录 collection_type 为标准段 Hash/Set/SortedSet/List/RangeIndex，与自定义标签 0x40/0x41 恒异型即 WrongType 出帧），再信封、再 String 域反探。Read 臂 probe_custom_read_sync 与 probe_custom_read_async（wedb/wnode/src/resp/objects/custom_object_commands.rs）只探 ObjectEnvelope 与 String 两域，无 Meta 闸。升阶键数据迁 Meta 域 + wbftree、信封物理删除（wkv session/collection.rs 模块头「首升阶落元记录、删信封」；「内存态通用对象只驻信封」），String/信封双域皆缺，Read 臂遂落 not_found 执行体按缺失应答：R.GETBIT 回 :0、R.BITCOUNT 回 :0、R.BITPOS 回 :from/-1、JSON.GET 回 nil；同键 R.SETBIT / JSON.SET（RMW 臂过 Meta 闸）出 -WRONGTYPE。实测（wnode 集成夹具，8000 字段×600B HSET 越字节阈触发升阶，HLEN 确认 :8000）：R.GETBIT big 10 → :0、R.BITCOUNT big → :0、R.SETBIT big 10 1 → -WRONGTYPE；字符串键双臂均 -WRONGTYPE。同步/异步、单键/多键四条 Read 臂（try_custom_object_read_sync、try_custom_object_multi_read_sync、custom_object_read_async、custom_object_multi_read_async）同缺此闸。文件头注自称「与装载族同一决策核」，实缺装载族第一步。

3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
review.md 4.2 多路径行为同构违例：同一逻辑态（分层存活异构键）RMW 臂 -WRONGTYPE、Read 臂缺失应答两态发散；5.1 错误契约分叉：对 C# 全键型存活判型收窄为两域判型。客户端把异构键误判缺失（R.BITCOUNT 错答 :0、JSON.GET 错答 nil），应用侧防御校验与监控口径失真。无数据破坏面（读臂零写回、零建键）。同族先例见 task/todo/wnode-get-slow-arm-object-key-nil-wrongtype-fork（标准对象通道 GET 慢臂 nil/WRONGTYPE 分叉，P2），本票系自定义通道同形，面不重叠。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/custom_object_commands.rs: probe_custom_read_sync、probe_custom_read_async（缺 Meta 闸的探测面）；try_custom_object_read_sync、try_custom_object_multi_read_sync、custom_object_read_async、custom_object_multi_read_async（四条消费臂）
wedb/wnode/src/resp/objects/object_store_utils.rs: obj_load_custom_sync、obj_load_custom（RMW 通道同键有闸对照）；step_load_meta、meta_gate、meta_probe（判定核单源）

对应 c# 文件与函数：
garnet/libs/server/Custom/CustomRespCommands.cs: TryCustomObjectCommand（Read_ObjectStore WRONGTYPE 出帧臂）
garnet/libs/server/Custom/CustomObjectBase.cs: Operate（判型先行 WrongType 旗标）

精炼执行方案：
1. probe_custom_read_sync / probe_custom_read_async 在信封探测前补 Meta 分层闸步：复用 step_load_meta + meta_probe + probe_tag_sync / probe_tag_async 既有单源（零新判据），MetaStep::Done(ObjLoad::WrongType) 映射 ReadProbe::WrongType、Done(ObjLoad::Degrade) 映射 ReadProbe::Degrade，闸内出帧仍写丢弃型 sink（出帧权留调用臂，与现有「探测面零出帧」纪律一致）；单键臂既有 WrongType 出帧臂、多键臂既有逐元素 nil 口径零改动
2. 判型语义零新造：自定义标签下任何存活 Meta 记录恒异型，meta_gate 判型臂直接承接；过期未清退分层键经 wkv 域内 TTL 单点门判缺失（obj_load_custom_sync 既有采序），守 deviations.md §150 判死吸收形不回改
3. 测试验证点：分层 Hash/Set/ZSet 键上 R.GETBIT / R.BITCOUNT / R.BITPOS / JSON.GET 恒 -WRONGTYPE（与 R.SETBIT / JSON.SET 同帧）；JSON.MGET 对应元素维持 nil（批量逐元素口径不变）；字符串键与内存态信封键既有应答回归不回退；过期未清退分层键仍按缺失应答（§150 锁面）
