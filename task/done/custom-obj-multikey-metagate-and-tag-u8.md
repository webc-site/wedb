优先级：低
分拣注记（qw.my 第 11 轮条 4 拆出；浅核 2026-09-19：resp_server_session.rs:1954 eq_ignore_ascii_case("JSON.MGET")（漂移自 :1931）、wcustom object_desc.rs:22 CustomCommandMeta 在场；与 done/custom-object-dispatch-single-list.md 边界成立——那票管慢路径/ACL/扩展内双表三轨分发，未覆盖本条的会话执行臂特判与标签型别擦除两处）

扩展对象执行期按命令名字符串特判 JSON.MGET，清单标签以裸 u8 跨过解析→执行缝
问题：wnode/src/resp/resp_server_session.rs:1931 network_custom_obj_cmd 内
`if custom.name.eq_ignore_ascii_case("JSON.MGET")` 自成一体的多键读循环（:1932-1954），绕开
:1963 try_custom_object_command 的统一执行面；这条「哪些扩展命令是多键读」的知识不在静态清单里
（wcustom/src/object_desc.rs:22-31 CustomCommandMeta 只有 name/command_type/arity/fns），于是
新增第二个多键扩展命令必须再改会话层的字符串比对臂——与 custom_objects.rs:1-13 模块头「加一行
即可、不再触碰分发代码内部的标签比对臂」的自述相反，静态分发面退化回运行时按名比串。
同一函数 :232 `pub object_tag: u8`：清单项标签本是有类型的
（object_desc.rs:36 `tag: CustomObjectType`，且 :12 明文「全仓严禁 CUSTOM_OBJECT_TYPE_BASE + n
裸偏移」），入槽时 parser/resp_command.rs:52 `entry.tag.as_u8()` 把型别擦掉，消费侧
wcol/src/object_payload.rs:88 obj_decode_custom(raw, want: u8) 与
wnode/src/resp/objects/custom_object_commands.rs:123 全程收裸 u8，任意字节都能当合法信封标签比对
（无 from_repr 校验），与标准段 GarnetObjectType::from_u8 分域并存。wcol 已依赖 wval
（wcol/Cargo.toml:24），该 u8 缝不是分层所迫。
C#：garnet/libs/server/Resp/RespServerSession.cs:NetworkCustomObjCmd 只有一条
TryCustomObjectCommand 路径，多键/单键差异由 Custom/CustomCommandManagerSession.cs:105-135 的
分型（RawString/Object/Transaction）承接，不在执行期比命令名；标签域
Custom/CustomCommandManager.cs:406 由 CustomObjectType 枚举承担强转。
修法：CustomCommandMeta 增 const 形态的多键读标志（或 fns 增设 read_multi 指针，None 即单键），
会话层按元数据 match 而非名字节；object_tag 全链保持 CustomObjectType，obj_decode_custom 另立
typed 口或在入口 from_repr 校验失败即报错。
条款：SKILL.md:14（编译期静态特性 + 静态枚举分发，杜绝运行时查表）、SKILL.md:51-53（类型枚举
一处定义，严禁散落裸 const u8）。
边界：task/ing/custom-object-dispatch-single-list.md 管 slow.rs 与 acl 闭包两轨及扩展内双表，
本条是它未覆盖的会话执行臂特判与标签型别擦除两处。

分拣补记（muse.my 条 7 同题；浅核 2026-09-19 主仓 dev）：标签 u8 擦除半边同本票（parser
入槽 as_u8、obj_decode_custom 裸 u8 比对、SessionParseState.object_tag 均在场）。增量一点：
wnode/src/resp/custom_objects.rs:33-42 custom_object_type_name 对 CUSTOM_OBJECT_ENTRIES
逐项线性比 tag（const fn，清单现仅 roaring/json 两三项，成本可忽略）——本票落强类型改造时
可顺带以 match 消线性扫，勿另立票。
