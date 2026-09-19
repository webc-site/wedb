拒绝：COSCAN 报错文本不改写为 CUSTOMOBJECTSCAN

来源：next/agy.data.md 条 11、next/muse.data.md 条 4（同题合并）
结论：不成立（取证不实：C# 的 cmdName 报错口径就是 COSCAN，rust 忠实对标；改写反而制造与 C# 的分叉）

引证：
- C# garnet/libs/server/Resp/Objects/SharedObjectCommands.cs:24-33 ObjectScan 的 arity 报错 cmdName switch：`GarnetObjectType.All => nameof(RespCommand.COSCAN)`，即 C# 报错文本本来就是 "COSCAN"（不是 CUSTOMOBJECTSCAN）
- 线名侧 C# garnet/libs/server/Resp/Parser/RespCommandHashLookupData.cs 注册 CUSTOMOBJECTSCAN，RespServerSession.cs:904 `RespCommand.COSCAN => ObjectScan(GarnetObjectType.All, ..)` 枚举名分派——「线名 CUSTOMOBJECTSCAN + 报错 COSCAN」是 C# 自身口径
- rust wedb/wnode/src/resp/parser/command_table.rs:39 线名 CUSTOMOBJECTSCAN 正确；shared_object_commands.rs 的 `All => "COSCAN"` 报错（:52、:198、:498）与 C# cmdName switch 逐字对标，注释（:47）也写明对标关系
- 两档指控「与 C# cmdName 口径不一致」不成立；按 1:1 对标原则（transpile SKILL）不把报错改写为 CUSTOMOBJECTSCAN
