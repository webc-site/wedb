拒绝：不为 Redis 兼容补 COPY/TOUCH 命令

来源：next/muse.data.md 条 3
结论：不成立（与 transpile SKILL 1:1 对标冲突：garnet C# 全仓无 COPY/TOUCH，属 Redis 兼容缺口而非转写遗漏，不补）

引证：
- C# garnet/libs/server/Resp/Parser/RespCommand.cs 全枚举无 COPY/TOUCH；garnet/libs/server/Resp/RespServerSession.cs 全分派无；garnet/libs/server/Resp/KeyAdminCommands.cs 全实现无
- rust wedb/wresp/src/command.rs、wedb/wnode/src/resp/parser/command_table.rs、key_admin_commands/ 同无——两边同缺，非 rust 回归（原条目自证「对标 garnet 无遗漏」）
- transpile SKILL：「尽量 1:1 对标 c# 的代码实现，不要实现自己的优化」；原条目的「若补，需新增枚举值 + DUMP/RESTORE 复用 + TTL 拷贝」属主动扩展 C# 没有的命令面，拒绝

附带：wedb/wnode/src/resp/key_admin_commands/keys.rs:1 模块头注释自称涵盖「COPY / TOUCH」但仓内无对应实现，属注释漂移；后续有人触碰该文件注释时可顺带删去这两个词，不单独立项。
