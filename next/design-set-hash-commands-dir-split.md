优先级：低
来源：next/agy.design.md 条 19。
取证基线：主仓 dev 当下代码。

问题
set_commands.rs 1431 行、hash_commands.rs 1116 行仍为单文件，而兄弟模块 list_commands
与 sorted_set_commands 均已目录化（read/write/slow/blocking 分文件）；同族模块两形态并存，
后续维护与审读路径不一致。

取证
- wedb/wnode/src/resp/objects/set_commands.rs 全 1431 行（含 :845 write_set_members、
  :1404 测试）
- wedb/wnode/src/resp/objects/hash_commands.rs 全 1116 行（含 :751 write_null_array 工具、
  :758 起内嵌 mod slow 慢路径臂）
- 目录化先例在场：wedb/wnode/src/resp/objects/list_commands/、sorted_set_commands/
  （各含 mod.rs、read.rs、write.rs、slow.rs 等）
- C# 对标：garnet/libs/server/Resp/Objects/SetCommands.cs 与 HashCommands.cs 各自独立，
  C# 单文件量级（HashCommands.cs 约 1300 行）与 rust 相当，但 rust 仓内已有目录化更细先例，
  对齐仓内形态优先

修法建议
对齐 list/sorted_set 模式拆目录：set_commands/{mod.rs, read.rs, write.rs, slow.rs}、
hash_commands/{mod.rs, read.rs, write.rs, slow.rs}（hash 的内嵌 mod slow 外提）。
mod.rs 只留声明与 pub use，对外路径不变。纯搬运禁夹带语义改动，锚点随函数搬位不重复挂载。
优先级低于 tiered-collection-ops-file-split（那是 2167 行巨峰），本票属形态对齐。
