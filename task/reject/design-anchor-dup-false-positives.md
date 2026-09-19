裁决：不成立（三处「锚点复挂报重复」前提均不实：或无 doc 锚点、或行内注释不被扫描、或空格形态不匹配正则）
来源：next/muse.design.md 条 4（WriteSetLength 三层）、条 6（tree_put_batch 挂 HashSet）、
条 12（Lua 快路径复挂总入口）。核销 2026-09-19。
check.js 判重口径实测：js/check.js:304 dupDefFind 仅对函数文档注释（rsDocExtract 收集的 /// doc）
按 CS_REF_REGEX（js/check/rustScan.js:35，要求「路径.cs」后紧跟 ::? 冒号）聚合判重。

条 4（WriteSetLength 三层同挂）核销
- wedb/wresp/src/resp_memory_writer.rs:509 write_set_length 的 doc 实测无 .cs: 锚点
  （仅「写 set 头：根据 P 静态分派」）；wedb/wresp/src/cmd_strings.rs:476 write_set_len 的 doc
  挂 RespServerSessionOutput.cs:WriteSetLength——全仓唯一挂载，不构成复挂。
- 三层结构本身是真源 + 薄壳 + 单点引用的合规形态：write_set_length 承担 P 静态分派帧型、
  write_set_len 承担运行时协议版本选择、set_commands.rs 七处调用点（:280/:395/:405/:847/
  :1032/:1227/:1234）全部走 cs::write_set_len，write_set_members doc 已自注
  「内部调用 cs::write_set_len 单点」。其修法（保留真源加薄壳、注明不另挂锚点）与现状一致，
  无事可做。

条 6（tree_put_batch 复挂 HashObjectImpl.cs:HashSet）核销
- tree_put_batch 的函数文档注释实测无 .cs: 锚点；muse 所指引用位于
  wedb/wnode/src/resp/objects/tiered_collection_ops.rs:691，是 exec_tiered_hash 函数体内的
  行内 // 注释，不在 rsDocExtract 收集范围，check.js 不扫不报。
- HashObjectImpl.cs:HashSet 的唯一正式挂载在 wedb/wcol/src/hash/hash_object_impl.rs:247，
  单挂合规。

条 12（try_fast_path_set/get 与总入口同挂锚点）核销
- wedb/wlua/src/functions/redis.rs 两快路径函数的 doc 写「对标 LuaRunner.Functions.cs
  ProcessCommandFromScripting 的 SET/GET 分支」——「.cs」与符号之间是空格，不匹配
  CS_REF_REGEX（要求紧跟冒号），不会与 process_command_from_scripting 的正式锚点
  （:挂 LuaRunner.Functions.cs:ProcessCommandFromScripting）聚合报重复。
- 且 doc 语义准确（快路径确为总入口内分支的对标描述），无需改写。
