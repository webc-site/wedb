裁决：不成立（票面所列调用点已全部转调 wresp 单点，问题已被修复）
来源：next/agy.design.md 条 10 + next/muse.design.md 条 3（两轮同题）。核销 2026-09-19。

一句话结论：RESP Null / Null Array 版本分派单点（wresp::ext::RespVecExt）早已在场且票面所列
全部调用点均已转调，局部函数只剩组合语义或一行转发，无第二份分派体可消。

逐条核销（票面位点 → 当前代码）
1. wedb/wcol/src/resp/output.rs:56 write_null / :62 write_null_array：一行转调
   write_resp_null_ver / write_resp_null_array_ver，文件头 :50-52 注释自述
   「null 一族的二选一只存在于 wresp::ext 两处入口，本文件转调而不复制第二份分派体」。
2. wedb/wnode/src/resp/resp_server_session_output.rs:148 write_null / :156 write_null_array：
   一行转调单点，doc 已注明「版本分派单源在 wresp::ext::RespVecExt」。
3. wedb/wnode/src/resp/objects/hash_commands.rs:751 write_null_array：HMGET 逐字段 nil 占位的
   组合语义（数组头 + N 个 null 元素），逐元素调用 write_resp_null_ver，无内联版本分派。
4. wedb/wnode/src/resp/objects/list_commands/blocking.rs:447 write_collection_item_result：
   空值臂转调 write_resp_null_array_ver / write_resp_null_ver，doc 自注「版本感知单源」。
5. 单点在场：wedb/wresp/src/ext.rs:89/:94 trait 声明、:132/:140 实现。
   C# 对标 garnet/libs/server/Resp/RespServerSessionOutput.cs:WriteNull / WriteNullArray
   的分派语义已由 rust 单点等价承接。
无剩余动作；源档两行随分拣删除。
