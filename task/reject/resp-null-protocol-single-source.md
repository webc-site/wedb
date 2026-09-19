RESP null 协议单源——拒录与本档观点勘误

来源：/Users/z/git/db/wedb/task/ing/resp-null-protocol-single-source.md（一棒认领后撞 150 轮
上限、分支零提交；二棒 fix-respnull-b 基于 dev 重做时逐 hunk 验尸）。取证基线为二棒开工时的
dev（合并前 HEAD，含一棒死树 /tmp/fork/resp-null-single-source 的 27 文件 +131/-145 未暂存
diff 全量过目）。本档同时是 ing 档第 4-6 行所指、dev 上此前并不存在的那份 reject 档案的首次落档。

一、本档自身观点不成立处（按本档改会制造新的 C# 分叉）

1. 第 43-44 行把 INFO 空段应答归入「命令层就地 RESP2 字面量 12 处」并要求「一律改调
   wresp/src/ext.rs 的两个 null 单点」——该位点在 C# 不是 WriteNull。

   原文（节）：「- wedb/wmetric/src/info/info_command.rs:85 与
   wedb/wnode/src/resp/garnet_api/slow.rs:357（INFO 空段应答）」，判据段：「C# 走
   RespServerSessionOutput.cs:193/:208 的 WriteNull / WriteNullArray 位点，一律改调
   wedb/wresp/src/ext.rs 的两个单点」。

   取证：garnet/libs/server/Metrics/Info/InfoCommand.cs:72-79——
   `if (!string.IsNullOrEmpty(info)) WriteLargeVerbatimString(...)` else
   `while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_EMPTY, ref dcurr, dend))`；
   garnet/libs/server/Resp/CmdStrings.cs:192 `RESP_EMPTY => "$0\r\n\r\n"`。
   即 INFO 无内容时回的是「协议恒定的空批量串」，不是 nil。照本档改调 null 单点会把
   RESP2 下的 `$0\r\n\r\n` 变成 `$-1\r\n`，反而新增一处与 C# 的帧分叉。

   定稿：新增 `cmd_strings::RESP_EMPTY`（同名对位 C# 常量），wmetric/info_command.rs 与
   wnode/resp/garnet_api/slow.rs 两处 INFO 空段臂改回该常量；此位点不再是 null 一族成员，
   验收 2 的「就地字面量为 0」同时满足。落地次序「零新常量」一句的射程是「并立分派口」那一步，
   本常量是该步之外的 C# 同名既有帧，不属自造。

2. 第 111-112 行「cmd_strings 侧只留 RESP_ERRNOTFOUND / RESP3_NULL_REPLY 常量」与验收 2
   第 131 行把这两常量列为「合法持有者」——收口后它们零消费者，留着就是第二套 nil 帧字节源。

   取证：全仓（含 */src、*/tests、wlua/wcluster/wedb）grep 实测这两个常量的唯一消费者是
   wnode/src/resp/bitmap/bitmap_commands.rs 的 write_bitfield_nil 分派体，而本档第 73-74 行
   正是命该分派体转调单点；C# 侧 CmdStrings.cs 的同名常量由 writer/utils 内部消费，会话层
   不直接持有帧字节。

   定稿：两常量随分派口一并删除，null 帧字节只由 wresp/src/resp_memory_writer.rs 的
   Resp2/Resp3 实现持有（验收 2 点名的合法持有者）。

3. 第 40-42 行要求 vector 应答编码器的两臂「改调 write_resp_null_ver /
   write_resp_null_array_ver」——该处不存在「版本二选一」缺失，而是版本已静态定型的分支。

   取证：wnode/src/resp/vector/resp_server_session_vectors.rs 的 VectorReply 自带
   encode_resp2 / encode_resp3 两个全帧型编码器，入口 `encode_resp(out, resp3)` 已按会话
   协议裁决（该文件 Double/Boolean/Map 头帧同此拓扑，本档第 28 行已把非 null 头帧排除在射程外）。
   在 RESP2 编码器里再传 `write_resp_null_ver(2)` 等于把运行时裁决下沉到静态已知处。

   定稿：两臂转调 `Resp2::write_null` / `Resp2::write_null_array` /
   `Resp3::write_null` / `Resp3::write_null_array`——即两个版本入口的底层唯一实现，帧字节
   零复制、无第二份分派体；其中 NullArray 的 RESP3 臂用 write_null_array 而非
   write_null（同字节 `_`，但保持与 write_resp_null_array_ver 的 RESP3 臂同名口径）。

二、一棒 diff 的弃用/改写项（余 26 文件逐 hunk 采纳，清单见二棒回报）

1. wedb/wnode/src/resp/vector/resp_server_session_vectors.rs：NullArray 的 RESP3 臂
   `Resp3::write_null(out)` → 改写为 `Resp3::write_null_array(out)`（见第一节第 3 条）。
2. 同文件导入：一棒引入 `RespProtocol` 后又由二棒一次删除复加（Resp2/Resp3 的 write_null 系
   trait 方法，trait 须在 scope），最终形态为
   `resp_memory_writer::{Resp2, Resp3, RespProtocol as _, format_double}`。
3. 半截项（一棒未做完即撞上限，二棒补全）：
   - wnode/src/resp/rangeindex/resp_server_session_range_index.rs 的 network_riget 加版本入参后
     漏改第二个调用点 wnode/src/resp/garnet_api/slow.rs:533（arity 编译不过）。
   - garnet_api/slow.rs 的 INFO 空段孪生臂（一棒只改 wmetric 侧）。
   - 本档第 3 节第 3 步整批未动：wext_json 43 + 4 处、wext_roaring 1 处就地字面量、
     wcustom CustomObjectFns 四执行体的版本入参穿线、ext.rs::write_resp_null 本体与其
     2 处测试消费（wresp/tests/writer.rs、ext.rs::vec_ext_formatting）。
   - 本档第 2 条 wnode 40 处中漏 1 处：tiered_collection_ops.rs exec_tiered_scan 扫描臂
     （.write_resp_null()），二棒为该函数补版本入参，调用点取存储会话现成口。
   - 验收 4 的新测试档（resp3_null_parity.rs）一棒未建。

三、实况与行号勘误（不影响判据，记录以免下棒重找）

- storage_session.rs「:320」实为 :274；garnet_api/slow.rs「:357」实为 :365；
  ext.rs 单点在合并后的 dev 上为 :72/:77（声明）与 :115/:123（实现）。
- 第 147-149 行所称第二棵同源死树 wave1-a-resp-null 在二棒开工时已不在 /tmp/fork
  （git worktree list 实测仅 resp-null-single-source，27 文件 +131/-145 未暂存、
  tip a7402c4 为 docs checkpoint、ahead=0，判据见 [[subagent-turn-limit-salvage]] 假合入口径）。
- 第 4-6 行所称「不成立段连同拒绝理由见 task/reject/resp-null-protocol-single-source.md」
  在 dev 上原不存在（git log -- 该路径零命中），本档即首次落档。
- 一棒 diff 的 4 处 wnode/wnode_test 之外命中均属本档射程，未见删他人 task/ing 文档或
  越域改动（死树 status 全为 ` M`，无 ` D`、无 untracked）。

四、未由本档收口的相邻缺口（不改，供后续派单）

- wnode/src/resp/array_commands.rs::network_mget 与 wnode/src/storage/session/
  storage_session.rs:274 分别按「会话版本」与「存储会话版本」两处现成口裁决 nil（均转调
  同一单点，无第二分派体），但 wnode_test::with_batch 造的存储会话不 arm resp_version，
  故 SG 批量 GET 臂的 RESP3 形态只能在 e2e 消费者测试（resp_json_commands_tests.rs 型
  setup + HELLO 3）里断言；本档验收 4 的新档因此只覆盖命令面/执行体/编码器三层。
