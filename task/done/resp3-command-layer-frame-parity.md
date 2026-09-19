命令层 RESP3 帧型对位：集合头与 zset 分值编码（同步命令臂）

来源 next/resp3-command-layer-frame-parity.md。前提已核：会话 resp_protocol_version 为真实可 HELLO 升级字段（resp_server_session.rs:263 定义、update_resp_protocol_version:909 赋值、:2834 已按 >=3 分支），版本感知写出单点已在位（wresp/cmd_strings.rs:470 write_set_len、:492 write_double_numeric），本票只补命令层取版本与转调，不改这些单点、不新建第二套版本源。分层出件臂（tiered_collection_ops 一族）不在射程，由已落地的 tiered 侧 RESP3 帧型票承接。

条 1（HIGH，最大功能缺口）SET 族集合头与空集，一律误写 RESP2 数组头：
- wnode/src/resp/objects/set_commands.rs 的 SMEMBERS 缺键、SPOP count==0、SPOP 缺键带 count 三处空集位点用 RESP_EMPTYLIST（*0\r\n），RESP3 应为 ~0\r\n；write_set_members 无条件 write_resp_array_len(members.len())，RESP3 集合头应为 ~N\r\n。消费点 set_intersect、set_union、set_diff。
- 修法：write_set_members 增 resp_protocol_version: u8 形参并改调 cs::write_set_len；三处空集位点改 cs::write_set_len(output, 0, self.resp_protocol_version)（同一调用即得 RESP2 *0 / RESP3 ~0）。
- 严禁误改合法 RESP2 位点：SRANDMEMBER、SMISMEMBER 的空数组位点（C# 本就 TryWriteEmptyArray/TryWriteArrayLength），保持 RESP2。

条 2（MED）ZDIFF/ZINTER/ZUNION 的 WITHSCORES 三处退化：
- wnode/src/resp/objects/sorted_set_commands/write.rs write_zset_entries 无版本形参，无条件写 *(n*2) 扁平头、分值恒 bulk；RESP3 且带分值应头写 n、逐成员前插 *2、分值走 write_double_numeric（,num）。同步消费点 sorted_set_difference/intersect/union。
- 修法：write_zset_entries 增 resp_protocol_version，>=3 时按上式，分值调 cs::write_double_numeric；同步臂调用点传 self.resp_protocol_version。正例结构照抄 wcol/src/zset/sorted_set_object_impl.rs write_sorted_set_result。
- 慢路径（slow.rs）调用点：仅当版本能从会话/存储既有真实载体干净取得时才转调；若存储侧版本源未接通（另票 resp3-storage-session-version-source 承接），则该慢路径调用点保持 RESP2 现状，禁止另立第二处版本源或伪造，把该残留写进最终报告。

条 3（MED）弹出族分值编码：
- sorted_set_commands/blocking.rs write_popped_pairs 及 BZPOPMIN/MAX、ZMPOP 内联同型写出：分值恒 bulk，RESP3 应为 ,num（cs::write_double_numeric 传会话版本）。*2 包装本身对，勿动。
- 优先并入单一分值写出小内核，勿留第三份；非阻塞 ZPOPMIN/ZPOPMAX 已版本感知，不动。

边界
- 只改帧型写出，不碰命令语义、门序、错误文案。nil/超时帧 RESP3 缺口不属本票。
- 不修改 wresp/cmd_strings.rs（单点已正确）；不修改 wbase/convert.rs 与 wcol/resp/output.rs（他票/他代理在途）；不新建版本源。
- 命令层不新增裸 */bulk 字面量，一律经 cs:: 版本感知单点。

约束：中文注释；禁 #[allow]；只在 100% 安全处 unwrap；不新增依赖（cargo add，禁改 Cargo.toml）。协议字节：RESP2 会话零变化（回归既有 RESP2 用例），RESP3 逐字节对位 C# 常量。

先做前提复核：打开上述文件确认 write_set_members 无条件数组头、空集用 RESP_EMPTYLIST、write_zset_entries/write_popped_pairs 无版本形参属实；若前提不成立（已对位），STOP、git checkout 回退、报 REJECT 附证据，不强行改。

验收
- 每条改点位补 RESP2 与 RESP3 双断言（同字节序列对照 C# 常量）：SMEMBERS/SPOP/SINTER/SUNION/SDIFF 空集与非空集头 RESP3 为 ~N；ZDIFF/ZINTER/ZUNION WITHSCORES 为 *n + 逐条 *2 + ,num；ZMPOP/BZMPOP/BZPOPMIN/BZPOPMAX 分值为 ,num。
- RESP2 字节零变化。
- cargo check --workspace --all-targets 绿。

---
前提复核结论（2026-09-19，主仓 dev）：全部属实，票据成立。
- set_commands.rs:280/:395/:405 RESP_EMPTYLIST、:843 write_set_members 无条件 write_resp_array_len 属实；C# RespServerSessionOutput.cs:100 WriteEmptySet（>=3 → ~0，否则 *0）、:238 WriteSetLength（>=3 → ~N）证实。
- write.rs:967 write_zset_entries 无版本形参属实；C# SortedSetCommands.cs:966-988（ZDIFF）/1135-1165（ZINTER）/1446-1462（ZUNION）：RESP3+WITHSCORES 头 n + 逐条 *2 + WriteDoubleNumeric，RESP2 扁平 n*2 + bulk 分值；空结果恒 TryWriteEmptyArray(*0)——rust None 臂 *0 保持不动。RespServerSessionOutput.cs:78 WriteDoubleNumeric（>=3 → ,num，否则 TryWriteDoubleBulkString）证实。
- blocking.rs:32 write_popped_pairs / :265-269 BZPOPMIN 内联分值恒 bulk 属实；C# SortedSetMPop :493-517、BlockingPop :1610-1619、BlockingMPop :1719-1734 分值均 WriteDoubleNumeric，*2/*3 包装对齐不动。

细化方案（实际改动清单）
- 条 1（wedb/wnode/src/resp/objects/set_commands.rs）：
  - write_set_members 增 resp_version: u8，头改 cs::write_set_len(output, members.len(), resp_version)。
  - 同步消费点 set_intersect/:583、set_union/:696、set_diff/:740 传 self.resp_protocol_version；slow 模块 :1136 传域内 resp_version（:975 已有 storage.resp_protocol_version()）。
  - 空集位点（对位 C# WriteEmptySet）：同步 SMEMBERS Missing :280、SPOP count==0 :395、SPOP Missing 带 count :405；慢路径同形态 :1030、:1225、:1232 —— 一律改 cs::write_set_len(output, 0, 版本)。
  - SRANDMEMBER（:460/:473、slow :1089/:1100）与 SMISMEMBER（:343、slow :1063）C# 本就 TryWriteEmptyArray/TryWriteArrayLength，保持 *0 不动。
- 条 2（sorted_set_commands/write.rs）：
  - write_zset_entries 增 resp_version: u8；>=3 且 with_scores：头 *n + 逐成员前插 *2 + cs::write_double_numeric；否则维持扁平 *n*2 + format_double bulk（RESP2 零变化）；None/空结果臂 *0 不动。结构对齐正例 wcol/src/zset/sorted_set_object_impl.rs:write_sorted_set_result 与 C# :966-988。
  - 同步消费点 :280/:347/:482 传 self.resp_protocol_version；slow.rs :546/:579 传域内 resp_version（:208）。
- 条 3（sorted_set_commands/blocking.rs + slow.rs）：
  - write_popped_pairs 增 resp_version: u8，分值改 cs::write_double_numeric（*2/*n 包装不动）；zset_pop_first_nonempty 增形参透传，同步调用方 sorted_set_m_pop/:192、sorted_set_blocking_m_pop/:369 传 self.resp_protocol_version。
  - BZPOPMIN/MAX 内联 :268-269 分值改 cs::write_double_numeric；slow.rs 同型两臂（zset_pop_first_nonempty_cold :853 增形参、:667 调用传 resp_version；Bzpopmin/Bzpopmax 内联 :690-691 分值改 cs::write_double_numeric）。分值单点内核即 wresp cs::write_double_numeric，不另造。
  - 慢路径版本源均在位（slow.rs:208 / set slow :975 storage.resp_protocol_version()），全部干净转调，无 RESP2 残留臂、无第二版本源。
- 测试：三个写出自由函数就地补 #[cfg(test)] mod tests，RESP2/RESP3 双字节断言（*N/~N、扁平 *2n vs *n+*2+,num、bulk 分值 vs ,num）。
- 不改 wresp/cmd_strings.rs、wbase/convert.rs、wcol/resp/output.rs；不新增依赖；命令层不新增裸帧字面量。
