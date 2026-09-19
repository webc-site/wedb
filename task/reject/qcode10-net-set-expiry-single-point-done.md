SET 选项环绕过 wresp 过期选项单点、手写五连 token 比较（甄别拒绝：已修复）

来源：next/qcode10.net.md 条 2（[MED]，原文件已整体拆除）。取证基线：主仓 dev 当前工作树 HEAD f71dbbc。

原条目主张
- set.rs 选项环手写 is_expiry_option 五连 eq_ignore_ascii_case，绕过
  wresp::options::try_get_expiration_option 单点；同文件 :226-229
  expiration_option_from_token 为零读者死别名。修法：改调单点 + 删死别名。

拒绝证据（当前工作树 grep）

1. 手写比较已不存在：`grep -n 'is_expiry_option'` 全仓零命中。
2. 单点已接线：wedb/wnode/src/resp/basic_commands/set.rs:672 现为
   `if let Some(parsed_option) = try_get_expiration_option(next_opt)`，
   且 :10 显式导入 try_get_expiration_option —— 与 C# NetworkSETEXNX 经
   TryGetExpirationOptionWithToken 单点识别同形态。
3. 死别名已删：`grep -n 'expiration_option_from_token'` 全仓零命中
   （该删除动作本就登记在 task/ing/zero-consumer-surfaces-batch-two.md 第六节射程内，
   现已完成）。
4. 佐证：task/reject/set-exist-options-single-source.md（基线 a7402c4）早已取证
   「set.rs 过期档 :672 走 wresp::try_get_expiration_option、存在性档走
   try_get_exist_options，两族同轨同源」，与本轮复核一致。
5. C# 锚点核实存在：garnet/libs/server/SessionParseStateExtensions.cs:717/:727
   TryGetExpirationOptionWithToken、garnet/libs/server/Resp/BasicCommands.cs（NetworkSETEXNX）。

结论：条目两个事实前提（绕行单点、死别名）在当前 HEAD 均已不存在，拒绝。
