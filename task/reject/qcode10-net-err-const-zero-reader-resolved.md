wresp 三枚零读者错误文案常量（甄别拒绝：主体已按修法落地）

来源：next/qcode10.net.md 条 1（[LOW]，原文件已整体拆除）。取证基线：主仓 dev 当前工作树 HEAD f71dbbc。

原条目主张
- RESP_ERR_SELECT_UNSUCCESSFUL / RESP_ERR_UBLOCKING_CLINET / RESP_ERR_DB_ID_CLUSTER_MODE
  三枚常量全仓零消费；其中 DB_ID_CLUSTER_MODE 对应的 C# cluster 门在 rust 整体缺席且未写
  「刻意不收」说明，形成口径不一致。修法：删三枚常量并补口径注释。

拒绝证据（当前工作树 grep）

1. 三枚中两枚已删除，全仓（含 tests）零命中：
   - RESP_ERR_SELECT_UNSUCCESSFUL：`grep -rn` wedb/ 零命中；
   - RESP_ERR_DB_ID_CLUSTER_MODE：`grep -rn` wedb/ 零命中。
2. 口径注释已补：wnode/src/resp/admin_commands.rs try_parse_database_id 文档注释现为
   「C# 集群模式拒 dbId>0 的门禁刻意不收（doc/zh/db.md §1.3 已删除集群切库限制）」，
   即原条目点名的「删门留文案无说明」缺口已按修法收口。
3. 残留物仅一枚：RESP_ERR_UBLOCKING_CLINET（wedb/wresp/src/cmd_strings.rs:204）仍在、
   全仓除定义行与锚点注释外零消费。单枚死文案属零消费普查域（zero-consumer 系列批票口径，
   见 task/ing/zero-consumer-surfaces-batch-two.md 等先例），不构成本条目（三枚捆绑 + 口径
   质疑）独立重开的理由；如需清理应归入后续零消费普查批。
4. C# 锚点核实存在（非锚点虚构）：garnet/libs/server/Resp/CmdStrings.cs:260/:261/:295
   三枚常量原样在册。

结论：条目主体已失效（修法已落地），拒绝；残留单枚常量移交零消费普查域，不另立票。
