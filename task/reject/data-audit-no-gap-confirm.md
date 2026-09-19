拒绝：muse.data 五条「已齐/无遗漏」确认型条目，无待办不立项

来源：next/muse.data.md 条 6、7、8、9、10（审查者自己的对照结论为「无需补功能」「无遗漏」，分拣复核属实，问题不存在）

逐条复核：
- 条 6（SET EXAT/PXAT 拒绝是忠实转写）：wedb/wnode/src/resp/basic_commands/set.rs:387、:667 注释已写明「SET 只接受 EX/PX/KEEPTTL（EXAT/PXAT 报语法错误）」，与 garnet/libs/server/Resp/BasicCommands.cs:628-740 同口径；GETEX 侧 EX/PX/EXAT/PXAT/PERSIST 全齐（get.rs，对标 BasicCommands.cs:99-181）。确认无缺陷。
- 条 7（键级 TTL NX/XX/GT/LT 已齐）：wresp/src/options.rs ExpireOption 位值与 garnet/libs/server/ExpireOption.cs 同位；keys.rs network_expire 双选项仅放行 XXGT/XXLT，对标 KeyAdminCommands.cs:364；EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT + TTL/PTTL/EXPIRETIME/PEXPIRETIME + PERSIST 接线齐。确认无遗漏。
- 条 8（字段级 TTL 命令全齐）：HEXPIRE 系列 + HTTL/HPTTL/HEXPIRETIME/HPEXPIRETIME、ZEXPIRE 系列 + ZTTL/ZPTTL/ZEXPIRETIME/ZPEXPIRETIME、HCOLLECT/ZCOLLECT 齐，对标 HashCommands.cs/SortedSetCommands.cs；wkv ttl.rs 两层载体对标 HashObject/SortedSetObject 条件。确认无遗漏。键级双选/字段级单选的注释口径沉淀已另立 next/data-ttl-option-combo-constraint-comment.md。
- 条 9（参数面抽查无缺）：BITFIELD OVERFLOW 三形态、BITCOUNT BYTE/BIT、BITOP DIFF、LCS 全参数、SCAN MATCH/COUNT/TYPE、GEO 族、VECTOR 族、RI 九命令逐项对标 BitmapCommands.cs/ArrayCommands.cs/RespServerSessionRangeIndex.cs/RespServerSessionVectors.cs 均在。确认无遗漏。
- 条 10（wcol 侧枚举 1:1 齐）：HashOperation/ListOperation/SetOperation/SortedSetOperation 各族含 SINTERCARD LIMIT、LMPOP/BLMPOP COUNT、ZADD/ZRANGE 全参数、HLL 三命令，对标各 Objects/*.cs 与 Resp 命令文件均在。确认无遗漏。
