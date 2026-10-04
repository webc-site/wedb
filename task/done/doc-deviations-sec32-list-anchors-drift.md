甄别结论：通过（甄别席 J7，2026-09-27，定级 P3——纯文档订正零行为，六连漂逐锚亲验全实）。条目 12 登 mod.rs:122 现为 list_save_or_gc 注释区，真位 parse_i32_pair_args :90-99、write.rs:180、slow.rs:410；13 登 slow.rs:234 现为 list_rmw_cold 签名区，LINDEX 真位 :333/:465、LSET 物层 list_object_impl.rs:310 strict_i32；14 登 write.rs:293 现落 Lrem 臂，真位 write.rs:139-149、slow.rs:397；15 登 mod.rs:60 注文档/:95 LTRIM 助手体/slow.rs:477 LINDEX 臂，真位 mod.rs:67 代理、shared_object_commands.rs:135/:166、blocking.rs:47/:276、slow.rs:656；16 登 slow.rs:322 现 try_tiered_arm 分派，真位 list_object_impl.rs:551。近 3h deviations 仅动 §163 不撞，五池无同订正在途。派沙箱席 c01o。

审核结论：通过（纯文档订正票。§32 条目 12-16 六连漂逐锚亲验全实：12 登 mod.rs:122 实为注释区，真位 parse_i32_pair_args:90-99/write.rs:180/slow.rs:410；13 真位 333/465、list_object_impl.rs:310；14 真位 139-149、slow.rs:397；15 真位 shared_object_commands.rs:135/166、代理 :67、blocking.rs:47/276、slow.rs:656；16 真位 :551。只漂行号不改判据，格式合规，五池无同订正在途，deviations.md git-clean 可落笔。勘误：票面 blocking.rs 函数名实为 list_pop_multiple 及阻塞臂，行锚正确，落笔按实名登符号锚）

deviations §32 条目 12-16 List 族消费点 Rust 侧行锚六连漂订正

问题分析：
1. Garnet 契约对齐：本条不涉及行为。deviations.md §32 b) 清单系严格整数文法
消费点单源台账，册内惯例为锚漂即订并注「原登…订锚」（条目 11 即 r118-triage
先例）。C# 侧条目 12-16 已订（各目均带订锚注），Rust 侧行锚未同步，现已全部
漂离所指解析点。
2. 工程现状确证（原登 → 现文实况 → 实测真位，均为本轮读码核验）：
条目 12 LTRIM：登 mod.rs:122，该处现为 list_save_or_gc 注释区；真位
  parse_i32_pair_args mod.rs:90-99（快慢径共用单源），消费点
  write.rs:180（快臂）/ slow.rs:410（慢臂）。
条目 13 LINDEX/LSET：登 slow.rs:234/389，该两处现为慢臂分派与 LPUSHX 臂；
  LINDEX 真位 slow.rs:333（分层臂）/465（慢物化臂）；LSET 实解析在物层
  wcol/src/list/list_object_impl.rs:310（strict_i32）。
条目 14 LPOP count：登 write.rs:293，该处已落入 list_remove（LREM）臂内；
  真位 write.rs:139-149（快臂门）/ slow.rs:397（慢臂门）。
条目 15 LMPOP/BLMPOP：登 mod.rs:60/95、slow.rs:477，mod.rs:60 系
  parse_lmpop_args 注文档，mod.rs:95 实为 parse_i32_pair_args（LTRIM/LRANGE
  助手），slow.rs:477 系 LINDEX 臂；真位 shared_object_commands.rs:135/166
  （parse_mpop_args 内 numkeys/count strict_i32），代理 mod.rs:67，消费点
  blocking.rs:47/276、slow.rs:656。
条目 16 LPOS RANK/COUNT：登 slow.rs:322，该处系 try_tiered_arm 分派调用点；
  RANK/COUNT 整数解析真位在物层 list_object_impl.rs:551
  （read_list_position_input），与条目 16 本目自述「C# 解析均在物层
  ListObjectImpl.cs:481/499」恰好双侧对位。
3. 逻辑危害确证：失真锚使后续 §32 族对账按图索骥落到错误命令区，可能误判
严格文法覆盖面或对着错误代码复勘起案，违背台账单源纪律；纯文档缺陷，无行为
危害。

涉及代码：
rust 文件与函数：
doc/zh/deviations.md §32 b) 条目 12-16
对照实现（只读不改）：
wnode/src/resp/objects/list_commands/mod.rs:parse_i32_pair_args, parse_lmpop_args
wnode/src/resp/objects/list_commands/shared_object_commands.rs:parse_mpop_args
wnode/src/resp/objects/list_commands/write.rs:list_trim, list_pop
wnode/src/resp/objects/list_commands/blocking.rs:list_mpop, list_blocking_mpop
wnode/src/resp/objects/list_commands/slow.rs:tiered_list_arm, 慢臂 Lindex/Ltrim/Lpop/Lrem/Lset/Lmpop
wcol/src/list/list_object_impl.rs:list_set, read_list_position_input

对应 c# 文件与函数：
libs/server/Resp/Objects/ListCommands.cs:ListTrim, ListIndex, ListPop, ListPopMultiple, ListBlockingPopMultiple, ListPosition
libs/server/Objects/List/ListObjectImpl.cs:ListSet, ReadListPositionInput

精炼执行方案：
1. 只改 doc/zh/deviations.md §32 条目 12-16 的 Rust 侧锚，照册内先例格式各
   注「原登…订锚」；为防再漂优先采用函数名符号锚辅以当前行号。
2. 建议新锚：条目 12 对 mod.rs:parse_i32_pair_args（快慢径共用，覆盖
   write.rs:180/slow.rs:410 两消费点）；条目 13 对 slow.rs 分层 Lindex 臂
   :333 与慢物化臂 :465，LSET 改登物层 list_object_impl.rs:310（与 C# 侧
   物层锚对称）；条目 14 对 write.rs:139-149；条目 15 对
   shared_object_commands.rs:135/166（代理 mod.rs:67）；条目 16 对物层
   list_object_impl.rs:551（命令层 LPOS 慢臂仅转调）。
3. 测试验证点：文档票零代码改动；rg 核验新锚行内容命中所述解析点即可；
   顺跑 wcol list 模块 strict_i32 相关锁测确认在册行为断言未受触碰。
