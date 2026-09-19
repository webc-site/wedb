SUNSUBSCRIBE 自增命令补入命令目录：ACL 位 370 授权面与 COMMAND 族可见性收口

来源：next/glm.design.md 第 10 轮条 1，原票 task/ing/sunsubscribe-catalog-acl-gap.md。
分支 sunsub-catalog，基线 dev f536d49，合入 e5a01a4b（并 dev 6717367a）。

甄别
判定成立，且是「命令面有实现、目录面无登记」的多套口径缺口，非文档缺条。命令链三面全活：
wedb/wresp/src/command.rs:395 `Sunsubscribe = 370`、:420 `LAST_VALID_COMMAND`，
wedb/wnode/src/resp/parser/command_table.rs:213 解析表项，
wedb/wpubsub/src/session_commands.rs:329 `network_sunsubscribe`（含无参全退臂），
会话侧订阅态允许集与分派在 wnode/src/resp/resp_server_session.rs。目录面零登记：
grep -c -i sunsubscribe 在 wedb/wresp/RespCommandsInfo.json 与 wedb/wresp/RespCommandsDocs.json
均为 0（同族 SSUBSCRIBE 各 2 命中）。死路链路实测口径：ACL 按名授权经
wacl/src/acl_parser.rs:242 `lookup_command` → `catalog::try_get_by_cs_name`，类别授权经
wacl/src/user.rs `add_category` → `commands_for_category`，两者唯一数据源都是目录；位图长度由
wacl/src/command_permission_set.rs:29 `COMMAND_LIST_LEN`（取 LAST_VALID_COMMAND）定到含位 370，
判定在 :182 区段 `can_run_command` → `bit_on`，入册前位 370 无任何置位入口。

主代理取证摘要有一处需纠正：C# 侧该命令不在目录内。摘要给的
garnet/libs/server/Resp/RespCommandsInfo.json 路径不存在，C# 目录实为
garnet/libs/resources/RespCommandsInfo.json 与 garnet/libs/resources/RespCommandsDocs.json，
两处 grep -i sunsubscribe 同样 0 命中；C# 枚举尾值确为 QUIT（无 SUNSUBSCRIBE 位），官方兼容矩阵
garnet/website/docs/azure/api-compatibility.md:224 与 website/docs/commands/
api-compatibility.md:321 均标 ➖。故本票不存在「与 C# 那条逐字段对齐」的对位，按原票方案乙
（保留为已声明的自增扩展，与 RI.COUNT / CLUSTER|FLUSHALL_NS 同档）收口，字段口径来源取同族
SSUBSCRIBE 的 C# 目录条目 + redis 7.2 官方命令表 src/commands/sunsubscribe.json（arity -1、
flags loading/no-script/pubsub/stale、键规格 begin_search index 1 + find_keys range -1/1/0）。

目录未经别处承载：wedb/wresp/src/catalog/mod.rs:20/:23 单点 include_str! 两份 JSON，
ACL 目录 CmdEntry 由 :218 `project_entries` 先序投影，docs 面由
wnode/src/resp/resp_command_docs.rs 复用同源 JSON 并按外部命令名过滤——无分片文件、
无运行时合成，故补登记只需改这两份 JSON。

方案甲（删整链）不采：分片订阅退订是 redis 7 分片 pubsub 的协议配套，删链等于在已打通的执行链
上另开缺口，且与既有 SSUBSCRIBE / SPUBLISH 面不对称；按既有自增入册惯例登记即闭合，不新增机制。
ACL 侧未开白名单、未加特判分支、未新增第二套授权通路。

落地
1. wedb/wresp/RespCommandsInfo.json：按文件名升序在 SUNIONSTORE 与 SWAPDB 之间插根条目
   SUNSUBSCRIBE，Arity -1（本实现支持无参全退；与 C# UNSUBSCRIBE / PUNSUBSCRIBE 同值；
   会话侧 arity 判定约定见 resp_server_session.rs `is_command_arity_valid_checked`，
   -1 → 参数数 ≥ 0，不再误拒裸命令），Flags "Loading, NoScript, PubSub, Stale"，
   AclCategories "PubSub, Read, Slow" 与 FirstKey 1 / LastKey -1 / Step 1 + RO 键规格
   一律取同族 SSUBSCRIBE 口径（不引入第三种形态）。
2. wedb/wresp/RespCommandsDocs.json：同位置补文档条目，Summary 与 Complexity 用 redis 官方文案，
   Group PubSub，参数 SHARDCHANNEL 取 Optional + Multiple（对齐 UNSUBSCRIBE 的可选多值口径）。
3. 目录投影面计数锚定同步（三处快照，非肉眼）：wresp/src/catalog/mod.rs 头注 355 → 356、
   `catalog_size_and_aliases` 355 → 356 条 / 261 → 262 根 / 94 子不变，注释把 sunsubscribe
   并入 rust 扩展三条清单；wresp/src/catalog/commands_info.rs `tables_initialize_and_lookup`
   根 261 → 262、外部根 257 → 258，并钉 SUNSUBSCRIBE 的 command / arity -1 / PubSub 类别；
   wnode/src/resp/resp_command_docs.rs `tables_initialize_and_lookup` 256 → 257，
   并钉 SUNSUBSCRIBE 的 Group = PubSub。
4. wedb/wresp/src/command.rs:412 注释值修订：原写「枚举最大值 = SUNSUBSCRIBE = 369」而实际 370，
   改为 370 并注明 C# 尾值为 QUIT、位 370 属本仓自增且已入目录。
5. ACL 读目录侧只加校验不改语义：wacl/src/acl_parser.rs `try_parse_command_for_acl_cases`
   补 `try_parse_command_for_acl("sunsubscribe") == Some(RespCommand::Sunsubscribe)`；
   wacl/src/user.rs 新增 `sunsubscribe_acl_grant_paths`，经 `AclParser::parse_acl_rule` 走三条
   真实规则串（-@all +@pubsub / -@all +sunsubscribe / -@all +@pubsub -sunsubscribe），
   断言 `can_access_command(Sunsubscribe)` 依次通过 / 通过 / 撤销，并钉显式按名授权不牵连通族
   SSUBSCRIBE。

验收
门禁：cargo check --workspace --all-targets 在合入前基线（worktree 私有 target
/tmp/fork/sunsub-catalog/target）exit 0 零告警；bun js/check.js 在 worktree 内 exit 0，跑后
git status --porcelain 仅本票 8 个文件、ignore 语料零回写零剪枝。并入最新 dev 后复跑
cargo check --workspace --all-targets 为 exit 101，48 处 E0061 全部落在 wext_json 与
wext_roaring（json_object.rs:139 `try_get` 由 dev 提交 e3e7dddd 加了第 6 参 resp_version，
而 wext_json/tests/json_commands_test.rs:51 等调用面与 wext_roaring lib test 未跟上），
与本票文件零交叠、这些文件与 dev 逐字节同态，系 dev 既有红（当前 dev tip 仍红），
归 resp-null-single-source / fix-respnull-b 在飞代理收口，已上报主代理对账，本票不越射程修。
实测口径：cargo test -p wresp --lib catalog:: 15 passed / 0 failed（含改后快照与新增
SUNSUBSCRIBE 断言）、cargo test -p wacl --lib 28 passed（含 sunsubscribe_acl_grant_paths、
try_parse_command_for_acl_cases）、cargo test -p wnode --lib resp::resp_command_docs 2 passed，
三者均在并入最新 dev 后复跑通过。未跑 ./test.sh 与 ./sh/clippy.sh（主代理集中回归）。

目录条目数增量（脚本实测）：RespCommandsInfo.json 根 261 → 262（+1，恰为本票射程命令数 1）、
扁平条目 355 → 356、子命令 94 不变、外部根 257 → 258；RespCommandsDocs.json 256 → 257。
两文件 JSON 解析通过，既有条目相对序保持不变，新增名集合恰为 {SUNSUBSCRIBE}、删除集为空，
C# 侧两份目录仍 0 命中（自增命令，非对位补齐）。ACL 授予实测：入册前 +sunsubscribe 必报未知
命令、+@pubsub 不覆盖位 370；入册后两条入口均可授予且可撤销。

需对账与未做
1. 声明面登记未做（不在本票射程）：.agents/skills/transpile/SKILL.md 自增扩展清单无 SUNSUBSCRIBE
   条目、js/check/ignore 无该自增命令登记——gate-anchor 在飞代理正改 ignore 语料，本票不碰；
   理由文本可直接取本档「甄别」段。
2. 会话级端到端用例（受限用户 SSUBSCRIBE 后 SUNSUBSCRIBE 的协议往返）属
   next/resp-pubsub-acl-e2e-test-parity.md 的面，本票只补到 ACL 规则串→权限判定这一层；
   无 ACL 侧的退订行为已由 wnode/tests/resp_pubsub.rs:271 与
   wpubsub/tests/namespace_isolation.rs:289 覆盖。
3. 位 370 使 wacl 权限位图比 C# 多一位（C# LastValidCommand = QUIT = 369）。若后续按 SKILL:10
   「尽量 1:1」把自增命令回归到 C# 尾值之前，需同时改 LAST_VALID_COMMAND 与目录登记，
   两面不得各说各话。
4. 口径选择记录：AclCategories 含 Read（随同族 SSUBSCRIBE）而非 redis 的纯 {pubsub, slow}，
   故 +@read 档用户亦可退订自有分片订阅；键规格随 SSUBSCRIBE 声明，COMMAND GETKEYS SUNSUBSCRIBE
   由 RESP_INVALID_COMMAND_SPECIFIED 改为回显通道名（与 SSUBSCRIBE 同行为）。若主代理判定应回归
   UNSUBSCRIBE 无键规格形态，改一处 JSON 即可，快照计数不受影响。
