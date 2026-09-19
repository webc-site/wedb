命令解析期 token 字面量无单点归属：wresp::cmd_strings 只承接了 CmdStrings 的输出半边，
COUNT / WITHSCORES / LIMIT 等在命令层逐处裸内联

来源：next/glm.design.md 第 9 轮条 2。取证基线：主仓绝对根 /Users/z/git/db/wedb，分支 dev，行号
按当下代码重取（票面部分行号已漂移，本单一律以重测为准）。

结论
C# 的 CmdStrings.cs 同时是输出帧常量与输入 token 常量的单点；rust 对位模块
/Users/z/git/db/wedb/wedb/wresp/src/cmd_strings.rs 只承接了错误文案与应答帧半边，输入侧 token
全仓零承接，逐处裸内联。判定成立且待做。

现状
- /Users/z/git/db/wedb/wedb/wresp/src/cmd_strings.rs:1 模块头自述「对标
  libs/server/Resp/CmdStrings.cs」，实际条目全为输出面：RESP_OK / RESP_PONG / RESP_EMPTYLIST /
  RESP_RETURN_VAL_* （:12-24）、pub/sub 帧头（:41-59）、错误文案（:280-289 等）；输入 token 只
  零星有 NOGET（:91）与 INFO 段字段名（:359-371），不成组。
- 输入侧裸内联实测（`grep -rn 'b"TOKEN"' */src`，剔除测试行后的生产位点）：
  COUNT 14 处 —— /Users/z/git/db/wedb/wedb/wcol/src/types/scan_input.rs:61、
  /Users/z/git/db/wedb/wedb/wcol/src/list/list_object_impl.rs:511（此处还按 C# 双写形态写成
  `== b"COUNT" || == b"count"`）、/Users/z/git/db/wedb/wedb/wnode/src/resp/
  rangeindex/resp_server_session_range_index.rs:494、resp/objects/list_commands/read.rs:137、
  list_commands/blocking.rs:70、:335、list_commands/slow.rs:374、:688、
  sorted_set_geo_commands.rs:323、sorted_set_commands/blocking.rs:175、:328、
  sorted_set_commands/slow.rs:839、array_commands.rs:99、vector/
  resp_server_session_vectors.rs:787。
  WITHSCORES 7 处 —— wcol/src/zset/sorted_set_object_impl.rs:538、
  wnode resp/objects/sorted_set_commands/write.rs:543、:762、:842、slow.rs:498、
  resp/vector/resp_server_session_vectors.rs:775、:1133。
  LIMIT 5 处 —— wcol/src/zset/sorted_set_object_impl.rs:516、
  wnode resp/objects/sorted_set_commands/write.rs:376、slow.rs:796、set_commands.rs:641、:1189。
  WITHSCORE 2 处 —— sorted_set_commands/read.rs:223、slow.rs:461。PERSIST 2 处 ——
  resp/basic_commands/get.rs:283、:284。TYPE 2 处 —— resp/array_commands.rs:110 与
  resp/client_commands.rs:33（后者已自造本文件私有 const FILTER_TYPE，正是「有单点意识但无处
  安放」的旁证）。WEIGHTS 1 处 —— sorted_set_commands/write.rs:804。WITHVALUES 1 处 ——
  resp/hash_commands.rs:487。MATCH / NOVALUES 亦同型散在 scan_input.rs:55、:73 与
  array_commands.rs:92。
- 后果：字面量跨 crate（wcol / wnode）散落，无一处改字面的入口；同一 token 在同步段与慢段各写一
  遍（如 WITHSCORES 的 write.rs:543 与 slow.rs:498），拼写漂移无人拦；违背 rust_review 与 refine
  的「一处定义」口径。

C# 参考
- /Users/z/git/db/wedb/garnet/libs/server/Resp/CmdStrings.cs:99 COUNT、:102 TYPE、:116
  WITHSCORE、:117 WITHSCORES、:118 WITHVALUES、:123 PERSIST、:154 LIMIT、:157 WEIGHTS（另有
  :98 MATCH、:101 NOVALUES），一处定义全仓共用；消费形态对照
  /Users/z/git/db/wedb/garnet/libs/server/Resp/Objects/SortedSetCommands.cs:847 直接用
  `CmdStrings.WITHSCORES`。
- C# 侧 COUNT/count、TYPE/type 这类大小写双常量（CmdStrings.cs:100、:103 相邻行）是为
  EqualsUpperCaseSpanIgnoringCase 之外的窄口径比较而存在；rust 统一走
  eq_ignore_ascii_case，只收一个常量即可，不必照抄双份。

修法
1. /Users/z/git/db/wedb/wedb/wresp/src/cmd_strings.rs 增设输入 token 常量组，沿用本文件既有命名
   与注释风格（每条带 `libs/server/Resp/CmdStrings.cs:COUNT` 形式的映射注释，便于 js/check.js 认
   账），类型用 `pub const TOKEN: &[u8] = b"COUNT";` 与既有帧常量一致。
2. 上述生产位点逐个替换为常量引用；wcol 侧两点（scan_input.rs:61、
   sorted_set_object_impl.rs:516/:538、list_object_impl.rs:511）本就依赖 wresp
   （/Users/z/git/db/wedb/wedb/wcol/Cargo.toml:25），无需新增依赖。
3. client_commands.rs:33 的私有 FILTER_TYPE 与 list_object_impl.rs:511 的 `count` 小写臂一并
   收编，避免留下第二套口径。
4. 只做字面量归位，不改任何比较语义与大小写策略；替换后按 SKILL:97 的口径跑一次
   ./js/check.js（注意其输出解读受 task/ing/garnet-scan-cs-corpus-parse-gate.md 所述语料失效影响）
   确认无新增重复定义。

优先级
重复机制（跨 2 crate 约 34 处同 token 复写，输入侧单点在 CmdStrings 对位模块整体缺席）。

边界
task/ing/set-exist-options-single-source.md 管 SET 族 ExistOptions 解析器的双轨（死单点 vs 手写
逻辑），是解析机制面；本单管跨命令族共享的输入 token 字面量归属，两者不重叠。输出半边的零星缺口
（RESP_QUEUED 裸写于 resp_server_session.rs:2829、RESP_ERR_NO_SCRIPT 裸内联于
wlua/src/commands.rs:207、ERR_SCRIPT_FLUSH_OPTION 以 crate 私有 const 绕开 wlua/src/strings.rs
单点表）与本单同属 cmd_strings 承接完整性，落地时一并按同一常量表收口。

分拣补记（next/agy.design.md 条 4 + next/muse.design.md 条 13 同题）：错误文案 const 自立增量——
六处文件级 const 复测在场且 cmd_strings 无同名承接：
wnode/src/resp/acl_commands.rs:34 RESP_ERR_ACL_FOREIGN_NAMESPACE、:37 RESP_ERR_ACL_GENPASS_BITS_RANGE、
wnode/src/resp/txn_resp_commands.rs:70 RESP_ERR_TRANSACTION_FAILED、
wnode/src/resp/array_commands.rs:36 RESP_ERR_LENGTH_AND_INDEXES（C# CmdStrings.cs:308 有原条）、
wnode/src/resp/objects/object_store_utils.rs:205 RESP_ERR_CORRUPT_PAYLOAD、
wnode/src/resp/objects/sorted_set_commands/mod.rs:29 RESP_ERR_MIN_OR_MAX_NOT_VALID_STRING_RANGE_ITEM
（pub(crate) 已被 read.rs/write.rs/slow.rs 三文件跨文件引用，自立单点实态成立）；
另 wnode/src/resp/garnet_api/slow.rs:61 RESP_ERR_CHECKPOINT_UNWIRED 为自造文案（C# 无对条），
按「迁入单点或注明本文件独有用」同口径处置。

盘点补记（qw13.invA cmd-strings-input-token-single-source）：dev e75716e 复核原样：wresp/src/cmd_strings.rs 仍只有输出帧常量、零输入 token 常量组；裸位点在场：wcol/src/types/scan_input.rs:61、wcol/src/list/list_object_impl.rs:511（b"COUNT"|b"count" 双写形态）、wnode/src/resp/rangeindex/resp_server_session_range_index.rs:495。机械收口定位不变。
