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

## 落地判词（开发棒 cmd-strings-token，基线 dev 1e3f06f → 合入前再取 dev a954680）

判词：成立，已落地。开工前对现刻 HEAD 逐条 Grep 工具复核（非 shell glob，防 cmd_strings.rs:362 那类
假阴性先例），票面主张全部在场，仅行号漂移；位点计数按重测。

主张复核（HEAD 实测行号 → 现刻行号）
- 输入 token 常量组缺席：成立。cmd_strings.rs 原表 623 行全为输出面，输入侧仅 NOGET（:93）与
  CONFIG 键名族（:365-377，票面「INFO 段字段名 :359-371」实为这族），不成组。
- COUNT 14 处 / WITHSCORES 7 处 / LIMIT 5 处 / WITHSCORE 2 处 / PERSIST 2 处 / TYPE 2 处 /
  WEIGHTS 1 处 / WITHVALUES 1 处 / MATCH、NOVALUES 同型散在：逐条命中，实际生产位点 28 处
  （票面 34 为含测试行的粗计；tests 与 #[cfg(test)] 段一律不动）。漂移样本：
  vectors.rs:775/:787/:1133 → :752/:764/:1110；sorted_set_commands/write.rs:543/:762/:842 →
  :559/:778/:858；slow.rs:498 → :489；list_commands/slow.rs:374/:688 → :356/:683；
  blocking.rs:175/:328 → :182/:342；range_index.rs:494 → :495。
- 后果段（同步段与慢段各写一遍、无改字面入口）：成立，WITHSCORES 的 write.rs:559 与 slow.rs:489
  即同 token 双写实例。
- C# 参考：CmdStrings.cs 实测 MATCH:98、COUNT:99、count:100、NOVALUES:102、TYPE:103、WITHSCORE:116、
  WITHSCORES:117、WITHVALUES:118、PERSIST:123、LIMIT:154、WEIGHTS:157 一处定义全仓共用。

落地形态
- wresp/src/cmd_strings.rs 增设「命令解析期输入 token 单点」组（MATCH/COUNT/COUNT_LOWER/NOVALUES/
  TYPE/PERSIST/LIMIT/WEIGHTS/WITHSCORE/WITHSCORES/WITHVALUES），每条带 `libs/server/Resp/CmdStrings.cs:<名>`
  全路径锚点，类型 `&[u8]` 与既有帧常量一致。
- 28 处生产位点全部改引常量；client_commands.rs 私有 FILTER_TYPE 删除（两处用法改 cs::TYPE）；
  比较语义与大小写策略逐处不变（`eq_ignore_ascii_case` / `equals_ignore_case` / `==` 原样保留）。
- 与票面 C# 参考「只收一个常量」的偏差：wcol/src/list/list_object_impl.rs:511 的 LPOS 选项解析按
  C# ListObjectImpl.cs:487 `SequenceEqual(COUNT) || SequenceEqual(count)` 精确双形态转写（该函数
  文档注释 :485-486 自述「混合形态报语法错误」），改 ignore-case 会放宽词法、违 修法4「不改比较语义」，
  故该处保留双臂并另收 COUNT_LOWER 一份，其余 token 均只收大写一份。
- 单站点 token 未收口（判为射程外）：RANK/rank、MAXLEN/maxlen（仅 list_object_impl.rs 同函数各 1 处）、
  BYSCORE/BYLEX/REV（仅 sorted_set_object_impl.rs 各 1 处）、EX/PX/EXAT/PXAT（仅 get.rs）——
  无跨点复写，不构成本单「重复机制」。

分拣并入条 3/4 裁决（按行粒度，实测真重复仅一处）
- 已收口：wnode/src/resp/objects/sorted_set_commands/mod.rs:29 的
  RESP_ERR_MIN_OR_MAX_NOT_VALID_STRING_RANGE_ITEM（整帧 &[u8]）与 wresp 侧
  cmd_strings.rs:143 RESP_ERR_MIN_MAX_NOT_VALID_STRING（同文案 &str）为全表唯一同形复写；
  删旧定义不留 shim，read.rs:200、write.rs:176、slow.rs:347/:436 四处改
  `cs::write_error_raw(output, cs::RESP_ERR_MIN_MAX_NOT_VALID_STRING)`，成帧字节逐位不变
  （write_error_raw 即 `-"<msg>"\r\n`，见 cmd_strings.rs 自检 write_error_raw_frames_message）。
- 不动（cmd_strings 零同名/零同文案，搬入即凭空造单点、非消重）：wnode/src/resp/acl_commands.rs:34
  RESP_ERR_ACL_FOREIGN_NAMESPACE、:37 RESP_ERR_ACL_GENPASS_BITS_RANGE、
  wnode/src/resp/txn_resp_commands.rs:70 RESP_ERR_TRANSACTION_FAILED、
  wnode/src/resp/array_commands.rs:36 RESP_ERR_LENGTH_AND_INDEXES（已带 C# 锚点、单点自洽）、
  wnode/src/resp/objects/object_store_utils.rs:205 RESP_ERR_CORRUPT_PAYLOAD、
  wnode/src/resp/garnet_api/slow.rs:61 RESP_ERR_CHECKPOINT_UNWIRED。

边界段输出半边三缺口（同表收口，均已落地）
- RESP_QUEUED：C# CmdStrings.cs:193 有原条、rust 全仓无常量，唯一生产位点
  wnode/src/resp/resp_server_session.rs:2856 裸写 b"+QUEUED\r\n" → 增设 RESP_QUEUED 并引用
  （wtxn_test/src/lib.rs:108 同形态但属测试门面且不依赖 wresp，不动）。
- RESP_ERR_NO_SCRIPT：C# CmdStrings.cs:293，与 cmd_strings.rs:190 的 RESP_ERR_NOSCRIPT（C# :207
  另一条）非同值，故分立新常量；wlua/src/commands.rs:201 裸内联改引。
- ERR_SCRIPT_FLUSH_OPTION：C# CmdStrings.cs:306 RESP_ERR_SCRIPT_FLUSH_OPTIONS；wlua/src/commands.rs:26
  crate 私有 const 删除、两处用法改引 wresp 单点（未落 wlua/src/strings.rs，该表专承
  LuaRunner.Strings.cs 常量族）。

门禁与验收
- `cargo check --tests -p wresp -p wnode -p wcol -p wlua` exit 0（私有 target /tmp/ct-cmdstrings）。
  注：首轮合入前 dev 一度因 0b318df「wip 合并前主仓快照」致 wkv/src/session/consistent_read.rs:146
  类型不匹配挡住 wnode 编译，后续棒已自行修复，与本单无关。
- 验收 grep（`b"(COUNT|WITHSCORES|LIMIT|WEIGHTS|WITHVALUES|WITHSCORE|MATCH|NOVALUES|PERSIST|TYPE)"` 限 */src）：
  余 16 命中全为 cmd_strings.rs 的 10 条新定义本体 + 6 处测试行，生产位点零命中。
- `bun js/check.js`（树内跑）：改动前后「重复定义」键集合逐字节相同（15 条，均为存量），
  「虚构锚点」A 层零命中，ignore 语料零回写；「实现缺失」段差量全部来自回合进来的 dev 他人提交。
- 与 task/done/scan-type-case-forms.md 的边界：本单只动 parse_scan_filter 的 param 面（选项名），
  其 type_arg 取值面（C# DbScan 双形态精确比对）归该票，两处改动在 array_commands.rs 行位相邻、
  回合后并存无冲突。
