wext_json json_commands.rs 体量拆分与头标锚点口径归一（21 命令面里只有 2 条有 garnet 对位）

来源：next/qcode10.design.md 条 11（切片四「模块拓扑与体量」，MED）分拣立项，含该条的两步事实
（头标锚点声明与实现面不一致 / 单文件 1616 行按命令族拆分）。
取证基线：主仓 /Users/z/git/db/wedb 当下代码，行号为当下实测（原快照 /tmp/rev10 旧行号作废）。

现状
- 文件 /Users/z/git/db/wedb/wedb/wext_json/src/json_commands.rs 共 1616 行：
  命令枚举 `pub enum JsonCommand` :32-:56（21 变体：Set Get Del Forget Type MGet NumIncrBy
  NumMultBy Toggle StrAppend StrLen ArrLen ArrAppend ArrPop ArrIndex ArrInsert ArrTrim
  ObjKeys ObjLen Clear Resp）、`JsonCommandInfo` :58、`match_command_meta` :224、
  `match_command` :233、`pub const COMMAND_INFOS` :66（编译期命令清单表）、
  命令臂表 :338 起、`impl JsonCommands` :364-:1591（1228 行单 impl）、
  自由函数 encode_val_resp :1592-:1616；对外再导出
  /Users/z/git/db/wedb/wedb/wext_json/src/lib.rs:10-12
  `pub use json_commands::{COMMAND_INFOS, JsonCommand, JsonCommandInfo, JsonCommands, is_command_registered};`。
- 单命令体按三件套铺开（实测位点）：json_set_need_initial_update :571、json_set_updater :609、
  json_get_reader :684、json_del_need_initial_update :729、json_del_not_found :734、
  json_del_updater :738、json_type_reader :761、json_numincrby_updater :788、
  json_nummultby_updater :852、json_toggle_updater :916、json_strappend_updater :955、
  json_strlen_reader :1014、json_arrlen_reader :1048、json_arrappend_updater :1082、
  json_arrpop_updater :1140、json_arrindex_reader :1205、json_arrinsert_updater :1264、
  json_arrtrim_updater :1336、json_objkeys_reader :1423、json_objlen_reader :1462、
  json_clear_updater :1496、json_resp_reader :1551；
  共用闸口 helper：payload_is_empty :538、not_found_null :542、reject_read_only_initial :546、
  reject_read_only_update :551、reject_write_only :560、reject_write_missing :565
  （另有 :366-:381 的 need_initial_update/updater/reader/abort_with_error_message 四枚
  CustomObjectFns 契约入口，按 C# CustomObjectFunctions 三钩子形态，是 trait 面不可删）。
- 头标锚点与实现面不符（本票第一步的事实）：
  :1 `//! JSON 静态命令执行面（对标 modules/GarnetJSON/JsonCommands.cs 与 JsonModule.cs）`、
  :65 `/// 编译期命令清单（对标 modules/GarnetJSON/JsonModule.cs:OnLoad）`、
  :195 同件锚点 —— 三处口径暗示全文件对标 C#，而 C# 只注册 2 条命令：
  /Users/z/git/db/wedb/garnet/modules/GarnetJSON/JsonModule.cs:32-33 逐字为
  `context.RegisterCommand("JSON.SET", ...)` 与 `("JSON.GET", ...)`，
  /Users/z/git/db/wedb/garnet/modules/GarnetJSON/JsonCommands.cs 全文件 208 行、
  仅 JsonSET（:16）与 JsonGET（:88）两个 CustomObjectFunctions 子类。
  余下 19 条 JSON.* 属 RedisJSON 兼容扩展面，garnet 无对位实现，
  但按 /Users/z/git/db/wedb/.agents/skills/transpile/SKILL.md:14 的方针对扩展命令走
  编译期静态枚举 + 静态分发（rust 现形态即该方针，命令本身该留，不该虚挂锚点）。
- 在册拆分票核实：原报称 wext-json-path-module-split 在册，主仓已无该档案；
  json_path 侧拆分实际已落地（/Users/z/git/db/wedb/wedb/wext_json/src/json_path/ 已是
  expression.rs / filter.rs / mod.rs / parser.rs / path.rs 目录模块），
  故 json_commands.rs 是 wext_json 内唯一未拆的巨峰文件，本票不与任何在途票相撞。

C# 参考
- /Users/z/git/db/wedb/garnet/modules/GarnetJSON/JsonCommands.cs（208，仅 SET/GET 两类）
- /Users/z/git/db/wedb/garnet/modules/GarnetJSON/JsonModule.cs:32-33（注册面）
- /Users/z/git/db/wedb/garnet/modules/GarnetJSON/GarnetJsonObject.cs（459，对象与派发侧）
- /Users/z/git/db/wedb/garnet/modules/GarnetJSON/JSONPath/（十余件小文件，路径语言侧；rust 同构已落 json_path/）
  即 C# 的 JSON 模块没有一处把 20 个命令体写进一个文件。

修法（两步，禁混做）
第一步（锚点口径归一，独立小提交，先落）
1. /Users/z/git/db/wedb/wedb/wext_json/src/json_commands.rs:1 文件头改为两段式声明：
   对位面仅 modules/GarnetJSON/JsonCommands.cs:JsonSET / :JsonGET 与
   JsonModule.cs:32-33 的注册面；JSON.DEL 起 19 条为 RedisJSON 兼容扩展面，
   garnet 无对位实现，依 .agents/skills/transpile/SKILL.md:14 采编译期静态枚举分发。
   :65 与 :195 两处 COMMAND_INFOS 锚点同批改写（OnLoad 只注册 2 条，写清「本表 21 条 = 2 对位 + 19 扩展」）。
2. 禁为 19 条扩展命令虚构 C# 符号锚点（check.js 的符号存在性为硬失败，
   见 task/ing/cs-anchor-dup-single-mount.md 判据基准第 2 条）；
   扩展命令臂的 doc 只写 RESP 语义与参数口径，不挂 `路径.cs:符号`。
3. 若 /Users/z/git/db/wedb/js/check/ignore/modules.yml 侧因本文件对位声明变化而产生新的
   缺失/未消费报告（该档现 62 行、无 GarnetJSON 条目），在该档登记
   modules/GarnetJSON 的不映射范围与理由；不得为此新增别的 ignore 口径。

第二步（按命令族拆目录，纯搬运）
4. 改为目录模块 wext_json/src/json_commands/：mod.rs 留 use 头 + `mod` 声明 + `pub use` 重导出，
   使 lib.rs:10-12 的再导出与 wcustom 派发侧路径逐字不变。
5. 分文件（每文件对位 C# 单文件量级，目标 300-450 行）：
   - dispatch.rs：JsonCommand :32-:56、JsonCommandInfo :58、COMMAND_INFOS :66、
     name/arity/acl :266/:292/:338 诸表、match_command_meta :224、match_command :233、
     is_command_registered（现役位点随表搬位）
   - common.rs：CustomObjectFns 四契约入口 :366-:381 + 闸口 helper :538-:570
     （payload_is_empty / not_found_null / reject_* 六枚，禁在各族文件里各抄一份）
   - set_get.rs：json_set_* :571/:609、json_get_reader :684
   - mutate.rs：json_del_* :729/:734/:738、json_numincrby_updater :788、
     json_nummultby_updater :852、json_toggle_updater :916、json_clear_updater :1496
   - string.rs：json_strappend_updater :955、json_strlen_reader :1014
   - array.rs：json_arr* :1048/:1082/:1140/:1205/:1264/:1336 六臂
   - object.rs：json_type_reader :761、json_objkeys_reader :1423、json_objlen_reader :1462
   - resp_encode.rs：json_resp_reader :1551 + encode_val_resp :1592-:1616
   Forget / MGet 等无专属臂的命令按其现役 dispatch 表指向复用既有臂，dispatch 表整块留
   dispatch.rs，禁为拆文件而复制臂体。
6. 同一类型的多个 `impl JsonCommands` 块分居子模块即本仓既有形态（wcol / wkv 的目录模块族），
   禁新增中间类型或 wrapper；跨子模块取用的私有臂提 `pub(super)`，
   对外 pub 面（lib.rs:10-12 所列）不增不减。

优先级
第一步是污染扩散防堵（虚假锚点会误导后续审查轮与 check.js 判读），先落；
第二步是打磨（体量与拓扑对标 C#），随后同一子代理续做或另派皆可，两步禁在同一提交里混做。

协调
- task/ing/cs-anchor-dup-single-mount.md：锚点重复挂载票，其判据基准（一处挂载 + 其余散文表述）
  即本票第一步的写法依据；本票只动 wext_json，不改该票射程的 18 组。
- task/ing/resp-null-protocol-single-source.md：射程含 wresp 写面，本票第二部的 resp_encode.rs
  只搬 json 侧臂体，不动协议单点。
- task/ing/zero-consumer-dead-surfaces-batch-five.md 的「try_select_node 与 C# 同构不立项」结论
  在本票射程外（json_path/ 域，已落地拆分）。

验收
- 第一步：文件内 C# 锚点仅落在真有对位的位点（SET/GET/CustomObjectFunctions 三钩子/注册面），
  19 条扩展命令面无 `路径.cs:符号` 挂载；`bun js/check.js` 缺失与重复组数不增。
- 第二步：json_commands/ 下无单文件逾 500 行；
  `grep -rn "json_commands" wedb/wext_json/src wedb/wcustom/src wedb/wnode/src` 命中文件 diff 为空
  （mod 声明与 use 头除外）；臂体逐字搬运，无新增 allow、无新增 pub 面。
- 仅 `cargo check --workspace --all-targets`（私有 target 目录）零 error 零 warning；
  test.sh 与 clippy 由中央整合轮执行。
