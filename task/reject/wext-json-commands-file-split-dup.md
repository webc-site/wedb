重复：task/ing/json-commands-file-split-anchor-decl.md（关键符号 json_commands.rs 体量/头标锚点 2/21 命中）
优先级：中

11 [MED] wext_json/src/json_commands.rs 体量 + 头标 C# 锚点只覆盖 2/21 命令
问题：1616 行单文件，impl JsonCommands 从 :364 到 :1591，逐命令体按
`json_<cmd>_{need_initial_update,updater,reader}` 三件套铺开（:571 json_set_*、:684 json_get_*、
:729 json_del_*、:761 json_type_reader、:788/:852 num*、:916 toggle、:955/:1014 str*、
:1048/:1082/:1140/:1205/:1264/:1336 arr*、obj*/clear/resp），命令枚举 :32-:56 列 21 条；
文件头 :1 与 :65/:195 自称对标 modules/GarnetJSON/JsonCommands.cs 与 JsonModule.cs:OnLoad，
而 C# 侧只注册 2 条命令，19 条命令体系无对位；在册的 wext-json-path-module-split 只覆盖
json_path 子模块，本文件未在册。
rust：/tmp/rev10/wedb/wext_json/src/json_commands.rs。
c#：garnet/modules/GarnetJSON/JsonCommands.cs 全文件 208 行，仅 JsonSET（:16）与 JsonGET（:88）
两个 CustomObjectFunctions 子类；garnet/modules/GarnetJSON/JsonModule.cs:32-33 只
RegisterCommand("JSON.SET")/("JSON.GET")；模块余量分布在 GarnetJsonObject.cs（459）与
JSONPath/ 十余件小文件。
修法：二步归一（勿混做）——(1) 头标与 match_command_meta 处补「JSON.DEL 起 19 条为 RedisJSON
扩展面，garnet 无对位」的显式声明，或在 js/check/ignore 登记不映射，使锚点声明与实现面一致；
(2) 按命令族拆 wext_json/src/json_commands/{set_get,mutate,array,object,resp_encode}.rs，
每文件不逾 C# 单文件规模量级；与 json_path 拆票并单排期，勿两侧各搬一半。

---

浅核附记（拆票代理，主仓 dev 复核）
- 现状成立：主仓 /Users/z/git/db/wedb/wedb/wext_json/src/json_commands.rs 仍 1616 行单文件、
  未拆目录；头标 :1「对标 modules/GarnetJSON/JsonCommands.cs 与 JsonModule.cs」、
  COMMAND_INFOS 清单注释仍自称对标 JsonModule.cs:OnLoad；命令枚举仍 21 条。
- C# 对位在：/Users/z/git/db/wedb/garnet/modules/GarnetJSON/JsonCommands.cs 208 行
  （仅 JsonSET/JsonGET），JsonModule.cs:32-33 只注册 JSON.SET/JSON.GET。
- 查重：原在册的 wext-json-path-module-split 已完成落地（wext_json/src/json_path/ 已是
  expression/filter/parser/path 子模块目录，票据已从队列消账），json_commands.rs 本体
  无任何在册票覆盖。原修法中「与 json_path 拆票并单排期」一句随之失效：json_path 侧已拆完，
  本票独立排期即可。
- 协调：拆分只搬位点不动语义；头标/锚点声明一步若走 js/check/ignore 登记，
  与既有 ignore 块合并编辑，禁新建重复条目。
