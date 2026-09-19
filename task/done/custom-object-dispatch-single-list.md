扩展命令静态分发三轨并存：慢路径与 ACL 校验绕开 CUSTOM_OBJECT_ENTRIES 单点清单

来源：glm.my 第 5 条（分拣判定成立）。取证基线：主仓 HEAD 1b944517，行号为当下实况。

现状（同一件事三处各写一遍，扩展内部再各写两遍）
- 清单轨（唯一声称的单点）：/Users/z/git/db/wedb/wedb/wnode/src/resp/custom_objects.rs:19
  `CUSTOM_OBJECT_ENTRIES`（按 Cargo feature 组装的 const 清单，元素持
  `match_command` 函数指针与 `tag`）与 :45 match_custom_object_command；消费点仅
  /Users/z/git/db/wedb/wedb/wnode/src/resp/parser/resp_command.rs:47（快路径按名落槽）。
  模块头 :1-13 自述「server 层扩展对象分发面的单点……本清单加一行，不再触碰分发代码内部的标签比对臂」。
- 慢路径轨（绕清单）：/Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/slow.rs:1055-1076
  resolve_custom_object_command 手写 `#[cfg(feature = "roaring")] if let Some(cmd) =
  wext_roaring::RoaringCommand::match_command(name)` + json 同型第二支，并各自直呼
  `RoaringCommand::OBJECT_TAG` / `JsonCommand::OBJECT_TAG` 自取标签；消费点同文件 :484
  `C::Customobjcmd` 慢路径重放臂。
- ACL 轨（绕清单）：/Users/z/git/db/wedb/wedb/wnode/src/resp/acl_commands.rs:661-671
  `is_custom_command_registered: Some(|name| ...)` 闭包内再手写两支 `wext_roaring::is_command_registered`
  / `wext_json::is_command_registered`，供 ACL SETUSER 按名规则失败关闭门（同文件 :231 起消费）。
- 扩展 crate 内双名单：/Users/z/git/db/wedb/wedb/wext_roaring/src/roaring_bitmap_commands.rs:88
  is_command_registered 遍历 COMMAND_INFOS（:60-84 常量表），:116 match_command 又手写
  `(b"R.SETBIT", Self::SetBit)` 四元组表——同一命令名集在扩展内维护两份，加一条命令须同步两处。
- 连带文档面：/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/mod.rs:1-4 注释自认「与
  resolve_custom_object_command 同门」，即三轨并存已是仓内共识而非隐蔽事实。

后果
新增第三个扩展要改四处（清单 + slow.rs if 链 + acl 闭包 + objects/mod.rs cfg 门），新增一条命令要改
扩展内两处。任一侧漏改即产生「快路径能解析、慢路径重放不认」或「解析能认、ACL 认不出来」的权限/重放漂移，
且这类漂移是静默的（三轨各自返回 Option/bool，无交叉校验）。

C# 参考（单点形态）
- /Users/z/git/db/wedb/garnet/libs/server/Custom/CustomCommandManagerSession.cs:105-135：
  四个 Match（RawString/Transaction/Object/Procedure）全部单行转发 `customCommandManager.Match(...)`，
  快慢路径与校验面共用同一处查表；实现 /Users/z/git/db/wedb/garnet/libs/server/Custom/CustomCommandManager.cs:316-326。
- 规范源 /Users/z/git/db/wedb/.agents/skills/transpile/SKILL.md:14：删除动态注册管理层后以
  「编译期静态特性 + 静态枚举分发（enum / match / enum_dispatch）」承接——静态化不等于把一处定义
  裂成三轨，本票是收敛回单点，不是恢复运行时注册。

修法（三轨收敛为「清单 find_map + entry 字段直取」）
1. 清单项已具备承接全部三轨所需信息：/Users/z/git/db/wedb/wedb/wcustom/src/object_desc.rs:42-49
   CustomObjectEntry{tag, type_name, match_command: fn(&[u8]) -> Option<CustomCommandMeta>}，
   而 /Users/z/git/db/wedb/wedb/wcustom/src/ 的 CustomCommandMeta 已携带
   name/command_type/arity/fns（构造实见 roaring_bitmap_commands.rs:132-138 match_command_meta）。
   即 slow.rs 需要的 (command_type, fns, tag) 与 acl 需要的「名字是否登记」都能由
   match_custom_object_command 的返回值直取，无需新增字段、无需第二张表。
2. slow.rs resolve_custom_object_command（:1055-1076）改为转调 match_custom_object_command 后投影
   `(meta.command_type, meta.fns, entry.tag)`，删两支 cfg if 链与 OBJECT_TAG 自取；
   其 cfg 门（:1054/:1077 的 any/not(any(feature)) 两版函数）随清单本身退化——空清单即 None，
   两支 cfg 版可合一。
3. acl_commands.rs:661-671 闭包改为 `match_custom_object_command(name).is_some()`，
   不再直呼各扩展 crate；大小写不敏感语义由 match_command 单点承接
   （现 is_command_registered 走 eq_ignore_ascii_case，收敛后须逐字节保持）。
4. 扩展 crate 内两份名单归一：roaring_bitmap_commands.rs 的 COMMAND_INFOS（:58 常量表，
   供 :88 is_command_registered 遍历）与 :116 match_command 手写四元组表同源于
   `Self::name()`（:141-148 已存在，逐命令返回静态名）——match_command 改为按 name() 反查枚举、
   或 COMMAND_INFOS 由枚举全集派生，二者只留一份权威表；wext_json 同型改法。
5. 删 objects/mod.rs:1-4 注释里「与 resolve_custom_object_command 同门」的双轨自述
   （落地时按事实修订为「本模块只承执行体、名解析走清单单点」——该注释实为模块 cfg 门的
   同侧引用，门本身随执行面仍在，不作整句删除）。

优先级
重复/多套架构（一处定义裂为三轨 + 扩展内双表，收敛后新增扩展只改清单与扩展自身）。

协调
- 慢路径文件拆分票（garnet-api-slow-path-command-split，在册）与本票同文件 slow.rs，
  该票为纯移动拆分：两票若同棒开工，先落本票改道再拆，或先拆完在本票点名的新落点重取行号，
  严禁两边各改一半。
- 与 custom_object_commands.rs（objects/ 下扩展对象 RMW 分派）无冲突，本票不动执行体。

验收
- 分发/校验面（快路径解析、慢路径重放、ACL 按名门、清单项标签取用）的
  `wext_roaring::` / `wext_json::` 直呼点除扩展 crate 自身与清单定义处外归零。
  （本条按事实收窄，原「全仓直呼点归零」口径拒录理由见
  /Users/z/git/db/wedb/task/reject/custom-object-dispatch-single-list.md：
  `object_store_utils.rs` 的 `envelope_heap_estimate` roaring 记账臂属
  custom_objects.rs 模块头与 `wcustom::CustomObjectEntry` 既有裁定的设计内例外，
  并入清单须给清单项新增函数字段、且 JSON 侧无该能力，属扩范围非收敛。）
- 新增一条扩展命令只需改「枚举 + 扩展内单表 + 清单加一行」，`./js/check.js` 无新增缺失或虚构锚点。
- ACL SETUSER 对一个只存在于 COMMAND_INFOS 而未登记在 match 的名字须失败关闭（回归用例锁定单源）。
  （落地后此类名字结构性不存在：两表同源于枚举 `name()`，故改以「目录 ↔ 枚举全集条目数与
  逐名一致」用例锁定单源，见两扩展 crate 的 `directory_and_match_share_one_name_source`。）
- 快路径解析、慢路径重放、ACL 校验三面同名的判定结果一致性用例（含大小写混写）。

落地（dev 6ff28dc，worktree /tmp/fork/custom-object-dispatch-single-list，代码提交 ca97c51，
dev 对账合并 8d4e100 + 381fd90，私有 target /tmp/fork/custom-object-dispatch-single-list/target）

1. 慢路径轨删除：`StoreGarnetApi::resolve_custom_object_command` 两支 cfg 版（原 :1055-1076
   与 :1077-1091）整体移除，重放臂 /Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/slow.rs:494-506
   直呼 `custom_objects::match_custom_object_command` 后投影 `(meta.command_type,
   entry.tag.as_u8(), meta.fns)`，两支 `#[cfg(feature=...)]` if 链与
   `RoaringCommand::OBJECT_TAG` / `JsonCommand::OBJECT_TAG` 自取一并消失；
   `custom_objects` 与执行体同侧接线（同文件 :30 一条 cfg use，无新增门控轨）。
   函数不残留转发壳：唯一消费者即该臂，投影就地完成。
2. ACL 轨删除：/Users/z/git/db/wedb/wedb/wnode/src/resp/acl_commands.rs:705
   `is_custom_command_registered: Some(custom_objects::is_custom_object_command)`，
   原两支 `wext_*::is_command_registered` 直呼闭包删除；`ccm == null`（未启用任何扩展特性）
   跳过校验门的既有 cfg 分侧语义逐字保留（:703/:706 两臂不动）。
   新增清单布尔投影 /Users/z/git/db/wedb/wedb/wnode/src/resp/custom_objects.rs:64
   `is_custom_object_command`（= :48 match_custom_object_command 的 is_some 承接），
   大小写不敏感由 `match_command` 单点继承，与旧 `is_command_registered` 判定逐字节等价
   （旧实现的 `!name.is_empty()` 前置对空名与新路径同为 false，无行为漂移）。
3. 扩展 crate 双表归一（名集单源 = 枚举 `name()`）：
   /Users/z/git/db/wedb/wedb/wext_roaring/src/roaring_bitmap_commands.rs:101 新增
   `RoaringCommand::ALL`，:123 `match_command` 改为 ALL × name() 反查（删手写四条字节面）；
   :94 `is_command_registered` 降为 match_command 的布尔投影（不再遍历 COMMAND_INFOS）。
   /Users/z/git/db/wedb/wedb/wext_json/src/json_commands.rs:207/:258/:200 同型改法。
   `COMMAND_INFOS` 保留为命令目录（name/arity 本已单源自枚举，独有面为
   acl_categories/summary），其 C# 锚点 `RoaringBitmapModule.cs:OnLoad` /
   `JsonModule.cs:OnLoad` / `CustomCommandManager.cs:IsCustomCommandRegistered` 全部原位保留。
4. 死面随手清：删 `wext_json::JsonCommand::OBJECT_TAG`（本票收敛后全仓零消费者；
   roaring 侧同名常量保留，唯一消费者是既有例外的记账臂）。
   /Users/z/git/db/wedb/wedb/wnode/src/resp/objects/mod.rs:2-5 注释改为「本模块只承执行体；
   命令名解析与标签取用一律走 resp/custom_objects 清单单点」，对已删函数的悬空引用归零
   （全仓源码 grep `resolve_custom_object_command` 零命中，仅 gitignore 的
   `*.scip` 索引工件残留旧符号）。
5. 回归用例（锁定单源，本票只 compile-check，未运行）：
   /Users/z/git/db/wedb/wedb/wnode/src/resp/custom_objects.rs:69 起
   `unregistered_names_rejected_on_both_faces`（解析面与 ACL 门对未登记名一致拒绝）、
   `entries_cover_enabled_extensions`（清单条目数 = 启用特性数 + 标签→TYPE 反查同源）、
   `roaring_directory_matches_single_list` / `json_directory_matches_single_list`
   （扩展目录逐名在清单与 ACL 门命中，含 to_ascii_lowercase 混写）；
   扩展侧同名/同数锁定见 roaring_bitmap_commands.rs:490 与
   wext_json/tests/json_commands_test.rs:549 `directory_and_match_share_one_name_source`。
6. 验收读数：`wext_roaring::` / `wext_json::` 在 wedb/wnode/src 的直呼点收敛为三处——
   清单定义 custom_objects.rs:24/:26、清单测试 custom_objects.rs:123/:133、
   以及拒录档案记下的既有例外 object_store_utils.rs:560/:562（堆内存估算臂）；
   分发/校验面直呼归零达成。全仓 server 层再无 `R.SETBIT` / `JSON.*` 字面名单
   （grep 非扩展 crate、非测试侧零命中）。
7. 门禁（按 brief 只跑 cargo check，禁 test.sh/clippy/测试）：
   `cargo check -p wnode --all-targets` 于 `--all-features`、`--no-default-features`、
   `--no-default-features --features roaring`、`--no-default-features --features json`
   四组合 + `cargo check -p wext_roaring -p wext_json --all-targets` 全部 exit 0、
   零 warning 零 error（含 tests 面，合并 dev 后复跑一遍同样全绿）；
   改动文件 rustfmt（`--config-path wedb/rustfmt.toml --check`）七文件全净。
8. 协调兑现：本票先落，同文件 slow.rs 净减 25 行（+16/-41：两支 cfg 版函数删除、
   重放臂就地投影），next/garnet-api-slow-path-command-split.md 若开工须按新落点
   重取行号；未触碰 custom_object_commands.rs 执行体与任何 Cargo.toml 特性定义。
