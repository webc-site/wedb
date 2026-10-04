锁定注记（2026-10-01 r9 波主控，基线 `731dcd8`；本票由符号锚订正票的**副作用**派生，非新病灶）

审核结论：通过（2026-10-01 主控亲验立案；P3 工具面。判据落本仓门禁自身契约：`transpile` 技能明令「禁止简单地通过修改注释绕过检查」，故不得用回退行号锚的方式消音）

符号锚化后 check.js「重复定义」段浮出 6 组一:C# 函数 → N:rust 站点的合法分形，门禁缺「一对多白名单」机制，重复定义段长期非空

问题分析：
1. 门禁契约对齐：`js/check.js` 的重复定义判定按「rust 函数文档注释里的 C# `文件::符号` 锚」反查同名锚的多 rust 实现者，
   因此**同一 C# 函数被 rust 按 RESP 命令形态拆成多站点**时必报重复。历史上这 10 处锚写作 `文件.cs:<行号>` 形，
   反查不命中，段为空；`ab04502`（符号锚订正票）把它们改为 `::符号名` 后，合法分形集体浮出——
   即本票是符号锚制的**必然连带面**，不是回归缺陷，也不是重复实现。
2. 工程现状（6 组逐组对 garnet 现树亲验，全部合法 1:N，无一为真重复）：
   - `CollectionItemBroker.cs::TryGetNextListResult`（现树 :394 唯一实体，C# **无** `TryMoveNextListResult`）
     → `wcol::itembroker::collection_item_broker` 的 `try_get_next_list_item` 与 `try_move_next_list_item` 两站点；
   - `BasicCommands.cs::NetworkSETEX`（:533 单实体，形参 `bool highPrecision`；`RespServerSession.cs:822/824` 分别以
     false/true 承接 SETEX/PSETEX）→ rust `network_setex` + `network_psetex`；
   - `Objects/HashCommands.cs::HashSet`（单实体多命令分支 HSET/HSETNX/HMSET）→ rust `hash_set`/`hash_set_nx`/`hash_set_map`；
   - `Objects/ListCommands.cs::ListPush`（LPUSHX/RPUSHX 共用内核，见 `GarnetApiObjectCommands.cs:233-253` 多重载）
     → rust `list_push`/`list_push_x`；
   - `Objects/SetCommands.cs::SetIsMember`（`SetObjectImpl.cs:55` 单实现，SMISMEMBER 多值形态）
     → rust `set_is_member`/`set_multi_is_member`；
   - `test.hlog/MoreLogCompactionTests.cs::DeleteCompactLookup`（:52 单测试）
     → rust `more_log_compaction_delete_lookup` + `more_log_compaction_scan_mode_stage2_early_termination`
     （后者系 rust 自有提前终止优化段，注释已自陈）。
3. 危害确证：无运行时危害；属**门禁信号面**——`bun js/check.js` 现 EXIT=0 但「重复定义」段恒非空，
   使该段失去「有新真重复就变红」的信噪比，后续波次无法据段空判定收口完成。定 P3。

涉及代码：
js 文件与函数：
js/check.js（重复定义段的聚簇与打印点）
js/check/ignore/garnet/**（现有 ignore 语义只表达「无需实现」，承载不了「一对多合法分形」）

对应 c# 文件与函数：
libs/server/Objects/ItemBroker/CollectionItemBroker.cs:TryGetNextListResult、libs/server/Resp/BasicCommands.cs:NetworkSETEX、
libs/server/Resp/Objects/HashCommands.cs:HashSet、libs/server/Resp/Objects/ListCommands.cs:ListPush、
libs/server/Resp/Objects/SetCommands.cs:SetIsMember、
libs/storage/Tsavorite/cs/test/test.hlog/MoreLogCompactionTests.cs:DeleteCompactLookup

精炼执行方案：
1. 给 check.js 的重复定义段加**显式白名单**：ignore yml 里新增一节（拟名 `one_to_many:`，与既有 `functions:`/`files:` 平行），
   条目形如 `<C# 文件相对路径>::<C# 符号>` + `reason` + `rust_sites`（可选，用于校验白名单未遮蔽新站点）；
   聚簇时命中白名单的 C# 符号从重复定义段摘除，并在「已豁免」统计里计数，防止白名单烂尾无人知。
   **禁**把这类符号写进既有 `functions:` ignore（那会连带屏蔽「实现缺失」判定，属消音）。
2. 首批录入上述 6 组，理由逐条取自本票面（一 C# 内核按 RESP 命令形态/测试阶段拆多 rust 站点）；
   `more_log_compaction_scan_mode_stage2_early_termination` 一栏注明 rust 自有优化、非 C# 对位实现。
3. 验证面：`bun js/check.js` 改后要求「重复定义」段为空且「实现缺失」段仍为空、EXIT=0；
   追加一条最小自检（临时造一个不在白名单内的同锚双站点 → 段仍须报出），证白名单未过泛。
4. 禁做项：禁改任何 rust 注释/回退行号锚来消音；禁删 check.js 的重复定义段；禁放宽 ignore 的默认只读语义
   （`--prune-ignore` 落盘形态不动）。

终态注记（2026-10-01 闭环）：
1. 在 `js/check/ignore/one_to_many.yml` 引入 `one_to_many:` 白名单语法，登记 6 组合法分形；
2. `js/check.js` 扩展 `ignoreLoadAndPrune` 解析一对多白名单，`dupDefFind` 识别白名单并将重复项移至 `exempt_li`，输出已豁免统计；同时增加陈旧白名单检测，防止死白名单；
3. `js/check_selftest.js` 追加第 9 节自检断言，覆盖白名单解析、豁免与拦截；
4. 验证通过：`bun js/check_selftest.js` 41 项断言全绿；`bun js/check.js` 退出码 0，重复定义与缺失段清零。
