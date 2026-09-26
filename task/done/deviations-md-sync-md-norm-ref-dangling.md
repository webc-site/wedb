甄别结论：通过（甄别席 zc-fix-r25-甲，2026-09-26）定级 P4
核验记录（现码复跑，非票面背书）：
1 悬空引用现读在位：doc/zh/deviations.md:1105（§83 尾段）仍写「catch_unwind 属死机制，违 sync.md 单机制纪律」。
2 sync.md 不存在复验：find 全仓（排除 node_modules/garnet）零命中 sync.md；deviations.md 全册 grep "sync\.md" 命中 :1105 一处与 :1537 desync.md 文件名子串一处——票面「独立命中仅此一处」属实。
3 规范真源亲验：task/review.md:87「全链路唯一机制：同类功能只保留一套最优解…严禁引入双重机制」在位，改指目标真实存在。
4 83 条本体不受触：§83 对位面与守卫（ri.cachesize 4x 叶页）在册，本票仅句级改引不违 §83 严禁回改条款；catch_unwind 禁令本身不动。
5 查重：deviations.md 无第二处 sync.md 引用；task/{done,ing,issue,reject} 无同轴票。
6 格式与可执行度：句级订正、验证 grep 归零闭环、不新建规范文件防双源、纯文档不触 .rs。定级 P4：台账引用卫生。

审核结论：通过（r23-review-misc，2026-09-26）

亲验摘要：
- 全仓 find -name "sync.md" 零命中（doc/zh 仅 deviations.md/db.md/collection.md
  三件；.agents/skills/rust_review 与 transpile 目录亦仅 SKILL.md）；
  git log --all -- "*sync.md" 及 doc/zh/sync.md 零提交记录——从未存在、亦无
  删除史，票面非虚构。
- doc/zh/deviations.md:1105（83 条尾段）「catch_unwind 属死机制，违 sync.md
  单机制纪律」在位；全册 sync.md 独立命中仅此一处（:1537 系 desync.md 文件名
  子串），与票面 grep 结论一致。
- 规范真源实位：task/review.md 板块 1「全链路唯一机制：同类功能只保留一套
  最优解…严禁引入双重机制」。
- 83 条守卫本体 wedb/wnode/src/resp/range_index/resp_server_session_range_index.rs
  的 "ERR CACHESIZE must be at least 4 times the leaf page size" 在位，
  本票不触该机制。
- 查重：无既有登记；task/todo、task/reject 无同类票。

整理执行方案（供 task/fix.md 直接消费）：
1. doc/zh/deviations.md:1105「违 sync.md 单机制纪律」改指「违 task/review.md
   板块 1 全链路唯一机制纪律」，句级订正，条目其余内容不动。
2. 验证：grep -n "sync\.md" doc/zh/deviations.md 仅剩 desync.md 子串命中；
   纯文档改动不触 .rs、不新建任何规范文件（防双源）。

deviations.md 83 把「单机制纪律」规范依据指向不存在的 sync.md，规范引用悬空

问题分析：
1. Garnet 契约对齐：本票纯文档治理条，无 C# 行为对位面；83 条本体（RI.CREATE CACHESIZE 容量守卫）已在册且现码核验一致（resp_server_session_range_index.rs:104 守卫在位），不因本票动摇。
2. 工程现状确证：doc/zh/deviations.md 83 条尾段（现树 :1105）写「catch_unwind 属死机制，违 sync.md 单机制纪律」，把裁决禁令的规范依据指向 sync.md。全仓 find 无任何 sync.md（doc/zh 仅 deviations.md/db.md/collection.md 三件；仓根与 .agents 亦无）；单机制纪律的法定真源是 task/review.md 板块 1「全链路唯一机制：同类功能只保留一套最优解…严禁引入双重机制」。grep 全册 sync.md 独立命中仅此一处（另一次命中系 wtxn-wkv-keybucket-hash-scope-desync.md 文件名子串，非引用）。
3. 逻辑危害确证：治理面危害——其一，后续席循引查证 sync.md 必扑空，83 条「严禁复活 catch_unwind」禁令的权威链断裂；其二，若有人据悬空引用补建 doc/zh/sync.md，将与 task/review.md 形成两份规范真源（双源），违本仓单真源纪律；其三，外部读者无法复核该纪律出处。无运行期危害。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/range_index/resp_server_session_range_index.rs:RiCreateOptions::validate（83 条守卫本体，现码在位无缺陷，本票不触）

文档锚：
doc/zh/deviations.md:1105（83 条「违 sync.md 单机制纪律」悬空引用句）
task/review.md 板块 1「全链路唯一机制」（规范真源实位）

对应 c# 文件与函数：
garnet/libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRICREATE（83 条对位面，仅供互指，本票无 C# 侧对账）

精炼执行方案：
1. doc/zh/deviations.md 83 条该句的「违 sync.md 单机制纪律」改指「违 task/review.md 板块 1 全链路唯一机制纪律」，一字句级订正，不触条目其余内容。
2. 全册复查 sync.md 独立引用（排除 *desync.md 文件名子串）确认仅此一处，随订正归零。
3. 测试验证点：grep -n "sync\.md" doc/zh/deviations.md 仅剩 desync.md 子串命中；纯文档改动不触 .rs、不新建任何规范文件。

合入哈希：7aaf475 收口形态：§83 悬空引用一字句级订正改指 task/review.md 板块 1，全册独立 sync.md 引用归零，纯文档零触码
