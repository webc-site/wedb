归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 dd4d3af（P4），收口形态：根 review.md 旧版提示词孤儿摘除，task/review.md 审查流程唯一真源，全仓根路径引用归零。

甄别结论：通过（甄别席 zc-fix-r25-甲，2026-09-26）定级 P4
核验记录（现码复跑，非票面背书）：
1 孤儿态复验：git ls-files 命中根 review.md（现读 22 行，票面「25 行」系含尾换行计数差，不碍结论），唯一提交 72aeb39 init；内容现读为旧版「rust 代码审查员」七条提示词，与 task/review.md 三阶段五板块结构零重叠属实。
2 零引用复跑：AGENTS.md/task/fix.md/task/doc.md/.github/.husky/package.json/js/sh grep 根路径 review.md 零命中；deviations 与池内「review.md 板块 N」引用经抽验只匹配 task/review.md 板块结构，双真源分裂成立。
3 不可执行指令面现读坐实：旧版第 3 行「查看 git 从建仓到现在的 git diff」在压扁史（单根提交）下空转。
4 查重：四池无同轴票；与 task/ing/root-qoder-notes 票同为入库卫生族但对象各异非重复。
5 格式与可执行度：git rm 单文件最小改动、验证 grep 归零闭环；仓库治理票 C# 侧 N.A. 已注明依据。定级 P4：流程双真源卫生。

审核结论：通过（zcode-r23-review-topology 独立复核，2026-09-26）
亲验复核：根 review.md 25 行旧版提示词（自称 rust 代码审查员、仅 7 条要点）与 task/review.md 三阶段五板块零结构重叠亲证；唯一提交 72aeb39 git log 亲证；AGENTS.md/task/fix.md/task/doc.md/.github/workflows/.husky/package.json/js 引用检索零命中亲证；deviations.md 与 task/todo、task/reject 现存票全部「review.md 板块 N」引用（§205 板块 4.1、§692 3.2、§1843 3.1 等）抽验均只匹配 task/review.md 板块结构，根侧无板块编号，法定真源确系 task/ 侧，双真源分裂成立。旧版要求「查看 git 从建仓到现在的 git diff」在压扁史下不可执行亦亲证。
整理执行方案：
1 git rm review.md（仓库根），task/review.md 为唯一审查流程真源，内容零改动
2 复核 grep -rn "review\.md"（排除 node_modules/garnet/target/task/review_history）：命中应全部为 task/ 全路径引用或其板块简称，根路径引用归零
3 验证：git ls-files 根层无 review.md；AGENTS.md 与 task/fix.md 流程引用零变化（现状零引用亲证在案）

根目录 review.md 系旧版审查提示词孤儿，与 task/review.md 审查流程职责重叠构成双真源

问题分析：
1 Garnet 契约对齐（本票契约对象是仓库自身审查流程而非 C# 原型）。审查流程的法定单源是 task/review.md：三阶段生命周期（线索定位、提案起草、独立审核）、五板块九维度判定标准、格式铁律与分流操作（git mv 至 todo/reject）。deviations.md 全册对审查标准的引用（如 §205「review.md 板块 4.1 算术溢出保护」、§692「task/review.md 3.2」）所引板块编号结构只存在于 task/review.md；task/todo 与 task/ing 现存票据正文中 8 处裸写「review.md 板块 N」的表述经抽验全部指向 task/review.md 的板块结构。根 review.md 为 25 行旧形态：自称「rust 代码审查员」提示词，仅 7 条要点（fork.sh 修订、对标 garnet、clippy 等），无阶段流程、无板块判定标准、无票据模版与分流操作。
2 工程现状确证。根 review.md 被 git 跟踪（git ls-files 确证），唯一触碰提交为根提交 72aeb39（init 压扁重建卷入，git log 亲证）。全仓引用检索（AGENTS.md、task/fix.md、task/doc.md、.github/workflows 三工作流、.husky、package.json、sh/、js/、全部 *.md 除审查档自身）零命中：无任何脚本、流程、文档引用根路径 review.md。文件名与 task/review.md 同名异位。
3 逻辑危害确证。其一，双真源分裂：新会话或外部 AI 工具按文件名就近检索「review.md」时，根路径旧版优先命中，按旧 7 条口径（无阶段流程、无查重义务、无格式铁律、无 reject 分流）产出审查结果，出口与 task/review.md 法定流程分叉。其二，误导性指令面：旧版要求「查看 git 从建仓到现在的 git diff」——init 压扁后现史仅一笔根提交，该指令已不可执行，照做即空转。其三，审查档引用锚污染：后续票据若按文件名裸引「review.md」无法区分两份，对账成本放大。

涉及代码：
rust 文件与函数：
review.md:全文（25 行旧版审查提示词）
task/review.md:全文（法定审查流程单源，三阶段与五板块九维度）

对应 c# 文件与函数：
N.A.（仓库流程治理票，无 C# 原型对应；对照依据为 task/review.md 阶段三判定标准与 process-commit-discipline.md 提交流四则所引流程单源均在 task/ 侧）

精炼执行方案：
1 git rm review.md（根目录），保留 task/review.md 为唯一审查流程真源
2 全仓 grep 「review\.md」 引用面复核：命中应全部为 task/review.md 全路径引用或其板块简称，根路径引用归零
3 测试验证点：git ls-files 根层无 review.md；task/review.md 内容零改动；AGENTS.md 与 task/fix.md 流程引用不受影响（现零引用亲证）
