甄别结论：通过（甄别席 zc-fix-r25-乙，2026-09-26）定级 P3
核验记录：现码亲验——wedb/test.sh 现树 7 行、:7 exec cargo nextest 无门禁行；根 test.sh、.moon/tasks/rust.yml、.husky/pre-commit.js、.github/workflows 接线面 grep 零 feature.check.sh/allow.check.sh 调用；git log -- wedb/test.sh 唯一提交 72aeb39 init；78be682a/866bcfff git cat-file 均报 Not a valid object name（死哈希坐实）；两脚本仓库根在位。查重：deviations 全册零命中；四池无同轴票（task/issue/process-commit-discipline.md 系本票订正对象之纪律档非并案）。架构与可执行度：方案甲一行接线恢复+纪律档订正为最小改动、单机制，验证点闭环（删 feature 声明→nextest 前拦红），allow.check 挂接顺带裁决留槽合理；流程票不入运行时架构面。格式：纯文本、脚本侧锚齐全。定级理由：构建门禁防护面静默失效+纪律档虚假安心，属防护缺口非运行期缺陷，P3。

审核结论：通过（zcode-r22-review-shjs，2026-09-26）
亲验坐实：wedb/test.sh 现树 7 行 :7 为 exec cargo nextest run 无门禁行；git log -- wedb/test.sh 唯一提交即根提交 72aeb39（2026-09-26 13:34 init），其树内 test.sh 出生即无门禁行——init 时刻晚于纪律档（2026-09-24）「866bcfff 核验在位亲证」两日，压扁重建发生在亲证之后成立；78be682a/866bcfff 在主仓与全部 11 个 fork 工作树（/private/tmp/fork/*）git cat-file 均不可达，旧史对象整体不存，纪律档记录为门禁行曾在位的唯一残留证据；接线面（根 test.sh、.moon/tasks/rust.yml、.husky/pre-commit.js、三 workflow）grep 零 feature.check.sh 与 allow.check.sh 调用；deviations 无此次下线登记；r15-arch 排除项 d「allow.check 挂接属流程票管辖」原文核实。查重：task 四池无同面票。
执行方案（整理版，推荐方案甲，最小恢复既裁状态）：
甲 恢复接线（推荐）：
1 wedb/test.sh 在 . sh/env.sh 之后、exec 行之前加门禁行 ../feature.check.sh（脚本已 cd 自身目录 wedb/wedb，仓库根 feature.check.sh 即 ../feature.check.sh；feature.check.sh 自含 cd $DIR/wedb 与 cargo metadata，勿重复传参），失败码经 set -e 传播
2 process-commit-discipline.md 订正：78be682a/866bcfff 两死哈希改述「前史已压扁（72aeb39 init 重建），哈希不可达」，巡检项行号随恢复后实况更新，门禁「在位」宣称与现树重新对齐
3 allow.check.sh 是否同挂本票顺带裁决（同一门禁编排面，r15-arch 排除项 d 留槽在此兑现，避免另开流程票）；若同挂，加 ../allow.check.sh 于 feature 门禁行旁
4 验证点：任一 crate 删一个 feature 声明，跑 ./test.sh 应在 nextest 前被 feature.check.sh 拦红；恢复声明后 ./test.sh 全绿、门禁行巡检在位
乙 如确认下线为有意：走独立流程票正式裁决，改写纪律档方案 3 巡检项并删死哈希引用，deviations.md 补登记——甲乙二选一收口不并存

feature.check.sh 门禁行在 72aeb39 init 压扁重建时从 wedb/test.sh 静默丢失，流程票巡检项仍宣称在位且引用死哈希，allow/feature 双门禁现处零自动接线状态

问题分析：
1 门禁契约对齐。task/issue/process-commit-discipline.md 方案 3 明文：门禁行 wedb/test.sh:7 feature.check.sh 常态在位（巡检项），且「wedb/test.sh 与 feature.check.sh 门禁行对一切业务提交禁改，门禁调整须独立流程票承载」；task/fix.md 第 5 节以 ./test.sh 为主代理统一全量门禁。feature.check.sh 自述其检查面为逐 crate 单独 cargo check 揪 feature 漏声明（workspace 级统一构建查不出的 E0432 破口）——该检查面只有挂进全量门禁才兑现。
2 工程现状确证。现树 wedb/test.sh 全文 7 行，:7 为 exec cargo nextest run，无任何 feature.check.sh 调用行；git log -- wedb/test.sh 显示现史唯一触碰提交即根提交 72aeb39（init，压扁重建），文件出生即无门禁行。全仓接线面核查：根 test.sh（透传 wedb/test.sh）、.moon/tasks/rust.yml（仅 clippy task）、.husky/pre-commit.js（fmt+codegraph 面）、.github/workflows/{wedb.test,regress.test,rust-test}.yml（仅 nextest）均无 feature.check.sh 与 allow.check.sh 调用，两者现为纯手动工具。流程票宣称的恢复提交 866bcfff 与违规提交 78be682a 在现史均不可达（git cat-file 报 not a valid object name），即压扁重建发生在该票「门禁行在位亲证」之后，且无流程票、无 doc/zh/deviations.md 登记裁决此次下线。allow.check.sh 未挂门禁一节曾由 r15-arch 排除项 d 裁「流程票管辖」，但管辖该面的流程票自身巡检锚已失效。
3 逻辑危害确证。feature 漏声明破口（某 crate 引用门控模块却未在自己 Cargo.toml 点名 feature）回归时无任何自动门禁拦截，仅靠人工记得手跑；巡检项「常态在位」给出虚假安心，后续轮按票巡检会放行一个实际不存在的检查点——门禁面静默收窄正是流程票 3 所述「若非独立核验，检查面静默失效且无任何流程痕迹」的自体复发。

涉及代码：
脚本文件与函数：wedb/test.sh:7（exec cargo nextest，门禁行缺席）；feature.check.sh:54-65（逐 crate 循环与红点判定，无人调用）；allow.check.sh:14-27（同零接线）

对照物锚：task/issue/process-commit-discipline.md 方案 3 巡检项（宣称 wedb/test.sh:7 feature.check.sh 在位，引用 78be682a/866bcfff 死哈希）；git log -- wedb/test.sh（仅 72aeb39 init 创建）；.github/workflows/rust-test.yml（CI 仅 nextest）；.moon/tasks/rust.yml、.husky/pre-commit.js（均无门禁行）；doc/zh/deviations.md §146（无此次下线登记）

精炼执行方案：
1 恢复接线：wedb/test.sh 在 nextest 前加 ./../feature.check.sh 门禁行（相对布局按现脚本 cd $DIR 形态落位，路径以仓库根脚本为准），与流程票巡检锚重新对齐
2 或如确认下线为有意：走独立流程票正式裁决，同步改写 process-commit-discipline.md 方案 3 巡检项并删死哈希引用（78be682a/866bcfff 改述为「前史已压扁」），doc/zh/deviations.md 补登记，二选一收口不并存
3 allow.check.sh 是否同挂由该流程票一并裁决（r15-arch 排除项 d 已留槽，本票不重复立挂接案，仅以巡检锚失效事实触发）
4 测试验证点：改 wedb 任一 crate 删一个 feature 声明（如 wcol 漏 features 引用），跑 ./test.sh 应在 nextest 前被 feature.check.sh 拦红；恢复后门禁行在位巡检可通过

合入哈希：85f0d16 收口形态：方案甲恢复接线——wedb/test.sh:10-11 双门禁行（feature.check.sh＋allow.check.sh 同挂兑现 r15-arch 留槽）在位，纪律档死哈希 78be682a/866bcfff 改述前史压扁、巡检锚对齐 :10-11 实况；红例验证 wcol 摘 supervise→门禁 exit 1 拦红、恢复复绿；deviations §146 走恢复线零新增登记。
