归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 16d7a9f（P3），收口形态：check_commits.js 零语料 fail-closed（throw 上抛＋空语料红 exit 1＋fixed.yml 首跑提示），bun 三态实测。续排注：本票沙箱席与方案详情见下文。

甄别结论：通过（甄别席 zc-fix-r25-甲，2026-09-26）定级 P3
核验记录（现码复跑，非票面背书）：
1 误绿三面现读在位：getGarnetCommits catch 臂 console.error 后 return []（现树 :101-104）；零语料仍抵达成功文案「🎉 没有发现待对标的提交！所有关键提交均已在 fixed.yml 覆盖。」（:190-193）；顶层 main().catch(console.error) 异常后退出码 0（:216）——三路误绿现状成立，未灭失。
2 fixed.yml 空转复验：现树 ls 无 fixed.yml、git log --all -- fixed.yml 零记录——去重通道引用从未存在的工作产物，且每次恒打「已在 fixed.yml 记录: 0」无首跑提示，属实。
3 查重：deviations.md §146 案五系 check.js ignore 双轨裁决、案一二三 cargo_sh 退出码族，均不覆盖本脚本错误码面；四池无同轴票。
4 架构合规与可执行度：失败上抛/空语料硬红与同仓 check.js corpus_invalid 硬失败先例同口径（单套门禁纪律），fixed.yml 二选一收口防双态并存；验证点闭环（临时挪走 garnet 复现退出 1，票面已勘正 GARNET_DIR 为脚本内常量不走 env 注入）。定级 P3：工具防护面条件性误绿。

审核结论：通过（zcode-r22-review-shjs，2026-09-26）
亲验坐实：:101-104 catch return []、:190-193 空清单即达成功文案、:216 main().catch 后退出 0、:8-10/:24-28/:183 fixed.yml 链逐行核对；fixed.yml 现树缺席、git log --all 零记录、.gitignore 未列、全仓引用仅本脚本自身——三路（零语料/未预期异常/fixed.yml 缺席）误绿成立。查重：§146 案五系 check.js ignore 双轨摘除，r15/r21 档均未涉本脚本，无重复。
执行方案（整理版，供 fix.md 直接消费）：
1 getGarnetCommits 采语料失败不再 return []：失败上抛（throw）或返回 null，main 内语料空时打「语料未采到（garnet/ 缺失或 git log 失败），判定不可作数」stderr 红并 process.exit(1)；成功文案仅非零语料路径可达
2 main().catch 改 console.error 后 process.exit(1)
3 fixed.yml 通道二选一收口：保留则 ENOENT 时打一行「首次运行：工作产物 fixed.yml 尚未建立」，计数行旁标来源路径；废弃则整链删 fixed.yml 相关代码（loadFixedCommits、FIXED_YML_PATH、isFixed 判定与计数输出）
4 验证点：临时重命名 garnet 目录复现语料缺失（GARNET_DIR 系脚本内常量非环境变量，勿走 env 注入），断言退出 1 且无「已覆盖」字样；正常环境跑断言清单输出与计数一致

js/check_commits.js 零语料被渲染成「所有关键提交均已在 fixed.yml 覆盖」的误绿结论，且 fixed.yml 在现史从未存在致去重机制空转无警示

问题分析：
1 工具契约对齐。本工具定位是 garnet 上游提交对标甄别入口（脚本自述用法 bun js/check_commits.js 查看待核验的 Bugfix & Core 提交）。契约应为：语料可采时如实列待核验清单；语料不可采（git log 失败、仓库缺席）时必须显式红，不得输出成功结论；自身异常不得静默退出 0。
2 工程现状确证。其一，getGarnetCommits 的 catch 分支（js/check_commits.js:101-104）console.error 后 return []，main 不区分零语料与全覆盖，继续走到 :190-193 的成功文案「没有发现待对标的提交！所有关键提交均已在 fixed.yml 覆盖。」——garnet/ 缺失或 git 不可用时（init.sh 未跑的新环境、garnet/ 为坏克隆），退出码 0 且结论为全部覆盖。其二，main().catch(console.error)（:216）捕获任意未预期异常后仅打印，进程退出码仍为 0，误绿面同型。其三，FIXED_YML_PATH 指向仓库根 fixed.yml（:8-10），loadFixedCommits 对 ENOENT 静默吞（:24-28），现树该文件不存在、本史 git log --all 零记录、.gitignore 亦未列（非有意忽略），即「已在 fixed.yml 记录」去重通道自诞生起空转，而每次运行恒打「已在 fixed.yml 记录: 0」（:183）无任何「首次使用/工作产物未建立」提示；全仓无第二处引用 fixed.yml，属工具引用从未存在工作产物的漂移。
3 逻辑危害确证。甄别代理或人工在新环境跑本工具会拿到「0 待核验 + 全部覆盖」的绿色结论直接收工（误绿）；真实待对标的 garnet 修复提交被系统性漏筛，对标缺口以「已覆盖」假象存续。对照同仓门禁纪律（gateaudit 案一/二/三专修恒真退出码，deviations §146 在册），本工具是同族残留面。

涉及代码：
脚本文件与函数：js/check_commits.js:getGarnetCommits（:94-132，catch return []）、main（:134-214，:190-193 成功文案）、loadFixedCommits（:13-30，ENOENT 静默）、顶层 main().catch(console.error)（:216）

对照物锚：js/check.js 的语料失效硬失败先例（check.js:568-574 corpus_invalid 即 stderr 红 + process.exit(1)，miss 不同步、判定不作数）；unused.sh:36-49 检测器非零退出点名计红先例；deviations.md §146 门禁恒真退出码修复族（zcode-r139c-gateaudit 案一/二/三）

精炼执行方案：
1 getGarnetCommits 失败不再 return []：改为 stderr 红字 + main 内 allCommits.length === 0 时打印「语料未采到（garnet/ 缺失或 git log 失败），判定不可作数」并 process.exit(1)，成功文案仅余非零语料路径可达
2 main().catch 改为 console.error 后 process.exit(1)，杜绝异常退出 0
3 fixed.yml 不存在时打印一行显式提示（首跑建立工作产物），「已在 fixed.yml 记录」计数旁标注来源文件路径；或如该通道已废弃则整链删除 fixed.yml 相关代码，二选一收口不并存
4 测试验证点：临时移走 garnet/.git 或置 GARNET_DIR 为空跑 bun js/check_commits.js，断言退出码 1 且无「已覆盖」字样；正常环境跑断言清单输出与计数一致
