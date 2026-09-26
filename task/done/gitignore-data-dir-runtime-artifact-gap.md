归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 ee5c8ce+e93a371+ab28222（P4），收口形态：.gitignore 四条运行态排除＋replication.toml 摘跟踪；主代理扩案：同族 cwd 变体 wedb/wedb/data/ 两运行态并摘，中间斜杠根锚定坑以两精确条收口，check-ignore 亲验不误伤 bench/regress 基线。

甄别结论：通过（甄别席 zc-fix-r25-甲，2026-09-26）定级 P4
核验记录（现码复跑，非票面背书）：
1 .gitignore 现读全文 22 行零 data 面规则、零 .DS_Store 规则，票面「无任何 data 规则」成立；.DS_Store 仓库根实存、regress/data/ 现树已产 latest.json。
2 运行态跟踪复验：git ls-files 命中 wedb/data/Store/checkpoints/cluster/replication.toml，文件头注「Auto-generated, do not edit manually」与 primary_repl_id 进程态现读在位；生产链 wconf/src/node_options.rs DEFAULT_DIR="./data" 注释锚经审核席亲读。
3 基线不误伤复验：bench/data/latest.json 与 regress/data/latest.json 均在 git ls-files 在册（入库基线，拟加规则不触）；regress/config.json data_file 锚在位。
4 查重：与 task/ing/root-data-latest-json-orphan-baseline 票（本批同流）为联动非重复——彼票删顶层孤儿文件、本票补排除规则防再生成，两票均已注明 .gitignore 同批一次编辑协调；deviations.md gitignore 命中系他条内文非本面登记；四池无同轴票。
5 格式与可执行度：四条排除规则 + git rm --cached 单文件为最小改动，验证闭环（重启换号+回归后 status 干净、ls-files 归零、基线仍在库）；仓库治理票 C# 侧 N.A. 已注明依据。定级 P4：入库边界卫生。

审核结论：通过（zcode-r23-review-topology 独立复核，2026-09-26）
亲验复核：.gitignore 全文 23 行无任何 data 规则亲证；wedb/data/Store/checkpoints/cluster/replication.toml 被 git 跟踪亲证，头注 Auto-generated 与 primary_repl_id 进程态亲读；wconf/src/node_options.rs:49 DEFAULT_DIR="./data" 与 :56 注释「检查点目录默认 {dir}/Store/checkpoints」亲读；regress/config.json data_file="data/history.json" 亲读；.DS_Store 仓库根实存、git check-ignore 命中 ~/.gitignore:3 仅本机全局兜底（仓库自身无规则）亲证；bench/data/latest.json 与 regress/data/latest.json 两基线在库亲证，四条规则零误伤。
整理执行方案：
1 .gitignore 追加四条：/data/、/wedb/data/、regress/data/history.json、.DS_Store（/data/ 条同时承接 root-data-latest-json-orphan-baseline 票删除顶层 data/ 后的防再生成，勿重复加规则；与该票及 qoder 票的 .gitignore 触碰同批一次编辑）
2 git rm --cached wedb/data/Store/checkpoints/cluster/replication.toml（仅摘跟踪，磁盘文件保留供本地运行；tracked 文件不受新规则影响，须显式摘除）
3 验证：跑一次服务重启（repl_id 换号）+ 一次 ./regress.js 后 git status 仍干净；git ls-files 不再含 wedb/data 路径；bench/data/latest.json 与 regress/data/latest.json 仍在库

.gitignore 缺 data 运行目录排除，wedb/data/Store 运行态自动生成文件已被跟踪，回归与基准产物无隔离

问题分析：
1 Garnet 契约对齐（本票契约对象是仓库自身入库边界而非 C# 原型）。入库边界法定形态：源码、流程档、门禁脚本与两份基准基线（bench/data/latest.json、regress/data/latest.json，r18-bench 消费面在案）入库；构建产物（target/）、本地工具链（/sh、/garnet、node_modules、.cargo/）与临时输出（out.txt、*.log、check 日志族）排除。运行时数据目录（数据库存储树、检查点、回归历史）属本地运行产物，应与构建产物同列排除面。
2 工程现状确证。.gitignore 逐行核验无任何 data 目录规则。其一，wedb/data/Store/checkpoints/cluster/replication.toml 被 git 跟踪（git ls-files 亲证，72aeb39 init 压扁卷入），文件头注自陈「WeDB Replication State - Auto-generated, do not edit manually」，内含进程启动生成的 primary_repl_id 复制状态；其生产链在 wconf/src/node_options.rs DEFAULT_DIR = "./data" 与注释「本仓检查点目录默认 {dir}/Store/checkpoints」——任何 cwd 在 wedb/ 跑过服务或测试的会话都会生成该树。其二，regress/config.json data_file = "data/history.json"：每跑一次 ./regress.js 即在 regress/data/ 产生新 history.json，无排除规则。其三，.DS_Store 在仓库根实际存在（find 亲证），当前仅靠本机 ~/.gitignore 全局规则排除（git check-ignore 亲证），仓库自身无规则，克隆到未配置全局排除的机器即暴露噪音。
3 逻辑危害确证。其一，git status 常态污染：replication.toml 磁盘版与跟踪版当前碰巧同值（repl_id 复现）故 status 暂净，服务重启换号后必脏；regress/data/history.json 每次回归后新增 untracked。其二，误提交面：宽泛 message 提交先例在案（data/latest.json 即 message="." 入库），运行态文件极易被「.」式提交顺带走库，多机 repl_id 漂移制造无意义合并冲突。其三，克隆者带入无意义运行态：检查点复制状态对任何第三方机器均非可用数据，纯死重。

涉及代码：
rust 文件与函数：
.gitignore:全文（排除规则清单，无 data 面）
wedb/data/Store/checkpoints/cluster/replication.toml:全文（运行态自动生成文件，被跟踪）
wedb/wconf/src/node_options.rs:DEFAULT_DIR 与 DATA_FILE 常量注释（{dir}/Store/checkpoints 布局单源）
regress/config.json:data_file 字段（history.json 产物路径）

对应 c# 文件与函数：
N.A.（仓库入库边界治理票，无 C# 原型对应；对照依据为 Garnet 自身 .gitignore 排除运行产物目录的通行做法，以及本仓 /garnet 对照源码树按 .gitignore /garnet 整树排除的既定同构先例）

精炼执行方案：
1 .gitignore 追加四条：/data/、/wedb/data/、regress/data/history.json、.DS_Store（bench/data/latest.json 与 regress/data/latest.json 为入库基线不排除；tracked 文件不受新规则影响）
2 git rm --cached wedb/data/Store/checkpoints/cluster/replication.toml（仅摘跟踪，磁盘文件保留供本地运行）
3 测试验证点：git status 在「跑一次服务重启 + 跑一次 ./regress.js」后仍保持干净（除 task/ 池正常流动）；git ls-files 不再含 wedb/data 路径；基线文件 bench/data/latest.json、regress/data/latest.json 仍在库
