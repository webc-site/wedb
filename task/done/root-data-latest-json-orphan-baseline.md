归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 63a30f9（P4），收口形态：顶层 data/latest.json 孤儿基线摘除（f7872561 不可达亲验），防再生成由 /data/ 条承接。

甄别结论：通过（甄别席 zc-fix-r25-甲，2026-09-26）定级 P4
核验记录（现码复跑，非票面背书）：
1 孤儿态复验：git ls-files 顶层 data/ 仅 latest.json 一条；git log --all -- data/ 唯一提交 72aeb39 init；文件内 commit 字段 f7872561 经 git cat-file 判定对象不存在（较票面「不可达」更强）——压扁史遗留快照坐实。
2 零消费者复跑：全仓 grep "data/latest"（bench/regress/js/sh 侧）命中全部锚定 regress/src/bin/run.rs:357 CARGO_MANIFEST_DIR、regress/report.js:270 BASE_DIR、bench/js/lib/data.js:5 BENCH_DIR 三处，顶层 data/ 零读写方属实。
3 查重与联动：与 task/ing/gitignore-data-dir-runtime-artifact-gap 票（本批同流）为联动非重复——彼票补 /data/ 排除规则承接本票删除后防再生成，两票互相注明勿重复加规则；四池无同轴票。
4 格式与可执行度：git rm 单文件最小改动，验证闭环（回归/基准工具链行为零变化、ls-files 归零）；仓库治理票 C# 侧 N.A. 已注明依据。定级 P4：假基线误导属卫生面，无运行期危害。

审核结论：通过（zcode-r23-review-topology 独立复核，2026-09-26）
亲验复核：顶层 data/ 目录 git ls-files 仅 latest.json 一条亲证；文件内 commit f7872561 经 git cat-file 判定现史不可达亲证；全仓 latest.json 读写锚逐一亲读——regress.js:12 DATA_DIR="regress/data"、regress/src/bin/run.rs:357 CARGO_MANIFEST_DIR 编译期锚、regress/report.js:270 BASE_DIR 锚、bench/js/lib/data.js:5 BENCH_DIR 锚、bench/bench/src/main.rs:187-190 bench_dir（root_dir.join("bench")）.join("data")——全部锚定 bench/data/ 或 regress/data/，顶层 data/ 零读写方坐实。
整理执行方案：
1 git rm data/latest.json（目录随空自动消失；防再生成由 gitignore-data-dir-runtime-artifact-gap 票的 /data/ 规则承接，该票已注明联动，勿重复加规则）
2 复核 grep -rn "data/latest"（排除 node_modules/garnet/target）：命中应全部落在 bench/ 与 regress/ 侧，顶层引用归零
3 验证：./regress.js 与 bench 工具链行为零变化（读写锚均不经顶层路径，亲证在案）；git ls-files 顶层无 data 条目

顶层 data/latest.json 系零消费者孤儿基线，与 bench/regress 双真基线同名同构易误取

问题分析：
1 Garnet 契约对齐（本票契约对象是仓库自身基准基线布局而非 C# 原型）。基准基线法定双源各有精确锚定：regress 基线由 regress/src/bin/run.rs:357 以 CARGO_MANIFEST_DIR 锚定写 regress/data/latest.json 并被 regress/report.js:270 以 BASE_DIR 同锚读取；bench 基线由 bench/js/lib/data.js:5 以 BENCH_DIR 锚定读 bench/data/latest.json。两基线随各自工具链更新维护（r18-bench 已审 bench 侧口径债并立案在飞）。
2 工程现状确证。顶层 data/latest.json（800 字节，wbftree 四指标 + wkv 三指标）被 git 跟踪，唯一触碰提交为 72aeb39 init 压扁卷入，内容所记 commit f7872561 在现史不可达（压扁后哈希失效）。全仓消费者检索（*.rs、*.js、*.sh、*.toml、*.yml，排除 node_modules/garnet/target）：latest.json 的全部读写方均锚定 bench/data/ 或 regress/data/，顶层 data/ 无任何读写方，纯孤儿。
3 逻辑危害确证。其一，假基线误取：三份 latest.json 同名同构（metrics 键族相同），后续会话或报告工具按文件名 glob 检索（如「**/data/latest.json」）会把顶层陈旧快照（2026-09-12 口径，commit 锚死哈希）并入对比，产出虚假回归结论。其二，误导溯源：文件内 commit/message 字段指向不可达哈希 f7872561，按图索骥必落空。其三，目录占位：data/ 目录仅因此一文件存在，删后顶层布局收敛。

涉及代码：
rust 文件与函数：
data/latest.json:全文（孤儿基线快照）
bench/js/lib/data.js:DEFAULT_JSON_PATH（bench 基线读取锚，BENCH_DIR/data/latest.json）
regress/src/bin/run.rs:LATEST_JSON 常量（regress 基线写入锚，CARGO_MANIFEST_DIR/data/latest.json）
regress/report.js:latest_file_path（regress 基线读取锚，BASE_DIR/data/latest.json）

对应 c# 文件与函数：
N.A.（仓库布局治理票，无 C# 原型对应；对照依据为 regress 与 bench 两工具链对各自基线的显式路径锚定，顶层路径无任何锚定方）

精炼执行方案：
1 git rm data/latest.json（目录随空自动消失，.gitignore 追加 /data/ 与票 gitignore-data-dir-runtime-artifact-gap 联动防再生成）
2 grep 全仓「data/latest」引用面复核：命中应全部落在 bench/ 与 regress/ 侧，顶层引用归零
3 测试验证点：./regress.js 与 bench 工具链行为零变化（两者均不触碰顶层路径，路径锚亲证）；git ls-files 顶层无 data 条目
