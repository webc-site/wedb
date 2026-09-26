甄别结论：通过（甄别席 zc-fix-r16-benchparity，2026-09-26）定级 P3
C# 侧亲验：KV.benchmark 六默认档全中（Options.cs :30 keys 1亿、:46 uniform/zipf、:58 warmup-sec 5、runsec Default=30、:64 batch-size 1024、:26 run-threads-sweep 1,2,4,8,16，README RUMD=reads/upserts/RMWs/deletes，KvBenchmark.Validate.cs 在位）；garnet/benchmark/Resp.benchmark/Program.cs、Tsavorite/cs/benchmark/Device.benchmark/BenchWorker.cs、BDN.benchmark 全家（Parsing/Auth/Bitmap/BfTree/Operations/Network 目录逐一点验）、playground 三件（Embedded.perftest、MigrateBench、LightEpochLitmus）、YCSB.benchmark 均在树。
rust 侧亲验：regress/benches/regress.rs 仅 bench_wkv_regression/bench_wbftree_regression 两函数（criterion_group :161 亲见）；regress/src/bin/run.rs assert_samples :91 与 black_box :156/:217/:269 在位；bench/bench/src/harness.rs run_benchmark 全文通读确为 uniform 插入/点读/扫描/删除，读结果 let _ 丢弃、无读写混合、无 RMW 工况、无 validate 回读；全仓 criterion 仅 regress 一 crate，无 divan/#[bench]，ycsb/zipf/RESP 端到端/wepoch/whlog/wdev 微基准零建，缺口现状仍在。
查重：grep doc/zh/deviations.md 零 bench 登记；task/issue、ing、reject 无同轴票；todo 内另五张 bench-* 票分别对位 r14 第 1/5/6/7后半/8-9 条，与本票第 10-15 条轴零重叠；YCSB/读写混合关键词全 task 池仅命中本票。
灭失核验：task/review_history/zcode-r14-bench.md 确不在树（r15-resil 末段在册留痕：d9bf03b 删四档含 r14-bench 176 行，process-commit-discipline 覆盖纪律面），缺口清单现仅存于本票与 zcode-r18-bench.md 第 26/50 行摘记，登记必要性成立；git 历史三提交之说因本席禁 git 未直接复跑，以 r15-resil 在案留痕背书。
架构与格式：方案沿用 regress 现有骨架（harness+criterion+run.rs 基线）单套机制不另起炉灶，属防护建设非行为偏差，不入 deviations 判定与前席门槛裁定一致；纯文本、双侧路径齐全、验证闭环（assert_samples 口径+history.json 门禁）成立，P3 恰当。

审核结论：通过，定级 P3。
确证 bench 对 C# Garnet 基准面存在对位缺口（YCSB 读写混合、RESP 端到端、wepoch/whlog 微基准未建）。方案沿用 regress crate 现有骨架按价值序补齐，方案正确。
复核（zcode-r18-review-benchmisc）：锚点全部亲验成立。C# 基准面五件在位（KV.benchmark 全套、garnet/benchmark/Resp.benchmark、BDN.benchmark/BfTree、Device.benchmark 位于 garnet/libs/storage/Tsavorite/cs/benchmark/Device.benchmark，playground/Embedded.perftest），另 YCSB.benchmark 亦在同目录在位，可作 YCSB 混合工况的直接对位参照；rust 侧现状属实：regress/benches/regress.rs 仅 bench_wkv_regression/bench_wbftree_regression 两函数（criterion_group :161），src/bin/run.rs assert_samples 与 black_box 口径在位；bench/bench harness.rs 全文亲读确为纯 uniform 插入/点查/扫描/删除，无读写混合、无 RMW、无 warmup 丢弃窗、无 validate 回读；r14 审查档被 d9bf03b 删除属实（git 历史仅 72aeb39/d9bf03b/67852ea 三提交，档案已从 HEAD 消失），缺口清单不落票即失传，立案必要性成立。
门槛裁定：不并入 deviations.md。本案非行为偏差登记，而是基准防护缺口的建设清单票；wresp 解析、wacl 鉴权、批量折叠、O(1) 计数规约等关键路径零基准守护是真实防护缺口，方案落点 regress crate 现有骨架（不另起新机制），够立案门槛，P3 定级恰当。
执行方案补强：YCSB 混合工况直接参照 garnet/libs/storage/Tsavorite/cs/benchmark/YCSB.benchmark 与 KV.benchmark 双源（zipf 参数化、读写比例、validate 回读三项为最小对齐集）；每件建成即在票面勾销该项，防止与 r14 档同型失传。

bench 对 garnet C# 基准面的对位缺口未登记，关键路径零基准守护（r14-bench 第 10-15 条复查：仅第 12 条部分落地）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# 基准面五件：KV.benchmark（1 亿键 RUMD 读写删混合、zipf/uniform、warmup 5s/run 30s、批量 1024 深度、validate 回读、线程 1-16 扫描）；Resp.benchmark（端到端 RESP 吞吐，线程 x 批量矩阵）；BDN.benchmark 微基准全家（命令解析、RESP 编码、对象命令、bitmap、鉴权等）；Device.benchmark（设备层读写吞吐）；playground 三件（内嵌 perftest、迁移、epoch litmus）。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   rust 侧现状：bench/ 为跨引擎对比壳（纯 uniform、无读写混合、无 RMW、无 warmup、无回读校验）；regress/ 已落地 wkv/wbftree 点写点读扫描删除的 criterion 微基准与 commit 基线（对位 BDN BfTreeOperations/BfTree 点查面，即 r14 第 12 条部分落地），但其余缺口全未建且全未登记：YCSB 风格读写混合（第 10 条）、RESP 端到端吞吐（第 11 条）、wepoch/whlog 微基准（第 13 条）、wdev 设备层直读写（第 14 条）、内嵌/迁移对位（第 15 条）。wresp 解析、wacl 鉴权、批量折叠、O(1) 计数规约等 SKILL 宣称的优化面改坏无报警。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
   与 C# 数字无互译锚点，自研优化（零拷贝读、前缀外提、无锁哈希）回归无哨兵；r14 审查档本身已被 d9bf03b 删除（process-commit-discipline 票在案），缺口清单若不落票据将再次失传。

涉及代码：
rust 文件与函数：
regress/benches/regress.rs:bench_wkv_regression/bench_wbftree_regression（已建面）
regress/src/bin/run.rs:main（commit 基线）
bench/bench/src/harness.rs:run_benchmark（对比壳，工况单一）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/benchmark/KV.benchmark/KvBenchmark.Worker.cs:RUMD 混合工况
garnet/benchmark/Resp.benchmark/Program.cs:端到端 RESP 吞吐矩阵
garnet/libs/storage/Tsavorite/cs/benchmark/Device.benchmark/BenchWorker.cs:设备层吞吐

精炼执行方案：
1. 按价值序补建（r14 既定顺序 10-13-11-14-15）：先 YCSB 风格 wkv 读写混合基准（zipf + warmup + validate，工况对齐 KV.benchmark 默认档），再 wepoch/whlog 微基准，再 RESP 解析/编码端到端微基准，wdev 与内嵌面视需要跟进。
2. 落点建议沿用 regress crate 现有骨架（harness + criterion + run.rs 基线），不另起新机制。
3. 测试验证点：每件基准带工况预校验断言（对齐 regress/bin/run.rs 现有 assert_samples 口径），基线入 regress/data/history.json 供 report.js 门禁对比。

合入哈希：be27f32 收口形态：件1-4 全收（YCSB/wepoch-whlog/RESP/wdev 四基准＋阈值校准），件5 内嵌面按票面『视需要』留观察
