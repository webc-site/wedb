甄别结论：通过（甄别席 zc-fix-r16-benchtimeout，2026-09-26）定级 P1
真实性核验：bench/bench/src/engines/mod.rs:150 固定 Duration::from_secs(300) 与模式无关，亲验成立；:181-189 超时分支仅 eprintln+kill+wait+清 out_file/sub_work_dir，all_results 不 push 任何行，成立；:143-146 子进程 spawn 失败 continue 同样静默缺行，成立；main.rs:164-194 结果直进 latest.json 与 print_console_table，缺席零痕迹，成立。--large 为 10_000_000×1024B（main.rs:75-84）、standard_5m removals=2_500_000（types.rs:121），大模式慢引擎 300s 出局推演成立。ResultType 现仅 Throughput/Latency/SizeInBytes/NA 四变体（types.rs:129-138），无 Timeout 标记面，成立。
C# 侧亲验：garnet/libs/storage/Tsavorite/cs/benchmark/KV.benchmark/EntryPoint.cs:Run 主控 Load phase→可选 validate→Run sweep 逐相推进，每相即时 EmitResultJson/Csv；全目录 grep timeout/kill 仅 sigint/sigterm 收口一处，无超时杀进程机制，结果面不缺行，票面对齐描述属实。
非重复非灭失：doc/zh/deviations.md 无任何 bench 条目覆盖本案；task/ing、task/reject、task/done、task/issue 及其余五张 bench-* 票（parity-gap/read-loop/report-notes/alloc-tax/wbftree-flush）均不同轴；mod.rs:150 现码仍为固定 300s，缺陷现状仍存在。
架构合规：方案恢复与 C# 结果面完整性的对齐（不缺行），Timeout 以 ResultType 单点枚举变体落地、--timeout-secs 单参数、无新依赖无假桩，符合 transpile/rust_review 单向单机制要求。
可执行度与格式：改动点具体（engines/mod.rs 超时分支 push timeout 行 + types.rs 变体 + JsonMetric/控制台同源呈现），验证闭环（小超时触发断言 JSON 该引擎存在且标记超时）；纯文本、双侧代码路径齐全。

审核结论：通过，定级 P1。
确证评测引擎 300 秒超时后整行静默消失导致幸存者偏差，大模式下自动变为快引擎专场。方案提供动态超时与显式 timeout 状态行标记，方案正确。
复核（zcode-r18-review-benchmisc）：锚点全部亲验成立。engines/mod.rs:150 固定 Duration::from_secs(300) 与模式无关；:181-189 超时分支仅 eprintln + kill + 清 out_file 与 sub_work_dir，all_results 不 push 任何行；main.rs:164-194 结果直进 latest.json 与控制台对比表，缺席零痕迹（子进程 spawn 失败 continue 同样缺行）。--large 为 10_000_000 x 1024B（main.rs:75-84，约 10GB+），standard_5m removals 2_500_000（types.rs:121），大模式慢引擎超时出局推演成立。C# 侧 EntryPoint.cs:Run 主控逐相推进（Load phase → Run sweep），仅 sigint 收口，无超时杀进程机制，对齐描述属实。
执行方案补强：timeout 行建议直接以 ResultType 新增 Timeout 变体落地（非复用 NA），JsonMetric 的 r#type 输出 "timeout" 并附 duration_ms 为超时上限值，控制台表与 latest.json 同源呈现；超时预算按 cfg 推导时以 bulk_elements x value_size 与 num_reads 为基（各段字节量已现成），--timeout-secs 显式参数优先级最高。

合入哈希：d986b6e 收口形态：ResultType 新增 Timeout{limit} 单点变体，超时预算 cfg.timeout_secs 缺省按 bulk+removals+reads 字节量对 300s 基线线性推导、--timeout-secs 显式覆盖；run_all_engines 超时/异常/spawn 失败一律按 METRIC_KEYS（段落键单点定义，harness 逐段引用）补齐整列标记行，控制台表与 latest.json 同源呈现「超时(>Ns)」不缺行。

bench 引擎评测固定 300 秒超时后整行静默消失，幸存者偏差（r14-bench 第 5 条 P1 复查未整改）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# 侧基准主控全程跑完所有配置的工况，无超时杀进程机制，结果面不缺行：garnet/libs/storage/Tsavorite/cs/benchmark/KV.benchmark/EntryPoint.cs 主控逐相推进，KvBenchmark.Worker.cs 输出各相结果；缺席即异常中止，不存在慢引擎静默出局。上游 redb-bench 同样无引擎行缺失面。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   bench/bench/src/engines/mod.rs:run_all_engines 每引擎子进程固定 timeout = Duration::from_secs(300)（约 :150），与评测模式无关；超时分支仅 eprintln + kill + 清 out_file（约 :181-189），all_results 不push 任何行、不记 timeout 标记；main.rs 将 all_results 直接序列化进 latest.json 并打印对比表，缺席引擎无任何痕迹。--large 模式 1000 万条 x 1KB（约 10GB+）、--5m 模式 removals 250 万，慢引擎大概率超时出局。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
   大数据量模式下对比表自动变成快引擎专场，读者无从知晓缺席原因与缺席者，跨引擎结论系统性偏向快引擎；发布 JSON 同样缺行，事后无法审计该次评测是否存在超时出局。

涉及代码：
rust 文件与函数：
bench/bench/src/engines/mod.rs:run_all_engines（固定 300s 超时与静默缺席分支）
bench/bench/src/main.rs:main（结果直接进 JSON，无缺席标记面）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/benchmark/KV.benchmark/EntryPoint.cs:主控逐相全程执行
garnet/libs/storage/Tsavorite/cs/benchmark/KV.benchmark/KvBenchmark.Worker.cs:各相结果完整产出

精炼执行方案：
1. 超时上限随模式放大：--large/--5m 显式给出更大预算（或提供 --timeout-secs 参数，缺省按 cfg 数据量推导），避免固定 300s 一刀切。
2. 超时出局不缺行：为该引擎 push 显式 timeout 结果行（如 ResultType 新增 Timeout 变体或复用 NA 并在 JsonMetric 附原因），控制台表与 latest.json 同步呈现「超时」而非消失。
3. 测试验证点：以临时小超时参数触发单引擎超时，断言结果 JSON 中该引擎存在且标记为超时。
