甄别结论：通过（甄别席 zc-fix-r16-benchbarrier，2026-09-26）定级 P1
核验记录（现码逐点复跑）：rust 侧全锚亲验成立——harness.rs:202 let _ = reader.get、:228 let (scanned, _) 丢校验和、:295 计时窗外预热读、:301 计时窗内丢弃，均逐字命中；traits.rs:74-75 range_scan 文档明言「供防优化校验」确未兑现；bench/Cargo.toml 双 profile 均 lto="fat"+codegen-units=1；五引擎（fjall/redb/rocksdb/sqlite/wbftree）range_scan 真实累加 sum 而 wkv_engine.rs:223-225 为 (0,0) 空桩记 NA，与票面一致；上游锚 /Users/z/git/db/redb/crates/redb-bench/src/lib.rs:236-245 checksum 累加+assert_eq、:277 assert!(value_sum>0) 属实；regress/src/bin/run.rs black_box 于 :156,:217,:269,:289(:309) 在位，唯 bench/ 未改，缺陷现状仍存在。
C# 锚勘误一处（非承重）：票面「KvBenchmark.Worker.cs GlobalSetup Debug.Assert 预校验」现树不存在（KV.benchmark 全目录 grep 无 Debug.Assert/GlobalSetup，系 BfTreeOperations.cs 锚误移），但 Validate.cs 全量回读校验（--validate，经 EntryPoint.cs:117-126 接线）与 redb-bench 消费+断言两主锚亲验成立，契约对齐论断不依赖该误引锚，执行时不得再引用 Worker.cs 锚。
发布数据侧：bench/data/latest.json 实测 machine 仅 cpu/内存/disk_type 八字段、无 data_dir/data_fs、无 durability 块，而 zh.yml:64-92 已有完整 durability 注记与「逐事务 fsync 持久写」口径，注记与数据矛盾属实，重测重发为硬前置。
非重复非灭失：doc/zh/deviations.md 无 bench 条目覆盖；其余五张 bench-* 票（parity-gap/timeout/report-notes/alloc-tax/wbftree-flush）各轴不同，本案消费屏障+重测重发为独立轴。
架构合规：方案为 harness 单点补 checksum 累加+black_box 消费断言，沿用同仓 regress 与上游 redb-bench 既有口径，不引新机制、无假桩、无过度设计；可执行度闭环（反证自检+新 latest.json 字段核对+ops/s 下限校验）。
注：方案第 2 点「断言 sum 与写入期期望一致」宜按上游 value_sum 同款落为非零断言（扫描起点随机、逐键期望不可预知），执行席按此口径实现。

审核结论：通过，定级 P1（重测重发的强制前置）。复核席 zcode-r18-review-rdloop 独立亲验，并修正前审一处千倍单位误读。
代码缺陷属实：harness.rs:202 let _ = reader.get、:228 range_scan 校验和随 _ 丢弃（traits.rs:75 承诺「供防优化校验」未兑现，5/6 引擎侧真实累加的 sum 出引擎即死，wkv 为 (0,0) 空桩记 NA）、:301 计时窗内读结果丢弃（:295 为计时窗外预热读，同型但不涉计时），bench/Cargo.toml 双 profile 均 lto=fat+codegen-units=1，编译器有许可消除整条读链。方案 checksum 累加 + black_box 消费最小，对齐上游 redb-bench 与同仓 regress 口径，通过。
数字指控修正：原「0.51ns/次物理不可能」系 r14 起 duration_ms=50.894 被误读为微秒的千倍笔误（实为毫秒）。现架 wkv random_reads 实测 50.894ms/10 万次 = 509ns/次 ≈ 1.96M ops/s，物理合理，且与 regress wkv_get_hot 约 1.8M ops/s 互证；六引擎排序亦合理，无证据表明读链在已发布二进制中实际被消除，榜首数字不判假。
重测重发的真实依据（不受上条影响，仍为硬要求）：bench/data/latest.json 自 init 提交（72aeb39）未重发，系 r14 发现 2 sync 坍塌整改前的旧口径数据（individual_writes 等实为全非持久写），machine 无 data_dir/data_fs 字段、无 durability 块，与 bench/js/i18n/zh.yml 新持久化注记直接矛盾，必须按新口径重测重发；消费屏障必须先落地再重测，否则新数据带着同一编译器许可上架。

bench 读循环读结果无消费屏障，编译器可整链消除，发布假数字仍在架（r14-bench 第 1 条 P0 复查未整改）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   上游 redb-bench（crates/redb-bench/src/lib.rs:238-246）随机读循环有 checksum 累加与 expected_checksum 的 assert_eq 比对，范围扫有 assert!(value_sum > 0)，读工作以消费+断言方式钉死。C# 侧 garnet/libs/storage/Tsavorite/cs/benchmark/KV.benchmark/KvBenchmark.Validate.cs 全量回读校验（--validate 口径），KvBenchmark.Worker.cs GlobalSetup 亦有 Debug.Assert 预校验，杜绝基准测到空转。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   bench/bench/src/harness.rs:run_benchmark 三处读结果丢弃：第 6 节单线程随机读 let _ = reader.get(&key)（约 :202）、第 7 节 let (scanned, _) = reader.range_scan(...)（约 :228，traits.rs BenchReadTransaction::range_scan 文档明言返回值含校验和「供防优化校验」，承诺未兑现）、第 8 节多线程读 let _ = reader.get(&thread_key)（约 :295,:301）。harness 为泛型静态分发，bench/Cargo.toml profile.release 与 profile.bench 均 lto="fat" + codegen-units=1，编译器有完全许可删除整条读链。同仓 regress/ 已示范正确口径（regress/src/bin/run.rs 读循环 black_box(len) 于 :156,:217,:269,:289、benches/regress.rs 全 black_box），唯独 bench/ 未改。发布数据未重测重发：bench/data/latest.json 仍是整改前旧口径（无 durability 块、machine 无 data_dir/data_fs 字段，且系 sync 坍塌整改前所测，individual_writes 等实为全非持久写）；zh.yml notes 已按新持久化口径书写而数据未按该口径重测，注记与数据自相矛盾。注：前审所引「0.51ns/次物理不可能」系单位误读（duration_ms 50.894 实为毫秒，折 509ns/次，物理合理），已由复核席修正，详见顶部审核结论。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
   发布表「随机点查」与「范围扫描」数字处于编译器许可可消除状态（本次未消除不代表后续内联/重构后仍不消除，许可一旦兑现即整表失真且无告警）；对外评测页（bench/js/benchGen.js 喂 latest.json）持续传播 sync 坍塌旧口径数据（individual_writes 等标注持久写实为非持久写）；多线程 random_reads_N 行含预热期固定成本（发现 3 修复前所测），8/16/32 线程慢于 4 线程的负扩展曲线不可解读，须随重测刷新。

涉及代码：
rust 文件与函数：
bench/bench/src/harness.rs:run_benchmark（第 6/7/8 节读循环）
bench/bench/src/traits.rs:BenchReadTransaction::get/range_scan
bench/data/latest.json（待重测重发）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/benchmark/KV.benchmark/KvBenchmark.Validate.cs:全量回读校验
garnet/libs/storage/Tsavorite/cs/benchmark/KV.benchmark/KvBenchmark.Worker.cs:GlobalSetup 预校验（Debug.Assert 三连）

精炼执行方案：
1. 第 6/8 节读循环：get 返回值首字节累加 checksum，循环外 std::hint::black_box 消费并与写入期累计的 expected_checksum 断言相等（对齐 redb-bench lib.rs:238-246）；可顺带以计数断言命中率。
2. 第 7 节范围扫：range_scan 返回的 (scanned, sum) 双消费，断言 sum 与写入期期望一致（上游 value_sum 同款）。
3. 第 8 节每线程 checksum 线程本地累计后汇总主线程统一断言。
4. 修完全量重测并重发 bench/data/latest.json（新数据须含 durability 块与 data_fs 字段，与 zh.yml 注记口径一致）。
5. 测试验证点：bench/bench/tests 增防优化回归测试（已知值集读写 checksum 往返断言）；反证自检——临时注释掉读循环体内 get 调用重编，checksum 断言必须失败，证明屏障真实钉死读工作；重测后核对新 latest.json 含 durability 块与 machine.data_fs 字段、wkv random_reads 折算单次耗时不低于 regress/data/latest.json wkv_get_hot 量级的一半（约 0.9M ops/s 下限，旧值 1.96M ops/s 供参照）。

合入哈希：d531206 收口形态：harness 第 6/8 节读循环逐条累加 get 值首字节校验和与命中数、循环外 black_box 消费并断言与写入期同序列期望校验和相等（对标 redb-bench lib.rs:236-246,301-316），多线程段 worker 线程本地累加 join 汇总主线程统一断言、预热读结果 black_box 消费；第 7 节 range_scan (scanned, sum) 双消费按甄别裁定落非零断言，兑现 traits.rs「供防优化校验」契约；wkv_engine range_scan 弃 (0,0) 空桩，改锚定起始键记录地址沿 hlog 地址游标正向步进 count 条记录真实累加（对标 C# LogAccessor.cs:Scan，与 wnode scan_cursor 同源单机制，wkv 范围扫行由 N/A 转真实数据）；新增 tests/consume_barrier.rs 已知值集校验和往返三例，反证自检（移除循环内 get 断言必失败）与 release --quick 冒烟（lto=fat，--features wkv,redb）全过。遗留：latest.json 六引擎全量重测重发（durability 块与 data_fs 字段随前序票已在发布链路，禁 --quick 覆盖）留待主代理集成门禁统一执行。
