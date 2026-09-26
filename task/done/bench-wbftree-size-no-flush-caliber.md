甄别结论：通过（甄别席 zc-fix-r16-benchflush，2026-09-26）定级 P2
核验记录（现码逐点复跑）：rust 侧全锚亲验成立——traits.rs:21 flush 默认空操作；wbftree_engine.rs:67-84 impl BenchDatabase 仅覆写 name/connect/compact（compact 恒 false），无 flush；harness.rs:342-344 drop(conn)→db.flush()→database_size 逐字命中（:360 内存段前二次 flush）；types.rs:376-378 JsonDurability wbftree 自述「不支持 set_sync，全程缓冲池批量落盘」。wbftree 公开面无 flush/drain 确证：service/ops.rs pub fn 仅 insert/read/contains_key/read_into/delete/scan 族，service/mod.rs:202 dispose 仅 tree.swap(None) 摘 Arc，manager 侧仅 snapshot_under_claim（manager/mod.rs:166），全 src grep 无 pub fn flush/drain；上游 bf-tree 0.5.6 亦无公开页池 flush API（仅 config.flush_interval 属 WAL 项），CircularBuffer::Drop 仅 dealloc 不落盘——缓冲滞留致口径偏小推演机制成立。zh.yml:85「结束统一执行 flush」与引擎 no-op 矛盾属实。C# 锚亲验：BfTreeOperations.cs GlobalSetup :39-40、Debug.Assert 三连 :84-89、GlobalCleanup tree?.Dispose() :92-95、第二夹具 :156；上游 redb-bench lib.rs:566-577 compact 后量 compacted_size 属实。
非重复非灭失：doc/zh/deviations.md 无覆盖条目（flush 相关仅 §27 检查点恢复，不同轴）；其余五张 bench-* 票各轴不同；现码缺陷未收口，latest.json:241 wbftree uncompacted 行仍无口径注记。
架构合规与可执行度：方案双路径互斥择一（服务层 flush 单点或口径注记收口）不引第二机制，且票面 r18 补强已含写路径二分预判步骤兜住 bf-tree 无公开 flush 的落地风险；验证闭环成立；纯文本、双侧路径齐全。执行时 zh.yml:85 宣称与 JsonDurability 须随所选路径同步收口。

审核结论：通过，定级 P2。
确证 wbftree 引擎尺寸测量前未覆写 flush，存在用户态缓冲池批量落盘脏页未刷出的口径偏小隐患。方案覆写 flush 或在发布表显式注明口径，方案正确。
复核（zcode-r18-review-benchmisc）：锚点全部亲验成立。traits.rs:21 flush 默认空操作；wbftree_engine.rs impl BenchDatabase 块（:67-84）仅覆写 name/connect/compact（compact 恒 false），无 flush；harness.rs:341-344 drop(conn) 后 db.flush() 再 database_size 量目录，对 wbftree 为 no-op；wbftree 公开接口面亲查无 flush/drain（service/ops.rs 仅 insert/read/delete/scan 族，service/mod.rs:202 dispose 仅 swap 摘除 Arc 无脏页排空，manager 侧仅 snapshot_under_claim 快照固化）；types.rs JsonDurability wbftree 自述「不支持 set_sync，全程缓冲池批量落盘」。另获一强证：zh.yml:85 durability_li wbftree bulk 行自宣「结束统一执行 flush」，与引擎实际 no-op 直接矛盾，本案执行时该宣称须一并收口。C# 侧 BfTreeOperations.cs GlobalSetup（:39-40）Debug.Assert 三连预校验、teardown tree?.Dispose()（:92-95）确定态口径描述属实。
执行方案补强：先以二分快速判定写路径形态（quick 模式插入后立即量目录，隔秒复量，目录字节仍增长即存在异步滞留）再选路径 1 或 2，判定结论写进票面防止后续复查重查；若走路径 1，flush/drain 落 BfTreeService（单点，禁在 bench 侧绕过引擎私下等待）；若走路径 2，zh.yml:85 的「结束统一执行 flush」宣称必须同步改写为实际口径，消除文档与实现的矛盾。

bench wbftree 引擎尺寸测量前无强制落盘点，uncompacted/compacted 口径与其它引擎不可对齐（r14-bench 第 7 条后半复查未整改）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# BfTree 基准在生命周期端点显式固化/收尾：garnet/benchmark/BDN.benchmark/BfTree/BfTreeOperations.cs 的 GlobalSetup（约 :39-95）对插入结果做 Debug.Assert 三连预校验、 teardown tree?.Dispose()（约 :95,:156）显式收尾释放，任何测量均在数据固化后的确定态上进行；上游 redb-bench 测 compacted_size 前显式 db.compact()，全引擎统一「固化后再量」。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   bench/bench/src/traits.rs:BenchDatabase::flush 默认空操作（约 :21）；bench/bench/src/engines/wbftree_engine.rs:WbftreeEngine 只覆写 name/connect/compact（compact 恒 false），未覆写 flush，harness 第 10 节 drop(conn) 后 db.flush() 对 wbftree 为 no-op，随即 database_size 量目录。types.rs JsonDurability 自述 wbftree「不支持 set_sync，全程缓冲池批量落盘」，即写路径存在用户态缓冲批量刷盘语义，尺寸测量前无显式触发点；wbftree 引擎侧亦无公开 flush/drain 接口（BfTreeService 仅 dispose，RangeIndexManager 仅 snapshot_under_claim）。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
   wbftree 的 uncompacted_size 可能缺未刷出的尾批数据而偏小，与其它引擎「flush 落盘后量目录」口径不可对齐，发布表磁盘占用横比失真；compacted_size 行虽诚实记 N/A，但 uncompacted 行的口径差异无任何标注。

涉及代码：
rust 文件与函数：
bench/bench/src/engines/wbftree_engine.rs:WbftreeEngine（未覆写 flush）
bench/bench/src/traits.rs:BenchDatabase::flush（默认空操作）
bench/bench/src/harness.rs:run_benchmark 第 10/11 节（drop 后 flush 再量目录）

对应 c# 文件与函数：
garnet/benchmark/BDN.benchmark/BfTree/BfTreeOperations.cs:GlobalSetup/teardown Dispose（固化后测量的确定态口径）

精炼执行方案：
1. WbftreeEngine 覆写 flush()：量尺寸前强制落盘收尾。若引擎需新增能力，最小路径是 BfTreeService 增加显式 flush/drain 接口（刷出缓冲池待落盘页，不 dispose、不影响后续 compact 段）；或覆写 flush 走 RangeIndexManager 快照固化口径，取其一，禁双机制。
2. 若判定引擎写路径实为写穿（无用户态脏页滞留），则改为在发布表与 JsonDurability 显式注明 wbftree 尺寸口径（无 flush 步骤），消除不可比对隐患。
3. 测试验证点：插入后不调 flush 直接量目录与调用 flush 后量目录对比，确认缓冲滞留量归零或口径注记到位。

---

执行判定记录（fix-benchflush 棒，2026-09-27，沙箱 /tmp/fork/fix-benchflush）：

一、写路径二分判定（r18 补强步骤，沙箱探针复跑 bench quick 装配：Std 磁盘后端、TUNE_BENCH 同参、24B 键/150B 值）：
- 档一 quick 形态（4MB 页环 / 38 万条 / 原始 ~66MB）：插入后立即量目录 S1=107,970,560B，隔 1.2s 复量 S2=S1 逐字节一致 → **无时间驱动后台落盘线程**（bf-tree 换页纯 demand-driven，eviction 仅在页环 alloc 压力下发生）；旁路 CPR 全固化快照（目录外）Ssnap=108,089,920B，Ssnap−S2=119,360B，仅 ~7 个 16KB mini 页量级，占全固化影像 0.11%；service.dispose() 后目录 S3=S1 → **缓冲尾永久滞留不回写**（CircularBuffer Drop 仅 dealloc 确证）。
- 档二低压力形态（128MB 页环 ≫ 17.5MB 数据）：残余 55,232B（0.19%）——数据量远小于页环时残余仍仅数页，证明叶子点写在页锁完成即以页为单位回写工作文件（bf-tree LeafEntryXLocked::drop → pwrite），**写路径形态判定为页级近写穿**，非「全程缓冲批量落盘」。
- 结论：票面精炼执行方案 #2 分支成立（近写穿 + <0.2% 恒定尾残），取**口径注记收口**，不新增引擎机制。路径 1（BfTreeService flush/drain）不可得：bf-tree 0.5.6 无公开页池排空接口（甄别确证），churn 挤环会向文件写入垃圾页/墓碑污染尺寸口径；「快照固化口径」变体（flush 内 cpr_snapshot）会把整幅影像写入被测目录直接污染 database_size，两案均弃，禁双机制成立。
- 验证一句话：沙箱 bench 定向编译通过，`--engine wbftree` quick 全段跑通（读校验断言全过），uncompacted size 行 98.78 MiB 产出正常；flush 前后尺寸差即登记残余 0.11~0.19%（恒定、不随规模放大）。

二、收口改动面：wbftree_engine.rs 显式覆写 flush() 为注记 no-op（判定与禁机制理由入注释）；types.rs JsonDurability wbftree 行同步实际口径；zh.yml/en.yml durability bulk/sync 行「结束统一执行 flush」矛盾宣称改写 + notes 补 wbftree 尺寸行口径注记；readme/{zh,en}/bench.md 与 bench/js/img/{zh,en}/bench.svg 按渲染链重生成（数值零改动）。wkv/wbftree 域零改动（探针验证件已删）。

合入哈希：18668777 收口形态：写路径二分判定为页级近写穿＋<0.2% 恒定尾残，取票面 #2 口径注记收口（wbftree_engine.rs 显式覆写 flush 注记 no-op，types.rs JsonDurability 与 zh/en yml notes/durability 随渲染链重生成发布页，零引擎新机制）