终态注记（2026-09-28 主控）：已合入 dev（合入 commit b150c29dcb8d531549a37990d1d37bd997025466）。
收口形态：
1. wedb/wreviv/src/pool.rs：提取 search_window 受控窗切片方法，put 级联迭代改为 for bin in self.search_window(bin_idx)，越窗未入即计 drop_count；take 统一调用 for bin in self.search_window(start_bin)，两窗同构；订正 FreeRecordPool::new 注释，明确 C# 原型未设 BestFitScanLimit 停在默认 UseFirstFit(0)，Rust BEST_FIT_SCAN_ALL 属几何收形内裁，禁以「对齐 C#」名义回摆。
2. wedb/wreviv/tests/reviv/concurrency_stress.rs：撤 STABLE_SPINS 越窗死局兜底，PROBE_SIZES 覆盖生产尺寸，增加收工排空断言。
3. wedb/wreviv/tests/reviv/purge_and_limits.rs：unelide_capacity_overflow 补充多桶级联断言与 take 全额取出断言。

甄别结论：通过（2026-09-28 主控现码二次复锚，票面行号随 oversize 臂并档已漂移，以下为现位）
- put 无界上联实位 pool.rs:355 `for bin in &self.bins[bin_idx..]`（票面 :290）；
  take 受控窗实位 pool.rs:442-445 `end = (start_bin + 1 + BINS_TO_SEARCH).min(len)`、
  常量 BINS_TO_SEARCH 实位 :49（票面 :46）；两窗不对称坐实。
- 候选尺寸闸实位 bin.rs:232 `if size >= required_size`（票面 :234），跨窗槽同尺寸 take 盖不到坐实。
- best-fit 误归 C# 的注释实位 pool.rs:176-179「等价 C# RevivificationSettings.PowerOf2Bins 预设：
  …+ 全桶最优适配」（票面 :149-151）；C# 一手现验：PowerOf2BinsRevivificationSettings 构造
  （RevivificationSettings.cs:219-238）只设 RecordSize/NumberOfRecords，BestFitScanLimit
  停在 :181 的 UseFirstFit=0 → rust BEST_FIT_SCAN_ALL 属 §111c 几何收形内裁，注释须订正。
- 越窗死局自认注释实位 concurrency_stress.rs:319-325（STABLE_SPINS=16），
  unelide_capacity_overflow 实位 tests/reviv/purge_and_limits.rs:35。
- C# 满则弃实位 FreeRecordPool.cs:518 TryAddToBin / :538 `++revivStats.failedAdds`。
- 非重报复核：ing/done/reject 四池零覆盖 put×take 窗失配交互；wreviv oversize 票已归档，
  本域无在途席（refactor-r7 96 处改动零涉 wreviv）。
执行域：仅 wedb/wreviv/**；禁触 wnode/wedb/wext_json（aof 哨兵席与 wext-json 席在途）。

审核结论：通过，严重度 P3 维持

分席确证与优化（供 fix 直接消费）：
- 机制全坐实：pool.rs:290 put 无界上联、:46/:353 take 窗收当期+一档；bin.rs:234 候选须 size>=required_size，故落 ≥2 档大桶的小槽同尺寸 take 盖不到、大尺寸 take 尺寸亦不足，仅靠 :224-229 水位淘汰/purge_below 回收且占大桶 256 名额；concurrency_stress.rs:317-324 自认「越窗死局」仅兜底。
- 附带点坐实：C# PowerOf2Bins 构造未设 BestFitScanLimit，默认 UseFirstFit(0)，rust 注释误归 best-fit 为 C# 一手。
- 非重报：§111c 只裁五旋钮与 take 侧收形，put×take 窗失配交互零登记。

优化执行方案：
1 put 级联窗收为 bins[bin_idx..(bin_idx+1+BINS_TO_SEARCH).min(len)]，与 take 用同一单界表达式（宜提公共窗函数保两窗同构），越界即走现 drop_count 弃路径。
2 订正 pool.rs:149-151 注释回指 §111c、声明 C# 预设 First-Fit、禁按「对齐 C#」名义回摆。
3 验证点：unelide_capacity_overflow 族补「溢出槽必落 take 可达窗」不变量；concurrency_stress 撤 STABLE_SPINS 越窗死局兜底后仍守恒；wreviv 全测绿。

reviv 池 put 向上级联无窗界而 take 仅探相邻一档，级联落越窗槽成僵尸占位

问题分析：
1. Garnet 契约对齐（C# 原型行为）
C# FreeRecordPool.TryAddToBin 满则弃（failedAdds 计数丢弃，不向上级联）；C# TryTake 跨桶检索窗由 NumberOfBinsToSearch 控制，预设 0（garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationSettings.cs:48），即当期桶落空后至多探相邻更大若干档。C# 入池与取池的桶可达域天然对称：既然入池只投目标桶，任何入池槽都在同尺寸取池的当期桶内。

2. 工程现状确证（Rust 实现路径）
rust FreeRecordPool::put 目标桶满时无界向上依次尝试更大分桶（wedb/wreviv/src/pool.rs:290 `for bin in &self.bins[bin_idx..]`），pool.rs:273-276 自陈「以可控的跨桶尝试换取空间留存」（较 C# 满则弃之有意发散）。
rust FreeRecordPool::take 检索窗上界为 start_bin + 1 + BINS_TO_SEARCH，且 BINS_TO_SEARCH 固化 1（wedb/wreviv/src/pool.rs:46 常量、:351-353 窗口计算；take 跨桶检索收相邻一档已登记 doc/zh/deviations.md §111/§1494 收形臂）。

3. 逻辑危害确证
入池侧无界上联、取池侧仅探一档，两窗不对称。一条小尺寸记录在目标桶及其相邻档皆满时被级联塞进距其 best-fit ≥2 档的大桶后，该槽只对映射到大桶或其相邻档的大尺寸 take 可达；对该记录真实尺寸的小 take 永不可达（take 窗盖不到）。这些槽占据大桶宝贵名额，既难被同尺寸复活、又挤占真正需要大槽的请求，构成跨窗僵尸槽与容量记账虚耗。压测 wedb/wreviv/tests/reviv/concurrency_stress.rs:318-324 注释自认存在「病态级联溢出越窗的槽位 / 越窗死局」，仅靠池面静止判定与空闲自旋上限兜底退出，非机制收口；此危害交互面 deviations 台账零登记（§1494 只裁 take 侧收形，未裁 put 无界级联与 take 窗失配）。
附带治理点：wedb/wreviv/src/pool.rs:149-151 文档注释把 best-fit 全桶适配归为 C# 一手形态，实则 C# 预设 BestFitScanLimit=UseFirstFit（RevivificationSettings.cs:181）为 First-Fit；rust 采 best-fit 属 §111c 几何收形内裁，但注释误导后续对账者按「对齐 C#」名义回摆。
严重度 P3：容量收敛与登记准确性，非即时崩溃/丢数据。

涉及代码：
rust 文件与函数：
wedb/wreviv/src/pool.rs:FreeRecordPool::put（无界上联 :290）
wedb/wreviv/src/pool.rs:FreeRecordPool::take（窗界 BINS_TO_SEARCH :46/:353）
wedb/wreviv/src/pool.rs:FreeRecordPool::new（best-fit 误归 C# 的注释 :149-151）

对应 c# 文件与函数：
libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/FreeRecordPool.cs:TryAddToBin（满则弃）/ TryTake
libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationSettings.cs:NumberOfBinsToSearch(:48) / BestFitScanLimit(:181)

精炼执行方案：
1. 令入池级联窗与取池检索窗对齐为同一单界：put 上联至多到 take 可达窗内（即至多 BINS_TO_SEARCH+1 档），越界即按 C# 满则弃计入 drop_count，杜绝产生活取池不可达的越窗槽。
2. 订正 pool.rs:149-151 注释，回指 §111c 并声明 C# 预设为 First-Fit、rust best-fit 为收形内裁，禁以「对齐 C#」名义回摆。
3. 测试验证点：unelide_capacity_overflow 族补「溢出槽必落 take 可达窗内」不变量断言；concurrency_stress 移除越窗死局的静止兜底依赖后仍守恒（无重复、地址不越下界、收工池可清空）。
