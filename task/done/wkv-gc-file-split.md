优先级：低（841 行后台回收驱动多职责混聚）
来源：next/agy.db.md 条 11。核销 2026-09-19，取证基线 = 主仓 dev 当下 HEAD。

结论一句话
wkv/src/gc.rs 一个文件里同时住着后台驱动循环、VDB 换号物理回收、TTL 过期扫描、
日志紧缩阈值与迟滞熔断、BfTree 注销排空与句柄门面；按域拆子模块、结构与驱动循环留门面，
纯搬移零语义改动。

现状（主仓 HEAD 实测，wkv/src/gc.rs 共 841 行）
1. 结构与统计：:193 pub struct GcStatsSnapshot、:213 pub struct GcManager<D: Device>
   （:225-:242 含 compact_boost 迟滞双水位字段）、impl 块起 :233。
2. 驱动循环域：:235 new、:258 spawn、:304 drive、:325 stats、:338 run_once、:348 tick。
3. 物理回收域：:371 reclaim_physical、:380 reclaim_when_scan_idle。
4. VDB 换号清扫域：:391 sweep_vdb。TTL 过期键扫描域：:450 sweep_expired。
5. 紧缩域：:551 refresh_compact_boost（双水位熔断）、:604 try_compact（None 档短路 +
   safe_ro 上界 + 经 WedbStore::compact 注入 WedbCompactionFunctions）。
6. 句柄与守卫：:750 impl Drop for RunGuard、:757 pub struct GcHandle<D>（:765 stop、
   :770 stats、:775 is_finished）；内联测试 :805 起（:790 亦有一处 cfg(test) 门内项）。
7. 相邻件已分域（勿混）：wkv/src/store/gc.rs 111 行是 CONFIG 调和启停门面
   （reconcile_gc_scan，对标 StoreWrapper.cs:ReconcilePrimaryTask），
   wkv/src/store/reclaim.rs 是 bftree 键排空回收落点。

C# 参考
1. libs/server/StoreWrapper.cs:ReconcilePrimaryTask（主任务生命周期与调和，已对位在
   wkv/src/store/gc.rs，本票不动它）
2. libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs（紧缩推进与上界校验，
   对位 wkv/src/compact.rs + wcompact）
3. libs/server/Storage/Functions/MainStore/GarnetRecordTriggers.cs:IsDeleted（记录判死谓词，
   对位 wkv/src/compact.rs 的 WedbCompactionFunctions）
4. 即 C# 侧这三域本就在三个文件，rust 的 GC 件只需按域分文件，不新增任何机制。

修法
1. 目录化 wkv/src/gc/ ：mod.rs 留 GcStatsSnapshot / GcManager 结构定义、GcHandle、RunGuard
   与 drive/run_once/tick 调度环；子件 vdb.rs（sweep_vdb）、ttl_sweep.rs（sweep_expired）、
   compact.rs（refresh_compact_boost + try_compact）、reclaim.rs（reclaim_physical +
   reclaim_when_scan_idle）。同 crate 内多文件 impl GcManager<D> 分部实现
   （本仓先例：wkv/src/store/*.rs 对 WedbStore 分域 impl）。
   子件命名避开与 wkv/src/store/{gc,reclaim}.rs 混淆，必要时用 gc/vdb.rs、gc/sweep_ttl.rs 等
   带动词形态，读者优先定位。
2. 跨子件私有项（如 gc/ 内的 enabled_by_config、scan_interval_ms :165、reclaim_expired_at
   等自由函数）保持单点：定义留在其语义归属子件，其余以 super:: 引用，禁复制。
3. 内联测试：纯谓词单测（enabled_by_config、档位判定、水位迟滞）随其被测件保留内联；
   只有需整 store 形态的集成用例才迁 wkv/tests/gc.rs（该文件已存在，勿新建第二份同名）。
4. 禁借机改阈值、档位投影链与熔断语义（GcConfig::compaction_type 的 CONFIG SET 投影口径不动）。

验收判据
1. wkv/src/gc.rs（或 gc/mod.rs）行数 ≤300，四个子件各 ≤250。
2. GcManager::sweep_vdb、GcManager::sweep_expired、GcManager::try_compact、
   GcManager::refresh_compact_boost、GcManager::reclaim_physical 各自定义点 1 处，
   且调度次序（drive → tick → 回收 → 清扫 → 紧缩）与统计计数（compactions、
   last_compact_dropped、compact_boosting）逐项不变。
3. WedbStore::reconcile_gc_scan（wkv/src/store/gc.rs）→ gc::enabled_by_config 的谓词单点未分裂
   （grep enabled_by_config 定义 1 处）。
4. cargo check 通过（禁在共享 target 跑 test.sh / clippy.sh）。

双花登记
并发代理就条 11 另立同题薄票 next/db-wkv-gc-split.md（同改 wkv/src/gc.rs，五域划分与本票一致），
两票同改一文件只取一棒：本票为正文载体，派发时以本票为准并删除该薄票，禁双花。

结案注记（载体分支 wkv-gc-split，代码提交 82a859f）
甄别复核：执行基线 dev=d5c6efa。现刻 gc.rs 实测 834 行（票称 841，后段被并发合入
微缩 7 行，域锚整体漂移 ≤7，五域与统计结构、内联测试位置逐一对号，判定不变）；
双花薄票 next/db-wkv-gc-split.md 已不在册（现 next/ 无 gc 题票），无需删除动作。
wkv/src/lib.rs 的 `mod gc;` 行随目录化自动收敛，零改动；session/** 零接触；
store/gc.rs、store/keyspace.rs、store/mod.rs、store/reclaim.rs 四个相邻件零 diff。

落地：按域拆为 gc/ 目录模块——gc/mod.rs 401 行（门面总述文档、结构三件
GcStats/GcStatsSnapshot/GcManager、new/spawn/drive/stats/run_once/tick 调度环、
启停谓词单点 enabled_by_config 与 scan_interval_ms、句柄门面 GcHandle/RunGuard、
随被测件保留的内联谓词单测）+ 子件四枚：vdb.rs 59（sweep_vdb 墓碑注销与空闲
路由析构）、ttl_sweep.rs 200（sweep_expired 两段扫描 + collect_expired/ScanBudget/
ExpiredKeySet 共享内核随 TTL 域走）、compact.rs 151（refresh_compact_boost 迟滞
熔断 + try_compact 紧缩推进）、reclaim.rs 101（reclaim_physical 回收内核 +
reclaim_when_scan_idle 兜底 + spawn_bftree_reclaimer 常驻排空与其 RELEASE_* 常量）。
子件命名按修法 1 逃逸条款取动词形态（sweep_ttl 形态），与 store/{gc,reclaim}.rs、
顶层 compact.rs 均不重名；旧 gc.rs git rm，无 shim。

搬家中性取证：全件非空行多重集比对，消失 6 行 = 4 条方法签名加 pub(super)
（sweep_vdb/sweep_expired/try_compact/reclaim_physical，件间家族可见，先例
wdev 拆分同款）+ 2 条 crate 导入拆件；新增 70 行全部为四枚件首 //! 说明、
4 条 mod 声明注释行、件内导入、4 枚 impl 包装与 2 行门面转发
（pub use reclaim::spawn_bftree_reclaimer、pub(crate) use ttl_sweep::
{ExpiredKeySet, ScanBudget, collect_expired}，crate::gc::* 既有引用路径
store/mod.rs 与 keyspace.rs 零改动保持），零逻辑行改动。C# 锚点
（[A-Za-z]+\.cs:[A-Za-z_]+ 形态）多重集 3 枚逐条不变、逐枚随代码块走位；
bun js/check.js 拆分前后输出逐字节相同（exit 0），ignore 语料零回写。

验收对照：判据 2——五方法各自定义点恰 1 处（grep 实测），调度次序
drive → tick → sweep_expired → reclaim_physical →（sweep_vdb → try_compact）
与统计计数行（compactions/last_compact_dropped/compact_boosting 的 fetch/store）
逐字节同旧；判据 3——enabled_by_config 定义 1 处（mod.rs:104，pub(crate) 原级
未动），store/gc.rs 的 gc::enabled_by_config 引用零改动；判据 1——子件四枚
59/101/151/200 全 ≤250 ✓，门面 401 行超 ≤300 目标：修法 1 保留清单在 834 行
基数下自身即 ~360 行（头述 51 + 环 130 + 句柄 58 + 结构 58 + 谓词 26 + 内联测
试 37），两条款互斥，按「结构与驱动循环留门面件 + 零语义」优先执行，行数偏差
如实注记（同先例 wdev 拆分门面 373 行获认尺度）。阈值/档位投影链/熔断语义零
改动（判据 4 附带纪律）。

门禁实测：私有 target /tmp/ct-gcsplit，cargo check --workspace --all-targets
0 error 0 warning（并入 d5c6efa 后复验）；cargo nextest run -p wkv 222/222 全绿
（含 gc 集成套件与 gc_scan_predicate_disabled_states 单测）；cargo fmt 已施。
