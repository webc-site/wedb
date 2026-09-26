甄别结论：通过（甄别席 zc-fix-r25-甲，2026-09-26）定级 P2
核验记录（现码复跑，非票面背书）：
1 回收缺位面现码亲验：grep detach_tree/dispose_tree_under_lock/release_detached 生产调用面全部 delete_file=true（promote/compact/ops/drain/bftree_release 删除语义），delete_file=false 形态仅存在于测试（wkv/tests/store/range_index.rs、tree_cache_budget.rs、wnode/tests/tiered_stub_heal_write.rs）——冷树回收零生产触发者未灭失；驱逐链（append.rs ensure_page_ready/shift.rs/wkv evict_pages_for）零记录级钩面与审核席 r20 亲验一致。
2 耗尽算术复跑：wkv/src/config.rs:44 DEFAULT_TREE_CACHE_BUDGET_BYTES=256MiB、wbftree/src/types.rs:86 DEFAULT_RI cache_size=16MiB、lifecycle.rs:196-197/:250-251 try_reserve_cache 败即 CacheBudgetExhausted、promote.rs:125 败臂回落信封态——16 棵尽后新升阶与 RI.CREATE 永久拒绝链在位；§120 降阶轮缺省 0 门控现读在位（无自愈三角成立）。
3 C# 锚亲验：GarnetRecordTriggers.cs:110 OnEvict 对 RangeIndexRecordType 调 DisposeTreeUnderLock(..., deleteFiles: false) 原文坐实；RangeIndexManager.Index.cs:240 DisposeTreeUnderLock 在册。
4 查重：deviations.md §111e) 仅登 tree_cache_budget 旋钮反向形、§120 仅登降阶轮宿主、§83 仅 RI.CREATE 守卫，均不覆盖回收通路缺位；r15-perf 裁「耗尽显式报错无死锁」行为与本票「冷树不回收」正交；四池无同轴票。
5 架构合规与可执行度：宿主裁定（挂 reclaimer_loop 恒开轮而非 §120 门控的 object_collect 轮）分层正确（wkv 同见 whlog 水位与 wbftree 注册表）、复用 detach_tree+get_or_open_tree 唯一内核零新机制、拒绝臂同步自愈在 spawn_blocking 线程池承接不违 compio thread-per-core；测试验证点闭环。定级 P2：容量预算先到先得无回收属资源防护缺口（非数据丢失非协议错）。

wbftree 冷树缓存无页驱逐回收通路——tree_cache_budget 先到先得永占，默认 16 棵耗尽后新键升阶与 RI.CREATE 永久拒绝且无自愈

审核结论：通过（席位 zcode-r20-review-evict，2026-09-26，dev 分支现树双侧亲验）

逐点亲验记录：
1. C# 回收通路属实：GarnetRecordTriggers.cs:55 CallOnEvict => rangeIndexManager != null（预览开即真）；:103-119 OnEvict 对 RangeIndexRecordType 调 DisposeTreeUnderLock(deleteFiles:false)；正常页关闭驱逐逐记录走 OnEvict（AllocatorBase.cs:1820-1852 OnPagesClosed → EvictRecordsInRange → storeFunctions.OnEvict，恢复路径 :1725-1727 同钩）；RangeIndexManager.Index.cs:240-303 在条带独占锁内同步摘 liveIndexes 条目（含 pending 条目）、storeEpoch 延迟 dispose 引擎、deleteFiles:false 分支文件保留供懒恢复；RestoreTree（RangeIndexManager.Locking.cs:273）按需重开。GarnetServer.cs:496-498 LogMemorySize 缺省 16g > 0 即 CacheSizeTracker.Initialize，默认形态可达。
2. rust 侧独缺释放上半场属实：驱逐链真实且零记录级钩子（whlog/src/hlog/append.rs:19 ensure_page_ready 三条件自动推 head、shift.rs:77 shift_head_address、wkv/src/session/raw/mod.rs:362 evict_pages_for 背压，全链无 on_evict/记录触发面）；树实例常驻 live_indexes（wbftree/src/manager/mod.rs:194）；唯一释放内核 dispose_tree_under_lock（lifecycle.rs:646 = detach_tree + release_detached 组合）全仓生产调用面仅删除语义（promote.rs:228 发布失败回滚、compact.rs:382 紧缩、ops.rs:129 RI.CREATE 回滚、drain.rs:63 删空、bftree_release.rs:80/:163/:235 换号回收，全部 delete_file=true），detach_tree(false) 懒恢复收口臂（lifecycle.rs:557-591 就地 CPR 快照钉死 data.bftree 自洽）零生产调用者；懒重开下半场完整在位（lifecycle.rs:295 get_or_open_tree、:363-365 reserve_cache_forced 恢复树不拒绝、:389-394 pending 激活、:425 pre_stage）。
3. 16 棵耗尽永久拒绝可达属实：DEFAULT_TREE_CACHE_BUDGET_BYTES=256MiB（wkv/src/config.rs:44）÷ TreeTuning::DEFAULT_RI.cache_size=16MiB（types.rs:86，升阶 DEFAULT_RI_COLLECTION 同承 16MiB）= 16 棵；第 17 棵 create_bftree_internal try_reserve_cache 拒绝（lifecycle.rs:196-198）→ promote.rs:125 回落信封态 / RI.CREATE 报错；无自愈三角齐全：页驱逐零联动 + 降阶轮缺省禁用（§120 亲验：DEFAULT_EXPIRED_OBJECT_COLLECTION_FREQUENCY_SECS=0 任务不 spawn）+ 仅 DEL/删空/紧缩/换号可释放。
4. 方案单套收敛成立：复用 detach_tree(false) 唯一释放内核 + get_or_open_tree 懒重开既有下半场，不新造第二套树释放机制；is_on_disk 判据既有（whlog/src/hlog/io.rs:21，wkv promote.rs:342 已用同款）。
5. 查重干净：r15-perf 第三节奏裁的是「预算耗尽显式报错无死锁」行为（review_history/zcode-r15-perf.md:50 亲核），§1483 只登旋钮反向形，§120 只登降阶轮宿主，§83 只登 RI.CREATE 容量守卫（均不覆盖回收通路）；js/check/ignore/server.yml:286 OnEvict 忽略块理由（:403 起）系「IFunctions 回调协议面」四类泛述，无 RI 树缓存回收专属裁决；向量侧专属裁决（:1062-1067/:1238）论证基础「索引记录不入 wkv 值域」对 RI 不成立（RI Meta 存根经 save_bftree_meta_stub 落 wkv 主日志，wkv/src/range_index/stub.rs:131 亲核），划界正确。

执行方案优化裁定（原方案两可处收口）：
1. 宿主裁定：冷树回收驱动挂 wkv 既有常驻回收轮 spawn_bftree_reclaimer / reclaimer_loop（wkv/src/gc/reclaim.rs:83/:128，RELEASE_POLL_MS 节拍），不挂 wnode object_collect_loop——后者受 expired-object-collection-freq 缺省 0 门控（§120，缺省不 spawn），挂彼处则缺省配置下后台回收臂不可达；reclaimer_loop 恒开（gc.enabled 只门控 SCAN，明确「绝不关停它」）、恰处 wkv 层（同见 whlog 水位与 wbftree 注册表，分层单向不破）、且已是换号树延迟释放队列唯一常驻消费者（drain_bftree_release，bftree_release.rs:189），回收驱动的摘除（detach_tree）与延迟释放（release_detached）收宿主同轮，零新增调度器。回放侧注意：reclaimer_loop 在弱引用升格后逐轮 drain，冷树扫描臂加入该循环体即可，单轮限批同 RELEASE_BATCH 纪律。
2. 判据：扫 live_indexes 各条目对应键的主日志 Meta 存根最新地址（wkv 哈希索引 key→addr 现成），is_on_disk(stub_addr) 且越迟滞窗口（存根页逐出后保持冷态时长阈值，防热键释放重开抖动；C# OnEvict 无迟滞但 C# 逐页同步驱逐天然滞后，本仓轮询驱动需显式迟滞补位）即 detach_tree(id_key, false)。pending 条目（tree=None）同扫同摘（C# 同臂），下轮 get_or_open_tree 重注册。
3. CacheBudgetExhausted 拒绝臂自愈：try_reserve_cache 失败时同步驱动一轮同判据冷树回收再重试一次预留，仍失败方回落信封态/报错。执行线程安全：RI.CREATE 与 promote 建树臂均已在 range_index_blocking（spawn_blocking，wkv/src/range_index/mod.rs:310）线程池，同步回收不触异步反应器；detach CPR 快照重 IO 同在阻塞池承接。
4. 与 §120 边界：本回收臂宿主 reclaimer_loop 与降阶轮宿主 object_collect_loop 分属两轮（一恒开一旋钮门控），非第二套降阶机制——降阶是「树→信封」语义迁移，本臂是「树内存→懒重开」纯释放，互不重叠；不得以本票为由改 §120 降阶轮门控。
5. 测试验证点（原案维持）：默认预算建 16+ 棵升阶树耗尽预算 → 驱动页池回绕使 Meta 存根页逐出 → 验证冷树 detach 后 cache_reserved 回落、新树创建成功、冷键再读走 get_or_open_tree 懒恢复且数据逐字段完好、checkpoint 与在途读写者（条带读锁）不受释放影响；补迟滞窗口内热键不释放用例。

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# 在 EnableRangeIndexPreview=true 形态下（rust 侧经 deviations.md §68 改良为恒开），主日志页驱逐会逐记录触发记录级驱逐钩子：GarnetRecordTriggers.OnEvict 对 RangeIndexRecordType 记录调用 DisposeTreeUnderLock(key, valueSpan, deleteFiles: false)，后者（RangeIndexManager.Index.cs:240-275）在条带独占锁内同步从 liveIndexes 移除该键条目（pending 条目同样移除）并延迟 dispose BfTree 引擎（释放常驻页环内存），数据文件保留；被逐键下次访问经 RestoreTree 懒恢复重建。页驱逐本身由 CacheSizeTracker/LogSizeTracker 默认启用驱动（GarnetServer.cs:494 LogMemorySize 缺省 16g > 0 即 Initialize；LogSizeTracker.ResizeIfNeeded 超高水位 trim 推 head，越线页上 RI 存根的树随之释放）。语义要点：冷 RI 键的树内存（页环）随页驱逐自动回收，键与文件不变，访问时按需重开——冷树内存存在与主存压力联动的自动回收通路。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   rust 侧主日志页驱逐真实存在且常发（页池回绕：whlog/src/hlog/append.rs:HybridLog::ensure_page_ready 三条件判定并自动 shift_head_address，回绕槽位复用时旧页上记录被逐；写路径背压 evict_pages_for 见 wkv/src/session/raw/mod.rs:362）。但驱逐链全程无记录级处置钩子：shift_head_address 与页槽位回收（clear_page_from_offset/preclear_page）均不感知页上分层 Meta 存根对应的树实例。wbftree 树实例一经打开即常驻 RangeIndexManager.live_indexes（wbftree/src/manager/mod.rs:194），唯一释放口是 dispose_tree_under_lock（lifecycle.rs:646），全仓调用面仅删除语义（delete_index 即 delete_file=true：用户 DEL/删空 drain.rs、紧缩丢死键 compact.rs、换号回收 vdb/bftree_release.rs、升阶发布失败回滚 promote.rs）——不存在 delete_file=false 的冷回收生产调用（detach_tree 的懒恢复收口臂 lifecycle.rs:546-576 系死码预备，无生产触发者）。同时懒恢复下半场机制完整在位（get_or_open_tree 懒重开 lifecycle.rs:295、pending 条目激活、pre_stage_and_register_pending 预置），即 C# OnEvict 的「释放后按需重开」下半场已移植，独缺「驱逐时释放」上半场触发。叠加两点：降阶轮缺省禁用（deviations.md §120：升阶后无前台写入的冷分层键永无降阶评估点）与 reserve_cache_forced 恢复树不拒绝（lifecycle.rs:363，正确性优先），冷树页环（TreeTuning::DEFAULT_RI.cache_size=16MiB/棵，types.rs:86）永久占用 DEFAULT_TREE_CACHE_BUDGET_BYTES=256MiB 全局预算（wkv/src/config.rs:44）。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
   默认预算 256MiB / 每树 16MiB = 16 棵在线树即耗尽。此后：(a) 新键集合升阶 create_bftree_internal 的 try_reserve_cache 失败返回 Error::CacheBudgetExhausted（lifecycle.rs:196-198），promote.rs:125 调用臂回落信封态——百万级集合永久滞留 wcol 内存信封（恰为升阶要消除的内存常驻与读放大），RI.CREATE 则显式报错；(b) 已占树先到先得永占，冷树（Meta 存根页早已逐出、键长期无访问）页环不回收，预算无法腾出；(c) 无自愈通路——页驱逐不联动、降阶轮默认禁用（§120）、仅 DEL/删空/换号可释放，运行期唯一缓解是重启并调大 --tree-cache-budget。属板块 3.2「容量限额与驱逐真实生效」与「资源生命周期收口」的回收通路缺失。划界：r15-perf 第三节 3 判净的是「预算耗尽显式报错无死锁」行为本身；§1483 登记 tree_cache_budget 旋钮反向形与观测面；§120 登记降阶轮宿主缺省禁用；三者均未登记「页驱逐联动释放冷树」的 C# OnEvict 对位缺失；js/check/ignore/server.yml:286 对 GarnetRecordTriggers.OnEvict 的函数级忽略归入「IFunctions 回调协议面」泛述，未对 RI 树缓存回收语义做专属裁决（同文件 1062-1067 向量侧丢弃链有专属自洽裁决——rust 索引记录驻留登记表不入 wkv 值域无驱逐事件，RI 侧 Meta 存根确在 wkv 主日志页上、驱逐事件真实存在，不适用该论证）。C# LogSizeTracker 动态 trim 整体不移植系既定改良（storage.yml:66/§70 自适应预算规划），本票不挑战该改良，只裁「页驱逐事件已发生时树缓存零联动」的漏项。

涉及代码：
rust 文件与函数：
wedb/whlog/src/hlog/append.rs:HybridLog::ensure_page_ready（页池回绕驱逐点，无记录级回调）
wedb/whlog/src/hlog/shift.rs:HybridLog::shift_head_address（head 推进，无记录级处置）
wedb/wbftree/src/manager/mod.rs:RangeIndexManager.live_indexes（树实例常驻字典，无冷回收）
wedb/wbftree/src/manager/lifecycle.rs:RangeIndexManager::create_bftree_internal（try_reserve_cache 拒绝）/ RangeIndexManager::get_or_open_tree（懒重开在位）/ RangeIndexManager::dispose_tree_under_lock（唯一释放口仅删除语义）/ RangeIndexManager::detach_tree（delete_file=false 懒恢复收口臂无生产调用）
wedb/wkv/src/range_index/promote.rs:promote_collection_to_bftree（CacheBudgetExhausted 回落信封态）
wedb/wkv/src/config.rs:DEFAULT_TREE_CACHE_BUDGET_BYTES（256MiB 默认预算）
wedb/wbftree/src/types.rs:TreeTuning::DEFAULT_RI（cache_size 16MiB）

对应 c# 文件与函数：
garnet/libs/server/Storage/Functions/GarnetRecordTriggers.cs:GarnetRecordTriggers.OnEvict（:103-119 页驱逐逐记录钩子，RI 分支 deleteFiles:false）
garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:RangeIndexManager.DisposeTreeUnderLock（:240-275 条目移除+延迟 dispose，文件保留）
garnet/libs/storage/Tsavorite/cs/src/core/Index/StoreFunctions/IRecordTriggers.cs:IRecordTriggers.OnEvict（页越过 HeadAddress 逐非墓碑记录回调）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Common/LogSizeTracker.cs:LogSizeTracker.ResizeIfNeeded（默认启用的高水位 trim 驱动页驱逐）
garnet/libs/host/GarnetServer.cs:GarnetServer.CreateStore（:494 LogMemorySize>0 即初始化 CacheSizeTracker）

精炼执行方案：
1. 在 wkv 层（可同时见 whlog 水位与 wbftree 注册表，不破坏 whlog 单向分层）补冷树回收单点驱动：挂入既有对象收集后台轮（wnode/src/primary_tasks.rs 的 object_collect_loop，与 tiered_demote_round 同宿主）或 gc 轮，扫描 live_indexes 各条目对应 Meta 存根地址，is_on_disk(stub_addr)（存根页已逐出）且越过迟滞窗口（防抖动，防热键反复释放重开）时经既有 detach_tree(id_key, false) 释放树页环（懒恢复收口内建：CPR 快照钉死 data.bftree 自洽）——复用唯一释放内核，不新造第二套树释放机制；门控与 tiered_demote_round 同轮钮（§120 同宿主）或独立轻量节拍，由审核裁定
2. CacheBudgetExhausted 拒绝臂补一次自愈降级：try_reserve_cache 失败时先同步驱动一轮冷树回收（仅释放 is_on_disk 且超迟滞窗口的树）再重试一次预留，仍失败方回落信封态/报错
3. 测试验证点：默认预算下建 16+ 棵升阶树耗尽预算，驱动页池回绕使 Meta 存根页逐出，验证冷树 detach 后 cache_reserved 回落、新树创建成功、冷键再读走 get_or_open_tree 懒恢复且数据逐字段完好、checkpoint 与在途读写者（条带读锁）不受释放影响

合入哈希：a51bb3f 收口形态：wkv gc/cold_tree 冷树回收内核挂常驻 reclaimer_loop 每轮驱动 + RI.CREATE/升阶建树 CacheBudgetExhausted 拒绝臂同步回收重试一次，dispose_tree_under_lock(false) 唯一释放内核零新机制，双侧锁测绿
