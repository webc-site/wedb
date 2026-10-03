归档注记：合入 850b6724，reclaim 就地 dispose_engine(幂等 swap)，延迟队列仅余 unlink 世代判据，时序纪律原样

甄别结论：通过（甄别席 J6，2026-09-27，定级 P1——整树随延迟队列滞留 86400s 常驻 cache_bytes，C# 仅纪元量级延迟）。亲验：PendingTreeRelease 携整树（bftree_release.rs:51-56）、摘除点即归还预算（lifecycle.rs:470-478,:475 release_cache）、dispose 单点 settle_detached_release（:497-508）、缺省 86400s（config.rs:108）、轮询 200ms/批 256（reclaim.rs:29/:33）、Drop 只收已到期（store/mod.rs:800-813 自陈）；cache_bytes 常驻自陈（service/mod.rs:86-90）。C# DisposeTreeUnderLock（:240）→BumpCurrentEpoch→DisposeAndDeleteFilesDeferred（:296/:313）延迟仅纪元量级、FlushDatabase（DatabaseManagerBase.cs:301）随实例消亡，双侧成立。即刻 dispose 安全性由 service/mod.rs:129/:194 swap(None)+Guard 保活自陈与冷树臂先例（cold_tree.rs:123）支撑。查重零同案。方案 tree=None 投递沿用 release_retries :516-521 既有形态，单套机制。派沙箱席 c01l。

审核结论：通过（P1 记账失真真案。亲码坐实：PendingTreeRelease 整批携 Some(Arc<BfTreeService>)（bftree_release.rs:51-56），dispose 单点 settle_detached_release（lifecycle.rs:506-508）须待 drain expired_at 门（:196），缺省 86400s+200ms 轮；cache_bytes 自陈环整块常驻（service/mod.rs:86-90），unlink 不释内存属实。2× 叠加可达：remove_and_take_tree:475 摘除即 release_cache，try_reserve_cache 据此放行新树至 256MiB 顶，旧环未释双口径叠加成立；Drop 臂只收已到期。C# 验真：DisposeTreeUnderLock（Index.cs:240）锁外 BumpCurrentEpoch→DisposeAndDeleteFilesDeferred 仅纪元量级；cold_tree.rs:123 即刻释放同内核先例在。detach-heal-fsync/memtracker/checkpoint-purge 三票正交，deviations 无登记）

整理执行方案（审核席订正版，供 fix 消费）：
1 reclaim_bftree_keys 逐键 detach 后即 tree.dispose()，仅 data_path=Some 方投递，投递条目 tree=None（沿用 release_retries 既有形态 :516-521，禁新结构）；drain/settle/unlink 的 expired_at 与世代双判据分毫不动——序合偏序屏障（摘索引同步、引擎短窗、unlink 长窗），dispose 无 I/O 回放不阻塞承诺不破
2 锁测：a) 换号风暴队列积压（>RELEASE_BATCH）断言入队即 is_disposed、cache_reserved 残留零冲突、新升阶预算真实可用；b) Drop 排空臂仅携路径条目，未到期文件留盘经启动对账收敛；c) flush_database.rs:311 unlink 断言与 :425 世代守卫回归全绿

换号待释放队列把引擎 dispose 与数据文件 unlink 同压安全纪元延迟窗，FLUSHDB 后旧域升阶树常驻页环滞留内存至期限届满（默认 86400s），与摘除点已归还的预算记账相悖（RSS 双倍超订 + 清库不减压）

问题分析：
1 Garnet 契约对齐
C# libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:DisposeTreeUnderLock(:240) 条带锁内摘 liveIndexes 并取走 disposedTree，锁外把「原生 dispose + 删文件」一并交 storeEpoch.BumpCurrentEpoch(:296) 的 DisposeAndDeleteFilesDeferred(:313)——延迟时长即纪元排空量级（毫秒），不存在按秒/日计的时间窗队列；其 FlushDatabase（libs/server/Databases/DatabaseManagerBase.cs:301）为每库独立 Tsavorite 实例整段日志截断，树随实例消亡，引擎内存即刻归还。
rust 的共享单存储 + 秒级换号（既定改良，非本票对象）引入 PendingTreeRelease 延迟队列：数据文件 unlink 受 db_gc_reclaim_delay_secs 门控是偏序回收屏障的必要面（review.md 板块 2.2「物理文件删除必须受安全纪元与延迟到期队列保护」），但引擎对象释放被同一队列顺带拖到期限届满，C# 无此形态。

2 工程现状确证
wedb/wbftree/src/manager/lifecycle.rs:remove_and_take_tree（:470-478）在摘注册当场 self.release_cache(t.cache_bytes())（:475，自陈「预算归还（唯一摘除点）……树清退后配额即回」），cache_reserved 记账即时归零。
同文件 detach_tree（:557-591）只取走 Arc<BfTreeService> 与 data_path，不做 dispose；引擎物理释放单点只在 settle_detached_release（:497-534，:506-508 tree.dispose()）。
wedb/wkv/src/vdb/bftree_release.rs:PendingTreeRelease（:51-56）同时携 expired_at 与整棵 DetachedTree（tree 字段为 Some(Arc)）；reclaim_bftree_keys（:158-176）入队，drain_bftree_release（:188-211）单趟分区把 expired_at > now 的条目原序留队——引擎对象随队列滞留整个安全窗。
期限默认 wedb/wkv/src/config.rs:108 DEFAULT_DB_GC_RECLAIM_DELAY_SECS = 86400（wconf 旋钮，缺省即一天）；消费节拍 wedb/wkv/src/gc/reclaim.rs:29/:33/:123（RELEASE_POLL_MS=200、RELEASE_BATCH=256），入队侧 :170 lock().extend(...) 无水位（板块 3.2「后台任务队列必须具备背压与高低水位限制」）。
反证「早释放不安全」不成立：wedb/wbftree/src/service/mod.rs:192-211 自陈 dispose 为单句柄 swap(None) 幂等，已借出的点读 Guard/扫描 Arc 各自保活底层引擎至用毕才析构，「无需显式排空」；冷树回收臂（wedb/wkv/src/gc/cold_tree.rs:123 → dispose_tree_under_lock → release_detached）正是「摘注册后即刻经纪元 dispose、不经延迟队列」的既有同型形态——同一释放内核在换号臂被拖成 24h，属双时长双机制。

3 逻辑危害确证
记账失真（板块 3.2 记账真实性）：摘除点已归还配额而常驻页环实际仍被队列钉住，crate 自订的 cache_reserved ⟺ 在线树环容量和契约在整个换号窗口内为假。
RSS 双倍超订：FLUSHDB 之后的新升阶与 RI.CREATE 经 try_reserve_cache（wedb/wbftree/src/manager/mod.rs:316-336）在「已空」预算内放行，旧环未释，峰值实际占用 ≈ 2× tree-cache-budget（缺省 256MiB，分层防 OOM 承诺面被穿透）；清库这一运维减压手段在期限届满前不降内存。
规模与形态：单次 FLUSHDB/FLUSHNS/SWAPDB 摘除 N 棵升阶树即滞留 N 个页环至期限；进程退出臂（wedb/wkv/src/store/mod.rs:796-813 Drop，只收割已到期条目）下未到期条目须随 Arc 归零才释放，短生命周期形态（容器重启、从库重建回放后换号）全程持有。

查重：doc/zh/deviations.md 无本项登记（§111 c 系 reviv 几何旋钮缺失、§32b-24 系 MEMORY USAGE samples，均不相干）；task 五池无同票，todo/wbftree-detach-tree-heal-rename-missing-dir-fsync.md 系 delete_file=false 的 heal 臂目录屏障，与本票消亡臂引擎释放时长不重叠。

涉及代码：
rust 文件与函数：
wedb/wkv/src/vdb/bftree_release.rs:PendingTreeRelease、WedbStore::reclaim_bftree_keys、WedbStore::drain_bftree_release
wedb/wbftree/src/manager/lifecycle.rs:RangeIndexManager::remove_and_take_tree、detach_tree、settle_detached_release
wedb/wbftree/src/manager/mod.rs:try_reserve_cache、release_cache
wedb/wbftree/src/service/mod.rs:BfTreeService::dispose
wedb/wkv/src/config.rs:DEFAULT_DB_GC_RECLAIM_DELAY_SECS
wedb/wkv/src/gc/reclaim.rs:RELEASE_POLL_MS、RELEASE_BATCH、reclaimer_loop
wedb/wkv/src/gc/cold_tree.rs:recycle_cold_bftrees（同内核即刻释放先例）

对应 c# 文件与函数：
libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:DisposeTreeUnderLock、DisposeAndDeleteFilesDeferred
libs/server/Databases/DatabaseManagerBase.cs:FlushDatabase

精炼执行方案：
1 拆「引擎释放」与「文件 unlink」两窗：reclaim_bftree_keys 逐键 detach_tree 取回批次后即处 tree.dispose()（与冷树回收 dispose_tree_under_lock 同释放时长口径，摘注册点已 release_cache 故配额与实际占用同步归零），投递条目仅余 unlink 所需世代判据与 data_path；unlink 仍由 drain_bftree_release 的 expired_at 门控，偏序回收屏障与崩溃窗承诺分毫不动。
2 数据结构随之自然收敛：DetachedTree.tree 在换号臂入队即为 None（settle_detached_release 既有 Some 分支保留给他臂，禁新增包装结构与第二套队列）。
3 测试：wbftree lifecycle/drain 与 wkv 换号回归全绿；wkv/tests/store/flush_database.rs 补断言——换号投递返回后队列条目 tree 字段为空且 range_index.cache_reserved() 与队列残留零冲突（即引擎在入队前已 dispose），期限届满经 drain_bftree_release 仍落 unlink，同名重建世代守卫用例行为不变。
