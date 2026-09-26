甄别结论：通过（甄别席 zc-fix-r25-甲，2026-09-26）定级 P2
核验记录（现码复跑，非票面背书）：
1 三处裸 spawn 现码亲验仍在位：wkv/src/gc/mod.rs:211 `let join = spawn(async move { loop {` 无监督包裹；wedb/src/server/cluster_manager.rs:199 `spawn(async move { ClusterManager::flush_task_async(...) }).detach()`；wnode/src/server.rs:929 monitor `spawn(async move { monitor.main_monitor_task_async(...) })`；三文件 grep supervise_task 零命中，同族 primary_tasks.rs:258/:317 监督形态在位对照成立。
2 死亡不可观测复跑：全仓 grep set_hook 零命中（默认 hook 仅 stderr）；flush_running 复位点仅 :284 停机臂，幂等门 :194 swap(true) 死亡后永闭；GcHandle::is_active（gc/mod.rs:349）与 reconcile_gc_scan（task.rs:48）重拉判定在位。
3 C# 锚抽验：StoreWrapper.cs:772 catch LogCritical「The task won't be resumed」原文在位；ClusterManager.cs 构造尾 Task.Run(FlushTaskAsync) + try/finally numActiveTasks 在位；wbase/src/supervise.rs:137 supervise_task 单点机制现码可用（模块头自陈全仓单点与 Err 臂复位纪律）。
4 查重：deviations.md 无三任务监督豁免在册（§130 反裁 bg_task_health 为自研超集严禁回改，与本修复同向）；task/{done,ing,issue,reject} 无同轴票。
5 架构合规与可执行度：仅包裹既有 supervise_task 零新机制、不建自动重拉（防毒丸风暴，与 reclaimer 有界重挂裁量一致）；线程模型不动（compio thread-per-core 原生兼容）；验证点仿 reclaim.rs 监督快照测试闭环。定级 P2：观测/防护缺口，非直接数据面。

审核结论：通过（审核席 zcode-r21-review-supervise，2026-09-26）

亲验记录（双侧源码逐点核实，全部属实）：
1. 三处裸 spawn 确证：wedb/wkv/src/gc/mod.rs:211（spawn(async move { loop {...} })）、wedb/wedb/src/server/cluster_manager.rs:199（spawn flush_task_async 后 detach）、wedb/wnode/src/server.rs:929（spawn main_monitor_task_async 后 detach），均无 supervise_task 包裹。
2. 同族任务全接线确证：primary_tasks.rs:258/:317、service.rs:582/:629、gc/reclaim.rs:95/:149（另配重挂环）、waof_sublog.rs:182、vector 量化/清理、collection_item_broker、gossip_manager、failover_manager.rs:218/:267、replication.rs:262/:344——唯三处漏接属实，无「有意排除」注释或登记。
3. 死亡不可观测确证：全仓无 panic::set_hook（默认 hook 仅写 stderr，文件日志通道收不到）；gc_stats/gc_running 生产消费零命中（仅 wkv/tests/gc.rs、wnode/tests/config_owner_bridge.rs 测试消费）；bg_task_health 名单由 supervise_task/register_counter 注册构成，三任务不在其列；flush_running 复位点仅 dispose_background_tasks（cluster_manager.rs:284）；start_flush_task 唯一生产调用点 replication.rs:109、start_server_monitor 唯一调用点 server.rs:311，均装配期一次，无重拉通路。
4. C# 契约确证：StoreWrapper.cs:770-773 ExpiredKeyDeletionScanTaskAsync catch LogCritical（The task won't be resumed）；GarnetServerMonitor.cs:268-319 try-catch LogCritical + finally done.Set()；ClusterManager.cs:119-142 构造尾一次性 Task.Run + try-finally 维护 numActiveTasks；TaskManager.cs:44-66 Dispose CancelAsync(All) 阻塞等待。一处引用微瑕：CompactionTaskAsync（StoreWrapper.cs:716-719）落 LogError 非 LogCritical，原文合并表述略过，契约结论（异常死亡必落 CRITICAL/ERROR 级留痕）不变。
5. 查重净：deviations.md 无三任务监督豁免在册；§130 裁定 bg_task_health 为自研超集观测行且严禁回改删除，本修复方向（三任务入监督快照）与该裁定同向；r21-wbase 裁的 supervise 存活位残留窗口（不立案）与本票正交；task/todo 与 task/reject 无同面票。
6. 修复零新机制确证：wbase/src/supervise.rs 模块头自陈全仓单点（禁再散写第二机制），supervise_task 现成可用，方案仅包裹既有机制，不新建重派器、不加自动重拉（防毒丸风暴，对齐 reclaimer REMOUNT_LIMIT 有界重挂裁量）。

优化执行方案（供 task/fix.md 直接消费）：
1. GC 扫描循环：gc/mod.rs:211 spawn 体改为 spawn(async move { let _ = supervise_task(GC_SCAN_TASK, async { loop {...} }).await; }) 形态（沿 primary_tasks.rs:258 现成先例，任务名常量定义于 gc 模块头监督名区）。panic 终局后 JoinHandle::is_finished 变真 → GcHandle::is_active 变假（mod.rs:339-351），既有 reconcile_gc_scan 的 is_active 判定（task.rs:48）即可经 CONFIG SET 调停或角色切换重拉，零新增复位逻辑。
2. flush 任务：cluster_manager.rs:199 spawn 体包 supervise_task(FLUSH_TASK, ...)，Err 后经 manager.upgrade() 复位 flush_running（upgrade 失败即宿主已亡无需复位），幂等门恢复可重拉能力。
3. monitor 循环：wnode/server.rs:929 spawn 体包 supervise_task(MONITOR_TASK, ...)，一次性任务死亡留观测即可，无启动位需复位。
4. 三任务名随 supervise_task 注册自动入 INFO bg_task_health 快照（§130 自研超集行），不新增 INFO 字段。
5. 测试验证点：仿 gc/reclaim.rs:283 test_reclaimer_registers_in_supervise_snapshot——三任务各自拉起后 snapshots() 含本名且 alive 真；注入 panic 任务体验证 panics 计数递增与 alive 复位假；GC 重拉通路已有 config_owner_bridge.rs 覆盖（禁用后重 SET 必重拉）不必重复新增。完成后跑 ./test.sh。

三个常驻周期任务（GC 扫描循环 / 集群配置周期刷盘 / 指标监视采样）未接 panic 监督单点，任务死亡不可观测且部分不可自愈

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 全部常驻后台任务经 TaskManager 注册并具异常必落日志的终止语义：ExpiredKeyDeletionScanTaskAsync 与 CompactionTaskAsync 循环体外层 catch (Exception) 落 LogCritical（libs/server/StoreWrapper.cs:770-773/:716-719，日志明示 The task won't be resumed，任务死亡对运维可观测）；MainMonitorTaskAsync 同为 try-catch (Exception) LogCritical + finally done.Set()（libs/server/Metrics/GarnetServerMonitor.cs:268-321 区间尾部）；ClusterConfigFlushTaskAsync 为构造尾一次性 Task.Run（libs/cluster/Server/ClusterManager.cs:119-140，try-finally 维护 numActiveTasks 计数）。TaskManager.Dispose 关停面 CancelAsync(All) 阻塞等待全部任务退出（libs/server/TaskManager/TaskManager.cs:44-66）。即 C# 契约：常驻任务异常死亡必有日志留痕（CRITICAL/ERROR 级），IsRunning/计数可查。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 已建全仓唯一 panic 监督单点 wbase/src/supervise.rs（模块头自陈「全仓 spawn 任务体 / 逐项处理体的 catch_unwind 一处封装……堵 panic 死亡与空闲不可区分的观测缺口」，INFO bg_task_health 为快照真源），且同族任务全部接线：aof_commit / object_collect（wedb/wnode/src/primary_tasks.rs:258/:317）、aof_size_limit / index_auto_grow（wedb/wnode/src/service.rs:582/:629）、bftree_reclaimer（wedb/wkv/src/gc/reclaim.rs:95，另配重挂环）、committer_loop（wedb/wnode/src/aof/waof_sublog.rs:182）、量化与向量清理双协程、collection_item_broker 主循环、gossip 主循环、failover 双会话、resync/attach 任务。唯三处常驻周期任务裸 spawn 未包监督：
a) GC 扫描循环（过期扫描 + 紧缩判定 + 换号物理回收 + 熔断的唯一完整轮次驱动）：wedb/wkv/src/gc/mod.rs:211 spawn(async move { loop {...} }) 无 supervise_task 包裹，任务体 panic 即静默死亡（默认 panic hook 仅写 stderr，文件日志通道收不到）；GcStats（expired_deleted/compactions 等）在生产 INFO 零出口（gc_stats/gc_running 全仓生产消费 grep 零命中，store_snapshots 为库快照不含 GC），bg_task_health 名单亦无本任务，死亡与空闲彻底不可区分。重拉面仅剩 reconcile_gc_scan 的 is_active 判定（wedb/wkv/src/gc/task.rs:48，仅 CONFIG SET 调停与角色切换触达），无事件级复活。
b) 集群配置周期刷盘任务：wedb/wedb/src/server/cluster_manager.rs:199 spawn(async move { flush_task_async(...) }).detach() 无监督；死亡后 flush_running 停留真值（复位点仅停机臂 dispose_background_tasks :284），start_flush_task 幂等门永闭（虽然其唯一生产调用点为装配期一次，但观测面同样双缺：无日志进文件、无 bg_task_health 条目），集群配置演化停止落盘无任何留痕。
c) 指标监视采样循环：wedb/wnode/src/server.rs:929-947 spawn 后直接 detach，main_monitor_task_async（wedb/wmetric/src/garnet_server_monitor.rs:471）无监督；死亡后 INFO 瞬时吞吐与延迟采样永久冻结，与「无负载」不可区分，无重拉通路（start_server_monitor 仅装配期一次调用）。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
毒丸场景（底层 bug 或介质异常触发单次 panic，如同任务体内紧缩/扫描对损坏记录的处理缺陷）下三任务同时具备「死亡不可观测 + 不自愈」形态：GC 扫描停摆使主动过期清理退化为纯惰性（短 TTL 键物理清除停摆、紧缩判定停摆，磁盘膨胀无告警）；flush 停摆使集群配置演化（槽位迁移、failover 后角色）不落盘，节点重启丢失近期拓扑；monitor 停摆使容量与延迟观测面静默冻结，运维误判无负载。与 C#「异常必落 CRITICAL 日志」契约分叉，亦违反仓内 supervise.rs 自陈的全仓单点封装纪律（同族四邻接任务全接线，三处漏接无任何「有意排除」注释或登记佐证）。

涉及代码：
rust 文件与函数：
wedb/wkv/src/gc/mod.rs:GcManager::spawn（:211 循环无监督）
wedb/wedb/src/server/cluster_manager.rs:ClusterManager::start_flush_task（:192-200）/ flush_task_async（:488-499）
wedb/wnode/src/server.rs:start_server_monitor（:911-950，spawn :929）
wedb/wbase/src/supervise.rs:supervise_task（单点机制，已备未接）
wedb/wkv/src/gc/task.rs:WedbStore::reconcile_gc_scan（:48 is_active 重拉判定）

对应 c# 文件与函数：
garnet/libs/server/StoreWrapper.cs:ExpiredKeyDeletionScanTaskAsync（catch LogCritical :770-773）
garnet/libs/server/Metrics/GarnetServerMonitor.cs:MainMonitorTaskAsync（try-catch LogCritical + finally done.Set）
garnet/libs/cluster/Server/ClusterManager.cs:FlushTaskAsync（:119-140 try-finally numActiveTasks）
garnet/libs/server/TaskManager/TaskManager.cs:Dispose（CancelAsync(All) 阻塞等待）

精炼执行方案：
1. 三处任务体以 supervise_task 包裹（沿 primary_tasks.rs 现成形态）：GC 循环外层 supervise_task(GC_SCAN_TASK, loop{...})，Err 臂将 GcHandle 槽交还 reconcile 语义（置 cancel 或依赖既有 is_active 判定即可，最小改动为仅落监督计数与 log::error）；flush_task 与 monitor 循环同理包 supervise_task（名字入 bg_task_health 名单），Err 臂复位 flush_running / 不需复位（一次性任务死亡留观测即可）
2. 零新机制：复用 wbase::supervise 既有单点，不新建重派器；GC 重拉维持既有 reconcile_gc_scan 通路不加自动重拉（防毒丸风暴，观测补齐后由运维触发 CONFIG SET 或角色切换重拉，与 reclaimer REMOUNT_LIMIT 有界重挂形态对齐裁量）
3. 测试验证点：仿 gc/reclaim.rs:283 test_reclaimer_registers_in_supervise_snapshot 三例——各自任务拉起后 snapshots() 含本名且 alive 真；注入 panic 任务体验证 log::error 落点与计数递增；INFO bg_task_health 行含三个新任务名

合入哈希：305dd17 收口形态：gc_scan/cluster_flush/server_monitor 三任务体仅包裹既有 wbase supervise_task 单点（零新机制零自动重拉），flush Err 臂复位 flush_running 幂等门，三任务名入 INFO bg_task_health 快照，三测闭环注册/复位/panic 计数
