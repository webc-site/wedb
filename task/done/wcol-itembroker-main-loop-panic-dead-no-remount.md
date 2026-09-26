甄别结论：通过（甄别席 zc-fix-r25-甲，2026-09-26）定级 P1
核验记录（现码复跑，非票面背书）：
1 永拒重拉形态现码亲验：collection_item_broker.rs:375-397 start_main_loop CAS(NOT_STARTED→STARTED) 在位，任务体 `let _ = supervise_task(BROKER_MAIN_TASK, ...)` 丢弃 Err 载荷——panic 后状态永停 STARTED，main_loop_task_status 写点全仓仅构造 :300/CAS :376-381/dispose swap :801 三处（grep 亲验），无任何复位臂，重拉面零通路属实。
2 通道一次性复验：start_async :724-727 `events_rx.lock()…take()` + None 即 return 在位，仅复位 CAS 不重建通道必空转——审核席补强的「通道重建+存量补扫」两条为必要面；unbounded_async 构造 :291 与逐事件 supervise_item 隔离（:742 区）同在位，外层臂（rx.recv/周期清扫/done 投递）确为逐项守卫之外的真实 panic 面。
3 挂起面锚验：slow.rs 头注 timeout=0 无限等待形态与 §24/§63 引用在位（登记的是无等待面/冷键竞速语义，非 CAS 复位面，正交）；C# StartMainLoop :118-126 Interlocked CAS 一次性原文亲验。
4 查重：deviations.md itembroker 命中区（:441-445）系观察者状态机与 clean_keys 清扫登记，无主循环死亡复位面；四池无同轴票（ing 池无 broker 票）。
5 架构合规与可执行度：Err 臂复位 CAS + 有界重挂沿 reclaimer REMOUNT_LIMIT 先例、通道换装走既有 unbounded_async 单点、补扫重投靠既有幂等状态机承接——零新机制，compio 下 spawn/supervise_task 原生兼容；测试点（注入 panic 双测/DISPOSED 终态拒复位/上限连败断言）闭环。定级 P1：panic 后阻塞客户端永挂+事件队列无界堆积，触发条件为外层 panic（逐事件已隔离故非确定性挂死），不入 P0。

审核结论：通过
审核席 zcode-r21-review-broker（2026-09-26）。双侧源码亲验全属实：rust 侧 start_main_loop（collection_item_broker.rs:375-397）load==NOT_STARTED + CAS(NOT_STARTED→STARTED)，任务体 :389 let _ = 丢弃 supervise_task 的 Err(PanicPayload)，main_loop_task_status 写点全仓仅构造/CAS/dispose swap 三处，panic 后停留 STARTED 永拒重拉，属实；supervise.rs 模块头自陈 Err 臂复位纪律，broker 确为唯一监督在位却不落实复位的消费点（reclaimer remount 环 reclaim.rs:83-160、primary_tasks.rs:258-263/:317-322、gossip_manager.rs:98-114 三先例亲验在位）；timeout=0 无限等待（slow.rs:8 头注 + deviations.md §24）与无界堆积成立且更顽固——clean_keys_to_observers 仅主循环内调用（:770），死亡后观察队列死节点永不弹出、键队列永非空、同键写持续入队；C# 对位准确（StartMainLoop :119-125 同 CAS 一次性，StartAsync :688 try/finally 无 catch 且生产异常面趋近零，Dispose :769-779），rust unwind 语义使形态真实可达。查重：deviations.md §24/§33/§63 及现存各票均正交。一处修正：原票第 9 行举例「配对路径越界」已被 supervise_item（:742）逐事件隔离兜住，主循环整体死亡的真实 panic 面在逐项守卫之外——rx.recv().await 队列臂、clean_keys_to_observers 周期清扫臂、done_tx 投递臂，论证不受影响。执行方案一处遗漏已补（见优化方案第 2 条通道重建）。

collection_item_broker 主循环 panic 死亡后 CAS 启动位永真拒绝一切重拉，无超时等待者永久挂起且事件队列无界堆积

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# CollectionItemBroker.StartMainLoop（garnet/libs/server/Objects/ItemBroker/CollectionItemBroker.cs:118-126）以 Interlocked.CompareExchange(mainLoopTaskStatus, MAIN_LOOP_STARTED, MAIN_LOOP_NOT_STARTED) 单次拉起 Task.Run(StartAsync)。.NET 运行时无 panic 穿透形态：托管代码意外异常同样杀任务，但 StartAsync 主循环内全部路径为受控队列消费与配对（AnySessionWaitingForCollectionItem/PairObservers 族纯内存操作），生产异常面趋近于零；Dispose（:769-779）置 MAIN_LOOP_DISPOSED 后 done.Wait() 排空。即 C#「一次性 CAS 拉起」成立的前提是任务体事实无异常终止面。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 侧 wedb/wcol/src/itembroker/collection_item_broker.rs:375-397 start_main_loop 以 main_loop_task_status CAS（MAIN_LOOP_NOT_STARTED → MAIN_LOOP_STARTED）单次拉起，:388-395 任务体经 supervise_task(BROKER_MAIN_TASK, ...) 包装但 Err 臂 let _ = 直接丢弃 PanicPayload——监督只落 log::error 与计数，main_loop_task_status 停留 MAIN_LOOP_STARTED，start_main_loop 的 CAS 永远失败，全仓无任何重拉通路（对比同仓四先例：bftree_reclaimer 的 remount 重挂环 gc/reclaim.rs:128、gossip 主循环的 MEET/入站合并事件安全点重拉、primary_tasks 的 started 复位 + CONFIG SET/resume 重拉、GcHandle 的 is_active 判定重拉）。supervise.rs 模块头自陈设计意图「调用方在 Err 臂复位各自幂等启动位，由既有调停/事件重拉通路自然复活」，broker 是唯一 supervise 在位却不落实复位与重拉的消费点。事件队列为 crossfire unbounded_async（:291，对标 C# AsyncQueue 同无界），消费侧死亡后 :804/:821 的 try_send 持续无界堆积。等待面：BLPOP/BRPOP timeout=0 无限等待（wnode/src/resp/objects/list_commands/slow.rs:8 头注明示），仅 ObserverDropGuard 在连接终止/会话 dispose/脚本取消时摘除观察者兜底。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
主循环体（start_async：NewObserver 登记、CollectionUpdated 配对、SessionDisposed 清扫、超时批扫）任一处 panic（如配对路径对畸形输入的未预期越界）后：其一，全部已登记观察者永不配对出件，timeout=0 的阻塞客户端在连接存活期间永久挂起（C# 同形态因无 panic 面不可达，rust 的 unwind 语义使其真实可达）；其二，keys_to_observers 非空期间每次同键写均经 handle_collection_update 入队 CollectionUpdated 事件，无界队列随写入流量无限增长（内存无界膨胀），违反「后台任务队列必须具备背压与高低水位限制」的通用维度；其三，死亡仅 bg_task_health 计数可见，业务面（阻塞命令行为）无差别劣化，运维无从定位。

涉及代码：
rust 文件与函数：
wedb/wcol/src/itembroker/collection_item_broker.rs:CollectionItemBroker::start_main_loop（:375-397，Err 臂丢弃 + CAS 永真）
wedb/wcol/src/itembroker/collection_item_broker.rs:enqueue_event / handle_collection_update（:819-823/:410-424，unbounded_async :291）
wedb/wnode/src/resp/objects/list_commands/slow.rs:BlockWaitFace（timeout=0 无限等待）
wedb/wbase/src/supervise.rs:supervise_task（Err 臂复位纪律出处）

对应 c# 文件与函数：
garnet/libs/server/Objects/ItemBroker/CollectionItemBroker.cs:StartMainLoop（:118-126）/ Dispose（:769-779）
garnet/libs/server/Utilities/AsyncQueue.cs（无界队列同构面）

精炼执行方案（审核席整理优化版，原票三条保留、补通道重建与存量自愈两点）：
1. start_main_loop 任务体 Err 臂复位 CAS 位：supervise_task(...).await 为 Err 时将 main_loop_task_status 由 MAIN_LOOP_STARTED compare_exchange 回 MAIN_LOOP_NOT_STARTED，随后沿 reclaimer 有界重挂形态（reclaim.rs remount_reclaimer 先例，REMOUNT_LIMIT=3）退避重挂，连败留 bg_task_health panic 计数与复位态退出，后续首个 start_wait（新阻塞命令）重新拉起
2. 通道重建（原票遗漏，必改）：start_async 开头 events_rx.lock().take()（:724-727）一次性取走 rx 随任务体 move，首次任务体 panic 死亡时 rx 已析构——仅复位 CAS 位不重建通道时，重挂任务体再入 start_async 得 None 直接 return（:728-730），重挂空转。复位臂须成对重建事件通道：unbounded_async 新建 (tx, rx) 原子换装（tx 换装选型遵守数据面纪律：enqueue_event 为热路径写入通知，不得加常驻同步锁；panic 重挂为罕见控制面路径），旧通道丢弃；done_tx 在 panic 路径未消耗（start_async 尾部投递未执行），重挂任务体退出时照常投递
3. 存量自愈（原票遗漏）：死亡窗口内 NewObserver/CollectionUpdated 事件滞留旧通道随重建丢弃，滞留观察者分两类——已挂 keys_to_observers 键队者与仅入 session_id_to_observer 未挂队者（NewObserver 事件未被消费），重挂任务体进入主循环前须对两表补扫重投（逐键重投 CollectionUpdated、逐观察者重投 NewObserver），配对幂等性由既有状态机承担（try_assign 拒弹死节点、判空挂队、状态校验）
4. dispose 语义保持：MAIN_LOOP_DISPOSED 终态不受复位影响（复位 CAS 期望值 STARTED，dispose swap DISPOSED 后复位 CAS 不匹配自动失败，无需显式预判）
5. 测试验证点：注入 panic 的 spawner 双测——复位 + 通道重建后重挂任务体可消费新事件（阻塞命令恢复出件）；死亡窗口内登记的滞留观察者经补扫重投后恢复配对；DISPOSED 后复位被拒且终态保持；重挂上限连败后计数与复位态断言；超时 BLPOP 在主循环死亡窗口内仍由超时臂兜底返回

合入哈希：18f5adfcbe689ccb9e8e117fccec05de881a2812 收口形态：start_main_loop Err 臂进入 remount_main_loop 有界重挂（REMOUNT_LIMIT=3，留 NOT_STARTED 复位态非死亡态）、events_tx 升 ArcSwap 换装重建通道（enqueue_event/dispose 热路径无锁 load）、recover_after_panic 定序「重建通道→双表存量补扫→复位 CAS(STARTED→NOT_STARTED)」且 DISPOSED 终态拒复位、观察者新增 OnceLock keys 供未挂队者重投 NewObserver；tests 四形（通道重建弃洪峰有界补扫/DISPOSED 拒复位/NOT_STARTED↔STARTED 可重认领/GateSpawner 泵起新循环消费补扫出件）+ 模块内 bg_task_health panic 计数登记，全域 cargo check --tests 零警告、wcol 30 测全绿。退避取 yield_now 协程让渡（compio sleep future 非 Send）。偏差登记 §157。
