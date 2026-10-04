终态：已合入 dev（2026-09-27）。e12bc00 Supervised 补 Drop 三臂复位幂等重合;gc/task.rs 替换臂先 drop 再重拉收窄误清窗;4 新测试含 gc 换入禁用即停

甄别结论：通过 | 定级 P3 | 2026-09-27 zcode fixloop 甄别席（双侧锚现码亲验，零撞票）
执行提示：Supervised 补 Drop 单机制收口全部取消站点，poll 热路径零改动

审核结论：通过（两订正已并入：rust 路径补 wedb/ 层、置位「永久」与「一行取放即免疫」措辞收窄。锚点亲验：compio-executor-0.1.4 join_handle.rs:148-153 cancel(true)、task/mod.rs:255-264 cancelled 即 Ready 弃未来不经 poll 实证；Supervised 仅 Ready/panic 两复位臂、无 Drop 属实；GC 替换重拉生产链（task.rs:46-53 + mod.rs:382-389）坐实非假想；C# TaskManager.cs:139-154 取消即确定性收敛对位亲验。方案单点 Drop 收口覆盖全部现有与未来取消站点、poll 热路径零改动，合零开销纪律；测试 a/b 两点各锁一径）

监督任务强取消路径丢弃未来不经 poll，Supervised 无 Drop 收口致存活位卡真：GC 句柄兜底强取消坐实，INFO bg_task_health 把已死任务谎报为活、不再重拉面下即永久

问题分析：
1 Garnet 契约对齐（C# 原型行为与协议约定）
C# 后台任务观测面收口口径：TaskManager.cs:CancelAsync（libs/server/TaskManager/TaskManager.cs:139-154）registry.TryRemove 命中后 IsRunning（:30-41）立即翻假——取消路径下存活观测位确定性收敛，绝无「任务已死、注册表仍报活」形态。rust 对位观测域 wedb/wbase/src/supervise.rs 模块头自陈对标 IsRunning/IsRegistered 快照面、立意堵「panic 死亡与空闲不可区分」；取消路径的复位收口在转写中缺失，观测承诺破洞。

2 工程现状确证（Rust 现有实现路径与代码缺陷）
Supervised 未来（wedb/wbase/src/supervise.rs:94-98）的 alive 复位仅两处：poll 得 Ready（:114-117）与 poll panic（:121-125），结构无 Drop 臂。compio-executor-0.1.4 取消语义锚：JoinHandle::drop 调 task.cancel(true)（join_handle.rs:148-153）；Task::run 见 cancelled 位直接 return Poll::Ready(())（task/mod.rs:255-264），任务未来体未经再 poll 即整图丢弃——Supervised 处于 sleep/await 停泊中被强取消时，Ready 复位臂永不触发，存活位卡真。注册条目按 &'static str 同名归组且进程常驻（:84-90 leak 换静态），卡位后唯一翻假点是同名新实例走完 poll 终局臂顺带清位——凡不再重拉该任务名的场景（禁用即停、析构窗口），卡位即无翻假点。
生产触发链坐实（非假想形态）：
其一，wedb/wkv/src/gc/task.rs:WedbStore::reconcile_gc_scan 替换重拉臂（:46-53）自陈「旧句柄 Drop 兜底强取消残留任务」：stop_gc 置协作位后、任务尚停泊于 gc_scan_loop 的 sleep(interval)（wedb/wkv/src/gc/mod.rs:306-331）未再 poll 时，start_gc 走 :48 is_active 判定（cancel 位已置即判假）进入替换，旧 GcHandle::drop（wedb/wkv/src/gc/mod.rs:382-389）丢弃 JoinHandle 即 cancel(true) ⇒ GC_SCAN_TASK 条目卡真。引擎在线置换、WedbStore 析构路径同臂触发。
其二，量化票 task/todo/wnode-quant-worker-handle-discard-instant-cancel 危害三末段明注「取消路径不经 Ready poll，alive 复位点落空，存活位卡真成永久假活着——本票修好拉起点后该残余面另席甄别」，本票即该另席立案；该票 detach 修复后量化 worker 不再被取消，本机制洞对其遮蔽面解除。
其三，wnode worker 运行时析构（server.rs 停机收册）drop 全部在挂任务，同走不经 poll 丢弃路径——停机窗口的快照同样失真，属同根次级面。
同域其余 supervise_task 环（primary_tasks 双环、reclaim reclaimer、monitor、gossip 主环、cluster flush、service 体积限额/索引扩容环）均以协作标志或条件判定的 Ready 退出 + panic 臂复位收口、无外部句柄取消面，不卡位，不在本票射程。

3 逻辑危害确证（板块 4.2 状态闭环）
bg_task_health 的唯一「死亡观察者」在取消路径系统性失真且方向是谎报存活：运维见 GC_SCAN_TASK 行 alive=1 判定过期键扫描环在跑，实则禁用即停/换入收口后该环零存活——比模块头立意堵的「死亡与空闲不可区分」更劣（空闲至少语义为可能活着，此为死任务假活，且在不再重拉该任务名场景下即永久）；观测信号恒真使「环死」告警面永不开口。后续同名新实例 supervise_task poll 置真与旧实例卡真不可分，快照对该任务名彻底失去判死力。零资源泄漏面：强取消的任务本体已被执行器回收，不涉句柄托管缺失。

涉及代码：
rust 文件与函数：
wedb/wbase/src/supervise.rs:Supervised（:94-132，poll 复位臂 :114-125，无 Drop 收口）
wedb/wkv/src/gc/mod.rs:GcManager::spawn（:217-236 任务内挂 supervise_task）、GcHandle::drop（:382-389 JoinHandle drop 兜底强取消）
wedb/wkv/src/gc/task.rs:WedbStore::reconcile_gc_scan（:26-54 替换重拉臂为生产触发点）
取消语义锚：compio-executor-0.1.4/src/join_handle.rs:148-153、src/task/mod.rs:255-264（run 检 cancelled 即 Ready，弃未来不经 poll）

对应 c# 文件与函数：
libs/server/TaskManager/TaskManager.cs:CancelAsync（:139-154，registry.TryRemove 即收口）、IsRunning（:30-41）

精炼执行方案：
1 wedb/wbase/src/supervise.rs：为 Supervised 补 Drop 实现——track_alive 为真时 entry(self.task).alive.store(false, Relaxed)；与 poll Ready/panic 复位臂幂等重合，专补「未经 poll 即被丢弃」的强取消与运行时清理路径，单机制收口全部监督任务的取消观测面（含未来新增取消站点），poll 热路径零改动。
2 wedb/wkv/src/gc/task.rs reconcile_gc_scan 替换臂次序界定：先 slot.take() 并 drop 旧句柄（强取消 + 经步骤 1 清位），再执行 *slot = Some(GcManager::spawn(...)) 重拉——同名归组下把旧句柄 Drop 误清新实例存活位的窗口收窄至最小（步骤 1 的伴生次序面）；注记边界：取消后旧未来由执行器异步 drop，先 take 后 spawn 仅压缩窗口而非免疫，残余瞬态误清由新实例下一轮 poll 置真自愈——勿为此扩代际机制。
3 测试验证点：a) wbase 锁测：监督任务完成首轮 poll 后丢弃其 JoinHandle（模拟强取消），断言 snapshots() 该条目 alive=false；b) wkv 锁测：start_gc 令任务停泊 sleep 后 stop_gc+start_gc 换入，断言 GC_SCAN_TASK 行不卡真、新环 poll 后回真；c) 既有 supervise 族与 gc 族回归全绿，INFO bg_task_health 快照输出面零格式变更。
