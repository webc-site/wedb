执行追记（性能席 sess_70b6，2026-09-27 四轮，waker 真唤醒形三形实验失败归因+回滚）：用户直令修复本红，三形实测全败已回滚（rmw_window/bucket 零残留，基线=10bfc98 通知位形）：
①公平位+礼让+instant 拆分形：10/10 红（第 0 轮即败）——等闩者被自己的 WAITS 位在入口自礼让自拖，且 collector 慢臂单轮持闩实测 ~5-8s（xctrace 时序铁证：快臂 3-5ms/轮放闩与慢臂单轮 5-8s 零放闩交替），等闩者轮询预算在慢臂轮内必然烧尽。
②waker 真唤醒形（AtomicWaker 表按 scoped hash 注册 + RmwWindow::drop 放闩单点 wake + 公平位 + 8s 超时兜底）：唤醒链验证全通（reg→wake n=1 相邻毫秒级），但唤醒重试 CAS 败于 collector 重入（让渡单 yield 窗 µs 级 < 等闩者 compio 唤醒 poll 延迟），且 **collector 慢臂单轮持闩实测 8s+（随集合规模无上界增长）**，等闩者任何有限 deadline（2s/5s/8s）均被慢臂轮吞没超时。
③超时+ever_woken 续等形（段超时后按「曾收到唤醒」无限续等）：引入新死锁——**collector 慢臂取闩同样走 rmw_window 挂起环**，等闩者挂起不持闩时 collector 慢臂的挂起无放闩者唤醒，双向互等 180s 挂死；修 ever_woken 判据（first-poll 非唤醒证据）后仍 117s 红且 collector 侧 120s 零放闩零取闩迹象（慢臂在 W 置公平位后长持闩）。
**结构性结论**：C# 对位面（ObjectCollect Common.cs:807）同为单轮全程持闩（分批 DbScan 只批 IO 不放闩，WriteUnlock 在 finally），写者靠 Tsavorite pending 无界等待——wedge 的 fail-closed 秒级界（钉闩测试 10s 锁）与 collector 慢臂无界持闩在**同键场景结构性互斥**，纯 wkv/windex 侧等待机制无解。可行方向收窄：④collector 慢臂分批放闩（对齐 C# DbScan 批界每批间隙让闩，改 tiered_collection_ops 持窗策略——需域主裁夺持窗正确性边界）；⑤钉闩 fail-closed 界与 collect 测试解耦（collect 双案的写者断言改为允许显式重试——改测试语义需票面裁决）；⑥等待方按「放闩方活跃度」有界续等（本席③的修正版，须先解 collector 慢臂侧挂起的唤醒缺失）。本席证据文件：/tmp/rmw_diag.txt（waker 形全时序）。iter 交回 fix 席统筹。

执行追记（性能席 sess_70b6，2026-09-27 三轮）：公平位+步进形实验失败归因实测（十连 10/10 红、写者第 0 轮即败，较通知位形 2/10 恶化，实验后已回滚改动面零残留）。装配=桶字共享计数缩 14→13 位腾位 61 做等待者登记位 + rmw_window 单键臂失闩 mark/得闩耗尽 unmark + try_lock_exclusive 入口见位让渡 + 等闩者重试拆 try_lock_exclusive_instant（避免自见自让）+ 未见通知时 compio sleep 100µs 步进。失败根因：**等闩者的唤醒延迟与让渡窗量级错配**——compio timer 挂起唤醒实测精度毫秒级，而新来者让渡窗（单次 yield_now）微秒级，等闩者醒到 CAS 时新来者已重入持闩；「公平位+通知位」两信号皆在，唯缺**即时唤醒通道**（放闩侧主动 ping 在册等闩者，futex/eventcount 形），轮询+让渡组合在 collector 紧循环下恒处相位劣势。方向 A 升级形的可行落点收窄为二：①放闩侧真唤醒（桶上登记等闩者事件通道，unlock_exclusive 时 notify——需异步域等待原语，wbase 现无）；②等闩者让核步进与新来者让渡窗同量级（~µs 级 sleep/自旋短步，但步进过密即退化忙等形、过疏即相位错开——窗比不可调，1ms 步进 100% 红即此理）。建议 c01k 权衡：通知位形 2/10 或已是纯轮询族上界，闭环须上真唤醒（①），或接受写者侧系统性概率错误改票方向（调用面重试语义化）。本席实验码基线：bucket 位段未动回滚、rmw_window 未动回滚，10bfc98/7a2658a 现状即最新基线。

执行追记（fix 席 r27，2026-09-27 同日二轮）：沙箱席 c01a 落地方向 A 成通知位形（合入 10bfc98/7a2658a——放闩单点置位、等闩者消费即重试，rust_review 席七项全过），但 collect_arm 双案十连跑实测 2/10 红且红案断言与修复前同根（-ERR slow path storage error，:167 writer 断言），notify 形未闭环本票。根因再定位：通知位只加速「感知放闩」，写者消费重试与 collector 紧循环 re-collect 的下一轮取闩仍公平竞争，闩空闲窗纳秒级下重试胜率不升——**公平性缺位才是根，感知延迟只是次生**。方向 A 须升级为「新来取闩者礼让在册等待者」形：桶字增等待者登记位段（或独立原子），新来 try_lock_exclusive 见有等待者在册即先让渡一轮再试，放闩置位通知等待者消费——新来者让渡即打破紧循环抢占，对位 C# LockTable pending 队列的公平语义；已落通知位保留为感知层（普通争用下减少无谓让核延迟），本轮红率实证：忙等形 0% 红/纯 yield 形 ~20%/1ms 步进形 100%/通知位形 2/10（含两轮 24-27s 长耗，写者挣扎变久仍败）。钉闩场景（无等待者登记、无放闩）行为不得劣化，预算环 fail-closed 不变。迭代派沙箱席 c01k（携本追记与 10bfc98 现码基线）。

甄别结论：通过（fix 席 r27 本席自甄，2026-09-27，定级 P1——同键 collect 与写并发负载下写者概率性存储错误，collect 密度越高越恶化）。真实性：全部判据系本席 2026-09-27 门禁循环亲手实证，非票面背书——三形态红率对照（旧嵌自旋形 0% 红/纯 yield 形五连跑一红/1ms sleep 步进形连跑 100% 红第 3 轮即炸）当场复跑取证；现码锚 rmw_window.rs 预算环（RMW_LATCH_YIELD_BUDGET=1024 轮间纯 yield_now）与 windex bucket.rs 无放闩唤醒机制（try_lock_exclusive 纯 CAS）亲验在位；collect_arm_rmw_window_scope.rs 双案系仓库 init 4878408 即在库的既有测试，回归归属 b02b 收口（237f478）白纸黑字。C# 对照：InternalRMW.cs 失闩 RETRY_LATER 转会话 pending 由调度唤醒承接、非盲轮询，亲验成立。查重：deviations.md 全册与 task 五池无同案；b02b done 票门禁追记已指认本回归并引向本票，非重复系承接口。架构合规：方案 A（HashBucket 增放闩唤醒等价物）零新锁表零全局态、分层单向（windex 内聚）、两臂与 ttl/wtxn 同桶闩消费面共享单套机制；方向 B/C 已标注弱性供审核裁夺。可执行度：改动点（bucket.rs 放闩通知位 + rmw_window 两环等唤醒）、验证闭环（两案十连跑 0 红 + 钉闩秒级 LockTimeout 界不回退）齐备。格式纯粹。派沙箱席 c01a。

问题分析：
1 Garnet 契约对齐：C# 会话取本键排他闩失败回 RETRY_LATER 转会话级 pending，由调度在闩释放后唤醒承接（garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRMW.cs 失闩即 return status 转调度级 pending 重试），等闩者非盲轮询、无饿死面；rust 侧对应物为 wkv 异步臂让核预算环 + 耗尽回 LockTimeout（fail-closed 有界防御）。
2 工程现状确证：票 wkv-rmw-window-single-key-async-latch-budget-latency-bomb（合入 237f478 + c39c306）把单键两臂收口为轮内单次尝试 + 轮间 `wbase::future::yield_now`（wedb/wkv/src/session/rmw_window.rs 的 rmw_window 预算环，RMW_LATCH_YIELD_BUDGET=1024）后，既有测试 wedb/wnode/tests/collect_arm_rmw_window_scope.rs 双案（hcollect/zcollect_concurrent_*_serializes_behind_window，仓库 init 即在库）双线程双 Runtime 真并发下，对面写者 HSET/ZADD 新建写以 ~20% 概率收 `-ERR slow path storage error`（`[ERROR] run_async_rmw rmw_window failed: Index(LockTimeout)`，单测连跑五次一红实证）。三形态对照实证：旧嵌自旋形（轮内 1024 自旋 ≈3.5s，自旋环即纳秒级高频采样）0% 红；收口后纯 yield 形（轮成本亚微秒，1024 轮总时长亚毫秒）~20% 红；1ms sleep 步进形（降频采样）100% 红（第 3 轮即炸）——collector 线程空转循环 re-collect 令闩空闲间隙仅纳秒级占比，盲轮询采样相位决定胜负、无公平性保证：降频必饿死，同频则竞速偶败。yield 形亚毫秒总预算亦扛不住持闩者单轮 collect 执行（毫秒级），两因复合。
3 逻辑危害确证：生产同键 collect 族命令（HCOLLECT/ZCOLLECT 收缩回收）与点查写（SET/HSET/ZADD 等 try_rmw_window 降级慢路径）并发负载下，写者概率性收存储错误应答；客户端可重试但系统性偏差，collect 循环越密写者胜率越低，极端下写通道事实不可用。系 b02b 收口的行为回归（旧忙等形自旋即高频采样从不饿死），非新缺陷。

涉及代码：
rust 文件与函数：
wedb/wkv/src/session/rmw_window.rs:rmw_window 预算环（RMW_LATCH_YIELD_BUDGET 轮间 yield_now）/ try_acquire_rmw_plan / rmw_window_sorted 多键环同形
wedb/windex/src/bucket.rs:try_lock_exclusive / try_lock_bucket_exclusive 内嵌 kMaxLockSpins（现无放闩唤醒/公平队列机制）
wedb/wnode/src/resp 慢路径存储错误包装面（run_async_rmw → -ERR slow path storage error）
wedb/wnode/tests/collect_arm_rmw_window_scope.rs:hcollect/zcollect_concurrent_*（红案复现夹具）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRMW.cs:失闩回 RETRY_LATER 转会话 pending
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/LockTable.cs:锁冲突 pending 唤醒语义（等闩者由放闩侧唤醒承接）

精炼执行方案：
1 方向 A（对齐 C# 唤醒契约）：windex HashBucket 增放闩唤醒等价物（放闩侧置通知位，等闩者预算环改「等唤醒为主、限次盲采为辅」），消除采样相位竞争；分层单向、单套机制，两臂与 ttl/wtxn 同桶闩消费面共享。
2 方向 B（有界防御重定）：预算环高频保持（纯 yield）+ 预算总额度按「持闩者单轮执行时长上界」重定，耗尽仍回 LockTimeout——承认可超时，但须配调用方重试语义说明，弱于 A。
3 方向 C（调用面退避）：collect 族取闩侧对点查写让位（退避/让优先）——只在 wnode 命令面治标，wkv 机制缺口仍在，不推荐单选。
4 测试验证点：collect_arm_rmw_window_scope 两案连跑 ≥10 次 0 红；wkv 单键钉闩案（钉闩下秒级预算内回 LockTimeout 的耗时上界断言）不回退；无争用点查写回归全绿。

---

## 收口记录（2026-09-28，R5-8 收尾席）

收口形态：方向 A 升级形——桶字第 61 位等闩让渡登记位（共享计数 14→13 位腾位）+ try_lock_exclusive 公平门控臂（见册单次判定让位，对位 C# LockTable pending FIFO 序）+ 登记人绕开臂 try_lock_exclusive_now + rmw_window 单键/多键两臂 RAII 入册（HandoffReg，成功/耗尽/换桶/panic 皆出册，入册 priming 消费滞留通知位防伪见证）+ 活跃度门控有界续等（入册后放闩见证才续等，4096 轮 + 4s 墙钟双界，钉闩零续等 fail-closed 形制逐轮不变）。

合入：b62d2c82（652c29a8 fix + 84d1c377 test，+224/-24，双参 diff 与票面范围严合）。

实测（收尾席亲手，不背书前席）：collect 双案十连 10/10 PASS；钉闩楔形套（rmw_window_single_key_latch 4 案）十连 0 红；windex 全量 48/48；wkv 相邻面（rmw_window 筛/ttl/ttl_purge）全绿；wnode acl 58/58、blocking 32/32、net_pump 30/30。

甄别记录：中途楔形长尾红经双二进制 A/B（补丁形 3/20 红 vs 基线形 4/20 红、红集中同时间窗）判系外部满核负载（yes 进程×3 + 他席测试，loadavg 31→56）非补丁回归；未改任何断言。

遗留注记：「摘让闩序定点反证」在当代满核环境未复现红（饿死相位被放闩窗拉宽掩盖），让闩序机制依赖性的直接反证待安静窗复验；楔形侧基线同态复红反证成立。
