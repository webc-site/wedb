终态：已合入 dev（merge fx0N 系，2026-09-27）。e8b32f0 detach 单机制:句柄就地 detach 交执行器持有,签名无返回值编译期挡复发;锁测 quantization_worker_survives_production_spawn_path

甄别结论：通过 | 定级 P1 | 2026-09-27 zcode fixloop 甄别席（双侧锚现码亲验，零撞票）
执行提示：与 ing 池量化两票构成依赖序：本票存活面先行；行号漂移 service.rs 实 2041

审核结论：通过（P1 假旋钮/worker 零存活真案。胜负手实证：compio-executor-0.1.4/join_handle.rs:148-155 Drop 臂 task.cancel(true)，task/mod.rs:238-249/261-263 future 未经 poll 即 drop；service.rs:2024 唯一生产拉起点返回值语句末即丢，get_session 同步栈，取消先于首轮 poll——生产量化 worker 零存活；reclaim.rs:78-82「fire-and-cancel 须 detach」纪律注与各处 .detach() 反证唯一违例点成立；EventWorkQueue 无界（event_queue.rs:12）+每 VADD 只进不出（vector_manager.rs:708-717、migration:200-207）叠加背压违背独立成立；C# 亲验 Quantization.cs:70-76 存数组托管、VectorManager.cs:228-231 WhenAll 注——.NET 弃引用不取消，转写漏语义定性准确。与 todo 量化落域票关系判正交非双立：本票系 worker 生命周期面（永不存活），落域票系存活后域绑定面；本票当前遮蔽落域票生产现形，两票并立且本票应先行）

整理执行方案（审核席认可，供 fix 消费）：
1 源头就地 detach、start_quantization_tasks 签名收敛无返回，编译期挡复发，单机制零扩面
2 残余注记：detach 失 C# Dispose 停机 join 面，与全仓 detach 纪律一致可接受，不另立托管结构
3 锁测：拉起点后 worker 存活跨多轮 poll 断言 + EventWorkQueue 消费面出队观测

量化 worker 协程生产拉起点丢弃返回 JoinHandle 即生即灭：建表回填全链静默失能且量化通道投递无人消费无界积压

问题分析：
1 Garnet 契约对齐
C# VectorManager.Quantization.cs:StartQuantizationTasks(:70-75) 把执行体 Task 逐一存入 quantizationTasks 数组字段持有（.NET 线程池任务引用丢弃亦不取消，数组另供 Dispose 的 Task.WhenAll 收口，VectorManager.cs:231 注释自证）。rust 对位口 vector_manager_quantization.rs:start_quantization_tasks(:77-91) 返回 Vec<JoinHandle<()>> 语义即「交调用方托管」，但 compio 的 JoinHandle Drop 即 task.cancel（compio-executor-0.1.4/src/join_handle.rs Drop 臂 :148-155；Task::cancel 后 run 检 cancelled 直接 Ready 终局，任务未来体未经 poll 即 drop_future），本仓同域纪律已明文：wkv/src/gc/reclaim.rs:78-82 自陈「compio JoinHandle 直抛是 fire-and-cancel，必须经 detach 才是后台运行」。
2 工程现状确证
生产唯一拉起点 wedb/wnode/src/service.rs:get_session(:2014-2025)：配额内每次调用 self.vector_manager.start_quantization_tasks(1); 返回值未接、语句末即 Drop——全部量化 worker 拉起即被取消，生产形态零存活消费者（get_session 为同步栈，取消必先于首轮 poll）。对照同函数内清理协程 ensure_cleanup_tasks_started 句柄收进 CleanupRuntime 托管（vector_manager_cleanup.rs:116）、同仓其余后台循环一律 .detach()（reclaim.rs:107、primary_tasks.rs:266/325、service.rs:602/651），本调用点系全仓唯一把受监督后台任务句柄就地丢弃的拉起点。既有测试（wvector quant_train_barrier/quant_enable_barrier_race、wnode 量化族）均自行 spawn 持柄或手动驱动 worker，恰漏检生产拉起链。
3 逻辑危害确证
其一，量化功能面静默失能：量化开关打开的部署里建表与回填永无人执行，向量集恒全精度，VSIM/内存收益承诺落空且零报错（板块 5.2「配置全链路真接线，严禁只读不用假旋钮」的执行面对撞——旋钮拉起的任务即生即灭）。其二，无界积压（板块 3.2 消除无界开销）：VADD 每中 DiskAnnInsertResult::QuantizationRequested 臂即向无界 EventWorkQueue 推一条携键 QuantizationState（vector_manager.rs:708-717，migration 臂 vector_manager_migration.rs:200-207 同）；表永不建成致该臂持续命中，队列只进不出，键集越大积压越深。其三，观测脱节（板块 4.2 状态闭环）：worker 从未 poll 则 INFO bg_task_health 无 quantization_worker 行、quantization_requests_processed 计数恒 0（register_counter 自陈「计数冻结与空闲不可区分」缺口在此坐实）；若个别时序下已首轮 poll（Supervised 已置 alive），取消路径不经 Ready poll，wbase/src/supervise.rs:113-131 的 alive 复位点落空，存活位卡真成永久假活着——本票修好拉起点后该残余面另席甄别。

涉及代码：
rust 文件与函数：
wedb/wnode/src/service.rs:get_session（:2023-2025 丢弃返回句柄）
wedb/wnode/src/resp/vector/vector_manager_quantization.rs:VectorManager::start_quantization_tasks（:77-91）
投递面：wedb/wnode/src/resp/vector/vector_manager.rs（:708-717）、vector_manager_migration.rs（:200-207）；队列 wedb/wbase/src/pool/event_queue.rs:EventWorkQueue（对标 C# CreateUnbounded）
取消语义锚：compio-executor-0.1.4/src/join_handle.rs JoinHandle::drop；同仓纪律注 wedb/wkv/src/gc/reclaim.rs:78-82

对应 c# 文件与函数：
libs/server/Resp/Vector/VectorManager.Quantization.cs:StartQuantizationTasks（:70-75，任务存 quantizationTasks 数组）
libs/server/Resp/Vector/VectorManager.cs:Initialize/Dispose（:240-255、:231 WhenAll 收口注）

精炼执行方案：
1 单机制收口：start_quantization_tasks 内对每个 spawn 句柄就地 .detach() 并去返回值（签名改无返回），杜绝「调用方须托管」的第二处纪律依赖；生产拉起点 service.rs:2024 随之无需改动。对标同仓 reclaim/primary_tasks 的 detach 唯一后台形态，不引入句柄存储新结构（清理协程的 CleanupRuntime 托管臂系停机 join 需要，量化 worker 无对位在网要求，勿扩面）。
2 测试验证点：a) 装配级锁测——经 get_session 生产路径拉起 worker 后投 VADD 触发 QuantizationRequested，断言通道条目被真实消费（quantization_requests_processed 递增、建表发生、bg_task_health 出 quantization_worker 行）；b) 现锁族 quant_train_barrier/quant_enable_barrier_race/wnode 量化回归全绿；c) 取消语义回归防复发：start_quantization_tasks 签名收敛后编译期即挡丢弃形态。
