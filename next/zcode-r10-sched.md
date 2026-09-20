轮10 调度视角:线程调度与亲和面(compio thread-per-core 落地质量)

架构盘点(基线事实,与 r9-load 总述一致不重复)
主线程 1 个 compio runtime(装配/监视器/信号/集群 gossip/刷盘/GC 扫描/紧缩/bftree 回收);n 个 worker 线程各 1 个 runtime(SO_REUSEPORT 各自 bind,accept+连接泵+kill 哨兵);UDS 端点独立 1 线程;副本重放独立线程(replica-replay)。n 默认 available_parallelism(逻辑核,含 SMT 兄弟),可用 --threads 覆盖(wedb/wnode/src/server.rs start:442-465)。连接 future !Send,一经 accept 永驻其 runtime(wedb/wnode/src/aof/waof_sublog.rs:131-135 注释自证)。

1. 后台周期任务惰性钉死「首会话 worker」单核,核间零分摊(头号)
机制 get_session 是全部周期任务的惰性拉起点(wedb/wnode/src/service.rs get_session:1623-1679):pubsub 消费循环(:1656 spawn_pubsub_consume_task)、AOF 周期提交(primary_tasks.rs:250 spawn_aof_commit_task)、全库对象收集+分层降阶评估轮(:296 spawn_object_collect_task)、索引自动扩容(service.rs:612 spawn_index_auto_grow_task)、AOF 体积限额、向量清理两常驻协程(:1647)。compio spawn 线程亲和,首个接连接的 worker runtime 被永久选定,各 swap(true) 幂等位保证后续会话不再重拉——落点由「谁抢到第一连接」随机定格,之后无迁移、无均衡。
证据 wedb/wnode/src/service.rs:1623-1679;wedb/wnode/src/primary_tasks.rs:250,296;对照面:向量量化协程是仓内唯一做了分摊的(:1632-1639 注释自述「逐 worker 分摊直至配额用尽」),证明分摊面已被意识到但只落了一处。
影响 随机选中的一个核在连接 IO 之外永久叠加:全库 H/Z 收集轮(object_collect_all 全库扫描)、tiered_demote_round、pubsub 全量扇出、AOF 周期 fsync、索引扩容判定。该核连接尾延迟系统性高于其余核;与其余后台任务叠加大命令时饿死窗口相乘。核间后台负载分摊完全缺失。
C# 对位 libs/server/StoreWrapper.cs TryStartCommitTask/TryStartObjectCollectTask/IndexAutoGrowTaskAsync 经 TaskManager 注册全部进 .NET 线程池,SubscribeBroker.cs:176 Task.Run 单消费亦池化——OS 全局负载均衡,无核耦合。rust 把「池化任意放置」降为「首触点永久钉定」,属结构性差异而非等价转写。
建议方向 后台常驻任务改独立线程+独立 runtime(仓内已有同款:replica-replay 线程、UDS 线程),或按 worker 取模分摊(量化协程同款)。

2. SO_REUSEPORT 分流平台语义未收敛,darwin 上硬倾斜;Linux 上静态映射无重平衡
机制 每 worker 独立 bind_reuseport(wedb/wnode/src/server.rs:497,569;wedb/wnode/src/net/socket_opt.rs:57-75),cfg 覆盖全部 unix(darwin 在列)。Linux 3.9+ 内核按四元组哈希分流;BSD/darwin 的 SO_REUSEPORT 无负载均衡语义,TCP 新连接交给最后完成 bind 的监听套接字——本仓 worker 0 先绑、1..n 顺序绑,最后绑定者是 worker n-1,darwin 上全部连接独落一核,thread-per-core 名存实亡(开发/测试机 darwin 即中招)。
证据 wedb/wnode/src/net/socket_opt.rs:64-71(set_reuseport 无 cfg(darwin) 排除);wedb/wnode/src/server.rs:546-600(顺序绑定)。
影响 darwin:全量连接单核承载,其余核空转;Linux:哈希近似均匀,但连接一经 accept 永驻其 runtime(!Send 不可迁移),连接数与核数同量级时哈希方差直接成核间负载差,某核因大命令/后台任务(条目 1)变慢时其 accept backlog(1024)积压、该核连接整体劣化,其他核无法分洪;长连接把倾斜固化到连接关闭,无任何重平衡面(无迁移、无窃取、无动态 accept 权重)。
C# 对位 libs/server/Servers/GarnetServerTcp.cs:146-173 单 acceptor 循环,每会话独立线程池任务,OS 随时把就绪会话调度到空闲核——负载面天然动态均衡。rust 把「会话→核」从 OS 动态决策降为 accept 时静态哈希,倾斜无自愈。
建议方向 darwin 下禁用 reuseport 多 worker(退单 worker 或 accept 转派);或 worker 0 单点 accept 后按最短队列转派 fd(compio 支持跨线程 fd 转移需复核),或至少登记平台差异。

3. 无核钉定:一线程一 runtime 不等于一线程一核,主线程 runtime 兼职重载
机制 全仓无任何 CPU 亲和设置(grep affinity 仅测试名命中)。线程总数 = 逻辑核(worker)+1 主线程+1 UDS+1 replica-replay(+blocking 池),按逻辑核配满后仍过订 2-4 线程;SMT 兄弟同算独立核。OS 可把两个 worker 挤同一物理核、把 worker 与主线程对排,核本地性(ILP/缓存驻留)随迁移丢失。主线程 runtime 除装配外常驻:GC 扫描+紧缩(wedb/wkv/src/store/mod.rs open_shared:542 start_gc→GcManager::spawn 于装配时主 runtime;wedb/wkv/src/gc/mod.rs spawn:203)、bftree 回收(store/mod.rs:534)、监视器采样(wnode/src/server.rs:902)、集群 gossip/刷盘(wedb/src/server/cluster_manager.rs:174 start_flush_task gossip_manager 同径)与 Lua 超时 tick(attach.rs:95 装配期 spawn,1ms 最小轮询)。
证据 wedb/wnode/src/server.rs:200,444;wedb/wkv/src/store/mod.rs:522-544;wedb/wnode/src/resp/resp_server_session/attach.rs:95;wedb/wdev/src/sys.rs:101(逻辑核口径)。
影响 worker 间与 worker/主线程间调度抖动,削弱「消除核间竞争」的架构承诺(锁竞争确实消了,缓存亲和没拿到)。紧缩轮 CPU 密集段(段合并内联于 GC tick)在主 runtime 独占 CPU,gossip/刷盘/监视器定时器全部顺延(异步让渡点之间互斥),紧缩长轮时 gossip 心跳延迟可观测。
C# 对位 C# LuaTimeoutManager 专属定时线程、GossipScheduler/紧缩/监视器全走线程池,.NET 调度器天然处理放置与抢占;Garnet 从不承诺核绑定,rust 文档承诺「一核心一 Runtime」(server.rs:7)但落地只有 runtime 隔离的一半,承诺与实现不符。
建议方向 ThreadBuilder 加亲和钉定(物理核口径),或收敛承诺措辞;主线程后台任务面已算良好隔离,保持。

4. 同核命令耦合饿死窗:同步命令执行零让渡,单命令钉死同核全部连接
机制 drive_loop 消费段内 try_consume_messages_into 同步执行命令(wedb/wnode/src/net/handler/drive.rs:179-211),命令 poll 返回前不让渡;让渡点仅三处:水位让渡、阻塞/慢路径挂起 await、写出 await。compio 单线程 reactor,一条 CPU 重命令(KEYS 全库、4MB 信封大对象展开、Lua 脚本、CLIENT LIST 全枚举、HCOLLECT 命令臂全库收集)执行 T 毫秒,同核所有连接的读、写、pubsub 推送直写、KILL 哨兵、accept 全部停摆 T 毫秒;异核不受影响。
证据 wedb/wnode/src/net/handler/drive.rs:157-258(泵主循环,消费与写出交替,无命令级抢占/切片机制)。
影响 会话级隔离被改写为核级耦合:任一租户的重命令把同核无租户连接一起拖住,尾延迟按核耦合传播。与 r9-load 已立的背压 wait_slow 同步阻塞(特例)不同,本条立的是命令本体同步执行的普遍耦合面;单命令复杂度界归 r10-bigo,此处只立调度耦合。
C# 对位 C# 每会话独立线程池任务,OS 抢占式调度,单命令只劣化自身会话,其余会话由其余线程并发服务——无「同批连接陪绑」形态。此差异是 thread-per-core 的固有代价,非实现缺陷;但命令面无任何切片/预算机制(如 Lua safepoint 已有、大对象遍历无 yield 点),使窗口上界等于最大单命令时长而非被主动钳制。
建议方向 长遍历命令臂(收集/KEYS/大信封读)插协同让渡点,或登记「同核陪绑」为已知运维特性。

已核实无增量面(逐项排查,不立)
连接分流 accept 成功路径:容量门计量、socket 装配、handler 装配、连接任务 spawn 全在 accept 线程本 runtime 内闭环,无跨线程转派(server.rs run_tcp_accept_loop:1011-1130)。
跨线程往返盘点 CLIENT KILL 他核会话:kill 位+event_listener 广播,哨兵任务驻受害方 runtime 执行 CancelToken 打断(wedb/wnode/src/net/handler/kill.rs:18-34),KILL 方零跨线程 future 操作;阻塞唤醒:观察者 event+Mutex 结果槽(wedb/wcol/src/itembroker/collection_item_observer.rs),唤醒方仅置结果,future 本核驱动;pubsub 跨连接投递:单消费任务+每订阅者有界邮箱+读段双路等待本核直写(drive.rs:309-357),无跨线程直写网络——单消费串行本身与 C# SubscribeBroker.cs:176 Task.Run 单循环 1:1,落点钉定已并入条目 1;阻塞经纪主循环单核 CAS 一次启动(wedb/wcol/src/itembroker/collection_item_broker.rs start_main_loop:300-322)与 C# CollectionItemBroker.StartMainLoop 单 Task.Run 同款,串行非转写引入;handle_collection_update 空观察者队列零事件(broker.rs:324-341)热路径仅一次并发表读,与 C# 同;WAIT/AOF 提交唤醒:committer 协程+flush_event 广播(waof_sublog.rs ensure_committer:119-145),运行时上下文内 spawn 无额外线程;复制推流串行面 r9-load 已立,不复述。无新增「隐性全局串行」。
亲和纪律 数据非核本地:全共享单 store+共享 hlog 全局尾+共享索引,所有核触达一切键——与 C# Tsavorite 共享 store 架构 1:1,非转写引入;papaya 分片+进程级随机种子(wedb/wbase/src/map.rs:17-48)对位 ConcurrentDictionary;epoch 表容量 max(128,逻辑核x2) 对位 C# kTableSize(wedb/wepoch/src/epoch.rs:159-163),worker 线程 TLS 单槽快路径核本地友好;缓冲池三级缓存 L1 TLS 0 锁热路径+L2 跨线程 inbox+L3 条带(wedb/wbase/src/pool/mod.rs 模块头),显式多核设计无伪共享。会话→存储无分片绑定与 C# Garnet 同构(Garnet 本无 session→partition 绑定)。
饿死面补充 GC tick inflight 闸防后台/手动紧缩并发(wedb/wkv/src/gc/mod.rs:283-290);spawn_blocking 仅用于索引扩容与范围索引重建(wedb/wkv/src/store/resize.rs:380,wkv/src/range_index/mod.rs:293),reactor 宿主核卸载面成立(wedb/wnode/src/database/database_manager_base.rs:472 注释)。
停机排空 三阶段成立:coordinator 停 accept→各 worker block_on 尾部经全局注册表全量下杀+25ms 轮询排空(5s 护栏强收,wedb/wnode/src/servers/consumer_registry.rs dispose_active_handlers:607-624)→join;AOF 背压闸放行先于 join 的硬约束、pubsub 收口先于 join(消费任务在 worker runtime)、AOF dispose 主 runtime 直驱、向量清理先于 stop,均有对位注释背书(server.rs stop:743-783);join 无超时上界已在对位注释声明(C# WaitSlow 亦无限等),非增量。多 runtime 句柄释放:worker 句柄 join 后 GC 循环/事务随 runtime 析构收敛,主 runtime 兜底 AOF 落盘,无跨 runtime 泄漏面。

视角结论:有增量
