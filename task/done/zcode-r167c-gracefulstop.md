甄别结论：通过（甄别席 zc-fix-r16-graceful，2026-09-26）定级 P2
核验记录：
案一锚成立：server.rs:717 dispose_vector_cleanup 确先于 :720 coordinator.stop，TCP/UDS 接入环 :1224/:1394 均以 coordinator.is_stopped 为退出判据，通道收敛期监听确未关闭；push 失败臂 vector_manager.rs:785 log error 后 abort delete、cleanup.rs:421/484 静默弃项，孤儿索引泄漏路径属实；C# GarnetServer.cs:565-584 三阶段序（servers[i].Close 先于 Provider.Dispose 级联 VectorManager.cs:468 Dispose）亲验。现码注释「必须先于停 coordinator」系误陈，硬约束仅为先于 join（worker 运行时于 stop→join 窗内经 dispose_active_handlers 排空臂 server.rs:1373/:1484 持续存活），协调器广播置位即 Phase 1 阻断新连接（spawn_shutdown_bridge 关套接字），方案可落。
案二锚成立：dispose_pubsub（:735）确在 join（:746-749）前；subscribe_broker.rs:468-479 dispose 即置 disposed 并 clear 三订阅表、无消费循环等待（:463 注释自证 rust 无常驻消费任务），server.rs:729「等消费循环跑完在途批次」注释与实装矛盾；C# SubscribeBroker.cs:392-401 done.WaitOne 先于清表、GarnetServer.cs:576-578 subscribeBroker.Dispose 排在 Provider.Dispose（即排空）之后，两侧锚亲验。执行席注意：方案第 2 点「有界超时完成通知通道」因 rust 无消费任务属不必要机制，按单套机制原则不予采纳，仅做时序后移与注释校正。
案三锚部分成立，订正后随票降级处置：server.rs:747-748 与 reclaim_workers :542-545 裸 join 属实，stop 头注释亦自认「join 无超时上界」；但主要挂死向量已有界——deviations §84（deviations.md:1109-1119）DRAIN_TIMEOUT_MS=5000 强收护栏 + kill_session + Runtime 析构兜底，残余面仅 worker 线程内部同步死锁之防御性加固；票面「§84 明确登记 join 确定性时延上界」系误读，§84 裁决对象为排空循环非 join。执行席按 P3 防御档实施有界超时告警收口并补登 deviations，严禁回改 §84 既有护栏。
C# 佐证锚 StoreWrapper.cs:909-922（itemBroker/luaTimeoutManager/rangeIndexManager 序列）与 GarnetServerBase.cs:168-199 DisposeActiveHandlers 亲验成立；集合项经纪先于 join 有自陈排空理由（server.rs:738-745），不随本案二前移，票面方案未越界。
非重复非灭失：todo/ing/reject/issue 四池 grep 无同轴票；task/issue/server-r201 事项二仅裁 reclaim_workers 锁域收窄、未涉 join 超时；deviations §84/§95/§101 均非本缺陷在册覆盖。缺陷三案时序面现码俱在，未灭失。格式纯文本、双侧路径齐全。

审核结论：通过，定级 P2。
确证 stop 流程提前终止向量清理通道致新入写任务丢弃、活跃连接未排空提前清空 pubsub 与 broker 破坏在途屏障、Worker join 裸调用缺乏超时保护导致进程永久挂起。执行方案清晰，供 task/fix.md 直接消费。

平滑关停与在途请求排空闭环审查提案

案一：停机流程在关闭网络监听与阻断新连接之前提前收敛清理通道，时序倒置致在途与新入写任务丢弃

问题分析：
1. Garnet 契约对齐：C# GarnetServer.cs 的 InternalDispose 严格遵守三阶段时序：Phase 1 率先遍历全部服务器调用 Close() 立即关闭监听端口（listenSocket.Dispose()），阻断内核 backlog 新连接进入；Phase 2 彻底排空在途活跃连接（activeHandlerCount 归零）；Phase 3 才调用 Provider.Dispose() 级联清理存储与后台协程。
2. 工程现状确证：wedb/wnode/src/server.rs:GarnetServer::stop 首行即执行 self.session_provider.dispose_vector_cleanup()，随后才调用 self.shutdown_coordinator.stop()。在停机协调器广播之前，各 Worker 线程的 TCP/UDS 监听套接字依然处于正常 accept 状态，新连接依然可以建立并提交命令。
3. 逻辑危害确证：在向量清理协程收敛期间，监听套接字完全开放，新客户端连接仍可涌入并执行 VADD/VREM 等向量写操作。由于清理通道已被关闭或处于收敛终止状态，新连接提交的向量删除/清理任务被拒绝或静默丢失，造成向量索引底层资源泄漏，且违反平滑关停先关闭监听端口阻断新连接的核心安全基线。

涉及代码：
rust 文件与函数：
wedb/wnode/src/server.rs:GarnetServer::stop
wedb/wnode/src/service.rs:WeNodeService::dispose_vector_cleanup

对应 c# 文件与函数：
garnet/libs/host/GarnetServer.cs:InternalDispose
garnet/libs/server/Resp/Vector/VectorManager.cs:Dispose

精炼执行方案：
1. 将 self.shutdown_coordinator.stop() 调整至 stop() 方法的最前置执行，确保首先通过协调器广播终止 Worker 接入循环并关闭监听套接字（阻断新连接进内核 backlog）。
2. 在新连接完全阻断后，各 Worker 运行时依然存活并排空活跃连接期间，再执行 dispose_vector_cleanup() 等待积压任务清理。
3. 测试验证点：编写测试模拟在 stop() 调用瞬间并发发起新连接与向量写入，验证新连接被即刻拒绝，不存在在途任务丢失或未清理的孤儿向量记录。


案二：停机流程在 Worker 线程活跃连接未排空前提前清空订阅表与集合项经纪，破坏 Phase 2 在途排空屏障

问题分析：
1. Garnet 契约对齐：C# GarnetServer.cs 的 InternalDispose 在 Phase 2（DisposeActiveHandlers）完全等待全部活动连接 activeHandlerCount 降为 0 并断开之后，才在 Phase 3 调用 subscribeBroker.Dispose() 以及 Provider.Dispose()（内部调用 itemBroker.Dispose()）。活跃连接排空完全先于消息代理与经纪的销毁。
2. 工程现状确证：wedb/wnode/src/server.rs:GarnetServer::stop 中，主线程在执行 handles.drain() 等待 Worker 线程 join 之前，提前调用了 self.session_provider.dispose_pubsub() 与 self.session_provider.dispose_item_broker()。
3. 逻辑危害确证：此时各 Worker 线程仍在执行 dispose_active_handlers() 的排空循环中，在途订阅连接可能正尝试排空推送缓冲或注销订阅（如 unsubscribe 或会话 dispose 中的 remove_subscription），在途阻塞连接（如 BLPOP/BRPOP）可能正准备写出响应。主线程提前清空订阅表（subscriptions.pin().clear()）并将 disposed 标志置真，导致在途 PUBLISH 消息直接丢失，在途订阅连接在注销时遇到已被清空的破坏状态，背离在途请求排空闭环要求。

涉及代码：
rust 文件与函数：
wedb/wnode/src/server.rs:GarnetServer::stop
wedb/wpubsub/src/subscribe_broker.rs:SubscribeBroker::dispose

对应 c# 文件与函数：
garnet/libs/host/GarnetServer.cs:InternalDispose
garnet/libs/server/PubSub/SubscribeBroker.cs:Dispose
garnet/libs/server/StoreWrapper.cs:Dispose

精炼执行方案：
1. 将 self.session_provider.dispose_pubsub() 的清理时序调整至 worker_threads 全部 join 之后，确保在途连接与会话完全注销断开后，再统一释放订阅中枢。
2. 针对必须依赖 worker 运行时驱动的收敛协程，统一采用有界超时的完成通知通道，严禁在活跃连接未排空前提前清除共享字典状态。
3. 测试验证点：在停机过程中并发发起订阅消息发布与注销，验证所有在途消息在连接断开前安全投递或正常响应，无清表与访问并发竞态。


案三：主线程 stop 与 reclaim_workers 裸调用 handle.join 缺乏超时防僵死防护，击穿运维关停确定性边界

问题分析：
1. Garnet 契约对齐：C# 原型在 GarnetServerBase.cs:DisposeActiveHandlers 中对活动连接计数进行监控诊断。Rust 侧 deviations 第 84 条明确登记了为服务生命周期提供确定性时延上界、保障运维关停可达性的设计原则。
2. 工程现状确证：wedb/wnode/src/server.rs 的 GarnetServer::stop 与 reclaim_workers 中，主线程使用 handles.drain(..) 遍历 JoinHandle 并调用裸 handle.join()。标准库 JoinHandle::join 为无超时、不可打断的系统调用。若某个 Worker 线程在执行过程中发生死锁、或 compio 运行时在 Runtime 析构时卡死在底层 driver 操作，Worker 线程将无法终止。
3. 逻辑危害确证：Worker 线程一旦挂死，主线程在执行 stop() 时将被永久阻塞在 handle.join() 处，导致系统停机流程无限期挂起。不仅无法继续执行后续的 AOF 尾部刷盘、文件锁释放与集群状态持久化，且外层接收到多次停机信号也无法退出，彻底击穿了平滑关停的可靠性。

涉及代码：
rust 文件与函数：
wedb/wnode/src/server.rs:GarnetServer::stop
wedb/wnode/src/server.rs:GarnetServer::reclaim_workers

对应 c# 文件与函数：
garnet/libs/server/Servers/GarnetServerBase.cs:DisposeActiveHandlers

精炼执行方案：
1. 为 Worker 线程 join 引入带有确定性超时（如与排空护栏协同的超时间隔）的超时等待机制，若超过硬阈值 Worker 仍未退出，记录错误日志并强行推进流程，防止主线程无限期死锁。
2. 确保在 Worker 异常滞留时，仍然能够安全完成 AOF 刷盘收尾与锁守护释放。
3. 测试验证点：模拟单个 Worker 线程内部故意挂起，断言主线程 stop 能够在超时限定时间内脱离阻塞并记录告警日志，进程退出不僵死。

视角结论:有增量
合入哈希：159db21 收口形态：stop() 三阶段时序对标 C# InternalDispose 闭环——Phase 1 coordinator.stop 前置阻断新连接后向量清理通道方收敛、pubsub 收口后移至 worker join 之后（甄别裁定仅时序与注释校正、不建通知通道）、join_worker_bounded 15s 防御档有界超时告警强推收场并补登 deviations §152，wnode/tests/graceful_stop_phases.rs 三时序锁全绿。

加固哈希：2e74683（案一时序锁改挂清理收口入口零积压窗口，SET/PING 真实流量，与 worker 运行时析构竞态解耦；主仓复验三时序锁全绿。原合入 159db21 不变）
