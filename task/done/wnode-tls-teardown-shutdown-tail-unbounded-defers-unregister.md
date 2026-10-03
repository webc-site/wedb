终态：已合入 dev（merge fx0N 系，2026-09-27）。9441c6c 收场尾 shutdown 挂 killable+timeout 双边界(对位握手臂同构);TLS_SHUTDOWN_TIMEOUT=1s builder 注入;黑洋试验证条目注销

甄别结论：通过 | 定级 P1 | 2026-09-27 zcode fixloop 甄别席（双侧锚现码亲验，零撞票）
执行提示：复用握手双边界同款 timeout 嵌套最小收口；drive.rs:77 收场尾裸 shutdown

审核结论：通过（逐锚亲验：drive.rs:77 收场尾裸 shutdown 无 killable/超时——在途五臂 :137/401/424/577/609 皆有挂唯尾无；futures-rustls-0.26.0 send_close_notify 队列未写尽即 Pending 黑洞悬挂实证；C# DisposeImpl 注销先于 ssl Dispose 序亲验、rust 注释自陈反转；三重泄漏链逐环闭合——unregister 全仓仅 dispose 一处旁路无、active_consumers 不过滤 kill 位、ConnectionGuard 随任务悬挂、DRAIN 5s 失配；方案复用 server.rs:1362-1372 握手双边界同单机制非新；kill 先行时令牌已触发不吃 1s 界，停机无扰。微瑕注记：C# sslStream.Dispose .NET Core 实不发 close_notify，票「补发」措辞略松，执行以现码为准）

连接收场尾 TLS close_notify 尾帧无界无可杀闸：黑洞对端令 dispose 永不执行，KILL 后注册表幽灵条目、幽灵订阅与容量守卫三泄漏（C# 收口序为 syscall 先行、注销无条件跟进，结构上不可残留）

问题分析：
1 Garnet 契约对齐：C# 连接收场唯一路径 TcpNetworkHandlerBase.cs:Dispose（:148-170）序为 socket.Shutdown(Both)+socket.Close（内核 syscall，无论对端是否排空即刻返回，尾帧交内核接管）→ DisposeImpl（NetworkHandler.cs:654-680：cancellationTokenSource.Cancel → serverHook.DisposeMessageConsumer 即 RespServerSession.Dispose 承接 ActiveConsumers.TryRemove 注销与 subscribeBroker 订阅摘除 → networkSender.Dispose → sslStream?.Dispose——socket 已关，close_notify 写失败即刻吞掉）。注册表注销与会话资源释放严格先于 TLS 尾帧，且收场链上不存在任何可被对端窗口卡住的异步等待点，「Kill 后残留注册表」在 C# 侧结构性不可达。
2 工程现状确证：rust 收场单点为 wnode/src/net/handler/drive.rs:process_stream 收场尾巴（:77 let _ = stream.shutdown().await; 后 :78 self.dispose()），注释自定「顺序不可颠倒——先 FIN / close_notify 后 dispose」。TLS 臂 shutdown 落 wnode/src/net/stream.rs:ConnectionStream::shutdown（:134）→ wtls/src/stream.rs:tls_shutdown（:149-157）→ BiLock 句柄 poll_close → futures-rustls-0.26 server.rs:111：send_close_notify 入队后 Stream::poll_close 须把连接发送队列全部写尽 socket 才返回 Ready，socket 发送缓冲满即 Poll::Pending 无限期挂起。该 await 未挂 killable 令牌——drive_loop 内其余一切在途收发（握手读、主读、命令臂 write_all、推送臂 write_all_shared、两臂 wait_for_commit_async，见 drive.rs:667 killable 注释「读写统一挂 KILL/注销终止令牌」）统一挂 kill_token，唯独收场尾裸奔；亦无任何超时界。明文 TCP/Unix 臂 shutdown = compio AsyncWrite::shutdown = shutdown(fd, SHUT_WR) 即刻就绪，仅 TLS 臂中招。
3 逻辑危害确证：TLS 连接存在未发积压（慢订阅者、大应答在途、AOF 提交等待期攒批）时被 CLIENT KILL（老式 addr 形、新式 ID/TYPE/USER/ADDR/LADDR/MAXAGE 过滤器形，及停机排空 dispose_active_handlers 全量下杀，consumer_registry.rs:680）命中：令牌打断在途 write_all、泵 break 'drive 退出，收场尾 close_notify 尾帧却因发送缓冲满、对端不排空而对「持续回探测包的零窗口对端」永久悬挂（probe_race 断连判定 Disposed 后的收场同路径）——dispose 永不执行，三组泄漏同时成立：
  a. ConsumerEntry 永驻 entries（active_consumers 不过滤 kill_flag/removed，consumer_registry.rs:551），CLIENT LIST/KILL 幽灵行永久可见；kill_flag 首杀已耗尽，二次 CLIENT KILL 回 :0 无法补救，幽灵条目仅能靠进程重启清除；received-disposed=活跃条目不变量下监视器每轮采样、INFO clients 活跃数永久虚高。
  b. RespServerSession::dispose（wnode/src/resp/resp_server_session/core.rs:749）全套资源闭环永不执行：broker.remove_subscription 缺席即幽灵订阅（本函数注释自认危害——PUBLISH 计数虚高、向已断连邮箱投递），会话指标归并 merge_metrics_history_session_dispose 缺席。
  c. serve_connection（wnode/src/server.rs:1260-1272）局部 _in_flight ConnectionGuard 永不 Drop，active_handler_count 永久 +1：conn_limit 形态容量槽逐连接蚕食直至拒接新连；停机排空收敛判据（entries 空且 count 归零）失配，只能吃 5 秒护栏强收（consumer_registry.rs:683 DRAIN_TIMEOUT_MS）。连接任务与 fd 泄漏至进程退出。
  C# 同场景注销先于尾帧且尾帧为 syscall 不可悬挂，无此缺陷；本缺陷非既定改良面（deviations 无 shutdown 收场序相关登记，已核）。

涉及代码：
rust 文件与函数：
wedb/wnode/src/net/handler/drive.rs:process_stream（:77 收场尾 shutdown 无界无取消）
wedb/wnode/src/net/stream.rs:ConnectionStream::shutdown（:134 TLS 臂派发）
wedb/wtls/src/stream.rs:tls_shutdown（:149-157 poll_close 写尽语义）
wedb/wnode/src/servers/consumer_registry.rs:ConsumerRegistry::dispose_active_handlers（:673-690 判据与 5s 护栏，受害消费面）、active_consumers（:551 幽灵枚举面）
wedb/wnode/src/resp/resp_server_session/core.rs:RespServerSession::dispose（:749 订阅摘除/指标归并，被卡方）
wedb/wnode/src/server.rs:serve_connection（:1260 ConnectionGuard 泄漏点）

对应 c# 文件与函数：
garnet/libs/common/Networking/TcpNetworkHandlerBase.cs:Dispose（:148-170 Shutdown/Close syscall 先于一切托管释放）
garnet/libs/common/Networking/NetworkHandler.cs:DisposeImpl（:654-680 DisposeMessageConsumer 注销先于 sslStream.Dispose）
garnet/libs/server/Servers/GarnetServerBase.cs:DisposeActiveHandlers（排空判据对位）

精炼执行方案：
1 收场尾加有界护栏（保留既定 shutdown→dispose 顺序，只消无界性）：drive.rs:process_stream 收场段改 killable(time::timeout(TLS_SHUTDOWN_TIMEOUT, stream.shutdown())) 三层同款嵌套（timeout 内层、取消外层，口径复制 server.rs:1368 TLS 握手臂「双边界」既有裁决）；新增常量 TLS_SHUTDOWN_TIMEOUT=1s 与 TLS_HANDSHAKE_TIMEOUT 同域留痕；超时或取消即弃尾帧照常落 dispose 收口，ConnectionStream 随连接任务返回 Drop，底层 fd 关闭发 FIN/RST（对位 C# socket.Close 兜底臂）；明文臂即刻就绪，包裹零成本无行为变化
2 顺序注释同步修订：「先 FIN 后 dispose」补「尾帧有界、弃帧不弃注销」不变式句，防后续席次再裸化
3 单测：集成形态注入 TLS_SHUTDOWN_TIMEOUT 短值（仿 wnode_tls_test 既有 with_tls_handshake_timeout 注入先例）；假客户端 setsockopt 收缓冲压小 + 只连不读致服务器发送缓冲满，管道命令制造积压后 CLIENT KILL ID，断言注入超时 + 裕量内条目自 entries 注销、CLIENT LIST 无幽灵行、total_connections_disposed 跟进（现码红：条目永驻）；补明文回归断言收场序不变
4 回归：wnode/tests/client_commands_tests.rs、session_panic_isolation_tests.rs、wnode_tls_test 套件全量
