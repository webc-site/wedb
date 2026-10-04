wconn 客户端 dispose 只断通道不拆读泵，静默对端下半开连接任务与 fd 永久泄漏（C# socket.Dispose 无条件收口面转写失真）

问题分析：
1. C# 契约对齐。C# 出站节点连接的收口契约是无条件拆 fd：GarnetClient.Dispose(bool)（garnet/libs/client/GarnetClient.cs:521-532）执行 socket?.Dispose() 双向关闭，挂起的 pending receive 随即 faulted/完成，networkHandler 读环收场，与对端是否配合无关；调用侧三条回收链全部落该契约——MEET 异常臂 catch 中 if (created) gsn?.Dispose()（garnet/libs/cluster/Server/Gossip/Gossip.cs:223-228）、连接池摘除 TryRemoveConnectionAsync 对 conn.Dispose()（garnet/libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs:214 起）、GarnetServerNode.Dispose 内 gc?.Dispose()（garnet/libs/cluster/Server/Gossip/GarnetServerNode.cs:116-134）。即 C# 语义为：dispose 之后连接资源必然回收，timeout 旋钮（timeoutMilliseconds，0=关闭在途超时，GarnetServerNode.GetClientTimeoutMilliseconds:69-70）只关闭"在途命令超时判成"这一行为面，从不关闭"dispose 即拆 fd"的收场面。
2. 工程现状确证。rust 侧 dispose 链在位但回收不彻底：facade GarnetClient::dispose 仅 inner 置 None（wedb/wedb/src/client.rs:589-591），最后持有者落下时 wconn GarnetClient 无 Drop 实现（wedb/wconn/src/client.rs:20-50），析构只丢 tx 通道；写泵见通道断连走计划收场（wedb/wconn/src/network/pump.rs:189-193），stream.shutdown() 为写半单向（wedb/wconn/src/network/stream.rs:177-185，compio-net socket/mod.rs:214 实测 Shutdown::Write），读半句柄仍被读泵持有；读泵三路竞速（pump.rs:301-330）中事件监听器仅在 progress 在位时挂载（pump.rs:304），而 PumpProgress 只在 timeout_millis>0 时创建（wedb/wconn/src/client.rs:148-157），stalled 支 cmd_fut 要求游标后有残字节（pump.rs:302），握手期静默对端下 read_buf 为空。于是 timeout 旋钮关闭形态（NodeConnection timeout_ms map_or(0)，wedb/wedb/src/server/gossip/node_connection.rs:61-63；cluster_node_timeout() 以 None 承载 0，wedb/wedb/src/server/cluster_provider/flags.rs:92-97；CONFIG SET cluster-node-timeout 值域下界 0 放行，wedb/wconf/src/runtime_server_config.rs:138-148）下，对端 accept 后不发不断（黑名单黑洞/错配非 RESP 服务）时：建连放弃臂（facade 5 秒缺省限时 wedb/wedb/src/client.rs:155-179、initialize_async gossip_delay 上界 wedb/wedb/src/server/gossip/node_connection.rs:138-150、MEET 无限 wait 臂 wedb/wedb/src/server/gossip/gossip_manager.rs:169-197）虽都如 C# 触发了 local.dispose/try_remove 回收链（connection_store.rs:36-46 remove_locked 即 dispose），被 dispose 连接的读泵却因常驻读 future 永不落定而无任何唤醒源，task、读半 fd、池借出缓冲（pump.rs:251 get_ref 连接级单次借出、退出 RAII 归池的契约被挂死击穿）与整个 LimitedFixedBufferPool 一起永久驻留。现码可达触发链：CONFIG SET cluster-node-timeout 0 后，gossip 主循环每轮（缺省 5 秒）对同一不可达静默节点 get_or_add 新建连接、initialize 超时放弃、失败即摘（gossip_manager.rs:286-296 建连失败当轮摘除），每轮净漏一套 fd+task+缓冲，进程 fd 线性侵蚀直至 EMFILE；旋钮开启态（缺省 60 秒）因 progress 在位、timeout_checker 判成经事件唤醒读泵（wedb/wconn/src/client.rs:248-271、pump.rs:323 双检臂）可自愈，反证泄漏窗口恰为"dispose 面依赖超时旋钮"这一 C# 不存在的耦合。
3. 逻辑危害确证。其一，转写语义失真：C# 的 0=关闭超时仅指在途判成，rust 实现将其放大为"读泵失去唯一中止信号"，同旋钮同场景下 C# 每轮摘除即回收 fd、rust 每轮摘除即永久漏一份内核句柄与池块，对拍资源维度必然发散且 rust 侧单调恶化。其二，故障放大面真实：黑洞对端（防火墙后残留 accept 半开、误指到非 RESP 监听）是集群运维常态输入，运维显式配 0（文档口径"无限超时"为合法档位）后节点将在数小时至数天内因 fd 耗尽拒绝全部新连接，gossip 池缓冲的持续占用还会挤压复制/迁移链共用池的配额。其三，network/mod.rs:24-28 退出闭环注释自陈"调用方全部退场 → 写泵 shutdown 写半 → 读泵 EOF 收场"，其对端配合前提在畸形输入面（半开/静默）不成立，即本仓自设契约在该场景被现码证伪，非假想。

涉及代码：
- wedb/wconn/src/client.rs（connect_async :127-182，progress 门控 :148-157，握手失败仅 tx=None :176-180，timeout_checker :248-271，无 Drop 实现）
- wedb/wconn/src/network/pump.rs（write_pump 计划收场 :189-193，read_pump 三路竞速 :257-362，listener 仅 progress 在位 :301-330，池借出 :251）
- wedb/wconn/src/network/stream.rs（WriteHalf::shutdown 写半单向 :177-185）
- wedb/wedb/src/client.rs（facade connect 限时放弃 :155-179，dispose :589-591）
- wedb/wedb/src/server/gossip/node_connection.rs（timeout_ms map_or(0) :61-63，initialize_async 放弃臂 :138-150）
- wedb/wedb/src/server/gossip/gossip_manager.rs（MEET 回收链 :169-197/:248-254，建连失败当轮摘除 :286-296）
- wedb/wedb/src/server/gossip/connection_store.rs（remove_locked 即 dispose :36-46）
- wedb/wedb/src/server/cluster_provider/flags.rs（0 → None 无限哨兵 :92-97）
- wedb/wconf/src/runtime_server_config.rs（cluster-node-timeout 值域下界 0 :138-148）
- garnet/libs/client/GarnetClient.cs（Dispose(bool) socket?.Dispose() 无条件拆 fd :521-532，GetClientTimeoutMilliseconds :69-70）
- garnet/libs/cluster/Server/Gossip/Gossip.cs（MEET catch 即 gsn.Dispose :223-228）
- garnet/libs/cluster/Server/Gossip/GarnetServerNode.cs（Dispose → gc?.Dispose :116-134）
- garnet/libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs（TryRemoveConnectionAsync 摘除即 Dispose :214 起）

精炼执行方案：
单机制：把"连接已废弃"从超时旋钮解耦为独立泵中止事件，补齐 C# Dispose 无条件收口面。PumpProgress 改为建连恒建（不再由 timeout_millis>0 门控创建，timeout_checker 任务的 spawn 仍按旋钮门控，在途判成语义不变），新增 aborted 粘滞标志并复用 timeout_event 通知链；为 wconn GarnetClient 实现 Drop（或暴露显式 abort()，由 facade dispose 与通道析构共同触达）：丢 tx 的同时 flag_aborted 并 notify；读泵三路竞速在 progress 恒在位后无条件挂 listener（pump.rs:304 的 map 臂去条件），见 aborted 且未 timed_out 即按计划收场 Ok(())，roundtrip 分类保持 timed_out 判 Error::Timeout、aborted 归 ReadPumpExited 不误伤超时口径。此机制同时收编三条现存泄漏入口（facade 5 秒建连限时放弃、initialize_async gossip_delay 放弃、MEET/摘池臂 local.dispose），全部在废弃时刻确定性归还 fd、task 与池缓冲，达成与 C# socket.Dispose 同强的收口契约。
测试验证点：1) wconn 级新用例：std TcpListener accept 后置索静默（不 send 不 shutdown），GarnetClient::new(timeout_millis=0) 的 connect_async 经外层 timeout 放弃（或直接 drop）后，以 network_loop 退出回调/池占用计数归零断言在有限时间内收场，现状必红；2) 分类锁测：timeout_millis>0 在途无进展仍结算 Error::Timeout，aborted 通道结算 ReadPumpExited；3) 集群回归：CONFIG SET cluster-node-timeout 0 后对静默端口反复 CLUSTER MEET（或等 gossip 数轮），断言进程 fd 数不随轮次线性增长（/proc 观测或池借出水位探针）；4) 既有 handshake_timeout、network_buffer_pool 用例保持绿。

## 销号注记（2026-09-28 主控）
立案已由 task/done/wconn-client-dispose-read-pump-hang.md 收口
（合并 779343c1：DisposeHandle dup fd 自持副本 + shutdown(2) 双向拆连、disposed 幂等位
承 is_connected/reconnect/收场分类、Drop 兜底、门面 dispose 锁外转调；
tests/dispose_reclaim.rs 五例含池借出归零直测，wconn 40/40 绿）。
