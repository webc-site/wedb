连接退出无关闭序：drive_loop 之后只 dispose 会话，TCP 退化为 close 可丢末批应答、TLS 自身永不发 close_notify

来源：next/glm.net.md 条 2（该文件已分拣清空删除）。逐句按主仓当下代码复核后判定成立待做。
取证基线：主仓 /Users/z/git/db/wedb，行号按符号定位。
载体唯一性：并发拆条在 next/ 下另留了一份本条原文照抄的壳（basename
connection-exit-shutdown-close-notify.md，只加「优先级：高」头、无取证订正），
以本文件为唯一载体，派单前先剪壳勿双花。

结论

C# 每条连接的退出都走显式关闭序（先 `socket.Shutdown(Both)` 再 `Close`，TLS 会话在 DisposeImpl 里
再 `sslStream?.Dispose()` 补发 close_notify）。rust 服务端的连接泵在 drive_loop 返回后只调 `self.dispose()`
（throttle 关闭 + 消费者注销 + 会话释放），完全不碰 socket，`ConnectionStream` 随作用域 drop 直接 close。
两个后果：一是取消类断连（CLIENT KILL 打断在途读、协议违规后客户端仍在发送）下 Linux 对仍有未读字节的
套接字 close 会发 RST 而非 FIN，RST 清空对端接收缓冲，刚 `write_all` 成功的 -ERR 应答可能在客户端读到
之前被丢掉；二是 TLS 下服务端自身从不发关闭通知，而接收侧又依赖对端的 close_notify 判 EOF，收发不对称，
对端 TLS 栈只能以截断告警收场。

现状

- /Users/z/git/db/wedb/wedb/wnode/src/net/handler/drive.rs:32-43 `process_stream` 拿到
  `mut stream: ConnectionStream`（所有权在栈上），drive_loop 返回后 :41 直接 `self.dispose()`，
  随后 stream 随作用域 drop。
- 退出位：drive.rs:357-372 —— `ReadEnd::Cancelled => break`（:358，KILL/注销取消在途读）、
  `Ok(0) => break`（:368，对端正常关闭）、`Err(UnexpectedEof) => break`（:370）、
  `Err(e) => return Err(e)`（:371，写/读错误上抛）。四条出口都不做关闭序。
- /Users/z/git/db/wedb/wedb/wnode/src/net/handler/mod.rs:91-101 `dispose` 只做 throttle.close、
  消费者注销、session.dispose，无 socket 面；:104-108 `Drop` 亦转调同一函数。
- /Users/z/git/db/wedb/wedb/wnode/src/net/stream.rs:88-159 `ConnectionStream` 的公开面只有
  read/write_all/flush/local_endpoint/read_shared/write_all_shared，无 shutdown 方法。
- 接收侧不对称自述：stream.rs:202-206 注明「对端未经 close_notify 直接断开时 rustls 以 UnexpectedEof
  表达 EOF（compio-tls 同口径归零，泵循环据此断连）」，即本仓承认收侧依赖 close_notify，
  而发侧零调用。
- 全目录证据：/Users/z/git/db/wedb/wedb/wnode/src/net 下 grep `shutdown` 零命中；
  全仓 `\.shutdown\(|poll_shutdown|AsyncWriteExt::close` 只有客户端面命中
  （/Users/z/git/db/wedb/wedb/wconn/src/network/pump.rs:127、:139、:151 与
  /Users/z/git/db/wedb/wedb/wconn/src/network/mod.rs:229），即同一套 compio 能力在客户端已用、
  服务端未用。

C# 参考

- /Users/z/git/db/wedb/garnet/libs/common/Networking/TcpNetworkHandlerBase.cs:148-170 `Dispose()`：
  `socket.Connected` 则 `socket.Shutdown(SocketShutdown.Both)`（:155），finally 里 `socket.Close()` +
  `socket.Dispose()`，末了 `DisposeImpl()`。
- 同文件 :180-200 `Dispose(SocketAsyncEventArgs e)`：EOF/错误路径同序
  （`e.AcceptSocket.Shutdown(Both)` :186 → Close → Dispose → DisposeImpl）。
- /Users/z/git/db/wedb/garnet/libs/common/Networking/NetworkHandler.cs:654-678 `DisposeImpl`，
  其中 :667 `sslStream?.Dispose()` 即 TLS 关闭通知的发出点。

修法

1. `ConnectionStream` 增一个 `shutdown(&mut self) -> io::Result<()>`：用文件内既有的
   `stream_io!` 派发宏（/Users/z/git/db/wedb/wedb/wnode/src/net/stream.rs:70-86）三臂同形落地，
   明文臂转 compio 的 `AsyncWrite::shutdown`
   （已核实存在：`~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/compio-net-0.12.5/src/tcp.rs:624`
   与 :646，unix.rs:478 与 :500），TLS 臂经写侧句柄
   （`TlsHandles.write`，与 :120-122 `flush` 同一句柄）调
   `compio-tls-0.10.0/src/stream.rs:274` 的 `shutdown`，其实现即 `futures_util::AsyncWriteExt::close`，
   对 rustls 就是 send_close_notify + flush。函数级映射注释挂
   `libs/common/Networking/TcpNetworkHandlerBase.cs:Dispose`。
2. 调用点单点插入：drive.rs:38-42 在 drive_loop 返回、`self.dispose()` 之前，
   按 `let _ = stream.shutdown().await;` 的容错形态执行（对端已断时错误可忽略，与客户端泵
   /Users/z/git/db/wedb/wedb/wconn/src/network/pump.rs:139 的 `let _ = stream.shutdown().await` 同口径），
   错误不覆盖 drive_loop 的原返回值。只关写侧即够：C# 的 `Shutdown(Both)` 中接收半边在 rust
   由读取段已结束天然满足，无需等对端 FIN，否则会在协议违规场景反向挂住。
3. 顺序约束写进注释：先 shutdown 后 dispose，因为 dispose 会释放会话与订阅推送通道，
   推送侧再写已关闭的流没有意义；本仓 drive.rs:363-366 已有「缓冲归还先于一切退出路径」的
   顺序说明，新增句紧随其语义。
4. 不做的事：不给 `Drop for NetworkHandler`（mod.rs:104-108）加异步关闭 —— Drop 不能 await，
   异步关闭只属于 process_stream 这一条正常收场路径；其余入口（如握手期直接 return Err）
   已经全部收敛到 process_stream 的这条尾巴上，无需第二处。
5. 集成测试放 /Users/z/git/db/wedb/wedb/wnode/tests：一条明文用例断言协议违规应答能被客户端读到
   （现有违规断连用例可直接补 FIN 判据），一条 TLS 用例断言客户端读到 close_notify
   （以客户端侧不报截断告警为判据），别写只能在自己进程内自证的假用例。

优先级

功能缺口（对外应答可达性与 TLS 关闭语义），不涉及架构重复，排在死代码/去重/污染扩散之后、
打磨之前。风险面集中在取消类断连，EOF 正常路径接收队列已空、本身无碍，因此不阻塞其他票，
但也不要以「只是优雅性」为由推到最后一批。

交叉引用

- 同文件的连接拆除族：task/ing/subscribe-broker-shutdown-dispose.md（pubsub broker 的 shutdown/dispose
  形态），两票都动 drive.rs 收场段与 handler/mod.rs:91-101，同批开工共用一次改动，判定各自独立。
- 服务端 TLS 的其余缺口（客户端证书认证、出站 TLS）不在本单：
  task/ing/inbound-tls-client-cert-auth.md、task/ing/cluster-outbound-tls-client.md。
- 会话巨文件族的拆分不动本单语义：task/ing/wnode-service-split.md（纯移动拆分里禁夹带删改）。
- 停机链上的范围索引收口步是另一层（进程停机 vs 单连接退出）：
  task/ing/range-index-replication-shutdown-dispose.md，与本单无共用判据，勿混改。

落地记录（实现代理，2026-09-19，分支 conn-exit-graceful-shutdown，基点 dev 7b20cef，
代码提交 aa3dde8 + f169675，合入 dev cfcd817b）

甄别核实（逐条独立复验，票面全部成立，无拒项）

- wedb/wnode/src/net/handler/drive.rs:32-43（改前）`process_stream` 收场只有
  `self.dispose()`；四条退出位 :358 `ReadEnd::Cancelled => break`、:368 `Ok(0) => break`、
  :370 `Err(UnexpectedEof) => break`、:371 `Err(e) => return Err(e)`，行号与票面逐一对上。
- wedb/wnode/src/net/handler/mod.rs:91-101 `dispose` = throttle.close + 消费者注销 +
  session.dispose，:104-108 `Drop` 转调同函数，皆无 socket 面。
- wedb/wnode/src/net/stream.rs:88-159（改前）公开面 read/write_all/flush/local_endpoint/
  read_shared/write_all_shared 六项，无 shutdown；:202-206 自述收侧依赖 close_notify。
- grep 复核：`shutdown` 在 wedb/wnode/src/net 全目录零命中；全仓 `\.shutdown\(` 只命中
  客户端面 wedb/wconn/src/network/pump.rs:127、:139、:151 与 wedb/wconn/src/network/mod.rs:229。
- C# 对位复核：garnet/libs/common/Networking/TcpNetworkHandlerBase.cs:148 `Dispose()` →
  :155 `socket.Shutdown(SocketShutdown.Both)` → finally `Close()`+`Dispose()` → `DisposeImpl()`；
  同文件 :180 `Dispose(SocketAsyncEventArgs)` → :186 同序；NetworkHandler.cs:654 `DisposeImpl`
  → :667 `sslStream?.Dispose()`。
- 原语可行性复核（比票面多一层落点确认）：compio-net-0.12.5/src/tcp.rs:624/:646、
  unix.rs:478/:500 皆有 `AsyncWrite::shutdown`，其末端 `Socket::shutdown`
  （compio-net-0.12.5/src/socket/mod.rs:213）提交的是 `ShutdownSocket::new(fd,
  std::net::Shutdown::Write)` —— 即明文臂本就是「只关写侧发 FIN」，与票面第 2 条口径
  天然一致，不需要额外拆半边；`NotConnected` 已被该函数吞成 Ok。
  TLS 臂 compio-tls-0.10.0/src/stream.rs:274 `shutdown` = `futures_util::AsyncWriteExt::close`
  → futures-rustls-0.26.0/src/server.rs:113 `poll_close` 内 `session.send_close_notify()`
  + 写队列 flush + 底层写半关闭，正对 C# 的 `sslStream?.Dispose()` + `socket.Shutdown`。

票面之外需补的一条实况（影响用例判据写法）

- 客户端若用 compio 的 `AsyncRead::read`，compio-tls-0.10.0/src/stream.rs:153-155 的
  `read_futures` 已把 `UnexpectedEof` 归零，截断与干净关闭在该口径下不可分辨；
  故 TLS 用例的末次读走 futures 侧接口（`futures_util::io::AsyncReadExt::read`），
  判据才是 rustls 原始口径：收到 close_notify → `Ok(0)`；只有 FIN → `Err(UnexpectedEof)`
  （rustls-0.23.45/src/conn.rs:183-190 `check_no_bytes_state`，`has_seen_eof` 在 :776 置位）。
  本用例缺关闭序时必红，非进程内自证。

改动（5 文件，+197/-8，与票面修法一一对应，无额外层）

1. wedb/wnode/src/net/stream.rs:124-137 新增 `ConnectionStream::shutdown(&mut self)`，
   经既有 `stream_io!` 三臂派发（明文臂 `s.shutdown()`，TLS 臂 `tls_shutdown(&h.write)`，
   与 :120-122 `flush` 同一写侧句柄），函数级映射注释挂
   `libs/common/Networking/TcpNetworkHandlerBase.cs:Dispose`（该锚点全仓此前零引用，
   不与 handler/mod.rs 的 `NetworkHandler.cs:Dispose` 撞重复定义）；
   新增内核 wedb/wnode/src/net/stream.rs:315-331 `tls_shutdown`（poll_fn 取锁 +
   `poll_close`，锁粒度与本文件其余内核同形），宏注释「三个内核」随改为四个
   （:64）、入口方法计数五改六（:68）。
2. wedb/wnode/src/net/handler/drive.rs:44-51 单点插入 `let _ = stream.shutdown().await;`，
   先关闭序后 dispose，错误弃用且不覆盖 `drive_loop` 返回值；:29-30 补函数级说明
   「本函数是连接退出的唯一收场点」。
3. wedb/wnode/src/net/handler/mod.rs:88-94 `dispose` 注释点明 socket 面不在本函数与
   Drop（Drop 不能 await），按票面第 4 条不加异步关闭、不加第二处调用点。
4. 用例 wedb/wnode/tests/net_pump_consume_tests.rs:165-198（`pump_violation_disconnects`
   补 FIN 判据：终止读必为 `Ok(0)`，读到 Err 即 panic 报「连接未以 FIN 收场」）；
   wedb/wnode/tests/tls_test.rs:253-385 新增 `test_garnet_server_tls_exit_sends_close_notify`
   （QUIT 桩令服务端主动收场，客户端先读尽 `+OK\r\n`，再以 futures 口径续读断言归零）。

不在本票面、记此备案

- 明文 FIN 判据是行为锁：票面描述的 RST 丢应答需「close 时接收队列仍有未读字节」方成立，
  该条件由客户端续写制造，但 FIN 与随后 close 触发的 RST 在 loopback 上几乎同时到达，
  用例结果会随调度抖动 —— 故本票只在客户端侧锁死「以 FIN 而非错误收场」，不复现丢包窗口。
- wedb/wnode/src/server.rs:977-984 TLS `acceptor.accept` 失败即 `return`，未经 process_stream
  尾巴（票面第 4 条明令不加第二处调用点；该路径无应用层应答可丢，且 rustls 未握手完成
  亦无 close_notify 语义），维持现状。
- 同批族票 task/ing/subscribe-broker-shutdown-dispose.md 已不在 task/ing（其代理自行处置），
  本票收场段仅加 3 行代码位，两票无实质冲突。

验收读数（worktree /tmp/fork/conn-exit-graceful-shutdown，CARGO_TARGET_DIR 同树 target，
按派单口径未跑 test.sh / sh/clippy.sh / 任何测试，未做全量 check）

- `cargo check -p wnode --all-targets --features tls`：改动后 exit 0、0 warning；
  并入 dev 三轮（ff247f5 / 0b26d23 / 合入后的 cfcd817b）后复跑皆 exit 0。
- `cargo check -p wnode --all-targets`（无 tls，验明文臂）：exit 0、0 warning。
- `rustfmt --edition 2024 --config-path wedb/rustfmt.toml --check` 五文件：exit 0（零改写）。
- `bun js/check.js`：改前改后 stdout 逐字节相同、两次 exit 0，
  js/check 语料与 check/miss 未被回写（worktree `git status` 干净）。
- 合入面：本票主体提交 aa3dde8（5 文件 +197/-8）；f169675 是并入 dev 7af8771 时按
  `GarnetServer::new` 新签名（改返 Result，端点解析 fail-fast 票落地）给本票新用例补
  一个 `?`，与同文件既有用例同形，别无改动。cfcd817b 为主仓 merge，落地面逐文件与
  aa3dde8 一致（`git show --stat` 只含本票 5 文件）。
- 并发实况备案：本票落地期间 dev 自身出现过一段 wnode lib 红（service.rs:1473 仍赋
  `acl_settings` 而 session_dependencies.rs 已删该字段，见 07de9817 对齐提交），
  非本票引入，合入时该红已由 07de9817 收口；本票在 cfcd817b 上复验 exit 0。
- 主仓本票文档由并发代理的检查点提交 a3f6cef5（chore: checkpoint f11-wnode-vtable）
  顺带扫入，故本代理自己的文档提交 63d7eba2 只剩 5+/5- 的行号与用字订正；正文完整，
  未双花。
