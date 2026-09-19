NetworkConnectionLimit 配置与 accept 计量拒绝（FD/内存耗尽护栏）

来源：next/qcode-my-r4.md 第 4 轮条 #6（该档已清账删除，本单为该条唯一载体，见
task/reject/qcode-my-r4.md 的去重让位记录）。按主仓 dev HEAD 复核：全仓 grep
`network_connection_limit` 与 `NetworkConnectionLimit` 零命中，护栏与配置面双缺。

C# 参考
- 配置：garnet/libs/server/Servers/GarnetServerOptions.cs:347 `NetworkConnectionLimit = -1`（-1 不限）；
  宿主面 garnet/libs/host/Configuration/Options.cs:399、:971 投影，garnet/libs/host/defaults.conf:304 默认 -1。
- 计量与拒绝：garnet/libs/server/Servers/GarnetServerTcp.cs:237-241
  accept 成功后 `Interlocked.Increment(ref activeHandlerCount)` 并判
  `networkConnectionLimit == -1 || currentActiveHandlerCount <= networkConnectionLimit`；
  超限臂 GarnetServerTcp.cs:302-307 先 `Decrement` 再 `e.AcceptSocket.Dispose()`，不写任何 RESP 应答；
  计数器与停机排空共用 garnet/libs/server/Servers/GarnetServerBase.cs:28 activeHandlerCount
  （排空收口 :175-195）。
- 装配：garnet/libs/host/GarnetServer.cs:294 把 opts.NetworkConnectionLimit 传入 GarnetServerTcp。

现状（rust 缺口的三处落点，行号按主仓 HEAD 复核）
- 配置面：wedb/wconf/src/node_options.rs:223 NodeArgs 无该字段，:834 `impl ServerArgs` 无访问器。
- 计量面：wedb/wnode/src/servers/consumer_registry.rs:304-306 只有累计量 total_connections_received
  （:363 register 递增）与 total_connections_disposed（:370-373 unregister 递增），
  没有在途量单点；停机排空与 INFO connected_clients、monitor_sample 同宿主于此注册表
  （文件头 :1-12 已声明该职责），不得另起第二套计数结构。
- accept 面：wedb/wnode/src/server.rs:926 run_tcp_accept_loop 成功臂 accept（:942）后直接
  configure_socket（:952）→ 建 NetworkHandler（:957）→ 起会话，全链无容量门；
  UDS 监听同一 loop 形态（:628 accept → :640 NetworkHandler::new）。
  门位放成功分支取守卫之前、建 handler 之后由 Drop 归还，超限即刻关闭新连接且不写 RESP。
- 装配面：wedb/wnode/src/server.rs:96 缺省 8 的 network_send_throttle_max 为既有同形旋钮
  （builder :173、字段透传 :118/:245/:265/:322/:341/:359 与 :458/:529/:589），新旋钮照该形态加，
  生产接线在 wedb/wedb/src/server/boot.rs:35 与 wedb_standalone/src/main.rs:59 的 ServerBootstrap 链。

落地要点
- 语义：limit = -1 与现状逐字节一致；计量在 accept 成功时（先于 handler 装配），
  连接任务结束/失败经守卫 Drop 归零，与排空/枚举共用同一在途计数。
- 配置项进 nested_text 导出与 CONFIG 面（wconf 既有单点，禁第二套格式化器）。
- 用例：limit=2 时第三条连接被立即关闭（客户端见 EOF，非错误应答）；limit=-1 行为不变；
  并发 accept 下计数不漂（含 handler 构造失败臂）。

优先级：功能缺口（平台侧资源耗尽防护为 C# 既有语义）。

细化方案（fixloop f23-net-limit，2026-09-19 复核 dev HEAD 后追加）
- 甄别结论：成立。C# 三点核实一致（Options.cs:399 键 network-connection-limit、
  IntRangeValidation(-1, int.MaxValue)、GarnetServerOptions.cs:347 默认 -1、
  GarnetServerTcp.cs:236-241 计量/302-307 超限臂、GarnetServerBase.cs:28 activeHandlerCount）。
  rust 行号漂移（run_tcp_accept_loop 现 :941、NetworkHandler::new :972、UDS accept :635/:648，
  connection-exit 会话所致），形态未变。
- 票据修正一处："配置项进 CONFIG 面"不采纳——C# ServerConfigType 枚举零命中
  NetworkConnectionLimit（纯启动期旋钮，非 CONFIG GET/SET 运行时项），按 transpile
  1:1 原则只进 CLI + nested_text（serde 自动导入导出）+ 启动期 validate。
- 改动清单：
  1. wconf/src/node_options.rs：DEFAULT_NETWORK_CONNECTION_LIMIT=-1 常量；NodeArgs 加
     network_connection_limit: i32（--network-connection-limit，serde default）；Default impl；
     validate() 加 < -1 拒启（ValueOutOfRange(-1, i32::MAX) 对标 IntRangeValidation）。
  2. wnode/src/servers/consumer_registry.rs：字段 active_handler_count: AtomicI64（单点，
     不另起第二套计数结构）+ ConnectionGuard（RAII Drop 归零）+
     try_acquire_connection(&Arc<Self>, limit: i64) -> Option<ConnectionGuard>
     （increment 后 limit==-1 || n<=limit 放行，超限回退计数返 None）。
  3. wnode/src/server.rs：ServerBootstrap 旋钮 network_connection_limit: i64（默认 -1，
     builder + with_cluster_provider 透传，照 network_send_throttle_max 形态）；GarnetServer
     字段默认 -1 + with_network_connection_limit（不改 new 签名，20+ 测试调用点零噪声，
     先例 with_tls_config）；TcpAcceptContext.conn_limit；TCP/UDS 两个 accept 成功臂在
     configure_socket/建 handler 之前取守卫，超限 drop(stream) 即刻关闭不写 RESP，
     守卫 move 进连接任务随任务结束归零；无 registry 的哑桩（trait 默认 None）不设门。
  4. 生产接线：wedb/wedb/src/server/boot.rs 与 wedb_standalone/src/main.rs 的
     ServerBootstrap 链 .network_connection_limit(i64::from(node.network_connection_limit))。
  5. 测试：registry 单元测试（limit=2 第三条 None、释放后再进、-1 恒 Some、并发归零）；
     wnode 集成测试（limit=2 时第三条连接 EOF、前两条正常 echo）；wconf 默认值/CLI/越界。
- 停机交互：不动 dispose_active_handlers 等待条件（等 entries 空，以当前代码为准）；
  守卫 Drop 与 unregister 同处连接任务尾部配对，5s 排空护栏兜底（C# 生产语义无限等，
  rust 既有护栏更稳，不回退）。
