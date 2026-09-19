优先级：重复（一处定义被违反的注释登记面；js/check.js「重复定义」段现刻实测 3 组属本域，属合并前必清项）

一句话：三个 C# 函数的文档锚点各被两处 rust 函数挂走，check.js 的重复定义段现报 3 条本域项；按「谁是 C# 函数本体的承接点谁持锚」收口，子步骤改散文说明。

复核追加（2026-09-19 HEAD be2fca2，必读）
- 本票三条复挂经复刻扫描（bun /tmp/dupscan.mjs，只 import js/check/rustScan.js、不回写 ignore 语料）确认仍在，且同域重复组已由立票时的 3 组增至 7 组：另 4 组为 RespClusterIterativeSlotVerify.cs:NetworkIterativeSlotVerify（/Users/z/git/db/wedb/wedb/wedb/src/server/cluster_manager_slot_gate.rs:351 evaluate_iterative_key_gate 与 /Users/z/git/db/wedb/wedb/wedb/wedb/src/server/cluster_session/slot_verify.rs:52 network_iterative_slot_verify）、GarnetClientSession.cs:GarnetClientSession（/Users/z/git/db/wedb/wedb/wconn/src/session.rs:40 与 :74、/Users/z/git/db/wedb/wedb/wconn/src/client.rs:114、/Users/z/git/db/wedb/wedb/wconn/src/network/pump.rs:50 四处）、GarnetClient.cs:ConnectAsync（client.rs:133 与 /Users/z/git/db/wedb/wedb/wconn/src/tls.rs:98）、GarnetClient.cs:GarnetClient（client.rs:65 与 client.rs:104）。源档条 10/11/12/13 立票时判「已单点」是并发会话瞬时撤锚造成的假象，按现刻 HEAD 作废，统一并入本票口径收口
- 碰撞告警：并发会话另立 /Users/z/git/db/wedb/next/net-anchor-dup-collapse.md（组 1-6 覆盖上述 4 组 + 本票条一、条三）与 /Users/z/git/db/wedb/next/design-anchor-remount-batch.md 组 1（本票条二）。同一批文件两份票，主代理必须二选一收编，勿双跑
- 与本票方向不一致处（据码裁定，勿照抄 next 票）：next/net-anchor-dup-collapse.md 组 6 选择「保留 drive.rs:35 process_stream 的 HandleNewConnection 锚、只去 consumer_registry.rs:357」，与本票相反。C# HandleNewConnection 函数体（garnet/libs/server/Servers/GarnetServerTcp.cs:226 起：:234 复位退避 → :236-241 计数与 networkConnectionLimit 判定 → socket NoDelay → 建 handler → :290 handler.Start）的 rust 承接点是 accept 循环本体 run_tcp_accept_loop（/Users/z/git/db/wedb/wedb/wnode/src/server.rs:975），其文档 :971-974 现只有裸行号散文、无锚点——按 SKILL「承接 C# 函数本体的 rust 函数须带路径:函数名」这是缺口；process_stream 的真对位是 NetworkHandler.cs:Start（其文档 :33 已挂），再挂 HandleNewConnection 属副挂。next 票的做法会把无锚点缺口固化

来源：next/agy.net.md 条 14、条 15、条 16（三条同型同域，一棒收口，勿拆三票各改一半）。

判据取法（务必按此复现，勿信票内行号）
- 复刻 js/check.js:304-335 dupDefFind 的口径（CS_REF_REGEX 只吃函数级 `///` 文档注释，struct/字段/枚举变体/模块 `//!` 文档不计）：`bun` 跑一段只 import js/check/rustScan.js 的脚本即可，rustScan 纯读、不回写 ignore 语料；禁在主仓跑 js/check.js 本体（会重排 js/check/ignore/**）
- 现刻实测重复共 15 组，其中 net 域 7 组（下列 3 条 + 复核追加段的 4 组）；其余 8 组（GarnetRecordTriggers.cs:OnDispose、LuaRunner.Functions.cs:ProcessCommandFromScripting、GarnetInfoMetrics.cs:GetDatabasePersistenceStats 与 :GetDatabaseStoreStats、RangeIndexManager.Locking.cs:AcquireExclusiveForDelete、VectorManager.cs:VectorManager、StoreWrapper.cs:GetDatabasesSnapshot、Tsavorite.cs:ContextReadWithPrefetch）属 db/design/my/lua 域，本票不越界

条一（源 next/agy.net.md 条 14）GarnetServerTcp.cs:HandleNewConnection
- 现状：/Users/z/git/db/wedb/wedb/wnode/src/net/handler/drive.rs:35 NetworkHandler::process_stream 的文档 :32-34 同时挂 NetworkHandler.cs:Start 与 GarnetServerTcp.cs:HandleNewConnection（模块头 :3-5 另列一遍）；/Users/z/git/db/wedb/wedb/wnode/src/servers/consumer_registry.rs:357 ConsumerRegistry::try_acquire_connection 的文档 :349 也挂 HandleNewConnection
- C# 对位：garnet/libs/server/Servers/GarnetServerTcp.cs:226 HandleNewConnection —— accept 成功回调本体：:234 复位退避、:236-241 activeHandlerCount 递增与 networkConnectionLimit 判定、:302-307 超限即 Decrement + AcceptSocket.Dispose、随后建 handler 并 Start
- 修法：HandleNewConnection 锚点归 accept 本体承接点 run_tcp_accept_loop（/Users/z/git/db/wedb/wedb/wnode/src/server.rs:975，其文档 :971-974 现只有散文行号引用、无函数锚点，正是缺口）；try_acquire_connection 的 :349 撤锚改散文（其本体是 C# :236-241 的容量门子步骤，:351-356 的语义说明保留）；process_stream 撤掉 HandleNewConnection 一行（:33 的 NetworkHandler.cs:Start 是它的真对位，保留），drive.rs 模块头 :5 同步删该 bullet
- 判据：`bun` 复刻扫描里 GarnetServerTcp.cs:HandleNewConnection 只命中 1 个函数（限定符号 run_tcp_accept_loop），NetworkHandler.cs:Start 仍命中 process_stream 单点，重复定义组数由 8 降到 7

条二（源条 15）GarnetTlsOptions.cs:GetSslServerAuthenticationOptions
- 现状：/Users/z/git/db/wedb/wedb/wnode/src/tls/config.rs:98 server_config（文档 :91 挂锚）与 :61 ServerTlsConfig::from_der（文档 :57 挂同一锚）
- C# 对位：garnet/libs/server/TLS/GarnetTlsOptions.cs:122 GetSslServerAuthenticationOptions（构造 SslServerAuthenticationOptions：服务器证书 + 客户端证书校验器 + 协议版本），装配单点即 rust 的 server_config；from_der 只做 DER 证书/私钥字节入装的证书面
- 修法：锚点留 server_config 单点；from_der 撤锚，只留散文说明其承载 C# 证书装载段。两条硬约束：撤锚后不得改挂 GarnetTlsOptions.cs 的其它函数名（无对位即不挂），也不得抢挂 CertificateUtils.cs:GetMachineCertificateByFile —— 该锚点是 task/ing/tls-pem-loader-single-source.md 的落点，两票撞同文件，必须串链：先 PEM 下沉、后本票收口（下沉会移动 config.rs 的行号与 from_der 文档块）
- 判据：扫描中 GetSslServerAuthenticationOptions 只命中 server_config；GetSslClientAuthenticationOptions（wconn/src/tls.rs:26 与 :46 两处，其中 :26 为 struct 文档不计）仍保持单函数命中

条三（源条 16）SubscribeBroker.cs:StartAsync
- 现状：/Users/z/git/db/wedb/wedb/wnode/src/service.rs:607 spawn_pubsub_consume_task（文档 :601 挂 StartAsync，指 ConsumeAllAsync 循环）与 /Users/z/git/db/wedb/wedb/wpubsub/src/subscribe_broker.rs:564 SubscribeBroker::consumer_finish（文档 :563 挂同一 StartAsync，指 finally 段）
- C# 对位：garnet/libs/server/PubSub/SubscribeBroker.cs:118 StartAsync —— :129 `await iterator.ConsumeAllAsync(...)` 循环体、:134 finally 的 done.Set()；:176 的 `_ = Task.Run(() => StartAsync(cts.Token))` 是宿主 spawn 位；Dispose 在 :392（其 done.WaitOne() 半边 rust 已是 wait_consumer_exit，subscribe_broker.rs:574，:571 用散文引用 Dispose、不挂锚）
- 修法：StartAsync 锚点归宿主消费循环 service.rs:607 单点（它是循环本体承接方）；consumer_finish 的 :563 撤锚，把「C# StartAsync 的 finally done.Set()」写成不带 `.cs:` 的散文（禁反向操作——把锚点从宿主挪到 consumer_finish 会让 C# 循环体失去登记，属绕过检查）；Dispose 锚点维持 subscribe_broker.rs:591 单点不动
- 判据：扫描中 SubscribeBroker.cs:StartAsync 只命中 spawn_pubsub_consume_task；wpubsub 侧 SubscribeBroker.cs 其余锚点（Subscribe/Broadcast/Dispose/Initialize 等 :152-:554 共 13 处）不新增不丢失

验收（三条合并）
- 复刻扫描的重复定义组数由现刻 15 降至 12（本票三条各自命中数归一），且本域 3 组清零；若主代理按复核追加段把 7 组一并交本票收口，则降至 8、余 8 组逐条点名归属域，不得顺手改他域锚点
- cargo check --workspace --all-targets 零警告、禁 #[allow]；纯注释改动，禁动任何函数体
- 改动前后 `git diff --stat` 只允许出现 drive.rs、consumer_registry.rs、server.rs、tls/config.rs、service.rs、subscribe_broker.rs 六个文件的注释行增删（tls/config.rs 若与 PEM 票串链则本票只动其 from_der 文档块）
- 文档注释仍满足 SKILL 格式要求：承接 C# 函数本体的 rust 函数须带 `/// 在 garnet 中的相对路径: <相对路径>:<函数名>`；子步骤只写散文，不得为消重删掉本体锚点

互斥与边界
- 与 task/ing/tls-pem-loader-single-source.md 在 wnode/src/tls/config.rs 交叠，按上文串链顺序执行；与 task/ing/garnet-client-unix-socket-connect.md 在 server.rs 的 UDS 臂（:598 start_unix_worker、:656 容量门调用）可能交叠，本票只动 :971-975 的文档块，不碰其代码
- 在途实测更正（2026-09-19 现刻，本条作废原「/tmp/fork 仅 C# 快照、无在途分支」的说法）：`git worktree list` 实得 11 个 fork（docs-data-comment-batch、docs-readme-crate-map、fix-custom-obj-multikey、fix-lua-pending-handoff、fix-pending-lat-mget、fix-reviv-crtt-gate、fix-rmw-atomic-window、fix-vector-registry-two、split-cluster-provider、windex-2pl-removal，另有 /tmp/fork/dev-2026-09-19 为 garnet C# 快照非本仓 worktree）。其中唯一与本票文件交叠的是 fix-lua-pending-handoff（`git diff --name-only dev...` 命中 /Users/z/git/db/wedb/wedb/wnode/src/net/handler/drive.rs）——本票条一动 drive.rs 的模块头 :3-5 与 process_stream 文档 :32-34，须在其合入后再改，或以其合入后的 drive.rs 现文重取锚点位；其余 fork 触及的文件（resp_server_session.rs、resp_session_consumer.rs、traits.rs、vector/*、sync_transport.rs、cluster_provider/*、README/readme/*）与本票六文件零交叠
- 本票与 wtxn/wkv/wnode/src/resp/objects 的 rmw 路径（fix-rmw-atomic-window 在跑）零交叠：纯注释改动且不改任何函数体
- 另注：dev HEAD 在分拣期间被并发会话多次推进（be2fca2 → 74186c9 量级），本票全部行号按现刻重取，落工前必用 `grep -n` 按符号名定位，勿照抄
