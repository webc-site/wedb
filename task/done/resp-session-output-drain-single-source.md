会话应答冲取出面四份同段记账：take_output / send_and_reset / resolve_blocked_wait 三枚零生产消费者副本口与 flushed_bytes 零读者死字段

来源：next/resp-server-session-file-split.md（该「按 C# partial 边界纯移动拆分」票判否归档，见
/Users/z/git/db/wedb/task/reject/resp-server-session-file-split.md）分拣复核其「输出队列管理段」
取证时暴露的真实重复机制与死面，独立立项。
取证基线：主仓 /Users/z/git/db/wedb，dev HEAD 228c1963，
/Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session.rs 现 3166 行。
行号按符号定位，开工前重取（该文件在两个快照间已从 3141 漂到 3166）。

现状（主仓 HEAD 实测）

一、同一段「冲写出记账」在本文件内有四份实现。
- resolve_blocked_wait_into :733-754：:749-752 `self.flushed_bytes += written as u64;`
  加 `if let Some(metrics) = &self.session_metrics { metrics.incr_total_net_output_bytes(..) }`。
- send_and_reset :2222-2232：:2226-2229 同两段，量取 `self.output.len()`。
- take_output :2241-2248：:2243-2246 同两段，量取 `mem::take` 出的 `pending.len()`。
- take_output_into :2251-2269：:2265-2268 同两段。
即「累计出向字节 + 会话指标 incr_total_net_output_bytes」这一件事抄了四遍。

二、其中三枚口在生产不可达或可由单点替代（口径 = 全仓 src/ 视图，tests/ 不计）。
- take_output :2241：src/ 零调用。全部 128 处调用在 wnode/tests/
  （resp_server_session_tests.rs 60、session_metrics_slowlog_tests.rs 26、acl_tests.rs 15、
  resp_command_parse.rs 8、server_monitor_tests.rs 8、lua_script_tests.rs 4、
  transaction_session_test.rs 2、aof_replay.rs / resp_pubsub.rs / resp_tests.rs /
  resp_commandstats_session.rs / tiered_watch_fence.rs 各 1）。
- send_and_reset :2222：src/ 唯一调用点是同文件 :2746（RespScriptingApi::dispatch_resp 收尾），
  而其上一行 :2745 已是 `response.extend_from_slice(&session.output)`——
  两行合起来正是 take_output_into 的函数体（并入目标缓冲 + 清空 + 记账），
  写出字节集相同（take_output_into 在 `out.is_empty()` 时走 swap 快路径，
  send_and_reset 恒 extend，二者产出的字节序列一致）；该调用点未取返回值，
  false 分支的唯一读者是 tests/resp_server_session_tests.rs:553-554。
- resolve_blocked_wait :756-766：文档注释自称「向后兼容接口」，函数体是建本地 Vec
  再转调 :762 `resolve_blocked_wait_into`，即 :733 的薄壳副本。生产驱动只调 `_into`
  （/Users/z/git/db/wedb/wedb/wnode/src/net/handler/drive.rs:182），
  消费者侧 override 见 resp_session_consumer.rs:247-256；Vec 形态的 src/ 转发
  （resp_session_consumer.rs:243-245）与 trait 默认体
  （/Users/z/git/db/wedb/wedb/wnode/src/traits.rs:97-113，其 `_into` 默认体反向调用
  resolve_blocked_wait）在生产均不可达——其余 MessageConsumerFace 实现者是
  server.rs:1161 的 cfg(test) 哑消费者与 tests/{node_test.rs,tls_test.rs,
  net_pump_consume_tests.rs} 的夹具，均不触发阻塞命令。Vec 口活读者只有
  tests/resp_blocking_commands.rs:96、:109。
- 在册死面普查三批均未含本族三口：task/ing/zero-consumer-surfaces-batch-two.md、
  task/done/zero-consumer-dead-surfaces-batch-five.md 与
  next/zero-consumer-dead-surfaces-batch-six.md 在 resp_server_session.rs 的命中面是
  attach_monitor、reset_all_latency_metrics、set_client_lib_info 与 write_direct_large
  （批六自陈第 13 轮已判 write_direct_large 形态分叉不裁），无本票三口。
  本票补登记，后续批次据此排除，勿再判为「新出」。

三、flushed_bytes 是零读者死字段。
声明 :333（其 :332 注释写「累计冲洗字节数（Send 累计，测试断言用）」）、初始化 :498、
累加 :749、:2226、:2243、:2265。全仓 `flushed_bytes` grep 命中恰好这 6 行
（含全部 crate 与 tests/），无一处读取、无访问器，注释所称「测试断言用」不实。

四、双套账不属本票（划界，勿顺手裁）。
泵侧另有 /Users/z/git/db/wedb/wedb/wnode/src/servers/consumer_registry.rs:161-167
`add_net_bytes` 的 net_output_bytes 原子镜像，由 drive.rs:250 以「实写 socket 字节」累加、
monitor_sample（同文件 :513-522）取作监视器瞬时吞吐；会话侧
`incr_total_net_output_bytes` 由 info_provider.rs:100-103 的 snapshot 供本连接 INFO。
两套量口径不同（缓冲冲出量 vs 实写量）且各有读者，本票只把会话侧四份实现并成一份。

C# 对位（相对路径:方法名）
- garnet/libs/server/Resp/RespServerSession.cs:SendAndReset（:1348 无参臂、:1368 内存块臂）：
  判游标是否前进，前进则调 Send + 重取响应对象，不前进即 GarnetException.Throw。
- garnet/libs/server/Resp/RespServerSession.cs:Send（:1440）：全仓唯一出向字节记账点，
  :1462 `sessionMetrics?.incr_total_net_output_bytes((ulong)sendBytes)`
  （:1497 为其 DebugSend 变体）。C# 侧既无第二枚冲出口，也无会话级出向累计字段。
- 冲写点归属在 RespServerSession.cs：garnet/libs/server/Resp/RespServerSessionOutput.cs
  全文只有 :22 ProcessOutput 与 :31-:275 的 Write* 系列，对 SendAndReset 只调用不定义。
- rust 托管缓冲下的真对位物是 take_output_into（把累积应答并入泵写缓冲并复位），
  三口收口后 C# 的 :1348 与 :1440 两枚锚点应挂到它身上。

目标形态
1. 冲取出面单点：`take_output_into(&mut self, out: &mut Vec<u8>)` 成为唯一
   「把会话输出并入目标缓冲并记账」的口。
2. 记账单点：新增私有 `fn account_output(&mut self, bytes: u64)`，内含唯一一处
   `incr_total_net_output_bytes`；由 take_output_into 与 resolve_blocked_wait_into 两处调用
   （前者量取会话缓冲整段、后者量取直写 resp_buf 的增量，口径不同故保留两个调用点、
   一份实现）。
3. 删 take_output :2241-2248，128 处测试改走 take_output_into
   （`let out = s.take_output()` → `let mut out = Vec::new(); s.take_output_into(&mut out);`，
   可脚本批量替换后逐处核对期望字节不变）。
4. 删 send_and_reset :2222-2232，:2745-2746 两行并一行
   `session.take_output_into(response);`，其 :2218 的 C# 锚点随迁（见门禁）。
5. 删 resolve_blocked_wait 会话口 :756-766、resp_session_consumer.rs:243-245 的 override、
   traits.rs:97-100 的 trait 默认口，并把 traits.rs:102-113 的 `_into` 默认体改为空实现
   （不再回调被删口）；tests/resp_blocking_commands.rs:94-110 夹具改走 `_into`
   （自备本地 Vec）。
6. 删 flushed_bytes（:332-333 声明与注释、:498 初始化、:749/:2226/:2243/:2265 四处累加），
   不接读者、不留 cfg(test) 读口。
7. 交付态判据：1 个冲出口 + 1 个记账 helper + 0 个零读者累计字段；
   禁「四份记账变两份但仍有两个同义口名」的中间态。

门禁与验收

先行依赖（同文件在途票，只裁不交叠部分）：
- task/ing/lua-call-pending-suspend-handoff.md：本票第 4 步动的 :2745-2746 正落在该票
  dispatch_resp 改造段（其修法 2 还要在同点调 resolve_blocked_wait_into）。
  先落该票、后做本票，否则该票返工。
- task/ing/pending-lat-no-timing-site.md：本票射程（:332-333、:498、:733-766、:2218-2269、
  :2745-2746）与该票（:447 构造段、:543 set_garnet_api、:876 attach_monitor、
  :1082-1130 latency_batch_*）不交叠，但同文件先后落地须按符号重取行号。
- task/ing/session-metrics-option-dead-track.md：本票保持
  `if let Some(metrics) = &self.session_metrics` 读法不变，只并实现；该票删的是选项侧
  创建轨（:140-141、:483-485），与本票无交叠，同批开工共用一次文件改动即可。
- task/ing/zero-consumer-surfaces-batch-two.md：该票射程 :1325-1340 的 PING/ASKING 分派臂，
  与本票不交叠，同文件须错开提交。
- task/ing/qcode10-parse-db-index-i32-parity.md：射程 wbase/src/num.rs 与
  array_commands.rs / admin_commands.rs，不经本文件，无冲突。
- 死面批六（next/zero-consumer-dead-surfaces-batch-six.md，已被
  task/ing/claim-dead-b6.md 认领在途）：同文件射程 :876 attach_monitor、
  :893/:897 reset_*_latency_metrics、:2376 set_client_lib_info、:2288 write_direct_large，
  与本票三口互不含；两票同文件开工须错开提交，且本票「勿双裁 write_direct_large」
  一条以该批裁定为准。

锚点登记（禁靠散文蒙过 check.js，亦禁虚构锚点，见
/Users/z/git/db/wedb/.agents/skills/transpile/SKILL.md:77、:79）：
删 send_and_reset 后，`libs/server/Resp/RespServerSession.cs:SendAndReset` 与
`:Send` 两枚锚点须正式挂到 take_output_into 的文档注释（双锚点同挂一函数，参照
task/ing/pending-lat-no-timing-site.md 落地时 StartPendingMetrics/StopPendingMetrics
双挂 with_pending_metrics 的形态）；本文件 :110 与 :1279 两处含
`libs/server/Resp/RespServerSession.cs:SendAndReset` 形态的散文提及须复核判定口径，
若被计为锚点则改写成不含 `路径.cs:符号` 形式的说明。

验收：
- `grep -rn "flushed_bytes" wedb` 归零；`take_output\b`（非 _into）与
  `resolve_blocked_wait\b`（非 _into）在 wnode/src/ 归零，tests/ 仅剩 _into 走法。
- `grep -c "incr_total_net_output_bytes" wnode/src/resp/resp_server_session.rs` = 1，
  调用点 2（take_output_into、resolve_blocked_wait_into）。
- 线协议字节不变为准绳：不得为过测试改期望字节；`cargo check --workspace --all-targets`
  零 error 零 warning、禁 #[allow]；./test.sh 与 ./sh/clippy.sh 由中央整合轮执行；
  `bun js/check.js` 缺失与重复组数不高于基线树（按 /tmp/fork 基线逐字节比对）。
- 冲取面回归入 wnode/tests（BLPOP 完成回写与脚本重入收尾两处既有形态即本票的回归位）。

坑与边界
- 不动 pending_output_len :2236（生产读者
  /Users/z/git/db/wedb/wedb/wnode/src/resp/parser/resp_command.rs:448 的 AOF 提交模式门）、
  不动 take_output_watermark_yield :2281（活读者 drive.rs:173 与本文件 :2741）、
  不动 write_direct_large :2288（在册批六已按「形态分叉不裁」登记，勿双裁）。
- 不动三枚同名 no-op：wpubsub/src/session_commands.rs:175 的
  `PubSubSessionCommands::send_and_reset`（其 :591 调用点走 trait 方法，与会话 inherent
  口删除无关）、本文件 :3007-3009 的 trait override、
  /Users/z/git/db/wedb/wedb/wlua/src/runner/resp_convert.rs:52 `RespOut::send_and_reset`。
  三者是 C# 刷新点位（RespServerSession.cs:SendAndReset 与 LuaRunner.cs:SendAndReset）
  在托管缓冲下的空适配，带锚点与语义声明，删之反丢对位面。
- send_and_reset 的「空缓冲返回 false」是 C# 「写超缓冲仍无进展即抛」探针的 rust 投影，
  现无任何生产读者消费该 false。若整合轮判定该探针须保留，正确落点是在
  take_output_into 内按 C# 形态返回并处理，而不是留一枚仅测试使用的第二口。
- 128 处测试改写属机械替换：禁顺手改断言期望值、禁删断言；
  tests/resp_blocking_commands.rs:94-110 改写时保留「应答字节按序进 resp_buf」的原断言。
- 本票不做任何文件搬家：resp_server_session_output.rs 的 write_* 与本票的冲出口在 C# 分属
  RespServerSessionOutput.cs 与 RespServerSession.cs 两个 partial，不得并成同文件
  （原拆分票的合并方向即错在此，见 task/reject/resp-server-session-file-split.md 第二条）。
- info_provider.rs:100-103 与 client_commands.rs:498-500 读的是同一
  SessionMetricsHandle 快照，删 flushed_bytes 不影响二者；勿把本票误读成「会话指标出向
  字节改由泵侧单供」，那属第四条划界之外的另一议题。
