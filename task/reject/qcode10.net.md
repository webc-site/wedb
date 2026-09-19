qcode 第 10 轮 net 视角审查（wresp / wconn / wnode resp+net / 会话门控 / 复制线面）分拣档案

原主张文件：next/qcode10.net.md（AI 生成，取证基线为 /tmp/rev10/wedb = dev fd17e895 快照，
行号已过期，本轮全部按符号在主仓 /Users/z/git/db/wedb 重定位）。该文件八条主张已全部裁决完毕、
文件清空后删除，本档案与 task/ing 下的新立文档是唯一留存。

裁决总览：三条已在 dev 上落地（条 1、条 2、条 7，证书见下）；三条成立且已有同主题单问题细化票在
队列中（条 3 → task/ing/qcode10-parse-db-index-i32-parity.md、
条 4 → task/ing/qcode10-enable-debug-command-knob.md、
条 8 主条 → task/ing/qcode10-client-outstanding-admission-gate.md，三张现仍在 next/ 待认领，
本轮不重复立项）；两条成立并新立细化票（条 5、条 6）；
条 8 的副条成立并新立票，但其论证前提有一部分被判不成立（记于本档案第二节）。

条 3 落地口径提醒：收口的是 RESP 线面的解析值域（对标 C# int32 的错误档位），
不是把本仓内部库 ID 的 u64 标量表示改窄（transpile 需求明列「原生 u64 库 ID 纯寄存器标量」）；
wconf 的 max-databases 定界 1..=256（/Users/z/git/db/wedb/wedb/wconf/src/node_options.rs:613-621）
决定值域收口无功能代价，但改派时勿顺手把内部 u64 改成 i32。

一、已落地（原条删除，证书按当下代码取）

条 1 wresp 错误文案表三枚常量零读者：两枚已消失、口径注释已补。
- RESP_ERR_SELECT_UNSUCCESSFUL 与 RESP_ERR_DB_ID_CLUSTER_MODE 全仓（含 tests）grep 零命中，
  已从常量表删除。
- 该条要求的「与刻意取消条款同形态的说明」已写入
  /Users/z/git/db/wedb/wedb/wnode/src/resp/admin_commands.rs:620-622
  try_parse_database_id 的文档注释：「C# 集群模式拒 dbId>0 的门禁刻意不收
  （doc/zh/db.md §1.3 已删除集群切库限制）」，判定序亦已写明「数字解析 → 范围门」，
  与 transpile 需求「删除 allow_multi_db 与集群限制」一致。
- 残余事实：/Users/z/git/db/wedb/wedb/wresp/src/cmd_strings.rs:204
  RESP_ERR_UBLOCKING_CLINET 仍是零读者死文案（全仓仅定义行与其 C# 锚点注释命中）。
  本轮不为其单独立项：单枚死常量的正确归属是零消费面普查批票（同族先例：已落地收档的
  task/done/zero-consumer-dead-surfaces-batch-four.md 与在队列的
  task/ing/zero-consumer-dead-surfaces-batch-five.md），但各批档现无此符号，
  须由主代理在派发零消费批时并入，或由后续普查轮重新捕获。

条 2 SET 选项环绕过过期选项单点：已按该条修法落地。
- /Users/z/git/db/wedb/wedb/wnode/src/resp/basic_commands/set.rs:670-684 现改调
  `wresp::options::try_get_expiration_option` 取 ExpirationOption，可接受集与「已出现过期间选项」
  判定为枚举比较（`matches!(parsed_option, Ex | Px | Keepttl)` 与 `exp_option != ExpirationOption::None`），
  与 C# BasicCommands.cs:628-633 同形态；EXAT/PXAT/KEEPTTL/PX 的重复 token 比较已全部消失，
  存在性选项亦经 try_get_exist_options 单点（set.rs:710）。
- 死别名 expiration_option_from_token 已从 wresp/src/options.rs 删除（全仓 grep 零命中），
  现文件内只剩 try_get_expiration_option 一处 token 判定（
  /Users/z/git/db/wedb/wedb/wresp/src/options.rs:192-223）。
- 载体：该主题的细化票经复核判为「修法已是仓库现状」而收档于
  /Users/z/git/db/wedb/task/reject/set-exist-options-single-source.md（同一 HEAD 取证，与本档结论一致）。

条 7 主端 AOF 时间脉冲帧 CLUSTER 子命令名写错（HIGH）：已修且已补往返测试。
- /Users/z/git/db/wedb/wedb/wconn/src/session.rs:256-257 常量前缀现为
  `*4\r\n$7\r\nCLUSTER\r\n$12\r\nADVANCE_TIME\r\n`，:261-269 encode_advance_time_frame 同源，
  注释亦订正为 ADVANCE_TIME。
- 锁死错误形态的两处断言已改：/Users/z/git/db/wedb/wedb/wedb/src/server/replication/
  aof_sync_task.rs:560 与 :584 现断言 `$12 ADVANCE_TIME`。
- 该条要求补的往返用例已存在且判据到位：
  /Users/z/git/db/wedb/wedb/wedb/tests/advance_time_frame_roundtrip.rs:30-49
  产帧喂副本侧解析器，断言 `Some(RespCommand::ClusterAdvanceTime)` 且 `!= Some(RespCommand::Invalid)`。
- 接收侧注册名 /Users/z/git/db/wedb/wedb/wnode/src/resp/parser/command_table.rs:375 未变（本就是正形）。
- 遗留动作：单问题票 next/aof-advance-time-frame-name-bug.md 的射程已全量落地，可由主代理直接归档删除。

二、论证前提被判不成立（结论仍成立，故已另立票，此处只记不成立的那部分）

条 8 副条 record_latency 与 wconn 客户端指标面。原条把 C# 的「参数面在、开关面在、消费面在」
三面齐备当作 rust 应当补齐的依据，此前提不成立：
- C# 侧该开关唯一入口是基准工具（garnet/benchmark/Resp.benchmark/Options.cs:110-111
  `[Option("client-hist")] ClientHistogram`），全部读者亦在基准树内
  （RespOnlineBench.cs:169/:180、TxnPerfBench.cs:176、ClusterBench/ClientRequestProvider.
  {Offline,Online,WorkerPool}.cs:619/:315/:119）。
- 而该基准树在本仓已被整体登记为不移植：
  /Users/z/git/db/wedb/js/check/ignore/benchmark.yml:240-242 逐条登记
  ClientRequestProvider.{Offline,Online,WorkerPool}.cs，同文件 :333 的理由为
  「C# BenchmarkDotNet/RESP 专属基准测试，Rust 侧采用 criterion/独立压测」。
  /Users/z/git/db/wedb/bench 压测树不使用 wconn 客户端（全目录 grep GarnetClient / histogram 零命中）。
- 故「为对齐 C# 而补 wconf 旋钮 + 造一个读者」会自造 C# 没有的宿主，违反 transpile 的 1:1 对标
  与「杜绝写死函数、严禁虚设实现」两条。成立的是事实层：生产构造点写死 false
  （/Users/z/git/db/wedb/wedb/wedb/src/client.rs:104 末参）、wconf 无旋钮、查询 API 无生产读者，
  即一整条恒不通电的观测链。裁决为删面而非补面，细化见
  task/ing/client-latency-histogram-unwired.md（其中「反向方案」段保留了两面齐的触发条件）。

三、自审撤销的初判（原文自带的五条撤销，本轮复核其撤销理由仍然成立，转录以免后续轮次重复误报）

- 「wnode/src/net/handler/buffer.rs 收包追加路径无上限」撤销成立：追加前统一经 buffer.rs 的
  try_reserve，容量不足返回 ReserveError，不属可立案缺口。
- 「wconn 客户端无在途超时」撤销成立：wconn/src/client.rs 建连按 timeout_millis > 0 才建
  PumpProgress 并 spawn timeout_checker，与 C# timeoutMilliseconds > 0 即启用 TimeoutChecker
  同一开关口径。
- 「RESP3 push/set/map 帧头在 wresp 多处重复拼装」撤销成立：长度写侧统一经 wresp/src/length.rs
  与 RespWriter 单点，属同构双份。
- 「pub/sub 邮箱双路等待与 C# 不同构」撤销成立：C# PubSub 侧同为订阅集合加超时轮询双支。
- 「dial 面 socket 选项缺 TCP_NODELAY」撤销成立：wconn/src/client.rs:84 建连即 set_nodelay(true)，
  与 C# GarnetClientSession.cs:268/:279、GarnetServerTcp.cs:249 的 NoDelay 同构。

四、自审核查通过位点（非发现，转录以免后续轮次重复误报）

- 快速路径模式表 18 条与 C# RespCommandSimdPatterns.cs:60-83 逐条对齐，argCount 口径差属约定
  （rust 头注释自述 N = 参数个数 + 1）。
- RespCommand 变体分派覆盖：以双形式（`RespCommand::X` 与短别名 `C::X`）重扫 367 变体无未落地臂，
  首轮「零分派」结论属误报。
- RESP2/RESP3 版本类型单源，与 C# libs/common/Networking/WireFormat.cs:10-16 同构。
- 复制/无盘同步线面命令名（APPENDLOG / ATTACH_SYNC / BEGIN_REPLICA_RECOVER /
  INITIATE_REPLICA_SYNC / SNAPSHOT_DATA）与 C# RespCommandHashLookupData.cs 逐字节一致，
  唯一名字漂移 ADVANCE_TIME 已随条 7 修毕。
- gossip 连接超时投影与 C# GarnetServerNode.cs:69-70 一致（旋钮对齐另有在册票）。
- 输出面门面方法零读者属 1:1 同构双份，不报。
- 命令查表归一化（精确字节二分加 make_upper_case）与 C# ignoringCase 哈希查表在
  「大小写不敏感、下划线敏感」上等价。
- SET 过期选项单点函数本体正确无缺陷，条 2 报的是消费侧绕行与零读者别名。
