甄别结论：通过（甄别席 J5，2026-09-27，定级 P2——TLS/排队窗系统性可观测分叉，CLIENT KILL MAXAGE 与 INFO 自报判据错位，运维诊断不可复现，无数据面危害）。亲验双源：条目源 wedb/wnode/src/servers/consumer_registry.rs:489 creation_ticks: now_ms_i64()（accept 预注册，server.rs:1343 调用亲验），会话源 wedb/wnode/src/resp/resp_server_session/core.rs:622 creation_ticks: session_now_ms()，两源零回传；消费点 client_commands.rs:560 KILL MAXAGE 读条目源、core.rs:1706 INFO 读会话源、consumer_registry.rs:319 LIST 行读条目源，分叉确凿。C# 单源亲验：RespServerSession.cs:269-270 构造期唯一落笔，ClientCommands.cs:483 与 BasicCommands.cs:1960 共读 CreationTicks。勘误两件：consumer_registry.rs 落笔现 :489（票面 :503）、LIST 行现 :319（票面 :333），漂 14 行。方案经 net/handler/mod.rs:116 set_session（亲验在位）从条目回填会话一行收口，三消费点零改动，无第二机制，时基单调论证成立。查重：§154/§63 均 KILL 语法面正交，全册无年龄时基登记。派沙箱席 c01f。

审核结论：通过（真案，可达性较票面更强。四锚亲验：条目源 accept 落笔 consumer_registry.rs:503（server.rs:1343 先于 TLS 握手与首字节），LIST:333、KILL MAXAGE client_commands.rs:560 读它；会话源 RespServerSession::new core.rs:622 自取 session_now_ms，INFO:1706 读它；ClientView 无投影无回传，分叉坐实。会话实为泵内首字节批才建（drive.rs:152-160），非 TLS 空闲连接亦拉大分叉——「INFO age=0 却被 MAXAGE 5 杀」连接池形态即观测，TLS 叠加握手段（≤10s §102）。C# 单源验真：RespServerSession.cs:270 唯一落笔，ClientCommands.cs:157/483、BasicCommands.cs:1941/1960 共读 CreationTicks。五池无同案，§63/§154/§101/§102 正交）

整理执行方案（审核席订正版，供 fix 消费）：
1 统一取条目源（accept 起算贴真 Redis；反向会话源会使握手中条目 age 倒退违时基单调）。注入点改省：经 NetworkHandler::set_session（mod.rs:116 既有 accept 侧端点单源注入口）从 self.consumer_entry 回填 session.creation_ticks 一行收口，条目 None（嵌入/哑桩）回落现自取；三消费点零改动，勿走 options 传参第二态
2 锁测：client_commands_tests.rs 补 LIST/INFO age 全等与 MAXAGE 一致性用例
3 票面订正：「deviations §1343」实为 §102 行号指涉，落册修锚

CLIENT 会话年龄双时钟源：注册表条目 accept 时刻与会话构造时刻并存，致 CLIENT LIST/KILL MAXAGE 与 CLIENT INFO 的 age 对同一连接读数分叉

问题分析：
1. C# 契约对齐：CreationTicks 为会话级唯一年龄源（garnet/libs/server/Resp/RespServerSession.cs:270 构造期 `this.CreationTicks = Environment.TickCount64` 单点落笔），CLIENT LIST 行、CLIENT INFO 行与 CLIENT KILL MAXAGE 判据三处共读同一字段（ClientCommands.cs:483 `targeAge = (nowMilliseconds - targetSession.CreationTicks) / 1_000`；BasicCommands.cs:1959 WriteClientInfo 内 `ageSec = (nowMilliseconds - targetSession.CreationTicks) / 1_000`，LIST/INFO 共函数）。C# 中同一条连接的 age 只有一个真值，无 LIST 与 INFO 互异之可能。
2. 工程现状确证：rust 侧并存两个出生时刻源且互不同步。源一为注册表条目 ConsumerEntry.creation_ticks，在 accept 预注册期落笔（wedb/wnode/src/server.rs worker_accept_loop 臂 `preregister_consumer` → consumer_registry.rs:register_with_type:503 `creation_ticks: now_ms_i64()`），CLIENT LIST 行（consumer_registry.rs:ConsumerEntry::write_client_info:333）与 CLIENT KILL MAXAGE 判据（client_commands.rs:ClientKillFilters::matches:560）读此源；源二为会话字段 RespServerSession.creation_ticks，在 TLS 握手完成、serve_connection 装配会话时另起炉灶落笔（resp_server_session/core.rs:RespServerSession::new:622 `creation_ticks: session_now_ms()`），CLIENT INFO 自身行读此源（core.rs:write_client_info_state:1706）。两源之间全仓无任何回传或对齐链路（grep 全仓 creation_ticks 仅上述四点），本文件头自陈「动态字段真值单源在会话字段，注册表条目持一份跨线程可读投影」，但 creation 时刻恰未纳入 ClientView 投影轨，成漏字段。
3. 逻辑危害确证：TLS 部署下（握手超时缺省十秒量级，见 deviations §1343 收口注）握手与装配耗时全部计入条目源、不计入会话源，同一连接 CLIENT LIST 报 age 恒大于其自身 CLIENT INFO 报值，偏差随握手 RTT、Slowloris 逼近超时与连接任务排队时长放大；CLIENT KILL MAXAGE 按条目源裁杀而客户端自报年龄按会话源，出现「INFO 见 age=1 却被 MAXAGE 5 命中」的判据错位。运维按 age 做连接寿命诊断与 KILL 白名单校验的结论不可复现。另按现两源形态任何单向修齐（如会话构造期回写条目）都会使握手期已可见的条目年龄中途改基、age 读数倒退，触本域时基单调红线，须同源单机制收口而非补钉。deviations 全册无本面登记（§63 裁 UNBLOCK 负数 ID、§154 裁 KILL ID 非正值、§101 裁容量计数，均不涉年龄时基），五池无在途同案。

涉及代码：
rust 文件与函数：
wedb/wnode/src/servers/consumer_registry.rs:register_with_type / ConsumerEntry::write_client_info / ConsumerEntry.creation_ticks
wedb/wnode/src/resp/resp_server_session/core.rs:RespServerSession::new / write_client_info_state / RespServerSession.creation_ticks
wedb/wnode/src/resp/client_commands.rs:ClientKillFilters::matches
wedb/wnode/src/server.rs:worker_accept_loop 预注册臂

对应 c# 文件与函数：
garnet/libs/server/Resp/RespServerSession.cs:构造函数 CreationTicks
garnet/libs/server/Resp/BasicCommands.cs:WriteClientInfo
garnet/libs/server/Resp/ClientCommands.cs:NetworkCLIENTKILL.IsMatch

精炼执行方案：
1. 时基单源收口取条目源（accept 时刻，即真 Redis age 从连接接入起算之语义，且天然满足单调无倒退；C# 单源不分叉契约经 rust 预注册可见性外扩后唯一可两全的落点）：会话出生时刻改由挂载点承接——serve_connection 会话装配处把已注册条目的 creation_ticks 注入会话（经既有会话装配选项单点传入，替代 RespServerSession::new 内 session_now_ms() 自取；注册表缺席形态（嵌入式单测会话）回落现自取臂，语义不变）。
2. write_client_info_state、ConsumerEntry::write_client_info、ClientKillFilters::matches 三消费点零改动，注入后三处共读同一真值即自动收敛；RespServerSession.creation_ticks 字段头注补「源=预注册条目，accept 时基」回指，防后审按 C# 构造期口径回改。
3. 测试验证点：wnode/tests/client_commands_tests.rs 新用例——同一连接断言 CLIENT LIST 行与 CLIENT INFO 行逐字段全等（含 age）；CLIENT KILL MAXAGE 判杀与 INFO 自报 age 一致性用例（阈值恰夹于两读数间时不再出现「可见年龄未达阈值却被杀」）；TLS 形态回归经 wnode_tls_test 既有夹具探测握手期条目可见性与握手完成后 LIST/INFO 年龄同源。

收口记录（收票席 R3 批次，2026-09-28）：合入 15430477（验货 commit 67131d8c）。收口形态=age 时基单源收口为 accept 预注册条目真源，泵 set_session 经 set_creation_ticks 装配期单点回填，LIST/INFO/KILL MAXAGE 三消费点零改动共读同基；哑桩形态回落构造期自取。偏差已登 §172。锁测 client_commands_tests.rs::client_list_info_age_single_source_and_maxage_consistency（真 TCP 慢首字节窗复刻，11/11 绿）。
