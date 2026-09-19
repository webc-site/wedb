优先级：中
分拣注记（qw.my 第 11 轮条 2 拆出；浅核 2026-09-19：sanitize_error_str wresp/src/ext.rs:31（漂移自 :15）、wmetric info :66-:68 与 latency :45-:46 extend_from_slice、wlua executor :422-:424 write_direct 三旁路全部在场；台账命中的 extend_from_slice 均为别题（发送路径字节帽/txn marker/null 协议），非重复）

RESP 输出门面的帧注入净化有三处手写回显旁路，攻击者字节可原样成帧
问题：我方自立的错误帧净化单点 wresp/src/ext.rs:15 sanitize_error_str（注释「以 \r 或 \n 截断
防止 RESP 协议帧注入」+ MAX_ERROR_MSG_LEN 512 / MAX_PARAM_NAME_LEN 128）被门面内各口一致调用
（cmd_strings.rs:386/:397/:405/:414/:430、resp_memory_writer.rs:702/:709、
wnode/src/resp/resp_server_session.rs:2293、parser/resp_command.rs:629-631、
basic_commands/set.rs:168-169），但三处绕开门面裸写：
- wmetric/src/latency/resp_latency_commands.rs:44-48（network_latency_histogram 三段
  extend_from_slice，invalid 即 :103 parse_events 原样返回的客户 arg）；
- wmetric/src/info/info_command.rs:63-68（network_info 段名回显，同型，两处注释均写
  「原样回显（二进制安全，不做 lossy 转写）」）；
- wlua/src/runner/executor.rs:421-424（write_direct 三段裸写 Lua err_buf）。
调用链可达：wnode/src/resp/metrics_commands.rs:42 与会话 INFO 臂
wnode/src/resp/resp_server_session.rs:1735 都把 self.parse_state 的客户原始终串喂进去，而
bulk string 内的 CRLF 由解析器原样保留，故 `LATENCY HISTOGRAM` / `INFO` 的一个参数含 CRLF 即
在一帧错误里注入第二帧，客户端应答流错位；同时长度不受 512/128 上限约束。
C#：garnet/libs/server/Metrics/Latency/RespLatencyCommands.cs:68 与
garnet/libs/server/Metrics/Info/InfoCommand.cs:51 都走单口 RespWriteUtils.TryWriteError，
lua 侧同为 TryWriteError 单口——C# 根本没有净化层，净化是我方输出门面自己加的机制，因此这三处
是我方机制的旁路断链，不是对标差异。
修法：三处改 cs::write_error_raw / abort_with_*（回显段先按 MAX_PARAM_NAME_LEN 净化），或把净化
下沉到 RespWriter 的错误帧出口使所有路径同源；收口判据：grep `extend_from_slice(b"-ERR` 与
`write_direct(b"-ERR` 在 wmetric/wlua 生产面归零，并补一条含 CRLF 参数的注入回归用例。
条款：SKILL.md:65（一处定义、收敛重复）、SKILL.md:10（自定义改造须自洽收口）。
