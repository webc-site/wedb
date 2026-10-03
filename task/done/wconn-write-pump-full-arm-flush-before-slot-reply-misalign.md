甄别结论：通过（甄别席 J7，2026-09-27，定级 P2——满臂 flush 先上线后注册的窗口孤儿误判断连，改序零新机制）。pump.rs:152 extend 先行、:161 flush 先上线、:162 send 后注册，窗口坐实；:276-280 孤儿误判 Error::Server 断连亲验；C# :732 tcsArray LoadFrom 槽注册、:721 AwaitPreviousTaskAsync 前置亲验；审核席「成功臂永久右移不成立、危害上限误断连」修正复核成立，P2 恰当。派沙箱席 c01o。

审核结论：通过（P2）

独立审核复核（2026-09-27，亲验 pump.rs/replies.rs 全文与 C# InternalExecuteAsync）：
1 窗口可达性属实：满臂 pump.rs:152 先 extend cur.frame、:161 flush 即真实上线、:162 send 挂起；写泵恢复仅靠读泵认领腾位，而 dispatch_replies（replies.rs:192 try_recv 非阻塞）单趟同步认领完全部 older 后通道必然瞬时为空，cur 应答若同批在 read_buf 即残留。回环 + 批量刷出 + 16KB 读块合并下并非窄窗。
2 错误臂属实：残留完整 -ERR 行 + 队空 → pump.rs:276-280 误判孤儿 → Error::Server 断连，roundtrip（types.rs）全在途结算 ResponseChannelClosed。C# 侧已亲核：帧入本地页缓冲（对端不可见）→ AwaitPreviousTaskAsync → tcsArray[shortTaskId].LoadFrom(tcs)（:732）注册后才可上线，错误应答恒达单个调用方，破缺成立。
3 票面一处修正：成功臂「配对永久右移一拍」不成立。残段恒为「未认领帧后缀」的应答，通道 FIFO=帧序、线上应答序=帧序，两侧同为同序后缀，cur 经 stalled/Wake::Command（pump.rs:302/:356）与残段确定性配对正确；且满臂挂起后写泵不再处理下一条 pending，任一时刻至多一帧处于上线未注册态，多帧错位不可达。故危害上限为误断连（显性、可见），无静默数据面错位——此为定 P2 非 P1 的依据。
4 方案复核：改序后 :161 前缀 flush 保住挂起期应答回收链（注释 :153-156 死锁防线不动），fire-and-forget 帧本不入在途、孤儿判语义不变，out_buf 攒批摊薄 syscall 与线上帧序（extend 先于下条 pending）均保留，无新机制、零新增分配。方案可落。

wconn 写泵在途闸满臂先刷帧后占槽：cur 帧随 flush_write_buf 先行上线、在途槽位其后才注册，读泵同步单趟派发在该窗合并到达时孤儿误判断连（C# 槽位注册严格先于帧上线不变式破缺）

问题分析：
1 Garnet 契约对齐：C# GarnetClient 的应答槽注册严格先于帧上线——garnet/libs/client/GarnetClient.cs:InternalExecuteAsync 先 networkWriter.TryAllocate 把帧写入页缓冲（对端不可见），经 AwaitPreviousTaskAsync 等槽后在 :732（同型 :845/:1049/:1164）完成 tcsArray 槽注册，此后才有网络写出；GarnetClientProcessReplies.cs 按 tcsOffset 顺序配对，槽位恒在位，「应答不可能先于其槽位存在」。
2 工程现状确证：wedb/wconn/src/network/pump.rs:152 写泵循环顶先把 cur 帧拼入 out_buf（out_buf.extend_from_slice(&cur.frame)），:160-167 闸满臂先 flush_write_buf（:161，cur 帧此刻随批真实上线）再 in_flight_tx.send(cur).await（:162，槽位此刻才注册）——窗口内 cur 的应答已可在途。读泵 dispatch_replies（pump.rs:258-264 循环顶、replies.rs:181-263）为同步单趟：older 在途项应答与 cur 应答合并同一读批次时，一趟把 older 槽全部认领后到 cur 应答时 in_flight 恰好瞬时为空（cur 未入队、写泵未恢复调度，compio 同线程协作调度下交错不可发生故同批合并条件下错位必然）——错误臂 pump.rs:276-279 orphan_error_reply（replies.rs:157-165）把 cur 的 -ERR 误判 fire-and-forget 拒收，读泵按 Error::Server 断连、全部在途命令结算 ResponseChannelClosed；成功臂残段滞留，cur 入队后经 Wake::Command（pump.rs:356）与陈旧应答错位配对，自该帧起整连接配对永久右移一拍。
3 逻辑危害确证：触发前提为带应答在途数打满闸值（wedb facade 恒 32，wedb/wedb/src/client.rs:20；直用 wconn GarnetClient 可至 1<<20）且爆发批次跨过刷出阈值。错位形态无错误无断连——gossip/迁移应答逐拍右移（配置串配错轮、ACK 配错批）静默发散，比断连更险。

涉及代码：
rust 文件与函数：
wedb/wconn/src/network/pump.rs:write_pump（:151-167 满臂序）、read_pump（:276-279 孤儿判、:356 Command 配对）
wedb/wconn/src/network/replies.rs:dispatch_replies / orphan_error_reply（:157-165）
wedb/wconn/src/types.rs:roundtrip

对应 c# 文件与函数：
garnet/libs/client/GarnetClient.cs:InternalExecuteAsync（TryAllocate → AwaitPreviousTaskAsync → :732 槽注册先于上线）
garnet/libs/client/GarnetClientProcessReplies.cs:ProcessReplies（tcsOffset 顺序配对）

精炼执行方案：
1 满臂改序保不变式：Full 臂先 flush_write_buf（仅含已入队前缀）再 send(cur)，cur 帧的 out_buf.extend 挪到槽位注册成功之后；fire-and-forget 臂不受影响（孤儿判语义保留）
2 单测：闸值=2 小客户端打满闸后发第三条命令，mock 服务端三应答合并一次写出，断言第三条配对正确无断连（现码应复现）
3 回归 wconn/tests/client_timeout.rs 与 network_tcp.rs 全量

审核裁定执行方案（审核席整理，供 task/fix.md 直接消费）：
1 pump.rs write_pump 满臂改序：保留 :161 flush_write_buf（仅含已注册前缀，死锁防线不动）；in_flight_tx.try_send Ok 臂与 send(cur).await 成功臂均在槽位注册成功后再执行 out_buf.extend_from_slice(&cur.frame)；ReplyTx::None 帧维持原位 extend（无槽位，无不变式约束）；后续阈值分片（:180）与批尾刷出（:186）不动，cur 帧注册后随批自然上线，线上帧序不变
2 单测修正（原方案第 2 条需收紧才可确定性复现）：第三条（窗口帧）的 mock 应答必须用完整 -ERR 错误行——错误臂下现码经 pump.rs:276-280 误判孤儿断连，断言「连接存活 + 错误送达第三条调用方」即红；若第三应答用 +PONG 成功形，现码经 Wake::Command 自愈配对、测试恒绿，无法复现。可在同测补成功形残段用例断言自愈序（现码亦绿，锁改序不回归）。write_pump/read_pump 均 pub，gate=2 可直连泵通道注入，mock 形态对标 wconn/tests/network_tcp.rs
3 回归 wconn/tests/client_timeout.rs、network_tcp.rs、network_socket_integration.rs 全量
